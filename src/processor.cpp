#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <csignal>
#include <pthread.h>
#include <semaphore.h>
#include <cerrno>
#include <string>
#include <iostream>

const int EXIT_SUCCESS_CODE = 0;
const int EXIT_BAD_ARGS    = 10;
const int EXIT_IPC_FAIL    = 20;
const int EXIT_CHILD_DIED  = 30;
const int EXIT_IO_ERROR    = 40;
const int EXIT_SIGINT      = 130;
const int EXIT_SIGTERM     = 143;

const char *FIFO_PATH_DEFAULT = "/tmp/os_proj_fifo";
const char *SHM_NAME_DEFAULT  = "/os_proj_shm";
const char *SEM_NAME_DEFAULT  = "/os_proj_ready_sem";

const unsigned int EOF_CHUNK_ID = 0xFFFFFFFF;

class ChunkHeader {
public:
    unsigned int chunk_id;
    unsigned int source_file_id;
    unsigned int byte_count;
};

const int MAX_CATEGORIES = 256;
const int CATEGORY_LEN   = 64;

class SensorEntry {
public:
    char   sensor_id[CATEGORY_LEN];
    double total_reading;
    long   record_count;
    long   anomaly_count;
};

class SharedData {
public:
    int    num_categories;
    long   total_records;
    SensorEntry categories[MAX_CATEGORIES];
};

const int MAX_QUEUE_SIZE = 256;

struct QueueItem {
    char   *data;
    size_t  size;
    bool    is_poison;
};

struct BoundedQueue {
    QueueItem       *items;
    int              capacity;
    int              head;
    int              tail;
    int              count;
    pthread_mutex_t  mutex;
    sem_t            sem_empty;
    sem_t            sem_full;
};

static BoundedQueue g_queue;

static SensorEntry g_agg[MAX_CATEGORIES];
static int         g_agg_count = 0;
static long        g_total_records = 0;
static pthread_mutex_t g_agg_mutex;

static volatile sig_atomic_t g_stop    = 0;
static volatile sig_atomic_t g_sigusr1 = 0;
static std::string g_fifo_path;
static std::string g_shm_name;
static std::string g_sem_name;
static int g_num_threads = 4;
static int g_queue_size = 16;
static int g_reader_status = EXIT_SUCCESS_CODE;

static int my_atoi(const char *s) {
    int res = 0;
    while (*s >= '0' && *s <= '9') {
        res = res * 10 + (*s - '0');
        s++;
    }
    return res;
}

static void my_perror(const char *msg) {
    std::cerr << msg << ": " << strerror(errno) << std::endl;
}

static void sem_wait_retry(sem_t *sem) {
    while (sem_wait(sem) < 0 && errno == EINTR) {
    }
}

static void queue_push(BoundedQueue *q, QueueItem item) {
    sem_wait_retry(&q->sem_empty);
    pthread_mutex_lock(&q->mutex);
    q->items[q->tail] = item;
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;
    pthread_mutex_unlock(&q->mutex);
    sem_post(&q->sem_full);
}

static QueueItem queue_pop(BoundedQueue *q) {
    sem_wait_retry(&q->sem_full);
    pthread_mutex_lock(&q->mutex);
    QueueItem item = q->items[q->head];
    q->head = (q->head + 1) % q->capacity;
    q->count--;
    pthread_mutex_unlock(&q->mutex);
    sem_post(&q->sem_empty);
    return item;
}

static bool is_space_char(char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

static char *trim_token(char *s) {
    while (*s && is_space_char(*s)) s++;
    char *end = s + strlen(s);
    while (end > s && is_space_char(*(end - 1))) {
        end--;
        *end = '\0';
    }
    return s;
}

static bool parse_double_token(const char *s, double *out) {
    if (!s || *s == '\0') return false;
    errno = 0;
    char *end = NULL;
    double value = strtod(s, &end);
    if (end == s) return false;
    while (*end && is_space_char(*end)) end++;
    if (*end != '\0') return false;
    if (errno == ERANGE) return false;
    *out = value;
    return true;
}

static SensorEntry *get_or_create_sensor(const char *sid) {
    for (int i = 0; i < g_agg_count; i++) {
        if (strcmp(g_agg[i].sensor_id, sid) == 0)
            return &g_agg[i];
    }
    if (g_agg_count >= MAX_CATEGORIES) return NULL;
    SensorEntry *e = &g_agg[g_agg_count++];
    memset(e, 0, sizeof(*e));
    strncpy(e->sensor_id, sid, CATEGORY_LEN - 1);
    e->sensor_id[CATEGORY_LEN - 1] = '\0';
    return e;
}

static void update_sensor(const char *sensor_id, double total,
                          long reading_count, long anomaly_count) {
    pthread_mutex_lock(&g_agg_mutex);
    SensorEntry *e = get_or_create_sensor(sensor_id);
    if (e) {
        e->total_reading += total;
        e->record_count += reading_count;
        e->anomaly_count += anomaly_count;
    }
    g_total_records++;
    pthread_mutex_unlock(&g_agg_mutex);
}

static void process_csv_line(char *line) {
    if (!line) return;
    line = trim_token(line);
    if (*line == '\0' || *line == '#') return;

    char *fields[128];
    int field_count = 0;
    char *saveptr_col = NULL;
    char *token = strtok_r(line, ",", &saveptr_col);
    while (token && field_count < 128) {
        fields[field_count++] = trim_token(token);
        token = strtok_r(NULL, ",", &saveptr_col);
    }

    if (field_count < 2) return;
    if (strcmp(fields[0], "sensor_id") == 0 || strcmp(fields[0], "SensorID") == 0) return;

    double dummy = 0.0;
    int first_reading_col = 1;
    if (field_count >= 4 &&
        parse_double_token(fields[1], &dummy) &&
        !parse_double_token(fields[2], &dummy)) {
        first_reading_col = 3;
    }

    double total = 0.0;
    long reading_count = 0;
    long anomaly_count = 0;
    for (int i = first_reading_col; i < field_count; i++) {
        double reading = 0.0;
        if (!parse_double_token(fields[i], &reading)) continue;
        total += reading;
        reading_count++;
        if (reading > 90.0 || reading < 10.0) {
            anomaly_count++;
        }
    }

    if (reading_count > 0) {
        update_sensor(fields[0], total, reading_count, anomaly_count);
    }
}

static void process_chunk(const char *data, size_t size) {
    char *buf = new char[size + 1];
    if (!buf) return;
    memcpy(buf, data, size);
    buf[size] = '\0';

    char *saveptr_line = NULL;
    char *line = strtok_r(buf, "\n", &saveptr_line);
    while (line) {
        process_csv_line(line);
        line = strtok_r(NULL, "\n", &saveptr_line);
    }
    delete[] buf;
}

static void print_stats_if_requested() {
    if (!g_sigusr1) return;

    pthread_mutex_lock(&g_agg_mutex);
    int sensors = g_agg_count;
    long records = g_total_records;
    pthread_mutex_unlock(&g_agg_mutex);

    std::cerr << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] SIGUSR1: sensors=" << sensors
              << " records=" << records << std::endl;
    g_sigusr1 = 0;
}

static void *worker_thread(void *arg) {
    (void)arg;
    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] Worker thread started (tid="
              << (unsigned long)pthread_self() << ")" << std::endl;

    while (true) {
        QueueItem item = queue_pop(&g_queue);
        if (item.is_poison) {
            std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
                      << "] Worker thread got poison pill, exiting." << std::endl;
            break;
        }
        process_chunk(item.data, item.size);
        delete[] item.data;
    }

    pthread_exit(NULL);
}

static bool read_exact(int fd, void *buffer, size_t bytes, bool *eof) {
    char *p = static_cast<char *>(buffer);
    size_t total = 0;
    *eof = false;
    while (total < bytes) {
        ssize_t n = read(fd, p + total, bytes - total);
        if (n == 0) {
            *eof = (total == 0);
            return false;
        }
        if (n < 0) {
            if (errno == EINTR && !g_stop) continue;
            return false;
        }
        total += static_cast<size_t>(n);
    }
    return true;
}

static void send_poison_pills() {
    for (int i = 0; i < g_num_threads; i++) {
        QueueItem poison;
        poison.data = NULL;
        poison.size = 0;
        poison.is_poison = true;
        queue_push(&g_queue, poison);
    }
}

static void *reader_thread(void *arg) {
    (void)arg;

    int fifo_fd = open(g_fifo_path.c_str(), O_RDONLY);
    if (fifo_fd < 0) {
        my_perror("open fifo read");
        g_reader_status = EXIT_IO_ERROR;
        send_poison_pills();
        pthread_exit(NULL);
    }

    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] Dedicated FIFO reader thread opened fd=" << fifo_fd << std::endl;

    while (!g_stop) {
        print_stats_if_requested();

        ChunkHeader hdr;
        bool eof = false;
        if (!read_exact(fifo_fd, &hdr, sizeof(hdr), &eof)) {
            if (!eof && !g_stop) {
                my_perror("read hdr");
                g_reader_status = EXIT_IO_ERROR;
            }
            break;
        }

        if (hdr.chunk_id == EOF_CHUNK_ID) {
            std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
                      << "] Received EOF chunk from ingester." << std::endl;
            break;
        }

        char *payload = new char[static_cast<size_t>(hdr.byte_count) + 1];
        if (!payload) {
            std::cerr << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
                      << "] Out of memory while reading chunk." << std::endl;
            g_reader_status = EXIT_IO_ERROR;
            break;
        }

        if (!read_exact(fifo_fd, payload, hdr.byte_count, &eof)) {
            delete[] payload;
            if (!g_stop) {
                my_perror("read payload");
                g_reader_status = EXIT_IO_ERROR;
            }
            break;
        }
        payload[hdr.byte_count] = '\0';

        QueueItem item;
        item.data = payload;
        item.size = hdr.byte_count;
        item.is_poison = false;
        queue_push(&g_queue, item);
    }

    close(fifo_fd);
    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] Reader thread done. Sending poison pills to "
              << g_num_threads << " workers." << std::endl;
    send_poison_pills();
    pthread_exit(NULL);
}

static void sig_handler(int signo) {
    if (signo == SIGTERM || signo == SIGINT) g_stop = 1;
    else if (signo == SIGUSR1) g_sigusr1 = 1;
}

static void usage(const char *prog) {
    std::cerr << "Usage: " << prog
              << " -f <fifo> -m <shm_name> -S <sem_name> -n <threads> -q <qsize>"
              << std::endl;
    _exit(EXIT_BAD_ARGS);
}

int main(int argc, char *argv[]) {
    g_fifo_path = FIFO_PATH_DEFAULT;
    g_shm_name = SHM_NAME_DEFAULT;
    g_sem_name = SEM_NAME_DEFAULT;

    int opt;
    while ((opt = getopt(argc, argv, "f:m:S:n:q:")) != -1) {
        switch (opt) {
            case 'f': g_fifo_path = optarg; break;
            case 'm': g_shm_name = optarg; break;
            case 'S': g_sem_name = optarg; break;
            case 'n': g_num_threads = my_atoi(optarg); break;
            case 'q': g_queue_size = my_atoi(optarg); break;
            default:  usage(argv[0]);
        }
    }

    if (g_num_threads < 1 || g_queue_size < 1 ||
        g_shm_name.empty() || g_sem_name.empty()) {
        usage(argv[0]);
    }

    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] Processor started. fifo=" << g_fifo_path
              << " shm=" << g_shm_name
              << " sem=" << g_sem_name
              << " threads=" << g_num_threads
              << " queue=" << g_queue_size << std::endl;

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sig_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGUSR1, &sa, NULL);

    if (g_queue_size > MAX_QUEUE_SIZE) g_queue_size = MAX_QUEUE_SIZE;
    g_queue.items = new QueueItem[static_cast<size_t>(g_queue_size)];
    g_queue.capacity = g_queue_size;
    g_queue.head = 0;
    g_queue.tail = 0;
    g_queue.count = 0;
    pthread_mutex_init(&g_queue.mutex, NULL);
    sem_init(&g_queue.sem_empty, 0, static_cast<unsigned>(g_queue_size));
    sem_init(&g_queue.sem_full,  0, 0);
    pthread_mutex_init(&g_agg_mutex, NULL);

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    pthread_attr_setstacksize(&attr, 2 * 1024 * 1024);

    pthread_t *workers = new pthread_t[static_cast<size_t>(g_num_threads)];
    for (int i = 0; i < g_num_threads; i++) {
        if (pthread_create(&workers[i], &attr, worker_thread, NULL) != 0) {
            my_perror("pthread_create worker");
            _exit(EXIT_IPC_FAIL);
        }
        std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
                  << "] Spawned worker thread[" << i << "]" << std::endl;
    }

    pthread_t reader;
    if (pthread_create(&reader, &attr, reader_thread, NULL) != 0) {
        my_perror("pthread_create reader");
        _exit(EXIT_IPC_FAIL);
    }
    pthread_attr_destroy(&attr);

    pthread_join(reader, NULL);
    for (int i = 0; i < g_num_threads; i++) {
        pthread_join(workers[i], NULL);
        std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
                  << "] Joined worker thread[" << i << "]" << std::endl;
    }

    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] All workers joined. Aggregation complete: categories="
              << g_agg_count << " records=" << g_total_records << std::endl;

    if (g_stop) _exit(EXIT_SIGTERM);
    if (g_reader_status != EXIT_SUCCESS_CODE) _exit(g_reader_status);

    int shm_fd = shm_open(g_shm_name.c_str(), O_RDWR, 0666);
    if (shm_fd < 0) {
        my_perror("shm_open in processor");
        _exit(EXIT_IPC_FAIL);
    }

    SharedData *shm = static_cast<SharedData *>(mmap(NULL, sizeof(SharedData),
                                                     PROT_READ | PROT_WRITE,
                                                     MAP_SHARED, shm_fd, 0));
    if (shm == MAP_FAILED) {
        my_perror("mmap in processor");
        close(shm_fd);
        _exit(EXIT_IPC_FAIL);
    }

    sem_t *ready_sem = sem_open(g_sem_name.c_str(), 0);
    if (ready_sem == SEM_FAILED) {
        my_perror("sem_open in processor");
        munmap(shm, sizeof(SharedData));
        close(shm_fd);
        _exit(EXIT_IPC_FAIL);
    }

    pthread_mutex_lock(&g_agg_mutex);
    shm->num_categories = g_agg_count;
    shm->total_records  = g_total_records;
    memcpy(shm->categories, g_agg, sizeof(SensorEntry) * static_cast<size_t>(g_agg_count));
    pthread_mutex_unlock(&g_agg_mutex);

    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] Results written to POSIX shared memory. Posting named semaphore..."
              << std::endl;

    sem_post(ready_sem);
    sem_close(ready_sem);
    munmap(shm, sizeof(SharedData));
    close(shm_fd);

    pthread_mutex_destroy(&g_agg_mutex);
    pthread_mutex_destroy(&g_queue.mutex);
    sem_destroy(&g_queue.sem_empty);
    sem_destroy(&g_queue.sem_full);

    delete[] g_queue.items;
    delete[] workers;

    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] Processor exiting cleanly." << std::endl;
    return EXIT_SUCCESS_CODE;
}
