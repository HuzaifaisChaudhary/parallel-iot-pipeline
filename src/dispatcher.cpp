#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <csignal>
#include <semaphore.h>
#include <cerrno>
#include <sys/time.h>
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

static pid_t g_children[3] = {-1, -1, -1};
static const char *g_child_name[3] = {"ingester", "processor", "reporter"};
static int g_child_done[3] = {0, 0, 0};
static int g_child_status[3] = {0, 0, 0};
static double g_child_runtime[3] = {0.0, 0.0, 0.0};
static timeval g_child_start[3];

static volatile sig_atomic_t g_shutdown_requested = 0;
static volatile sig_atomic_t g_sigchld_seen = 0;
static volatile sig_atomic_t g_sigusr1_seen = 0;
static volatile sig_atomic_t g_stop_signal = 0;
static int g_child_failed = 0;

static char g_fifo_path[256];
static char g_shm_name[128];
static char g_sem_name[128];
static char g_logs_dir[700];

static int g_shm_fd = -1;
static SharedData *g_shm = NULL;
static sem_t *g_ready_sem = SEM_FAILED;

static int my_atoi(const char *s) {
    int res = 0;
    while (*s >= '0' && *s <= '9') {
        res = res * 10 + (*s - '0');
        s++;
    }
    return res;
}

static void copy_arg(char *dst, size_t dst_size, const char *src) {
    if (dst_size == 0) return;
    strncpy(dst, src, dst_size - 1);
    dst[dst_size - 1] = '\0';
}

static void my_perror(const char *msg) {
    std::cerr << msg << ": " << strerror(errno) << std::endl;
}

static void usage(const char *prog) {
    std::cerr << "Usage: " << prog
              << " -i <input_dir> -o <output_dir> -n <threads> -q <queue_size>"
              << " [-f <fifo_path>] [-m <shm_name>] [-S <sem_name>]"
              << std::endl;
    _exit(EXIT_BAD_ARGS);
}

static double elapsed_seconds(const timeval &start, const timeval &finish) {
    return (finish.tv_sec - start.tv_sec) +
           (finish.tv_usec - start.tv_usec) / 1000000.0;
}

static int find_child_index(pid_t pid) {
    for (int i = 0; i < 3; i++) {
        if (g_children[i] == pid) return i;
    }
    return -1;
}

static int all_children_done() {
    for (int i = 0; i < 3; i++) {
        if (!g_child_done[i]) return 0;
    }
    return 1;
}

static void remember_child_status(pid_t pid, int status) {
    int idx = find_child_index(pid);
    if (idx < 0 || g_child_done[idx]) return;

    timeval now;
    gettimeofday(&now, NULL);
    g_child_done[idx] = 1;
    g_child_status[idx] = status;
    g_child_runtime[idx] = elapsed_seconds(g_child_start[idx], now);

    if (!WIFEXITED(status) || WEXITSTATUS(status) != EXIT_SUCCESS_CODE) {
        g_child_failed = 1;
    }
}

static void reap_children_nonblocking() {
    while (true) {
        int status = 0;
        pid_t pid = waitpid(-1, &status, WNOHANG);
        if (pid > 0) {
            remember_child_status(pid, status);
            continue;
        }
        if (pid < 0 && errno == EINTR) continue;
        break;
    }
}

static void reap_remaining_children() {
    while (!all_children_done()) {
        int status = 0;
        pid_t pid = waitpid(-1, &status, 0);
        if (pid > 0) {
            remember_child_status(pid, status);
            continue;
        }
        if (pid < 0 && errno == EINTR) continue;
        break;
    }
}

static void stop_live_children() {
    for (int i = 0; i < 3; i++) {
        if (!g_child_done[i] && g_children[i] > 0) {
            kill(g_children[i], SIGTERM);
        }
    }
}

static void cleanup_ipc() {
    if (g_ready_sem != SEM_FAILED) {
        sem_close(g_ready_sem);
        g_ready_sem = SEM_FAILED;
    }
    if (g_sem_name[0] != '\0') {
        sem_unlink(g_sem_name);
    }
    if (g_shm && g_shm != MAP_FAILED) {
        munmap(g_shm, sizeof(SharedData));
        g_shm = NULL;
    }
    if (g_shm_fd >= 0) {
        close(g_shm_fd);
        g_shm_fd = -1;
    }
    if (g_shm_name[0] != '\0') {
        shm_unlink(g_shm_name);
    }
    if (g_fifo_path[0] != '\0') {
        unlink(g_fifo_path);
    }
}

static void sig_handler(int signo) {
    if (signo == SIGCHLD) {
        g_sigchld_seen = 1;
    } else if (signo == SIGINT || signo == SIGTERM) {
        g_shutdown_requested = 1;
        g_stop_signal = signo;
    } else if (signo == SIGUSR1) {
        g_sigusr1_seen = 1;
    }
}

static pid_t spawn_child(const char *exe, char *const argv[], const char *log_path) {
    pid_t pid = fork();
    if (pid < 0) {
        my_perror("fork");
        return -1;
    }
    if (pid == 0) {
        int log_fd = open(log_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (log_fd < 0) {
            my_perror("open log");
            _exit(EXIT_IO_ERROR);
        }

        if (dup2(log_fd, STDOUT_FILENO) < 0) {
            my_perror("dup2 stdout");
            close(log_fd);
            _exit(EXIT_IO_ERROR);
        }
        if (dup2(log_fd, STDERR_FILENO) < 0) {
            my_perror("dup2 stderr");
            close(log_fd);
            _exit(EXIT_IO_ERROR);
        }
        close(log_fd);

        execvp(exe, argv);
        my_perror("execvp");
        _exit(EXIT_BAD_ARGS);
    }
    return pid;
}

int main(int argc, char *argv[]) {
    char input_dir[512]  = "";
    char output_dir[512] = "";
    int  num_threads     = 4;
    int  queue_size      = 16;

    copy_arg(g_fifo_path, sizeof(g_fifo_path), FIFO_PATH_DEFAULT);
    copy_arg(g_shm_name, sizeof(g_shm_name), SHM_NAME_DEFAULT);
    copy_arg(g_sem_name, sizeof(g_sem_name), SEM_NAME_DEFAULT);

    int opt;
    while ((opt = getopt(argc, argv, "i:o:n:q:f:m:S:")) != -1) {
        switch (opt) {
            case 'i': copy_arg(input_dir, sizeof(input_dir), optarg); break;
            case 'o': copy_arg(output_dir, sizeof(output_dir), optarg); break;
            case 'n': num_threads = my_atoi(optarg); break;
            case 'q': queue_size  = my_atoi(optarg); break;
            case 'f': copy_arg(g_fifo_path, sizeof(g_fifo_path), optarg); break;
            case 'm': copy_arg(g_shm_name, sizeof(g_shm_name), optarg); break;
            case 'S': copy_arg(g_sem_name, sizeof(g_sem_name), optarg); break;
            default:  usage(argv[0]);
        }
    }
    if (input_dir[0] == '\0' || output_dir[0] == '\0') usage(argv[0]);
    if (num_threads < 1 || queue_size < 1) {
        std::cerr << "Threads and queue size must be >= 1" << std::endl;
        _exit(EXIT_BAD_ARGS);
    }
    if (g_shm_name[0] != '/' || g_sem_name[0] != '/') {
        std::cerr << "POSIX shared memory and semaphore names must start with '/'" << std::endl;
        _exit(EXIT_BAD_ARGS);
    }

    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] Dispatcher started. input=" << input_dir
              << " output=" << output_dir
              << " threads=" << num_threads
              << " queue=" << queue_size
              << " fifo=" << g_fifo_path
              << " shm=" << g_shm_name
              << " sem=" << g_sem_name << std::endl;

    copy_arg(g_logs_dir, sizeof(g_logs_dir), output_dir);
    strncat(g_logs_dir, "/logs", sizeof(g_logs_dir) - strlen(g_logs_dir) - 1);

    pid_t mk_pid = fork();
    if (mk_pid == 0) {
        char *mk_args[] = {const_cast<char*>("mkdir"), const_cast<char*>("-p"), g_logs_dir, NULL};
        execvp("mkdir", mk_args);
        _exit(EXIT_IO_ERROR);
    } else if (mk_pid > 0) {
        waitpid(mk_pid, NULL, 0);
    } else {
        my_perror("fork mkdir");
        _exit(EXIT_IPC_FAIL);
    }

    unlink(g_fifo_path);
    if (mkfifo(g_fifo_path, 0666) < 0) {
        my_perror("mkfifo");
        cleanup_ipc();
        _exit(EXIT_IPC_FAIL);
    }
    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] FIFO created: " << g_fifo_path << std::endl;

    shm_unlink(g_shm_name);
    g_shm_fd = shm_open(g_shm_name, O_CREAT | O_EXCL | O_RDWR, 0666);
    if (g_shm_fd < 0) {
        my_perror("shm_open");
        cleanup_ipc();
        _exit(EXIT_IPC_FAIL);
    }
    if (ftruncate(g_shm_fd, sizeof(SharedData)) < 0) {
        my_perror("ftruncate shm");
        cleanup_ipc();
        _exit(EXIT_IPC_FAIL);
    }
    g_shm = static_cast<SharedData *>(mmap(NULL, sizeof(SharedData),
                                           PROT_READ | PROT_WRITE,
                                           MAP_SHARED, g_shm_fd, 0));
    if (g_shm == MAP_FAILED) {
        my_perror("mmap shm");
        cleanup_ipc();
        _exit(EXIT_IPC_FAIL);
    }
    memset(g_shm, 0, sizeof(SharedData));

    sem_unlink(g_sem_name);
    g_ready_sem = sem_open(g_sem_name, O_CREAT | O_EXCL, 0666, 0);
    if (g_ready_sem == SEM_FAILED) {
        my_perror("sem_open");
        cleanup_ipc();
        _exit(EXIT_IPC_FAIL);
    }
    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] POSIX shared memory and named semaphore created." << std::endl;

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sig_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGCHLD, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGUSR1, &sa, NULL);

    std::string threads_str = std::to_string(num_threads);
    std::string queue_str = std::to_string(queue_size);

    char *ingester_args[] = {
        const_cast<char*>("./ingester"),
        const_cast<char*>("-i"), input_dir,
        const_cast<char*>("-f"), g_fifo_path,
        NULL
    };
    char *processor_args[] = {
        const_cast<char*>("./processor"),
        const_cast<char*>("-f"), g_fifo_path,
        const_cast<char*>("-m"), g_shm_name,
        const_cast<char*>("-S"), g_sem_name,
        const_cast<char*>("-n"), const_cast<char*>(threads_str.c_str()),
        const_cast<char*>("-q"), const_cast<char*>(queue_str.c_str()),
        NULL
    };
    char *reporter_args[] = {
        const_cast<char*>("./reporter"),
        const_cast<char*>("-m"), g_shm_name,
        const_cast<char*>("-S"), g_sem_name,
        const_cast<char*>("-o"), output_dir,
        NULL
    };

    std::string ingester_log = std::string(g_logs_dir) + "/ingester.log";
    std::string processor_log = std::string(g_logs_dir) + "/processor.log";
    std::string reporter_log = std::string(g_logs_dir) + "/reporter.log";

    char *const *child_args[3] = {ingester_args, processor_args, reporter_args};
    const char *child_exe[3] = {"./ingester", "./processor", "./reporter"};
    const char *child_logs[3] = {ingester_log.c_str(), processor_log.c_str(), reporter_log.c_str()};

    timeval pipeline_start;
    gettimeofday(&pipeline_start, NULL);

    for (int i = 0; i < 3; i++) {
        gettimeofday(&g_child_start[i], NULL);
        g_children[i] = spawn_child(child_exe[i], child_args[i], child_logs[i]);
        if (g_children[i] < 0) {
            std::cerr << "Failed to spawn " << g_child_name[i] << std::endl;
            stop_live_children();
            reap_remaining_children();
            cleanup_ipc();
            _exit(EXIT_IPC_FAIL);
        }
        std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
                  << "] Spawned " << g_child_name[i]
                  << " with PID=" << (int)g_children[i] << std::endl;
    }

    sigset_t empty_mask;
    sigemptyset(&empty_mask);
    while (!all_children_done() && !g_shutdown_requested && !g_child_failed) {
        if (g_sigchld_seen) {
            g_sigchld_seen = 0;
            reap_children_nonblocking();
        }
        if (g_sigusr1_seen) {
            g_sigusr1_seen = 0;
            std::cerr << "[DISPATCHER] SIGUSR1: report-ready/status signal received" << std::endl;
        }
        if (!all_children_done() && !g_shutdown_requested && !g_child_failed) {
            sigsuspend(&empty_mask);
        }
    }
    reap_children_nonblocking();

    if ((g_shutdown_requested || g_child_failed) && !all_children_done()) {
        stop_live_children();
    }
    reap_remaining_children();

    timeval pipeline_end;
    gettimeofday(&pipeline_end, NULL);
    double total_runtime = elapsed_seconds(pipeline_start, pipeline_end);

    for (int i = 0; i < 3; i++) {
        std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
                  << "] [SUMMARY] " << g_child_name[i]
                  << " PID=" << (int)g_children[i] << " ";
        if (g_child_done[i]) {
            if (WIFEXITED(g_child_status[i])) {
                std::cout << "exited with status " << WEXITSTATUS(g_child_status[i]);
            } else if (WIFSIGNALED(g_child_status[i])) {
                std::cout << "killed by signal " << WTERMSIG(g_child_status[i]);
            } else {
                std::cout << "finished with unknown status";
            }
            std::cout << " runtime=" << g_child_runtime[i] << " seconds";
        } else {
            std::cout << "was not reaped";
        }
        std::cout << std::endl;
    }

    cleanup_ipc();

    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] [SUMMARY] Total pipeline runtime: " << total_runtime
              << " seconds" << std::endl;
    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] Dispatcher exiting cleanly." << std::endl;

    if (g_shutdown_requested) {
        if (g_stop_signal == SIGINT) return EXIT_SIGINT;
        return EXIT_SIGTERM;
    }
    if (g_child_failed) return EXIT_CHILD_DIED;
    return EXIT_SUCCESS_CODE;
}
