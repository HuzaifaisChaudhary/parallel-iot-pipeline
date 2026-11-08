#include <cstring>
#include <cstdio>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <csignal>
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

const char *SHM_NAME_DEFAULT = "/os_proj_shm";
const char *SEM_NAME_DEFAULT = "/os_proj_ready_sem";

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

static volatile sig_atomic_t g_stop = 0;

static void sig_handler(int signo) {
    if (signo == SIGTERM || signo == SIGINT) g_stop = 1;
}

static void my_perror(const char *msg) {
    std::cerr << msg << ": " << strerror(errno) << std::endl;
}

static void my_sort(SensorEntry *arr, int n) {
    for (int i = 0; i < n - 1; i++) {
        for (int j = 0; j < n - i - 1; j++) {
            double avg_j = arr[j].record_count > 0 ? arr[j].total_reading / arr[j].record_count : 0.0;
            double avg_next = arr[j + 1].record_count > 0 ? arr[j + 1].total_reading / arr[j + 1].record_count : 0.0;
            if (avg_j < avg_next) {
                SensorEntry tmp = arr[j];
                arr[j] = arr[j + 1];
                arr[j + 1] = tmp;
            }
        }
    }
}

static void usage(const char *prog) {
    std::cerr << "Usage: " << prog
              << " -m <shm_name> -S <sem_name> -o <output_dir>"
              << std::endl;
    _exit(EXIT_BAD_ARGS);
}

static bool write_all(int fd, const void *buffer, size_t bytes) {
    const char *p = static_cast<const char *>(buffer);
    size_t written = 0;
    while (written < bytes) {
        ssize_t n = write(fd, p + written, bytes - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        written += static_cast<size_t>(n);
    }
    return true;
}

int main(int argc, char *argv[]) {
    std::string shm_name = SHM_NAME_DEFAULT;
    std::string sem_name = SEM_NAME_DEFAULT;
    std::string output_dir;

    int opt;
    while ((opt = getopt(argc, argv, "m:S:o:")) != -1) {
        switch (opt) {
            case 'm': shm_name = optarg; break;
            case 'S': sem_name = optarg; break;
            case 'o': output_dir = optarg; break;
            default:  usage(argv[0]);
        }
    }
    if (output_dir.empty()) usage(argv[0]);

    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] Reporter started. shm=" << shm_name
              << " sem=" << sem_name
              << " output_dir=" << output_dir << std::endl;

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sig_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);

    int shm_fd = shm_open(shm_name.c_str(), O_RDONLY, 0666);
    if (shm_fd < 0) {
        my_perror("shm_open reporter");
        _exit(EXIT_IPC_FAIL);
    }

    SharedData *shm = static_cast<SharedData *>(mmap(NULL, sizeof(SharedData),
                                                     PROT_READ, MAP_SHARED,
                                                     shm_fd, 0));
    if (shm == MAP_FAILED) {
        my_perror("mmap reporter");
        close(shm_fd);
        _exit(EXIT_IPC_FAIL);
    }

    sem_t *ready_sem = sem_open(sem_name.c_str(), 0);
    if (ready_sem == SEM_FAILED) {
        my_perror("sem_open reporter");
        munmap(shm, sizeof(SharedData));
        close(shm_fd);
        _exit(EXIT_IPC_FAIL);
    }

    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] Attached to POSIX shared memory. Waiting on named semaphore..."
              << std::endl;

    while (sem_wait(ready_sem) < 0) {
        if (errno == EINTR && !g_stop) continue;
        break;
    }

    if (g_stop) {
        sem_close(ready_sem);
        munmap(shm, sizeof(SharedData));
        close(shm_fd);
        _exit(EXIT_SIGTERM);
    }

    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] Semaphore acquired. Reading aggregation results: categories="
              << shm->num_categories << " records=" << shm->total_records
              << std::endl;

    int num_sensors = shm->num_categories;
    long total_records = shm->total_records;
    SensorEntry *sensors = new SensorEntry[static_cast<size_t>(num_sensors)];
    memcpy(sensors, shm->categories, sizeof(SensorEntry) * static_cast<size_t>(num_sensors));

    sem_close(ready_sem);
    munmap(shm, sizeof(SharedData));
    close(shm_fd);

    std::string report_txt_path = output_dir + "/report.txt";

    /*
     * Required dup()/dup2() save-restore pattern:
     * 1. dup(STDOUT_FILENO) saves the current stdout descriptor.
     * 2. dup2(report_fd, STDOUT_FILENO) redirects stdout to report.txt.
     * 3. The human-readable report is written with normal printf() calls.
     * 4. fflush(stdout) pushes the report to disk before stdout is restored.
     * 5. dup2(saved_stdout, STDOUT_FILENO) restores the original stdout stream.
     */
    int saved_stdout = dup(STDOUT_FILENO);
    if (saved_stdout < 0) {
        my_perror("dup stdout");
        delete[] sensors;
        _exit(EXIT_IO_ERROR);
    }

    int report_fd = open(report_txt_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (report_fd < 0) {
        my_perror("open report.txt");
        close(saved_stdout);
        delete[] sensors;
        _exit(EXIT_IO_ERROR);
    }

    if (dup2(report_fd, STDOUT_FILENO) < 0) {
        my_perror("dup2 to report.txt");
        close(report_fd);
        close(saved_stdout);
        delete[] sensors;
        _exit(EXIT_IO_ERROR);
    }
    close(report_fd);

    printf("=============================================================\n");
    printf("  IOT SENSOR PIPELINE REPORT\n");
    printf("  Generated by OS Final Project - IoT Variant\n");
    printf("=============================================================\n\n");
    printf("Total CSV rows processed : %ld\n", total_records);
    printf("Unique sensors found     : %d\n\n", num_sensors);
    printf("%-30s  %11s  %13s  %9s\n", "Sensor ID", "Moving Avg", "Reading Count", "Anomalies");
    printf("------------------------------  -----------  -------------  ---------\n");

    my_sort(sensors, num_sensors);
    for (int i = 0; i < num_sensors; i++) {
        const SensorEntry &s = sensors[i];
        double avg = s.record_count > 0 ? s.total_reading / s.record_count : 0.0;
        printf("%-30s  %11.2f  %13ld  %9ld\n",
               s.sensor_id, avg, s.record_count, s.anomaly_count);
    }
    printf("\n=============================================================\n");
    fflush(stdout);

    if (dup2(saved_stdout, STDOUT_FILENO) < 0) {
        my_perror("dup2 restore stdout");
        close(saved_stdout);
        delete[] sensors;
        _exit(EXIT_IO_ERROR);
    }
    close(saved_stdout);

    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] report.txt written via dup/dup2 save-restore pattern."
              << std::endl;

    std::string report_csv_path = output_dir + "/report.csv";
    int csv_fd = open(report_csv_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (csv_fd < 0) {
        my_perror("open report.csv");
        delete[] sensors;
        _exit(EXIT_IO_ERROR);
    }

    const char *csv_hdr = "sensor_id,moving_average,reading_count,anomaly_count\n";
    if (!write_all(csv_fd, csv_hdr, strlen(csv_hdr))) {
        my_perror("write report.csv header");
        close(csv_fd);
        delete[] sensors;
        _exit(EXIT_IO_ERROR);
    }

    char line[256];
    for (int i = 0; i < num_sensors; i++) {
        const SensorEntry &s = sensors[i];
        double avg = s.record_count > 0 ? s.total_reading / s.record_count : 0.0;
        int len = snprintf(line, sizeof(line), "%s,%.2f,%ld,%ld\n",
                           s.sensor_id, avg, s.record_count, s.anomaly_count);
        if (len < 0 || static_cast<size_t>(len) >= sizeof(line) ||
            !write_all(csv_fd, line, static_cast<size_t>(len))) {
            my_perror("write report.csv row");
            close(csv_fd);
            delete[] sensors;
            _exit(EXIT_IO_ERROR);
        }
    }

    int len = snprintf(line, sizeof(line), "TOTAL_ROWS,,%ld,\n", total_records);
    if (len < 0 || static_cast<size_t>(len) >= sizeof(line) ||
        !write_all(csv_fd, line, static_cast<size_t>(len))) {
        my_perror("write report.csv total");
        close(csv_fd);
        delete[] sensors;
        _exit(EXIT_IO_ERROR);
    }

    close(csv_fd);
    delete[] sensors;
    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] report.csv written." << std::endl;

    pid_t parent = getppid();
    if (parent > 1) {
        kill(parent, SIGUSR1);
        std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
                  << "] Sent SIGUSR1 to dispatcher (PID=" << (int)parent
                  << ") - report is ready." << std::endl;
    }

    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] Reporter exiting with status 0." << std::endl;
    return EXIT_SUCCESS_CODE;
}
