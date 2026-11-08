#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <csignal>
#include <cerrno>
#include <dirent.h>
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

const unsigned int CHUNK_SIZE   = 1024 * 64;
const unsigned int EOF_CHUNK_ID = 0xFFFFFFFF;

class ChunkHeader {
public:
    unsigned int chunk_id;
    unsigned int source_file_id;
    unsigned int byte_count;
};

static volatile sig_atomic_t g_stop    = 0;
static volatile sig_atomic_t g_sigusr1 = 0;

static volatile long g_files_done  = 0;
static volatile long g_chunks_sent = 0;
static volatile long g_bytes_sent  = 0;

static std::string g_input_dir;
static std::string g_fifo_path;

static void sig_handler(int signo) {
    if (signo == SIGTERM || signo == SIGINT) {
        g_stop = 1;
    } else if (signo == SIGUSR1) {
        g_sigusr1 = 1;
    }
}

static void my_perror(const char *msg) {
    std::cerr << msg << ": " << strerror(errno) << std::endl;
}

static void print_stats() {
    std::cerr << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] SIGUSR1 stats: files=" << g_files_done
              << " chunks=" << g_chunks_sent
              << " bytes=" << g_bytes_sent << std::endl;
}

static void usage(const char *prog) {
    std::cerr << "Usage: " << prog << " -i <input_dir> -f <fifo_path>" << std::endl;
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

static bool send_chunk(int fifo_fd, unsigned int *chunk_id,
                       unsigned int source_file_id, const std::string &chunk) {
    if (chunk.empty()) return true;

    ChunkHeader hdr;
    hdr.chunk_id = (*chunk_id)++;
    hdr.source_file_id = source_file_id;
    hdr.byte_count = static_cast<unsigned int>(chunk.size());

    if (!write_all(fifo_fd, &hdr, sizeof(hdr))) return false;
    if (!write_all(fifo_fd, chunk.data(), chunk.size())) return false;

    g_chunks_sent++;
    g_bytes_sent += static_cast<long>(chunk.size());
    return true;
}

int main(int argc, char *argv[]) {
    g_fifo_path = FIFO_PATH_DEFAULT;

    int opt;
    while ((opt = getopt(argc, argv, "i:f:")) != -1) {
        switch (opt) {
            case 'i': g_input_dir = optarg; break;
            case 'f': g_fifo_path = optarg; break;
            default:  usage(argv[0]);
        }
    }
    if (g_input_dir.empty()) usage(argv[0]);

    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] Ingester started. input_dir=" << g_input_dir
              << " fifo=" << g_fifo_path << std::endl;

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sig_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGUSR1, &sa, NULL);

    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] Opening FIFO for writing." << std::endl;
    int fifo_fd = open(g_fifo_path.c_str(), O_WRONLY);
    if (fifo_fd < 0) {
        my_perror("open fifo");
        _exit(EXIT_IO_ERROR);
    }

    DIR *dir = opendir(g_input_dir.c_str());
    if (!dir) {
        my_perror("opendir");
        close(fifo_fd);
        _exit(EXIT_IO_ERROR);
    }

    std::string csv_files[1024];
    int csv_count = 0;
    dirent *ent;
    while ((ent = readdir(dir)) != NULL) {
        std::string name = ent->d_name;
        if (name.size() > 4 && name.substr(name.size() - 4) == ".csv") {
            if (csv_count < 1024) {
                csv_files[csv_count++] = g_input_dir + "/" + name;
            }
        }
    }
    closedir(dir);

    if (csv_count == 0) {
        std::cerr << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
                  << "] No CSV files found in " << g_input_dir << std::endl;
        close(fifo_fd);
        _exit(EXIT_IO_ERROR);
    }
    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] Found " << csv_count << " CSV file(s) to process." << std::endl;

    unsigned int chunk_id = 0;
    char read_buf[4096];

    for (int fi = 0; fi < csv_count && !g_stop; fi++) {
        if (g_sigusr1) {
            print_stats();
            g_sigusr1 = 0;
        }

        int csv_fd = open(csv_files[fi].c_str(), O_RDONLY);
        if (csv_fd < 0) {
            std::cerr << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
                      << "] Cannot open " << csv_files[fi]
                      << ": " << strerror(errno) << std::endl;
            continue;
        }
        std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
                  << "] Processing file[" << fi << "]: " << csv_files[fi] << std::endl;

        std::string chunk;
        ssize_t bytes_read = 0;
        while (!g_stop && (bytes_read = read(csv_fd, read_buf, sizeof(read_buf))) > 0) {
            for (ssize_t i = 0; i < bytes_read; i++) {
                chunk += read_buf[i];
                if (read_buf[i] == '\n' && chunk.size() >= CHUNK_SIZE) {
                    if (!send_chunk(fifo_fd, &chunk_id, static_cast<unsigned int>(fi), chunk)) {
                        my_perror("write chunk");
                        g_stop = 1;
                        break;
                    }
                    chunk.clear();
                }
            }
            if (g_sigusr1) {
                print_stats();
                g_sigusr1 = 0;
            }
        }
        if (bytes_read < 0 && errno != EINTR) {
            my_perror("read csv");
        }
        if (!send_chunk(fifo_fd, &chunk_id, static_cast<unsigned int>(fi), chunk)) {
            my_perror("write final chunk");
            g_stop = 1;
        }

        close(csv_fd);
        g_files_done++;
        std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
                  << "] Done with file[" << fi
                  << "]. chunks_so_far=" << g_chunks_sent << std::endl;
    }

    ChunkHeader eof_hdr;
    eof_hdr.chunk_id = EOF_CHUNK_ID;
    eof_hdr.source_file_id = 0;
    eof_hdr.byte_count = 0;
    if (!write_all(fifo_fd, &eof_hdr, sizeof(eof_hdr))) {
        my_perror("write EOF chunk");
    } else {
        std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
                  << "] EOF chunk written to FIFO." << std::endl;
    }

    close(fifo_fd);

    std::cout << "[PID=" << (int)getpid() << " PPID=" << (int)getppid()
              << "] Ingester done. files=" << g_files_done
              << " chunks=" << g_chunks_sent
              << " bytes=" << g_bytes_sent << std::endl;

    if (g_stop) _exit(EXIT_SIGTERM);
    return EXIT_SUCCESS_CODE;
}
