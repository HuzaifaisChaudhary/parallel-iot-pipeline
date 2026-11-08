#!/usr/bin/env bash
# run.sh - Master orchestration script for OS Final Project Pipeline

set -euo pipefail

INPUT_DIR="./data"
OUTPUT_DIR="./output"
NUM_THREADS=4
QUEUE_SIZE=16
FIFO_PATH="/tmp/os_proj_fifo"
SHM_NAME="/os_proj_shm"
SEM_NAME="/os_proj_ready_sem"
DO_CLEAN=0
DISPATCHER_PID=0
PID_FILE=".dispatcher.pid"
START_TIME=0

print_usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Options:
  -i <input_dir>   Directory containing CSV files  (default: ./data)
  -o <output_dir>  Directory for output files       (default: ./output)
  -n <threads>     Number of worker threads         (default: 4)
  -q <queue_size>  Bounded queue size               (default: 16)
  -m <shm_name>    POSIX shared memory name         (default: /os_proj_shm)
  -S <sem_name>    POSIX named semaphore name       (default: /os_proj_ready_sem)
  -c               Clean build before running
  -h               Show this help message

Example:
  $0 -i ./data -o ./output -n 4 -q 16
EOF
}

check_prerequisites() {
    local errors=0

    if ! command -v gcc >/dev/null 2>&1; then
        echo "[ERROR] gcc is not installed." >&2
        errors=$(( errors + 1 ))
    fi

    if ! command -v g++ >/dev/null 2>&1; then
        echo "[ERROR] g++ is not installed." >&2
        errors=$(( errors + 1 ))
    fi

    if ! command -v make >/dev/null 2>&1; then
        echo "[ERROR] make is not installed." >&2
        errors=$(( errors + 1 ))
    fi

    if [ ! -d "$INPUT_DIR" ]; then
        echo "[ERROR] Input directory '$INPUT_DIR' does not exist." >&2
        errors=$(( errors + 1 ))
    else
        local csv_count
        csv_count=$(find "$INPUT_DIR" -maxdepth 1 -name "*.csv" | wc -l)
        if [ "$csv_count" -eq 0 ]; then
            echo "[ERROR] No CSV files found in '$INPUT_DIR'." >&2
            errors=$(( errors + 1 ))
        else
            echo "[INFO] Found $csv_count CSV file(s) in '$INPUT_DIR'."
        fi
    fi

    if [ "$errors" -gt 0 ]; then
        exit 1
    fi
}

build_project() {
    echo "[INFO] Building project..."

    if [ "$DO_CLEAN" -eq 1 ]; then
        echo "[INFO] Cleaning previous build..."
        make clean
    fi

    if ! make; then
        echo "[ERROR] Build failed. Please fix compilation errors." >&2
        exit 1
    fi

    echo "[INFO] Build successful."
}

cleanup() {
    echo ""
    echo "[INFO] Cleanup triggered."

    if [ "$DISPATCHER_PID" -gt 0 ] && kill -0 "$DISPATCHER_PID" 2>/dev/null; then
        echo "[INFO] Sending SIGTERM to dispatcher (PID=$DISPATCHER_PID)..."
        kill -TERM "$DISPATCHER_PID" 2>/dev/null || true

        local waited=0
        while kill -0 "$DISPATCHER_PID" 2>/dev/null && [ "$waited" -lt 5 ]; do
            sleep 1
            waited=$(( waited + 1 ))
        done

        if kill -0 "$DISPATCHER_PID" 2>/dev/null; then
            echo "[WARN] Dispatcher did not exit cleanly; sending SIGKILL."
            kill -KILL "$DISPATCHER_PID" 2>/dev/null || true
        fi
    fi

    if [ -f "$PID_FILE" ]; then
        rm -f "$PID_FILE"
    fi

    if [ -e "$FIFO_PATH" ]; then
        rm -f "$FIFO_PATH"
        echo "[INFO] Removed stale FIFO: $FIFO_PATH"
    fi

    local shm_file="/dev/shm/${SHM_NAME#/}"
    local sem_file="/dev/shm/sem.${SEM_NAME#/}"
    if [ -e "$shm_file" ]; then
        rm -f "$shm_file"
        echo "[INFO] Removed stale POSIX shared memory: $SHM_NAME"
    fi
    if [ -e "$sem_file" ]; then
        rm -f "$sem_file"
        echo "[INFO] Removed stale POSIX semaphore: $SEM_NAME"
    fi

    echo "[INFO] Cleanup complete."
}

print_summary() {
    local end_time
    end_time=$(date +%s)
    local runtime=$(( end_time - START_TIME ))

    echo ""
    echo "=============================================="
    echo "       PIPELINE EXECUTION SUMMARY"
    echo "=============================================="
    printf "Total runtime    : %d second(s)\n" "$runtime"

    local csv_file="${OUTPUT_DIR}/report.csv"
    if [ -f "$csv_file" ]; then
        local total_records
        total_records=$(grep "^TOTAL_ROWS" "$csv_file" | cut -d',' -f3 || echo "N/A")
        printf "Rows processed   : %s\n" "$total_records"
        printf "Report location  : %s\n" "$OUTPUT_DIR"
    else
        echo "report.csv not found"
    fi
    echo "Final exit status: $DISPATCHER_EXIT"
    echo "=============================================="
}

while getopts ":i:o:n:q:m:S:ch" opt; do
    case "$opt" in
        i) INPUT_DIR="$OPTARG"   ;;
        o) OUTPUT_DIR="$OPTARG"  ;;
        n) NUM_THREADS="$OPTARG" ;;
        q) QUEUE_SIZE="$OPTARG"  ;;
        m) SHM_NAME="$OPTARG"    ;;
        S) SEM_NAME="$OPTARG"    ;;
        c) DO_CLEAN=1            ;;
        h) print_usage; exit 0   ;;
        :) echo "[ERROR] Option -$OPTARG requires an argument." >&2; exit 1 ;;
        *) echo "[ERROR] Unknown option: -$OPTARG" >&2; print_usage; exit 1 ;;
    esac
done

if [ $(( NUM_THREADS < 1 )) -eq 1 ] || [ $(( NUM_THREADS > 64 )) -eq 1 ]; then
    echo "[ERROR] Number of threads must be between 1 and 64." >&2
    exit 1
fi

if [ $(( QUEUE_SIZE < 1 )) -eq 1 ] || [ $(( QUEUE_SIZE > 256 )) -eq 1 ]; then
    echo "[ERROR] Queue size must be between 1 and 256." >&2
    exit 1
fi

if [[ "$SHM_NAME" != /* || "$SEM_NAME" != /* ]]; then
    echo "[ERROR] POSIX shared memory and semaphore names must start with '/'." >&2
    exit 1
fi

trap cleanup EXIT INT TERM

echo "[INFO] OS Final Project - Parallel CSV Pipeline"
echo "[INFO] Input  : $INPUT_DIR"
echo "[INFO] Output : $OUTPUT_DIR"
echo "[INFO] Threads: $NUM_THREADS  Queue: $QUEUE_SIZE"
echo "[INFO] FIFO   : $FIFO_PATH"
echo "[INFO] POSIX shared memory: $SHM_NAME"
echo "[INFO] POSIX semaphore    : $SEM_NAME"

mkdir -p "$OUTPUT_DIR" "$OUTPUT_DIR/logs"

check_prerequisites
build_project

START_TIME=$(date +%s)

echo "[INFO] Launching dispatcher..."
./dispatcher \
    -i "$INPUT_DIR" \
    -o "$OUTPUT_DIR" \
    -n "$NUM_THREADS" \
    -q "$QUEUE_SIZE" \
    -f "$FIFO_PATH" \
    -m "$SHM_NAME" \
    -S "$SEM_NAME" &

DISPATCHER_PID=$!
echo "$DISPATCHER_PID" > "$PID_FILE"
echo "[INFO] Dispatcher launched (PID=$DISPATCHER_PID). PID saved to $PID_FILE"

echo "[INFO] Waiting for pipeline to complete..."
PREV_LOG_SIZE=0
while kill -0 "$DISPATCHER_PID" 2>/dev/null; do
    sleep 2

    local_log="$OUTPUT_DIR/logs/processor.log"
    if [ -f "$local_log" ]; then
        current_size=$(wc -c < "$local_log")
        if [ "$current_size" -gt "$PREV_LOG_SIZE" ]; then
            echo "[INFO] Pipeline running... (processor log: ${current_size} bytes)"
            PREV_LOG_SIZE=$current_size
        fi
    fi
done

set +e
wait "$DISPATCHER_PID" 2>/dev/null
DISPATCHER_EXIT=$?
set -e
DISPATCHER_PID=0

echo "[INFO] Dispatcher exited with status: $DISPATCHER_EXIT"
print_summary
exit "$DISPATCHER_EXIT"
