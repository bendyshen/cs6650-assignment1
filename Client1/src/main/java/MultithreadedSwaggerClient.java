import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;

public class MultithreadedSwaggerClient {
    // Configuration
    private static final String IP_ADDRESS = "52.13.215.91";  // Replace with your EC2 IP address
    private static final String BASE_PATH = "http://" + IP_ADDRESS + ":8080/JavaServlet_war";
    private static final int TOTAL_REQUESTS = 200_000;       // Total number of requests to send
    private static final int INITIAL_THREADS = 32;           // Number of threads in Phase 1
    private static final int REQUESTS_PER_INITIAL = 1_000;   // Number of requests per thread in Phase 1
    private static final int MAX_RETRIES = 5;                // Maximum retry attempts for each request
    private static final int DYNAMIC_THREADS = 90;           // Number of threads in Phase 2 (dynamic phase)

    // Shared variables for tracking and communication between threads
    private static boolean generatingDone = false;           // Indicates when data generation is complete
    private static final AtomicInteger successCount = new AtomicInteger(0);  // Tracks the number of successful requests
    private static final AtomicInteger failureCount = new AtomicInteger(0);  // Tracks the number of failed requests
    private static final AtomicInteger remainingInitials = new AtomicInteger(INITIAL_THREADS); // Tracks the remaining threads in Phase 1
    private static final BlockingQueue<LiftRideEvent> eventQueue = new ArrayBlockingQueue<>(TOTAL_REQUESTS); // Queue to store generated events
    private static final AtomicBoolean phase2Started = new AtomicBoolean(false); // Tracks whether Phase 2 has started

    public static void main(String[] args) throws InterruptedException {
        // Step 1: Generate all LiftRide events and add them to the event queue
        generateLiftRides();
        generatingDone = true;
        System.out.println("[Generator] All events generated");

        long startTime = System.currentTimeMillis();  // Record the start time for performance measurement

        // Step 2: Launch Phase 1 with 32 threads, each processing 1000 requests
        ExecutorService phase1Executor = Executors.newFixedThreadPool(INITIAL_THREADS);
        for (int i = 0; i < INITIAL_THREADS; i++) {
            phase1Executor.execute(new Phase1Worker());
        }

        // Step 3: Launch Phase 2 dynamically once at least one Phase 1 thread completes
        ExecutorService phase2Executor = Executors.newFixedThreadPool(DYNAMIC_THREADS);
        new Thread(() -> {
            while (!phase2Started.get()) {
                if (remainingInitials.get() < INITIAL_THREADS) {  // Phase 2 starts once any Phase 1 thread completes
                    System.out.println("[Phase2] Starting dynamic threads...");
                    for (int i = 0; i < DYNAMIC_THREADS; i++) {
                        phase2Executor.execute(new DynamicWorker());
                    }
                    phase2Started.set(true);
                }
                LockSupport.parkNanos(1_000_000);  // Check every 1 ms
            }
        }).start();

        // Step 4: Wait for both phases to complete
        phase1Executor.shutdown();
        phase1Executor.awaitTermination(1, TimeUnit.HOURS);  // Ensure all Phase 1 threads finish
        phase2Executor.shutdown();
        phase2Executor.awaitTermination(1, TimeUnit.HOURS);  // Ensure all Phase 2 threads finish

        long endTime = System.currentTimeMillis();  // Record the end time
        printReport(startTime, endTime);  // Generate the performance report
    }

    // Worker class for Phase 1
    static class Phase1Worker implements Runnable {
        private final SkiersApi skiersApi = new SkiersApi(new ApiClient().setBasePath(BASE_PATH));
        private int processed = 0;  // Tracks the number of requests processed by this thread

        @Override
        public void run() {
            try {
                while (processed < REQUESTS_PER_INITIAL) {
                    LiftRideEvent event = eventQueue.poll();
                    if (event != null) {
                        if (sendPostWithRetry(event, skiersApi)) {
                            processed++;
                            successCount.incrementAndGet();  // Increment success count if the request succeeds
                        }
                    } else {
                        handleEmptyQueue();  // Wait briefly if the queue is temporarily empty
                    }
                }
            } finally {
                remainingInitials.decrementAndGet();  // Mark this Phase 1 thread as completed
            }
        }

        private void handleEmptyQueue() {
            if (!eventQueue.isEmpty()) return;  // Avoid waiting if there are events in the queue
            LockSupport.parkNanos(100_000);  // Wait 100 μs before checking again
        }
    }

    // Worker class for Phase 2 (dynamic phase)
    static class DynamicWorker implements Runnable {
        private final SkiersApi skiersApi = new SkiersApi(new ApiClient().setBasePath(BASE_PATH));

        @Override
        public void run() {
            while (!(generatingDone && eventQueue.isEmpty())) {  // Continue until all events are processed
                LiftRideEvent event = eventQueue.poll();
                if (event != null) {
                    if (sendPostWithRetry(event, skiersApi)) {
                        successCount.incrementAndGet();  // Increment success count if the request succeeds
                    }
                } else {
                    LockSupport.parkNanos(50_000);  // Wait 50 μs before checking again
                }
            }
        }
    }

    // Sends a POST request with retries
    private static boolean sendPostWithRetry(LiftRideEvent event, SkiersApi skiersApi) {
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                skiersApi.writeNewLiftRide(event.liftRide, event.resortID, "2025", "1", event.skierID);
                return true;
            } catch (ApiException e) {
                if (attempt == MAX_RETRIES) {
                    failureCount.incrementAndGet();  // Mark the request as failed after the maximum retries
                    return false;
                }
                handleRetryDelay(attempt);
            }
        }
        return false;
    }

    // Implements exponential backoff for retries
    private static void handleRetryDelay(int attempt) {
        long delay = Math.min((long) Math.pow(2, attempt) * 10_000_000, 1_000_000_000);  // Cap at 1 second
        LockSupport.parkNanos(delay);
    }

    // Generates random LiftRide events and adds them to the queue
    private static void generateLiftRides() {
        for (int i = 0; i < TOTAL_REQUESTS; i++) {
            eventQueue.offer(createRandomEvent());
        }
    }

    // Creates a random LiftRideEvent with random IDs and time values
    private static LiftRideEvent createRandomEvent() {
        return new LiftRideEvent(
                new LiftRide()
                        .liftID(ThreadLocalRandom.current().nextInt(1, 41))
                        .time(ThreadLocalRandom.current().nextInt(1, 361)),
                ThreadLocalRandom.current().nextInt(1, 11),
                ThreadLocalRandom.current().nextInt(1, 100_001)
        );
    }

    // Prints the final performance report
    private static void printReport(long startTime, long endTime) {
        long totalTime = endTime - startTime;
        System.out.println("\n=== FINAL REPORT ===");
        System.out.println("Successful requests: " + successCount.get());
        System.out.println("Failed requests:    " + failureCount.get());
        System.out.println("Dynamic threads used: " + DYNAMIC_THREADS);
        System.out.printf("Total time: %.2f seconds%n", totalTime / 1000.0);
        System.out.printf("Throughput: %,.0f req/s%n", successCount.get() / (totalTime / 1000.0));
        System.out.println("Remaining events:   " + eventQueue.size());
    }

    // Represents a LiftRide event with necessary details
    static class LiftRideEvent {
        final LiftRide liftRide;
        final int resortID;
        final int skierID;

        LiftRideEvent(LiftRide liftRide, int resortID, int skierID) {
            this.liftRide = liftRide;
            this.resortID = resortID;
            this.skierID = skierID;
        }
    }
}