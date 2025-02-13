import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;

public class MultithreadedSwaggerClient {
    // Configuration for the client and server
    private static final String IP_ADDRESS = "52.13.215.91";  // Replace with your EC2 IP address
    private static final String BASE_PATH = "http://" + IP_ADDRESS + ":8080/JavaServlet_war";
    private static final int TOTAL_REQUESTS = 200_000;
    private static final int INITIAL_THREADS = 32;  // Phase 1 threads
    private static final int REQUESTS_PER_INITIAL = 1_000;  // Requests per thread in Phase 1
    private static final int MAX_RETRIES = 5;  // Maximum retries for failed requests
    private static final int DYNAMIC_THREADS = 90;  // Phase 2 dynamic threads

    // Shared variables for tracking progress and metrics
    private static boolean generatingDone = false;
    private static final AtomicInteger successCount = new AtomicInteger(0);
    private static final AtomicInteger failureCount = new AtomicInteger(0);
    private static final AtomicInteger remainingInitials = new AtomicInteger(INITIAL_THREADS);
    private static final BlockingQueue<LiftRideEvent> eventQueue = new ArrayBlockingQueue<>(TOTAL_REQUESTS);
    private static final AtomicBoolean phase2Started = new AtomicBoolean(false);

    // Lists for recording response times and request details for logging purposes
    private static final List<Long> responseTimes = Collections.synchronizedList(new ArrayList<>());
    private static final List<String> requestLogs = Collections.synchronizedList(new ArrayList<>());
    private static final List<String> throughputLogs = Collections.synchronizedList(new ArrayList<>());

    public static void main(String[] args) throws InterruptedException {
        // Generate lift ride events
        generateLiftRides();
        generatingDone = true;
        System.out.println("[Generator] All events generated");

        long startTime = System.currentTimeMillis();
        scheduleThroughputLogging(startTime);  // Start logging throughput at 1-second intervals

        // Phase 1: Launch initial threads
        ExecutorService phase1Executor = Executors.newFixedThreadPool(INITIAL_THREADS);
        for (int i = 0; i < INITIAL_THREADS; i++) {
            phase1Executor.execute(new Phase1Worker());
        }

        // Phase 2: Launch dynamic threads after Phase 1 starts to complete
        ExecutorService phase2Executor = Executors.newFixedThreadPool(DYNAMIC_THREADS);
        new Thread(() -> {
            while (!phase2Started.get()) {
                if (remainingInitials.get() < INITIAL_THREADS) {
                    System.out.println("[Phase2] Starting dynamic threads...");
                    for (int i = 0; i < DYNAMIC_THREADS; i++) {
                        phase2Executor.execute(new DynamicWorker());
                    }
                    phase2Started.set(true);
                }
                LockSupport.parkNanos(1_000_000);  // Check every 1 ms
            }
        }).start();

        // Wait for all threads in both phases to complete
        phase1Executor.shutdown();
        phase1Executor.awaitTermination(1, TimeUnit.HOURS);
        phase2Executor.shutdown();
        phase2Executor.awaitTermination(1, TimeUnit.HOURS);

        long endTime = System.currentTimeMillis();
        printReport(startTime, endTime);  // Print final report with response time statistics
        writeRequestLogs("request_details.csv");  // Write request logs to a CSV file
        writeThroughputLogs("throughput_logs.csv");  // Write throughput logs to a CSV file
        System.exit(0);
    }

    // Phase 1 worker that sends 1000 requests per thread
    static class Phase1Worker implements Runnable {
        private final SkiersApi skiersApi = new SkiersApi(new ApiClient().setBasePath(BASE_PATH));
        private int processed = 0;

        @Override
        public void run() {
            try {
                while (processed < REQUESTS_PER_INITIAL) {
                    LiftRideEvent event = eventQueue.poll();  // Take an event from the queue
                    if (event != null) {
                        if (sendPostWithRetry(event, skiersApi)) {
                            processed++;
                            successCount.incrementAndGet();
                        }
                    } else {
                        handleEmptyQueue();
                    }
                }
            } finally {
                remainingInitials.decrementAndGet();  // Decrease remaining initial threads count
            }
        }

        private void handleEmptyQueue() {
            if (!eventQueue.isEmpty()) return;
            LockSupport.parkNanos(100_000);  // Wait 100 μs if the queue is empty
        }
    }

    // Phase 2 dynamic worker that keeps sending requests until all events are processed
    static class DynamicWorker implements Runnable {
        private final SkiersApi skiersApi = new SkiersApi(new ApiClient().setBasePath(BASE_PATH));

        @Override
        public void run() {
            while (!(generatingDone && eventQueue.isEmpty())) {
                LiftRideEvent event = eventQueue.poll();
                if (event != null) {
                    if (sendPostWithRetry(event, skiersApi)) {
                        successCount.incrementAndGet();
                    }
                } else {
                    LockSupport.parkNanos(50_000);  // Wait 50 μs if the queue is empty
                }
            }
        }
    }

    // Send a POST request with retry logic and record the response time
    private static boolean sendPostWithRetry(LiftRideEvent event, SkiersApi skiersApi) {
        long startTime = System.currentTimeMillis();
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                skiersApi.writeNewLiftRide(event.liftRide, event.resortID, "2025", "1", event.skierID);
                long endTime = System.currentTimeMillis();
                logRequest(startTime, "POST", endTime - startTime, 201);
                responseTimes.add(endTime - startTime);
                return true;
            } catch (ApiException e) {
                if (attempt == MAX_RETRIES) {
                    failureCount.incrementAndGet();
                    long endTime = System.currentTimeMillis();
                    logRequest(startTime, "POST", endTime - startTime, e.getCode());
                    return false;
                }
                handleRetryDelay(attempt);
            }
        }
        return false;
    }

    private static void handleRetryDelay(int attempt) {
        long delay = Math.min((long) Math.pow(2, attempt) * 10_000_000, 1_000_000_000);
        LockSupport.parkNanos(delay);  // Exponential backoff for retries
    }

    // Generate lift ride events and add them to the queue
    private static void generateLiftRides() {
        for (int i = 0; i < TOTAL_REQUESTS; i++) {
            eventQueue.offer(createRandomEvent());
        }
    }

    // Create a random LiftRide event
    private static LiftRideEvent createRandomEvent() {
        return new LiftRideEvent(
                new LiftRide()
                        .liftID(ThreadLocalRandom.current().nextInt(1, 41))
                        .time(ThreadLocalRandom.current().nextInt(1, 361)),
                ThreadLocalRandom.current().nextInt(1, 11),
                ThreadLocalRandom.current().nextInt(1, 100_001)
        );
    }

    // Log each request with its start time, request type, latency, and response code
    private static void logRequest(long startTime, String requestType, long latency, int responseCode) {
        requestLogs.add(Instant.ofEpochMilli(startTime) + "," + requestType + "," + latency + "," + responseCode);
    }

    // Write request logs to a CSV file
    private static void writeRequestLogs(String filePath) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write("Start Time,Request Type,Latency (ms),Response Code\n");
            for (String log : requestLogs) {
                writer.write(log + "\n");
            }
        } catch (IOException e) {
            System.err.println("Error writing request logs: " + e.getMessage());
        }
    }

    // Log throughput at 1-second intervals
    private static void scheduleThroughputLogging(long startTime) {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            long elapsedTime = System.currentTimeMillis() - startTime;
            double throughput = successCount.get() / (elapsedTime / 1000.0);
            throughputLogs.add(elapsedTime + "," + throughput);
        }, 1, 1, TimeUnit.SECONDS);
    }

    // Write throughput logs to a CSV file
    private static void writeThroughputLogs(String filePath) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write("Elapsed Time (ms),Throughput (req/sec)\n");
            for (String log : throughputLogs) {
                writer.write(log + "\n");
            }
        } catch (IOException e) {
            System.err.println("Error writing throughput logs: " + e.getMessage());
        }
    }

    // Print the final report with statistics
    private static void printReport(long startTime, long endTime) {
        long totalTime = endTime - startTime;
        List<Long> sortedTimes = new ArrayList<>(responseTimes);
        Collections.sort(sortedTimes);
        long minTime = sortedTimes.get(0);
        long maxTime = sortedTimes.get(sortedTimes.size() - 1);
        long meanTime = (long) sortedTimes.stream().mapToLong(Long::longValue).average().orElse(0);
        long medianTime = sortedTimes.get(sortedTimes.size() / 2);
        long p99Time = sortedTimes.get((int) (sortedTimes.size() * 0.99));

        System.out.println("\n=== FINAL REPORT ===");
        System.out.println("Successful requests: " + successCount.get());
        System.out.println("Failed requests:    " + failureCount.get());
        System.out.printf("Total time: %.2f seconds%n", totalTime / 1000.0);
        System.out.printf("Throughput: %,.0f req/s%n", successCount.get() / (totalTime / 1000.0));
        System.out.println("Min response time: " + minTime + " ms");
        System.out.println("Max response time: " + maxTime + " ms");
        System.out.println("Mean response time: " + meanTime + " ms");
        System.out.println("Median response time: " + medianTime + " ms");
        System.out.println("99th percentile response time: " + p99Time + " ms");
    }

    // Class representing a LiftRide event with associated data
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