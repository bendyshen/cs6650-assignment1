import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Test {
    public static final String BASE_PATH = "http://34.222.146.28:8080/JavaServlet_war";
    private static final int TOTAL_REQUESTS = 200_000;
    private static final int INITIAL_THREADS = 32;
    private static final int REQUESTS_PER_THREAD = 1_000;
    private static final int MAX_RETRIES = 5;

    private static final AtomicInteger successCount = new AtomicInteger(0);
    private static final AtomicInteger failureCount = new AtomicInteger(0);
    private static final AtomicBoolean allEventsGenerated = new AtomicBoolean(false);
    private static final AtomicBoolean expanded = new AtomicBoolean(false);

    public static void main(String[] args) throws InterruptedException {
        BlockingQueue<LiftRideEvent> queue = new LinkedBlockingQueue<>(10000);

        // Event generation thread
        new Thread(() -> {
            generateLiftRides(queue);
            allEventsGenerated.set(true);
        }).start();

        long startTime = System.currentTimeMillis();

        // Initial thread pool
        ExecutorService executor = Executors.newFixedThreadPool(INITIAL_THREADS);
        CountDownLatch initialLatch = new CountDownLatch(INITIAL_THREADS);

        for (int i = 0; i < INITIAL_THREADS; i++) {
            executor.execute(new RequestTask(queue, initialLatch, () -> {
                if (expanded.compareAndSet(false, true)) {
                    System.out.println("One of the initial threads completed. Launching 90 additional threads...");
                    addMoreThreads(queue);  // Launch 90 additional threads
                }
            }, REQUESTS_PER_THREAD));
        }

        initialLatch.await();  // Wait for all initial threads to complete
        executor.shutdown();

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        double throughput = (double) successCount.get() / (totalTime / 1000.0);

        // Final report
        System.out.println("\nFinal Report:");
        System.out.println("Total successful requests: " + successCount.get());
        System.out.println("Total failed requests: " + failureCount.get());
        System.out.println("Total run time (ms): " + totalTime);
        System.out.println("Throughput (requests/second): " + throughput);
    }

    // Adds 90 threads to handle the remaining requests
    private static void addMoreThreads(BlockingQueue<LiftRideEvent> queue) {
        int remainingRequests = TOTAL_REQUESTS - (INITIAL_THREADS * REQUESTS_PER_THREAD);
        int numNewThreads = 90;
        int requestsPerThread = remainingRequests / numNewThreads;
        int extraRequests = remainingRequests % numNewThreads;

        ExecutorService newExecutor = Executors.newFixedThreadPool(numNewThreads);
        CountDownLatch newLatch = new CountDownLatch(numNewThreads);

        for (int i = 0; i < numNewThreads; i++) {
            int requests = requestsPerThread + (i < extraRequests ? 1 : 0);  // Distribute extra requests evenly
            newExecutor.execute(new RequestTask(queue, newLatch, null, requests));
        }

        try {
            newLatch.await();  // Wait for all new threads to complete
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        newExecutor.shutdown();
    }

    // Generates LiftRide events and puts them into the queue
    private static void generateLiftRides(BlockingQueue<LiftRideEvent> queue) {
        for (int i = 0; i < TOTAL_REQUESTS; i++) {
            int resortID = ThreadLocalRandom.current().nextInt(1, 11);
            int skierID = ThreadLocalRandom.current().nextInt(1, 100_001);
            int liftID = ThreadLocalRandom.current().nextInt(1, 41);
            int time = ThreadLocalRandom.current().nextInt(1, 361);

            LiftRide liftRide = new LiftRide();
            liftRide.setLiftID(liftID);
            liftRide.setTime(time);

            try {
                queue.put(new LiftRideEvent(liftRide, resortID, skierID));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // Runnable task for sending requests to the API
    static class RequestTask implements Runnable {
        private final BlockingQueue<LiftRideEvent> queue;
        private final CountDownLatch latch;
        private final Runnable onComplete;
        private final int numRequests;

        public RequestTask(BlockingQueue<LiftRideEvent> queue, CountDownLatch latch, Runnable onComplete, int numRequests) {
            this.queue = queue;
            this.latch = latch;
            this.onComplete = onComplete;
            this.numRequests = numRequests;
        }

        @Override
        public void run() {
            SkiersApi skiersApi = new SkiersApi(new ApiClient().setBasePath(BASE_PATH));
            int localSuccess = 0;
            int localFailure = 0;

            try {
                for (int i = 0; i < numRequests; i++) {
                    LiftRideEvent event = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (event == null) {
                        if (allEventsGenerated.get() && queue.isEmpty()) break;
                        continue;
                    }

                    boolean success = sendPostRequest(skiersApi, event);
                    if (success) localSuccess++;
                    else localFailure++;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                successCount.addAndGet(localSuccess);
                failureCount.addAndGet(localFailure);
                if (latch != null) latch.countDown();
                if (onComplete != null) onComplete.run();
            }
        }

        private boolean sendPostRequest(SkiersApi skiersApi, LiftRideEvent event) {
            for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
                try {
                    skiersApi.writeNewLiftRide(
                            event.getLiftRide(),
                            event.getResortID(),
                            "2025",
                            "1",
                            event.getSkierID()
                    );
                    return true;
                } catch (ApiException e) {
                    if (attempt == MAX_RETRIES) {
                        return false;
                    }
                }
            }
            return false;
        }
    }

    // Class to represent a LiftRide event
    static class LiftRideEvent {
        private final LiftRide liftRide;
        private final int resortID;
        private final int skierID;

        public LiftRideEvent(LiftRide liftRide, int resortID, int skierID) {
            this.liftRide = liftRide;
            this.resortID = resortID;
            this.skierID = skierID;
        }

        public LiftRide getLiftRide() {
            return liftRide;
        }

        public int getResortID() {
            return resortID;
        }

        public int getSkierID() {
            return skierID;
        }
    }
}