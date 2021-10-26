/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class WorkerBulkByScrollTaskStateTests extends ESTestCase {
    private BulkByScrollTask task;
    private WorkerBulkByScrollTaskState workerState;

    @Before
    public void createTask() {
        task = new BulkByScrollTask(1, "test_type", "test_action", "test", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        task.setWorker(Float.POSITIVE_INFINITY, null);
        workerState = task.getWorkerState();
    }

    public void testBasicData() {
        assertEquals(1, task.getId());
        assertEquals("test_type", task.getType());
        assertEquals("test_action", task.getAction());
        assertEquals("test", task.getDescription());
    }

    public void testProgress() {
        long created = 0;
        long updated = 0;
        long deleted = 0;
        long versionConflicts = 0;
        long noops = 0;
        int batch = 0;
        BulkByScrollTask.Status status = task.getStatus();
        assertEquals(0, status.getTotal());
        assertEquals(created, status.getCreated());
        assertEquals(updated, status.getUpdated());
        assertEquals(deleted, status.getDeleted());
        assertEquals(versionConflicts, status.getVersionConflicts());
        assertEquals(batch, status.getBatches());
        assertEquals(noops, status.getNoops());

        long totalHits = randomIntBetween(10, 1000);
        workerState.setTotal(totalHits);
        for (long p = 0; p < totalHits; p++) {
            status = task.getStatus();
            assertEquals(totalHits, status.getTotal());
            assertEquals(created, status.getCreated());
            assertEquals(updated, status.getUpdated());
            assertEquals(deleted, status.getDeleted());
            assertEquals(versionConflicts, status.getVersionConflicts());
            assertEquals(batch, status.getBatches());
            assertEquals(noops, status.getNoops());

            if (randomBoolean()) {
                created++;
                workerState.countCreated();
            } else if (randomBoolean()) {
                updated++;
                workerState.countUpdated();
            } else {
                deleted++;
                workerState.countDeleted();
            }

            if (rarely()) {
                versionConflicts++;
                workerState.countVersionConflict();
            }

            if (rarely()) {
                batch++;
                workerState.countBatch();
            }

            if (rarely()) {
                noops++;
                workerState.countNoop();
            }
        }
        status = task.getStatus();
        assertEquals(totalHits, status.getTotal());
        assertEquals(created, status.getCreated());
        assertEquals(updated, status.getUpdated());
        assertEquals(deleted, status.getDeleted());
        assertEquals(versionConflicts, status.getVersionConflicts());
        assertEquals(batch, status.getBatches());
        assertEquals(noops, status.getNoops());
    }

    /**
     * Furiously rethrottles a delayed request to make sure that we never run it twice.
     */
    public void testDelayAndRethrottle() throws IOException, InterruptedException {
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        AtomicBoolean done = new AtomicBoolean();
        int threads = between(1, 10);
        CyclicBarrier waitForShutdown = new CyclicBarrier(threads);

        /*
         * We never end up waiting this long because the test rethrottles over and over again, ratcheting down the delay a random amount
         * each time.
         */
        float originalRequestsPerSecond = (float) randomDoubleBetween(1, 10000, true);
        workerState.rethrottle(originalRequestsPerSecond);
        TimeValue maxDelay = timeValueSeconds(between(1, 5));
        assertThat(maxDelay.nanos(), greaterThanOrEqualTo(0L));
        int batchSizeForMaxDelay = (int) (maxDelay.seconds() * originalRequestsPerSecond);
        ThreadPool threadPool = new TestThreadPool(getTestName()) {
            @Override
            public ScheduledCancellable schedule(Runnable command, TimeValue delay, String name) {
                assertThat(delay.nanos(), both(greaterThanOrEqualTo(0L)).and(lessThanOrEqualTo(maxDelay.nanos())));
                return super.schedule(command, delay, name);
            }
        };
        try {
            workerState.delayPrepareBulkRequest(threadPool, System.nanoTime(), batchSizeForMaxDelay,
                new AbstractRunnable() {
                    @Override
                    protected void doRun() throws Exception {
                        boolean oldValue = done.getAndSet(true);
                        if (oldValue) {
                            throw new RuntimeException("Ran twice oh no!");
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        errors.add(e);
                    }
                }
            );

            // Rethrottle on a random number of threads, one of which is this thread.
            Runnable test = () -> {
                try {
                    int rethrottles = 0;
                    while (false == done.get()) {
                        float requestsPerSecond = (float) randomDoubleBetween(0, originalRequestsPerSecond * 2, true);
                        workerState.rethrottle(requestsPerSecond);
                        rethrottles += 1;
                    }
                    logger.info("Rethrottled [{}] times", rethrottles);
                    waitForShutdown.await();
                } catch (Exception e) {
                    errors.add(e);
                }
            };
            for (int i = 1; i < threads; i++) {
                threadPool.generic().execute(test);
            }
            test.run();
        } finally {
            // Other threads should finish up quickly as they are checking the same AtomicBoolean.
            threadPool.shutdown();
            threadPool.awaitTermination(10, TimeUnit.SECONDS);
        }
        assertThat(errors, empty());
    }

    public void testDelayNeverNegative() throws IOException {
        // Thread pool that returns a ScheduledFuture that claims to have a negative delay
        ThreadPool threadPool = new TestThreadPool("test") {
            public ScheduledCancellable schedule(Runnable command, TimeValue delay, String name) {
                return new ScheduledCancellable() {
                    @Override
                    public long getDelay(TimeUnit unit) {
                        return -1;
                    }

                    @Override
                    public int compareTo(Delayed o) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean cancel() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean isCancelled() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
        try {
            // Have the task use the thread pool to delay a task that does nothing
            workerState.delayPrepareBulkRequest(threadPool, 0, 1, new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                }
                @Override
                public void onFailure(Exception e) {
                    throw new UnsupportedOperationException();
                }
            });
            // Even though the future returns a negative delay we just return 0 because the time is up.
            assertEquals(timeValueSeconds(0), task.getStatus().getThrottledUntil());
        } finally {
            threadPool.shutdown();
        }
    }

    public void testPerfectlyThrottledBatchTime() {
        workerState.rethrottle(Float.POSITIVE_INFINITY);
        assertThat((double) workerState.perfectlyThrottledBatchTime(randomInt()), closeTo(0f, 0f));

        int total = between(0, 1000000);
        workerState.rethrottle(1);
        assertThat((double) workerState.perfectlyThrottledBatchTime(total),
                closeTo(TimeUnit.SECONDS.toNanos(total), TimeUnit.SECONDS.toNanos(1)));
    }
}
