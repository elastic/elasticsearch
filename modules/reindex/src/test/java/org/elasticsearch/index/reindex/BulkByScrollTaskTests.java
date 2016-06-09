/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;
import static org.elasticsearch.common.unit.TimeValue.timeValueNanos;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BulkByScrollTaskTests extends ESTestCase {
    private BulkByScrollTask task;

    @Before
    public void createTask() {
        task = new BulkByScrollTask(1, "test_type", "test_action", "test", TaskId.EMPTY_TASK_ID, Float.POSITIVE_INFINITY);
    }

    public void testBasicData() {
        assertEquals(1, task.getId());
        assertEquals("test_type", task.getType());
        assertEquals("test_action", task.getAction());
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
        task.setTotal(totalHits);
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
                task.countCreated();
            } else if (randomBoolean()) {
                updated++;
                task.countUpdated();
            } else {
                deleted++;
                task.countDeleted();
            }

            if (rarely()) {
                versionConflicts++;
                task.countVersionConflict();
            }

            if (rarely()) {
                batch++;
                task.countBatch();
            }

            if (rarely()) {
                noops++;
                task.countNoop();
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

    public void testStatusHatesNegatives() {
        checkStatusNegatives(-1, 0, 0, 0, 0, 0, 0, 0, 0, "total");
        checkStatusNegatives(0, -1, 0, 0, 0, 0, 0, 0, 0, "updated");
        checkStatusNegatives(0, 0, -1, 0, 0, 0, 0, 0, 0, "created");
        checkStatusNegatives(0, 0, 0, -1, 0, 0, 0, 0, 0, "deleted");
        checkStatusNegatives(0, 0, 0, 0, -1, 0, 0, 0, 0, "batches");
        checkStatusNegatives(0, 0, 0, 0, 0, -1, 0, 0, 0, "versionConflicts");
        checkStatusNegatives(0, 0, 0, 0, 0, 0, -1, 0, 0, "noops");
        checkStatusNegatives(0, 0, 0, 0, 0, 0, 0, -1, 0, "bulkRetries");
        checkStatusNegatives(0, 0, 0, 0, 0, 0, 0, 0, -1, "searchRetries");
    }

    /**
     * Build a task status with only some values. Used for testing negative values.
     */
    private void checkStatusNegatives(long total, long updated, long created, long deleted, int batches, long versionConflicts,
            long noops, long bulkRetries, long searchRetries, String fieldName) {
        TimeValue throttle = parseTimeValue(randomPositiveTimeValue(), "test");
        TimeValue throttledUntil = parseTimeValue(randomPositiveTimeValue(), "test");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new BulkByScrollTask.Status(total, updated, created,
                deleted, batches, versionConflicts, noops, bulkRetries, searchRetries, throttle, 0f, null, throttledUntil));
        assertEquals(e.getMessage(), fieldName + " must be greater than 0 but was [-1]");
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
        task.rethrottle(originalRequestsPerSecond);
        TimeValue maxDelay = timeValueSeconds(between(1, 5));
        assertThat(maxDelay.nanos(), greaterThanOrEqualTo(0L));
        int batchSizeForMaxDelay = (int) (maxDelay.seconds() * originalRequestsPerSecond);
        ThreadPool threadPool = new TestThreadPool(getTestName()) {
            @Override
            public ScheduledFuture<?> schedule(TimeValue delay, String name, Runnable command) {
                assertThat(delay.nanos(), both(greaterThanOrEqualTo(0L)).and(lessThanOrEqualTo(maxDelay.nanos())));
                return super.schedule(delay, name, command);
            }
        };
        try {
            task.delayPrepareBulkRequest(threadPool, timeValueNanos(System.nanoTime()), batchSizeForMaxDelay, new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                    boolean oldValue = done.getAndSet(true);
                    if (oldValue) {
                        throw new RuntimeException("Ran twice oh no!");
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    errors.add(t);
                }
            });

            // Rethrottle on a random number of threads, on of which is this thread.
            Runnable test = () -> {
                try {
                    int rethrottles = 0;
                    while (false == done.get()) {
                        float requestsPerSecond = (float) randomDoubleBetween(0, originalRequestsPerSecond * 2, true);
                        task.rethrottle(requestsPerSecond);
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
            public ScheduledFuture<?> schedule(TimeValue delay, String name, Runnable command) {
                return new ScheduledFuture<Void>() {
                    @Override
                    public long getDelay(TimeUnit unit) {
                        return -1;
                    }

                    @Override
                    public int compareTo(Delayed o) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean cancel(boolean mayInterruptIfRunning) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean isCancelled() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean isDone() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Void get() throws InterruptedException, ExecutionException {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
        try {
            // Have the task use the thread pool to delay a task that does nothing
            task.delayPrepareBulkRequest(threadPool, timeValueSeconds(0), 1, new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                }
                @Override
                public void onFailure(Throwable t) {
                    throw new UnsupportedOperationException();
                }
            });
            // Even though the future returns a negative delay we just return 0 because the time is up.
            assertEquals(timeValueSeconds(0), task.getStatus().getThrottledUntil());
        } finally {
            threadPool.shutdown();
        }
    }

    public void testXContentRepresentationOfUnlimitedRequestsPerSecon() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        task.getStatus().toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(builder.string(), containsString("\"requests_per_second\":\"unlimited\""));
    }

    public void testPerfectlyThrottledBatchTime() {
        task.rethrottle(Float.POSITIVE_INFINITY);
        assertThat((double) task.perfectlyThrottledBatchTime(randomInt()), closeTo(0f, 0f));

        int total = between(0, 1000000);
        task.rethrottle(1);
        assertThat((double) task.perfectlyThrottledBatchTime(total),
                closeTo(TimeUnit.SECONDS.toNanos(total), TimeUnit.SECONDS.toNanos(1)));
    }
}
