/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionTracker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamingSegmentatorAdmissionTests extends ESTestCase {

    /**
     * The controller must never hand more than {@code maxConcurrentSegmentators} tasks to the pool at once, even
     * when the pool itself has spare threads: excess submissions are queued in the controller (holding no pool
     * thread) and dispatched only as running ones complete. This is the invariant that keeps pool threads free for
     * the parser tasks a segmentator depends on.
     */
    public void testCapsConcurrentSegmentatorsBelowPoolSize() throws Exception {
        int cap = 2;
        int submissions = 5;
        // Pool larger than the cap so the CONTROLLER, not the pool, is the limiter.
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            StreamingSegmentatorAdmission admission = new StreamingSegmentatorAdmission(cap);
            CountDownLatch release = new CountDownLatch(1);
            CountDownLatch allDone = new CountDownLatch(submissions);
            AtomicInteger concurrent = new AtomicInteger();
            AtomicInteger maxConcurrent = new AtomicInteger();

            for (int i = 0; i < submissions; i++) {
                admission.submit(() -> {
                    int now = concurrent.incrementAndGet();
                    maxConcurrent.accumulateAndGet(now, Math::max);
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        concurrent.decrementAndGet();
                        allDone.countDown();
                    }
                }, pool, e -> fail("no submission should have been rejected: " + e));
            }

            // Exactly `cap` tasks run; the rest are held in the controller, not the pool.
            assertBusy(() -> assertEquals(cap, concurrent.get()), 5, TimeUnit.SECONDS);
            assertEquals("controller must hold the excess submissions", submissions - cap, admission.pending());
            assertEquals(cap, admission.running());

            release.countDown();
            assertTrue("all submissions must run once slots free up", allDone.await(10, TimeUnit.SECONDS));
            assertEquals("the cap must never be exceeded", cap, maxConcurrent.get());
            assertBusy(() -> {
                assertEquals(0, admission.running());
                assertEquals(0, admission.pending());
            }, 5, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
    }

    /** Queued segmentators are promoted in submission (FIFO) order as running ones complete. */
    public void testPendingSegmentatorsRunInSubmissionOrder() throws Exception {
        int submissions = 6;
        // Single-permit controller on a single-thread pool forces strictly serial execution, so completion order
        // equals dispatch order equals the controller's promotion order.
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            StreamingSegmentatorAdmission admission = new StreamingSegmentatorAdmission(1);
            List<Integer> order = new CopyOnWriteArrayList<>();
            CountDownLatch allDone = new CountDownLatch(submissions);
            for (int i = 0; i < submissions; i++) {
                final int id = i;
                admission.submit(() -> {
                    order.add(id);
                    allDone.countDown();
                }, pool, e -> fail("no submission should have been rejected: " + e));
            }
            assertTrue(allDone.await(10, TimeUnit.SECONDS));
            for (int i = 0; i < submissions; i++) {
                assertEquals("segmentators must be promoted FIFO", Integer.valueOf(i), order.get(i));
            }
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * When the executor rejects a segmentator, the controller must run the caller's reject callback and free the
     * slot it reserved (promoting any successor) so the budget is not permanently consumed by a task that never ran.
     */
    public void testRejectionFreesSlotAndInvokesCallback() {
        Executor rejecting = command -> { throw new RejectedExecutionException("pool closed"); };
        StreamingSegmentatorAdmission admission = new StreamingSegmentatorAdmission(2);
        AtomicInteger rejected = new AtomicInteger();
        for (int i = 0; i < 5; i++) {
            admission.submit(() -> fail("a rejected task must never run"), rejecting, e -> rejected.incrementAndGet());
        }
        assertEquals("every submission's reject callback must fire", 5, rejected.get());
        assertEquals("rejected slots must be freed, not leaked", 0, admission.running());
        assertEquals(0, admission.pending());
    }

    /** The unbounded controller dispatches every segmentator immediately and never queues. */
    public void testUnboundedDispatchesImmediately() throws Exception {
        int submissions = 16;
        ExecutorService pool = Executors.newFixedThreadPool(submissions);
        try {
            StreamingSegmentatorAdmission admission = StreamingSegmentatorAdmission.unbounded();
            CountDownLatch release = new CountDownLatch(1);
            CountDownLatch running = new CountDownLatch(submissions);
            for (int i = 0; i < submissions; i++) {
                admission.submit(() -> {
                    running.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }, pool, e -> fail("no submission should have been rejected: " + e));
            }
            assertTrue("unbounded must run all submissions at once", running.await(10, TimeUnit.SECONDS));
            assertEquals("unbounded must never queue", 0, admission.pending());
            release.countDown();
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * {@link StreamingSegmentatorAdmission.Handle#cancel()} drops a still-queued FIFO-head without
     * occupying a slot; the next pending item is what {@code releaseAndPromote} dispatches. Callbacks
     * (executor hand-off, reject) must not run while the admission monitor is held.
     */
    public void testCancelRemovesPendingFifoHeadAndPromotesNext() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        RecordingTracker tracker = new RecordingTracker();
        StreamingSegmentatorAdmission admission = new StreamingSegmentatorAdmission(1, tracker);
        Executor asserting = command -> {
            assertFalse("executor.execute must run after the admission monitor is released", Thread.holdsLock(admission));
            pool.execute(command);
        };
        try {
            CountDownLatch holdFirst = new CountDownLatch(1);
            CountDownLatch firstRunning = new CountDownLatch(1);
            List<Integer> ran = new CopyOnWriteArrayList<>();
            List<StreamingSegmentatorAdmission.Handle> handles = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                final int id = i;
                handles.add(admission.submit(() -> {
                    ran.add(id);
                    if (id == 0) {
                        firstRunning.countDown();
                        try {
                            holdFirst.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }, asserting, e -> {
                    assertFalse("onReject must run after the admission monitor is released", Thread.holdsLock(admission));
                    fail("no submission should have been rejected: " + e);
                }));
            }
            assertTrue(firstRunning.await(5, TimeUnit.SECONDS));
            assertEquals(1, admission.running());
            assertEquals(2, admission.pending());
            assertFalse("already-dispatched cancel is a no-op", handles.get(0).cancel());
            assertTrue("pending FIFO-head must be removable", handles.get(1).cancel());
            assertEquals("cancelling the head leaves the successor pending", 1, admission.pending());
            holdFirst.countDown();
            assertBusy(() -> {
                assertEquals(List.of(0, 2), ran);
                assertEquals(0, admission.running());
                assertEquals(0, admission.pending());
            }, 5, TimeUnit.SECONDS);
            assertFalse("already-cancelled handle stays cancelled", handles.get(1).cancel());
            assertEquals(1, tracker.finished.get());
            assertEquals(0, tracker.outstanding.get());
        } finally {
            pool.shutdownNow();
        }
    }

    /** Cancel of a never-queued (immediately dispatched) submission is a no-op and does not free a running slot. */
    public void testCancelOfDispatchedWorkIsNoOp() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            StreamingSegmentatorAdmission admission = new StreamingSegmentatorAdmission(1);
            CountDownLatch hold = new CountDownLatch(1);
            CountDownLatch running = new CountDownLatch(1);
            StreamingSegmentatorAdmission.Handle handle = admission.submit(() -> {
                running.countDown();
                try {
                    hold.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, pool, e -> fail("no submission should have been rejected: " + e));
            assertTrue(running.await(5, TimeUnit.SECONDS));
            assertEquals(1, admission.running());
            assertFalse(handle.cancel());
            assertEquals(1, admission.running());
            hold.countDown();
            assertBusy(() -> assertEquals(0, admission.running()), 5, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
    }

    private static final class RecordingTracker implements AdmissionTracker {
        private final AtomicInteger outstanding = new AtomicInteger();
        private final AtomicInteger grants = new AtomicInteger();
        private final AtomicInteger finished = new AtomicInteger();

        @Override
        public Wait waitStarted(String gate, String waiter) {
            outstanding.incrementAndGet();
            return new Wait() {
                @Override
                public void granted() {
                    outstanding.decrementAndGet();
                    grants.incrementAndGet();
                }

                @Override
                public void finished() {
                    outstanding.decrementAndGet();
                    finished.incrementAndGet();
                }
            };
        }
    }
}
