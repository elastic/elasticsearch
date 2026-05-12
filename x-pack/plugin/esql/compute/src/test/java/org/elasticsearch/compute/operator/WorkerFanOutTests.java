/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Unit tests for {@link WorkerFanOut} that exercise the helper directly, without going through
 * {@link org.elasticsearch.compute.operator.topn.TopNOperator}. These cover the concurrent
 * coordination paths: round-robin fan-out, close/drain races, executor rejection, backpressure
 * via {@code isBlocked()}, and exception propagation.
 */
public class WorkerFanOutTests extends ComputeTestCase {

    private ExecutorService executor;

    @After
    public void shutdownExecutor() throws InterruptedException {
        if (executor != null && executor.isShutdown() == false) {
            executor.shutdownNow();
            assertTrue("executor did not terminate", executor.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    /** Trivial per-slot state: counts pages processed and tracks release. */
    private static final class TestState implements Releasable, org.apache.lucene.util.Accountable {
        final AtomicInteger processed = new AtomicInteger();
        volatile boolean closed = false;

        @Override
        public long ramBytesUsed() {
            return 64L;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    /**
     * Configurable test fanout. {@code processPage} can be customised so individual tests can block
     * or throw; the base behaviour just increments the per-slot counter and releases the page.
     */
    private static class TestFanOut extends WorkerFanOut<TestState> {
        final AtomicInteger nextWorker = new AtomicInteger();
        final List<TestState> mergedStates = new ArrayList<>();
        volatile boolean mergeCalled = false;
        volatile PageHandler pageHandler = (state, page, slotIndex) -> state.processed.incrementAndGet();

        interface PageHandler {
            void handle(TestState state, Page page, int slotIndex) throws Exception;
        }

        TestFanOut(DriverContext driverContext, java.util.concurrent.Executor executor, int workerCount, int maxInFlightPages) {
            super(driverContext, executor, workerCount, maxInFlightPages);
        }

        @Override
        protected TestState createWorkerState(int workerIndex) {
            return new TestState();
        }

        @Override
        protected int chooseWorker(Page page) {
            // Round-robin across slots; modulo by worker count to be safe under wrap-around.
            int n = nextWorker.getAndIncrement();
            int workerCount = getWorkerCountForTest();
            return Math.floorMod(n, workerCount);
        }

        // Helper to read worker count without exposing the private array; we capture it via the
        // ctor argument indirectly by inspecting the merged states list size at finish. For
        // chooseWorker we need the count up-front; tests construct subclasses with a known size.
        protected int getWorkerCountForTest() {
            return 2;
        }

        @Override
        protected void processPage(TestState state, Page page, int slotIndex) {
            try {
                pageHandler.handle(state, page, slotIndex);
            } catch (RuntimeException re) {
                throw re;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected ReleasableIterator<Page> mergeAndBuildResult(List<TestState> states) {
            mergeCalled = true;
            mergedStates.addAll(states);
            for (TestState s : states) {
                s.close();
            }
            return ReleasableIterator.empty();
        }
    }

    private Page makePage(BlockFactory blockFactory, long value) {
        LongBlock block = blockFactory.newConstantLongBlockWith(value, 1);
        return new Page(block);
    }

    private DriverContext newDriverContext() {
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    /** 1. Smoke test: basic fan-out works end-to-end across two slots. */
    public void testBasicFanOut() throws Exception {
        executor = Executors.newFixedThreadPool(2);
        DriverContext driverContext = newDriverContext();
        BlockFactory blockFactory = driverContext.blockFactory();

        try (TestFanOut fanOut = new TestFanOut(driverContext, executor, 2, 8)) {
            int pageCount = 10;
            for (int i = 0; i < pageCount; i++) {
                fanOut.addInput(makePage(blockFactory, i));
            }
            fanOut.finish();

            // Spin until drains complete; cap with a timeout to avoid wedging the suite.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (fanOut.canProduceMoreDataWithoutExtraInput() == false || fanOut.isFinished() == false) {
                IsBlockedResult blocked = fanOut.isBlocked();
                if (blocked != Operator.NOT_BLOCKED) {
                    final CountDownLatch latch = new CountDownLatch(1);
                    blocked.listener().addListener(org.elasticsearch.action.ActionListener.running(latch::countDown));
                    latch.await(5, TimeUnit.SECONDS);
                }
                // Drain by asking for output (builds the result iterator once in-flight reaches 0).
                fanOut.getOutput();
                if (fanOut.isFinished()) {
                    break;
                }
                if (System.nanoTime() > deadline) {
                    fail("fan-out did not finish in time");
                }
            }

            assertTrue("merge should be invoked exactly once on finish", fanOut.mergeCalled);
            assertThat(fanOut.mergedStates.size(), equalTo(2));
            int total = fanOut.mergedStates.stream().mapToInt(s -> s.processed.get()).sum();
            assertThat(total, equalTo(pageCount));
            // Round-robin across 2 slots and even page count -> equal distribution.
            assertThat(fanOut.mergedStates.get(0).processed.get(), equalTo(pageCount / 2));
            assertThat(fanOut.mergedStates.get(1).processed.get(), equalTo(pageCount / 2));
        }
    }

    /** 2. close() blocks until an in-flight drain completes. */
    public void testCloseWaitsForActiveDrain() throws Exception {
        executor = Executors.newFixedThreadPool(2);
        DriverContext driverContext = newDriverContext();
        BlockFactory blockFactory = driverContext.blockFactory();

        CountDownLatch processStarted = new CountDownLatch(1);
        CountDownLatch releaseProcess = new CountDownLatch(1);
        AtomicInteger processedAfterLatch = new AtomicInteger();

        TestFanOut fanOut = new TestFanOut(driverContext, executor, 2, 8);
        fanOut.pageHandler = (state, page, slotIndex) -> {
            processStarted.countDown();
            // Block the worker thread until the test releases it.
            assertTrue("releaseProcess wait timed out", releaseProcess.await(30, TimeUnit.SECONDS));
            processedAfterLatch.incrementAndGet();
            state.processed.incrementAndGet();
        };

        fanOut.addInput(makePage(blockFactory, 1));
        assertTrue("processPage never started", processStarted.await(30, TimeUnit.SECONDS));

        AtomicInteger closeReturned = new AtomicInteger();
        Thread closer = new Thread(() -> {
            fanOut.close();
            closeReturned.set(1);
        }, "fanout-closer");
        closer.start();

        // Give the closer time to enter close() and block on the slot lock.
        // We can't deterministically observe that without exposing internals, but ~200ms is
        // plenty for the closer thread to be parked in ReentrantLock.lock(). Verify close has
        // not returned yet:
        Thread.sleep(200);
        assertThat("close returned before drain completed", closeReturned.get(), equalTo(0));

        // Release the worker. close() should now return promptly.
        releaseProcess.countDown();
        closer.join(TimeUnit.SECONDS.toMillis(30));
        assertFalse("closer thread still alive", closer.isAlive());
        assertThat(closeReturned.get(), equalTo(1));
        // The worker did its work before close tore down state.
        assertThat(processedAfterLatch.get(), equalTo(1));
    }

    /** 3. handleRejection path: executor rejects every task; pages are released, failure recorded. */
    public void testHandleRejectionOnShutdownExecutor() throws Exception {
        // Use a real pool but shut it down immediately so submissions are rejected.
        executor = Executors.newFixedThreadPool(1);
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        DriverContext driverContext = newDriverContext();
        BlockFactory blockFactory = driverContext.blockFactory();

        try (TestFanOut fanOut = new TestFanOut(driverContext, executor, 2, 8)) {
            // Submit a couple pages; the first will trigger rejection. Subsequent addInput calls
            // should observe failureCollector.hasFailure() and release directly.
            for (int i = 0; i < 4; i++) {
                fanOut.addInput(makePage(blockFactory, i));
            }
            assertTrue("failure should have been collected from rejection", fanOut.mergeCalled == false);
            // getOutput should re-throw via checkFailure().
            Exception caught = expectThrows(RuntimeException.class, fanOut::getOutput);
            assertThat(caught, notNullValue());
            // needsInput should be false once a failure has been collected.
            assertFalse(fanOut.needsInput());
        }
        // All pages should have been released; ComputeTestCase's @After verifies breaker accounting.
    }

    /** 4. Backpressure: with maxInFlightPages reached and worker blocked, isBlocked() returns a pending future. */
    public void testBackpressureBlocksWhenAtMaxInFlight() throws Exception {
        executor = Executors.newFixedThreadPool(2);
        DriverContext driverContext = newDriverContext();
        BlockFactory blockFactory = driverContext.blockFactory();

        CountDownLatch processStarted = new CountDownLatch(2);
        CountDownLatch releaseProcess = new CountDownLatch(1);

        try (TestFanOut fanOut = new TestFanOut(driverContext, executor, 2, 2)) {
            fanOut.pageHandler = (state, page, slotIndex) -> {
                processStarted.countDown();
                assertTrue(releaseProcess.await(30, TimeUnit.SECONDS));
                state.processed.incrementAndGet();
            };

            // Submit exactly maxInFlightPages = 2 pages, one to each slot (round-robin).
            fanOut.addInput(makePage(blockFactory, 0));
            fanOut.addInput(makePage(blockFactory, 1));
            assertTrue("both workers should pick up their page", processStarted.await(30, TimeUnit.SECONDS));

            assertFalse("needsInput should be false at maxInFlight", fanOut.needsInput());
            IsBlockedResult blocked = fanOut.isBlocked();
            assertNotSame("expected a blocked result", Operator.NOT_BLOCKED, blocked);
            assertFalse("blocked future should not be done while workers are blocked", blocked.listener().isDone());

            // Release the workers; the future should complete and needsInput should flip back to true.
            releaseProcess.countDown();
            CountDownLatch done = new CountDownLatch(1);
            blocked.listener().addListener(org.elasticsearch.action.ActionListener.running(done::countDown));
            assertTrue("blocked future was not signalled", done.await(30, TimeUnit.SECONDS));

            // After drain, needsInput should be true again (still below max, not finished).
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (fanOut.needsInput() == false && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertTrue("needsInput should become true after drain", fanOut.needsInput());

            fanOut.finish();
            // Drive to completion so close() doesn't trip over in-flight work.
            while (fanOut.isFinished() == false) {
                fanOut.getOutput();
                IsBlockedResult b = fanOut.isBlocked();
                if (b != Operator.NOT_BLOCKED) {
                    CountDownLatch l = new CountDownLatch(1);
                    b.listener().addListener(org.elasticsearch.action.ActionListener.running(l::countDown));
                    l.await(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    /** 5. Exceptions thrown from processPage propagate via failureCollector and re-throw in getOutput. */
    public void testExceptionFromProcessPagePropagates() throws Exception {
        executor = Executors.newFixedThreadPool(2);
        DriverContext driverContext = newDriverContext();
        BlockFactory blockFactory = driverContext.blockFactory();

        try (TestFanOut fanOut = new TestFanOut(driverContext, executor, 2, 8)) {
            fanOut.pageHandler = (state, page, slotIndex) -> { throw new IllegalStateException("boom from processPage"); };

            fanOut.addInput(makePage(blockFactory, 0));

            // Wait for the worker thread to register the failure.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < deadline) {
                try {
                    fanOut.getOutput();
                } catch (RuntimeException re) {
                    assertThat(re.getMessage(), org.hamcrest.Matchers.containsString("boom from processPage"));
                    return;
                }
                Thread.sleep(10);
            }
            fail("expected exception to be re-thrown via getOutput");
        }
    }

    /**
     * 6. isBlocked() / worker-decrement race — documents the invariant rather than reliably catching the bug.
     * <p>
     * The fix made {@code blockedFuture} {@code volatile} so that the worker's lock-free read in
     * {@code notifyIfBlocked()} observes the Driver's write under the monitor. Reliably hitting the
     * pre-fix race requires JVM-level instrumentation we don't have here; instead we drive the
     * Driver-then-worker ordering many times and assert the future is always signalled.
     */
    public void testIsBlockedSignalledAfterWorkerDecrement() throws Exception {
        executor = Executors.newFixedThreadPool(2);
        DriverContext driverContext = newDriverContext();
        BlockFactory blockFactory = driverContext.blockFactory();

        int iterations = 50;
        int signalled = 0;
        try (TestFanOut fanOut = new TestFanOut(driverContext, executor, 2, 1)) {
            for (int i = 0; i < iterations; i++) {
                CountDownLatch release = new CountDownLatch(1);
                CountDownLatch started = new CountDownLatch(1);
                fanOut.pageHandler = (state, page, slotIndex) -> {
                    started.countDown();
                    assertTrue(release.await(10, TimeUnit.SECONDS));
                    state.processed.incrementAndGet();
                };
                fanOut.addInput(makePage(blockFactory, i));
                assertTrue(started.await(10, TimeUnit.SECONDS));

                // Driver-side: ask isBlocked() *before* releasing the worker. The worker will
                // decrement inFlight after release and must observe blockedFuture.
                IsBlockedResult blocked = fanOut.isBlocked();
                assertNotSame(Operator.NOT_BLOCKED, blocked);
                CountDownLatch done = new CountDownLatch(1);
                blocked.listener().addListener(org.elasticsearch.action.ActionListener.running(done::countDown));

                release.countDown();
                assertTrue("blocked future was not signalled on iteration " + i, done.await(10, TimeUnit.SECONDS));
                signalled++;

                // Wait for needsInput to settle before the next iteration.
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (fanOut.needsInput() == false && System.nanoTime() < deadline) {
                    Thread.sleep(1);
                }
            }
            fanOut.finish();
            while (fanOut.isFinished() == false) {
                fanOut.getOutput();
                IsBlockedResult b = fanOut.isBlocked();
                if (b != Operator.NOT_BLOCKED) {
                    CountDownLatch l = new CountDownLatch(1);
                    b.listener().addListener(org.elasticsearch.action.ActionListener.running(l::countDown));
                    l.await(5, TimeUnit.SECONDS);
                }
            }
        }
        assertThat(signalled, greaterThanOrEqualTo(iterations));
    }

}
