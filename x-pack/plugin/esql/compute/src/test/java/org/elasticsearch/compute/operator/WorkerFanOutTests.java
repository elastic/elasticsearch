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

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/** Exercises {@link WorkerFanOut}'s concurrent coordination paths directly. */
public class WorkerFanOutTests extends ComputeTestCase {

    private ExecutorService executor;

    @After
    public void shutdownExecutor() throws InterruptedException {
        if (executor != null && executor.isShutdown() == false) {
            executor.shutdownNow();
            assertTrue("executor did not terminate", executor.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

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
            int n = nextWorker.getAndIncrement();
            int workerCount = getWorkerCountForTest();
            return Math.floorMod(n, workerCount);
        }

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

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (fanOut.canProduceMoreDataWithoutExtraInput() == false || fanOut.isFinished() == false) {
                IsBlockedResult blocked = fanOut.isBlocked();
                if (blocked != Operator.NOT_BLOCKED) {
                    final CountDownLatch latch = new CountDownLatch(1);
                    blocked.listener().addListener(org.elasticsearch.action.ActionListener.running(latch::countDown));
                    latch.await(5, TimeUnit.SECONDS);
                }
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
            assertThat(fanOut.mergedStates.get(0).processed.get(), equalTo(pageCount / 2));
            assertThat(fanOut.mergedStates.get(1).processed.get(), equalTo(pageCount / 2));
        }
    }

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

        // ~200ms is enough for the closer to park in ReentrantLock.lock(); we can't observe that directly.
        Thread.sleep(200);
        assertThat("close returned before drain completed", closeReturned.get(), equalTo(0));

        releaseProcess.countDown();
        closer.join(TimeUnit.SECONDS.toMillis(30));
        assertFalse("closer thread still alive", closer.isAlive());
        assertThat(closeReturned.get(), equalTo(1));
        assertThat(processedAfterLatch.get(), equalTo(1));
    }

    public void testHandleRejectionOnShutdownExecutor() throws Exception {
        executor = Executors.newFixedThreadPool(1);
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        DriverContext driverContext = newDriverContext();
        BlockFactory blockFactory = driverContext.blockFactory();

        try (TestFanOut fanOut = new TestFanOut(driverContext, executor, 2, 8)) {
            for (int i = 0; i < 4; i++) {
                fanOut.addInput(makePage(blockFactory, i));
            }
            assertTrue("failure should have been collected from rejection", fanOut.mergeCalled == false);
            Exception caught = expectThrows(RuntimeException.class, fanOut::getOutput);
            assertThat(caught, notNullValue());
            assertFalse(fanOut.needsInput());
        }
    }

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

            fanOut.addInput(makePage(blockFactory, 0));
            fanOut.addInput(makePage(blockFactory, 1));
            assertTrue("both workers should pick up their page", processStarted.await(30, TimeUnit.SECONDS));

            assertFalse("needsInput should be false at maxInFlight", fanOut.needsInput());
            IsBlockedResult blocked = fanOut.isBlocked();
            assertNotSame("expected a blocked result", Operator.NOT_BLOCKED, blocked);
            assertFalse("blocked future should not be done while workers are blocked", blocked.listener().isDone());

            releaseProcess.countDown();
            CountDownLatch done = new CountDownLatch(1);
            blocked.listener().addListener(org.elasticsearch.action.ActionListener.running(done::countDown));
            assertTrue("blocked future was not signalled", done.await(30, TimeUnit.SECONDS));

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (fanOut.needsInput() == false && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertTrue("needsInput should become true after drain", fanOut.needsInput());

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
    }

    public void testExceptionFromProcessPagePropagates() throws Exception {
        executor = Executors.newFixedThreadPool(2);
        DriverContext driverContext = newDriverContext();
        BlockFactory blockFactory = driverContext.blockFactory();

        try (TestFanOut fanOut = new TestFanOut(driverContext, executor, 2, 8)) {
            fanOut.pageHandler = (state, page, slotIndex) -> { throw new IllegalStateException("boom from processPage"); };

            fanOut.addInput(makePage(blockFactory, 0));

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

    // Exercises the volatile-blockedFuture invariant: Driver asks isBlocked() before releasing the worker,
    // and the worker's lock-free read in notifyIfBlocked() must observe the write.
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

                IsBlockedResult blocked = fanOut.isBlocked();
                assertNotSame(Operator.NOT_BLOCKED, blocked);
                CountDownLatch done = new CountDownLatch(1);
                blocked.listener().addListener(org.elasticsearch.action.ActionListener.running(done::countDown));

                release.countDown();
                assertTrue("blocked future was not signalled on iteration " + i, done.await(10, TimeUnit.SECONDS));
                signalled++;

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

    public void testMapStates() throws Exception {
        executor = Executors.newFixedThreadPool(2);
        DriverContext driverContext = newDriverContext();
        BlockFactory blockFactory = driverContext.blockFactory();

        try (TestFanOut fanOut = new TestFanOut(driverContext, executor, 2, 8)) {
            // Slot order is preserved: 6 pages round-robin → 3 each.
            for (int i = 0; i < 6; i++) {
                fanOut.addInput(makePage(blockFactory, i));
            }
            awaitProcessed(fanOut, 6);

            List<Integer> counts = fanOut.mapStates(s -> s.processed.get());
            assertThat(counts.size(), equalTo(2));
            assertThat(counts.get(0), equalTo(3));
            assertThat(counts.get(1), equalTo(3));

            // Concurrent-write safety: park a drain on a latch, hammer mapStates from this thread,
            // assert it never throws and returns plausible values.
            CountDownLatch release = new CountDownLatch(1);
            CountDownLatch started = new CountDownLatch(1);
            fanOut.pageHandler = (state, page, slotIndex) -> {
                started.countDown();
                assertTrue(release.await(10, TimeUnit.SECONDS));
                state.processed.incrementAndGet();
            };
            fanOut.addInput(makePage(blockFactory, 99));
            assertTrue(started.await(10, TimeUnit.SECONDS));
            for (int i = 0; i < 100; i++) {
                List<Integer> snap = fanOut.mapStates(s -> s.processed.get());
                assertThat(snap.size(), equalTo(2));
                for (Integer c : snap) {
                    assertThat(c, greaterThanOrEqualTo(3));
                }
            }
            release.countDown();
            awaitProcessed(fanOut, 7);

            // close() nulls slot.state, so mapStates returns an empty list.
            fanOut.close();
            assertThat(fanOut.mapStates(s -> s.processed.get()), empty());
        }
    }

    private void awaitProcessed(TestFanOut fanOut, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            int total = fanOut.mapStates(s -> s.processed.get()).stream().mapToInt(Integer::intValue).sum();
            if (total >= expected) {
                return;
            }
            Thread.sleep(1);
        }
        fail("workers did not process " + expected + " pages in time");
    }

}
