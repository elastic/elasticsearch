/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.tasks.TaskId.EMPTY_TASK_ID;
import static org.hamcrest.Matchers.equalTo;

/**
 * Concurrency tests for the {@link AbstractGeoIpDownloader} state machine.
 * <p>
 * These tests deliberately avoid going through {@link GeoIpDownloader}'s I/O surface so that the
 * state-machine invariants (drain contract, request coalescing, exception handling, re-entrancy)
 * are pinned in isolation. The fixture {@link TestableDownloader} provides controllable hooks for
 * {@code runDownloader} and counters for {@link AbstractGeoIpDownloader#markAsCompleted()} so each
 * test can assert the relevant invariant directly.
 */
public class AbstractGeoIpDownloaderTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setupThreadPool() {
        threadPool = new ThreadPool(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), getTestName()).build(),
            MeterRegistry.NOOP,
            new DefaultBuiltInExecutorBuilders()
        );
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    /**
     * Minimal {@link AbstractGeoIpDownloader} subclass that records every {@code runDownloader}
     * invocation and every {@code markAsCompleted} call, and exposes hooks ({@link #onEnter}) so
     * tests can pause or fail individual runs.
     * <p>
     * {@code markAsCompleted} is overridden to never call {@code super}: the persistent-tasks
     * framework state is not initialized here, and the tests pin the <i>timing</i> of completion
     * rather than the framework's notification.
     */
    @SuppressWarnings("NewClassNamingConvention")
    private static class TestableDownloader extends AbstractGeoIpDownloader {

        final AtomicInteger runDownloaderCalls = new AtomicInteger();
        final AtomicInteger completionCalls = new AtomicInteger();
        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger maxObservedConcurrentRuns = new AtomicInteger();
        /** Set if {@code markAsCompleted} ever fires while {@link #inFlight} is non-zero. */
        final AtomicReference<AssertionError> drainContractViolation = new AtomicReference<>();

        volatile Runnable onEnter;

        TestableDownloader(ThreadPool threadPool) {
            super(0, "test", "test", "test", EMPTY_TASK_ID, Map.of(), threadPool, () -> TimeValue.timeValueDays(1));
            // null PersistentTasksService and TaskManager: the overridden markAsCompleted never delegates to super.
            init(null, null, "test-task", 0);
        }

        @Override
        void runDownloader() {
            int concurrent = inFlight.incrementAndGet();
            try {
                runDownloaderCalls.incrementAndGet();
                int previousMax;
                do {
                    previousMax = maxObservedConcurrentRuns.get();
                    if (concurrent <= previousMax) break;
                } while (maxObservedConcurrentRuns.compareAndSet(previousMax, concurrent) == false);
                Runnable hook = onEnter;
                if (hook != null) {
                    hook.run();
                }
            } finally {
                inFlight.decrementAndGet();
            }
        }

        @Override
        public void markAsCompleted() {
            int observed = inFlight.get();
            if (observed != 0) {
                drainContractViolation.compareAndSet(null, new AssertionError("markAsCompleted fired with inFlight=" + observed));
            }
            completionCalls.incrementAndGet();
        }

        /** Test-only exposer for the {@code protected final} {@link #shouldKeepRunning()}. */
        boolean checkShouldKeepRunning() {
            return shouldKeepRunning();
        }
    }

    /**
     * While a single {@code runDownloader} call is held, many concurrent {@code requestRunOnDemand}
     * calls must coalesce into exactly one additional {@code runDownloader} invocation.
     */
    public void testCoalescingDuringInFlightRun() throws Exception {
        TestableDownloader downloader = new TestableDownloader(threadPool);
        CountDownLatch enteredFirst = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        AtomicBoolean keepRunningAtFirstEntry = new AtomicBoolean();
        downloader.onEnter = () -> {
            // Only the first run blocks; the coalesced second run returns immediately.
            if (downloader.runDownloaderCalls.get() == 1) {
                keepRunningAtFirstEntry.set(downloader.checkShouldKeepRunning());
                enteredFirst.countDown();
                safeAwait(releaseFirst);
            }
        };

        try {
            downloader.requestRunOnDemand();
            safeAwait(enteredFirst);
            assertTrue("shouldKeepRunning must be true while in RUNNING state", keepRunningAtFirstEntry.get());

            int piledUp = 50;
            for (int i = 0; i < piledUp; i++) {
                downloader.requestRunOnDemand();
            }
            assertTrue(
                "shouldKeepRunning must remain true when extra requests transition to RUNNING_AND_REQUESTED",
                downloader.checkShouldKeepRunning()
            );

            releaseFirst.countDown();

            assertBusy(() -> assertThat(downloader.runDownloaderCalls.get(), equalTo(2)));
            assertThat(downloader.completionCalls.get(), equalTo(0));
            assertThat(downloader.maxObservedConcurrentRuns.get(), equalTo(1));
            assertNull(downloader.drainContractViolation.get());
            assertBusy(
                () -> assertFalse(
                    "shouldKeepRunning must be false once the state settles back to IDLE",
                    downloader.checkShouldKeepRunning()
                )
            );
        } finally {
            releaseFirst.countDown();
        }
    }

    /**
     * Many concurrent {@code onCancelled} calls racing with many {@code requestRunOnDemand} calls
     * while a {@code runDownloader} is in flight must result in exactly one {@code markAsCompleted}
     * call, fired only after the in-flight run returns.
     */
    public void testDrainContractUnderConcurrentCancellationAndRequests() throws Exception {
        TestableDownloader downloader = new TestableDownloader(threadPool);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        downloader.onEnter = () -> {
            entered.countDown();
            safeAwait(release);
        };

        try {
            downloader.requestRunOnDemand();
            safeAwait(entered);

            int actors = 16;
            CyclicBarrier barrier = new CyclicBarrier(actors);
            CountDownLatch done = new CountDownLatch(actors);
            for (int i = 0; i < actors; i++) {
                final int idx = i;
                new Thread(() -> {
                    try {
                        safeAwait(barrier);
                        if (idx % 2 == 0) {
                            downloader.onCancelled();
                        } else {
                            downloader.requestRunOnDemand();
                        }
                    } finally {
                        done.countDown();
                    }
                }).start();
            }
            safeAwait(done);

            assertThat("markAsCompleted must NOT fire while runDownloader is in flight", downloader.completionCalls.get(), equalTo(0));

            release.countDown();

            assertBusy(() -> assertThat(downloader.completionCalls.get(), equalTo(1)));
            assertThat("only the original run should have executed", downloader.runDownloaderCalls.get(), equalTo(1));
            assertThat(downloader.maxObservedConcurrentRuns.get(), equalTo(1));
            assertNull("drain contract must not be violated", downloader.drainContractViolation.get());
            assertFalse("shouldKeepRunning must be false in the terminal COMPLETED state", downloader.checkShouldKeepRunning());
        } finally {
            release.countDown();
        }
    }

    /**
     * {@code onCancelled} from the {@code IDLE} state fires {@code markAsCompleted} immediately
     * without ever entering {@code runDownloader}.
     */
    public void testCancellationBeforeAnyRunFiresCompletionImmediately() {
        TestableDownloader downloader = new TestableDownloader(threadPool);
        downloader.onCancelled();
        assertThat(downloader.completionCalls.get(), equalTo(1));
        assertThat(downloader.runDownloaderCalls.get(), equalTo(0));
        assertNull(downloader.drainContractViolation.get());
        assertFalse("shouldKeepRunning must be false in the terminal COMPLETED state", downloader.checkShouldKeepRunning());
    }

    /**
     * After cancellation, further {@code requestRunOnDemand} calls are no-ops (the {@code COMPLETED}
     * state is terminal).
     */
    public void testRequestAfterCancellationIsNoOp() {
        TestableDownloader downloader = new TestableDownloader(threadPool);
        downloader.onCancelled();
        assertFalse("shouldKeepRunning must be false immediately after cancellation", downloader.checkShouldKeepRunning());
        for (int i = 0; i < 10; i++) {
            downloader.requestRunOnDemand();
        }
        assertThat(downloader.runDownloaderCalls.get(), equalTo(0));
        assertThat(downloader.completionCalls.get(), equalTo(1));
        assertFalse("shouldKeepRunning must remain false even after follow-up requests", downloader.checkShouldKeepRunning());
    }

    /**
     * If {@code runDownloader} throws, the state machine must transition back to {@code IDLE} so
     * that a subsequent {@code requestRunOnDemand} can still trigger a fresh run.
     */
    public void testExceptionRecoversForFutureRuns() throws Exception {
        TestableDownloader downloader = new TestableDownloader(threadPool);
        CountDownLatch firstCallRecorded = new CountDownLatch(1);
        CountDownLatch secondCallEntered = new CountDownLatch(1);
        AtomicBoolean keepRunningOnFirstCall = new AtomicBoolean();
        AtomicBoolean keepRunningOnSecondCall = new AtomicBoolean();
        downloader.onEnter = () -> {
            if (downloader.runDownloaderCalls.get() == 1) {
                keepRunningOnFirstCall.set(downloader.checkShouldKeepRunning());
                // Signal AFTER the write, so the test thread synchronizes on the hook output, not on
                // runDownloaderCalls (which the fixture increments before invoking the hook).
                firstCallRecorded.countDown();
                throw new RuntimeException("synthetic failure on first run");
            }
            keepRunningOnSecondCall.set(downloader.checkShouldKeepRunning());
            secondCallEntered.countDown();
        };

        downloader.requestRunOnDemand();
        safeAwait(firstCallRecorded);
        assertTrue("shouldKeepRunning must have been true while the throwing run executed", keepRunningOnFirstCall.get());
        assertBusy(
            () -> assertFalse(
                "shouldKeepRunning must be false after the state machine recovers to IDLE",
                downloader.checkShouldKeepRunning()
            )
        );

        downloader.requestRunOnDemand();
        safeAwait(secondCallEntered);
        assertTrue("shouldKeepRunning must be true while the recovered run executes", keepRunningOnSecondCall.get());
        assertBusy(() -> assertThat(downloader.runDownloaderCalls.get(), equalTo(2)));
        assertThat(downloader.completionCalls.get(), equalTo(0));
        assertNull(downloader.drainContractViolation.get());
        assertBusy(() -> assertFalse("shouldKeepRunning must be false once the second run completes", downloader.checkShouldKeepRunning()));
    }

    /**
     * If {@code runDownloader} throws while a request is owed (state {@code RUNNING_AND_REQUESTED}),
     * the owed run must still execute on a freshly-submitted successor worker.
     */
    public void testExceptionWithOwedRequestStillRunsSuccessor() throws Exception {
        TestableDownloader downloader = new TestableDownloader(threadPool);
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        AtomicBoolean keepRunningAtFirstEntry = new AtomicBoolean();
        downloader.onEnter = () -> {
            if (downloader.runDownloaderCalls.get() == 1) {
                keepRunningAtFirstEntry.set(downloader.checkShouldKeepRunning());
                firstEntered.countDown();
                safeAwait(releaseFirst);
                throw new RuntimeException("synthetic failure on first run");
            }
        };

        try {
            downloader.requestRunOnDemand();
            safeAwait(firstEntered);
            assertTrue("shouldKeepRunning must be true while in RUNNING state", keepRunningAtFirstEntry.get());

            // Pile up an owed run while the first call is still in flight.
            downloader.requestRunOnDemand();
            assertTrue(
                "shouldKeepRunning must remain true after transitioning to RUNNING_AND_REQUESTED",
                downloader.checkShouldKeepRunning()
            );

            releaseFirst.countDown();

            assertBusy(() -> assertThat(downloader.runDownloaderCalls.get(), equalTo(2)));
            assertThat(
                "successor must run on a fresh worker; no concurrent runs allowed",
                downloader.maxObservedConcurrentRuns.get(),
                equalTo(1)
            );
            assertThat(downloader.completionCalls.get(), equalTo(0));
            assertNull(downloader.drainContractViolation.get());
            assertBusy(
                () -> assertFalse(
                    "shouldKeepRunning must be false once the successor finishes and state returns to IDLE",
                    downloader.checkShouldKeepRunning()
                )
            );
        } finally {
            releaseFirst.countDown();
        }
    }

    /**
     * Re-entrant {@code requestRunOnDemand} from inside {@code runDownloader} must coalesce into
     * exactly one extra run, with no nested concurrent execution.
     */
    public void testReentrantRequestFromInsideRunDownloader() throws Exception {
        TestableDownloader downloader = new TestableDownloader(threadPool);
        AtomicBoolean keepRunningBeforeReentry = new AtomicBoolean();
        AtomicBoolean keepRunningAfterReentry = new AtomicBoolean();
        downloader.onEnter = () -> {
            if (downloader.runDownloaderCalls.get() == 1) {
                keepRunningBeforeReentry.set(downloader.checkShouldKeepRunning());
                downloader.requestRunOnDemand();
                keepRunningAfterReentry.set(downloader.checkShouldKeepRunning());
            }
        };

        downloader.requestRunOnDemand();
        assertBusy(() -> assertThat(downloader.runDownloaderCalls.get(), equalTo(2)));
        assertThat(downloader.maxObservedConcurrentRuns.get(), equalTo(1));
        assertThat(downloader.completionCalls.get(), equalTo(0));
        assertNull(downloader.drainContractViolation.get());
        assertTrue("shouldKeepRunning must be true while in RUNNING state", keepRunningBeforeReentry.get());
        assertTrue(
            "shouldKeepRunning must remain true after a re-entrant request transitions to RUNNING_AND_REQUESTED",
            keepRunningAfterReentry.get()
        );
        assertBusy(
            () -> assertFalse(
                "shouldKeepRunning must be false once both runs finish and the state returns to IDLE",
                downloader.checkShouldKeepRunning()
            )
        );
    }

    /**
     * {@link AbstractGeoIpDownloader#shouldKeepRunning()} must return {@code true} while a
     * {@code runDownloader} call is in flight, and flip to {@code false} as soon as
     * {@link AbstractGeoIpDownloader#onCancelled()} has transitioned the state machine to
     * {@code DRAINING} — without the in-flight call having to return first.
     */
    public void testShouldKeepRunningFlipsOnCancellation() throws Exception {
        TestableDownloader downloader = new TestableDownloader(threadPool);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch cancellationDelivered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean beforeCancel = new AtomicBoolean();
        // Initialize to true so that a missing write would still fail the post-cancel assertion.
        AtomicBoolean afterCancel = new AtomicBoolean(true);

        downloader.onEnter = () -> {
            beforeCancel.set(downloader.shouldKeepRunning());
            entered.countDown();
            safeAwait(cancellationDelivered);
            afterCancel.set(downloader.shouldKeepRunning());
            safeAwait(release);
        };

        try {
            downloader.requestRunOnDemand();
            safeAwait(entered);
            assertTrue("shouldKeepRunning must be true while the run-state is RUNNING", beforeCancel.get());

            downloader.onCancelled();
            cancellationDelivered.countDown();
            release.countDown();

            assertBusy(() -> assertFalse("shouldKeepRunning must be false once the run-state is DRAINING", afterCancel.get()));
            assertBusy(() -> assertThat(downloader.completionCalls.get(), equalTo(1)));
            assertNull(downloader.drainContractViolation.get());
        } finally {
            cancellationDelivered.countDown();
            release.countDown();
        }
    }

}
