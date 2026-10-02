/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class FillCacheMemoryPressureTests extends ESTestCase {

    /** Completes deferred grants on the releasing thread for deterministic FIFO drain; prod passes the acquirer's pool. */
    private static final Executor INLINE_GRANTS = Runnable::run;

    private TestThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    private FillCacheMemoryPressure pressureWithLimit(long limitBytes) {
        return new FillCacheMemoryPressure(
            Settings.builder().put(FillCacheMemoryPressure.FILL_BYTES_LIMIT.getKey(), ByteSizeValue.ofBytes(limitBytes)).build(),
            MeterRegistry.NOOP,
            threadPool
        );
    }

    private static ActionListener<Releasable> collectTo(List<Releasable> granted) {
        return ActionListener.wrap(granted::add, e -> fail(e, "unexpected failure"));
    }

    public void testGrantsImmediatelyWithinLimit() {
        var pressure = pressureWithLimit(100);
        List<Releasable> granted = new ArrayList<>();
        pressure.acquire(60, INLINE_GRANTS, collectTo(granted));
        pressure.acquire(40, INLINE_GRANTS, collectTo(granted));
        assertThat(granted, hasSize(2));
        assertThat(pressure.getCurrentBytes(), equalTo(100L));
        assertThat(pressure.getWaiterCount(), equalTo(0));
        granted.forEach(Releasable::close);
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
    }

    public void testQueuesWhenOverLimitAndDrainsFifoOnRelease() {
        var pressure = pressureWithLimit(100);
        List<Releasable> granted = new ArrayList<>();
        pressure.acquire(80, INLINE_GRANTS, collectTo(granted));
        assertThat(granted, hasSize(1));

        List<String> grantOrder = new CopyOnWriteArrayList<>();
        List<Releasable> queuedGrants = new CopyOnWriteArrayList<>();
        pressure.acquire(50, INLINE_GRANTS, ActionListener.wrap(r -> {
            grantOrder.add("first");
            queuedGrants.add(r);
        }, e -> fail(e, "unexpected failure")));
        pressure.acquire(30, INLINE_GRANTS, ActionListener.wrap(r -> {
            grantOrder.add("second");
            queuedGrants.add(r);
        }, e -> fail(e, "unexpected failure")));
        assertThat(grantOrder, empty());
        assertThat(pressure.getWaiterCount(), equalTo(2));

        // the 30-byte waiter would fit alongside the 80 in flight, but must not jump the 50-byte head
        granted.get(0).close();
        assertThat(grantOrder, contains("first", "second"));
        assertThat(pressure.getWaiterCount(), equalTo(0));
        assertThat(pressure.getCurrentBytes(), equalTo(80L));
        queuedGrants.forEach(Releasable::close);
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
    }

    public void testLaterAcquirersQueueBehindExistingWaiters() {
        var pressure = pressureWithLimit(100);
        List<Releasable> granted = new ArrayList<>();
        pressure.acquire(90, INLINE_GRANTS, collectTo(granted));
        List<Releasable> queuedGrants = new CopyOnWriteArrayList<>();
        pressure.acquire(50, INLINE_GRANTS, collectTo(queuedGrants));
        // 5 bytes would fit now, but granting it would starve the 50-byte head
        pressure.acquire(5, INLINE_GRANTS, collectTo(queuedGrants));
        assertThat(queuedGrants, empty());
        assertThat(pressure.getWaiterCount(), equalTo(2));
        granted.get(0).close();
        assertThat(queuedGrants, hasSize(2));
        queuedGrants.forEach(Releasable::close);
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
    }

    public void testOversizedRequestGrantedWhenNothingInFlight() {
        var pressure = pressureWithLimit(100);
        List<Releasable> granted = new ArrayList<>();
        // oversized: granted immediately because nothing is in flight
        pressure.acquire(500, INLINE_GRANTS, collectTo(granted));
        assertThat(granted, hasSize(1));
        assertThat(pressure.getCurrentBytes(), equalTo(500L));

        // everything else waits until it completes
        List<Releasable> queuedGrants = new CopyOnWriteArrayList<>();
        pressure.acquire(10, INLINE_GRANTS, collectTo(queuedGrants));
        assertThat(queuedGrants, empty());
        granted.get(0).close();
        assertThat(queuedGrants, hasSize(1));
        queuedGrants.forEach(Releasable::close);
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
    }

    public void testOversizedWaiterGrantedOnceInFlightDrains() {
        var pressure = pressureWithLimit(100);
        List<Releasable> granted = new ArrayList<>();
        pressure.acquire(60, INLINE_GRANTS, collectTo(granted));
        List<Releasable> queuedGrants = new CopyOnWriteArrayList<>();
        pressure.acquire(500, INLINE_GRANTS, collectTo(queuedGrants));
        assertThat(queuedGrants, empty());
        granted.get(0).close();
        assertThat(queuedGrants, hasSize(1));
        assertThat(pressure.getCurrentBytes(), equalTo(500L));
        queuedGrants.forEach(Releasable::close);
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
    }

    public void testDeferredGrantCompletesOnSuppliedExecutor() {
        var pressure = pressureWithLimit(100);
        List<Releasable> granted = new ArrayList<>();
        pressure.acquire(80, INLINE_GRANTS, collectTo(granted));

        List<Runnable> deferredGrants = new ArrayList<>();
        List<Releasable> queuedGrants = new CopyOnWriteArrayList<>();
        pressure.acquire(50, deferredGrants::add, collectTo(queuedGrants));
        assertThat(pressure.getWaiterCount(), equalTo(1));

        // release charges the budget and hands the grant to the waiter's executor — the listener must not complete before it runs
        granted.get(0).close();
        assertThat(deferredGrants, hasSize(1));
        assertThat(queuedGrants, empty());
        assertThat(pressure.getCurrentBytes(), equalTo(50L));
        deferredGrants.get(0).run();
        assertThat(queuedGrants, hasSize(1));
        queuedGrants.forEach(Releasable::close);
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
    }

    public void testRejectedGrantReturnsBudgetAndFailsWaiter() {
        var pressure = pressureWithLimit(100);
        List<Releasable> granted = new ArrayList<>();
        pressure.acquire(100, INLINE_GRANTS, collectTo(granted));

        AtomicReference<Exception> failure = new AtomicReference<>();
        pressure.acquire(60, r -> { throw new EsRejectedExecutionException("simulated rejection", true); }, ActionListener.wrap(r -> {
            fail("must not be granted, the executor rejected the grant");
        }, failure::set));
        // a second waiter with a working executor must still be served by the budget the rejected waiter returned
        List<Releasable> queuedGrants = new CopyOnWriteArrayList<>();
        pressure.acquire(40, INLINE_GRANTS, collectTo(queuedGrants));
        assertThat(pressure.getWaiterCount(), equalTo(2));

        granted.get(0).close();
        assertThat(failure.get(), instanceOf(EsRejectedExecutionException.class));
        assertThat(queuedGrants, hasSize(1));
        assertThat(pressure.getCurrentBytes(), equalTo(40L));
        queuedGrants.forEach(Releasable::close);
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
        assertThat(pressure.getWaiterCount(), equalTo(0));
    }

    public void testReclaimedBytesFromRejectedGrantFundWaiterThatDidNotFitInitially() {
        var pressure = pressureWithLimit(100);
        List<Releasable> granted = new ArrayList<>();
        pressure.acquire(100, INLINE_GRANTS, collectTo(granted));

        // both waiters are queued; on release only the head fits, so the second is granted solely by the
        // bytes reclaimed when the head's executor rejects the grant
        AtomicReference<Exception> failure = new AtomicReference<>();
        pressure.acquire(80, r -> { throw new EsRejectedExecutionException("simulated rejection", true); }, ActionListener.wrap(r -> {
            fail("must not be granted, the executor rejected the grant");
        }, failure::set));
        List<Releasable> queuedGrants = new CopyOnWriteArrayList<>();
        pressure.acquire(80, INLINE_GRANTS, collectTo(queuedGrants));
        assertThat(pressure.getWaiterCount(), equalTo(2));

        granted.get(0).close();
        assertThat(failure.get(), instanceOf(EsRejectedExecutionException.class));
        assertThat(queuedGrants, hasSize(1));
        assertThat(pressure.getCurrentBytes(), equalTo(80L));
        assertThat(pressure.getWaiterCount(), equalTo(0));
        queuedGrants.forEach(Releasable::close);
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
    }

    public void testThrowingOnFailureDoesNotStrandSubsequentWaiters() {
        var pressure = pressureWithLimit(100);
        List<Releasable> granted = new ArrayList<>();
        pressure.acquire(100, INLINE_GRANTS, collectTo(granted));

        // first queued waiter: executor rejects, then its onFailure throws directly.
        // uses a raw ActionListener (not ActionListener.wrap) so the RuntimeException surfaces at the pressure's
        // fanout boundary rather than being converted to AssertionError by ActionListenerImplementations.expectNoException.
        pressure.acquire(30, r -> { throw new EsRejectedExecutionException("simulated rejection", true); }, new ActionListener<>() {
            @Override
            public void onResponse(Releasable r) {
                fail("must not be granted, the executor rejected the grant");
            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException("simulated onFailure failure");
            }
        });
        // second queued waiter: normal — must still be granted with the budget the first returned
        List<Releasable> queuedGrants = new CopyOnWriteArrayList<>();
        pressure.acquire(40, INLINE_GRANTS, collectTo(queuedGrants));
        assertThat(pressure.getWaiterCount(), equalTo(2));

        // matches ES's ActionListener.onFailure(Iterable, Exception): the loop finishes all remaining waiters first,
        // then rethrows the collected listener failure as a RuntimeException. The second waiter must be granted before
        // the exception surfaces.
        var thrown = expectThrows(RuntimeException.class, () -> granted.get(0).close());
        assertThat(thrown.getMessage(), containsString("simulated onFailure failure"));

        assertThat(queuedGrants, hasSize(1));
        assertThat(pressure.getCurrentBytes(), equalTo(40L));
        queuedGrants.forEach(Releasable::close);
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
        assertThat(pressure.getWaiterCount(), equalTo(0));
    }

    public void testWarnsWhenQueueHeadIsStalled() throws Exception {
        var pressure = new FillCacheMemoryPressure(
            Settings.builder()
                .put(FillCacheMemoryPressure.FILL_BYTES_LIMIT.getKey(), ByteSizeValue.ofBytes(100))
                .put(FillCacheMemoryPressure.STALL_WARN_THRESHOLD.getKey(), TimeValue.timeValueMillis(50))
                .build(),
            MeterRegistry.NOOP,
            threadPool
        );
        List<Releasable> granted = new ArrayList<>();
        pressure.acquire(100, INLINE_GRANTS, collectTo(granted));
        List<Releasable> queuedGrants = new CopyOnWriteArrayList<>();
        pressure.acquire(10, INLINE_GRANTS, collectTo(queuedGrants));

        try (var mockLog = MockLog.capture(FillCacheMemoryPressure.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "stall warning",
                    FillCacheMemoryPressure.class.getCanonicalName(),
                    Level.WARN,
                    "cache-fill memory budget stalled*"
                )
            );
            assertBusy(mockLog::assertAllExpectationsMatched);
        }

        // draining the in-flight read unblocks the waiter and disarms the stall check
        granted.get(0).close();
        assertThat(queuedGrants, hasSize(1));
        queuedGrants.forEach(Releasable::close);
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
        assertThat(pressure.getWaiterCount(), equalTo(0));
    }

    public void testRandomizedAcquireReleaseNeverExceedsLimitAndFullyDrains() {
        final long limit = randomLongBetween(100, 1000);
        var pressure = pressureWithLimit(limit);
        List<Releasable> outstanding = new CopyOnWriteArrayList<>();
        int acquires = randomIntBetween(50, 200);
        for (int i = 0; i < acquires; i++) {
            long bytes = randomLongBetween(1, limit / 2);
            pressure.acquire(bytes, INLINE_GRANTS, ActionListener.wrap(outstanding::add, e -> fail(e, "unexpected failure")));
            assertThat("in-flight bytes exceed limit", pressure.getCurrentBytes(), lessThanOrEqualTo(limit));
            if (outstanding.isEmpty() == false && randomBoolean()) {
                outstanding.remove(randomIntBetween(0, outstanding.size() - 1)).close();
                assertThat("in-flight bytes exceed limit", pressure.getCurrentBytes(), lessThanOrEqualTo(limit));
            }
        }
        // closing a grant can synchronously grant a waiter, which appends to the list; loop until truly drained
        while (outstanding.isEmpty() == false) {
            outstanding.remove(0).close();
        }
        assertThat(pressure.getWaiterCount(), equalTo(0));
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
    }

    /**
     * Contended acquire/release across threads, with inline grant delivery so a release can synchronously grant queued waiters on
     * the releasing thread — the most adversarial reentrancy shape for {@link FillCacheMemoryPressure#release}. Verifies the limit
     * is never exceeded, no bytes are lost to racing grants, and no waiter is stranded.
     */
    public void testConcurrentAcquireReleaseNeverExceedsLimitAndFullyDrains() {
        final long limit = randomLongBetween(100, 1000);
        var pressure = pressureWithLimit(limit);
        final int threads = between(4, 8);
        final int opsPerThread = between(200, 500);
        final int totalAcquires = threads * opsPerThread;

        // sizes precomputed here: ESTestCase randomness must not be used from the spawned threads.
        // capped at limit / 2 so the oversized-when-idle rule never fires and the <= limit invariant is unconditional
        final long[][] acquireSizes = new long[threads][opsPerThread];
        for (int t = 0; t < threads; t++) {
            for (int i = 0; i < opsPerThread; i++) {
                acquireSizes[t][i] = randomLongBetween(1, limit / 2);
            }
        }

        final Queue<Releasable> outstanding = new ConcurrentLinkedQueue<>();
        final AtomicInteger grantCount = new AtomicInteger();
        startInParallel(threads, t -> {
            for (int i = 0; i < opsPerThread; i++) {
                pressure.acquire(acquireSizes[t][i], INLINE_GRANTS, ActionListener.wrap(r -> {
                    grantCount.incrementAndGet();
                    outstanding.add(r);
                }, e -> fail(e, "unexpected failure")));
                assertThat("in-flight bytes exceed limit", pressure.getCurrentBytes(), lessThanOrEqualTo(limit));
                // best-effort release of any grant (own or another thread's) to keep the budget churning under contention
                Releasable release = outstanding.poll();
                if (release != null) {
                    release.close();
                }
            }
        });

        // grants deliver inline within close(), so once the threads have joined, every still-queued waiter is only ever granted
        // from this loop's closes; when the queue stays empty the ledger must balance exactly
        Releasable release;
        while ((release = outstanding.poll()) != null) {
            release.close();
            assertThat("in-flight bytes exceed limit", pressure.getCurrentBytes(), lessThanOrEqualTo(limit));
        }
        assertThat(grantCount.get(), equalTo(totalAcquires));
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
        assertThat(pressure.getWaiterCount(), equalTo(0));
    }
}
