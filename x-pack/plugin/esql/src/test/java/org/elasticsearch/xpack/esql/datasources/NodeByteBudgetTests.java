/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.NodeByteBudget;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;

public class NodeByteBudgetTests extends ESTestCase {

    public void testNullLeaseOverCapFailsAtEnqueue() throws Exception {
        NodeByteBudgetService budget = new NodeByteBudgetService(10);
        CountDownLatch failed = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        budget.admitAsync(20, null, () -> false, Runnable::run).addListener(ActionListener.wrap(hold -> {
            fail("null-lease over-cap unit must not grant");
            hold.close();
        }, e -> {
            error.set(e);
            failed.countDown();
        }));
        assertTrue(failed.await(5, TimeUnit.SECONDS));
        assertThat(error.get(), instanceOf(EsRejectedExecutionException.class));
        assertEquals(0, budget.used());
        assertEquals(0, budget.waiterCount());
        NodeByteBudget.Hold small = budget.tryAdmit(4);
        assertNotNull(small);
        small.close();
    }

    public void testTryAdmitFitsAndRefusesOverCap() {
        NodeByteBudgetService budget = new NodeByteBudgetService(100);
        NodeByteBudget.Hold first = budget.tryAdmit(40);
        assertNotNull(first);
        assertEquals(40, budget.used());
        NodeByteBudget.Hold second = budget.tryAdmit(40);
        assertNotNull(second);
        assertNull("tryAdmit must not take the overshoot slot", budget.tryAdmit(30));
        assertEquals(80, budget.used());
        first.close();
        second.close();
        assertEquals(0, budget.used());
    }

    public void testTryAdmitZeroAlwaysSucceeds() {
        NodeByteBudgetService budget = new NodeByteBudgetService(1);
        budget.add(1);
        NodeByteBudget.Hold hold = budget.tryAdmit(0);
        assertNotNull(hold);
        assertEquals(1, budget.used());
        hold.close();
        assertEquals(1, budget.used());
    }

    public void testTryAdmitRefusesWhenWaitersQueued() throws Exception {
        NodeByteBudgetService budget = new NodeByteBudgetService(10);
        NodeByteBudget.Hold owner = occupyOvershoot(budget, 15);
        CountDownLatch granted = new CountDownLatch(1);
        AtomicReference<NodeByteBudget.Hold> waiterHold = new AtomicReference<>();
        budget.admitAsync(4, new RowGroupIo(), () -> false, Runnable::run).addListener(ActionListener.wrap(hold -> {
            waiterHold.set(hold);
            granted.countDown();
        }, e -> granted.countDown()));
        assertBusy(() -> assertEquals(1, budget.waiterCount()));
        assertNull(budget.tryAdmit(1));
        owner.close();
        budget.clearOwner(owner.lease());
        assertTrue(granted.await(5, TimeUnit.SECONDS));
        assertNotNull(waiterHold.get());
        waiterHold.get().close();
        assertEquals(0, budget.used());
        assertEquals(0, budget.waiterCount());
    }

    public void testUnitLargerThanCapUsesOvershootOnly() throws Exception {
        NodeByteBudgetService budget = new NodeByteBudgetService(50);
        assertNull(budget.tryAdmit(80));
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<NodeByteBudget.Hold> hold = new AtomicReference<>();
        RowGroupIo lease = new RowGroupIo();
        budget.admitAsync(80, lease, () -> false, Runnable::run).addListener(ActionListener.wrap(h -> {
            hold.set(h);
            done.countDown();
        }, e -> done.countDown()));
        assertTrue(done.await(5, TimeUnit.SECONDS));
        assertNotNull(hold.get());
        assertTrue(hold.get().isOvershoot());
        assertSame(lease, budget.overshootOwner());
        assertEquals(80, budget.used());
        assertEquals(80, budget.peakUsed());
        hold.get().close();
        assertEquals(0, budget.used());
    }

    public void testFifoGrantOnRelease() throws Exception {
        NodeByteBudgetService budget = new NodeByteBudgetService(10);
        NodeByteBudget.Hold blocking = occupyOvershoot(budget, 15);
        List<Integer> order = new CopyOnWriteArrayList<>();
        CountDownLatch allGranted = new CountDownLatch(3);
        for (int i = 0; i < 3; i++) {
            int id = i;
            budget.admitAsync(1, new RowGroupIo(), () -> false, Runnable::run).addListener(ActionListener.wrap(hold -> {
                order.add(id);
                hold.close();
                allGranted.countDown();
            }, e -> allGranted.countDown()));
        }
        assertBusy(() -> assertEquals(3, budget.waiterCount()));
        blocking.close();
        budget.clearOwner(blocking.lease());
        assertTrue(allGranted.await(5, TimeUnit.SECONDS));
        assertEquals(List.of(0, 1, 2), order);
        assertEquals(0, budget.used());
        assertEquals(0, budget.waiterCount());
    }

    public void testCancelQueuedWaiterReleasesGrantToNext() throws Exception {
        NodeByteBudgetService budget = new NodeByteBudgetService(10);
        NodeByteBudget.Hold blocking = occupyOvershoot(budget, 15);
        AtomicBoolean cancelFirst = new AtomicBoolean();
        CountDownLatch firstFailed = new CountDownLatch(1);
        CountDownLatch secondGranted = new CountDownLatch(1);
        AtomicReference<NodeByteBudget.Hold> secondHold = new AtomicReference<>();
        budget.admitAsync(4, new RowGroupIo(), cancelFirst::get, Runnable::run)
            .addListener(ActionListener.wrap(hold -> fail("cancelled waiter must not grant"), e -> firstFailed.countDown()));
        budget.admitAsync(4, new RowGroupIo(), () -> false, Runnable::run).addListener(ActionListener.wrap(hold -> {
            secondHold.set(hold);
            secondGranted.countDown();
        }, e -> secondGranted.countDown()));
        assertBusy(() -> assertEquals(2, budget.waiterCount()));
        cancelFirst.set(true);
        budget.wakeWaiters();
        assertTrue(firstFailed.await(5, TimeUnit.SECONDS));
        blocking.close();
        budget.clearOwner(blocking.lease());
        assertTrue(secondGranted.await(5, TimeUnit.SECONDS));
        assertNotNull(secondHold.get());
        secondHold.get().close();
        assertEquals(0, budget.used());
        assertEquals(0, budget.waiterCount());
    }

    public void testGrantForksToSuppliedExecutor() throws Exception {
        NodeByteBudgetService budget = new NodeByteBudgetService(10);
        NodeByteBudget.Hold blocking = occupyOvershoot(budget, 15);
        ExecutorService grantPool = Executors.newSingleThreadExecutor(r -> new Thread(r, "nbb-grant"));
        try {
            CountDownLatch granted = new CountDownLatch(1);
            AtomicReference<String> grantThread = new AtomicReference<>();
            AtomicReference<NodeByteBudget.Hold> hold = new AtomicReference<>();
            budget.admitAsync(4, new RowGroupIo(), () -> false, grantPool).addListener(ActionListener.wrap(h -> {
                grantThread.set(Thread.currentThread().getName());
                hold.set(h);
                granted.countDown();
            }, e -> granted.countDown()));
            assertBusy(() -> assertEquals(1, budget.waiterCount()));
            blocking.close();
            budget.clearOwner(blocking.lease());
            assertTrue(granted.await(5, TimeUnit.SECONDS));
            assertEquals("nbb-grant", grantThread.get());
            assertNotNull(hold.get());
            hold.get().close();
        } finally {
            grantPool.shutdownNow();
        }
    }

    public void testHoldCloseIsIdempotent() {
        NodeByteBudgetService budget = new NodeByteBudgetService(10);
        NodeByteBudget.Hold hold = budget.tryAdmit(4);
        assertNotNull(hold);
        hold.close();
        hold.close();
        assertEquals(0, budget.used());
    }

    /**
     * Randomized concurrent tickets stay within cap plus one unit, leak no waiters, and drop
     * every grant including the overshoot owner.
     */
    public void testRandomizedPeakCapPlusOneUnit() throws Exception {
        final long cap = 1_000L;
        final int threads = 4;
        final int ops = 10_000;
        NodeByteBudgetService budget = new NodeByteBudgetService(cap);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CyclicBarrier start = new CyclicBarrier(threads);
        AtomicLong maxUnit = new AtomicLong();
        List<Future<?>> futures = new ArrayList<>(threads);
        try {
            for (int t = 0; t < threads; t++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < ops / threads; i++) {
                        long bytes = between(1, 200);
                        maxUnit.accumulateAndGet(bytes, Math::max);
                        RowGroupIo lease = new RowGroupIo();
                        if (randomBoolean()) {
                            NodeByteBudget.Hold hold = budget.tryAdmit(bytes);
                            if (hold != null) {
                                hold.close();
                            }
                            continue;
                        }
                        CountDownLatch done = new CountDownLatch(1);
                        AtomicReference<NodeByteBudget.Hold> hold = new AtomicReference<>();
                        AtomicBoolean cancel = new AtomicBoolean();
                        budget.admitAsync(bytes, lease, cancel::get, Runnable::run).addListener(ActionListener.wrap(h -> {
                            hold.set(h);
                            done.countDown();
                        }, e -> done.countDown()));
                        if (randomInt(9) == 0) {
                            cancel.set(true);
                            budget.wakeWaiters();
                        }
                        assertTrue(done.await(10, TimeUnit.SECONDS));
                        NodeByteBudget.Hold granted = hold.get();
                        if (granted != null) {
                            RowGroupIo grantedLease = granted.lease();
                            granted.close();
                            if (grantedLease != null) {
                                budget.clearOwner(grantedLease);
                            }
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals(0, budget.used());
        assertEquals(0, budget.waiterCount());
        assertTrue("peakUsed=" + budget.peakUsed() + " cap=" + cap + " maxUnit=" + maxUnit.get(), budget.peakUsed() <= cap + maxUnit.get());
    }

    public void testOverlappingHoldsPeakCapPlusOneUnit() throws Exception {
        NodeByteBudgetService budget = new NodeByteBudgetService(10);
        NodeByteBudget.Hold under = budget.tryAdmit(8);
        assertNotNull(under);
        CountDownLatch granted = new CountDownLatch(1);
        AtomicReference<NodeByteBudget.Hold> overshoot = new AtomicReference<>();
        budget.admitAsync(8, new RowGroupIo(), () -> false, Runnable::run).addListener(ActionListener.wrap(hold -> {
            overshoot.set(hold);
            granted.countDown();
        }, e -> granted.countDown()));
        assertTrue(granted.await(5, TimeUnit.SECONDS));
        assertNotNull(overshoot.get());
        assertTrue(overshoot.get().isOvershoot());
        assertEquals(16, budget.used());
        assertEquals(16, budget.peakUsed());
        under.close();
        overshoot.get().close();
        budget.clearOwner(overshoot.get().lease());
        assertEquals(0, budget.used());
    }

    /**
     * Closing the overshoot hold without {@link NodeByteBudget#clearOwner} keeps the slot, so a
     * second over-cap unit queues instead of stacking to 2×unit.
     */
    public void testOvershootSerializesSecondUnit() throws Exception {
        NodeByteBudgetService budget = new NodeByteBudgetService(10);
        NodeByteBudget.Hold first = occupyOvershoot(budget, 15);
        assertEquals(15, budget.used());
        CountDownLatch granted = new CountDownLatch(1);
        AtomicReference<NodeByteBudget.Hold> second = new AtomicReference<>();
        AtomicReference<Exception> error = new AtomicReference<>();
        budget.admitAsync(15, new RowGroupIo(), () -> false, Runnable::run).addListener(ActionListener.wrap(hold -> {
            second.set(hold);
            granted.countDown();
        }, e -> {
            error.set(e);
            granted.countDown();
        }));
        assertBusy(() -> assertEquals(1, budget.waiterCount()));
        assertEquals(15, budget.used());
        first.close();
        assertEquals(0, budget.used());
        assertEquals(1, budget.waiterCount());
        budget.clearOwner(first.lease());
        assertTrue(granted.await(5, TimeUnit.SECONDS));
        assertNull(error.get());
        assertNotNull(second.get());
        assertEquals(15, budget.used());
        assertEquals(15, budget.peakUsed());
        second.get().close();
        budget.clearOwner(second.get().lease());
        assertEquals(0, budget.used());
        assertEquals(0, budget.waiterCount());
    }

    public void testCancelledGrantDoesNotLeak() throws Exception {
        NodeByteBudgetService budget = new NodeByteBudgetService(10);
        NodeByteBudget.Hold blocking = occupyOvershoot(budget, 15);
        AtomicBoolean cancel = new AtomicBoolean();
        CountDownLatch failed = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        budget.admitAsync(8, new RowGroupIo(), cancel::get, Runnable::run).addListener(ActionListener.wrap(hold -> {
            fail("grant should have been cancelled");
            hold.close();
        }, e -> {
            error.set(e);
            failed.countDown();
        }));
        assertBusy(() -> assertEquals(1, budget.waiterCount()));
        cancel.set(true);
        blocking.close();
        budget.clearOwner(blocking.lease());
        assertTrue(failed.await(5, TimeUnit.SECONDS));
        assertThat(error.get(), instanceOf(EsRejectedExecutionException.class));
        assertEquals(0, budget.used());
        assertEquals(0, budget.waiterCount());
    }

    /**
     * A unit larger than the cap takes the single overshoot slot so later tickets queue instead of
     * also overshooting. File-source leases have a null scheduler and would otherwise become owner
     * immediately.
     */
    private static NodeByteBudget.Hold occupyOvershoot(NodeByteBudgetService budget, long bytes) throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<NodeByteBudget.Hold> hold = new AtomicReference<>();
        AtomicReference<Exception> error = new AtomicReference<>();
        budget.admitAsync(bytes, new RowGroupIo(), () -> false, Runnable::run).addListener(ActionListener.wrap(h -> {
            hold.set(h);
            done.countDown();
        }, e -> {
            error.set(e);
            done.countDown();
        }));
        assertTrue(done.await(5, TimeUnit.SECONDS));
        assertNull(error.get());
        assertNotNull(hold.get());
        assertTrue(hold.get().isOvershoot());
        return hold.get();
    }
}
