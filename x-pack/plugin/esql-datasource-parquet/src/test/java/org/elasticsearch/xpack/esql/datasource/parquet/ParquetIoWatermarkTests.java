/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionTracker;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.HeapFootprint;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupScheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ParquetIoWatermarkTests extends ESTestCase {

    public void testForHeapIsOneEighth() {
        ParquetIoWatermark watermark = ParquetIoWatermark.forHeap();
        long heapBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        assertEquals(Math.max(1L, heapBytes / ParquetIoWatermark.HEAP_DIVISOR), watermark.limit());
    }

    public void testAdmitUntilLimitThenRejectLookahead() {
        ParquetIoWatermark watermark = new ParquetIoWatermark(100);
        assertTrue(watermark.tryReserve(40));
        assertTrue(watermark.tryReserve(40));
        assertEquals(80, watermark.used());
        assertFalse("tryReserve must not cross the cap", watermark.tryReserve(30));
        assertEquals(80, watermark.used());
        assertEquals(1, watermark.holders());
    }

    public void testHoldersCountsOutstandingAdmitHolds() {
        ParquetIoWatermark watermark = new ParquetIoWatermark(100);
        ParquetIoWatermark.AdmitHold first = watermark.tryAdmit(40);
        ParquetIoWatermark.AdmitHold second = watermark.tryAdmit(30);
        assertNotNull(first);
        assertNotNull(second);
        assertEquals(2, watermark.holders());
        first.drop();
        assertEquals(1, watermark.holders());
        second.drop();
        assertEquals(0, watermark.holders());
        assertTrue(watermark.tryReserve(40));
        assertEquals("used without an AdmitHold still occupies the cap", 1, watermark.holders());
        watermark.release(40);
        assertEquals(0, watermark.holders());
    }

    public void testOneNodeWideOvershootForGroupLargerThanLimit() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(50);
        assertFalse("tryReserve refuses a group larger than the cap", watermark.tryReserve(80));
        assertEquals(0, watermark.used());
        RowGroupIo lease = new RowGroupIo();
        ParquetIoWatermark.AdmitHold hold = awaitAdmit(watermark, 80, lease);
        assertEquals(80, watermark.used());
        assertSame(lease, watermark.overshootOwner());
        assertFalse(watermark.tryReserve(80));
        assertFalse(watermark.tryReserve(1));
        hold.drop();
        watermark.clearOwner(lease);
    }

    public void testReleaseAllowsNextIterator() {
        ParquetIoWatermark watermark = new ParquetIoWatermark(50);
        assertTrue(watermark.tryReserve(50));
        watermark.release(50);
        assertEquals(0, watermark.used());
        assertTrue("release returns capacity", watermark.tryReserve(50));
        assertEquals(50, watermark.used());
    }

    public void testZeroReserveIsNoop() {
        ParquetIoWatermark watermark = new ParquetIoWatermark(10);
        assertTrue(watermark.tryReserve(0));
        assertEquals(0, watermark.used());
        watermark.forceAdd(0);
        watermark.release(0);
        assertEquals(0, watermark.used());
    }

    public void testAccountingFactoryChargesAndReleasesBesideRequest() throws Exception {
        CircuitBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        ParquetIoWatermark watermark = new ParquetIoWatermark(1024);
        DirectBufferFactory factory = watermark.accountingFactory(breaker);
        DirectReadBuffer buffer = factory.allocate(64);
        assertEquals(HeapFootprint.byteArrayBytes(64), watermark.used());
        assertEquals(HeapFootprint.byteArrayBytes(64), breaker.getUsed());
        buffer.close();
        assertEquals(0, watermark.used());
        assertEquals(0, breaker.getUsed());
    }

    public void testAdmitHoldDroppedOnceOnAllocNotDoubleCounted() throws Exception {
        CircuitBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        ParquetIoWatermark watermark = new ParquetIoWatermark(1024);
        ParquetIoWatermark.AdmitHold hold = watermark.tryAdmit(64);
        assertNotNull(hold);
        assertEquals(64, watermark.used());
        DirectBufferFactory factory = watermark.accountingFactory(breaker, hold);
        DirectReadBuffer buffer = factory.allocate(64);
        assertEquals("alloc swaps the estimate for the retained array", HeapFootprint.byteArrayBytes(64), watermark.used());
        hold.drop();
        assertEquals("second drop is a no-op", HeapFootprint.byteArrayBytes(64), watermark.used());
        buffer.close();
        assertEquals(0, watermark.used());
        assertEquals(0, breaker.getUsed());
    }

    public void testAdmitHoldDropsPerAllocKeepsInFlightEstimate() throws Exception {
        CircuitBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(16));
        ParquetIoWatermark watermark = new ParquetIoWatermark(1024);
        ParquetIoWatermark.AdmitHold hold = watermark.tryAdmit(152);
        assertNotNull(hold);
        DirectBufferFactory factory = watermark.accountingFactory(breaker, hold);
        DirectReadBuffer first = factory.allocate(10);
        assertEquals("first alloc must not drop the rest of the in-flight group", 152, watermark.used());
        DirectReadBuffer second = factory.allocate(10);
        assertEquals(152, watermark.used());
        hold.drop();
        assertEquals("leftover estimate released; retained arrays remain", 2 * HeapFootprint.byteArrayBytes(10), watermark.used());
        first.close();
        second.close();
        assertEquals(0, watermark.used());
        assertEquals(0, breaker.getUsed());
    }

    public void testTryReserveRetriesWhenReleaseLandsOverLimit() {
        ParquetIoWatermark watermark = new ParquetIoWatermark(10);
        watermark.forceAdd(50);
        watermark.release(50);
        assertTrue("release returns capacity for a later tryReserve", watermark.tryReserve(10));
        assertEquals(10, watermark.used());
    }

    public void testTryAdmitNullWhenLookaheadWouldExceed() {
        ParquetIoWatermark watermark = new ParquetIoWatermark(50);
        ParquetIoWatermark.AdmitHold current = watermark.tryAdmit(40);
        assertNotNull(current);
        assertNull(watermark.tryAdmit(20));
        assertEquals(40, watermark.used());
        current.drop();
        assertEquals(0, watermark.used());
    }

    public void testConcurrentTryReserveNeverOvershoots() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(10);
        AtomicInteger admitted = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        Thread[] threads = new Thread[8];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (watermark.tryReserve(50)) {
                    admitted.incrementAndGet();
                }
            });
            threads[i].start();
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
        assertEquals("tryReserve must not take the overshoot slot", 0, admitted.get());
        assertEquals(0, watermark.used());
    }

    /**
     * Hard cap: concurrent tickets share one overshoot owner. Extra waiters stay queued instead of
     * force-charging. Peak occupancy is cap plus one unit.
     */
    public void testConcurrentTicketsTakeOneOvershootRestWait() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(200);
        CountDownLatch started = new CountDownLatch(5);
        CountDownLatch done = new CountDownLatch(8);
        List<ParquetIoWatermark.AdmitHold> holds = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            watermark.admitAsync(50, new RowGroupIo(), () -> false, Runnable::run).addListener(ActionListener.wrap(hold -> {
                synchronized (holds) {
                    holds.add(hold);
                }
                started.countDown();
                done.countDown();
            }, e -> done.countDown()));
        }
        assertTrue("cap 200 plus one 50-byte overshoot admits five units", started.await(5, TimeUnit.SECONDS));
        assertBusy(() -> assertEquals(3, watermark.waiterCount()));
        assertThat(watermark.used(), lessThanOrEqualTo(250L));
        assertNotNull(watermark.overshootOwner());
        for (ParquetIoWatermark.AdmitHold hold : List.copyOf(holds)) {
            RowGroupIo lease = hold.lease();
            hold.drop();
            if (lease != null) {
                watermark.clearOwner(lease);
            }
        }
        assertTrue(done.await(5, TimeUnit.SECONDS));
    }

    public void testNonFavouredWaitsUntilRelease() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(50);
        RowGroupIo favoured = lease(true);
        RowGroupIo other = lease(false);
        ParquetIoWatermark.AdmitHold ownerHold = awaitAdmit(watermark, 80, favoured);
        assertSame(favoured, watermark.overshootOwner());
        CountDownLatch granted = new CountDownLatch(1);
        AtomicReference<ParquetIoWatermark.AdmitHold> hold = new AtomicReference<>();
        watermark.admitAsync(10, other, () -> false, Runnable::run).addListener(ActionListener.wrap(h -> {
            hold.set(h);
            granted.countDown();
        }, e -> granted.countDown()));
        assertBusy(() -> assertEquals(1, watermark.waiterCount()));
        assertFalse("non-favoured must wait while the owner sits above the cap", granted.await(50, TimeUnit.MILLISECONDS));
        ownerHold.drop();
        watermark.release(0);
        watermark.clearOwner(favoured);
        assertTrue(granted.await(5, TimeUnit.SECONDS));
        assertNotNull(hold.get());
        assertEquals(10, watermark.used());
        hold.get().drop();
    }

    public void testFavouredContinuesPastCap() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(10);
        RowGroupIo favoured = lease(true);
        awaitAdmit(watermark, 10, favoured);
        awaitAdmit(watermark, 1, favoured);
        assertEquals(11, watermark.used());
        assertSame(favoured, watermark.overshootOwner());
        awaitAdmit(watermark, 1, favoured);
        assertEquals(12, watermark.used());
    }

    public void testSecondQueryDoesNotTakeSecondOvershoot() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(50);
        RowGroupIo first = lease(true);
        RowGroupIo second = lease(true);
        ParquetIoWatermark.AdmitHold ownerHold = awaitAdmit(watermark, 80, first);
        assertSame(first, watermark.overshootOwner());
        CountDownLatch granted = new CountDownLatch(1);
        watermark.admitAsync(40, second, () -> false, Runnable::run).addListener(ActionListener.wrap(hold -> {
            hold.drop();
            granted.countDown();
        }, e -> granted.countDown()));
        assertBusy(() -> assertEquals(1, watermark.waiterCount()));
        assertFalse(granted.await(50, TimeUnit.MILLISECONDS));
        ownerHold.drop();
        first.finish();
        watermark.clearOwner(first);
        assertTrue(granted.await(5, TimeUnit.SECONDS));
    }

    public void testNullSchedulerCrossesOnce() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(50);
        RowGroupIo first = new RowGroupIo();
        RowGroupIo second = new RowGroupIo();
        ParquetIoWatermark.AdmitHold ownerHold = awaitAdmit(watermark, 80, first);
        assertSame(first, watermark.overshootOwner());
        CountDownLatch granted = new CountDownLatch(1);
        AtomicReference<ParquetIoWatermark.AdmitHold> secondHold = new AtomicReference<>();
        watermark.admitAsync(80, second, () -> false, Runnable::run).addListener(ActionListener.wrap(hold -> {
            secondHold.set(hold);
            granted.countDown();
        }, e -> granted.countDown()));
        assertBusy(() -> assertEquals(1, watermark.waiterCount()));
        assertFalse("a second null-scheduler lease must not take another overshoot", granted.await(50, TimeUnit.MILLISECONDS));
        ownerHold.drop();
        watermark.clearOwner(first);
        assertTrue(granted.await(5, TimeUnit.SECONDS));
        assertSame(second, watermark.overshootOwner());
        secondHold.get().drop();
        watermark.clearOwner(second);
    }

    /**
     * T1: a waiter under a parked overshoot owner does not force-charge after a timeout. The
     * ticket stays queued ({@code granted.await(50ms)==false}, {@code used} unchanged) until
     * the owner drops, then grants.
     */
    public void testWaiterDoesNotForceChargeAfterTimeout() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(100);
        RowGroupIo owner = new RowGroupIo();
        ParquetIoWatermark.AdmitHold ownerHold = awaitAdmit(watermark, 150, owner);
        RowGroupIo waiter = new RowGroupIo();
        CountDownLatch granted = new CountDownLatch(1);
        AtomicReference<ParquetIoWatermark.AdmitHold> hold = new AtomicReference<>();
        watermark.admitAsync(1, waiter, () -> false, Runnable::run).addListener(ActionListener.wrap(h -> {
            hold.set(h);
            granted.countDown();
        }, e -> granted.countDown()));
        assertBusy(() -> assertEquals(1, watermark.waiterCount()));
        assertFalse("hard cap must not force-charge after 50ms", granted.await(50, TimeUnit.MILLISECONDS));
        assertEquals(150, watermark.used());
        ownerHold.drop();
        watermark.clearOwner(owner);
        assertTrue(granted.await(5, TimeUnit.SECONDS));
        assertNotNull(hold.get());
        assertEquals(1, watermark.used());
        hold.get().drop();
    }

    public void testThirteenWaitersUnblockAfterOwnerFinish() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(100);
        RowGroupIo owner = new RowGroupIo();
        ParquetIoWatermark.AdmitHold ownerHold = awaitAdmit(watermark, 150, owner);
        int waiters = 12;
        CountDownLatch done = new CountDownLatch(waiters);
        AtomicInteger admitted = new AtomicInteger();
        for (int i = 0; i < waiters; i++) {
            watermark.admitAsync(5, new RowGroupIo(), () -> false, Runnable::run).addListener(ActionListener.wrap(hold -> {
                admitted.incrementAndGet();
                hold.drop();
                done.countDown();
            }, e -> done.countDown()));
        }
        assertBusy(() -> assertEquals(waiters, watermark.waiterCount()));
        assertFalse("waiters stay blocked while the owner sits above the cap", done.await(50, TimeUnit.MILLISECONDS));
        ownerHold.drop();
        owner.finish();
        watermark.clearOwner(owner);
        assertTrue(done.await(5, TimeUnit.SECONDS));
        assertEquals(waiters, admitted.get());
        assertNull("under-cap admits must not keep an owner", watermark.overshootOwner());
    }

    public void testOwnerClearedOnFinishLetsSecondLeaseCross() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(50);
        RowGroupIo first = lease(true);
        RowGroupIo second = lease(true);
        ParquetIoWatermark.AdmitHold firstHold = awaitAdmit(watermark, 80, first);
        first.finish();
        firstHold.drop();
        watermark.clearOwner(first);
        assertNull(watermark.overshootOwner());
        ParquetIoWatermark.AdmitHold secondHold = awaitAdmit(watermark, 80, second);
        assertSame(second, watermark.overshootOwner());
        assertEquals(80, watermark.used());
        secondHold.drop();
        watermark.clearOwner(second);
    }

    /**
     * T5: waiters on a shared pool stay on tickets. The owner's release queued behind them can
     * run because they do not pin the pool in a blocking wait.
     */
    public void testWaitersOnSharedPoolStayOnTickets() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(100);
        RowGroupIo owner = new RowGroupIo();
        ParquetIoWatermark.AdmitHold ownerHold = awaitAdmit(watermark, 150, owner);
        assertSame(owner, watermark.overshootOwner());
        final int poolSize = 2;
        ExecutorService pool = Executors.newFixedThreadPool(poolSize);
        CountDownLatch allDone = new CountDownLatch(poolSize);
        AtomicInteger granted = new AtomicInteger();
        try {
            for (int i = 0; i < poolSize; i++) {
                watermark.admitAsync(10, new RowGroupIo(), () -> false, pool).addListener(ActionListener.wrap(hold -> {
                    granted.incrementAndGet();
                    hold.drop();
                    allDone.countDown();
                }, e -> allDone.countDown()));
            }
            assertBusy(() -> assertEquals(poolSize, watermark.waiterCount()));
            ownerHold.drop();
            owner.finish();
            watermark.clearOwner(owner);
            assertTrue(allDone.await(30, TimeUnit.SECONDS));
        } finally {
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }
        assertEquals(poolSize, granted.get());
    }

    /**
     * T6: a unit larger than the cap takes the single overshoot slot. Later tickets queue.
     */
    public void testOversizeUnitUsesOvershootOnly() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(50);
        RowGroupIo lease = new RowGroupIo();
        ParquetIoWatermark.AdmitHold hold = awaitAdmit(watermark, 80, lease);
        assertEquals(80, watermark.used());
        assertSame(lease, watermark.overshootOwner());
        AtomicBoolean secondGranted = new AtomicBoolean();
        AtomicReference<ParquetIoWatermark.AdmitHold> second = new AtomicReference<>();
        AtomicReference<Exception> error = new AtomicReference<>();
        watermark.admitAsync(80, new RowGroupIo(), () -> false, Runnable::run).addListener(ActionListener.wrap(h -> {
            second.set(h);
            secondGranted.set(true);
        }, e -> error.set(e)));
        assertBusy(() -> assertEquals(1, watermark.waiterCount()));
        assertFalse(secondGranted.get());
        hold.drop();
        watermark.clearOwner(lease);
        assertBusy(() -> assertTrue("second oversize unit must grant after owner drop", secondGranted.get()));
        assertNull(error.get());
        assertNotNull(second.get());
        second.get().drop();
        assertEquals(0, watermark.waiterCount());
    }

    /**
     * T7: a cancel storm of queued tickets leaks no bytes and never force-charges.
     */
    public void testCancelStormLeaksNoBytes() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(10);
        RowGroupIo owner = new RowGroupIo();
        ParquetIoWatermark.AdmitHold ownerHold = awaitAdmit(watermark, 20, owner);
        int storm = 32;
        AtomicBoolean cancel = new AtomicBoolean();
        CountDownLatch failed = new CountDownLatch(storm);
        for (int i = 0; i < storm; i++) {
            watermark.admitAsync(10, new RowGroupIo(), cancel::get, Runnable::run)
                .addListener(ActionListener.wrap(hold -> fail("cancelled waiter must not grant"), e -> failed.countDown()));
        }
        assertBusy(() -> assertEquals(storm, watermark.waiterCount()));
        cancel.set(true);
        watermark.nodeByteBudget().wakeWaiters();
        assertTrue(failed.await(5, TimeUnit.SECONDS));
        assertEquals(20, watermark.used());
        ownerHold.drop();
        watermark.clearOwner(owner);
        assertEquals(0, watermark.used());
        assertEquals(0, watermark.waiterCount());
    }

    /**
     * T10: FORK-shaped cap=1. Two concurrent units serialize through the single overshoot slot
     * without force-charging.
     */
    public void testForkCapOneSerializesOvershoot() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(1);
        RowGroupIo firstLease = new RowGroupIo();
        RowGroupIo secondLease = new RowGroupIo();
        CountDownLatch firstGranted = new CountDownLatch(1);
        CountDownLatch secondGranted = new CountDownLatch(1);
        AtomicReference<ParquetIoWatermark.AdmitHold> first = new AtomicReference<>();
        AtomicReference<ParquetIoWatermark.AdmitHold> second = new AtomicReference<>();
        watermark.admitAsync(4, firstLease, () -> false, Runnable::run).addListener(ActionListener.wrap(hold -> {
            first.set(hold);
            firstGranted.countDown();
        }, e -> firstGranted.countDown()));
        watermark.admitAsync(4, secondLease, () -> false, Runnable::run).addListener(ActionListener.wrap(hold -> {
            second.set(hold);
            secondGranted.countDown();
        }, e -> secondGranted.countDown()));
        assertTrue(firstGranted.await(5, TimeUnit.SECONDS));
        assertNotNull(first.get());
        assertFalse("second unit waits on cap=1", secondGranted.await(50, TimeUnit.MILLISECONDS));
        assertThat(watermark.used(), lessThanOrEqualTo(5L));
        first.get().drop();
        watermark.clearOwner(firstLease);
        assertTrue(secondGranted.await(5, TimeUnit.SECONDS));
        assertNotNull(second.get());
        second.get().drop();
        watermark.clearOwner(secondLease);
        assertEquals(0, watermark.used());
    }

    public void testLeaseCancelStillWakesWaiterPromptly() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(10);
        RowGroupIo owner = new RowGroupIo();
        ParquetIoWatermark.AdmitHold ownerHold = awaitAdmit(watermark, 20, owner);
        RowGroupIo lease = new RowGroupIo();
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        long usedBefore = watermark.used();
        watermark.admitAsync(10, lease, lease::isCancelled, Runnable::run).addListener(ActionListener.wrap(hold -> {
            hold.drop();
            done.countDown();
        }, e -> {
            error.set(e);
            done.countDown();
        }));
        assertBusy(() -> assertEquals(1, watermark.waiterCount()));
        long start = System.nanoTime();
        lease.cancel();
        watermark.nodeByteBudget().wakeWaiters();
        assertTrue(done.await(1, TimeUnit.SECONDS));
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertThat(error.get(), instanceOf(EsRejectedExecutionException.class));
        assertThat(error.get().getMessage(), containsString("Cancelled"));
        assertTrue("cancel must wake promptly, took " + elapsedMs + "ms", elapsedMs < 1_000L);
        assertEquals(usedBefore, watermark.used());
        ownerHold.drop();
        watermark.clearOwner(owner);
    }

    public void testSmallCapSecondQueryWaitsThenAdmitsWithoutForce() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(128);
        RowGroupIo owner = lease(true);
        ParquetIoWatermark.AdmitHold ownerHold = awaitAdmit(watermark, 150, owner);
        RowGroupIo second = lease(true);
        CountDownLatch granted = new CountDownLatch(1);
        AtomicReference<ParquetIoWatermark.AdmitHold> hold = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        watermark.admitAsync(100, second, () -> false, Runnable::run).addListener(ActionListener.wrap(h -> {
            hold.set(h);
            granted.countDown();
        }, e -> {
            error.set(e);
            granted.countDown();
        }));
        assertBusy(() -> assertEquals(1, watermark.waiterCount()));
        ownerHold.drop();
        owner.finish();
        watermark.clearOwner(owner);
        assertTrue(granted.await(5, TimeUnit.SECONDS));
        assertNull(error.get());
        assertNotNull(hold.get());
        assertEquals(100, watermark.used());
        hold.get().drop();
        watermark.clearOwner(second);
    }

    public void testCancelledTicketDoesNotTakeOwnerSlot() throws Exception {
        RecordingTracker tracker = new RecordingTracker();
        ParquetIoWatermark watermark = new ParquetIoWatermark(10);
        watermark.bindTracker(tracker);
        AtomicBoolean cancel = new AtomicBoolean(true);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        watermark.admitAsync(20, new RowGroupIo(), cancel::get, Runnable::run).addListener(ActionListener.wrap(hold -> {
            hold.drop();
            done.countDown();
        }, e -> {
            error.set(e);
            done.countDown();
        }));
        assertTrue(done.await(5, TimeUnit.SECONDS));
        assertThat(error.get(), instanceOf(EsRejectedExecutionException.class));
        assertThat(error.get().getMessage(), containsString("Cancelled"));
        assertNull(watermark.overshootOwner());
        assertEquals(0, watermark.used());
    }

    private static ParquetIoWatermark.AdmitHold awaitAdmit(ParquetIoWatermark watermark, long bytes, RowGroupIo lease) throws Exception {
        return awaitAdmit(watermark, bytes, lease, () -> false);
    }

    private static ParquetIoWatermark.AdmitHold awaitAdmit(
        ParquetIoWatermark watermark,
        long bytes,
        RowGroupIo lease,
        BooleanSupplier cancel
    ) throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<ParquetIoWatermark.AdmitHold> hold = new AtomicReference<>();
        AtomicReference<Exception> error = new AtomicReference<>();
        watermark.admitAsync(bytes, lease, cancel, Runnable::run).addListener(ActionListener.wrap(h -> {
            hold.set(h);
            done.countDown();
        }, e -> {
            error.set(e);
            done.countDown();
        }));
        assertTrue(done.await(10, TimeUnit.SECONDS));
        if (error.get() != null) {
            throw error.get();
        }
        assertNotNull(hold.get());
        return hold.get();
    }

    private static RowGroupIo lease(boolean pin) {
        RowGroupIo io = new RowGroupIo();
        io.attachScheduler(new RowGroupScheduler() {
            @Override
            public boolean tryPinOvershoot(RowGroupIo candidate) {
                return pin && candidate == io;
            }

            @Override
            public void unpin(RowGroupIo candidate) {}

            @Override
            public void finish(RowGroupIo candidate) {
                candidate.markFinished();
            }
        });
        return io;
    }

    private static final class RecordingTracker implements AdmissionTracker {
        private final AtomicInteger outstanding = new AtomicInteger();
        private final AtomicInteger finished = new AtomicInteger();

        @Override
        public Wait waitStarted(String gate, String waiter) {
            outstanding.incrementAndGet();
            return new Wait() {
                @Override
                public void granted() {
                    outstanding.decrementAndGet();
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
