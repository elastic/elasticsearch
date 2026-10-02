/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupScheduler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

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
    }

    public void testOneNodeWideOvershootForGroupLargerThanLimit() {
        ParquetIoWatermark watermark = new ParquetIoWatermark(50);
        assertFalse("tryReserve refuses a group larger than the cap", watermark.tryReserve(80));
        assertEquals(0, watermark.used());
        RowGroupIo lease = new RowGroupIo();
        watermark.admitWait(80, lease, 1_000L);
        assertEquals(80, watermark.used());
        assertSame(lease, watermark.overshootOwner());
        assertFalse(watermark.tryReserve(80));
        assertFalse(watermark.tryReserve(1));
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
        assertEquals(64, watermark.used());
        assertEquals(64, breaker.getUsed());
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
        assertEquals("alloc swaps the estimate for the retained array", 64, watermark.used());
        hold.drop();
        assertEquals("second drop is a no-op", 64, watermark.used());
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
        assertEquals("leftover estimate released; retained arrays remain", 20, watermark.used());
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

    public void testConcurrentAdmitWaitTakesOneOvershoot() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(10);
        AtomicInteger admitted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        CyclicBarrier start = new CyclicBarrier(9);
        Thread[] threads = new Thread[8];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    start.await(5, TimeUnit.SECONDS);
                    watermark.admitWait(50, new RowGroupIo(), 1_000L);
                    admitted.incrementAndGet();
                } catch (EsRejectedExecutionException e) {
                    rejected.incrementAndGet();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            threads[i].start();
        }
        start.await(5, TimeUnit.SECONDS);
        for (Thread thread : threads) {
            thread.join();
        }
        assertEquals("overshoot is node-wide", 1, admitted.get());
        assertEquals(7, rejected.get());
        assertEquals(50, watermark.used());
        assertNotNull(watermark.overshootOwner());
    }

    public void testNonFavouredWaitsUntilRelease() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(50);
        RowGroupIo favoured = lease(true);
        RowGroupIo other = lease(false);
        watermark.admitWait(80, favoured, 1_000L);
        assertSame(favoured, watermark.overshootOwner());
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<ParquetIoWatermark.AdmitHold> hold = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            entered.countDown();
            hold.set(watermark.admitWait(10, other, 5_000L));
            done.countDown();
        });
        waiter.start();
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        assertFalse("non-favoured must wait while the owner sits above the cap", done.await(50, TimeUnit.MILLISECONDS));
        watermark.release(80);
        assertTrue(done.await(5, TimeUnit.SECONDS));
        waiter.join();
        assertNotNull(hold.get());
        assertEquals(10, watermark.used());
        assertSame("release under the cap must not hand the owner slot to the waiter", favoured, watermark.overshootOwner());
    }

    public void testFavouredContinuesPastCap() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(10);
        RowGroupIo favoured = lease(true);
        RowGroupIo other = lease(false);
        watermark.admitWait(10, favoured, 1_000L);
        assertEquals(10, watermark.used());
        watermark.admitWait(1, favoured, 1_000L);
        assertEquals(11, watermark.used());
        assertSame(favoured, watermark.overshootOwner());
        watermark.admitWait(1, favoured, 1_000L);
        assertEquals(12, watermark.used());
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                watermark.admitWait(1, other, 200L);
            } catch (Throwable t) {
                error.set(t);
            } finally {
                done.countDown();
            }
        });
        waiter.start();
        assertTrue(done.await(5, TimeUnit.SECONDS));
        waiter.join();
        assertThat(error.get(), instanceOf(EsRejectedExecutionException.class));
        assertSame(favoured, watermark.overshootOwner());
        assertEquals(12, watermark.used());
    }

    public void testSecondQueryDoesNotTakeSecondOvershoot() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(50);
        RowGroupIo first = lease(true);
        RowGroupIo second = lease(true);
        watermark.admitWait(80, first, 1_000L);
        assertSame(first, watermark.overshootOwner());
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            entered.countDown();
            watermark.admitWait(40, second, 5_000L);
            done.countDown();
        });
        waiter.start();
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        assertFalse(done.await(50, TimeUnit.MILLISECONDS));
        watermark.release(80);
        assertTrue(done.await(5, TimeUnit.SECONDS));
        waiter.join();
        assertEquals(40, watermark.used());
        assertSame("the second query must not become a second owner", first, watermark.overshootOwner());
    }

    public void testNullSchedulerCrossesOnce() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(50);
        RowGroupIo first = new RowGroupIo();
        RowGroupIo second = new RowGroupIo();
        watermark.admitWait(80, first, 1_000L);
        assertSame(first, watermark.overshootOwner());
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            entered.countDown();
            watermark.admitWait(10, second, 5_000L);
            done.countDown();
        });
        waiter.start();
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        assertFalse("a second null-scheduler lease must not take another overshoot", done.await(50, TimeUnit.MILLISECONDS));
        watermark.clearOwner(first);
        assertTrue(done.await(5, TimeUnit.SECONDS));
        waiter.join();
        assertSame(second, watermark.overshootOwner());
        assertEquals(90, watermark.used());
    }

    public void testAdmitWaitTimeoutUsesArgument() {
        ParquetIoWatermark watermark = new ParquetIoWatermark(10);
        RowGroupIo owner = new RowGroupIo();
        watermark.admitWait(20, owner, 1_000L);
        RowGroupIo waiter = new RowGroupIo();
        long start = System.nanoTime();
        EsRejectedExecutionException e = expectThrows(EsRejectedExecutionException.class, () -> watermark.admitWait(1, waiter, 50L));
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertThat(e.getMessage(), containsString("[50]ms"));
        assertTrue("timeout must honour the caller argument, took " + elapsedMs + "ms", elapsedMs < 1_000L);
    }

    public void testThirteenPartialGroupsUnblockAfterOwnerFinish() throws Exception {
        ParquetIoWatermark watermark = new ParquetIoWatermark(100);
        RowGroupIo owner = new RowGroupIo();
        watermark.admitWait(150, owner, 1_000L);
        int waiters = 12;
        CyclicBarrier start = new CyclicBarrier(waiters + 1);
        CountDownLatch done = new CountDownLatch(waiters);
        AtomicInteger admitted = new AtomicInteger();
        Thread[] threads = new Thread[waiters];
        for (int i = 0; i < waiters; i++) {
            threads[i] = new Thread(() -> {
                try {
                    start.await(5, TimeUnit.SECONDS);
                    watermark.admitWait(5, new RowGroupIo(), 5_000L);
                    admitted.incrementAndGet();
                } catch (Exception e) {
                    throw new AssertionError(e);
                } finally {
                    done.countDown();
                }
            });
            threads[i].start();
        }
        start.await(5, TimeUnit.SECONDS);
        assertFalse("waiters stay blocked while the owner sits above the cap", done.await(50, TimeUnit.MILLISECONDS));
        watermark.release(150);
        owner.finish();
        watermark.clearOwner(owner);
        assertTrue(done.await(5, TimeUnit.SECONDS));
        for (Thread thread : threads) {
            thread.join();
        }
        assertEquals(waiters, admitted.get());
        assertEquals(60, watermark.used());
        assertNull("under-cap admits must not keep an owner", watermark.overshootOwner());
    }

    public void testOwnerClearedOnFinishLetsSecondLeaseCross() {
        ParquetIoWatermark watermark = new ParquetIoWatermark(50);
        RowGroupIo first = lease(true);
        RowGroupIo second = lease(true);
        watermark.admitWait(80, first, 1_000L);
        first.finish();
        watermark.clearOwner(first);
        assertNull(watermark.overshootOwner());
        watermark.admitWait(80, second, 1_000L);
        assertSame(second, watermark.overshootOwner());
        assertEquals(160, watermark.used());
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
}
