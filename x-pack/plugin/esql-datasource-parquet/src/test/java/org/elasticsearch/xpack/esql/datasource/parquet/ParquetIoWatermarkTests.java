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
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class ParquetIoWatermarkTests extends ESTestCase {

    public void testForHeapIsOneEighth() {
        ParquetIoWatermark watermark = ParquetIoWatermark.forHeap();
        long heapBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        assertEquals(Math.max(1L, heapBytes / ParquetIoWatermark.HEAP_DIVISOR), watermark.limit());
    }

    public void testAdmitUntilLimitThenRejectLookahead() {
        ParquetIoWatermark watermark = new ParquetIoWatermark(100);
        assertTrue(watermark.tryReserve(40, false));
        assertTrue(watermark.tryReserve(40, true));
        assertEquals(80, watermark.used());
        assertFalse("look-ahead must not cross the cap", watermark.tryReserve(30, true));
        assertEquals(80, watermark.used());
        assertTrue("current work may take the one overshoot", watermark.tryReserve(30, false));
        assertEquals(110, watermark.used());
        assertFalse("second overshoot is refused even for current work", watermark.tryReserve(1, false));
        assertFalse(watermark.tryReserve(1, true));
    }

    public void testOneNodeWideOvershootForGroupLargerThanLimit() {
        ParquetIoWatermark watermark = new ParquetIoWatermark(50);
        assertTrue(watermark.tryReserve(80, false));
        assertEquals(80, watermark.used());
        assertFalse(watermark.tryReserve(80, false));
        assertFalse(watermark.tryReserve(1, true));
    }

    public void testReleaseAllowsNextIterator() {
        ParquetIoWatermark watermark = new ParquetIoWatermark(50);
        assertTrue(watermark.tryReserve(80, false));
        watermark.release(80);
        assertEquals(0, watermark.used());
        assertTrue("release returns the overshoot slot", watermark.tryReserve(80, false));
        assertEquals(80, watermark.used());
    }

    public void testZeroReserveIsNoop() {
        ParquetIoWatermark watermark = new ParquetIoWatermark(10);
        assertTrue(watermark.tryReserve(0, true));
        assertTrue(watermark.tryReserve(0, false));
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
        ParquetIoWatermark.AdmitHold hold = watermark.tryAdmit(64, false);
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
        ParquetIoWatermark.AdmitHold hold = watermark.tryAdmit(152, false);
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
        assertTrue(watermark.tryReserve(50, false));
        watermark.release(50);
        assertTrue("stale over-limit snapshot must not refuse after a concurrent release", watermark.tryReserve(10, false));
        assertEquals(10, watermark.used());
    }

    public void testTryAdmitNullWhenLookaheadWouldExceed() {
        ParquetIoWatermark watermark = new ParquetIoWatermark(50);
        ParquetIoWatermark.AdmitHold current = watermark.tryAdmit(40, false);
        assertNotNull(current);
        assertNull(watermark.tryAdmit(20, true));
        assertEquals(40, watermark.used());
        current.drop();
        assertEquals(0, watermark.used());
    }

    public void testConcurrentCurrentWorkTakesOneOvershoot() throws Exception {
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
                if (watermark.tryReserve(50, false)) {
                    admitted.incrementAndGet();
                }
            });
            threads[i].start();
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
        assertEquals("overshoot is node-wide", 1, admitted.get());
        assertEquals(50, watermark.used());
    }
}
