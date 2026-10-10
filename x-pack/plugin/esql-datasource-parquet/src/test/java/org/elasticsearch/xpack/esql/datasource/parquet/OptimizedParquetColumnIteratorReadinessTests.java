/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.cache.FooterByteCache;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.NodeByteBudget;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.datasource.parquet.OptimizedParquetColumnIterator.ReadinessPhase;
import static org.hamcrest.Matchers.lessThan;

/**
 * {@link CloseableIterator#waitForReady()} / {@link CloseableIterator#tryAdvance()} contract for
 * {@link OptimizedParquetColumnIterator}: every ticket/I/O state parks, close cancels, and a
 * transient GET failure re-tickets instead of joining a sync fallback.
 */
public class OptimizedParquetColumnIteratorReadinessTests extends ESTestCase {

    private BlockFactory blockFactory;
    private ExecutorService asyncIo;

    @Before
    public void init() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
        asyncIo = Executors.newFixedThreadPool(2, EsExecutors.daemonThreadFactory("test", "opci-ready"));
    }

    @After
    public void stop() throws Exception {
        terminate(asyncIo);
    }

    public void testWaitForReadyParksUntilFirstGroupIoCompletes() throws Exception {
        byte[] parquet = smallFile();
        CountDownLatch allowRead = new CountDownLatch(1);
        GatedAsyncStorage storage = new GatedAsyncStorage(parquet, asyncIo, allowRead);
        try (CloseableIterator<Page> iter = open(storage, null)) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            SubscribableListener<Void> ready = iter.waitForReady();
            assertFalse("constructor must return before the gated GET completes", ready.isDone());
            assertNull(iter.tryAdvance());
            assertTrue(opci.readinessPhase() == ReadinessPhase.GROUP_IO_PENDING || opci.readinessPhase() == ReadinessPhase.OPEN_PENDING);
            allowRead.countDown();
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            Page page = iter.tryAdvance();
            assertNotNull(page);
            page.releaseBlocks();
        } finally {
            allowRead.countDown();
        }
    }

    public void testCloseCompletesWaitForReadyAndCancelsTicket() throws Exception {
        byte[] parquet = smallFile();
        CountDownLatch allowRead = new CountDownLatch(1);
        GatedAsyncStorage storage = new GatedAsyncStorage(parquet, asyncIo, allowRead);
        CloseableIterator<Page> iter = open(storage, null);
        try {
            SubscribableListener<Void> ready = iter.waitForReady();
            assertFalse(ready.isDone());
            iter.close();
            assertTrue("close must wake a parked consumer", ready.isDone());
            assertNull(iter.tryAdvance());
        } finally {
            allowRead.countDown();
        }
    }

    public void testTinyWatermarkParksOnFirstGroupTicket() throws Exception {
        byte[] parquet = smallFile();
        ParquetIoWatermark watermark = new ParquetIoWatermark(1);
        NodeByteBudget.Hold blocker = occupyOvershoot(watermark);
        try (CloseableIterator<Page> iter = open(new ImmediateAsyncStorage(parquet, asyncIo), watermark)) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertEquals(ReadinessPhase.OPEN_PENDING, opci.readinessPhase());
            SubscribableListener<Void> ready = iter.waitForReady();
            assertFalse("first group must wait for the overshoot slot", ready.isDone());
            assertNull(iter.tryAdvance());
            releaseOvershoot(watermark, blocker);
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            Page page = iter.tryAdvance();
            if (page != null) {
                page.releaseBlocks();
            } else {
                assertTrue(iter.hasNext());
                Page next = iter.next();
                next.releaseBlocks();
            }
        }
    }

    public void testAllFilteredStillReachesDoneWithoutBlockingHasNextAfterReady() throws Exception {
        byte[] parquet = smallFile();
        ImmediateAsyncStorage storage = new ImmediateAsyncStorage(parquet, asyncIo);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(
            FilterCompat.get(FilterApi.eq(FilterApi.intColumn("id"), -1))
        );
        try (CloseableIterator<Page> iter = reader.read(storage, FormatReadContext.of(null, 64))) {
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            assertNull(iter.tryAdvance());
            assertFalse(iter.hasNext());
        }
    }

    public void testTransientPrefetchFailureReticketsInsteadOfJoining() throws Exception {
        byte[] parquet = smallFile();
        FailFirstAsyncStorage storage = new FailFirstAsyncStorage(parquet, asyncIo, new IOException("injected prefetch miss"));
        try (CloseableIterator<Page> iter = open(storage, null)) {
            Page page = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (page == null && System.nanoTime() < deadline) {
                if (iter.waitForReady().isDone()) {
                    page = iter.tryAdvance();
                }
            }
            assertNotNull("re-ticket GET must complete without joining hasNext/awaitReady", page);
            page.releaseBlocks();
            assertEquals(1, storage.failures.get());
            assertTrue(storage.successes.get() >= 1);
        }
    }

    public void testSecondPrefetchMissFailsWithoutSyncJoin() throws Exception {
        byte[] parquet = smallFile();
        AlwaysFailAsyncStorage storage = new AlwaysFailAsyncStorage(parquet, asyncIo, new IOException("injected persistent miss"));
        try (CloseableIterator<Page> iter = open(storage, null)) {
            Exception thrown = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (thrown == null && System.nanoTime() < deadline) {
                if (iter.waitForReady().isDone()) {
                    try {
                        iter.tryAdvance();
                    } catch (RuntimeException e) {
                        thrown = e;
                    }
                }
            }
            assertNotNull("second async miss must fail without fetchSync", thrown);
            assertTrue("first miss plus re-ticket miss", storage.failures.get() >= 2);
        }
    }

    public void testFillPrefetchQueueFailureOnRetryFailsWithoutSyncFetch() throws Exception {
        byte[] parquet = smallFile();
        ThrowOnRetryAsyncStorage storage = new ThrowOnRetryAsyncStorage(parquet, asyncIo, new IOException("injected prefetch miss"));
        Exception thrown = null;
        try (CloseableIterator<Page> iter = open(storage, null)) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (thrown == null && System.nanoTime() < deadline) {
                if (iter.waitForReady().isDone()) {
                    try {
                        iter.tryAdvance();
                    } catch (RuntimeException e) {
                        thrown = e;
                    }
                }
            }
        } catch (RuntimeException openFailure) {
            thrown = openFailure;
        }
        assertNotNull("fillPrefetchQueue failure must fail the query", thrown);
        assertTrue(
            "expected injected fillPrefetchQueue failure, got " + thrown,
            thrown.getMessage() != null && thrown.getMessage().contains("injected fillPrefetchQueue failure")
        );
        assertEquals("async retry must not fall through to fetchSync", 0, storage.syncReads.get());
    }

    public void testDrainRecheckParksOnPhase2WithoutBlockingHasNext() throws Exception {
        byte[] parquet = twoColumnFile();
        CountDownLatch allowPhase2 = new CountDownLatch(1);
        AtomicBoolean gatePhase2 = new AtomicBoolean();
        GatedOnDemandStorage storage = new GatedOnDemandStorage(parquet, asyncIo, gatePhase2, allowPhase2);
        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 4L, DataType.LONG), null);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(
            new ParquetPushedExpressions(List.of(filter))
        );
        try (CloseableIterator<Page> iter = reader.read(storage, FormatReadContext.of(null, 64))) {
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            gatePhase2.set(true);
            long started = System.nanoTime();
            Page page = iter.tryAdvance();
            assertNull("phase-2 GET must not be joined on tryAdvance", page);
            SubscribableListener<Void> recheck = iter.waitForReady();
            assertFalse("drain recheck must park, not call hasNext", recheck.isDone());
            assertTrue("worker must return without awaiting phase-2 GET", System.nanoTime() - started < TimeUnit.SECONDS.toNanos(2));
            allowPhase2.countDown();
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            page = iter.tryAdvance();
            if (page != null) {
                page.releaseBlocks();
            }
        } finally {
            allowPhase2.countDown();
        }
    }

    public void testRevokeLookAheadDoesNotConsumeNextGroupRetry() throws Exception {
        byte[] parquet = multiRowGroupFile();
        FailOnceAfterArmStorage storage = new FailOnceAfterArmStorage(parquet, asyncIo, new IOException("injected post-revoke miss"));
        try (CloseableIterator<Page> iter = open(storage, new ParquetIoWatermark(1 << 20))) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            Page page = iter.tryAdvance();
            assertNotNull(page);
            page.releaseBlocks();
            assertBusy(() -> assertTrue(opci.pendingPrefetchCount() > 0), 5, TimeUnit.SECONDS);
            opci.revokeOvershootOnPark();
            assertEquals(0, opci.pendingPrefetchCount());
            storage.arm.set(true);
            Page next = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (next == null && System.nanoTime() < deadline) {
                if (iter.waitForReady().isDone()) {
                    next = iter.tryAdvance();
                }
            }
            assertNotNull("revoke-cancel must not spend the next group's re-ticket", next);
            next.releaseBlocks();
            assertEquals(1, storage.failures.get());
            assertTrue(storage.successes.get() >= 1);
        }
    }

    public void testUnsupportedOnlyProjectionEmitsNullsWithoutSyncFallbackAssert() throws Exception {
        byte[] parquet = unsupportedListOfStructFile();
        try (CloseableIterator<Page> iter = open(new ImmediateAsyncStorage(parquet, asyncIo), null)) {
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            Page page = iter.tryAdvance();
            assertNotNull("zero-byte unsupported projection must emit rows, not AssertionError", page);
            assertEquals(1, page.getPositionCount());
            assertTrue(page.getBlock(0).areAllValuesNull());
            page.releaseBlocks();
            assertNull(iter.tryAdvance());
        }
    }

    public void testCloseDuringTicketWaitWakesWaiter() throws Exception {
        byte[] parquet = smallFile();
        ParquetIoWatermark watermark = new ParquetIoWatermark(1);
        NodeByteBudget.Hold blocker = occupyOvershoot(watermark);
        CloseableIterator<Page> iter = open(new ImmediateAsyncStorage(parquet, asyncIo), watermark);
        try {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertEquals(ReadinessPhase.OPEN_PENDING, opci.readinessPhase());
            SubscribableListener<Void> ready = iter.waitForReady();
            iter.close();
            assertTrue(ready.isDone());
        } finally {
            releaseOvershoot(watermark, blocker);
        }
    }

    public void testGroupTicketPendingOnEmptyQueueRefill() throws Exception {
        byte[] parquet = smallFile();
        ParquetIoWatermark watermark = new ParquetIoWatermark(1 << 20);
        FailFirstAsyncStorage storage = new FailFirstAsyncStorage(parquet, asyncIo, new IOException("injected prefetch miss"));
        try (CloseableIterator<Page> iter = open(storage, watermark)) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            NodeByteBudget.Hold blocker = occupyOvershoot(watermark);
            try {
                assertNull(iter.tryAdvance());
                assertEquals(
                    "re-ticket after a miss must wait, not join a sync GET",
                    ReadinessPhase.GROUP_TICKET_PENDING,
                    opci.readinessPhase()
                );
                releaseOvershoot(watermark, blocker);
                blocker = null;
                assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
                Page page = iter.tryAdvance();
                if (page != null) {
                    page.releaseBlocks();
                } else {
                    assertTrue(iter.hasNext());
                    Page next = iter.next();
                    next.releaseBlocks();
                }
            } finally {
                if (blocker != null) {
                    releaseOvershoot(watermark, blocker);
                }
            }
        }
    }

    public void testRevokeOvershootOnParkDropsLookAhead() throws Exception {
        byte[] parquet = multiRowGroupFile();
        try (CloseableIterator<Page> iter = open(new ImmediateAsyncStorage(parquet, asyncIo), new ParquetIoWatermark(1 << 20))) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            Page page = iter.tryAdvance();
            assertNotNull(page);
            page.releaseBlocks();
            assertBusy(
                () -> assertTrue("look-ahead should queue after the current group emits", opci.pendingPrefetchCount() > 0),
                5,
                TimeUnit.SECONDS
            );
            opci.revokeOvershootOnPark();
            assertEquals("park on space must drop look-ahead prefetches", 0, opci.pendingPrefetchCount());
            // M1: current-group overshoot owner is not cleared here. Clearing it while those
            // bytes stay charged would admit a second overshoot. PR7 may re-ticket on park.
        }
    }

    public void testPhase2IoParksAfterPredicateDecode() throws Exception {
        byte[] parquet = twoColumnFile();
        CountDownLatch allowPhase2 = new CountDownLatch(1);
        AtomicBoolean gatePhase2 = new AtomicBoolean();
        GatedOnDemandStorage storage = new GatedOnDemandStorage(parquet, asyncIo, gatePhase2, allowPhase2);
        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 4L, DataType.LONG), null);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(
            new ParquetPushedExpressions(List.of(filter))
        );
        try (CloseableIterator<Page> iter = reader.read(storage, FormatReadContext.of(null, 64))) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            gatePhase2.set(true);
            assertNull("phase-2 GET must park", iter.tryAdvance());
            assertEquals(ReadinessPhase.PHASE2_IO_PENDING, opci.readinessPhase());
            allowPhase2.countDown();
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            Page page = iter.tryAdvance();
            if (page != null) {
                page.releaseBlocks();
            } else {
                assertTrue(iter.hasNext());
                Page next = iter.next();
                next.releaseBlocks();
            }
        } finally {
            allowPhase2.countDown();
        }
    }

    public void testPhase2DoesNotAdmitAsyncWhenWaitersQueued() throws Exception {
        byte[] parquet = twoColumnFile();
        CountDownLatch allowPhase2 = new CountDownLatch(1);
        AtomicBoolean gatePhase2 = new AtomicBoolean();
        GatedOnDemandStorage storage = new GatedOnDemandStorage(parquet, asyncIo, gatePhase2, allowPhase2);
        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 4L, DataType.LONG), null);
        ParquetIoWatermark watermark = new ParquetIoWatermark(1 << 20);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withIoWatermark(watermark)
            .withPushedFilter(new ParquetPushedExpressions(List.of(filter)));
        NodeByteBudget.Hold overshoot = null;
        try (CloseableIterator<Page> iter = reader.read(storage, FormatReadContext.of(null, 64))) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            overshoot = occupyOvershoot(watermark);
            RowGroupIo foreignLease = new RowGroupIo();
            SubscribableListener<ParquetIoWatermark.AdmitHold> foreign = watermark.admitAsync(
                watermark.limit() + 1,
                foreignLease,
                () -> false,
                Runnable::run
            );
            assertFalse(foreign.isDone());
            int waiters = watermark.waiterCount();
            assertEquals(1, waiters);
            gatePhase2.set(true);
            assertNull(iter.tryAdvance());
            assertEquals(ReadinessPhase.PHASE2_IO_PENDING, opci.readinessPhase());
            assertEquals("phase 2 must not enqueue a second byte ticket", 1, watermark.waiterCount());
            assertFalse(foreign.isDone());
            allowPhase2.countDown();
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            Page page = iter.tryAdvance();
            if (page != null) {
                page.releaseBlocks();
            }
            foreignLease.cancel();
            watermark.nodeByteBudget().wakeWaiters();
        } finally {
            allowPhase2.countDown();
            if (overshoot != null) {
                releaseOvershoot(watermark, overshoot);
            }
        }
    }

    public void testPhase2RetryKeepsGroupReservation() throws Exception {
        byte[] parquet = twoColumnFile();
        PhaseSizes group0 = phaseSizes(parquet, 0);
        FailOnceAfterArmStorage storage = new FailOnceAfterArmStorage(parquet, asyncIo, new IOException("injected phase-2 miss"));
        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 4L, DataType.LONG), null);
        ParquetIoWatermark watermark = new ParquetIoWatermark(1 << 20);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withIoWatermark(watermark)
            .withPushedFilter(new ParquetPushedExpressions(List.of(filter)));
        try (CloseableIterator<Page> iter = reader.read(storage, FormatReadContext.of(null, 64))) {
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            long usedAfterPhase1 = watermark.used();
            assertTrue("phase 1 must hold R; used=" + usedAfterPhase1, usedAfterPhase1 >= group0.reservation);
            storage.arm.set(true);
            assertNull(iter.tryAdvance());
            assertEquals("phase-2 retry must not re-ticket", 0, watermark.waiterCount());
            assertTrue(
                "phase-2 retry must keep the group hold; used=" + watermark.used() + " R=" + group0.reservation,
                watermark.used() >= group0.reservation
            );
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            Page page = iter.tryAdvance();
            if (page != null) {
                page.releaseBlocks();
            }
            assertEquals(1, storage.failures.get());
            assertTrue(storage.successes.get() >= 1);
        }
        assertLedgerClear(watermark);
    }

    public void testCloseQueuedTicketReleasesLedger() throws Exception {
        byte[] parquet = smallFile();
        ParquetIoWatermark watermark = new ParquetIoWatermark(1);
        NodeByteBudget.Hold blocker = occupyOvershoot(watermark);
        CloseableIterator<Page> iter = open(new ImmediateAsyncStorage(parquet, asyncIo), watermark);
        try {
            assertEquals(ReadinessPhase.OPEN_PENDING, ((OptimizedParquetColumnIterator) iter).readinessPhase());
            iter.close();
        } finally {
            releaseOvershoot(watermark, blocker);
        }
        assertLedgerClear(watermark);
    }

    public void testCloseGrantedUndeliveredReleasesLedger() throws Exception {
        byte[] parquet = smallFile();
        ParquetIoWatermark watermark = new ParquetIoWatermark(1);
        NodeByteBudget.Hold blocker = occupyOvershoot(watermark);
        CloseableIterator<Page> iter = open(new ImmediateAsyncStorage(parquet, asyncIo), watermark);
        try {
            assertEquals(ReadinessPhase.OPEN_PENDING, ((OptimizedParquetColumnIterator) iter).readinessPhase());
            releaseOvershoot(watermark, blocker);
            blocker = null;
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            iter.close();
        } finally {
            if (blocker != null) {
                releaseOvershoot(watermark, blocker);
            }
        }
        assertLedgerClear(watermark);
    }

    public void testCloseDuringGroupIoReleasesLedger() throws Exception {
        byte[] parquet = smallFile();
        CountDownLatch allowRead = new CountDownLatch(1);
        GatedAsyncStorage storage = new GatedAsyncStorage(parquet, asyncIo, allowRead);
        ParquetIoWatermark watermark = new ParquetIoWatermark(1 << 20);
        CloseableIterator<Page> iter = open(storage, watermark);
        try {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertBusy(() -> assertEquals(ReadinessPhase.GROUP_IO_PENDING, opci.readinessPhase()), 5, TimeUnit.SECONDS);
            iter.close();
        } finally {
            allowRead.countDown();
        }
        assertLedgerClear(watermark);
    }

    public void testCloseDuringPhase2IoReleasesLedger() throws Exception {
        byte[] parquet = twoColumnFile();
        CountDownLatch allowPhase2 = new CountDownLatch(1);
        AtomicBoolean gatePhase2 = new AtomicBoolean();
        GatedOnDemandStorage storage = new GatedOnDemandStorage(parquet, asyncIo, gatePhase2, allowPhase2);
        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 4L, DataType.LONG), null);
        ParquetIoWatermark watermark = new ParquetIoWatermark(1 << 20);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withIoWatermark(watermark)
            .withPushedFilter(new ParquetPushedExpressions(List.of(filter)));
        try (CloseableIterator<Page> iter = reader.read(storage, FormatReadContext.of(null, 64))) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            gatePhase2.set(true);
            assertNull(iter.tryAdvance());
            assertEquals(ReadinessPhase.PHASE2_IO_PENDING, opci.readinessPhase());
            iter.close();
        } finally {
            allowPhase2.countDown();
        }
        assertLedgerClear(watermark);
    }

    public void testNoMatchReleasesGroupReservation() throws Exception {
        byte[] parquet = twoColumnSparseGroupsFile();
        PhaseSizes group0 = phaseSizes(parquet, 0);
        PhaseSizes group1 = phaseSizes(parquet, 1);
        assertThat(
            "group 1 must sit outside the footer tail so the gated GET is a real miss",
            group1.startingPos(),
            lessThan((long) parquet.length - ParquetFormatReader.FOOTER_TAIL_PREFETCH_BYTES)
        );
        CountDownLatch allowGroup1 = new CountDownLatch(1);
        AtomicBoolean gateGroup1 = new AtomicBoolean();
        GatedOnDemandStorage storage = new GatedOnDemandStorage(parquet, asyncIo, gateGroup1, allowGroup1);
        ParquetIoWatermark watermark = new ParquetIoWatermark(1 << 20);
        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new Equals(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 5L, DataType.LONG), null);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withIoWatermark(watermark)
            .withPushedFilter(new ParquetPushedExpressions(List.of(filter)));
        try (CloseableIterator<Page> iter = reader.read(storage, FormatReadContext.of(null, 64))) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            long usedWhenReady = watermark.used();
            assertTrue("group 0 must hold a reservation; used=" + usedWhenReady, usedWhenReady >= group0.reservation);
            gateGroup1.set(true);
            assertNull(iter.tryAdvance());
            assertEquals(ReadinessPhase.GROUP_IO_PENDING, opci.readinessPhase());
            long usedParked = watermark.used();
            assertTrue("group 1 must hold R1; used=" + usedParked, usedParked >= group1.reservation);
            assertTrue(
                "no-match must drop R0 before the group-1 GET; used was " + usedWhenReady + " then " + usedParked,
                usedParked <= usedWhenReady - group0.reservation + group1.reservation
            );
            assertEquals(0, blockFactory.breaker().getUsed());
            allowGroup1.countDown();
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            Page page = iter.tryAdvance();
            if (page != null) {
                page.releaseBlocks();
            }
        } finally {
            allowGroup1.countDown();
        }
        assertLedgerClear(watermark);
    }

    public void testLookAheadRefusesWhenReservationDoesNotFit() throws Exception {
        byte[] parquet = twoColumnMultiGroupFile();
        PhaseSizes group1 = phaseSizes(parquet, 1);
        assertTrue("fixture must have P1 < R so the cap can distinguish them", group1.phase1 > 0 && group1.phase1 < group1.reservation);
        ParquetIoWatermark watermark = new ParquetIoWatermark(1 << 20);
        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 20L, DataType.LONG), null);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withIoWatermark(watermark)
            .withPushedFilter(new ParquetPushedExpressions(List.of(filter)));
        ParquetIoWatermark.AdmitHold eater = null;
        try (CloseableIterator<Page> iter = reader.read(new ImmediateAsyncStorage(parquet, asyncIo), FormatReadContext.of(null, 64))) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertEquals("tiny fixture is depth 1; look-ahead is not attempted yet", 1, opci.prefetchDepth());
            assertEquals(1, opci.pendingPrefetchCount());
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            long extra = watermark.limit() - watermark.used() - group1.phase1;
            assertTrue("need leftover room equal to P1", extra > 0);
            eater = watermark.tryAdmit(extra);
            assertNotNull(eater);
            long leftover = watermark.limit() - watermark.used();
            assertTrue("P1 of group 1 must still fit after the eater", leftover >= group1.phase1);
            assertTrue("R of group 1 must not fit after the eater", leftover < group1.reservation);
            long usedBeforeLookAhead = watermark.used();
            opci.setPrefetchDepth(2);
            opci.fillLookaheadPrefetches();
            assertEquals("P1 would fit; look-ahead must refuse when R does not", 1, opci.pendingPrefetchCount());
            assertEquals("used must not grow by group-1 R", usedBeforeLookAhead, watermark.used());
        } finally {
            if (eater != null) {
                eater.drop();
            }
        }
        assertLedgerClear(watermark);
    }

    /**
     * Lease cancel is the same {@link RowGroupIo#cancel()} that {@code QueryConcurrencyBudget.close}
     * issues. Parquet tests cannot construct that package-private budget; favoured-lease and
     * QCB.close() coverage is {@code NodeByteBudgetWedgeTests}. No scheduler is bound here, so
     * {@link RowGroupIo#isPinned()} is false after cancel. Between-phases is not a waiter: retarget
     * and the phase-2 GET run on the consumer thread with no park.
     */
    public void testCancelLeaseQueuedTicketReleasesLedger() throws Exception {
        byte[] parquet = smallFile();
        ParquetIoWatermark watermark = new ParquetIoWatermark(1);
        NodeByteBudget.Hold blocker = occupyOvershoot(watermark);
        ImmediateAsyncStorage storage = new ImmediateAsyncStorage(parquet, asyncIo);
        CloseableIterator<Page> iter = open(storage, watermark);
        try {
            assertEquals(ReadinessPhase.OPEN_PENDING, ((OptimizedParquetColumnIterator) iter).readinessPhase());
            assertNotNull(storage.boundLease);
            storage.boundLease.cancel();
            expectThrows(EsRejectedExecutionException.class, iter::waitForReady);
            iter.close();
            assertFalse(storage.boundLease.isPinned());
            assertEquals(0, blockFactory.breaker().getUsed());
        } finally {
            releaseOvershoot(watermark, blocker);
        }
        assertLedgerClear(watermark);
    }

    public void testCancelLeaseDuringGroupIoReleasesLedger() throws Exception {
        byte[] parquet = smallFile();
        CountDownLatch allowRead = new CountDownLatch(1);
        GatedAsyncStorage storage = new GatedAsyncStorage(parquet, asyncIo, allowRead);
        ParquetIoWatermark watermark = new ParquetIoWatermark(1 << 20);
        CloseableIterator<Page> iter = open(storage, watermark);
        try {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertBusy(() -> assertEquals(ReadinessPhase.GROUP_IO_PENDING, opci.readinessPhase()), 5, TimeUnit.SECONDS);
            assertNotNull(storage.boundLease);
            storage.boundLease.cancel();
            iter.close();
            assertFalse(storage.boundLease.isPinned());
            assertEquals(0, blockFactory.breaker().getUsed());
        } finally {
            allowRead.countDown();
        }
        assertLedgerClear(watermark);
    }

    public void testCancelLeaseDuringPhase2IoReleasesLedger() throws Exception {
        byte[] parquet = twoColumnFile();
        CountDownLatch allowPhase2 = new CountDownLatch(1);
        AtomicBoolean gatePhase2 = new AtomicBoolean();
        GatedOnDemandStorage storage = new GatedOnDemandStorage(parquet, asyncIo, gatePhase2, allowPhase2);
        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 4L, DataType.LONG), null);
        ParquetIoWatermark watermark = new ParquetIoWatermark(1 << 20);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withIoWatermark(watermark)
            .withPushedFilter(new ParquetPushedExpressions(List.of(filter)));
        try (CloseableIterator<Page> iter = reader.read(storage, FormatReadContext.of(null, 64))) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            gatePhase2.set(true);
            assertNull(iter.tryAdvance());
            assertEquals(ReadinessPhase.PHASE2_IO_PENDING, opci.readinessPhase());
            assertNotNull(storage.boundLease);
            storage.boundLease.cancel();
            iter.close();
            assertFalse(storage.boundLease.isPinned());
            assertEquals(0, blockFactory.breaker().getUsed());
        } finally {
            allowPhase2.countDown();
        }
        assertLedgerClear(watermark);
    }

    public void testCancelLeaseGrantedUndeliveredReleasesLedger() throws Exception {
        byte[] parquet = smallFile();
        ParquetIoWatermark watermark = new ParquetIoWatermark(1);
        NodeByteBudget.Hold blocker = occupyOvershoot(watermark);
        ImmediateAsyncStorage storage = new ImmediateAsyncStorage(parquet, asyncIo);
        CloseableIterator<Page> iter = open(storage, watermark);
        try {
            assertEquals(ReadinessPhase.OPEN_PENDING, ((OptimizedParquetColumnIterator) iter).readinessPhase());
            releaseOvershoot(watermark, blocker);
            blocker = null;
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            assertEquals(ReadinessPhase.OPEN_PENDING, ((OptimizedParquetColumnIterator) iter).readinessPhase());
            assertNotNull(storage.boundLease);
            storage.boundLease.cancel();
            iter.close();
            assertFalse(storage.boundLease.isPinned());
            assertEquals(0, blockFactory.breaker().getUsed());
        } finally {
            if (blocker != null) {
                releaseOvershoot(watermark, blocker);
            }
        }
        assertLedgerClear(watermark);
    }

    public void testCancelLeaseWhileDecodingReleasesLedger() throws Exception {
        byte[] parquet = smallFile();
        ImmediateAsyncStorage storage = new ImmediateAsyncStorage(parquet, asyncIo);
        ParquetIoWatermark watermark = new ParquetIoWatermark(1 << 20);
        try (CloseableIterator<Page> iter = open(storage, watermark)) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            Page page = iter.tryAdvance();
            if (page != null) {
                page.releaseBlocks();
            }
            assertEquals(ReadinessPhase.DECODING, opci.readinessPhase());
            assertNotNull(storage.boundLease);
            storage.boundLease.cancel();
            iter.close();
            assertFalse(storage.boundLease.isPinned());
            assertEquals(0, blockFactory.breaker().getUsed());
        }
        assertLedgerClear(watermark);
    }

    private CloseableIterator<Page> open(StorageObject storage, ParquetIoWatermark watermark) throws IOException {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true);
        if (watermark != null) {
            reader = reader.withIoWatermark(watermark);
        }
        return reader.read(storage, FormatReadContext.of(null, 64));
    }

    /**
     * {@code tryAdmit} never takes the overshoot slot, so a 1-byte cap still lets the iterator
     * {@code admitAsync} grant inline. Occupy overshoot first so the constructor ticket queues.
     */
    private static NodeByteBudget.Hold occupyOvershoot(ParquetIoWatermark watermark) throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<NodeByteBudget.Hold> hold = new AtomicReference<>();
        AtomicReference<Exception> error = new AtomicReference<>();
        long bytes = watermark.limit() + 1;
        watermark.nodeByteBudget().admitAsync(bytes, new RowGroupIo(), () -> false, Runnable::run).addListener(ActionListener.wrap(h -> {
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

    private void assertLedgerClear(ParquetIoWatermark watermark) throws Exception {
        assertBusy(() -> {
            assertEquals(0, watermark.used());
            assertNull(watermark.overshootOwner());
            assertEquals(0, watermark.waiterCount());
            assertEquals(0, blockFactory.breaker().getUsed());
        }, 5, TimeUnit.SECONDS);
    }

    private PhaseSizes phaseSizes(byte[] parquet, int ordinal) throws IOException {
        PlainCompressionCodecFactory codecFactory = new PlainCompressionCodecFactory();
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new ParquetStorageObjectAdapter(
                    new ImmediateAsyncStorage(parquet, asyncIo),
                    FooterByteCache.fromSettings(Settings.EMPTY),
                    NoopCircuitBreaker.INSTANCE
                ),
                PlainParquetReadOptions.builder(codecFactory).build()
            )
        ) {
            BlockMetaData block = reader.getRowGroups().get(ordinal);
            long phase1Io = ColumnChunkPrefetcher.computePrefetchBytes(block, Set.of("id"));
            long phase2Io = ColumnChunkPrefetcher.computePrefetchBytes(block, Set.of("label"));
            long phase1 = ParquetDecodeWorkingSet.admitTotal(phase1Io, ParquetDecodeWorkingSet.estimateBytes(block, Set.of("id")));
            long phase2 = ParquetDecodeWorkingSet.admitTotal(phase2Io, ParquetDecodeWorkingSet.estimateBytes(block, Set.of("label")));
            return new PhaseSizes(phase1, Math.max(phase1, phase2), block.getStartingPos());
        } finally {
            codecFactory.release();
        }
    }

    private record PhaseSizes(long phase1, long reservation, long startingPos) {}

    private static void releaseOvershoot(ParquetIoWatermark watermark, NodeByteBudget.Hold hold) {
        RowGroupIo lease = hold.lease();
        hold.close();
        watermark.nodeByteBudget().clearOwner(lease);
    }

    private static byte[] smallFile() throws IOException {
        return smallFile(1024);
    }

    private static byte[] multiRowGroupFile() throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("id").named("ready");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(1)
                .withRowGroupRowCountLimit(1)
                .withPageSize(128)
                .build()
        ) {
            for (int i = 0; i < 8; i++) {
                writer.write(factory.newGroup().append("id", i));
            }
        }
        return out.toByteArray();
    }

    private static byte[] smallFile(int rowGroupSize) throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("id").named("ready");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(rowGroupSize)
                .withPageSize(128)
                .build()
        ) {
            for (int i = 0; i < 8; i++) {
                writer.write(factory.newGroup().append("id", i));
            }
        }
        return out.toByteArray();
    }

    private static byte[] unsupportedListOfStructFile() throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("""
            message test {
              optional group a (LIST) {
                repeated group list {
                  optional group element {
                    optional binary key (UTF8);
                  }
                }
              }
            }
            """);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(1024)
                .withPageSize(128)
                .build()
        ) {
            Group row = factory.newGroup();
            row.addGroup("a").addGroup("list").addGroup("element").add("key", Binary.fromString("k"));
            writer.write(row);
        }
        return out.toByteArray();
    }

    private static byte[] twoColumnMultiGroupFile() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("label")
            .named("ready_two_multi");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(1)
                .withRowGroupRowCountLimit(16)
                .withPageSize(128)
                .build()
        ) {
            String fat = "x".repeat(256);
            for (int i = 0; i < 32; i++) {
                writer.write(factory.newGroup().append("id", (long) i).append("label", fat + "_" + i));
            }
        }
        return out.toByteArray();
    }

    /**
     * Two matching row groups whose {@code id} stats overlap a missing value: group 0 is
     * {@code {0,10}}, group 1 is {@code {5,15}}. {@code id == 5} is not row-group-pruned and
     * matches only group 1. A third stats-pruned group holds a fat pad so group 1 sits outside
     * the 64 KiB footer tail; otherwise the group-1 GET is a cache hit and never parks.
     */
    private static byte[] twoColumnSparseGroupsFile() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("label")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .named("pad")
            .named("ready_two_sparse");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(false)
                .withDictionaryEncoding("pad", false)
                .withRowGroupSize(1)
                .withRowGroupRowCountLimit(2)
                .withPageSize(128)
                .build()
        ) {
            String fat = "x".repeat(256);
            byte[] tiny = new byte[] { 0 };
            for (long id : new long[] { 0L, 10L, 5L, 15L }) {
                writer.write(
                    factory.newGroup().append("id", id).append("label", fat + "_" + id).append("pad", Binary.fromConstantByteArray(tiny))
                );
            }
            byte[] trailer = new byte[80 * 1024];
            for (long id : new long[] { 100L, 101L }) {
                writer.write(
                    factory.newGroup().append("id", id).append("label", fat + "_" + id).append("pad", Binary.fromConstantByteArray(trailer))
                );
            }
        }
        return out.toByteArray();
    }

    private static byte[] twoColumnFile() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("label")
            .named("ready_two");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(1024)
                .withPageSize(128)
                .build()
        ) {
            String fat = "x".repeat(256);
            for (int i = 0; i < 32; i++) {
                writer.write(factory.newGroup().append("id", (long) i).append("label", fat + "_" + i));
            }
        }
        return out.toByteArray();
    }

    private static OutputFile outputFile(ByteArrayOutputStream out) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    @Override
                    public long getPos() {
                        return out.size();
                    }

                    @Override
                    public void write(int b) {
                        out.write(b);
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        out.write(b, off, len);
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }

            @Override
            public String getPath() {
                return "memory://opci-ready.parquet";
            }
        };
    }

    private static class ImmediateAsyncStorage extends AbstractTestStorageObject {
        final byte[] data;
        final ExecutorService asyncIo;
        volatile RowGroupIo boundLease;

        private ImmediateAsyncStorage(byte[] data, ExecutorService asyncIo) {
            this.data = data;
            this.asyncIo = asyncIo;
        }

        @Override
        public void bindRowGroup(RowGroupIo io) {
            boundLease = io;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            return new ByteArrayInputStream(data, (int) position, (int) Math.min(length, data.length - position));
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of("memory://opci-ready.parquet");
        }

        @Override
        public boolean supportsNativeAsync() {
            return true;
        }

        @Override
        public void readBytesAsync(
            long position,
            long length,
            DirectBufferFactory factory,
            Executor executor,
            ActionListener<DirectReadBuffer> listener
        ) {
            fillAsync(data, position, length, factory, asyncIo, listener);
        }
    }

    private static final class GatedAsyncStorage extends ImmediateAsyncStorage {
        private final CountDownLatch allowRead;

        private GatedAsyncStorage(byte[] data, ExecutorService asyncIo, CountDownLatch allowRead) {
            super(data, asyncIo);
            this.allowRead = allowRead;
        }

        @Override
        public void readBytesAsync(
            long position,
            long length,
            DirectBufferFactory factory,
            Executor executor,
            ActionListener<DirectReadBuffer> listener
        ) {
            super.asyncIo.execute(() -> {
                try {
                    allowRead.await();
                    fillAsync(super.data, position, length, factory, Runnable::run, listener);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    listener.onFailure(e);
                }
            });
        }
    }

    private static final class GatedOnDemandStorage extends ImmediateAsyncStorage {
        private final AtomicBoolean gate;
        private final CountDownLatch allow;

        private GatedOnDemandStorage(byte[] data, ExecutorService asyncIo, AtomicBoolean gate, CountDownLatch allow) {
            super(data, asyncIo);
            this.gate = gate;
            this.allow = allow;
        }

        @Override
        public void readBytesAsync(
            long position,
            long length,
            DirectBufferFactory factory,
            Executor executor,
            ActionListener<DirectReadBuffer> listener
        ) {
            if (gate.get() == false) {
                super.readBytesAsync(position, length, factory, executor, listener);
                return;
            }
            super.asyncIo.execute(() -> {
                try {
                    allow.await();
                    fillAsync(super.data, position, length, factory, Runnable::run, listener);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    listener.onFailure(e);
                }
            });
        }
    }

    private static final class AlwaysFailAsyncStorage extends ImmediateAsyncStorage {
        private final Exception failure;
        private final AtomicInteger failures = new AtomicInteger();

        private AlwaysFailAsyncStorage(byte[] data, ExecutorService asyncIo, Exception failure) {
            super(data, asyncIo);
            this.failure = failure;
        }

        @Override
        public void readBytesAsync(
            long position,
            long length,
            DirectBufferFactory factory,
            Executor executor,
            ActionListener<DirectReadBuffer> listener
        ) {
            failures.incrementAndGet();
            super.asyncIo.execute(() -> listener.onFailure(failure));
        }
    }

    private static final class ThrowOnRetryAsyncStorage extends ImmediateAsyncStorage {
        private final Exception failure;
        private final AtomicInteger asyncAttempts = new AtomicInteger();
        private final AtomicInteger syncReads = new AtomicInteger();

        private ThrowOnRetryAsyncStorage(byte[] data, ExecutorService asyncIo, Exception failure) {
            super(data, asyncIo);
            this.failure = failure;
        }

        @Override
        public int readBytes(long position, java.nio.ByteBuffer target) throws IOException {
            syncReads.incrementAndGet();
            return super.readBytes(position, target);
        }

        @Override
        public void readBytesAsync(
            long position,
            long length,
            DirectBufferFactory factory,
            Executor executor,
            ActionListener<DirectReadBuffer> listener
        ) {
            if (asyncAttempts.getAndIncrement() == 0) {
                super.asyncIo.execute(() -> listener.onFailure(failure));
                return;
            }
            throw new RuntimeException("injected fillPrefetchQueue failure");
        }
    }

    private static final class FailOnceAfterArmStorage extends ImmediateAsyncStorage {
        private final Exception failure;
        private final AtomicBoolean arm = new AtomicBoolean();
        private final AtomicBoolean failNext = new AtomicBoolean(true);
        private final AtomicInteger failures = new AtomicInteger();
        private final AtomicInteger successes = new AtomicInteger();

        private FailOnceAfterArmStorage(byte[] data, ExecutorService asyncIo, Exception failure) {
            super(data, asyncIo);
            this.failure = failure;
        }

        @Override
        public void readBytesAsync(
            long position,
            long length,
            DirectBufferFactory factory,
            Executor executor,
            ActionListener<DirectReadBuffer> listener
        ) {
            if (arm.get() && failNext.compareAndSet(true, false)) {
                failures.incrementAndGet();
                super.asyncIo.execute(() -> listener.onFailure(failure));
                return;
            }
            successes.incrementAndGet();
            super.readBytesAsync(position, length, factory, executor, listener);
        }
    }

    private static final class FailFirstAsyncStorage extends ImmediateAsyncStorage {
        private final Exception failure;
        private final AtomicBoolean failNext = new AtomicBoolean(true);
        private final AtomicInteger failures = new AtomicInteger();
        private final AtomicInteger successes = new AtomicInteger();

        private FailFirstAsyncStorage(byte[] data, ExecutorService asyncIo, Exception failure) {
            super(data, asyncIo);
            this.failure = failure;
        }

        @Override
        public void readBytesAsync(
            long position,
            long length,
            DirectBufferFactory factory,
            Executor executor,
            ActionListener<DirectReadBuffer> listener
        ) {
            if (failNext.compareAndSet(true, false)) {
                failures.incrementAndGet();
                super.asyncIo.execute(() -> listener.onFailure(failure));
                return;
            }
            successes.incrementAndGet();
            super.readBytesAsync(position, length, factory, executor, listener);
        }
    }

    private static void fillAsync(
        byte[] data,
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener
    ) {
        DirectReadBuffer drb;
        try {
            drb = factory.allocateWritableWindow((int) length);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        executor.execute(() -> {
            try {
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                ByteBuffer buffer = drb.buffer();
                buffer.put(data, pos, len);
                buffer.flip();
                listener.onResponse(drb);
            } catch (Exception e) {
                drb.close();
                listener.onFailure(e);
            }
        });
    }
}
