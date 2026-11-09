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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.topn.SharedNumericThreshold;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceBuffer;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceDrainUtils;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheSettings;
import org.elasticsearch.xpack.esql.datasources.cache.FooterByteCache;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.DynamicThreshold;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.HeapFootprint;
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
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongPredicate;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.datasource.parquet.OptimizedParquetColumnIterator.ReadinessPhase;
import static org.hamcrest.Matchers.lessThan;

/**
 * {@link CloseableIterator#waitForReady()} / {@link CloseableIterator#tryAdvance()} contract for
 * {@link OptimizedParquetColumnIterator}: every ticket/I/O state parks, close cancels, and a
 * transient GET failure re-tickets instead of joining a sync fallback.
 */
public class OptimizedParquetColumnIteratorReadinessTests extends ESTestCase {

    /**
     * Footer byte cache budget that admits no entry. The fixtures here fit the footer prefetch, so with the
     * default cache the footer parse stores each file whole and the scan is served from that entry with no
     * I/O; these tests observe the scan's own GETs, tickets and injected failures, so the cache must stay out.
     */
    private static final Settings NO_FOOTER_BYTE_CACHE = Settings.builder()
        .put(ExternalSourceCacheSettings.FOOTER_CACHE_SIZE.getKey(), "1b")
        .build();

    private BlockFactory blockFactory;
    private ExecutorService asyncIo;

    /** Optimized reader whose footer byte cache admits nothing; see {@link #NO_FOOTER_BYTE_CACHE}. */
    private ParquetFormatReader optimizedReader() {
        return new ParquetFormatReader(NO_FOOTER_BYTE_CACHE, blockFactory);
    }

    @Before
    public void init() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
        asyncIo = Executors.newFixedThreadPool(2, EsExecutors.daemonThreadFactory("test", "opci-ready"));
    }

    @After
    public void stop() throws Exception {
        terminate(asyncIo);
    }

    /**
     * Guard: a forgotten {@code isExhausted} on a {@code waitForReady} override is silent drain EOF.
     */
    public void testWaitForReadyOverridesAlsoDeclareIsExhausted() throws Exception {
        assertWaitForReadyOverrideDeclaresIsExhausted(OptimizedParquetColumnIterator.class);
        assertWaitForReadyOverrideDeclaresIsExhausted(
            Class.forName("org.elasticsearch.xpack.esql.datasource.parquet.ParquetFormatReader$DeferredWarnProducer")
        );
    }

    private static void assertWaitForReadyOverrideDeclaresIsExhausted(Class<?> clazz) {
        boolean waitForReady = false;
        boolean isExhausted = false;
        for (Method method : clazz.getMethods()) {
            if (method.getDeclaringClass() != clazz || method.getParameterCount() != 0) {
                continue;
            }
            if ("waitForReady".equals(method.getName())) {
                waitForReady = true;
            } else if ("isExhausted".equals(method.getName())) {
                isExhausted = true;
            }
        }
        assertTrue(clazz.getName() + " must override waitForReady", waitForReady);
        assertTrue(clazz.getName() + " overrides waitForReady without isExhausted", isExhausted);
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
        ParquetFormatReader reader = optimizedReader().withPushedFilter(FilterCompat.get(FilterApi.eq(FilterApi.intColumn("id"), -1)));
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
        ParquetFormatReader reader = optimizedReader().withPushedFilter(new ParquetPushedExpressions(List.of(filter)));
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

    /**
     * Mid-file ticket: a TopN bound that dominates the waiting row group must advance readiness
     * so the consumer can skip remaining groups and reach {@code DONE}.
     */
    public void testDominatedGroupTicketGrantAdvancesReadiness() throws Exception {
        byte[] parquet = multiRowGroupFile();
        ParquetIoWatermark watermark = new ParquetIoWatermark(1 << 20);
        SharedNumericThreshold channel = new SharedNumericThreshold.Supplier(false, false).get();
        channel.offer(0L);
        DynamicThreshold threshold = descIntThreshold(channel);
        try (
            threshold;
            CloseableIterator<Page> iter = openThreshold(
                new ImmediateAsyncStorage(parquet, EsExecutors.DIRECT_EXECUTOR_SERVICE),
                watermark,
                threshold
            )
        ) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertTrue(iter.waitForReady().isDone());
            Page first = iter.tryAdvance();
            assertNotNull(first);
            first.releaseBlocks();

            NodeByteBudget.Hold blocker = occupyOvershoot(watermark);
            try {
                Page prefetched;
                int drained = 0;
                while (drained < 8 && (prefetched = iter.tryAdvance()) != null) {
                    prefetched.releaseBlocks();
                    drained++;
                }
                assertTrue("fixture must contain multiple row groups", drained > 0);
                assertEquals(ReadinessPhase.GROUP_TICKET_PENDING, opci.readinessPhase());
                channel.offer(100L);
                SubscribableListener<Void> granted = iter.waitForReady();
                assertFalse(granted.isDone());
                releaseOvershoot(watermark, blocker);
                blocker = null;
                assertTrue(granted.isDone());

                assertNull(iter.tryAdvance());
                SubscribableListener<Void> resumed = iter.waitForReady();
                assertTrue("dominated grant must advance readiness", resumed.isDone());
                assertNull(iter.tryAdvance());
                assertEquals(ReadinessPhase.DONE, opci.readinessPhase());
            } finally {
                if (blocker != null) {
                    releaseOvershoot(watermark, blocker);
                }
            }
        }
        assertLedgerClear(watermark);
    }

    /**
     * Constructor ticket ({@code OPEN_PENDING}): dominating the first group must skip it and
     * still emit the rest. A "ready, null" EOF would truncate to zero rows.
     */
    public void testDominatedFirstGroupGrantDoesNotStrandOpen() throws Exception {
        byte[] parquet = multiRowGroupFile();
        ParquetIoWatermark watermark = new ParquetIoWatermark(1);
        SharedNumericThreshold channel = new SharedNumericThreshold.Supplier(false, false).get();
        channel.offer(0L);
        DynamicThreshold threshold = descIntThreshold(channel);
        NodeByteBudget.Hold blocker = occupyOvershoot(watermark);
        DeterministicTaskQueue queue = new DeterministicTaskQueue();
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1 << 20);
        PlainActionFuture<Void> drained = new PlainActionFuture<>();
        ImmediateAsyncStorage storage = new ImmediateAsyncStorage(parquet, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        try (threshold; CloseableIterator<Page> iter = openThreshold(storage, watermark, threshold)) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertEquals(ReadinessPhase.OPEN_PENDING, opci.readinessPhase());
            channel.offer(1L);
            try {
                ExternalSourceDrainUtils.drainPagesAsync(iter, buffer, queue.getThreadPool().generic(), buffer::readCancelled, drained);
                queue.runAllRunnableTasks();
                assertFalse(drained.isDone());
                releaseOvershoot(watermark, blocker);
                blocker = null;
                assertDrainCompletes(queue, drained, () -> drainState(opci, watermark));
                assertEquals(List.of(1, 2, 3, 4, 5, 6, 7), collectIds(buffer));
            } finally {
                buffer.finish(true);
                queue.runAllRunnableTasks();
            }
        } finally {
            if (blocker != null) {
                releaseOvershoot(watermark, blocker);
            }
        }
        assertLedgerClear(watermark);
    }

    /**
     * Production drain park: after a dominated mid-file grant, the DTQ drain must finish with
     * every row that the tightened bound does not dominate.
     */
    public void testDominatedGrantThroughDrainCompletes() throws Exception {
        byte[] parquet = multiRowGroupFile();
        ParquetIoWatermark watermark = new ParquetIoWatermark(1 << 20);
        SharedNumericThreshold channel = new SharedNumericThreshold.Supplier(false, false).get();
        channel.offer(0L);
        DynamicThreshold threshold = descIntThreshold(channel);
        DeterministicTaskQueue queue = new DeterministicTaskQueue();
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1 << 20);
        PlainActionFuture<Void> drained = new PlainActionFuture<>();
        ImmediateAsyncStorage storage = new ImmediateAsyncStorage(parquet, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        try (threshold; CloseableIterator<Page> iter = openThreshold(storage, watermark, threshold)) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertTrue(iter.waitForReady().isDone());
            NodeByteBudget.Hold blocker = occupyOvershoot(watermark);
            try {
                ExternalSourceDrainUtils.drainPagesAsync(iter, buffer, queue.getThreadPool().generic(), buffer::readCancelled, drained);
                queue.runAllRunnableTasks();
                assertFalse(drained.isDone());
                assertEquals(ReadinessPhase.GROUP_TICKET_PENDING, opci.readinessPhase());
                List<Integer> emitted = collectIds(buffer);
                assertFalse("drain must emit at least the constructor-queued group", emitted.isEmpty());
                int k = emitted.stream().mapToInt(Integer::intValue).max().orElseThrow() + 1;
                List<Integer> prefix = new ArrayList<>(k);
                for (int id = 0; id < k; id++) {
                    prefix.add(id);
                }
                assertEquals("drain prefix must be the constructor-queued groups", prefix, emitted);
                assertTrue("fixture must leave groups after the ticket", k <= 5);
                channel.offer(k + 2L);
                releaseOvershoot(watermark, blocker);
                blocker = null;
                assertDrainCompletes(queue, drained, () -> drainState(opci, watermark));
                List<Integer> all = new ArrayList<>(emitted);
                all.addAll(collectIds(buffer));
                List<Integer> expected = new ArrayList<>(emitted);
                for (int id = k + 2; id < 8; id++) {
                    expected.add(id);
                }
                assertEquals(expected, all);
            } finally {
                if (blocker != null) {
                    releaseOvershoot(watermark, blocker);
                }
                buffer.finish(true);
                queue.runAllRunnableTasks();
            }
        }
        assertLedgerClear(watermark);
    }

    /**
     * Local / non-native-async storage: the required group tickets through {@code admitRequiredSync}
     * rather than constructor prefetch. Same dominated-grant hang, same wake.
     */
    public void testDominatedGrantOnSyncStorageAdvancesReadiness() throws Exception {
        byte[] parquet = multiRowGroupFile();
        ParquetIoWatermark watermark = new ParquetIoWatermark(1 << 20);
        SharedNumericThreshold channel = new SharedNumericThreshold.Supplier(false, false).get();
        channel.offer(0L);
        DynamicThreshold threshold = descIntThreshold(channel);
        try (threshold; CloseableIterator<Page> iter = openThreshold(new SyncLocalStorage(parquet), watermark, threshold)) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertTrue(iter.waitForReady().isDone());
            Page first = iter.tryAdvance();
            assertNotNull(first);
            first.releaseBlocks();

            NodeByteBudget.Hold blocker = occupyOvershoot(watermark);
            try {
                assertNull(iter.tryAdvance());
                assertEquals(ReadinessPhase.GROUP_TICKET_PENDING, opci.readinessPhase());
                channel.offer(100L);
                SubscribableListener<Void> granted = iter.waitForReady();
                assertFalse(granted.isDone());
                releaseOvershoot(watermark, blocker);
                blocker = null;
                assertTrue(granted.isDone());

                assertNull(iter.tryAdvance());
                SubscribableListener<Void> resumed = iter.waitForReady();
                assertTrue("dominated sync-path grant must advance readiness", resumed.isDone());
                assertNull(iter.tryAdvance());
                assertEquals(ReadinessPhase.DONE, opci.readinessPhase());
            } finally {
                if (blocker != null) {
                    releaseOvershoot(watermark, blocker);
                }
            }
        }
        assertLedgerClear(watermark);
    }

    /**
     * Inline re-ticket grant: the bind hook dominates the waiting group after
     * {@code fillPrefetchQueue} has already decided to ticket it. Discarding that grant must
     * skip the group, not throw "async storage I/O failed for required row group".
     */
    public void testDominatedInlineReticketGrantDoesNotFailQuery() throws Exception {
        byte[] parquet = multiRowGroupFile();
        ParquetIoWatermark watermark = new ParquetIoWatermark(1 << 20);
        SharedNumericThreshold channel = new SharedNumericThreshold.Supplier(false, false).get();
        channel.offer(0L);
        DynamicThreshold threshold = descIntThreshold(channel);
        ImmediateAsyncStorage storage = new ImmediateAsyncStorage(parquet, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        AtomicInteger maxSeen = new AtomicInteger(-1);
        List<Integer> emitted = new ArrayList<>();
        DeterministicTaskQueue queue = new DeterministicTaskQueue();
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1 << 20);
        PlainActionFuture<Void> drained = new PlainActionFuture<>();
        long charged = watermark.limit();
        try (threshold; CloseableIterator<Page> iter = openThreshold(storage, watermark, threshold)) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertTrue(iter.waitForReady().isDone());
            watermark.nodeByteBudget().add(charged, null);
            storage.onBind.set(() -> channel.offer(maxSeen.get() + 2L));
            try {
                for (int i = 0; i < 8; i++) {
                    Page page;
                    try {
                        page = iter.tryAdvance();
                    } catch (ElasticsearchException e) {
                        throw new AssertionError(
                            "inline re-ticket grant for a group dominated mid-ticket must not fail the query: " + e.getMessage(),
                            e
                        );
                    }
                    if (page == null) {
                        break;
                    }
                    try {
                        IntBlock block = page.getBlock(0);
                        for (int p = 0; p < page.getPositionCount(); p++) {
                            int id = block.getInt(p);
                            emitted.add(id);
                            maxSeen.set(Math.max(maxSeen.get(), id));
                        }
                    } finally {
                        page.releaseBlocks();
                    }
                }
                assertFalse("constructor-queued group must emit before the re-ticket", emitted.isEmpty());
                int k = maxSeen.get() + 1;
                assertEquals("tiny INT32 groups are prefetch depth 1", 1, opci.prefetchDepth());
                assertTrue("re-ticket must leave a live group to skip", k < 8);
                ExternalSourceDrainUtils.drainPagesAsync(iter, buffer, queue.getThreadPool().generic(), buffer::readCancelled, drained);
                assertDrainCompletes(queue, drained, () -> drainState(opci, watermark));
                List<Integer> all = new ArrayList<>(emitted);
                all.addAll(collectIds(buffer));
                List<Integer> expected = new ArrayList<>(emitted);
                for (int id = k + 1; id < 8; id++) {
                    expected.add(id);
                }
                assertEquals(expected, all);
                assertEquals(7, all.size());
                assertEquals(ReadinessPhase.DONE, opci.readinessPhase());
            } finally {
                buffer.finish(true);
                queue.runAllRunnableTasks();
                watermark.nodeByteBudget().release(charged, null);
            }
        }
        assertLedgerClear(watermark);
    }

    /**
     * No sort-column statistics: {@code rowGroupDominatedByThreshold} cannot skip. Marking
     * {@code noFurtherCandidates} before the empty-queue re-ticket must be EOF, not a GET-miss
     * error — {@code fillPrefetchQueue} breaks before issuing I/O.
     */
    public void testNoStatsNoFurtherCandidatesReticketIsEof() throws Exception {
        byte[] parquet = multiRowGroupFileNoStats();
        ParquetIoWatermark watermark = new ParquetIoWatermark(1 << 20);
        SharedNumericThreshold channel = new SharedNumericThreshold.Supplier(false, false).get();
        channel.offer(0L);
        DynamicThreshold threshold = descIntThreshold(channel);
        ImmediateAsyncStorage storage = new ImmediateAsyncStorage(parquet, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        DeterministicTaskQueue queue = new DeterministicTaskQueue();
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1 << 20);
        PlainActionFuture<Void> drained = new PlainActionFuture<>();
        long charged = watermark.limit();
        try (threshold; CloseableIterator<Page> iter = openThreshold(storage, watermark, threshold)) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertTrue(iter.waitForReady().isDone());
            watermark.nodeByteBudget().add(charged, null);
            try {
                Page first = iter.tryAdvance();
                assertNotNull(first);
                first.releaseBlocks();
                channel.markNoFurtherCandidates();
                try {
                    assertNull(iter.tryAdvance());
                } catch (ElasticsearchException e) {
                    fail("no-statistics re-ticket after markNoFurtherCandidates must not fail the query: " + e.getMessage());
                }
                ExternalSourceDrainUtils.drainPagesAsync(iter, buffer, queue.getThreadPool().generic(), buffer::readCancelled, drained);
                assertDrainCompletes(queue, drained, () -> drainState(opci, watermark));
                assertTrue(collectIds(buffer).isEmpty());
                assertEquals(ReadinessPhase.DONE, opci.readinessPhase());
            } finally {
                buffer.finish(true);
                queue.runAllRunnableTasks();
                watermark.nodeByteBudget().release(charged, null);
            }
        }
        assertLedgerClear(watermark);
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
            // bytes stay charged would admit a second overshoot. A later change may re-ticket on park.
        }
    }

    public void testPhase2IoParksAfterPredicateDecode() throws Exception {
        byte[] parquet = twoColumnFile();
        CountDownLatch allowPhase2 = new CountDownLatch(1);
        AtomicBoolean gatePhase2 = new AtomicBoolean();
        GatedOnDemandStorage storage = new GatedOnDemandStorage(parquet, asyncIo, gatePhase2, allowPhase2);
        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 4L, DataType.LONG), null);
        ParquetFormatReader reader = optimizedReader().withPushedFilter(new ParquetPushedExpressions(List.of(filter)));
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
        ParquetFormatReader reader = optimizedReader().withIoWatermark(watermark)
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
        ParquetFormatReader reader = optimizedReader().withIoWatermark(watermark)
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

    public void testPhase2RetryAfterAllocatedFailureStaysWithinCap() throws Exception {
        byte[] parquet = twoColumnFile();
        PhaseSizes group0 = phaseSizes(parquet, 0);
        AtomicLong peakUsed = new AtomicLong();
        ParquetIoWatermark watermark = new ParquetIoWatermark(1 << 20);
        FailAfterAllocateOnceStorage storage = new FailAfterAllocateOnceStorage(
            parquet,
            asyncIo,
            new IOException("injected phase-2 alloc-then-fail"),
            watermark::used,
            peakUsed
        );
        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 4L, DataType.LONG), null);
        ParquetFormatReader reader = optimizedReader().withIoWatermark(watermark)
            .withPushedFilter(new ParquetPushedExpressions(List.of(filter)));
        int rows = 0;
        try (CloseableIterator<Page> iter = reader.read(storage, FormatReadContext.of(null, 64))) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            peakUsed.accumulateAndGet(watermark.used(), Math::max);
            storage.arm.set(true);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline) {
                peakUsed.accumulateAndGet(watermark.used(), Math::max);
                assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
                Page page = iter.tryAdvance();
                if (page != null) {
                    rows += page.getPositionCount();
                    page.releaseBlocks();
                    continue;
                }
                if (opci.readinessPhase() == ReadinessPhase.DONE) {
                    break;
                }
            }
            assertEquals(1, storage.failures.get());
            assertTrue(storage.successes.get() >= 1);
            assertEquals(4, rows);
            assertThat(
                "used must stay within cap + one phase-2 range; peak="
                    + peakUsed.get()
                    + " cap="
                    + watermark.limit()
                    + " R="
                    + group0.reservation,
                peakUsed.get(),
                lessThan(watermark.limit() + group0.reservation + 1)
            );
        }
        assertLedgerClear(watermark);
        assertEquals(0, blockFactory.breaker().getUsed());
    }

    public void testPhase2FetchRangesFallsBackWhenFilteredExceedsRemaining() {
        RowRanges survivors = RowRanges.of(0, 1, 8);
        assertSame(survivors, OptimizedParquetColumnIterator.phase2FetchRanges(survivors, 100, 100));
        assertSame(survivors, OptimizedParquetColumnIterator.phase2FetchRanges(survivors, 100, 101));
        assertNull(OptimizedParquetColumnIterator.phase2FetchRanges(survivors, 101, 100));
        assertNull(OptimizedParquetColumnIterator.phase2FetchRanges(null, 101, 100));
        // Humongous full chunk vs many mid-size pages: filtered footprint can exceed the
        // one-region charge admission used for the merged chunk (4 MiB G1 regions).
        long fullCharge = HeapFootprint.byteArrayBytes((2L << 20) + 1);
        long filteredCharge = 5 * HeapFootprint.byteArrayBytes((1L << 20) + 1024);
        assertTrue(filteredCharge > fullCharge);
        assertNull(OptimizedParquetColumnIterator.phase2FetchRanges(survivors, filteredCharge, fullCharge));
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

    /**
     * Regression for elastic/esql-planning#2271 against a real Parquet iterator. The producer drain
     * parks on {@code waitForReady()} while the row-group GET is in flight; a cancel
     * ({@code buffer.finish(true)}) must complete the drain <em>before</em> the GET returns -- the
     * readiness signal belongs to the iterator and nothing on the cancel path completes it -- and the
     * byte ledger must still clear once the iterator is closed and the GET lands.
     */
    public void testBufferFinishWakesDrainParkedOnGroupIo() throws Exception {
        byte[] parquet = smallFile();
        CountDownLatch allowRead = new CountDownLatch(1);
        GatedAsyncStorage storage = new GatedAsyncStorage(parquet, asyncIo, allowRead);
        ParquetIoWatermark watermark = new ParquetIoWatermark(1 << 20);
        AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(1 << 20);
        ExecutorService drainExec = Executors.newSingleThreadExecutor(EsExecutors.daemonThreadFactory("test", "opci-drain"));
        try (CloseableIterator<Page> iter = open(storage, watermark)) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertBusy(() -> assertEquals(ReadinessPhase.GROUP_IO_PENDING, opci.readinessPhase()), 5, TimeUnit.SECONDS);

            CountDownLatch drained = new CountDownLatch(1);
            AtomicReference<Exception> error = new AtomicReference<>();
            ExternalSourceDrainUtils.drainPagesAsync(
                iter,
                buffer,
                drainExec,
                buffer::readCancelled,
                ActionListener.wrap(v -> drained.countDown(), e -> {
                    error.set(e);
                    drained.countDown();
                })
            );
            assertFalse("drain must stay parked while the group GET is in flight", drained.await(200, TimeUnit.MILLISECONDS));
            assertEquals(ReadinessPhase.GROUP_IO_PENDING, opci.readinessPhase());

            buffer.finish(true);

            assertTrue("finish(true) must wake the drain while the GET is still in flight", drained.await(10, TimeUnit.SECONDS));
            assertNull(error.get());
            assertEquals("the wake must not depend on the object store answering", 1, allowRead.getCount());
            assertEquals("a cancelled drain delivers nothing", 0, buffer.size());
        } finally {
            // The iterator is closed by try-with-resources while the GET is still gated; only then
            // let the GET land so the ledger release after close is what gets asserted.
            allowRead.countDown();
            terminate(drainExec);
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
        ParquetFormatReader reader = optimizedReader().withIoWatermark(watermark)
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
        long group2Start = phaseSizes(parquet, 2).startingPos();
        GatedOnDemandStorage storage = new GatedOnDemandStorage(
            parquet,
            asyncIo,
            new AtomicBoolean(true),
            allowGroup1,
            pos -> pos >= group1.startingPos() && pos < group2Start
        );
        ParquetIoWatermark watermark = new ParquetIoWatermark(1 << 20);
        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new Equals(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 5L, DataType.LONG), null);
        ParquetFormatReader reader = optimizedReader().withIoWatermark(watermark)
            .withPushedFilter(new ParquetPushedExpressions(List.of(filter)));
        try (CloseableIterator<Page> iter = reader.read(storage, FormatReadContext.of(null, 64))) {
            OptimizedParquetColumnIterator opci = (OptimizedParquetColumnIterator) iter;
            assertBusy(() -> assertTrue(iter.waitForReady().isDone()), 5, TimeUnit.SECONDS);
            long usedWhenReady = watermark.used();
            assertTrue("group 0 must hold a reservation; used=" + usedWhenReady, usedWhenReady >= group0.reservation);
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
        ParquetFormatReader reader = optimizedReader().withIoWatermark(watermark)
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
        ParquetFormatReader reader = optimizedReader().withIoWatermark(watermark)
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
        ParquetFormatReader reader = optimizedReader();
        if (watermark != null) {
            reader = reader.withIoWatermark(watermark);
        }
        return reader.read(storage, FormatReadContext.of(null, 64));
    }

    private CloseableIterator<Page> openThreshold(StorageObject storage, ParquetIoWatermark watermark, DynamicThreshold threshold)
        throws IOException {
        ParquetFormatReader reader = (ParquetFormatReader) optimizedReader().withIoWatermark(watermark).withDynamicThreshold(threshold);
        return reader.read(storage, FormatReadContext.of(null, 64));
    }

    private static DynamicThreshold descIntThreshold(SharedNumericThreshold channel) {
        return new DynamicThreshold("id", ElementType.INT, false, false, channel);
    }

    private static List<Integer> collectIds(AsyncExternalSourceBuffer buffer) {
        List<Integer> ids = new ArrayList<>();
        Page page;
        while ((page = buffer.pollPage()) != null) {
            try {
                IntBlock block = page.getBlock(0);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    ids.add(block.getInt(i));
                }
            } finally {
                page.releaseBlocks();
            }
        }
        return ids;
    }

    private static void assertDrainCompletes(DeterministicTaskQueue queue, PlainActionFuture<Void> future, Supplier<String> state) {
        queue.runAllRunnableTasks();
        assertFalse(queue.hasRunnableTasks());
        assertTrue("drain parked with nothing outstanding after quiescence: " + state.get(), future.isDone());
        future.actionGet();
    }

    private static String drainState(OptimizedParquetColumnIterator opci, ParquetIoWatermark watermark) {
        return "phase="
            + opci.readinessPhase()
            + " pendingPrefetch="
            + opci.pendingPrefetchCount()
            + " waiters="
            + watermark.waiterCount()
            + " used="
            + watermark.used()
            + " owner="
            + watermark.overshootOwner();
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
        return multiRowGroupFile(true);
    }

    private static byte[] multiRowGroupFileNoStats() throws IOException {
        return multiRowGroupFile(false);
    }

    private static byte[] multiRowGroupFile(boolean statistics) throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("id").named("ready");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(outputFile(out))
            .withConf(new PlainParquetConfiguration())
            .withCodecFactory(new PlainCompressionCodecFactory())
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .withRowGroupSize(1)
            .withRowGroupRowCountLimit(1)
            .withPageSize(128);
        if (statistics == false) {
            builder = builder.withStatisticsEnabled("id", false);
        }
        try (ParquetWriter<Group> writer = builder.build()) {
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
        final AtomicReference<Runnable> onBind = new AtomicReference<>();

        private ImmediateAsyncStorage(byte[] data, ExecutorService asyncIo) {
            this.data = data;
            this.asyncIo = asyncIo;
        }

        /**
         * A version, so this object's footer cache key is non-null. Without one the key is null, nothing is ever
         * stored or read, and NO_FOOTER_BYTE_CACHE stops being what keeps the cache out of these tests.
         */
        @Override
        public String version() {
            return "v1";
        }

        @Override
        public void bindRowGroup(RowGroupIo io) {
            boundLease = io;
            Runnable hook = onBind.getAndSet(null);
            if (hook != null) {
                hook.run();
            }
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

    /**
     * Local-file style storage: {@code supportsNativeAsync()} is false, so the required group
     * tickets through {@code admitRequiredSync} instead of constructor prefetch.
     */
    private static final class SyncLocalStorage extends ImmediateAsyncStorage {
        private SyncLocalStorage(byte[] data) {
            super(data, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        }

        @Override
        public boolean supportsNativeAsync() {
            return false;
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
        private final LongPredicate gatedPosition;

        private GatedOnDemandStorage(byte[] data, ExecutorService asyncIo, AtomicBoolean gate, CountDownLatch allow) {
            this(data, asyncIo, gate, allow, pos -> true);
        }

        private GatedOnDemandStorage(
            byte[] data,
            ExecutorService asyncIo,
            AtomicBoolean gate,
            CountDownLatch allow,
            LongPredicate gatedPosition
        ) {
            super(data, asyncIo);
            this.gate = gate;
            this.allow = allow;
            this.gatedPosition = gatedPosition;
        }

        @Override
        public void readBytesAsync(
            long position,
            long length,
            DirectBufferFactory factory,
            Executor executor,
            ActionListener<DirectReadBuffer> listener
        ) {
            if (gate.get() == false || gatedPosition.test(position) == false) {
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

    private static final class FailAfterAllocateOnceStorage extends ImmediateAsyncStorage {
        private final Exception failure;
        private final AtomicBoolean arm = new AtomicBoolean();
        private final AtomicBoolean failNext = new AtomicBoolean(true);
        private final AtomicInteger failures = new AtomicInteger();
        private final AtomicInteger successes = new AtomicInteger();
        private final LongSupplier used;
        private final AtomicLong peakUsed;

        private FailAfterAllocateOnceStorage(
            byte[] data,
            ExecutorService asyncIo,
            Exception failure,
            LongSupplier used,
            AtomicLong peakUsed
        ) {
            super(data, asyncIo);
            this.failure = failure;
            this.used = used;
            this.peakUsed = peakUsed;
        }

        @Override
        public void readBytesAsync(
            long position,
            long length,
            DirectBufferFactory factory,
            Executor executor,
            ActionListener<DirectReadBuffer> listener
        ) {
            DirectBufferFactory sampling = len -> {
                DirectReadBuffer allocated = factory.allocate(len);
                peakUsed.accumulateAndGet(used.getAsLong(), Math::max);
                return allocated;
            };
            if (arm.get() && failNext.compareAndSet(true, false)) {
                DirectReadBuffer allocated;
                try {
                    allocated = sampling.allocate((int) length);
                } catch (Exception e) {
                    listener.onFailure(e);
                    return;
                }
                allocated.close();
                failures.incrementAndGet();
                super.asyncIo.execute(() -> listener.onFailure(failure));
                return;
            }
            successes.incrementAndGet();
            super.readBytesAsync(position, length, sampling, executor, listener);
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
