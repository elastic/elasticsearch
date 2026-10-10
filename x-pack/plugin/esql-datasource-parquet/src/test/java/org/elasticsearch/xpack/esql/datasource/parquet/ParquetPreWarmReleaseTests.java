/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.NodeByteBudgetService;
import org.elasticsearch.xpack.esql.datasources.cache.FooterByteCache;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.HeapFootprint;
import org.elasticsearch.xpack.esql.datasources.spi.RangeReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Guards the dictionary pre-warm floor on small row groups (esql-planning#2270).
 * <p>
 * {@code PreloadedRowGroupMetadata} used to keep coalesced dictionary/bloom buffers force-added
 * until iterator end. After the row-group filter those buffers have no reader; {@code
 * releaseRawBuffers()} drops them. {@link #testForceAddedBytesAfterOpenByRowGroupSize} is the
 * call-site guard: after open, {@code used} stays below the index-span heap. The concurrent
 * test is a liveness guard for that release together with the waste-bounded merge and the
 * one-ticket group hold (PR-D).
 * <p>
 * The fixture is a {@code category} dictionary column (16 values, the filter column), an
 * {@code id} column, and an unprojected random {@code pad} column that makes 16,384 rows about
 * 19 MB.
 */
public class ParquetPreWarmReleaseTests extends ESTestCase {

    private static final int DICT_CARDINALITY = 16;
    private static final int ROWS = 16_384;
    private static final String FILTER_VALUE = "cat_03";
    private static final int EXPECTED_ROWS = ROWS / DICT_CARDINALITY;
    /** About 1.1 KiB of unprojected random padding per row. */
    private static final int PAD_PER_ROW = (ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE + 512 * 1024 + 4_096 - 1) / 4_096;
    private static final List<String> CATEGORY = List.of("category");
    /** {@code id} is projection-only and much larger than the predicate column, so the iterator reads in two phases. */
    private static final List<String> CATEGORY_AND_ID = List.of("category", "id");

    private ExecutorService asyncIo;
    private LimitedBreaker breaker;
    private BlockFactory blockFactory;

    @Before
    public void startAsyncIo() {
        asyncIo = Executors.newFixedThreadPool(4, EsExecutors.daemonThreadFactory("test", "prewarm-release"));
        breaker = new LimitedBreaker("prewarm-release", ByteSizeValue.ofGb(2));
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();
    }

    @After
    public void stopAsyncIo() throws Exception {
        terminate(asyncIo);
    }

    /**
     * One filtered whole-file open per row-group size. Force-added bytes held after open stay
     * below one window at every size. At 512 KiB the leftover is also below the index-span heap:
     * without {@code releaseRawBuffers} the merged index buffer stays charged.
     */
    public void testForceAddedBytesAfterOpenByRowGroupSize() throws Exception {
        long window = HeapFootprint.byteArrayBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE);
        Set<String> predicate = Set.of("category");
        int[] rowGroupKib = { 128, 256, 512, 768, 1024, 1280, 1536, 2048, 4096 };
        StringBuilder table = new StringBuilder(
            "\nrow group KiB | file bytes | used after open | default windows | async bytes requested during open\n"
        );
        List<String> overWindow = new ArrayList<>();
        for (int kib : rowGroupKib) {
            byte[] file = parquetFile(ROWS, kib * 1024L);
            FooterMetrics footer = footerMetrics(file, predicate);
            ParquetIoWatermark watermark = new ParquetIoWatermark(1024L * 1024 * 1024);
            CountingAsyncStorage storage = new CountingAsyncStorage(file, asyncIo);
            try (CloseableIterator<Page> iter = open(storage, watermark, CATEGORY)) {
                long used = watermark.used();
                table.append(
                    String.format(
                        Locale.ROOT,
                        "%d | %d | %d | %.2f | %d%n",
                        kib,
                        file.length,
                        used,
                        (double) used / window,
                        storage.asyncBytes.get()
                    )
                );
                if (used >= window) {
                    overWindow.add(kib + " KiB");
                }
                if (kib == 512) {
                    assertThat("fixture must write page indexes", footer.indexSpan(), greaterThan(0L));
                    // Leftover is the first surviving group's ticket. Without releaseRawBuffers
                    // the merged index buffer stays charged on top of that, so used is at least
                    // the index-span heap and this fails.
                    assertThat(
                        "after open, used must drop below the index-span heap; leftover="
                            + used
                            + " indexSpan="
                            + footer.indexSpan()
                            + table,
                        used,
                        lessThan(HeapFootprint.byteArrayBytes(footer.indexSpan()))
                    );
                }
                if (kib == 512 || kib == 1024 || kib == 2048) {
                    assertThat(
                        "open GETs at "
                            + kib
                            + " KiB stay within indexSpan + 2×(dict+bloom) + rg0 prefetch; metrics="
                            + footer
                            + " async="
                            + storage.asyncBytes.get(),
                        storage.asyncBytes.get(),
                        lessThanOrEqualTo(footer.openGetBound())
                    );
                }
                assertEquals(EXPECTED_ROWS, drain(iter).rows());
            }
            assertEquals("watermark returns to the pre-open baseline after close", 0L, watermark.used());
            assertEquals("breaker refunds the same forceAdd", 0L, breaker.getUsed());
        }
        logger.info("{}", table);
        assertTrue("force-added bytes after open stay below one window for row groups " + overWindow + table, overWindow.isEmpty());
    }

    /**
     * Same 512 KiB open as {@link #testForceAddedBytesAfterOpenByRowGroupSize}, with a watermark
     * whose free share is the 64 KiB floor. The #2270 fixture span is over 1 MiB, so the preload
     * stays on the split path.
     */
    public void testForceAddedBytesAfterOpenWithFullBudget() throws Exception {
        byte[] file = parquetFile(ROWS, 512 * 1024L);
        assertThat(
            "full-budget cell must stay on the #2270 split, file=" + file.length,
            file.length,
            greaterThan((int) CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP)
        );
        FooterMetrics footer = footerMetrics(file, Set.of("category"));
        ParquetIoWatermark watermark = new ParquetIoWatermark(1);
        CountingAsyncStorage storage = new CountingAsyncStorage(file, asyncIo);
        try (CloseableIterator<Page> iter = open(storage, watermark, CATEGORY)) {
            long used = watermark.used();
            assertThat("fixture must write page indexes", footer.indexSpan(), greaterThan(0L));
            assertThat(used, lessThan(HeapFootprint.byteArrayBytes(footer.indexSpan())));
            assertThat(storage.asyncBytes.get(), lessThanOrEqualTo(footer.openGetBound()));
            assertEquals(EXPECTED_ROWS, drain(iter).rows());
        }
        assertEquals(0L, watermark.used());
        assertEquals(0L, breaker.getUsed());
    }

    /** The production range path: one 32 MiB macro split of a file with 512 KiB row groups. */
    public void testForceAddedBytesAfterOpenOnRangeSplit() throws Exception {
        long forRangeWindow = HeapFootprint.byteArrayBytes(ParquetStorageObjectAdapter.MAX_WINDOW_SIZE);
        byte[] file = parquetFile(57_344, 512 * 1024);
        ParquetIoWatermark watermark = new ParquetIoWatermark(1024L * 1024 * 1024);
        CountingAsyncStorage storage = new CountingAsyncStorage(file, asyncIo);
        ReferenceAttribute category = keyword("category");
        long rangeEnd = ParquetFormatReader.DEFAULT_ROW_GROUP_MACRO_SPLIT_TARGET_BYTES;
        long used;
        try (
            CloseableIterator<Page> iter = reader(watermark, false).readRange(
                storage,
                new RangeReadContext(CATEGORY, 64, 0, rangeEnd, List.of(category), ErrorPolicy.STRICT)
            )
        ) {
            used = watermark.used();
            logger.info(
                "file [{}] bytes, split [0, {}), used after open [{}], async bytes requested during open [{}]",
                file.length,
                rangeEnd,
                used,
                storage.asyncBytes.get()
            );
            assertThat(drain(iter).rows(), greaterThan(0));
        }
        assertEquals("watermark returns to the pre-open baseline after close", 0L, watermark.used());
        assertEquals("breaker refunds the same forceAdd", 0L, breaker.getUsed());
        // Range path allocates MAX_WINDOW_SIZE, not DEFAULT_WINDOW_SIZE.
        assertThat("force-added bytes after opening one 32 MiB split stay below one window", used, lessThan(forRangeWindow));
    }

    /**
     * Six iterators, each drained by its own thread like a driver, against a node byte cap of three
     * default windows. Rescue is off: every row must complete without an over-cap grant. Liveness
     * for release + waste-bounded merge + the one-ticket group hold; the 512 KiB index-span
     * assert in {@link #testForceAddedBytesAfterOpenByRowGroupSize} is what guards the release.
     */
    public void testConcurrentReadsAgainstSmallCap() throws Exception {
        byte[] small = parquetFile(ROWS, 512 * 1024);
        byte[] control = parquetFile(ROWS, 2 * 1024 * 1024);
        StringBuilder table = new StringBuilder(
            "\nrow groups | projection | rescue | outcome | elapsed ms | waiters at end | used at end (windows) | rows per iterator\n"
        );
        List<String> problems = new ArrayList<>();
        for (byte[] file : List.of(small, control)) {
            String label = file == small ? "512 KiB" : "2 MiB";
            for (List<String> projection : List.of(CATEGORY, CATEGORY_AND_ID)) {
                RunResult r = runConcurrent(file, projection);
                table.append(
                    String.format(
                        Locale.ROOT,
                        "%s | %s | off | %s | %d | %d | %.2f | %s%n",
                        label,
                        projection,
                        r.outcome(),
                        r.elapsedMs(),
                        r.waitersAtEnd(),
                        r.usedAtEndWindows(),
                        r.rows()
                    )
                );
                if (r.completed() == false) {
                    problems.add(label + " " + projection + " rescue off");
                }
                assertEquals("breaker refunds after concurrent close", 0L, breaker.getUsed());
            }
        }
        logger.info("{}", table);
        assertTrue("reads that wedged or needed over-cap rescues: " + problems + table, problems.isEmpty());
    }

    /**
     * Sorted {@code category} so dictionaries prune most row groups and one group holds only
     * {@code cat_03}. Covers the field case of time-sorted logs.
     */
    public void testClusteredCategoryPrunesRowGroups() throws Exception {
        byte[] file = parquetFile(ROWS, 512 * 1024L, CompressionCodecName.UNCOMPRESSED, true, false);
        ParquetIoWatermark watermark = new ParquetIoWatermark(1024L * 1024 * 1024);
        CountingAsyncStorage storage = new CountingAsyncStorage(file, asyncIo);
        ParquetReaderCounters counters = new ParquetReaderCounters();
        long used;
        try (CloseableIterator<Page> iter = open(storage, watermark, CATEGORY_AND_ID, counters, false)) {
            used = watermark.used();
            DrainResult drained = drain(iter, true);
            assertEquals(EXPECTED_ROWS, drained.rows());
            assertEquals(expectedIdSum(ROWS), drained.idSum());
        }
        ParquetReaderStatus snap = counters.snapshot();
        assertThat(snap.rowGroupsTotal() - snap.rowGroupsKept(), greaterThan(0L));
        assertThat(used, lessThan(HeapFootprint.byteArrayBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE)));
        assertEquals(0L, watermark.used());
        assertEquals(0L, breaker.getUsed());
    }

    /**
     * Concurrent two-phase 512 KiB cell with a compressed codec. Pre-warm and decode both
     * charge the breaker; it must return to 0.
     */
    public void testConcurrentCompressedTwoPhase() throws Exception {
        CompressionCodecName codec = randomFrom(CompressionCodecName.SNAPPY, CompressionCodecName.ZSTD);
        byte[] file = parquetFile(ROWS, 512 * 1024L, codec, false, false);
        String label = "compressed " + codec;
        RunResult r = runConcurrent(file, CATEGORY_AND_ID, 6, false, label);
        assertTrue(label + " " + r.outcome(), r.completed());
        assertThat(codec + " peak breaker", r.peakBreaker(), greaterThan(0L));
        assertEquals(0L, breaker.getUsed());
    }

    /**
     * One seeded cell from the Julian-repro matrix. The drawn axes are in the failure text.
     */
    public void testSeededRandomPreWarmCell() throws Exception {
        int kib = randomFrom(128, 256, 512, 1024, 2048);
        List<String> projection = randomBoolean() ? CATEGORY : CATEGORY_AND_ID;
        boolean extraPredicate = randomBoolean();
        int readers = randomFrom(1, 4, 8);
        CompressionCodecName codec = randomFrom(CompressionCodecName.UNCOMPRESSED, CompressionCodecName.SNAPPY, CompressionCodecName.ZSTD);
        String cell = "rg="
            + kib
            + "KiB projection="
            + projection
            + " predicates="
            + (extraPredicate ? 2 : 1)
            + " readers="
            + readers
            + " codec="
            + codec;
        byte[] file = parquetFile(ROWS, kib * 1024L, codec, false, extraPredicate);
        RunResult r = runConcurrent(file, projection, readers, extraPredicate, cell);
        assertTrue(cell + " " + r.outcome(), r.completed());
        assertEquals(cell, 0L, breaker.getUsed());
    }

    /**
     * Close after the first page (LIMIT / cancel), including a two-phase iterator that may
     * already have phase-2 I/O in flight. Pre-warm is already gone after open; this guards
     * the ticket and group-hold paths.
     */
    public void testEarlyCloseReleasesTicket() throws Exception {
        long window = HeapFootprint.byteArrayBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE);
        NodeByteBudgetService budget = new NodeByteBudgetService(3 * window);
        ParquetIoWatermark watermark = new ParquetIoWatermark(budget);
        byte[] file = parquetFile(ROWS, 512 * 1024L);
        CountingAsyncStorage storage = new CountingAsyncStorage(file, asyncIo);
        try (CloseableIterator<Page> iter = open(storage, watermark, CATEGORY)) {
            consumeOnePage(iter);
        }
        assertEquals(0L, watermark.used());
        assertEquals(0L, breaker.getUsed());
        assertEquals(0, budget.waiterCount());

        try (CloseableIterator<Page> iter = open(storage, watermark, CATEGORY_AND_ID)) {
            consumeOnePage(iter);
        }
        assertEquals(0L, watermark.used());
        assertEquals(0L, breaker.getUsed());
        assertEquals(0, budget.waiterCount());
    }

    private record RunResult(
        boolean completed,
        String outcome,
        long elapsedMs,
        int waitersAtEnd,
        double usedAtEndWindows,
        String rows,
        long peakBreaker
    ) {}

    private record DrainResult(int rows, long idSum) {}

    private record FooterMetrics(long indexSpan, long dictBloom, long rg0Prefetch) {
        long openGetBound() {
            return indexSpan + 2 * dictBloom + rg0Prefetch;
        }
    }

    private RunResult runConcurrent(byte[] file, List<String> projection) throws Exception {
        return runConcurrent(file, projection, 6, false, "");
    }

    private RunResult runConcurrent(byte[] file, List<String> projection, int n, boolean extraPredicate, String label) throws Exception {
        long window = HeapFootprint.byteArrayBytes(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE);
        NodeByteBudgetService budget = new NodeByteBudgetService(3 * window);
        ParquetIoWatermark watermark = new ParquetIoWatermark(budget);
        CountingAsyncStorage storage = new CountingAsyncStorage(file, asyncIo);
        List<CloseableIterator<Page>> iters = new ArrayList<>(n);
        ExecutorService consumers = Executors.newFixedThreadPool(n, EsExecutors.daemonThreadFactory("test", "consumer"));
        AtomicBoolean stop = new AtomicBoolean();
        AtomicIntegerArray rows = new AtomicIntegerArray(n);
        AtomicLongArray idSums = new AtomicLongArray(n);
        AtomicLong peakBreaker = new AtomicLong();
        boolean sumId = projection.equals(CATEGORY_AND_ID);
        List<Future<?>> futures = new ArrayList<>(n);
        long timeoutNanos = TimeUnit.SECONDS.toNanos(20);
        boolean completed = false;
        String outcome = "wedged";
        long elapsedMs = 0L;
        int waitersAtEnd = 0;
        long usedAtEnd = 0L;
        long start = System.nanoTime();
        try {
            for (int i = 0; i < n; i++) {
                long remaining = timeoutNanos - (System.nanoTime() - start);
                if (remaining <= 0L) {
                    outcome = "wedged during open";
                    break;
                }
                Future<CloseableIterator<Page>> opening = consumers.submit(
                    () -> open(storage, watermark, projection, null, extraPredicate)
                );
                try {
                    iters.add(opening.get(remaining, TimeUnit.NANOSECONDS));
                    peakBreaker.accumulateAndGet(breaker.getUsed(), Math::max);
                } catch (TimeoutException e) {
                    opening.cancel(true);
                    outcome = "wedged during open";
                    break;
                } catch (ExecutionException e) {
                    outcome = "failed opening: " + e.getCause();
                    break;
                }
            }
            if (iters.size() == n) {
                for (int i = 0; i < n; i++) {
                    int idx = i;
                    futures.add(consumers.submit(() -> {
                        consume(iters.get(idx), rows, idSums, sumId, idx, stop, peakBreaker, label);
                        return null;
                    }));
                }
                while (true) {
                    peakBreaker.accumulateAndGet(breaker.getUsed(), Math::max);
                    long now = System.nanoTime();
                    if (futures.stream().allMatch(Future::isDone)) {
                        outcome = "completed";
                        completed = true;
                        for (Future<?> f : futures) {
                            try {
                                f.get();
                            } catch (ExecutionException e) {
                                outcome = "failed: " + e.getCause();
                                completed = false;
                            }
                        }
                        break;
                    }
                    if (now - start > timeoutNanos) {
                        outcome = "wedged after " + TimeUnit.NANOSECONDS.toSeconds(timeoutNanos) + " s";
                        break;
                    }
                    Thread.sleep(10);
                }
            }
            elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            waitersAtEnd = budget.waiterCount();
            usedAtEnd = budget.used();
        } finally {
            stop.set(true);
            consumers.shutdownNow();
            assertTrue(consumers.awaitTermination(30, TimeUnit.SECONDS));
            IOException first = null;
            for (CloseableIterator<Page> iter : iters) {
                try {
                    iter.close();
                } catch (IOException e) {
                    if (first == null) {
                        first = e;
                    } else {
                        first.addSuppressed(e);
                    }
                }
            }
            if (first != null) {
                throw first;
            }
            assertEquals(label + " watermark returns to baseline after concurrent close", 0L, watermark.used());
            assertEquals(label + " waiters", 0, watermark.waiterCount());
        }
        if (completed && sumId) {
            long expected = expectedIdSum(ROWS);
            for (int i = 0; i < n; i++) {
                assertEquals(label + " id sum iterator " + i, expected, idSums.get(i));
            }
        }
        StringBuilder perIterator = new StringBuilder();
        for (int i = 0; i < n; i++) {
            perIterator.append(i == 0 ? "" : ",").append(rows.get(i));
        }
        return new RunResult(
            completed,
            outcome,
            elapsedMs,
            waitersAtEnd,
            (double) usedAtEnd / window,
            perIterator.toString(),
            peakBreaker.get()
        );
    }

    /**
     * Driver-like consumer. Uses the EOF rule of {@code ExternalSourceDrainUtils#tryAdvanceOrPark} and
     * parks on a latch: grants are delivered inline on other consumer threads, so blocking on a
     * {@code PlainActionFuture} here trips its same-pool deadlock assertion.
     */
    private void consume(
        CloseableIterator<Page> iter,
        AtomicIntegerArray rows,
        AtomicLongArray idSums,
        boolean sumId,
        int idx,
        AtomicBoolean stop,
        AtomicLong peakBreaker,
        String label
    ) throws Exception {
        while (stop.get() == false) {
            SubscribableListener<Void> ready = iter.waitForReady();
            if (ready.isDone() == false) {
                CountDownLatch latch = new CountDownLatch(1);
                ready.addListener(ActionListener.running(latch::countDown));
                latch.await(50, TimeUnit.MILLISECONDS);
                continue;
            }
            Page page = iter.tryAdvance();
            if (page == null) {
                if (iter.waitForReady().isDone() == false) {
                    continue;
                }
                page = iter.tryAdvance();
                if (page == null) {
                    if (iter.waitForReady().isDone() == false) {
                        continue;
                    }
                    if (rows.get(idx) != EXPECTED_ROWS) {
                        throw new AssertionError(label + " iterator " + idx + " ended after " + rows.get(idx) + " rows");
                    }
                    return;
                }
            }
            rows.addAndGet(idx, page.getPositionCount());
            peakBreaker.accumulateAndGet(breaker.getUsed(), Math::max);
            if (sumId) {
                idSums.addAndGet(idx, sumIdColumn(page));
            }
            page.releaseBlocks();
        }
    }

    /** Single-threaded drain with the same EOF rule as {@link #consume}. */
    private static DrainResult drain(CloseableIterator<Page> iter) {
        return drain(iter, false);
    }

    private static DrainResult drain(CloseableIterator<Page> iter, boolean sumId) {
        int rows = 0;
        long idSum = 0L;
        while (true) {
            SubscribableListener<Void> ready = iter.waitForReady();
            if (ready.isDone() == false) {
                safeAwait(ready, TimeValue.timeValueSeconds(30));
                continue;
            }
            Page page = iter.tryAdvance();
            if (page == null) {
                if (iter.waitForReady().isDone() == false) {
                    continue;
                }
                page = iter.tryAdvance();
                if (page == null) {
                    if (iter.waitForReady().isDone() == false) {
                        continue;
                    }
                    return new DrainResult(rows, idSum);
                }
            }
            rows += page.getPositionCount();
            if (sumId) {
                idSum += sumIdColumn(page);
            }
            page.releaseBlocks();
        }
    }

    private static void consumeOnePage(CloseableIterator<Page> iter) {
        while (true) {
            SubscribableListener<Void> ready = iter.waitForReady();
            if (ready.isDone() == false) {
                safeAwait(ready, TimeValue.timeValueSeconds(30));
                continue;
            }
            Page page = iter.tryAdvance();
            if (page == null) {
                if (iter.waitForReady().isDone() == false) {
                    continue;
                }
                page = iter.tryAdvance();
                if (page == null) {
                    if (iter.waitForReady().isDone() == false) {
                        continue;
                    }
                    throw new AssertionError("expected at least one page");
                }
            }
            page.releaseBlocks();
            return;
        }
    }

    private static long sumIdColumn(Page page) {
        IntBlock ids = (IntBlock) page.getBlock(1);
        long sum = 0L;
        for (int i = 0; i < page.getPositionCount(); i++) {
            sum += ids.getInt(ids.getFirstValueIndex(i));
        }
        return sum;
    }

    /** Ids with {@code i % 16 == 3}: {@code 3, 19, 35, ...}. */
    private static long expectedIdSum(int rows) {
        long n = rows / DICT_CARDINALITY;
        return 3L * n + 8L * n * (n - 1);
    }

    private ParquetFormatReader reader(ParquetIoWatermark watermark, boolean extraPredicate) {
        ReferenceAttribute category = keyword("category");
        Literal categoryValue = new Literal(Source.EMPTY, new BytesRef(FILTER_VALUE), DataType.KEYWORD);
        List<Expression> pushed = new ArrayList<>();
        pushed.add(new Equals(Source.EMPTY, category, categoryValue, null));
        if (extraPredicate) {
            pushed.add(new Equals(Source.EMPTY, keyword("tag"), new Literal(Source.EMPTY, new BytesRef("tag_03"), DataType.KEYWORD), null));
        }
        return new ParquetFormatReader(blockFactory, true).withPushedFilter(new ParquetPushedExpressions(pushed))
            .withIoWatermark(watermark);
    }

    private CloseableIterator<Page> open(StorageObject storage, ParquetIoWatermark watermark, List<String> projection) throws IOException {
        return open(storage, watermark, projection, null, false);
    }

    private CloseableIterator<Page> open(
        StorageObject storage,
        ParquetIoWatermark watermark,
        List<String> projection,
        ParquetReaderCounters counters,
        boolean extraPredicate
    ) throws IOException {
        ReferenceAttribute category = keyword("category");
        List<Attribute> schema = new ArrayList<>();
        for (String name : projection) {
            schema.add(name.equals("id") ? new ReferenceAttribute(Source.EMPTY, "id", DataType.INTEGER) : category);
        }
        var builder = FormatReadContext.builder().projectedColumns(projection).readSchema(schema).batchSize(64);
        if (counters != null) {
            builder.readCounters(counters);
        }
        return reader(watermark, extraPredicate).read(storage, builder.build());
    }

    private static ReferenceAttribute keyword(String name) {
        return new ReferenceAttribute(Source.EMPTY, name, DataType.KEYWORD);
    }

    /**
     * Index span of needed column/offset indexes, dictionary+bloom bytes, and first-group
     * predicate prefetch. Open-time async GETs stay within {@link FooterMetrics#openGetBound()}.
     */
    private static FooterMetrics footerMetrics(byte[] file, Set<String> predicateColumns) throws IOException {
        StorageObject storage = new AbstractTestStorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(file);
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new ByteArrayInputStream(file, (int) position, (int) Math.min(length, file.length - position));
            }

            @Override
            public long length() {
                return file.length;
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
                return StoragePath.of("memory://preload-floor.parquet");
            }
        };
        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new ParquetStorageObjectAdapter(storage, FooterByteCache.fromSettings(Settings.EMPTY), NoopCircuitBreaker.INSTANCE),
                options
            )
        ) {
            long dictBloom = 0L;
            long minIndex = Long.MAX_VALUE;
            long maxIndex = Long.MIN_VALUE;
            for (BlockMetaData block : reader.getRowGroups()) {
                for (ColumnChunkMetaData col : block.getColumns()) {
                    if (predicateColumns.contains(col.getPath().toDotString()) == false) {
                        continue;
                    }
                    if (col.hasDictionaryPage() && col.getDictionaryPageOffset() > 0) {
                        dictBloom += col.getFirstDataPageOffset() - col.getDictionaryPageOffset();
                    }
                    int bloom = col.getBloomFilterLength();
                    if (col.getBloomFilterOffset() > 0 && bloom > 0) {
                        dictBloom += bloom;
                    }
                    IndexReference ci = col.getColumnIndexReference();
                    if (ci != null && ci.getLength() > 0) {
                        minIndex = Math.min(minIndex, ci.getOffset());
                        maxIndex = Math.max(maxIndex, ci.getOffset() + ci.getLength());
                    }
                    IndexReference oi = col.getOffsetIndexReference();
                    if (oi != null && oi.getLength() > 0) {
                        minIndex = Math.min(minIndex, oi.getOffset());
                        maxIndex = Math.max(maxIndex, oi.getOffset() + oi.getLength());
                    }
                }
            }
            long indexSpan = minIndex < maxIndex ? maxIndex - minIndex : 0L;
            long rg0Prefetch = 0L;
            List<BlockMetaData> groups = reader.getRowGroups();
            if (groups.isEmpty() == false) {
                var ranges = ColumnChunkPrefetcher.computeColumnChunkRanges(groups.get(0), predicateColumns);
                for (var merged : CoalescedRangeReader.mergeRanges(ranges, CoalescedRangeReader.DEFAULT_MAX_COALESCE_GAP)) {
                    rg0Prefetch += merged.length();
                }
            }
            return new FooterMetrics(indexSpan, dictBloom, rg0Prefetch);
        }
    }

    private static byte[] parquetFile(int rows, long rowGroupSize) throws IOException {
        return parquetFile(rows, rowGroupSize, CompressionCodecName.UNCOMPRESSED, false, false);
    }

    private static byte[] parquetFile(int rows, long rowGroupSize, CompressionCodecName codec, boolean sortCategory, boolean extraPredicate)
        throws IOException {
        MessageType schema;
        if (extraPredicate) {
            schema = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.INT32)
                .named("id")
                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("category")
                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("tag")
                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                .named("pad")
                .named("preload_floor");
        } else {
            schema = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.INT32)
                .named("id")
                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("category")
                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                .named("pad")
                .named("preload_floor");
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(out))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(codec)
                .withDictionaryEncoding(true)
                .withDictionaryEncoding("pad", false)
                .withRowGroupSize(rowGroupSize)
                .withPageSize(8 * 1024)
                .withDictionaryPageSize(64 * 1024)
                .build()
        ) {
            String[] categories = new String[DICT_CARDINALITY];
            String[] tags = extraPredicate ? new String[DICT_CARDINALITY] : null;
            for (int c = 0; c < DICT_CARDINALITY; c++) {
                categories[c] = c < 10 ? "cat_0" + c : "cat_" + c;
                if (tags != null) {
                    tags[c] = c < 10 ? "tag_0" + c : "tag_" + c;
                }
            }
            if (sortCategory) {
                for (int c = 0; c < DICT_CARDINALITY; c++) {
                    for (int i = 0; i < rows; i++) {
                        if (i % DICT_CARDINALITY == c) {
                            writeRow(writer, groups, i, categories[c], tags == null ? null : tags[c]);
                        }
                    }
                }
            } else {
                for (int i = 0; i < rows; i++) {
                    int c = i % DICT_CARDINALITY;
                    writeRow(writer, groups, i, categories[c], tags == null ? null : tags[c]);
                }
            }
        }
        return out.toByteArray();
    }

    private static void writeRow(ParquetWriter<Group> writer, SimpleGroupFactory groups, int id, String category, String tag)
        throws IOException {
        byte[] pad = new byte[PAD_PER_ROW];
        random().nextBytes(pad);
        Group group = groups.newGroup().append("id", id).append("category", category);
        if (tag != null) {
            group.append("tag", tag);
        }
        group.append("pad", Binary.fromConstantByteArray(pad));
        writer.write(group);
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
                return "memory://preload-floor.parquet";
            }
        };
    }

    /**
     * In-memory storage with native async reads, so the optimized iterator takes its first ticket at
     * construction and the preload goes through {@code CoalescedRangeReader}. Counts requested bytes.
     */
    private static final class CountingAsyncStorage extends AbstractTestStorageObject {
        private final byte[] data;
        private final ExecutorService asyncIo;
        private final AtomicLong asyncBytes = new AtomicLong();

        private CountingAsyncStorage(byte[] data, ExecutorService asyncIo) {
            this.data = data;
            this.asyncIo = asyncIo;
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
            return StoragePath.of("memory://preload-floor.parquet");
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
            asyncBytes.addAndGet(length);
            DirectReadBuffer drb;
            try {
                drb = factory.allocateWritableWindow((int) length);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            asyncIo.execute(() -> {
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
}
