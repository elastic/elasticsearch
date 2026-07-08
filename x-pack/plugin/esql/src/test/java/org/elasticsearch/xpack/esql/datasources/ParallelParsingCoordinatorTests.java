/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStats;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalStatsCapture;
import org.elasticsearch.xpack.esql.datasources.cache.StatsCapturingIterator;
import org.elasticsearch.xpack.esql.datasources.spi.BufferingPageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StripeColumnScope;
import org.hamcrest.Matchers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ParallelParsingCoordinatorTests extends ESTestCase {

    private static final List<Attribute> SCHEMA = List.of(
        new FieldAttribute(Source.EMPTY, "line", new EsField("line", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
    );

    public void testComputeSegmentsSimple() throws IOException {
        String content = "line1\nline2\nline3\nline4\nline5\nline6\n";
        StorageObject obj = new InMemoryStorageObject(content.getBytes(StandardCharsets.UTF_8));
        NewlineSegmentableReader reader = new NewlineSegmentableReader(1);

        List<long[]> segments = ParallelParsingCoordinator.computeSegments(reader, obj, content.length(), 3, 1);

        assertTrue("Should have multiple segments", segments.size() > 1);

        long totalCoverage = 0;
        for (long[] seg : segments) {
            totalCoverage += seg[1];
        }
        assertEquals("Segments must cover entire file", content.length(), totalCoverage);

        for (int i = 1; i < segments.size(); i++) {
            assertEquals("Segments must be contiguous", segments.get(i - 1)[0] + segments.get(i - 1)[1], segments.get(i)[0]);
        }
    }

    public void testComputeSegmentsSmallFile() throws IOException {
        String content = "ab\n";
        StorageObject obj = new InMemoryStorageObject(content.getBytes(StandardCharsets.UTF_8));
        NewlineSegmentableReader reader = new NewlineSegmentableReader(64 * 1024);

        List<long[]> segments = ParallelParsingCoordinator.computeSegments(reader, obj, content.length(), 4, 64 * 1024);

        assertEquals("Small file should produce single segment", 1, segments.size());
        assertEquals(0, segments.get(0)[0]);
        assertEquals(content.length(), segments.get(0)[1]);
    }

    public void testComputeSegmentsAlignsToBoundaries() throws IOException {
        String content = "aaaa\nbbbb\ncccc\ndddd\n";
        StorageObject obj = new InMemoryStorageObject(content.getBytes(StandardCharsets.UTF_8));
        NewlineSegmentableReader reader = new NewlineSegmentableReader(1);

        List<long[]> segments = ParallelParsingCoordinator.computeSegments(reader, obj, content.length(), 2, 1);

        for (int i = 1; i < segments.size(); i++) {
            long offset = segments.get(i)[0];
            byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
            assertTrue("Segment boundary at " + offset + " should follow a newline", offset == 0 || bytes[(int) offset - 1] == '\n');
        }
    }

    /**
     * Regression guard: {@link ParallelParsingCoordinator#computeSegments} opens a range stream for
     * each nominal split probe, reads only enough bytes to find the next record boundary, then must
     * call {@link StorageObject#abortStream} — not a draining {@code close()}.
     */
    public void testComputeSegmentsDoesNotDrainStream() throws IOException {
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();

        StringBuilder csv = new StringBuilder("id,name\n");
        while (csv.length() < 3 * 1024 * 1024) {
            csv.append(csv.length()).append(",value\n");
        }
        byte[] payload = csv.toString().getBytes(StandardCharsets.UTF_8);
        long fileLength = payload.length;

        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject object = DrainSimulatingStorageObject.create(payload, tracking);

        CsvFormatReader csvReader = new CsvFormatReader(blockFactory);
        List<long[]> segments = ParallelParsingCoordinator.computeSegments(
            csvReader,
            object,
            fileLength,
            4,
            csvReader.minimumSegmentSize()
        );

        assertThat("expected multiple parse segments", segments.size(), Matchers.greaterThan(1));
        assertTrue("each segment probe must abort the underlying stream", tracking.abortCalls.get() >= segments.size() - 1);
        assertThat(
            "segment probes must not drain the range streams; consumed " + tracking.bytesConsumed.get() + " of " + fileLength + " bytes",
            tracking.bytesConsumed.get(),
            Matchers.lessThan(fileLength / 2)
        );
    }

    /**
     * Completeness across a multi-segment file: every row is emitted exactly once. This replaces a former
     * "preserves order" assertion. Pages are now emitted in completion (as-ready) order, not segment order,
     * so cross-segment row order is intentionally not preserved (an external scan has no order guarantee
     * absent an explicit SORT). The contract this test guards is exactly-once delivery of all rows, so it
     * sorts before comparing.
     */
    public void testParallelReadEmitsAllRowsExactlyOnce() throws Exception {
        StringBuilder sb = new StringBuilder();
        int lineCount = 200;
        for (int i = 0; i < lineCount; i++) {
            sb.append("line-").append(String.format(java.util.Locale.ROOT, "%04d", i)).append("\n");
        }
        String content = sb.toString();
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        StorageObject obj = new InMemoryStorageObject(contentBytes);
        BlockFactory blockFactory = blockFactory();
        LineFormatReader reader = new LineFormatReader(blockFactory);

        // Verify segments cover the full file
        List<long[]> segments = ParallelParsingCoordinator.computeSegments(reader, obj, contentBytes.length, 4, 1);
        long totalCoverage = 0;
        for (long[] seg : segments) {
            totalCoverage += seg[1];
        }
        assertEquals("Segments must cover entire file", contentBytes.length, totalCoverage);
        assertThat("test needs a genuinely multi-segment file to be meaningful", segments.size(), Matchers.greaterThan(1));

        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 50, 4, exec);

            List<String> allLines = new ArrayList<>();
            try (iter) {
                while (iter.hasNext()) {
                    Page page = iter.next();
                    BytesRefBlock block = (BytesRefBlock) page.getBlock(0);
                    BytesRef scratch = new BytesRef();
                    for (int i = 0; i < page.getPositionCount(); i++) {
                        allLines.add(block.getBytesRef(i, scratch).utf8ToString());
                    }
                    page.releaseBlocks();
                }
            }

            assertEquals("All lines should be read", lineCount, allLines.size());
            // Order-insensitive: emission order is as-ready, so sort before comparing the set.
            List<String> sorted = new ArrayList<>(allLines);
            Collections.sort(sorted);
            for (int i = 0; i < lineCount; i++) {
                assertEquals(
                    "every line must appear exactly once",
                    "line-" + String.format(java.util.Locale.ROOT, "%04d", i),
                    sorted.get(i)
                );
            }
        } finally {
            exec.shutdown();
        }
    }

    public void testParallelReadSingleThread() throws Exception {
        String content = "alpha\nbeta\ngamma\n";
        StorageObject obj = new InMemoryStorageObject(content.getBytes(StandardCharsets.UTF_8));
        BlockFactory blockFactory = blockFactory();
        LineFormatReader reader = new LineFormatReader(blockFactory);

        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 100, 1, exec);

            List<String> allLines = new ArrayList<>();
            try (iter) {
                while (iter.hasNext()) {
                    Page page = iter.next();
                    BytesRefBlock block = (BytesRefBlock) page.getBlock(0);
                    BytesRef scratch = new BytesRef();
                    for (int i = 0; i < page.getPositionCount(); i++) {
                        allLines.add(block.getBytesRef(i, scratch).utf8ToString());
                    }
                    page.releaseBlocks();
                }
            }

            assertEquals(3, allLines.size());
            assertEquals("alpha", allLines.get(0));
            assertEquals("beta", allLines.get(1));
            assertEquals("gamma", allLines.get(2));
        } finally {
            exec.shutdown();
        }
    }

    /**
     * Early-close leak regression for {@code AsReadyParallelIterator}'s own look-ahead. Distinct from the
     * per-reader {@link BufferingPageIterator} buffer: the coordinator parks one as-ready page in its
     * private {@code buffered} field when {@code hasNext()} runs ahead of {@code next()}. A consumer that
     * aborts after {@code hasNext()} (a pushed-down {@code LIMIT}, a cancellation, a downstream error)
     * reaches {@code close()} with that page still parked; {@code close()} must release it (and drain the
     * shared queue, and wait out the workers so each releases its own in-flight page) or the breaker leaks.
     * A genuinely multi-segment file is required so the parallel path engages rather than the single-stream
     * fallback (which returns the reader's own iterator directly).
     */
    public void testCloseReleasesBufferedPageOnEarlyTermination() throws Exception {
        StringBuilder sb = new StringBuilder();
        int lineCount = 500;
        for (int i = 0; i < lineCount; i++) {
            sb.append("line-").append(String.format(java.util.Locale.ROOT, "%04d", i)).append('\n');
        }
        byte[] contentBytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        StorageObject obj = new InMemoryStorageObject(contentBytes);

        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(64)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory trackingFactory = BlockFactory.builder(bigArrays).breaker(breaker).build();
        LineFormatReader reader = new LineFormatReader(trackingFactory);

        // Guard: the test only exercises AsReadyParallelIterator if the file actually splits.
        assertThat(
            "test needs a genuinely multi-segment file or it would hit the single-stream fallback",
            ParallelParsingCoordinator.computeSegments(reader, obj, contentBytes.length, 4, 1).size(),
            Matchers.greaterThan(1)
        );

        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            // Small batch + many lines => several pages produced; a hasNext() parks one in `buffered`.
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 8, 4, exec);
            assertTrue("workers should produce at least one page", iter.hasNext());
            assertThat("hasNext must have parked a buffered page", breaker.getUsed(), Matchers.greaterThan(0L));

            // Abandon without draining: this is the LIMIT / cancel / downstream-error shape.
            iter.close();

            assertEquals(
                "close() must release the parked buffered page, drain the queue, and let workers release their in-flight pages",
                0L,
                breaker.getUsed()
            );
        } finally {
            exec.shutdown();
            assertTrue("executor did not terminate", exec.awaitTermination(60, TimeUnit.SECONDS));
        }
    }

    /**
     * Contract: when a consumer wraps {@link ParallelParsingCoordinator#parallelRead}'s outer
     * iterator with a {@link StatsCapturingIterator}-bound sink, every per-segment chunk's
     * {@link ExternalStatsCapture#record} contribution must reach that sink — regardless of which
     * worker thread the chunk's iterator was drained on. The production {@code CsvFormatReader} /
     * {@code NdJsonPageIterator} close hooks call {@code ExternalStatsCapture.record(...)} once
     * per drained chunk; the coordinator-side reconciler relies on those partials plus the outer
     * finalize marker to reconstruct a whole-file stat. Lose the partials, lose the warm-path
     * aggregate pushdown for any file large enough to be parallel-parsed.
     * <p>
     * {@code ExternalStatsCapture}'s active sink is a plain {@link ThreadLocal} that does not
     * propagate to executor threads, so the coordinator threads through an explicit
     * {@code captureSink} parameter and binds it around the per-segment {@code reader.read(...)
     * .close()} block inside {@code parseSegment}. This test asserts that bind actually wires
     * each worker's published contribution into the consumer-owned sink.
     */
    public void testParallelReadPropagatesPerSegmentPartialsToSink() throws Exception {
        // Build a multi-segment file. Lines are short so 4 workers each see at least one chunk
        // when parallelism=4 + minimumSegmentSize=1.
        StringBuilder sb = new StringBuilder();
        int lineCount = 200;
        for (int i = 0; i < lineCount; i++) {
            sb.append("line-").append(String.format(java.util.Locale.ROOT, "%04d", i)).append("\n");
        }
        byte[] contentBytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        StorageObject obj = new InMemoryStorageObject(contentBytes);
        String path = obj.path().toString();
        StatsPublishingLineReader reader = new StatsPublishingLineReader(blockFactory(), path);

        // Sanity: this fixture must actually exercise the multi-segment path, otherwise the bug
        // is masked by the single-segment fast path that runs inline on the caller thread.
        List<long[]> segments = ParallelParsingCoordinator.computeSegments(
            reader,
            obj,
            contentBytes.length,
            4,
            reader.minimumSegmentSize()
        );
        assertThat("test must drive the multi-segment worker path", segments.size(), Matchers.greaterThanOrEqualTo(2));

        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            // Use the explicit-sink overload: the coordinator binds `sink` on each worker around
            // the per-segment reader.read(...).close() so the close hook's record() call lands in
            // the same map the consumer's StatsCapturingIterator binds for the outer finalize marker.
            CloseableIterator<Page> outer = ParallelParsingCoordinator.parallelRead(
                reader,
                obj,
                List.of("line"),
                50,
                4,
                exec,
                null,
                false,
                true,
                null,
                ParallelParsingCoordinator.DEFAULT_MAX_CONCURRENT_OPEN_SEGMENTS,
                sink
            );
            try (CloseableIterator<Page> iter = StatsCapturingIterator.wrap(outer, sink)) {
                while (iter.hasNext()) {
                    iter.next().releaseBlocks();
                }
            }
        } finally {
            exec.shutdown();
        }

        List<Map<String, Object>> contributions = sink.getOrDefault(path, List.of());
        long partialCount = contributions.stream().filter(m -> Boolean.TRUE.equals(m.get(ExternalStats.PARTIAL_CHUNK_KEY))).count();

        // One partial per segment is the contract. Cap the lower bound at "at least 2" so the
        // assertion stays stable against future tweaks to the boundary-probing heuristic.
        assertThat(
            "Parallel parsing must propagate per-segment partials to the bound sink. Saw "
                + contributions.size()
                + " total contributions, "
                + partialCount
                + " partials, across "
                + segments.size()
                + " segments. Contributions: "
                + contributions,
            partialCount,
            Matchers.greaterThanOrEqualTo(2L)
        );
    }

    public void testParallelReadEmptyFile() throws Exception {
        StorageObject obj = new InMemoryStorageObject(new byte[0]);
        BlockFactory blockFactory = blockFactory();
        LineFormatReader reader = new LineFormatReader(blockFactory);

        ExecutorService exec = Executors.newFixedThreadPool(2);
        try {
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 100, 4, exec);

            try (iter) {
                assertFalse("Empty file should produce no pages", iter.hasNext());
            }
        } finally {
            exec.shutdown();
        }
    }

    public void testFindNextRecordBoundaryNewline() throws IOException {
        NewlineSegmentableReader reader = new NewlineSegmentableReader(1);

        byte[] data = "abcde\nfghij\n".getBytes(StandardCharsets.UTF_8);
        try (InputStream stream = new ByteArrayInputStream(data)) {
            long skipped = reader.recordSplitter(SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES).findNextRecordBoundary(stream);
            assertEquals(6, skipped);
        }
    }

    public void testFindNextRecordBoundaryCRLF() throws IOException {
        NewlineSegmentableReader reader = new NewlineSegmentableReader(1);

        byte[] data = "abcde\r\nfghij\n".getBytes(StandardCharsets.UTF_8);
        try (InputStream stream = new ByteArrayInputStream(data)) {
            long skipped = reader.recordSplitter(SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES).findNextRecordBoundary(stream);
            assertEquals(7, skipped);
        }
    }

    public void testFindNextRecordBoundaryEOF() throws IOException {
        NewlineSegmentableReader reader = new NewlineSegmentableReader(1);

        byte[] data = "no-newline-here".getBytes(StandardCharsets.UTF_8);
        try (InputStream stream = new ByteArrayInputStream(data)) {
            long skipped = reader.recordSplitter(SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES).findNextRecordBoundary(stream);
            assertEquals(-1, skipped);
        }
    }

    public void testParallelReadHandlesRejectedExecution() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.append("line-").append(i).append("\n");
        }
        String content = sb.toString();
        StorageObject obj = new InMemoryStorageObject(content.getBytes(StandardCharsets.UTF_8));
        BlockFactory blockFactory = blockFactory();
        LineFormatReader reader = new LineFormatReader(blockFactory);

        ExecutorService exec = Executors.newFixedThreadPool(2);
        exec.shutdown();

        RuntimeException ex = expectThrows(RuntimeException.class, () -> {
            try (CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 50, 4, exec)) {
                while (iter.hasNext()) {
                    Page page = iter.next();
                    page.releaseBlocks();
                }
            }
        });
        assertNotNull("Should propagate rejection error", ex);
    }

    private static final int REPRO_PARALLELISM = 12;
    private static final int REPRO_POOL_SIZE = 8; // wider than the small cap, so an uncapped run can open many at once
    private static final int REPRO_SMALL_CAP = 3;

    /**
     * Hard regression gate for the fix: with {@code max_concurrent_open_segments = cap}, the number of
     * concurrently-open object-store range streams never exceeds {@code cap}, no matter how many segments
     * or parser threads exist. This is an upper bound the window enforces regardless of thread scheduling,
     * so it is not timing-dependent. {@link StreamCountingStorageObject} holds each open briefly so that a
     * regressed (uncapped) implementation would open more than {@code cap} at once and trip this assertion.
     */
    public void testCapBoundsConcurrentOpenSegments() throws Exception {
        byte[] content = repeatedLines(1200);
        int cappedPeak = peakConcurrentOpensFor(content, REPRO_PARALLELISM, REPRO_SMALL_CAP, REPRO_POOL_SIZE);
        assertThat(
            "peak concurrently-open range streams [" + cappedPeak + "] must not exceed the cap [" + REPRO_SMALL_CAP + "]",
            cappedPeak,
            Matchers.lessThanOrEqualTo(REPRO_SMALL_CAP)
        );
    }

    /**
     * Best-effort reproduction of the unbounded-fanout congestion (in plain terms:
     * a wide read opened every segment's stream at once and pinned the heap). Run uncapped, the peak should
     * climb well above the small cap. Demonstrating that requires threads to actually overlap, which a
     * heavily loaded or serializing scheduler cannot guarantee, so this is an {@code assumeTrue} diagnostic,
     * not a hard gate — the hard invariant lives in {@link #testCapBoundsConcurrentOpenSegments}.
     */
    public void testUncappedReproducesCongestion() throws Exception {
        byte[] content = repeatedLines(1200);
        int segmentCount = ParallelParsingCoordinator.computeSegments(
            new LineFormatReader(blockFactory()),
            new StreamCountingStorageObject(content),
            content.length,
            REPRO_PARALLELISM,
            1
        ).size();
        assertThat("test needs more segments than the small cap to be meaningful", segmentCount, Matchers.greaterThan(REPRO_SMALL_CAP));

        // cap >= segment count == no effective cap (the pre-fix shape).
        int congestedPeak = peakConcurrentOpensFor(content, REPRO_PARALLELISM, segmentCount, REPRO_POOL_SIZE);
        assumeTrue(
            "uncapped run only congests when threads overlap; skipped on a serializing/loaded scheduler (peak="
                + congestedPeak
                + ", cap="
                + REPRO_SMALL_CAP
                + ")",
            congestedPeak > REPRO_SMALL_CAP
        );
    }

    /** Runs the parallel read once with the given {@code maxConcurrentOpenSegments} and returns the peak concurrent opens. */
    private int peakConcurrentOpensFor(byte[] content, int parallelism, int maxConcurrentOpenSegments, int poolSize) throws Exception {
        StreamCountingStorageObject obj = new StreamCountingStorageObject(content);
        LineFormatReader reader = new LineFormatReader(blockFactory());
        ExecutorService exec = Executors.newFixedThreadPool(poolSize);
        try {
            try (
                CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(
                    reader,
                    obj,
                    List.of("line"),
                    50,
                    parallelism,
                    exec,
                    null,
                    false,
                    true,
                    null,
                    maxConcurrentOpenSegments,
                    null
                )
            ) {
                while (iter.hasNext()) {
                    iter.next().releaseBlocks();
                }
            }
        } finally {
            exec.shutdown();
            assertTrue("executor did not terminate", exec.awaitTermination(60, TimeUnit.SECONDS));
        }
        assertThat("sanity: more range streams opened than the cap", obj.totalOpens(), Matchers.greaterThan(maxConcurrentOpenSegments));
        return obj.peakConcurrent();
    }

    private static byte[] repeatedLines(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.append("line-").append(String.format(java.util.Locale.ROOT, "%05d", i)).append("\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Tightest window: {@code max_concurrent_open_segments == 1}. Asserts only one segment stream is ever
     * open at a time and the read completes (no deadlock). At cap=1 segments run strictly one after another
     * — the next is submitted only in the previous worker's finally — so the single FIFO shared queue
     * happens to yield rows in file order. That ordering is a <em>consequence</em> of serial execution here,
     * not a general contract: with cap &gt; 1 pages emit in as-ready order (see
     * {@link #testParallelReadEmitsAllRowsExactlyOnce} and {@link #testEmitsReadySegmentBeforeSlowerEarlierSegment}).
     */
    public void testCapOfOneIsSerialPreservesOrderAndProgresses() throws Exception {
        final int parallelism = 8; // many threads available, but the cap must keep opens to 1
        int rows = 600;
        byte[] content = repeatedLines(rows);
        StreamCountingStorageObject obj = new StreamCountingStorageObject(content);
        LineFormatReader reader = new LineFormatReader(blockFactory());

        List<String> read = new ArrayList<>();
        ExecutorService exec = Executors.newFixedThreadPool(parallelism);
        try {
            try (
                CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(
                    reader,
                    obj,
                    List.of("line"),
                    50,
                    parallelism,
                    exec,
                    null,
                    false,
                    true,
                    null,
                    1,
                    null
                )
            ) {
                while (iter.hasNext()) {
                    Page p = iter.next();
                    BytesRefBlock block = (BytesRefBlock) p.getBlock(0);
                    BytesRef scratch = new BytesRef();
                    for (int i = 0; i < block.getPositionCount(); i++) {
                        read.add(block.getBytesRef(block.getFirstValueIndex(i), scratch).utf8ToString());
                    }
                    p.releaseBlocks();
                }
            }
        } finally {
            exec.shutdown();
            assertTrue("executor did not terminate (possible deadlock at cap=1)", exec.awaitTermination(60, TimeUnit.SECONDS));
        }

        assertThat("cap=1 must keep at most one segment stream open", obj.peakConcurrent(), Matchers.lessThanOrEqualTo(1));
        assertThat("all rows must be read", read.size(), Matchers.equalTo(rows));
        for (int i = 0; i < rows; i++) {
            assertThat("rows must stay in file order", read.get(i), Matchers.equalTo(String.format(java.util.Locale.ROOT, "line-%05d", i)));
        }
        assertThat("no segment stream may be left open after close", obj.currentOpen(), Matchers.equalTo(0));
    }

    /**
     * Mid-stream teardown: the consumer abandons after a few pages and closes the iterator. Exercises the
     * close path + the early-return guard + the window cascade for not-yet-run segments. Asserts close
     * returns (no hang) and no segment read stream is left open — i.e. the cascade still poisons/finishes
     * every segment so nothing leaks an object-store stream.
     */
    public void testCloseMidStreamLeaksNoOpenStreams() throws Exception {
        final int parallelism = 6;
        byte[] content = repeatedLines(2000);
        StreamCountingStorageObject obj = new StreamCountingStorageObject(content);
        LineFormatReader reader = new LineFormatReader(blockFactory());

        ExecutorService exec = Executors.newFixedThreadPool(parallelism);
        try {
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(
                reader,
                obj,
                List.of("line"),
                50,
                parallelism,
                exec,
                null,
                false,
                true,
                null,
                3,
                null
            );
            // Consume just a couple of pages, then abandon the rest and close early.
            int pages = 0;
            while (iter.hasNext() && pages < 2) {
                iter.next().releaseBlocks();
                pages++;
            }
            iter.close();
            assertThat("expected to consume at least one page before closing", pages, Matchers.greaterThan(0));
        } finally {
            exec.shutdown();
            assertTrue("executor did not terminate after early close", exec.awaitTermination(60, TimeUnit.SECONDS));
        }

        assertThat("early close must leave no segment stream open", obj.currentOpen(), Matchers.equalTo(0));
    }

    /**
     * Wiring test for {@code reader.pool.rejected}: when the parser executor refuses a segment submission
     * ({@link RejectedExecutionException} — a saturated or shutting-down pool), the coordinator records one
     * {@code reader.pool.rejected} event per rejected segment on the attached {@link ExternalSourceMetrics}.
     * Drives the real {@code submitSegment} rejection path against a registry-backed holder (no mocks) and
     * asserts the observable registry counter, not just that the read fails. The end-to-end integration test
     * cannot force this deterministically, so this unit test is the coverage for the counter.
     */
    public void testReaderPoolRejectionRecordsReaderPoolRejected() throws Exception {
        StringBuilder sb = new StringBuilder();
        int lineCount = 200;
        for (int i = 0; i < lineCount; i++) {
            sb.append("line-").append(String.format(java.util.Locale.ROOT, "%04d", i)).append("\n");
        }
        byte[] contentBytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        StorageObject obj = new InMemoryStorageObject(contentBytes);
        LineFormatReader reader = new LineFormatReader(blockFactory());

        // The fixture must be genuinely multi-segment, else the coordinator takes the single-threaded fallback
        // that never submits to the executor (and so never rejects).
        assertThat(
            "test needs a multi-segment file to reach the executor submission path",
            ParallelParsingCoordinator.computeSegments(reader, obj, contentBytes.length, 4, 1).size(),
            Matchers.greaterThan(1)
        );

        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        ExternalSourceMetrics metrics = new ExternalSourceMetrics(registry);
        // An executor that refuses every submission, exactly as a saturated / shutting-down parser pool would.
        Executor rejecting = command -> { throw new RejectedExecutionException("parser pool saturated"); };

        CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(
            reader,
            obj,
            List.of("line"),
            50,
            4,
            rejecting,
            null,
            true,
            true,
            null,
            0L,
            ParallelParsingCoordinator.DEFAULT_MAX_CONCURRENT_OPEN_SEGMENTS,
            null,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            -1L,
            StripeColumnScope.PROJECTED,
            false,
            metrics
        );
        // Every segment submission is rejected: firstError is set and draining surfaces it. We only assert the
        // telemetry moved; the surfaced failure is expected.
        expectThrows(Exception.class, () -> {
            try (iter) {
                while (iter.hasNext()) {
                    iter.next().releaseBlocks();
                }
            }
        });

        long rejected = registry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, ExternalSourceMetrics.READER_POOL_REJECTED_TOTAL)
            .stream()
            .mapToLong(Measurement::getLong)
            .sum();
        assertThat("a rejected parser-segment submission must bump reader.pool.rejected", rejected, Matchers.greaterThanOrEqualTo(1L));
    }

    public void testParallelReadPropagatesError() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.append("line-").append(i).append("\n");
        }
        String content = sb.toString();
        StorageObject obj = new InMemoryStorageObject(content.getBytes(StandardCharsets.UTF_8));
        FailingFormatReader reader = new FailingFormatReader(blockFactory(), 5);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 10, 4, exec);

            RuntimeException ex = expectThrows(RuntimeException.class, () -> {
                try (iter) {
                    while (iter.hasNext()) {
                        Page page = iter.next();
                        page.releaseBlocks();
                    }
                }
            });
            assertTrue(
                "Should contain the injected error message",
                ex.getMessage().contains("injected") || (ex.getCause() != null && ex.getCause().getMessage().contains("injected"))
            );
        } finally {
            exec.shutdown();
        }
    }

    /**
     * Pins the production wiring for elastic/esql-planning#836 on the parallel coordinator: a worker
     * IOException surfaces as a typed {@link ExternalClientException} at the iterator's {@code hasNext()}
     * boundary (mirroring {@code CsvFormatReader} / {@code NdJsonPageIterator}), the coordinator stores it
     * in {@code firstError}, and {@code checkError()}'s {@code surface()} passes it through unchanged so the
     * 400 status survives end-to-end instead of being downgraded to a generic 500 wrapper.
     */
    public void testParallelReadSurfacesIoFailureAsExternalClientException() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.append("line-").append(i).append("\n");
        }
        StorageObject obj = new InMemoryStorageObject(sb.toString().getBytes(StandardCharsets.UTF_8));
        FailingFormatReader reader = new FailingFormatReader(blockFactory(), 5);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 10, 4, exec);

            RuntimeException ex = expectThrows(RuntimeException.class, () -> {
                try (iter) {
                    while (iter.hasNext()) {
                        iter.next().releaseBlocks();
                    }
                }
            });
            assertThat(
                "iterator-boundary IOException must surface as a typed ExternalClientException, not a generic RuntimeException",
                ex,
                Matchers.instanceOf(ExternalClientException.class)
            );
            assertEquals(
                "ExternalClientException must classify as HTTP 400 so the read failure stops being labeled as a server fault",
                RestStatus.BAD_REQUEST,
                ExceptionsHelper.status(ex)
            );
            assertThat(
                "the original IOException must remain reachable as the cause",
                ex.getCause(),
                Matchers.instanceOf(IOException.class)
            );
            assertThat("the injected detail must survive end-to-end", ex.getMessage(), Matchers.containsString("injected"));
        } finally {
            exec.shutdown();
        }
    }

    /**
     * Verifies the per-segment context flags set by {@link ParallelParsingCoordinator}:
     * <ul>
     *   <li>Exactly one segment owns the file's leading bytes ({@code firstSplit=true}).</li>
     *   <li>Exactly one segment runs to file end ({@code lastSplit=true}); non-final segments must
     *       not be marked lastSplit so codecs/readers correctly handle the segment-boundary tail.</li>
     *   <li>Every segment is {@code recordAligned=true} so line-oriented readers can skip the
     *       leading-partial-line trim that byte-range macro-splits otherwise need.</li>
     * </ul>
     */
    public void testParseSegmentSetsExpectedSplitFlags() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            sb.append("line-").append(String.format(java.util.Locale.ROOT, "%04d", i)).append("\n");
        }
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        StorageObject obj = new InMemoryStorageObject(content);
        BlockFactory blockFactory = blockFactory();
        ContextCapturingLineReader reader = new ContextCapturingLineReader(blockFactory);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 50, 4, exec);
            try (iter) {
                while (iter.hasNext()) {
                    iter.next().releaseBlocks();
                }
            }
        } finally {
            exec.shutdown();
        }

        List<FormatReadContext> seen;
        synchronized (reader.contexts) {
            seen = new ArrayList<>(reader.contexts);
        }
        assertTrue("Expected at least 2 segments, recorded " + seen.size(), seen.size() >= 2);
        int firstSplitCount = 0;
        int lastSplitCount = 0;
        for (int i = 0; i < seen.size(); i++) {
            FormatReadContext ctx = seen.get(i);
            if (ctx.firstSplit()) {
                firstSplitCount++;
            }
            if (ctx.lastSplit()) {
                lastSplitCount++;
            }
            assertTrue("segment[" + i + "] must have recordAligned=true", ctx.recordAligned());
        }
        assertEquals("exactly one segment must own the file's leading bytes", 1, firstSplitCount);
        assertEquals("exactly one segment must run to file end", 1, lastSplitCount);
    }

    public void testParseSegmentHonorsNonLeadingMacroSplitFirstFlag() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            sb.append("line-").append(String.format(java.util.Locale.ROOT, "%04d", i)).append("\n");
        }
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        StorageObject obj = new InMemoryStorageObject(content);
        BlockFactory blockFactory = blockFactory();
        ContextCapturingLineReader reader = new ContextCapturingLineReader(blockFactory);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(
                reader,
                obj,
                List.of("line"),
                50,
                4,
                exec,
                null,
                true,
                false
            );
            try (iter) {
                while (iter.hasNext()) {
                    iter.next().releaseBlocks();
                }
            }
        } finally {
            exec.shutdown();
        }

        List<FormatReadContext> seen;
        synchronized (reader.contexts) {
            seen = new ArrayList<>(reader.contexts);
        }
        assertTrue("Expected at least 2 segments, recorded " + seen.size(), seen.size() >= 2);
        int firstSplitCount = 0;
        int lastSplitCount = 0;
        for (FormatReadContext ctx : seen) {
            if (ctx.firstSplit()) {
                firstSplitCount++;
            }
            if (ctx.lastSplit()) {
                lastSplitCount++;
            }
            assertTrue("non-leading macro split still starts on a record boundary", ctx.recordAligned());
        }
        assertEquals("non-leading macro split must not mark any parallel segment as firstSplit", 0, firstSplitCount);
        assertEquals("exactly one segment must run to file end", 1, lastSplitCount);
    }

    /**
     * Files smaller than {@code 2 * minimumSegmentSize()} fall back to single-threaded reading;
     * the coordinator skips segment computation and forwards the original {@link StorageObject}
     * directly. Verifies that NDJSON's bumped 4 MiB threshold (Stage 5) actually enforces the
     * "no parallelism below ~8 MiB" contract documented on {@code minimumSegmentSize()}.
     */
    public void testFallsBackToSingleThreadedReadWhenFileTooSmall() throws Exception {
        // 64 KiB minimum vs ~21 byte fixture: file is far below `2 * minSegmentSize`, must fall
        // back to a whole-file read instead of fanning out across the executor.
        byte[] content = "line-a\nline-b\nline-c\n".getBytes(StandardCharsets.UTF_8);
        StorageObject obj = new InMemoryStorageObject(content);
        ContextRecordingFormatReader reader = new ContextRecordingFormatReader(blockFactory(), 64 * 1024);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 100, 4, exec);
            try (iter) {
                while (iter.hasNext()) {
                    iter.next().releaseBlocks();
                }
            }
        } finally {
            exec.shutdown();
        }

        List<FormatReadContext> seen = reader.contexts();
        assertEquals("Below the 2*minSegmentSize threshold, parallelRead must call read() exactly once", 1, seen.size());
        // Single-threaded fallback uses the baseCtx with the builder's defaults (firstSplit=true,
        // lastSplit=true) - the file is read whole, so it is by definition both the first and last
        // split. The parallel path sets the same flags but only after slicing.
        FormatReadContext only = seen.get(0);
        assertTrue("Whole-file fallback path must mark firstSplit=true", only.firstSplit());
        assertTrue("Whole-file fallback path must mark lastSplit=true", only.lastSplit());
    }

    /**
     * Regression: {@code COUNT(*)} passes empty projected columns; parallel segment workers must
     * still see the file column width via metadata-bound schema (otherwise structural validation
     * compares rows against schema size 0).
     */
    public void testParallelReadEmptyProjectionInfersCsvSchemaBeforeSegments() throws Exception {
        String header = "a,b,c\n";
        String row = "1,2,3\n";
        StringBuilder sb = new StringBuilder(header);
        while (sb.length() < 3 * 1024 * 1024) {
            sb.append(row);
        }
        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        StorageObject obj = new InMemoryStorageObject(bytes);
        CsvFormatReader reader = new CsvFormatReader(blockFactory());

        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of(), 500, 4, exec);
            long rows = 0;
            try (iter) {
                while (iter.hasNext()) {
                    Page p = iter.next();
                    rows += p.getPositionCount();
                    p.releaseBlocks();
                }
            }
            assertTrue(rows > 0);
        } finally {
            exec.shutdown();
        }
    }

    public void testParallelReadEmptyProjectionNonLeadingCsvMacroSplitSkipsMetadataRebind() throws Exception {
        String header = "a,b,c\n";
        StringBuilder sb = new StringBuilder(header);
        while (sb.length() < 3 * 1024 * 1024) {
            sb.append("1,2,3\n");
        }
        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        InMemoryStorageObject full = new InMemoryStorageObject(bytes);
        long headerBytes = header.getBytes(StandardCharsets.UTF_8).length;
        long bodyLength = bytes.length - headerBytes;
        assertTrue("payload must exceed 2*minimumSegmentSize for parallel parsing", bodyLength > 2 * 1024 * 1024);
        StorageObject nonLeadingRange = new RangeStorageObject(full, headerBytes, bodyLength);

        CsvFormatReader base = new CsvFormatReader(blockFactory());
        SourceMetadata meta = base.metadata(full);
        CsvFormatReader withSchema = base.withSchema(meta.schema());

        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(
                withSchema,
                nonLeadingRange,
                List.of(),
                500,
                4,
                exec,
                null,
                true,
                false
            );
            long rows = 0;
            try (iter) {
                while (iter.hasNext()) {
                    Page p = iter.next();
                    rows += p.getPositionCount();
                    p.releaseBlocks();
                }
            }
            assertTrue(rows > 0);
        } finally {
            exec.shutdown();
        }
    }

    /**
     * Regression: a non-leading record-aligned macro-split must set {@code recordAligned=true}
     * in the fallback context; otherwise CsvFormatReader drops the first data row (treats it as
     * a partial-line fragment from the previous split). Validates both paths and asserts the
     * row-count difference.
     */
    public void testCsvNonLeadingMacroSplitRecordAlignedPreservesAllRows() throws Exception {
        String header = "a,b,c\n";
        int dataRows = 20;
        StringBuilder sb = new StringBuilder(header);
        for (int i = 0; i < dataRows; i++) {
            sb.append("1,2,3\n");
        }
        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        InMemoryStorageObject full = new InMemoryStorageObject(bytes);
        long headerBytes = header.getBytes(StandardCharsets.UTF_8).length;
        long bodyLength = bytes.length - headerBytes;
        StorageObject nonLeadingRange = new RangeStorageObject(full, headerBytes, bodyLength);

        CsvFormatReader base = new CsvFormatReader(blockFactory());
        SourceMetadata meta = base.metadata(full);
        CsvFormatReader withSchema = base.withSchema(meta.schema());

        long rowsWithRecordAligned = countCsvRows(withSchema, nonLeadingRange, List.of("a", "b", "c"), false, true);
        long rowsWithoutRecordAligned = countCsvRows(withSchema, nonLeadingRange, List.of("a", "b", "c"), false, false);

        assertEquals("recordAligned=true must preserve all data rows", dataRows, rowsWithRecordAligned);
        assertTrue(
            "recordAligned=false drops the first row (treats it as partial-line fragment)",
            rowsWithoutRecordAligned < rowsWithRecordAligned
        );
    }

    /**
     * Regression: the coordinator's fallback path must pass recordAligned through to the
     * reader even when parallelism is 1 (openWithParallelism returns null). This validates
     * the single-threaded CSV read of a non-leading macro-split.
     */
    public void testCsvNonLeadingMacroSplitSingleThreadPreservesRows() throws Exception {
        String header = "x,y\n";
        int dataRows = 50;
        StringBuilder sb = new StringBuilder(header);
        for (int i = 0; i < dataRows; i++) {
            sb.append(i).append(",").append(i * 10).append("\n");
        }
        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        InMemoryStorageObject full = new InMemoryStorageObject(bytes);
        long headerBytes = header.getBytes(StandardCharsets.UTF_8).length;
        StorageObject nonLeadingRange = new RangeStorageObject(full, headerBytes, bytes.length - headerBytes);

        CsvFormatReader base = new CsvFormatReader(blockFactory());
        SourceMetadata meta = base.metadata(full);
        CsvFormatReader withSchema = base.withSchema(meta.schema());

        long rowsAligned = countCsvRows(withSchema, nonLeadingRange, List.of("x", "y"), false, true);
        assertEquals("all data rows must be read with recordAligned=true", dataRows, rowsAligned);
    }

    private static long countCsvRows(
        CsvFormatReader reader,
        StorageObject object,
        List<String> projectedColumns,
        boolean firstSplit,
        boolean recordAligned
    ) throws IOException {
        FormatReadContext ctx = FormatReadContext.builder()
            .projectedColumns(projectedColumns)
            .batchSize(100)
            .firstSplit(firstSplit)
            .lastSplit(true)
            .recordAligned(recordAligned)
            .build();
        long rows = 0;
        try (CloseableIterator<Page> iter = reader.read(object, ctx)) {
            while (iter.hasNext()) {
                Page p = iter.next();
                rows += p.getPositionCount();
                p.releaseBlocks();
            }
        }
        return rows;
    }

    /**
     * Partial projection (selecting 1 of 3 columns) on a non-leading record-aligned macro-split
     * must return the correct row count and exactly one block per page (the projected column).
     */
    public void testCsvNonLeadingMacroSplitPartialProjectionReturnsOneColumn() throws Exception {
        String header = "a,b,c\n";
        int dataRows = 60;
        StringBuilder sb = new StringBuilder(header);
        for (int i = 0; i < dataRows; i++) {
            sb.append(i).append(",val").append(i).append(",end").append(i).append("\n");
        }
        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        InMemoryStorageObject full = new InMemoryStorageObject(bytes);
        long headerBytes = header.getBytes(StandardCharsets.UTF_8).length;
        StorageObject nonLeadingRange = new RangeStorageObject(full, headerBytes, bytes.length - headerBytes);

        CsvFormatReader base = new CsvFormatReader(blockFactory());
        SourceMetadata meta = base.metadata(full);
        CsvFormatReader withSchema = base.withSchema(meta.schema());

        FormatReadContext ctx = FormatReadContext.builder()
            .projectedColumns(List.of("b"))
            .batchSize(100)
            .firstSplit(false)
            .lastSplit(true)
            .recordAligned(true)
            .build();

        long rows = 0;
        try (CloseableIterator<Page> iter = withSchema.read(nonLeadingRange, ctx)) {
            while (iter.hasNext()) {
                Page p = iter.next();
                assertEquals("partial projection must yield exactly 1 block per page", 1, p.getBlockCount());
                rows += p.getPositionCount();
                p.releaseBlocks();
            }
        }
        assertEquals("all data rows must be returned with partial projection", dataRows, rows);
    }

    /**
     * When {@code metadata()} returns null on a non-leading split with empty projection and
     * {@code splitIncludesFileLeader=false}, the coordinator must not crash — it should proceed
     * without schema binding and still produce rows via the reader's own inference.
     */
    public void testParallelReadNullMetadataNonLeadingSplitDoesNotCrash() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            sb.append("line-").append(String.format(java.util.Locale.ROOT, "%04d", i)).append("\n");
        }
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        StorageObject obj = new InMemoryStorageObject(content);
        LineFormatReader reader = new LineFormatReader(blockFactory());

        assertNull("precondition: LineFormatReader.metadata() returns null", reader.metadata(obj));

        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of(), 50, 4, exec, null, true, false);
            long rows = 0;
            try (iter) {
                while (iter.hasNext()) {
                    Page p = iter.next();
                    rows += p.getPositionCount();
                    p.releaseBlocks();
                }
            }
            assertTrue("reader with null metadata must still produce rows", rows > 0);
        } finally {
            exec.shutdown();
        }
    }

    /**
     * As-ready emission (the root-fix behaviour): a later segment whose reader finishes quickly must be
     * emitted before an earlier segment that is artificially slowed. If pages were still gated on segment
     * order, the first row out would belong to segment 0 and this would fail.
     * <p>
     * The first-split segment (segment 0) stalls before producing its first page; every other segment runs
     * at full speed. With the cap wide enough for both to be open at once, a later segment must reach the
     * consumer first. Rows are prefixed with their segment's start offset so the test can tell which segment
     * emitted them.
     */
    public void testEmitsReadySegmentBeforeSlowerEarlierSegment() throws Exception {
        int rows = 800;
        byte[] content = repeatedLines(rows);
        InMemoryStorageObject obj = new InMemoryStorageObject(content);

        // Sanity: confirm the fixture is genuinely multi-segment so "later segment" is meaningful.
        int segmentCount = ParallelParsingCoordinator.computeSegments(new LineFormatReader(blockFactory()), obj, content.length, 4, 1)
            .size();
        assertThat("test needs multiple segments", segmentCount, Matchers.greaterThan(1));

        // Segment 0 (the file-leader split) blocks before emitting any page until the consumer has already
        // pulled a page. Because the blocked leader has enqueued nothing, that first page can only have come
        // from a later segment — proving as-ready emission deterministically, with no enqueue race. A
        // regressed in-order implementation would gate every page behind the blocked leader and time out.
        java.util.concurrent.CountDownLatch leaderRelease = new java.util.concurrent.CountDownLatch(1);
        FirstSegmentStallingReader reader = new FirstSegmentStallingReader(blockFactory(), leaderRelease);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        String firstLineEmitted = null;
        try {
            try (
                CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(
                    reader,
                    obj,
                    List.of("line"),
                    50,
                    4,
                    exec,
                    null,
                    false,
                    true,
                    null,
                    4 // wide window so the slowed segment 0 and a fast later segment are open together
                )
            ) {
                while (iter.hasNext()) {
                    Page p = iter.next();
                    if (firstLineEmitted == null && p.getPositionCount() > 0) {
                        BytesRefBlock block = (BytesRefBlock) p.getBlock(0);
                        firstLineEmitted = block.getBytesRef(block.getFirstValueIndex(0), new BytesRef()).utf8ToString();
                        // First page is in hand while the leader is still blocked — now let the leader proceed.
                        leaderRelease.countDown();
                    }
                    p.releaseBlocks();
                }
            }
        } finally {
            exec.shutdown();
            assertTrue("executor did not terminate", exec.awaitTermination(60, TimeUnit.SECONDS));
        }

        // The leader segment owns the file's leading bytes, hence line-00000. It is structurally blocked from
        // emitting until a later segment has produced a page, so the first page reaching the consumer cannot
        // be the leader's: the first line out must not be line-00000. Under strict segment-order emission it
        // would have been (and the read would have deadlocked on the stalled leader instead).
        assertNotNull("expected at least one page", firstLineEmitted);
        assertThat(
            "first emitted row must come from a later segment, not the stalled leader",
            firstLineEmitted,
            Matchers.not(Matchers.equalTo("line-00000"))
        );
    }

    /**
     * Readiness contract, "parks while parsing" branch: with every segment worker blocked in {@code read()}
     * (segments still parsing) and the shared queue empty, {@link CloseableIterator#waitForReady()} must NOT
     * complete synchronously. Once production is released and the first page is enqueued, the parked listener
     * must fire via {@code signalReady()}. This is the honest-readiness signal that lets {@code drainHotPath}
     * yield its scarce consumer-pool thread instead of blocking inside {@link CloseableIterator#hasNext()} for
     * the whole segment read (the fast-follow to #153074 for the uncompressed seekable path).
     */
    public void testWaitForReadyParksWhileParsingThenSignalsOnPage() throws Exception {
        byte[] content = repeatedLines(400);
        InMemoryStorageObject obj = new InMemoryStorageObject(content);
        assertThat(
            "test needs a genuinely multi-segment file or it hits the single-stream fallback (no AsReadyParallelIterator)",
            ParallelParsingCoordinator.computeSegments(new LineFormatReader(blockFactory()), obj, content.length, 4, 1).size(),
            Matchers.greaterThan(1)
        );

        CountDownLatch readEntered = new CountDownLatch(1);
        CountDownLatch producePermit = new CountDownLatch(1);
        GatedReadLineReader reader = new GatedReadLineReader(blockFactory(), readEntered, producePermit);

        ExecutorService exec = Executors.newFixedThreadPool(6);
        try {
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 50, 4, exec);
            // A segment worker has entered read() and is blocked before producing any page: segments are
            // parsing, remainingSegments is still full, and the shared queue is empty.
            assertTrue("a segment worker must have entered read()", readEntered.await(10, TimeUnit.SECONDS));
            SubscribableListener<Void> ready = iter.waitForReady();
            assertFalse("waitForReady must NOT complete while segments parse with an empty queue", ready.isDone());

            // Release production: the first successful sharedQueue.offer fires signalReady, completing the listener.
            producePermit.countDown();
            assertBusy(() -> assertTrue("ready listener must fire once a page is enqueued", ready.isDone()), 10, TimeUnit.SECONDS);

            try (iter) {
                while (iter.hasNext()) {
                    iter.next().releaseBlocks();
                }
            }
        } finally {
            producePermit.countDown();
            exec.shutdown();
            assertTrue("executor did not terminate", exec.awaitTermination(60, TimeUnit.SECONDS));
        }
    }

    /**
     * Readiness contract, "EOF" branch: once every segment has finished and the shared queue is drained,
     * {@link CloseableIterator#waitForReady()} must be immediately done (terminal EOF) so the drain concludes
     * without a residual wait rather than parking forever.
     */
    public void testWaitForReadyImmediateAtEof() throws Exception {
        byte[] content = repeatedLines(200);
        InMemoryStorageObject obj = new InMemoryStorageObject(content);
        LineFormatReader reader = new LineFormatReader(blockFactory());
        assertThat(
            "test needs a genuinely multi-segment file or it hits the single-stream fallback",
            ParallelParsingCoordinator.computeSegments(reader, obj, content.length, 4, 1).size(),
            Matchers.greaterThan(1)
        );

        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 50, 4, exec);
            try (iter) {
                while (iter.hasNext()) {
                    iter.next().releaseBlocks();
                }
                assertTrue(
                    "waitForReady must be immediately done once every segment finished and the queue drained",
                    iter.waitForReady().isDone()
                );
            }
        } finally {
            exec.shutdown();
            assertTrue("executor did not terminate", exec.awaitTermination(60, TimeUnit.SECONDS));
        }
    }

    /**
     * Readiness contract, "non-terminal segment finish" branch (guards the {@code finishSegment()} signal
     * gate). A leader segment finishes producing <em>no</em> pages while later segments stay blocked in
     * {@code read()}: remainingSegments is still non-zero and the queue is empty, so {@code isReadyNow()} is
     * false and a consumer parked on {@link CloseableIterator#waitForReady()} must stay parked. Signalling on
     * every segment completion (the pre-review shape) would spuriously complete the listener here and drop
     * {@code drainHotPath} into a blocking {@code hasNext()}. Once the later segments are released and enqueue
     * a page, the listener fires.
     */
    public void testWaitForReadyStaysParkedWhenNonTerminalSegmentFinishesEmpty() throws Exception {
        byte[] content = repeatedLines(400);
        InMemoryStorageObject obj = new InMemoryStorageObject(content);
        assertThat(
            "test needs a genuinely multi-segment file",
            ParallelParsingCoordinator.computeSegments(new LineFormatReader(blockFactory()), obj, content.length, 4, 1).size(),
            Matchers.greaterThan(1)
        );

        CountDownLatch leaderFinished = new CountDownLatch(1);
        CountDownLatch releaseRest = new CountDownLatch(1);
        LeaderEmptyRestGatedReader reader = new LeaderEmptyRestGatedReader(blockFactory(), leaderFinished, releaseRest);

        ExecutorService exec = Executors.newFixedThreadPool(6);
        try {
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 50, 4, exec);
            SubscribableListener<Void> ready = iter.waitForReady();
            assertFalse("waitForReady must NOT complete while segments parse with an empty queue", ready.isDone());

            // The leader segment finishes producing zero pages; the later segments are still blocked in read().
            assertTrue("leader segment must finish", leaderFinished.await(10, TimeUnit.SECONDS));
            // A non-terminal segment finishing must not wake the parked consumer. Give finishSegment() a beat.
            Thread.sleep(200);
            assertFalse("a mid-parse segment finishing with an empty queue must not complete waitForReady", ready.isDone());

            // Release the rest: pages now flow and the listener fires via enqueueOrRelease's signalReady.
            releaseRest.countDown();
            assertBusy(() -> assertTrue("ready listener must fire once a page is enqueued", ready.isDone()), 10, TimeUnit.SECONDS);

            try (iter) {
                while (iter.hasNext()) {
                    iter.next().releaseBlocks();
                }
            }
        } finally {
            releaseRest.countDown();
            exec.shutdown();
            assertTrue("executor did not terminate", exec.awaitTermination(60, TimeUnit.SECONDS));
        }
    }

    /**
     * Concurrency parity with the streaming sibling ({@code StreamingParallelParsingCoordinatorTests
     * #testConcurrentProducerLoopsOnSharedPoolDoNotDeadlock}), for the uncompressed seekable path.
     * <p>
     * {@code F} concurrent {@link ParallelParsingCoordinator#parallelRead} iterators are drained by a
     * {@code drainHotPath}-mirroring producer loop on a <em>consumer pool of size &lt; F</em>; segment parsing
     * runs on a separate, amply-sized pool. Every segment worker is gated in {@code read()} before producing a
     * page, so at the point of the assertion the queues are empty and all segments are still parsing.
     * <ul>
     *   <li><b>Pre-fix</b> (no {@code waitForReady} override → default immediately-ready): each drain falls
     *       into a blocking {@link CloseableIterator#hasNext()} and pins its consumer-pool thread for the whole
     *       read. With the pool smaller than {@code F}, only {@code poolSize} drains ever start; the rest stay
     *       queued and never begin — so {@code startedLatch} never reaches zero and this times out.</li>
     *   <li><b>Post-fix</b>: each drain parks on {@link CloseableIterator#waitForReady()} and yields its thread,
     *       so all {@code F} drains start on the sub-{@code F} pool. After production is released every drain
     *       resumes and delivers all rows.</li>
     * </ul>
     * As-ready emission does not preserve cross-segment order, so per-file completeness is asserted set-wise.
     */
    public void testConcurrentDrainsOnUndersizedConsumerPoolPark() throws Exception {
        int fileCount = 4;
        int parallelism = 2;
        int openCap = 2;
        int consumerPoolSize = 2; // strictly < fileCount
        int linesPerFile = 400;

        byte[] sample = repeatedLines(linesPerFile);
        assertThat(
            "each file must be genuinely multi-segment to exercise AsReadyParallelIterator",
            ParallelParsingCoordinator.computeSegments(
                new LineFormatReader(blockFactory()),
                new InMemoryStorageObject(sample),
                sample.length,
                parallelism,
                1
            ).size(),
            Matchers.greaterThan(1)
        );

        // Segment workers run here; sized to hold every gated (blocked-in-read) segment plus post-release work.
        ExecutorService parserPool = Executors.newFixedThreadPool(fileCount * parallelism + 4);
        // Deliberately smaller than fileCount: a blocking drain would starve the not-yet-started files.
        ExecutorService consumerPool = Executors.newFixedThreadPool(consumerPoolSize);

        CountDownLatch producePermit = new CountDownLatch(1);
        CountDownLatch startedLatch = new CountDownLatch(fileCount);

        List<CloseableIterator<Page>> iterators = new ArrayList<>();
        List<PlainActionFuture<List<String>>> dones = new ArrayList<>();
        try {
            for (int f = 0; f < fileCount; f++) {
                InMemoryStorageObject obj = new InMemoryStorageObject(repeatedLines(linesPerFile));
                GatedReadLineReader reader = new GatedReadLineReader(blockFactory(), new CountDownLatch(1), producePermit);
                CloseableIterator<Page> it = ParallelParsingCoordinator.parallelRead(
                    reader,
                    obj,
                    List.of("line"),
                    50,
                    parallelism,
                    parserPool,
                    null,
                    false,
                    true,
                    null,
                    openCap,
                    null
                );
                iterators.add(it);
                PlainActionFuture<List<String>> done = new PlainActionFuture<>();
                dones.add(done);
                AtomicBoolean started = new AtomicBoolean();
                List<String> collected = new ArrayList<>();
                consumerPool.execute(() -> drainAsReadyViaProducerLoop(it, consumerPool, started, startedLatch, collected, done));
            }

            // The load-bearing assertion: on a sub-F consumer pool every drain must have STARTED. Pre-fix, the
            // first poolSize drains block in hasNext() and the remainder never run — this times out. Post-fix,
            // they park and free their threads, so all F start even though production is still gated.
            assertBusy(
                () -> assertEquals("a sub-F consumer pool must let all F drains start (park, don't block)", 0L, startedLatch.getCount()),
                15,
                TimeUnit.SECONDS
            );

            // Release production; every drain must now resume via signalReady and deliver all of its rows.
            producePermit.countDown();

            List<String> expected = new ArrayList<>();
            for (int i = 0; i < linesPerFile; i++) {
                expected.add(String.format(java.util.Locale.ROOT, "line-%05d", i));
            }
            Collections.sort(expected);
            for (int f = 0; f < fileCount; f++) {
                List<String> got = dones.get(f).actionGet(TimeValue.timeValueSeconds(30));
                List<String> sorted = new ArrayList<>(got);
                Collections.sort(sorted);
                assertEquals("file " + f + " must deliver every row exactly once (as-ready order)", expected, sorted);
            }
        } finally {
            producePermit.countDown();
            for (CloseableIterator<Page> it : iterators) {
                try {
                    it.close();
                } catch (IOException ignored) {}
            }
            consumerPool.shutdownNow();
            parserPool.shutdownNow();
        }
    }

    /**
     * Mirrors {@code AsyncExternalSourceOperatorFactory#drainHotPath}: parks (yields its pool thread by
     * registering a listener and returning) when the iterator is not ready, resumes on {@code signalReady}.
     * Counts the file's first entry into {@code startedLatch} so the concurrency test can distinguish "all
     * drains started" (post-fix, parking) from "only poolSize drains started" (pre-fix, blocking).
     */
    private static void drainAsReadyViaProducerLoop(
        CloseableIterator<Page> it,
        Executor pool,
        AtomicBoolean started,
        CountDownLatch startedLatch,
        List<String> collected,
        PlainActionFuture<List<String>> done
    ) {
        try {
            if (started.compareAndSet(false, true)) {
                startedLatch.countDown();
            }
            BytesRef scratch = new BytesRef();
            while (true) {
                SubscribableListener<Void> ready = it.waitForReady();
                if (ready.isDone() == false) {
                    ready.addListener(
                        ActionListener.wrap(
                            v -> pool.execute(() -> drainAsReadyViaProducerLoop(it, pool, started, startedLatch, collected, done)),
                            done::onFailure
                        )
                    );
                    return;
                }
                if (it.hasNext() == false) { // pre-fix: BLOCKS here for the whole read (default waitForReady is immediately done)
                    SubscribableListener<Void> recheck = it.waitForReady();
                    if (recheck.isDone()) {
                        done.onResponse(collected);
                        return;
                    }
                    recheck.addListener(
                        ActionListener.wrap(
                            v -> pool.execute(() -> drainAsReadyViaProducerLoop(it, pool, started, startedLatch, collected, done)),
                            done::onFailure
                        )
                    );
                    return;
                }
                Page p = it.next();
                BytesRefBlock block = (BytesRefBlock) p.getBlock(0);
                for (int i = 0; i < block.getPositionCount(); i++) {
                    collected.add(block.getBytesRef(block.getFirstValueIndex(i), scratch).utf8ToString());
                }
                p.releaseBlocks();
            }
        } catch (Exception e) {
            done.onFailure(e);
        }
    }

    /**
     * {@link LineFormatReader} variant that blocks every segment's {@code read()} on a shared
     * {@code producePermit} latch before producing any page (counting {@code readEntered} down on entry so a
     * test can observe that a worker is parked in-read). Used by the readiness and concurrency tests to hold
     * the coordinator in the "segments parsing, queue empty" state deterministically.
     */
    private static class GatedReadLineReader extends LineFormatReader {
        private final CountDownLatch readEntered;
        private final CountDownLatch producePermit;

        GatedReadLineReader(BlockFactory blockFactory, CountDownLatch readEntered, CountDownLatch producePermit) {
            super(blockFactory);
            this.readEntered = readEntered;
            this.producePermit = producePermit;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            readEntered.countDown();
            try {
                if (producePermit.await(30, TimeUnit.SECONDS) == false) {
                    throw new IOException("gated reader was never released");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while gated in read()", e);
            }
            return super.read(object, context);
        }
    }

    /**
     * {@link LineFormatReader} variant used to isolate a non-terminal segment finish: the leader segment
     * ({@code firstSplit=true}) returns an immediately-exhausted iterator (zero pages) and counts
     * {@code leaderFinished} down on close, while every other segment blocks in {@code read()} on
     * {@code releaseRest}. This holds the coordinator with remainingSegments non-zero and the queue empty
     * right after the leader finishes, so a test can assert {@code waitForReady()} stays parked.
     */
    private static class LeaderEmptyRestGatedReader extends LineFormatReader {
        private final CountDownLatch leaderFinished;
        private final CountDownLatch releaseRest;

        LeaderEmptyRestGatedReader(BlockFactory blockFactory, CountDownLatch leaderFinished, CountDownLatch releaseRest) {
            super(blockFactory);
            this.leaderFinished = leaderFinished;
            this.releaseRest = releaseRest;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            if (context.firstSplit()) {
                return new CloseableIterator<>() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public Page next() {
                        throw new java.util.NoSuchElementException();
                    }

                    @Override
                    public void close() {
                        leaderFinished.countDown();
                    }
                };
            }
            try {
                if (releaseRest.await(30, TimeUnit.SECONDS) == false) {
                    throw new IOException("gated non-leader segment was never released");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while gated in read()", e);
            }
            return super.read(object, context);
        }
    }

    /**
     * {@link LineFormatReader} variant where the segment owning the file leader ({@code firstSplit=true})
     * blocks before emitting its first page until a later segment has emitted; every other segment runs at
     * full speed. Proves the coordinator emits pages as they become ready rather than in segment order.
     */
    private static class FirstSegmentStallingReader extends LineFormatReader {
        private final java.util.concurrent.CountDownLatch leaderRelease;

        FirstSegmentStallingReader(BlockFactory blockFactory, java.util.concurrent.CountDownLatch leaderRelease) {
            super(blockFactory);
            this.leaderRelease = leaderRelease;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            boolean isFirstSplit = context.firstSplit();
            CloseableIterator<Page> delegate = super.read(object, context);
            return new CloseableIterator<>() {
                private boolean firstPage = true;

                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public Page next() {
                    if (firstPage && isFirstSplit) {
                        firstPage = false;
                        // Block the leader before emitting anything until the consumer releases it (which it
                        // does only after pulling a page — necessarily from a later segment, since the leader
                        // has enqueued nothing). Later segments are never gated and run at full speed.
                        try {
                            leaderRelease.await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    return delegate.next();
                }

                @Override
                public void close() throws IOException {
                    delegate.close();
                }
            };
        }
    }

    /**
     * Records the {@link FormatReadContext} of every {@code read} call so tests can assert the
     * coordinator's per-segment split-flag dispatch behavior. Otherwise behaves like the parent
     * {@link LineFormatReader}.
     */
    private static class ContextCapturingLineReader extends LineFormatReader {
        final List<FormatReadContext> contexts = Collections.synchronizedList(new ArrayList<>());

        ContextCapturingLineReader(BlockFactory blockFactory) {
            super(blockFactory);
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            contexts.add(context);
            return super.read(object, context);
        }
    }

    private static final BlockFactory TEST_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
        .build();

    private static BlockFactory blockFactory() {
        return TEST_BLOCK_FACTORY;
    }

    /**
     * Minimal SegmentableFormatReader that scans for newlines.
     */
    private static class NewlineSegmentableReader implements SegmentableFormatReader, NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        private final long minSegmentSize;

        NewlineSegmentableReader(long minSegmentSize) {
            this.minSegmentSize = minSegmentSize;
        }

        @Override
        public RecordSplitter recordSplitter(int maxRecordBytes) {
            return TestRecordSplitters.newlineSplitter(maxRecordBytes);
        }

        @Override
        public long minimumSegmentSize() {
            return minSegmentSize;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            return emptyIterator();
        }

        @Override
        public String formatName() {
            return "test-newline";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".txt");
        }

        @Override
        public void close() {}
    }

    /**
     * A line-oriented format reader that reads newline-delimited text and produces
     * single-column pages with keyword blocks. Used for testing parallel parsing.
     */
    private static class LineFormatReader implements SegmentableFormatReader, NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        private final BlockFactory blockFactory;

        LineFormatReader(BlockFactory blockFactory) {
            this.blockFactory = blockFactory;
        }

        @Override
        public RecordSplitter recordSplitter(int maxRecordBytes) {
            return TestRecordSplitters.newlineSplitter(maxRecordBytes);
        }

        @Override
        public long minimumSegmentSize() {
            return 1;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            // Mirror production semantics: drop a leading partial record only when the caller has
            // not guaranteed record-alignment. `ParallelParsingCoordinator` now sets
            // `recordAligned=true` for every segment, so `skipFirstLine` is effectively false here.
            boolean skipFirstLine = context.firstSplit() == false && context.recordAligned() == false;
            int batchSize = context.batchSize();
            InputStream stream = object.newStream();
            java.io.BufferedReader br = new java.io.BufferedReader(new java.io.InputStreamReader(stream, StandardCharsets.UTF_8));

            if (skipFirstLine) {
                br.readLine();
            }

            return new BufferingPageIterator() {
                private final List<String> buffer = new ArrayList<>();
                private boolean done = false;

                @Override
                public boolean hasNext() {
                    if (nextPage != null) {
                        return true;
                    }
                    try {
                        nextPage = readBatch();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return nextPage != null;
                }

                @Override
                public Page next() {
                    if (hasNext() == false) {
                        throw new java.util.NoSuchElementException();
                    }
                    Page p = nextPage;
                    nextPage = null;
                    return p;
                }

                private Page readBatch() throws IOException {
                    if (done) {
                        return null;
                    }
                    buffer.clear();
                    while (buffer.size() < batchSize) {
                        String line = br.readLine();
                        if (line == null) {
                            done = true;
                            break;
                        }
                        if (line.isEmpty()) {
                            continue;
                        }
                        buffer.add(line);
                    }
                    if (buffer.isEmpty()) {
                        return null;
                    }
                    try (var builder = blockFactory.newBytesRefBlockBuilder(buffer.size())) {
                        for (String s : buffer) {
                            builder.appendBytesRef(new BytesRef(s));
                        }
                        Block block = builder.build();
                        return new Page(buffer.size(), block);
                    }
                }

                @Override
                protected void closeInternal() throws IOException {
                    br.close();
                    stream.close();
                }
            };
        }

        @Override
        public String formatName() {
            return "test-line";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".txt");
        }

        @Override
        public void close() {}
    }

    /**
     * Wraps {@link LineFormatReader} so each segment's iterator publishes a per-chunk
     * {@code _stats.*} contribution via {@link ExternalStatsCapture#record} on natural EOF — same
     * close-hook pattern as the production {@code CsvFormatReader} / {@code NdJsonPageIterator}
     * chunk paths. Used by {@link #testParallelReadPropagatesPerSegmentPartialsToSink} to
     * observe whether parallel-parsing worker threads can reach a bound sink.
     */
    private static class StatsPublishingLineReader implements SegmentableFormatReader, NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        private final LineFormatReader delegate;
        private final String path;

        StatsPublishingLineReader(BlockFactory blockFactory, String path) {
            this.delegate = new LineFormatReader(blockFactory);
            this.path = path;
        }

        @Override
        public RecordSplitter recordSplitter(int maxRecordBytes) {
            return delegate.recordSplitter(maxRecordBytes);
        }

        @Override
        public long minimumSegmentSize() {
            return delegate.minimumSegmentSize();
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return delegate.metadata(object);
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            boolean chunkMode = context.recordAligned();
            CloseableIterator<Page> inner = delegate.read(object, context);
            return new CloseableIterator<>() {
                long rowsEmitted = 0;
                boolean naturallyExhausted = false;

                @Override
                public boolean hasNext() {
                    if (inner.hasNext()) {
                        return true;
                    }
                    naturallyExhausted = true;
                    return false;
                }

                @Override
                public Page next() {
                    Page p = inner.next();
                    rowsEmitted += p.getPositionCount();
                    return p;
                }

                @Override
                public void close() throws IOException {
                    try {
                        inner.close();
                    } finally {
                        if (naturallyExhausted) {
                            Map<String, Object> stats = new HashMap<>();
                            stats.put(SourceStatisticsSerializer.STATS_ROW_COUNT, rowsEmitted);
                            if (chunkMode) {
                                stats.put(ExternalStats.PARTIAL_CHUNK_KEY, Boolean.TRUE);
                            }
                            ExternalStatsCapture.record(path, stats);
                        }
                    }
                }
            };
        }

        @Override
        public String formatName() {
            return "test-stats-publishing-line";
        }

        @Override
        public List<String> fileExtensions() {
            return delegate.fileExtensions();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    /**
     * SegmentableFormatReader that records the {@link FormatReadContext} it was handed for each
     * {@link #read} call. Lets tests assert per-segment flag wiring without re-implementing line
     * parsing.
     */
    private static class ContextRecordingFormatReader implements SegmentableFormatReader, NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        private final BlockFactory blockFactory;
        private final long minSegmentSize;
        private final List<FormatReadContext> contexts = new CopyOnWriteArrayList<>();

        /** Convenience: matches the original test behaviour (force multi-segment even on tiny fixtures). */
        ContextRecordingFormatReader(BlockFactory blockFactory) {
            this(blockFactory, 1);
        }

        ContextRecordingFormatReader(BlockFactory blockFactory, long minSegmentSize) {
            this.blockFactory = blockFactory;
            this.minSegmentSize = minSegmentSize;
        }

        List<FormatReadContext> contexts() {
            return contexts;
        }

        @Override
        public RecordSplitter recordSplitter(int maxRecordBytes) {
            return TestRecordSplitters.newlineSplitter(maxRecordBytes);
        }

        @Override
        public long minimumSegmentSize() {
            return minSegmentSize;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            contexts.add(context);
            return new LineFormatReader(blockFactory).read(object, context);
        }

        @Override
        public String formatName() {
            return "test-recording";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".txt");
        }

        @Override
        public void close() {}
    }

    private static CloseableIterator<Page> emptyIterator() {
        return new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Page next() {
                throw new java.util.NoSuchElementException();
            }

            @Override
            public void close() {}
        };
    }

    /**
     * A line-oriented reader that throws after producing a configurable number of lines.
     */
    private static class FailingFormatReader implements SegmentableFormatReader, NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        private final BlockFactory blockFactory;
        private final int failAfterLines;

        FailingFormatReader(BlockFactory blockFactory, int failAfterLines) {
            this.blockFactory = blockFactory;
            this.failAfterLines = failAfterLines;
        }

        @Override
        public RecordSplitter recordSplitter(int maxRecordBytes) {
            return TestRecordSplitters.newlineSplitter(maxRecordBytes);
        }

        @Override
        public long minimumSegmentSize() {
            return 1;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            // See note in the other test fixture: drop a leading partial record only when the caller
            // has not guaranteed record-alignment.
            boolean skipFirstLine = context.firstSplit() == false && context.recordAligned() == false;
            int batchSize = context.batchSize();
            InputStream stream = object.newStream();
            java.io.BufferedReader br = new java.io.BufferedReader(new java.io.InputStreamReader(stream, StandardCharsets.UTF_8));
            if (skipFirstLine) {
                br.readLine();
            }

            return new BufferingPageIterator() {
                private int linesRead = 0;
                private boolean done = false;

                @Override
                public boolean hasNext() {
                    if (nextPage != null) {
                        return true;
                    }
                    try {
                        nextPage = readBatch();
                    } catch (IOException e) {
                        // Mirror the production iterators (CsvFormatReader, NdJsonPageIterator), which surface
                        // a raw IOException as a typed ExternalClientException at the hasNext() boundary so the
                        // 400 status survives into the coordinator's firstError.
                        throw ExternalFailures.surface(e, "Failed to read injected test page");
                    }
                    return nextPage != null;
                }

                @Override
                public Page next() {
                    if (hasNext() == false) {
                        throw new java.util.NoSuchElementException();
                    }
                    Page p = nextPage;
                    nextPage = null;
                    return p;
                }

                private Page readBatch() throws IOException {
                    if (done) {
                        return null;
                    }
                    List<String> buffer = new ArrayList<>();
                    while (buffer.size() < batchSize) {
                        if (linesRead >= failAfterLines) {
                            throw new IOException("injected failure after " + failAfterLines + " lines");
                        }
                        String line = br.readLine();
                        if (line == null) {
                            done = true;
                            break;
                        }
                        if (line.isEmpty()) {
                            continue;
                        }
                        buffer.add(line);
                        linesRead++;
                    }
                    if (buffer.isEmpty()) {
                        return null;
                    }
                    try (var builder = blockFactory.newBytesRefBlockBuilder(buffer.size())) {
                        for (String s : buffer) {
                            builder.appendBytesRef(new BytesRef(s));
                        }
                        Block block = builder.build();
                        return new Page(buffer.size(), block);
                    }
                }

                @Override
                protected void closeInternal() throws IOException {
                    br.close();
                    stream.close();
                }
            };
        }

        @Override
        public String formatName() {
            return "test-failing";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".txt");
        }

        @Override
        public void close() {}
    }

    private static class InMemoryStorageObject implements StorageObject {
        private final byte[] data;

        InMemoryStorageObject(byte[] data) {
            this.data = data;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            return new ByteArrayInputStream(data, (int) position, (int) length);
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
            return StoragePath.of("mem://test");
        }
    }

    /**
     * In-memory {@link StorageObject} that records the peak number of positional range streams
     * ({@code newStream(pos,len)}) open at once. Segment workers always read through the positional overload
     * (via {@link RangeStorageObject}), so this captures the concurrently-open-segment count. Each open
     * lingers a few ms so overlapping threads coincide -- a plain delay, not a barrier, so it cannot
     * deadlock. The whole-file {@code newStream()} overload is not counted (segment workers never use it).
     */
    private static class StreamCountingStorageObject implements StorageObject {
        private final byte[] data;
        private final AtomicInteger open = new AtomicInteger();
        private final AtomicInteger peak = new AtomicInteger();
        private final AtomicInteger total = new AtomicInteger();

        StreamCountingStorageObject(byte[] data) {
            this.data = data;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            int now = open.incrementAndGet();
            peak.accumulateAndGet(now, Math::max);
            total.incrementAndGet();
            // Linger so concurrently-open segment streams overlap in time; plain sleep, no barrier.
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new ByteArrayInputStream(data, (int) position, (int) length) {
                private boolean closed = false;

                @Override
                public void close() {
                    if (closed == false) {
                        closed = true;
                        open.decrementAndGet();
                    }
                }
            };
        }

        int peakConcurrent() {
            return peak.get();
        }

        int totalOpens() {
            return total.get();
        }

        /** Currently-open positional range streams (incremented on open, decremented on close). */
        int currentOpen() {
            return open.get();
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
            return StoragePath.of("mem://stream-counting");
        }
    }
}
