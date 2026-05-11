/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamingParallelParsingCoordinatorTests extends ESTestCase {

    private static final BlockFactory TEST_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
        .build();

    public void testBasicStreamingParallelParse() throws Exception {
        int lineCount = 200;
        String content = buildContent(lineCount);
        InputStream stream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

        ExecutorService executor = Executors.newFixedThreadPool(6);
        try {
            LineFormatReader reader = new LineFormatReader(1024);
            List<String> allLines = collectLines(
                StreamingParallelParsingCoordinator.parallelRead(reader, stream, List.of("line"), 50, 4, executor, ErrorPolicy.STRICT)
            );

            assertEquals(lineCount, allLines.size());
            for (int i = 0; i < lineCount; i++) {
                assertEquals("line-" + String.format(java.util.Locale.ROOT, "%04d", i), allLines.get(i));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    public void testSingleLineFallback() throws Exception {
        String content = "line-0000\n";
        InputStream stream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            LineFormatReader reader = new LineFormatReader(1024);
            List<String> allLines = collectLines(
                StreamingParallelParsingCoordinator.parallelRead(reader, stream, List.of("line"), 50, 1, executor, ErrorPolicy.STRICT)
            );

            assertEquals(1, allLines.size());
            assertEquals("line-0000", allLines.get(0));
        } finally {
            executor.shutdownNow();
        }
    }

    public void testEmptyStream() throws Exception {
        InputStream stream = new ByteArrayInputStream(new byte[0]);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            LineFormatReader reader = new LineFormatReader(1024);
            List<String> allLines = collectLines(
                StreamingParallelParsingCoordinator.parallelRead(reader, stream, List.of("line"), 50, 4, executor, ErrorPolicy.STRICT)
            );

            assertEquals(0, allLines.size());
        } finally {
            executor.shutdownNow();
        }
    }

    public void testLargeContentPreservesOrder() throws Exception {
        int lineCount = 5000;
        String content = buildContent(lineCount);
        InputStream stream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

        ExecutorService executor = Executors.newFixedThreadPool(10);
        try {
            LineFormatReader reader = new LineFormatReader(4096);
            List<String> allLines = collectLines(
                StreamingParallelParsingCoordinator.parallelRead(reader, stream, List.of("line"), 100, 8, executor, ErrorPolicy.STRICT)
            );

            assertEquals(lineCount, allLines.size());
            for (int i = 0; i < lineCount; i++) {
                assertEquals("line-" + String.format(java.util.Locale.ROOT, "%04d", i), allLines.get(i));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    public void testParserErrorPropagates() throws Exception {
        String content = buildContent(100);
        InputStream stream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            FailingFormatReader reader = new FailingFormatReader(5, 1024);
            RuntimeException ex = expectThrows(
                RuntimeException.class,
                () -> collectLines(
                    StreamingParallelParsingCoordinator.parallelRead(reader, stream, List.of("line"), 50, 4, executor, ErrorPolicy.STRICT)
                )
            );
            assertTrue(
                "Expected injected failure message but got: " + ex.getMessage(),
                ex.getMessage().contains("injected") || (ex.getCause() != null && ex.getCause().getMessage().contains("injected"))
            );
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Verifies the Stage-1 contract: the coordinator infers schema exactly once (from the first
     * chunk on the segmentator thread) and every parser-thread {@code read()} dispatches against
     * the schema-bound reader. Without this, each chunk re-inferred the schema from 20K sample
     * lines — for a 100M-row file split into ~24K chunks, that wasted ~33% of total parsing CPU.
     */
    public void testSchemaInferredOnceAndBoundReaderUsedByParsers() throws Exception {
        int lineCount = 5000;
        String content = buildContent(lineCount);
        InputStream stream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            // Small chunkSize forces many chunks so per-chunk inference would be obvious.
            LineFormatReader reader = new LineFormatReader(1024);
            List<String> allLines = collectLines(
                StreamingParallelParsingCoordinator.parallelRead(reader, stream, List.of("line"), 100, 4, executor, ErrorPolicy.STRICT)
            );

            assertEquals(lineCount, allLines.size());
            assertEquals("metadata() should be called exactly once for the whole stream", 1, reader.metadataCalls.get());
            assertTrue(
                "expected many parser invocations against the bound reader, got " + reader.boundReadCalls.get(),
                reader.boundReadCalls.get() > 1
            );
            assertEquals("no parser invocation should land on the unbound root reader", 0, reader.unboundReadCalls.get());
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * The segmentator only dispatches a chunk after locating its trailing {@code \n} (carry-over
     * is shifted into the next chunk), so every chunk handed to a parser ends on a record
     * terminator. The coordinator therefore must mark every chunk {@code lastSplit=true} so the
     * NDJSON reader can skip its byte-by-byte {@code TrimLastPartialLineInputStream} scan; an
     * earlier version only set this on the EOF chunk, leaving every interior chunk paying the
     * tail-scan cost (visible as ~5% CPU in async-profiler runs over gzip-compressed COUNT(*)).
     */
    public void testParseChunkMarksAllChunksAsLastSplit() throws Exception {
        int lineCount = 1000;
        String content = buildContent(lineCount);
        InputStream stream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            // Small chunkSize forces several parser-thread invocations so the assertion exercises
            // both interior chunks and the final EOF chunk.
            LineFormatReader reader = new LineFormatReader(1024);
            collectLines(
                StreamingParallelParsingCoordinator.parallelRead(reader, stream, List.of("line"), 100, 4, executor, ErrorPolicy.STRICT)
            );

            List<FormatReadContext> seen;
            synchronized (reader.seenContexts) {
                seen = new ArrayList<>(reader.seenContexts);
            }
            assertTrue("Expected at least 2 chunks, recorded " + seen.size(), seen.size() >= 2);
            // Contexts may not arrive in chunk-index order across parser threads. Count rather
            // than positional-assert: exactly one chunk owns the file's leading bytes (firstSplit),
            // every chunk is record-aligned, and every chunk is marked lastSplit so line-oriented
            // readers can skip the trailing-partial-line scan.
            int firstSplitCount = 0;
            for (int i = 0; i < seen.size(); i++) {
                FormatReadContext ctx = seen.get(i);
                if (ctx.firstSplit()) {
                    firstSplitCount++;
                }
                assertTrue("chunk[" + i + "] must have lastSplit=true (record-boundary aligned)", ctx.lastSplit());
                assertTrue("chunk[" + i + "] must have recordAligned=true (sliced on \\n)", ctx.recordAligned());
            }
            assertEquals("exactly one chunk must own the file's leading bytes", 1, firstSplitCount);
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Guards against reuse of {@code chunk.index % pageQueueRingSize} slots ahead of the consumer:
     * without {@code dispatchPermits} a fast parser could recycle buffers and interleave pages from
     * different chunk generations into the same queue while the consumer is still draining an earlier chunk.
     * <p>
     * Internally repeats {@value #SLOT_REUSE_REPEATS} times to make the race likely to surface on any single CI
     * run instead of relying on the framework's repeat-the-suite mechanism (which forbidden APIs disallows).
     */
    private static final int SLOT_REUSE_REPEATS = 20;

    public void testFastParserSlowConsumerPreservesOrder() throws Exception {
        int lineCount = 20_000;
        String content = buildContent(lineCount);
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);

        ExecutorService executor = Executors.newFixedThreadPool(16);
        try {
            for (int attempt = 0; attempt < SLOT_REUSE_REPEATS; attempt++) {
                InputStream stream = new ByteArrayInputStream(contentBytes);
                LineFormatReader reader = new LineFormatReader(1024);
                CloseableIterator<Page> iterator = StreamingParallelParsingCoordinator.parallelRead(
                    reader,
                    stream,
                    List.of("line"),
                    50,
                    8,
                    executor,
                    ErrorPolicy.STRICT
                );
                List<String> allLines = collectLinesSlow(iterator, 1);
                assertEquals("attempt " + attempt, lineCount, allLines.size());
                int prev = -1;
                for (String line : allLines) {
                    assertTrue(line, line.startsWith("line-"));
                    int ord = Integer.parseInt(line.substring("line-".length()), 10);
                    assertTrue("attempt " + attempt + ": lines must be strictly increasing, saw " + ord + " after " + prev, ord > prev);
                    prev = ord;
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }

    public void testQuoteAwareReaderPreventsChunkCutInsideQuotedField() throws Exception {
        // Each record is one line, but some records contain a {@code \n} inside a {@code "..."}
        // quoted field. A quote-unaware backward newline scan would slice a chunk in the middle
        // of the quoted field; the per-chunk parser would then see an unterminated record and either
        // throw or emit wrong data. With the SPI override the coordinator drives the reader's
        // quote-aware {@link SegmentableFormatReader#findLastRecordBoundary}, which keeps the entire
        // quoted record inside a single chunk.
        List<String> expected = new ArrayList<>();
        StringBuilder content = new StringBuilder();
        for (int i = 0; i < 80; i++) {
            String rec;
            if (i % 3 == 1) {
                // Quoted field with embedded newline. The reader treats the whole thing as one record.
                rec = "\"rec-" + i + "-line-a\nline-b\nline-c\"";
            } else {
                rec = "rec-" + i + "-plain";
            }
            expected.add(rec);
            content.append(rec).append('\n');
        }
        InputStream stream = new ByteArrayInputStream(content.toString().getBytes(StandardCharsets.UTF_8));

        ExecutorService executor = Executors.newFixedThreadPool(6);
        try {
            // Small chunk size forces many chunk boundaries; deterministically lands one inside
            // a quoted-newline record.
            QuoteAwareLineFormatReader reader = new QuoteAwareLineFormatReader(64);
            List<String> got = collectLines(
                StreamingParallelParsingCoordinator.parallelRead(reader, stream, List.of("line"), 16, 4, executor, ErrorPolicy.STRICT)
            );
            assertEquals(expected, got);
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * End-to-end exercise of the real {@link CsvFormatReader} through the streaming coordinator: the
     * quote-aware-stub tests above prove the SPI wiring; this one proves the production CSV implementation
     * of {@link SegmentableFormatReader#findLastRecordBoundary} actually keeps embedded-newline quoted rows
     * intact when chunked. Subclasses {@code CsvFormatReader} solely to shrink {@code minimumSegmentSize} —
     * the default 1 MiB would either need a multi-megabyte fixture or simply not exercise multiple chunks.
     */
    public void testRealCsvFormatReaderThroughCoordinatorPreservesQuotedNewlineRows() throws Exception {
        int rowCount = 200;
        // Mix of plain rows and rows whose "name" cell holds embedded \n inside a quoted field. With a
        // small chunk size every couple of rows triggers a chunk cut; some boundaries will land inside
        // the quoted block. Pre-fix the segmentator cut a chunk on the embedded \n and the parser saw
        // "Unclosed quoted field" on the first record.
        StringBuilder csv = new StringBuilder("id:long,name:keyword,tag:keyword\n");
        List<Long> expectedIds = new ArrayList<>(rowCount);
        List<String> expectedNames = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            String name = (i % 4 == 1) ? "\"line-a-" + i + "\nline-b-" + i + "\nline-c-" + i + "\"" : "plain-" + i;
            csv.append(i).append(',').append(name).append(",t-").append(i).append('\n');
            expectedIds.add((long) i);
            // Unquote+unescape what the CSV parser will emit so the comparison reflects the parsed value.
            expectedNames.add(name.startsWith("\"") ? name.substring(1, name.length() - 1) : name);
        }
        InputStream stream = new ByteArrayInputStream(csv.toString().getBytes(StandardCharsets.UTF_8));

        ExecutorService executor = Executors.newFixedThreadPool(6);
        try {
            // 256-byte chunks deterministically force boundaries inside the multi-line quoted cells.
            CsvFormatReader reader = new CsvFormatReader(TEST_BLOCK_FACTORY) {
                @Override
                public long minimumSegmentSize() {
                    return 256;
                }
            };
            List<Long> gotIds = new ArrayList<>();
            List<String> gotNames = new ArrayList<>();
            try (
                CloseableIterator<Page> it = StreamingParallelParsingCoordinator.parallelRead(
                    reader,
                    stream,
                    List.of("id", "name"),
                    32,
                    4,
                    executor,
                    ErrorPolicy.STRICT
                )
            ) {
                BytesRef scratch = new BytesRef();
                while (it.hasNext()) {
                    Page page = it.next();
                    LongBlock idBlock = page.<LongBlock>getBlock(0);
                    BytesRefBlock nameBlock = page.<BytesRefBlock>getBlock(1);
                    for (int i = 0; i < page.getPositionCount(); i++) {
                        gotIds.add(idBlock.getLong(i));
                        gotNames.add(nameBlock.getBytesRef(i, scratch).utf8ToString());
                    }
                    page.releaseBlocks();
                }
            }
            assertEquals(expectedIds, gotIds);
            assertEquals(expectedNames, gotNames);
        } finally {
            executor.shutdownNow();
        }
    }

    public void testQuoteAwareGrowPathHandlesSingleOversizedQuotedRecord() throws Exception {
        // Single record larger than the chunk size, with embedded newlines inside its quoted field.
        // Exercises the grow path: the segmentator must keep growing the buffer until a real (outside-quotes)
        // record terminator appears. The quote-unaware code would have stopped at the first embedded \n
        // and dispatched a mid-quoted chunk.
        StringBuilder big = new StringBuilder();
        big.append('"');
        for (int i = 0; i < 200; i++) {
            big.append("embedded-line-").append(i).append('\n');
        }
        big.append("\"\n");
        String record = big.toString();
        String tail = "after,plain\n";
        InputStream stream = new ByteArrayInputStream((record + tail).getBytes(StandardCharsets.UTF_8));

        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            QuoteAwareLineFormatReader reader = new QuoteAwareLineFormatReader(64);
            List<String> got = collectLines(
                StreamingParallelParsingCoordinator.parallelRead(reader, stream, List.of("line"), 16, 2, executor, ErrorPolicy.STRICT)
            );
            // Two records emitted: the big quoted one (with all embedded newlines preserved) and the plain trailing one.
            assertEquals(2, got.size());
            assertEquals(record.substring(0, record.length() - 1), got.get(0)); // drop the terminating \n
            assertEquals(tail.substring(0, tail.length() - 1), got.get(1));
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Closing must unblock the segmentator when it is parked on {@code dispatchPermits.acquire()}
     * while no consumer drains permits — relies on {@code dispatchChunk} observing {@code closed} after a wake-up acquire.
     * <p>
     * Synchronisation is deterministic: we wait via {@link #assertBusy} until
     * {@link StreamingParallelParsingCoordinator.StreamingParallelIterator#isSegmentatorParkedOnDispatchPermits()}
     * returns {@code true}, then assert that {@link CloseableIterator#close()} returns within a generous
     * deadline. A timing-only sleep would leave the test passing for the wrong reason if the segmentator
     * happened to be blocked on {@code chunkQueue.put} or upstream {@code read()} instead.
     */
    public void testCloseWhileSegmentatorParkedOnDispatchPermit() throws Exception {
        int parallelism = 2;
        byte[] payload = new byte[100 * 1024];
        Arrays.fill(payload, (byte) 'x');
        for (int i = 127; i < payload.length - 1; i += 128) {
            payload[i] = '\n';
        }
        payload[payload.length - 1] = '\n';

        InputStream stream = new ByteArrayInputStream(payload);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            LineFormatReader reader = new LineFormatReader(256);
            CloseableIterator<Page> iterator = StreamingParallelParsingCoordinator.parallelRead(
                reader,
                stream,
                List.of("line"),
                50,
                parallelism,
                executor,
                ErrorPolicy.STRICT
            );
            StreamingParallelParsingCoordinator.StreamingParallelIterator streamingIterator =
                (StreamingParallelParsingCoordinator.StreamingParallelIterator) iterator;
            assertBusy(() -> assertTrue(streamingIterator.isSegmentatorParkedOnDispatchPermits()), 5, TimeUnit.SECONDS);
            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            iterator.close();
            assertTrue("close() must return within 5s of segmentator being parked", System.nanoTime() <= deadlineNanos);
        } finally {
            executor.shutdownNow();
        }
    }

    private static String buildContent(int lineCount) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < lineCount; i++) {
            sb.append("line-").append(String.format(java.util.Locale.ROOT, "%04d", i)).append('\n');
        }
        return sb.toString();
    }

    private static List<String> collectLines(CloseableIterator<Page> iterator) throws IOException {
        List<String> lines = new ArrayList<>();
        try (iterator) {
            BytesRef scratch = new BytesRef();
            while (iterator.hasNext()) {
                Page page = iterator.next();
                BytesRefBlock block = page.<BytesRefBlock>getBlock(0);
                for (int i = 0; i < block.getPositionCount(); i++) {
                    lines.add(block.getBytesRef(i, scratch).utf8ToString());
                }
                page.releaseBlocks();
            }
        }
        return lines;
    }

    private static List<String> collectLinesSlow(CloseableIterator<Page> iterator, int sleepEveryNPages) throws Exception {
        List<String> lines = new ArrayList<>();
        try (iterator) {
            BytesRef scratch = new BytesRef();
            int pageOrdinal = 0;
            while (iterator.hasNext()) {
                Page page = iterator.next();
                BytesRefBlock block = page.<BytesRefBlock>getBlock(0);
                for (int i = 0; i < block.getPositionCount(); i++) {
                    lines.add(block.getBytesRef(i, scratch).utf8ToString());
                }
                page.releaseBlocks();
                pageOrdinal++;
                if (sleepEveryNPages > 0 && pageOrdinal % sleepEveryNPages == 0) {
                    Thread.sleep(1L);
                }
            }
        }
        return lines;
    }

    /**
     * A minimal format reader that treats each line as a record, producing one keyword block per page.
     * <p>
     * Tracks {@link #metadataCalls}, {@link #boundReadCalls}, and {@link #unboundReadCalls} per
     * <strong>root</strong> reader (the counters are aliased into every {@link #withSchema}
     * variant). Tests use these counters to assert that the coordinator infers schema once and
     * dispatches every parser-thread read to the schema-bound variant.
     */
    private static class LineFormatReader implements SegmentableFormatReader, NoConfigFormatReader {

        private final long minSegment;
        private final List<Attribute> resolvedSchema;
        final AtomicInteger metadataCalls;
        final AtomicInteger boundReadCalls;
        final AtomicInteger unboundReadCalls;
        final List<FormatReadContext> seenContexts;

        LineFormatReader(long minSegment) {
            this(
                minSegment,
                null,
                new AtomicInteger(),
                new AtomicInteger(),
                new AtomicInteger(),
                Collections.synchronizedList(new ArrayList<>())
            );
        }

        private LineFormatReader(
            long minSegment,
            List<Attribute> resolvedSchema,
            AtomicInteger metadataCalls,
            AtomicInteger boundReadCalls,
            AtomicInteger unboundReadCalls,
            List<FormatReadContext> seenContexts
        ) {
            this.minSegment = minSegment;
            this.resolvedSchema = resolvedSchema;
            this.metadataCalls = metadataCalls;
            this.boundReadCalls = boundReadCalls;
            this.unboundReadCalls = unboundReadCalls;
            this.seenContexts = seenContexts;
        }

        @Override
        public long findNextRecordBoundary(InputStream stream) throws IOException {
            long consumed = 0;
            int b;
            while ((b = stream.read()) != -1) {
                consumed++;
                if (b == '\n') {
                    return consumed;
                }
            }
            return -1;
        }

        @Override
        public long minimumSegmentSize() {
            return minSegment;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            metadataCalls.incrementAndGet();
            List<Attribute> schema = List.of(
                new ReferenceAttribute(Source.EMPTY, null, "line", DataType.KEYWORD, Nullability.TRUE, null, false)
            );
            return new SimpleSourceMetadata(schema, formatName(), object.path().toString());
        }

        @Override
        public FormatReader withSchema(List<Attribute> schema) {
            return new LineFormatReader(minSegment, schema, metadataCalls, boundReadCalls, unboundReadCalls, seenContexts);
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            (resolvedSchema != null ? boundReadCalls : unboundReadCalls).incrementAndGet();
            seenContexts.add(context);
            InputStream stream = object.newStream();
            List<String> lines = new ArrayList<>();
            StringBuilder current = new StringBuilder();

            // Mirror the production behavior: skip a leading partial record only when the caller
            // has not guaranteed record-alignment (e.g. byte-range macro-splits). Streaming chunks
            // and segment-aligned splits set recordAligned=true, in which case the leading bytes
            // are a complete record.
            boolean skipFirst = context.firstSplit() == false && context.recordAligned() == false;

            int b;
            while ((b = stream.read()) != -1) {
                if (b == '\n') {
                    if (skipFirst) {
                        skipFirst = false;
                        current.setLength(0);
                        continue;
                    }
                    if (current.length() > 0) {
                        lines.add(current.toString());
                    }
                    current.setLength(0);
                } else {
                    current.append((char) b);
                }
            }
            if (current.length() > 0 && skipFirst == false) {
                lines.add(current.toString());
            }

            List<Page> pages = new ArrayList<>();
            int batchSize = context.batchSize();
            for (int start = 0; start < lines.size(); start += batchSize) {
                int end = Math.min(start + batchSize, lines.size());
                int count = end - start;
                try (var builder = TEST_BLOCK_FACTORY.newBytesRefBlockBuilder(count)) {
                    for (int i = start; i < end; i++) {
                        builder.appendBytesRef(new BytesRef(lines.get(i)));
                    }
                    pages.add(new Page(count, builder.build()));
                }
            }

            return new CloseableIterator<>() {
                int idx = 0;

                @Override
                public boolean hasNext() {
                    return idx < pages.size();
                }

                @Override
                public Page next() {
                    return pages.get(idx++);
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public String formatName() {
            return "line";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".txt");
        }

        @Override
        public void close() {}
    }

    /**
     * Minimal quote-aware reader used to validate that the coordinator routes chunk-cut decisions
     * through {@link SegmentableFormatReader#findLastRecordBoundary}. Records are one-per-line, but
     * a {@code "} character opens a quoted region in which {@code \n} is a literal byte rather than
     * a record terminator. A matching {@code "} closes the region. This mirrors the rule CSV applies
     * to quoted fields, in just enough detail to drive the segmentator's quote-aware path.
     */
    private static class QuoteAwareLineFormatReader implements SegmentableFormatReader, NoConfigFormatReader {

        private final long minSegment;

        QuoteAwareLineFormatReader(long minSegment) {
            this.minSegment = minSegment;
        }

        @Override
        public long findNextRecordBoundary(InputStream stream) throws IOException {
            long consumed = 0;
            boolean inQuotes = false;
            int b;
            while ((b = stream.read()) != -1) {
                consumed++;
                if (b == '"') {
                    inQuotes = inQuotes == false;
                } else if (b == '\n' && inQuotes == false) {
                    return consumed;
                }
            }
            return -1;
        }

        @Override
        public int findLastRecordBoundary(byte[] buf, int length) throws IOException {
            if (length <= 0) {
                return -1;
            }
            ByteArrayInputStream bis = new ByteArrayInputStream(buf, 0, length);
            int lastBoundary = -1;
            int cumulative = 0;
            while (true) {
                long consumed = findNextRecordBoundary(bis);
                if (consumed < 0) {
                    return lastBoundary;
                }
                cumulative += Math.toIntExact(consumed);
                lastBoundary = cumulative - 1;
            }
        }

        @Override
        public long minimumSegmentSize() {
            return minSegment;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return new SimpleSourceMetadata(
                List.of(new ReferenceAttribute(Source.EMPTY, null, "line", DataType.KEYWORD, Nullability.TRUE, null, false)),
                formatName(),
                object.path().toString()
            );
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            InputStream stream = object.newStream();
            List<String> records = new ArrayList<>();
            StringBuilder current = new StringBuilder();
            boolean skipFirst = context.firstSplit() == false && context.recordAligned() == false;
            boolean inQuotes = false;
            int b;
            while ((b = stream.read()) != -1) {
                if (b == '"') {
                    inQuotes = inQuotes == false;
                    current.append((char) b);
                    continue;
                }
                if (b == '\n' && inQuotes == false) {
                    if (skipFirst) {
                        skipFirst = false;
                        current.setLength(0);
                        continue;
                    }
                    if (current.length() > 0) {
                        records.add(current.toString());
                    }
                    current.setLength(0);
                } else {
                    current.append((char) b);
                }
            }
            if (current.length() > 0 && skipFirst == false) {
                records.add(current.toString());
            }

            int batchSize = context.batchSize();
            List<Page> pages = new ArrayList<>();
            for (int start = 0; start < records.size(); start += batchSize) {
                int end = Math.min(start + batchSize, records.size());
                int count = end - start;
                try (var builder = TEST_BLOCK_FACTORY.newBytesRefBlockBuilder(count)) {
                    for (int i = start; i < end; i++) {
                        builder.appendBytesRef(new BytesRef(records.get(i)));
                    }
                    pages.add(new Page(count, builder.build()));
                }
            }
            return new CloseableIterator<>() {
                int idx = 0;

                @Override
                public boolean hasNext() {
                    return idx < pages.size();
                }

                @Override
                public Page next() {
                    return pages.get(idx++);
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public String formatName() {
            return "quoted-line";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".qline");
        }

        @Override
        public void close() {}
    }

    /**
     * A format reader that fails after reading a configured number of lines.
     */
    private static class FailingFormatReader implements SegmentableFormatReader, NoConfigFormatReader {

        private final int failAfterLines;
        private final long minSegment;

        FailingFormatReader(int failAfterLines, long minSegment) {
            this.failAfterLines = failAfterLines;
            this.minSegment = minSegment;
        }

        @Override
        public long findNextRecordBoundary(InputStream stream) throws IOException {
            long consumed = 0;
            int b;
            while ((b = stream.read()) != -1) {
                consumed++;
                if (b == '\n') {
                    return consumed;
                }
            }
            return -1;
        }

        @Override
        public long minimumSegmentSize() {
            return minSegment;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            // Return a non-null schema so the coordinator's first-chunk inference does not
            // mask the injected parser failure being tested here.
            return new SimpleSourceMetadata(
                List.of(new ReferenceAttribute(Source.EMPTY, null, "line", DataType.KEYWORD, Nullability.TRUE, null, false)),
                formatName(),
                object.path().toString()
            );
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            InputStream stream = object.newStream();
            int lineCount = 0;
            int b;
            while ((b = stream.read()) != -1) {
                if (b == '\n') {
                    lineCount++;
                    if (lineCount >= failAfterLines) {
                        throw new IOException("injected failure after " + failAfterLines + " lines");
                    }
                }
            }
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

        @Override
        public String formatName() {
            return "failing";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of();
        }

        @Override
        public void close() {}
    }
}
