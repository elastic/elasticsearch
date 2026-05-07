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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
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
    private static class LineFormatReader implements SegmentableFormatReader {
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
     * A format reader that fails after reading a configured number of lines.
     */
    private static class FailingFormatReader implements SegmentableFormatReader {
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
