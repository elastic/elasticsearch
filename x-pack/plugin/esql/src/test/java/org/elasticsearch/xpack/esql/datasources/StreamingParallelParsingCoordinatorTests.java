/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
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
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonFormatReader;
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
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
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
                assertEquals("line-" + String.format(Locale.ROOT, "%04d", i), allLines.get(i));
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
                assertEquals("line-" + String.format(Locale.ROOT, "%04d", i), allLines.get(i));
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

    /**
     * On a fresh iterator with no chunks yet dispatched, {@link CloseableIterator#waitForReady()} must
     * NOT complete synchronously — it must register a listener that fires only when the segmentator
     * publishes the first chunk or reaches EOF. This is the core consumer-yield contract that lets the
     * producer-loop release its executor slot back to the pool while parser/segmenter sub-tasks run.
     * Without it, the producer-loop would spin inside {@code hasNext()} holding the slot, deadlocking
     * the pool on multi-file gzip globs with default {@code parsing_parallelism = cores}.
     */
    public void testWaitForReadyParksUntilFirstChunkOrEof() throws Exception {
        // Synthesize a stream the segmenter cannot yet make progress on by gating it behind a latch
        // wired into a slow read.
        CountDownLatch unblockRead = new CountDownLatch(1);
        byte[] payload = "line-0\nline-1\n".getBytes(StandardCharsets.UTF_8);
        InputStream gatedStream = new InputStream() {
            int pos = 0;

            @Override
            public int read() throws IOException {
                if (pos == 0) {
                    try {
                        unblockRead.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("interrupted", e);
                    }
                }
                if (pos >= payload.length) return -1;
                return payload[pos++] & 0xff;
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            LineFormatReader reader = new LineFormatReader(1024);
            CloseableIterator<Page> iterator = StreamingParallelParsingCoordinator.parallelRead(
                reader,
                gatedStream,
                List.of("line"),
                50,
                2,
                executor,
                ErrorPolicy.STRICT
            );
            org.elasticsearch.action.support.SubscribableListener<Void> ready = iterator.waitForReady();
            assertFalse("waitForReady must NOT complete before any chunk is dispatched", ready.isDone());
            // Now unblock the segmenter; it dispatches chunk 0 → signalReady fires → listener completes.
            unblockRead.countDown();
            assertBusy(() -> assertTrue("ready listener must fire after first chunk dispatched", ready.isDone()), 5, TimeUnit.SECONDS);
            iterator.close();
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * When a chunk's page is already in {@code pageQueues[currentSlot]}, {@link CloseableIterator#waitForReady()}
     * must complete synchronously — the consumer can drain without yielding.
     */
    public void testWaitForReadyImmediateWhenPageAlreadyAvailable() throws Exception {
        String content = buildContent(20);
        InputStream stream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            LineFormatReader reader = new LineFormatReader(1024);
            CloseableIterator<Page> iterator = StreamingParallelParsingCoordinator.parallelRead(
                reader,
                stream,
                List.of("line"),
                50,
                2,
                executor,
                ErrorPolicy.STRICT
            );
            // Wait until the iterator has produced at least one page worth of work, then check ready.
            assertBusy(() -> assertTrue(iterator.hasNext()), 5, TimeUnit.SECONDS);
            org.elasticsearch.action.support.SubscribableListener<Void> ready = iterator.waitForReady();
            assertTrue("waitForReady must complete immediately when a page is buffered or available", ready.isDone());
            collectLines(iterator);
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Closing while a consumer is parked on {@link CloseableIterator#waitForReady()} must complete
     * the listener so the producer-loop doesn't leak its registered callback past iterator shutdown.
     */
    public void testWaitForReadyResolvesOnClose() throws Exception {
        CountDownLatch unblockRead = new CountDownLatch(1);
        InputStream gatedStream = new InputStream() {
            @Override
            public int read() throws IOException {
                try {
                    unblockRead.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted", e);
                }
                return -1;
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            LineFormatReader reader = new LineFormatReader(1024);
            CloseableIterator<Page> iterator = StreamingParallelParsingCoordinator.parallelRead(
                reader,
                gatedStream,
                List.of("line"),
                50,
                2,
                executor,
                ErrorPolicy.STRICT
            );
            org.elasticsearch.action.support.SubscribableListener<Void> ready = iterator.waitForReady();
            assertFalse("waitForReady must NOT complete while stream is gated", ready.isDone());
            iterator.close();
            assertBusy(() -> assertTrue("ready listener must fire after close()", ready.isDone()), 5, TimeUnit.SECONDS);
            unblockRead.countDown();
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Regression test for the producer-loop pool-exhaustion deadlock: verifies that many file
     * readers can drain against a tiny shared executor pool without one iterator's producer-loop
     * blocking the sub-tasks that another iterator (or its own) needs to make progress. Pre-fix,
     * {@code F} file readers with {@code parsing_parallelism = N} submitted {@code F × (1 + N)}
     * sub-tasks plus {@code F} producer-loop drivers — a pool of {@code F × (2 + N)} threads.
     * With a smaller pool, producer-loops blocked inside {@code hasNext()} occupy slots that their
     * sub-tasks need; post-fix, producer-loops yield via {@link CloseableIterator#waitForReady()}
     * so the deadlock can't form.
     */
    public void testConcurrentFileReadersWithUndersizedPoolDoNotDeadlock() throws Exception {
        int fileCount = 8;
        int parsingParallelism = 4;
        // Pool size strictly less than F + F * (1 + parsingParallelism) so the pre-fix code would have
        // sub-tasks queued behind producer-loops indefinitely.
        int poolSize = 6;
        ExecutorService executor = Executors.newFixedThreadPool(poolSize);
        java.util.List<CloseableIterator<Page>> iterators = new java.util.ArrayList<>();
        try {
            for (int f = 0; f < fileCount; f++) {
                InputStream s = new ByteArrayInputStream(buildContent(50).getBytes(StandardCharsets.UTF_8));
                LineFormatReader reader = new LineFormatReader(1024);
                iterators.add(
                    StreamingParallelParsingCoordinator.parallelRead(
                        reader,
                        s,
                        List.of("line"),
                        50,
                        parsingParallelism,
                        executor,
                        ErrorPolicy.STRICT
                    )
                );
            }
            // Drain all iterators on a SINGLE consumer thread (= mimics the producer-loop pattern of
            // one driver per iterator, all competing for the same pool). Without the waitForReady
            // yield this would deadlock; with it, each iterator's hasNext() forces a sub-task slot
            // to free up before the producer-loop reclaims it.
            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            for (CloseableIterator<Page> it : iterators) {
                // We don't have a producer-loop here so we can't directly test the yield from inside
                // hasNext(). Instead we exercise the same waitForReady contract that the producer-loop
                // depends on: until it is done, the iterator should NOT claim more page-emitting work.
                while (it.hasNext()) {
                    if (System.nanoTime() > deadlineNanos) {
                        fail("iterator did not drain within 60s — likely deadlock");
                    }
                    Page p = it.next();
                    p.releaseBlocks();
                }
                it.close();
            }
        } finally {
            for (CloseableIterator<Page> it : iterators) {
                try {
                    it.close();
                } catch (IOException ignored) {}
            }
            executor.shutdownNow();
        }
    }

    /**
     * Stress test the consumer's EOF predicate against the race between the segmentator's final
     * {@code chunksDispatched.incrementAndGet()} and the dispatched parser task's
     * {@code tasksOutstanding.decrementAndGet()} in its {@code finally} block. The EOF condition
     * — {@code currentChunk >= chunksDispatched && tasksOutstanding == 0} — must be re-checked
     * after the apparent-empty branch of {@code takeNextPage}; without the re-read, the consumer
     * can return false while a parser is mid-page-publish, dropping the last chunk's rows.
     * <p>
     * Tiny chunks plus a small input plus high parallelism maximises the chance the consumer
     * arrives at the EOF check exactly inside the dispatch / decrement window. A regression that
     * reorders the writes or removes the re-read drops a chunk in a small fraction of iterations
     * and trips the exact-row-count or ordering assertion below within a few hundred runs.
     * <p>
     * Lifted from <a href="https://github.com/elastic/elasticsearch/pull/148802">#148802</a>
     * (closed in favour of this PR's broader fix shape); the test is portable because it asserts
     * only the externally-visible contract (row count + ordering across {@code N} iterations),
     * not the internal counter names.
     */
    public void testStressEofDetectionRace() throws Exception {
        int iterations = 1000;
        int lineCount = 50;
        int parallelism = 8;
        int batchSize = 4;
        int chunkSize = 32;

        String content = buildContent(lineCount);
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        ExecutorService executor = Executors.newFixedThreadPool(parallelism + 1);
        try {
            for (int iter = 0; iter < iterations; iter++) {
                LineFormatReader reader = new LineFormatReader(chunkSize);
                List<String> got = collectLines(
                    StreamingParallelParsingCoordinator.parallelRead(
                        reader,
                        new ByteArrayInputStream(bytes),
                        List.of("line"),
                        batchSize,
                        parallelism,
                        executor,
                        ErrorPolicy.STRICT
                    )
                );
                assertEquals("iter " + iter, lineCount, got.size());
                for (int i = 0; i < lineCount; i++) {
                    assertEquals("iter " + iter + " line " + i, "line-" + String.format(Locale.ROOT, "%04d", i), got.get(i));
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private static String buildContent(int lineCount) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < lineCount; i++) {
            sb.append("line-").append(String.format(Locale.ROOT, "%04d", i)).append('\n');
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
     * Content with multi-line quoted fields where the embedded {@code \n} falls exactly on a chunk
     * boundary must not split the logical record across two parser-thread chunks.
     * <p>
     * Uses a tiny chunk size (64 bytes) so the quoted field's inner newline is almost certain to
     * land on a chunk boundary at some offset; the test verifies every record is delivered intact.
     */
    public void testMultiLineQuotedFieldsAcrossChunkBoundaries() throws Exception {
        // 50 records, some with multi-line quoted fields containing embedded \n
        StringBuilder sb = new StringBuilder();
        int recordCount = 0;
        for (int i = 0; i < 50; i++) {
            if (i % 5 == 0) {
                sb.append("\"multi\nline-").append(String.format(Locale.ROOT, "%04d", i)).append("\"\n");
            } else {
                sb.append("simple-").append(String.format(Locale.ROOT, "%04d", i)).append('\n');
            }
            recordCount++;
        }
        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        ExecutorService executor = Executors.newFixedThreadPool(6);
        try {
            // chunkSize=64 forces many splits; quoted \n must not become a chunk boundary
            QuoteAwareLineFormatReader reader = new QuoteAwareLineFormatReader(64);
            List<String> allLines = collectLines(
                StreamingParallelParsingCoordinator.parallelRead(
                    reader,
                    new ByteArrayInputStream(bytes),
                    List.of("line"),
                    50,
                    4,
                    executor,
                    ErrorPolicy.STRICT
                )
            );

            assertEquals(recordCount, allLines.size());
            int multiLineCount = 0;
            for (String line : allLines) {
                if (line.startsWith("multi\nline-")) {
                    multiLineCount++;
                }
            }
            assertEquals("every 5th record is multi-line", 10, multiLineCount);
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * When the chunk size is smaller than a single multi-line record (no record boundary in the
     * initial buffer), the coordinator must grow the buffer until it finds a real record boundary.
     */
    public void testGrowUntilRecordBoundaryForOversizedQuotedField() throws Exception {
        // One giant multi-line quoted field that is larger than the chunk size, followed by a
        // normal record. The coordinator must grow past the first \n inside the quoted field.
        StringBuilder sb = new StringBuilder();
        sb.append("\"");
        // ~200 bytes of quoted content with embedded \n every 40 chars
        for (int i = 0; i < 5; i++) {
            sb.append("chunk-of-text-that-is-about-forty-bytes!");
            if (i < 4) sb.append('\n');
        }
        sb.append("\"\n");
        sb.append("trailing-record\n");

        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        ExecutorService executor = Executors.newFixedThreadPool(6);
        try {
            // Chunk size smaller than the first record → forces growUntilRecordBoundary
            QuoteAwareLineFormatReader reader = new QuoteAwareLineFormatReader(64);
            List<String> allLines = collectLines(
                StreamingParallelParsingCoordinator.parallelRead(
                    reader,
                    new ByteArrayInputStream(bytes),
                    List.of("line"),
                    50,
                    4,
                    executor,
                    ErrorPolicy.STRICT
                )
            );

            assertEquals(2, allLines.size());
            assertTrue("first record should contain embedded newlines", allLines.get(0).contains("\n"));
            assertEquals("trailing-record", allLines.get(1));
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * A large stream where every Nth record has a multi-line quoted field: verifies order
     * preservation and correct record count end-to-end under realistic parallelism.
     */
    public void testLargeStreamWithInterspersedMultiLineRecords() throws Exception {
        int totalRecords = 2000;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < totalRecords; i++) {
            if (i % 7 == 0) {
                sb.append("\"quoted\nrecord-").append(String.format(Locale.ROOT, "%04d", i)).append("\"\n");
            } else {
                sb.append("plain-").append(String.format(Locale.ROOT, "%04d", i)).append('\n');
            }
        }
        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        try {
            QuoteAwareLineFormatReader reader = new QuoteAwareLineFormatReader(512);
            List<String> allLines = collectLines(
                StreamingParallelParsingCoordinator.parallelRead(
                    reader,
                    new ByteArrayInputStream(bytes),
                    List.of("line"),
                    100,
                    6,
                    executor,
                    ErrorPolicy.STRICT
                )
            );

            assertEquals(totalRecords, allLines.size());
            for (int i = 0; i < totalRecords; i++) {
                String line = allLines.get(i);
                if (i % 7 == 0) {
                    String expected = "quoted\nrecord-" + String.format(Locale.ROOT, "%04d", i);
                    assertEquals("record " + i, expected, line);
                } else {
                    String expected = "plain-" + String.format(Locale.ROOT, "%04d", i);
                    assertEquals("record " + i, expected, line);
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * When all records are simple (no quoting), the quote-aware boundary finder must behave
     * identically to the naive newline finder — regression guard.
     */
    public void testQuoteAwareReaderWithNoQuotedFieldsMatchesNaiveBehavior() throws Exception {
        int lineCount = 500;
        String content = buildContent(lineCount);
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        ExecutorService executor = Executors.newFixedThreadPool(6);
        try {
            QuoteAwareLineFormatReader reader = new QuoteAwareLineFormatReader(256);
            List<String> allLines = collectLines(
                StreamingParallelParsingCoordinator.parallelRead(
                    reader,
                    new ByteArrayInputStream(bytes),
                    List.of("line"),
                    50,
                    4,
                    executor,
                    ErrorPolicy.STRICT
                )
            );

            assertEquals(lineCount, allLines.size());
            for (int i = 0; i < lineCount; i++) {
                assertEquals("line-" + String.format(Locale.ROOT, "%04d", i), allLines.get(i));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * End-to-end test: drives {@link NdJsonFormatReader} through the streaming parallel coordinator,
     * verifying that chunk splitting at record boundaries, schema inference from the first chunk, and
     * multi-chunk ordered output all work with the real NDJSON parser.
     * <p>
     * Uses a 64 KiB segment size (the minimum) with ~4 000 records (~300 KiB) so the coordinator
     * produces 4–5 chunks. Projects a single {@code "name"} keyword column for easy comparison with
     * the existing {@link #collectLines} helper.
     */
    public void testNdJsonFormatReaderStreamingParallel() throws Exception {
        int recordCount = 4000;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < recordCount; i++) {
            sb.append(String.format(Locale.ROOT, "{\"id\":%d,\"name\":\"record-%04d\",\"value\":%d}\n", i, i, i * 7));
        }
        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);

        Settings settings = Settings.builder().put(NdJsonFormatReader.SEGMENT_SIZE_SETTING, "64kb").build();
        NdJsonFormatReader reader = new NdJsonFormatReader(settings, TEST_BLOCK_FACTORY, null);

        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<String> names = collectLines(
                StreamingParallelParsingCoordinator.parallelRead(
                    reader,
                    new ByteArrayInputStream(bytes),
                    List.of("name"),
                    100,
                    4,
                    executor,
                    ErrorPolicy.STRICT
                )
            );

            assertEquals(recordCount, names.size());
            for (int i = 0; i < recordCount; i++) {
                assertEquals("record " + i, "record-" + String.format(Locale.ROOT, "%04d", i), names.get(i));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Variant of {@link #testNdJsonFormatReaderStreamingParallel()} that projects no columns,
     * exercising the {@code COUNT(*)} fast path where the NDJSON decoder only counts records without
     * materialising field values. The coordinator must still split and order chunks correctly.
     */
    public void testNdJsonFormatReaderStreamingParallelCountOnly() throws Exception {
        int recordCount = 5000;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < recordCount; i++) {
            sb.append(String.format(Locale.ROOT, "{\"id\":%d,\"name\":\"row-%04d\"}\n", i, i));
        }
        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);

        Settings settings = Settings.builder().put(NdJsonFormatReader.SEGMENT_SIZE_SETTING, "64kb").build();
        NdJsonFormatReader reader = new NdJsonFormatReader(settings, TEST_BLOCK_FACTORY, null);

        ExecutorService executor = Executors.newFixedThreadPool(6);
        try {
            int totalRows = 0;
            try (
                CloseableIterator<Page> pages = StreamingParallelParsingCoordinator.parallelRead(
                    reader,
                    new ByteArrayInputStream(bytes),
                    List.of(),
                    200,
                    4,
                    executor,
                    ErrorPolicy.STRICT
                )
            ) {
                while (pages.hasNext()) {
                    Page page = pages.next();
                    totalRows += page.getPositionCount();
                    page.releaseBlocks();
                }
            }
            assertEquals(recordCount, totalRows);
        } finally {
            executor.shutdownNow();
        }
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

    /**
     * A format reader that supports {@code "}-quoted multi-line fields, modelling the CSV case
     * where a {@code \n} inside {@code "..."} is part of the field value, not a record boundary.
     * <p>
     * The format is trivial: each record is one "field" terminated by {@code \n}. If the field
     * starts with {@code "}, the record ends at the closing {@code "} followed by {@code \n}
     * (embedded {@code \n} are literal). {@link #findNextRecordBoundary} mirrors this quoting
     * convention so the streaming coordinator splits chunks at real record boundaries.
     * <p>
     * <b>Limitation:</b> escaped quotes ({@code ""}) are not handled; {@code "} is treated as a
     * simple open/close toggle. This is sufficient for testing the coordinator's chunk-splitting
     * logic but does not model full RFC 4180 quoting.
     */
    private static class QuoteAwareLineFormatReader implements SegmentableFormatReader, NoConfigFormatReader {

        private final long minSegment;
        private final List<Attribute> resolvedSchema;

        QuoteAwareLineFormatReader(long minSegment) {
            this(minSegment, null);
        }

        private QuoteAwareLineFormatReader(long minSegment, List<Attribute> resolvedSchema) {
            this.minSegment = minSegment;
            this.resolvedSchema = resolvedSchema;
        }

        @Override
        public long findNextRecordBoundary(InputStream stream) throws IOException {
            long consumed = 0;
            int first = stream.read();
            if (first == -1) return -1;
            consumed++;
            if (first == '\n') {
                return consumed;
            }
            boolean inQuotes = (first == '"');
            int b;
            while ((b = stream.read()) != -1) {
                consumed++;
                if (inQuotes) {
                    if (b == '"') {
                        inQuotes = false;
                    }
                } else if (b == '\n') {
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
            List<Attribute> schema = List.of(
                new ReferenceAttribute(Source.EMPTY, null, "line", DataType.KEYWORD, Nullability.TRUE, null, false)
            );
            return new SimpleSourceMetadata(schema, formatName(), object.path().toString());
        }

        @Override
        public FormatReader withSchema(List<Attribute> schema) {
            return new QuoteAwareLineFormatReader(minSegment, schema);
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            InputStream stream = object.newStream();
            List<String> records = new ArrayList<>();
            StringBuilder current = new StringBuilder();
            boolean inQuotes = false;

            int b;
            while ((b = stream.read()) != -1) {
                char c = (char) b;
                if (inQuotes) {
                    if (c == '"') {
                        inQuotes = false;
                    } else {
                        current.append(c);
                    }
                } else if (c == '"' && current.length() == 0) {
                    inQuotes = true;
                } else if (c == '\n') {
                    if (current.length() > 0) {
                        records.add(current.toString());
                    }
                    current.setLength(0);
                } else {
                    current.append(c);
                }
            }
            if (current.length() > 0) {
                records.add(current.toString());
            }

            List<Page> pages = new ArrayList<>();
            int batchSize = context.batchSize();
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
            return "quote-aware-line";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".txt");
        }

        @Override
        public void close() {}
    }
}
