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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Locale.ROOT;

/**
 * Adversarial probes against the as-ready / reset-replay rewrite of {@link ParallelParsingCoordinator}.
 * Production code is NOT modified; these tests demonstrate (or clear) specific defects.
 */
public class ParallelParsingAdversarialTests extends ESTestCase {

    private static final BlockFactory TEST_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
        .build();

    private static byte[] repeatedLines(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.append("line-").append(String.format(ROOT, "%05d", i)).append("\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /* ===================================================================================
     * ATTACK 1 (now a recovery guard): page boundaries differ on re-read. The reset-replay must
     * NOT assume the re-read reproduces identical page boundaries. A reader whose page sizes vary
     * across reads (legitimate: a streaming reader that fills pages by time/buffer availability
     * rather than a fixed batchSize) makes the resume skip land mid-page. The fix skips at the ROW
     * level — dropping the already-delivered prefix of that page and enqueuing only the remaining
     * rows — so the query recovers fully: every row exactly once, no exception.
     * =================================================================================== */
    public void testResetReplayWithDifferentPageBoundariesRecoversFully() throws Exception {
        int rows = 1200;
        byte[] content = repeatedLines(rows);
        InMemoryStorageObject obj = new InMemoryStorageObject(content);

        // Segment 1: first read emits pages of size 50; the re-read (after the reset) emits size 40.
        // deliveredRows before the reset is a multiple of 50; on re-read the skip boundary will not align
        // to a 40-row page boundary, so the resume must slice mid-page rather than fail.
        VaryingPageSizeResetReader reader = new VaryingPageSizeResetReader(blockFactory(), 1, 3, 50, 40);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        List<String> read = new ArrayList<>();
        try {
            try (CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 50, 4, exec)) {
                while (iter.hasNext()) {
                    Page p = iter.next();
                    BytesRefBlock b = (BytesRefBlock) p.getBlock(0);
                    BytesRef scratch = new BytesRef();
                    for (int i = 0; i < b.getPositionCount(); i++) {
                        read.add(b.getBytesRef(b.getFirstValueIndex(i), scratch).utf8ToString());
                    }
                    p.releaseBlocks();
                }
            }
            assertTrue("reset must have fired", reader.resetFired.get());
            // The fix: row-level resume across a re-read that repaginates. No exception, all rows once.
            assertEquals("every row exactly once despite the re-read repaginating", rows, read.size());
            List<String> sorted = new ArrayList<>(read);
            Collections.sort(sorted);
            for (int i = 0; i < rows; i++) {
                assertEquals(
                    "no row dropped or duplicated when the skip boundary lands mid-page",
                    String.format(ROOT, "line-%05d", i),
                    sorted.get(i)
                );
            }
        } finally {
            exec.shutdown();
            assertTrue(exec.awaitTermination(60, TimeUnit.SECONDS));
        }
    }

    /* ===================================================================================
     * ATTACK 2: reset fires AGAIN during the replay (reset while re-reading). Each attempt
     * re-reads from the start, but if the resume keeps tripping resets, do we still deliver
     * every row exactly once, and do we respect the retry bound?
     * =================================================================================== */
    public void testResetDuringReplayStillDeliversAllRowsOnce() throws Exception {
        int rows = 1500;
        byte[] content = repeatedLines(rows);
        InMemoryStorageObject obj = new InMemoryStorageObject(content);

        // Fire a reset on the 1st, 2nd and 3rd reads of segment 1 (each after a few pages), then succeed.
        // That is exactly MAX_STREAM_RESET_RETRIES (3) retries; the 4th attempt must read clean.
        MultiResetReader reader = new MultiResetReader(blockFactory(), 1, 3, 3);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        List<String> read = new ArrayList<>();
        try {
            try (CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 50, 4, exec)) {
                while (iter.hasNext()) {
                    Page p = iter.next();
                    BytesRefBlock b = (BytesRefBlock) p.getBlock(0);
                    BytesRef scratch = new BytesRef();
                    for (int i = 0; i < b.getPositionCount(); i++) {
                        read.add(b.getBytesRef(b.getFirstValueIndex(i), scratch).utf8ToString());
                    }
                    p.releaseBlocks();
                }
            }
            assertEquals("the 3 configured resets must have fired", 3, reader.resetCount.get());
            assertEquals("every row exactly once across repeated resets", rows, read.size());
            List<String> sorted = new ArrayList<>(read);
            Collections.sort(sorted);
            for (int i = 0; i < rows; i++) {
                assertEquals(String.format(ROOT, "line-%05d", i), sorted.get(i));
            }
        } finally {
            exec.shutdown();
            assertTrue(exec.awaitTermination(60, TimeUnit.SECONDS));
        }
    }

    /* ===================================================================================
     * ATTACK 3: retry exhaustion. Every attempt resets. Must fail cleanly (no hang, no leak,
     * executor terminates), and the error must surface to the consumer.
     * =================================================================================== */
    public void testResetRetryExhaustionFailsCleanly() throws Exception {
        int rows = 1500;
        byte[] content = repeatedLines(rows);
        InMemoryStorageObject obj = new InMemoryStorageObject(content);

        // Reset on EVERY read of segment 1 (more than MAX_STREAM_RESET_RETRIES).
        MultiResetReader reader = new MultiResetReader(blockFactory(), 1, 3, Integer.MAX_VALUE);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            RuntimeException ex = expectThrows(RuntimeException.class, () -> {
                try (CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 50, 4, exec)) {
                    while (iter.hasNext()) {
                        iter.next().releaseBlocks();
                    }
                }
            });
            assertTrue("the connection-reset error must surface after exhausting retries", containsMessage(ex, "Connection reset"));
        } finally {
            exec.shutdown();
            assertTrue("executor must terminate (no hung worker)", exec.awaitTermination(60, TimeUnit.SECONDS));
        }
    }

    /* ===================================================================================
     * ATTACK 4: reset on the LAST page but before EOF. deliveredRows == total rows of the
     * segment at the time of the reset. On replay every page is skipped and the segment ends
     * cleanly. Verify no duplicate / no loss / no spurious IllegalStateException.
     * =================================================================================== */
    public void testResetAfterLastPageBeforeEofDeliversAllOnce() throws Exception {
        int rows = 1200;
        byte[] content = repeatedLines(rows);
        InMemoryStorageObject obj = new InMemoryStorageObject(content);

        // Compute how many pages segment 1 produces at batchSize 50 so we can fail "after the last page".
        // We don't know the exact segment row count, so use a large failAfterPages; the reader fails only
        // if it actually reaches that many pages, otherwise it reads clean — either way must be correct.
        LastPageResetReader reader = new LastPageResetReader(blockFactory(), 1);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        List<String> read = new ArrayList<>();
        try {
            try (CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 50, 4, exec)) {
                while (iter.hasNext()) {
                    Page p = iter.next();
                    BytesRefBlock b = (BytesRefBlock) p.getBlock(0);
                    BytesRef scratch = new BytesRef();
                    for (int i = 0; i < b.getPositionCount(); i++) {
                        read.add(b.getBytesRef(b.getFirstValueIndex(i), scratch).utf8ToString());
                    }
                    p.releaseBlocks();
                }
            }
            assertTrue("reset-at-EOF must have fired", reader.resetFired.get());
            assertEquals("every row exactly once", rows, read.size());
            List<String> sorted = new ArrayList<>(read);
            Collections.sort(sorted);
            for (int i = 0; i < rows; i++) {
                assertEquals(String.format(ROOT, "line-%05d", i), sorted.get(i));
            }
        } finally {
            exec.shutdown();
            assertTrue(exec.awaitTermination(60, TimeUnit.SECONDS));
        }
    }

    /* ===================================================================================
     * ATTACK 5: reset on segment 0 (the file-leader split). The leader carries firstSplit=true;
     * verify the reset-replay correctly re-opens with firstSplit honored and delivers all rows once.
     * =================================================================================== */
    public void testResetOnSegmentZeroDeliversAllOnce() throws Exception {
        int rows = 1200;
        byte[] content = repeatedLines(rows);
        InMemoryStorageObject obj = new InMemoryStorageObject(content);

        MultiResetReader reader = new MultiResetReader(blockFactory(), 0, 2, 1);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        List<String> read = new ArrayList<>();
        try {
            try (CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 50, 4, exec)) {
                while (iter.hasNext()) {
                    Page p = iter.next();
                    BytesRefBlock b = (BytesRefBlock) p.getBlock(0);
                    BytesRef scratch = new BytesRef();
                    for (int i = 0; i < b.getPositionCount(); i++) {
                        read.add(b.getBytesRef(b.getFirstValueIndex(i), scratch).utf8ToString());
                    }
                    p.releaseBlocks();
                }
            }
            assertTrue(reader.resetCount.get() >= 1);
            assertEquals("every row exactly once after a reset on the leader segment", rows, read.size());
            List<String> sorted = new ArrayList<>(read);
            Collections.sort(sorted);
            for (int i = 0; i < rows; i++) {
                assertEquals(String.format(ROOT, "line-%05d", i), sorted.get(i));
            }
        } finally {
            exec.shutdown();
            assertTrue(exec.awaitTermination(60, TimeUnit.SECONDS));
        }
    }

    /* ===================================================================================
     * ATTACK 6 (now a no-retry guard): a genuine DATA/parse error whose message merely contains the
     * substring "connection reset" must NOT be misclassified as transient. The classifier keys off the
     * exception TYPE first ({@code java.net.SocketException} et al.); a plain {@link RuntimeException}
     * data error is not a transport type, and the narrow message fallback applies only to a bare
     * {@link IOException}. So this data error surfaces immediately and the target segment is read exactly
     * once — no wasted re-opens.
     * =================================================================================== */
    public void testDataErrorMentioningConnectionResetIsNotRetried() throws Exception {
        int rows = 1200;
        byte[] content = repeatedLines(rows);
        InMemoryStorageObject obj = new InMemoryStorageObject(content);

        // A deterministic *data* error (a plain RuntimeException, not a SocketException) whose message
        // contains "connection reset". Type-based classification must treat it as a real error, not transient.
        PhraseErrorReader reader = new PhraseErrorReader(blockFactory(), 1, 3, "row 7: 'connection reset note' is not a valid value");

        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            RuntimeException ex = expectThrows(RuntimeException.class, () -> {
                try (CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 50, 4, exec)) {
                    while (iter.hasNext()) {
                        iter.next().releaseBlocks();
                    }
                }
            });
            assertTrue("the data error must surface", containsMessage(ex, "not a valid value"));
            // The fix: a NON-transport data error is read exactly once for its segment — never retried, even
            // though its message mentions "connection reset".
            assertEquals(
                "data error mentioning 'connection reset' must not be retried as transient",
                1,
                reader.targetSegmentReadCount.get()
            );
        } finally {
            exec.shutdown();
            assertTrue(exec.awaitTermination(60, TimeUnit.SECONDS));
        }
    }

    /* ===================================================================================
     * ATTACK 7: empty / zero-row file. Single segment fast path; must terminate, no hang.
     * =================================================================================== */
    public void testEmptyFileTerminates() throws Exception {
        InMemoryStorageObject obj = new InMemoryStorageObject(new byte[0]);
        LineReader reader = new LineReader(blockFactory());
        ExecutorService exec = Executors.newFixedThreadPool(4);
        int count = 0;
        try {
            try (CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 50, 4, exec)) {
                while (iter.hasNext()) {
                    Page p = iter.next();
                    count += p.getPositionCount();
                    p.releaseBlocks();
                }
            }
            assertEquals(0, count);
        } finally {
            exec.shutdown();
            assertTrue(exec.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    /* ===================================================================================
     * ATTACK 8: a non-terminal segment fails with a NON-transient error AFTER the consumer has
     * already drained pages from a sibling segment via as-ready emission. The error must propagate
     * to the consumer — the partial success of a sibling must not mask the failure as a silent
     * success.
     * =================================================================================== */
    public void testNonTransientFailureAfterSiblingDrainedPropagates() throws Exception {
        int rows = 2000;
        byte[] content = repeatedLines(rows);
        InMemoryStorageObject obj = new InMemoryStorageObject(content);

        // A sibling segment must hand at least one page to the consumer before the target segment throws.
        CountDownLatch siblingDrained = new CountDownLatch(1);
        // Target = segment 1 (non-terminal); it waits for the sibling to be drained, then throws a plain
        // (non-transport) RuntimeException — must NOT be retried and MUST surface to the consumer.
        FailAfterSiblingReader reader = new FailAfterSiblingReader(blockFactory(), 1, siblingDrained);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            RuntimeException ex = expectThrows(RuntimeException.class, () -> {
                try (CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 50, 4, exec)) {
                    while (iter.hasNext()) {
                        Page p = iter.next();
                        // The first page the consumer drains from any non-target segment unblocks the target's throw.
                        siblingDrained.countDown();
                        p.releaseBlocks();
                    }
                }
            });
            assertTrue("the non-transient failure must surface to the consumer", containsMessage(ex, "fatal data error"));
            assertThat("a non-transient error must not be retried", reader.targetSegmentReadCount.get(), org.hamcrest.Matchers.equalTo(1));
        } finally {
            exec.shutdown();
            assertTrue("executor must terminate (no hung worker)", exec.awaitTermination(60, TimeUnit.SECONDS));
        }
    }

    /**
     * Fires a plain (non-transport) {@link RuntimeException} from the target segment, but only once a sibling
     * segment has been drained by the consumer (signalled via {@code siblingDrained}). Models a non-terminal
     * segment failing after as-ready emission has already handed sibling pages downstream.
     */
    private static class FailAfterSiblingReader implements SegmentableFormatReader, NoConfigFormatReader {
        private final BlockFactory blockFactory;
        private final int targetSegmentIndex;
        private final CountDownLatch siblingDrained;
        private volatile StorageObject targetObject = null;
        private final Set<StorageObject> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        private final AtomicInteger firstReadSegmentCounter = new AtomicInteger();
        final AtomicInteger targetSegmentReadCount = new AtomicInteger();

        FailAfterSiblingReader(BlockFactory bf, int targetSegment, CountDownLatch siblingDrained) {
            this.blockFactory = bf;
            this.targetSegmentIndex = targetSegment;
            this.siblingDrained = siblingDrained;
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
            return 1;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public synchronized CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            boolean isTarget;
            if (object == targetObject) {
                isTarget = true;
            } else if (seen.add(object)) {
                int segOrdinal = firstReadSegmentCounter.getAndIncrement();
                isTarget = segOrdinal == targetSegmentIndex;
                if (isTarget) {
                    targetObject = object;
                }
            } else {
                isTarget = false;
            }
            CloseableIterator<Page> delegate = readBatched(blockFactory, object, context.batchSize());
            if (isTarget == false) {
                return delegate;
            }
            targetSegmentReadCount.incrementAndGet();
            return new CloseableIterator<>() {
                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public Page next() {
                    // Block until the consumer has drained a sibling page, then fail non-transiently.
                    try {
                        siblingDrained.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    throw new RuntimeException("fatal data error in segment");
                }

                @Override
                public void close() throws IOException {
                    delegate.close();
                }
            };
        }

        @Override
        public String formatName() {
            return "adv-fail-after-sibling";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".txt");
        }

        @Override
        public void close() {}
    }

    private static boolean containsMessage(Throwable t, String needle) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c.getMessage() != null && c.getMessage().contains(needle)) {
                return true;
            }
            if (c.getCause() == c) {
                break;
            }
        }
        return false;
    }

    private static BlockFactory blockFactory() {
        return TEST_BLOCK_FACTORY;
    }

    // ---- reader fixtures ----

    /** Plain line reader, fixed batchSize pages. */
    private static class LineReader implements SegmentableFormatReader, NoConfigFormatReader {
        final BlockFactory blockFactory;

        LineReader(BlockFactory blockFactory) {
            this.blockFactory = blockFactory;
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
            return 1;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            return readBatched(blockFactory, object, context.batchSize());
        }

        @Override
        public String formatName() {
            return "adv-line";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".txt");
        }

        @Override
        public void close() {}
    }

    static CloseableIterator<Page> readBatched(BlockFactory blockFactory, StorageObject object, int batchSize) throws IOException {
        InputStream stream = object.newStream();
        BufferedReader br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        return new CloseableIterator<>() {
            private final List<String> buffer = new ArrayList<>();
            private boolean done = false;
            private Page nextPage = null;

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
                    throw new NoSuchElementException();
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
            public void close() throws IOException {
                br.close();
                stream.close();
            }
        };
    }

    /**
     * Reads the target segment with page size {@code firstPageSize} on the first read, and {@code replayPageSize}
     * on the re-read. Fires a one-shot reset after {@code failAfterPages} pages on the first read.
     */
    private static class VaryingPageSizeResetReader implements SegmentableFormatReader, NoConfigFormatReader {
        private final BlockFactory blockFactory;
        private final int targetSegmentIndex;
        private final int failAfterPages;
        private final int firstPageSize;
        private final int replayPageSize;
        private volatile StorageObject targetObject = null;
        private final Set<StorageObject> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        private final AtomicInteger firstReadSegmentCounter = new AtomicInteger();
        final AtomicBoolean resetFired = new AtomicBoolean(false);

        VaryingPageSizeResetReader(BlockFactory bf, int targetSegmentIndex, int failAfterPages, int firstPageSize, int replayPageSize) {
            this.blockFactory = bf;
            this.targetSegmentIndex = targetSegmentIndex;
            this.failAfterPages = failAfterPages;
            this.firstPageSize = firstPageSize;
            this.replayPageSize = replayPageSize;
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
            return 1;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public synchronized CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            boolean isTarget;
            if (object == targetObject) {
                isTarget = true;
            } else if (seen.add(object)) {
                int segOrdinal = firstReadSegmentCounter.getAndIncrement();
                isTarget = segOrdinal == targetSegmentIndex;
                if (isTarget) {
                    targetObject = object;
                }
            } else {
                isTarget = false;
            }
            boolean alreadyReset = resetFired.get();
            // First read of target: page size firstPageSize, inject reset. Re-read: page size replayPageSize.
            int pageSize = (isTarget && alreadyReset == false) ? firstPageSize : (isTarget ? replayPageSize : context.batchSize());
            CloseableIterator<Page> delegate = readBatched(blockFactory, object, pageSize);
            if (isTarget == false || alreadyReset) {
                return delegate;
            }
            return new CloseableIterator<>() {
                private int pages = 0;

                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public Page next() {
                    if (pages >= failAfterPages && resetFired.compareAndSet(false, true)) {
                        throw new RuntimeException("read failed", new SocketException("Connection reset"));
                    }
                    Page p = delegate.next();
                    pages++;
                    return p;
                }

                @Override
                public void close() throws IOException {
                    delegate.close();
                }
            };
        }

        @Override
        public String formatName() {
            return "adv-varying";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".txt");
        }

        @Override
        public void close() {}
    }

    /**
     * Fires a transport reset on up to {@code maxResets} reads of the target segment, after {@code failAfterPages}
     * pages each. The target segment is identified by its byte-range length (stable across re-reads). Only the
     * target segment ever faults, so every re-read of it is a reset-resume re-open.
     */
    private static class MultiResetReader implements SegmentableFormatReader, NoConfigFormatReader {
        private final BlockFactory blockFactory;
        private final int targetSegmentIndex;
        private final int failAfterPages;
        private final int maxResets;
        // Segments are dispatched in index order; the first read of each segment index arrives in order.
        // We map a read to its segment index by counting first-reads (one distinct StorageObject per segment).
        // The coordinator reuses the SAME segObj instance across reset-resume re-reads, so a re-read of the
        // target is detected by object identity — robust even when two segments share a byte length.
        private volatile StorageObject targetObject = null;
        private final Set<StorageObject> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        private final AtomicInteger firstReadSegmentCounter = new AtomicInteger();
        final AtomicInteger resetCount = new AtomicInteger();

        MultiResetReader(BlockFactory bf, int targetSegment, int failAfterPages, int maxResets) {
            this.blockFactory = bf;
            this.targetSegmentIndex = targetSegment;
            this.failAfterPages = failAfterPages;
            this.maxResets = maxResets;
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
            return 1;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public synchronized CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            boolean isTarget;
            if (object == targetObject) {
                isTarget = true; // a reset-resume re-read of the (already identified) target segment
            } else if (seen.add(object)) {
                int segOrdinal = firstReadSegmentCounter.getAndIncrement();
                isTarget = segOrdinal == targetSegmentIndex;
                if (isTarget) {
                    targetObject = object;
                }
            } else {
                isTarget = false;
            }
            CloseableIterator<Page> delegate = readBatched(blockFactory, object, context.batchSize());
            if (isTarget && resetCount.get() < maxResets) {
                return wrap(delegate);
            }
            return delegate;
        }

        private CloseableIterator<Page> wrap(CloseableIterator<Page> delegate) {
            return new CloseableIterator<>() {
                private int pages = 0;
                private boolean fired = false;

                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public Page next() {
                    if (fired == false && pages >= failAfterPages) {
                        fired = true;
                        resetCount.incrementAndGet();
                        throw new RuntimeException("read failed", new SocketException("Connection reset"));
                    }
                    Page p = delegate.next();
                    pages++;
                    return p;
                }

                @Override
                public void close() throws IOException {
                    delegate.close();
                }
            };
        }

        @Override
        public String formatName() {
            return "adv-multireset";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".txt");
        }

        @Override
        public void close() {}
    }

    /**
     * Fires a deterministic *data* error (a plain {@link RuntimeException}, not a SocketException) whose message
     * contains the supplied phrase, after {@code failAfterPages} pages, on every read of the target segment.
     * Identifies the target segment by byte-range length (stable across re-reads).
     */
    private static class PhraseErrorReader implements SegmentableFormatReader, NoConfigFormatReader {
        private final BlockFactory blockFactory;
        private final int targetSegmentIndex;
        private final int failAfterPages;
        private final String phrase;
        private volatile StorageObject targetObject = null;
        private final Set<StorageObject> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        private final AtomicInteger firstReadSegmentCounter = new AtomicInteger();
        final AtomicInteger targetSegmentReadCount = new AtomicInteger();

        PhraseErrorReader(BlockFactory bf, int targetSegment, int failAfterPages, String phrase) {
            this.blockFactory = bf;
            this.targetSegmentIndex = targetSegment;
            this.failAfterPages = failAfterPages;
            this.phrase = phrase;
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
            return 1;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public synchronized CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            boolean isTarget;
            if (object == targetObject) {
                isTarget = true;
            } else if (seen.add(object)) {
                int segOrdinal = firstReadSegmentCounter.getAndIncrement();
                isTarget = segOrdinal == targetSegmentIndex;
                if (isTarget) {
                    targetObject = object;
                }
            } else {
                isTarget = false;
            }
            CloseableIterator<Page> delegate = readBatched(blockFactory, object, context.batchSize());
            if (isTarget == false) {
                return delegate;
            }
            targetSegmentReadCount.incrementAndGet();
            return new CloseableIterator<>() {
                private int pages = 0;

                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public Page next() {
                    if (pages >= failAfterPages) {
                        // A pure data/validation error (no SocketException), but the message mentions the phrase.
                        throw new RuntimeException(phrase);
                    }
                    Page p = delegate.next();
                    pages++;
                    return p;
                }

                @Override
                public void close() throws IOException {
                    delegate.close();
                }
            };
        }

        @Override
        public String formatName() {
            return "adv-phrase";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".txt");
        }

        @Override
        public void close() {}
    }

    /** Fires a one-shot reset only when the target segment reaches EOF (after its final page). */
    private static class LastPageResetReader implements SegmentableFormatReader, NoConfigFormatReader {
        private final BlockFactory blockFactory;
        private final int targetReadIndex;
        private final AtomicInteger readCallCount = new AtomicInteger();
        final AtomicBoolean resetFired = new AtomicBoolean(false);

        LastPageResetReader(BlockFactory bf, int targetReadIndex) {
            this.blockFactory = bf;
            this.targetReadIndex = targetReadIndex;
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
            return 1;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            int idx = readCallCount.getAndIncrement();
            CloseableIterator<Page> delegate = readBatched(blockFactory, object, context.batchSize());
            if (idx != targetReadIndex || resetFired.get()) {
                return delegate;
            }
            return new CloseableIterator<>() {
                @Override
                public boolean hasNext() {
                    boolean more = delegate.hasNext();
                    if (more == false && resetFired.compareAndSet(false, true)) {
                        // EOF reached on the original read: all pages delivered, but the stream is reset
                        // before the iterator signals clean completion.
                        throw new RuntimeException("read failed", new SocketException("Connection reset"));
                    }
                    return more;
                }

                @Override
                public Page next() {
                    return delegate.next();
                }

                @Override
                public void close() throws IOException {
                    delegate.close();
                }
            };
        }

        @Override
        public String formatName() {
            return "adv-lastpage";
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
            return StoragePath.of("mem://adv");
        }
    }
}
