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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    public void testParallelReadPreservesOrder() throws Exception {
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

        ExecutorService exec = Executors.newFixedThreadPool(4);
        try {
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 50, 4, exec, SCHEMA);

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
            for (int i = 0; i < lineCount; i++) {
                assertEquals("Lines must be in order", "line-" + String.format(java.util.Locale.ROOT, "%04d", i), allLines.get(i));
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
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 100, 1, exec, SCHEMA);

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

    public void testParallelReadEmptyFile() throws Exception {
        StorageObject obj = new InMemoryStorageObject(new byte[0]);
        BlockFactory blockFactory = blockFactory();
        LineFormatReader reader = new LineFormatReader(blockFactory);

        ExecutorService exec = Executors.newFixedThreadPool(2);
        try {
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 100, 4, exec, SCHEMA);

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
            long skipped = reader.findNextRecordBoundary(stream);
            assertEquals(6, skipped);
        }
    }

    public void testFindNextRecordBoundaryCRLF() throws IOException {
        NewlineSegmentableReader reader = new NewlineSegmentableReader(1);

        byte[] data = "abcde\r\nfghij\n".getBytes(StandardCharsets.UTF_8);
        try (InputStream stream = new ByteArrayInputStream(data)) {
            long skipped = reader.findNextRecordBoundary(stream);
            assertEquals(7, skipped);
        }
    }

    public void testFindNextRecordBoundaryEOF() throws IOException {
        NewlineSegmentableReader reader = new NewlineSegmentableReader(1);

        byte[] data = "no-newline-here".getBytes(StandardCharsets.UTF_8);
        try (InputStream stream = new ByteArrayInputStream(data)) {
            long skipped = reader.findNextRecordBoundary(stream);
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
            try (
                CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 50, 4, exec, SCHEMA)
            ) {
                while (iter.hasNext()) {
                    Page page = iter.next();
                    page.releaseBlocks();
                }
            }
        });
        assertNotNull("Should propagate rejection error", ex);
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
            CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 10, 4, exec, SCHEMA);

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

    private static final BlockFactory TEST_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
        .build();

    private static BlockFactory blockFactory() {
        return TEST_BLOCK_FACTORY;
    }

    /**
     * Minimal SegmentableFormatReader that scans for newlines.
     */
    private static class NewlineSegmentableReader implements SegmentableFormatReader {
        private final long minSegmentSize;

        NewlineSegmentableReader(long minSegmentSize) {
            this.minSegmentSize = minSegmentSize;
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
                if (b == '\r') {
                    int next = stream.read();
                    consumed++;
                    if (next == '\n') {
                        return consumed;
                    }
                    if (next == -1) {
                        return consumed - 1;
                    }
                    return consumed - 1;
                }
            }
            return -1;
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
        public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) {
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
    private static class LineFormatReader implements SegmentableFormatReader {
        private final BlockFactory blockFactory;

        LineFormatReader(BlockFactory blockFactory) {
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
        public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
            return readSplit(object, projectedColumns, batchSize, false, true, SCHEMA);
        }

        @Override
        public CloseableIterator<Page> readSplit(
            StorageObject object,
            List<String> projectedColumns,
            int batchSize,
            boolean skipFirstLine,
            boolean lastSplit,
            List<Attribute> resolvedAttributes
        ) throws IOException {
            InputStream stream = object.newStream();
            java.io.BufferedReader br = new java.io.BufferedReader(new java.io.InputStreamReader(stream, StandardCharsets.UTF_8));

            if (skipFirstLine) {
                br.readLine();
            }

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
                public void close() throws IOException {
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
    private static class FailingFormatReader implements SegmentableFormatReader {
        private final BlockFactory blockFactory;
        private final int failAfterLines;

        FailingFormatReader(BlockFactory blockFactory, int failAfterLines) {
            this.blockFactory = blockFactory;
            this.failAfterLines = failAfterLines;
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
        public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
            return readSplit(object, projectedColumns, batchSize, false, true, SCHEMA);
        }

        @Override
        public CloseableIterator<Page> readSplit(
            StorageObject object,
            List<String> projectedColumns,
            int batchSize,
            boolean skipFirstLine,
            boolean lastSplit,
            List<Attribute> resolvedAttributes
        ) throws IOException {
            InputStream stream = object.newStream();
            java.io.BufferedReader br = new java.io.BufferedReader(new java.io.InputStreamReader(stream, StandardCharsets.UTF_8));
            if (skipFirstLine) {
                br.readLine();
            }

            return new CloseableIterator<>() {
                private int linesRead = 0;
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
                public void close() throws IOException {
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
}
