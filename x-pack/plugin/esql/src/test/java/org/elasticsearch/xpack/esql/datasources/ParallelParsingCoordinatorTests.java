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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
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
            try (CloseableIterator<Page> iter = ParallelParsingCoordinator.parallelRead(reader, obj, List.of("line"), 50, 4, exec)) {
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
        CsvFormatReader withSchema = (CsvFormatReader) base.withSchema(meta.schema());

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
        CsvFormatReader withSchema = (CsvFormatReader) base.withSchema(meta.schema());

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
        CsvFormatReader withSchema = (CsvFormatReader) base.withSchema(meta.schema());

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
        CsvFormatReader withSchema = (CsvFormatReader) base.withSchema(meta.schema());

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

    /**
     * SegmentableFormatReader that records the {@link FormatReadContext} it was handed for each
     * {@link #read} call. Lets tests assert per-segment flag wiring without re-implementing line
     * parsing.
     */
    private static class ContextRecordingFormatReader implements SegmentableFormatReader {
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
