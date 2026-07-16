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
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;

/**
 * Unit tests for {@link ParquetColumnExtractor}: random-access reads of single columns by
 * file-local row position, validation, and correct lifecycle. The fixture writes small in-memory
 * parquet files with multiple row groups so the extractor's sort + permute logic is exercised on
 * positions that span row groups.
 */
public class ParquetColumnExtractorTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() throws Exception {
        ParquetStorageObjectAdapter.clearFooterCacheForTests();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    /**
     * A schema-vs-planner incompatibility on the deferred path emits a response Warning header (mirroring the eager
     * scan). Drain any accumulated warnings so the parent {@code ensureNoWarnings} post-check passes; a test that asserts
     * on them calls {@link #drainWarnings()} from inside the method first.
     */
    @After
    public void clearWarningHeaders() {
        if (threadContext != null) {
            threadContext.stashContext();
        }
    }

    private List<String> drainWarnings() {
        List<String> raw = threadContext.getResponseHeaders().getOrDefault("Warning", List.of());
        List<String> messages = raw.stream().map(s -> HeaderWarning.extractWarningValueFromWarningHeader(s, false)).toList();
        threadContext.stashContext();
        return messages;
    }

    /**
     * Test helper: builds a {@link ParquetColumnExtractor} scoped to the file's full footer by
     * directly loading the parquet footer and using the package-private extractor constructor.
     * The production wiring goes through {@code OptimizedParquetColumnIterator}'s
     * {@code createColumnExtractor()} producer handshake; tests deliberately bypass that to
     * focus on the extractor's own logic.
     */
    private ParquetColumnExtractor newFullFileExtractor(StorageObject so) throws IOException {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        return new ParquetColumnExtractor(so, reader, loadFooter(so), ErrorPolicy.PERMISSIVE);
    }

    private ParquetMetadata loadFooter(StorageObject so) throws IOException {
        try (
            ParquetFileReader fr = ParquetFileReader.open(
                new ParquetStorageObjectAdapter(so, blockFactory.arrowAllocator()),
                PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build()
            )
        ) {
            return fr.getFooter();
        }
    }

    public void testRowCount() throws IOException {
        byte[] data = writeIntFile(100);
        StorageObject so = createStorageObject(data);
        try (ColumnExtractor extractor = newFullFileExtractor(so)) {
            assertEquals(100, extractor.rowCount());
        }
    }

    public void testExtractInOrder() throws IOException {
        byte[] data = writeIntFile(20);
        StorageObject so = createStorageObject(data);
        try (ColumnExtractor extractor = newFullFileExtractor(so)) {
            long[] positions = { 0, 1, 2, 5, 10, 19 };
            Block block = extractor.extract("v", positions, blockFactory);
            try {
                IntBlock ints = (IntBlock) block;
                assertEquals(positions.length, ints.getPositionCount());
                for (int i = 0; i < positions.length; i++) {
                    assertEquals((int) positions[i], ints.getInt(i));
                }
            } finally {
                block.close();
            }
        }
    }

    public void testExtractOutOfOrderWithDuplicates() throws IOException {
        byte[] data = writeIntFile(50);
        StorageObject so = createStorageObject(data);
        try (ColumnExtractor extractor = newFullFileExtractor(so)) {
            // Out-of-order: 17, 3, 42, 0, 17 (repeat), 49, 25
            long[] positions = { 17, 3, 42, 0, 17, 49, 25 };
            Block block = extractor.extract("v", positions, blockFactory);
            try {
                IntBlock ints = (IntBlock) block;
                assertEquals(positions.length, ints.getPositionCount());
                for (int i = 0; i < positions.length; i++) {
                    assertEquals((int) positions[i], ints.getInt(i));
                }
            } finally {
                block.close();
            }
        }
    }

    public void testExtractAcrossRowGroups() throws IOException {
        // Force two row groups by setting a small page-size budget on the writer. Each row group
        // gets ~250 rows, so positions spanning [0, 249] vs [250, 499] live in different groups
        // and force the baseline scan to advance multiple parquet pages.
        byte[] data = writeMultiRowGroupFile(500);
        StorageObject so = createStorageObject(data);
        try (ColumnExtractor extractor = newFullFileExtractor(so)) {
            assertEquals(500, extractor.rowCount());
            long[] positions = { 250, 0, 499, 125, 375, 1, 498 };
            Block block = extractor.extract("v", positions, blockFactory);
            try {
                IntBlock ints = (IntBlock) block;
                assertEquals(positions.length, ints.getPositionCount());
                for (int i = 0; i < positions.length; i++) {
                    assertEquals((int) positions[i], ints.getInt(i));
                }
            } finally {
                block.close();
            }
        }
    }

    public void testExtractFlatAcrossRowGroupsWithDuplicates() throws IOException {
        byte[] data = writeMultiRowGroupFile(500);
        StorageObject so = createStorageObject(data);
        ParquetMetadata fullFooter = loadFooter(so);
        assertTrue("expected multiple row groups", fullFooter.getBlocks().size() >= 2);

        long secondRowGroupFirstRow = fullFooter.getBlocks().get(0).getRowCount();
        long[] positions = { 0, secondRowGroupFirstRow, secondRowGroupFirstRow, 0 };
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        try (
            ColumnExtractor extractor = new ParquetColumnExtractor(so, reader, fullFooter, ErrorPolicy.PERMISSIVE);
            Block block = extractor.extract("v", positions, blockFactory)
        ) {
            IntBlock ints = (IntBlock) block;
            assertEquals(positions.length, ints.getPositionCount());
            for (int i = 0; i < positions.length; i++) {
                assertEquals((int) positions[i], ints.getInt(i));
            }
        }
    }

    public void testExtractNullableFlatAcrossRowGroupsWithDuplicateNulls() throws IOException {
        byte[] data = writeNullableIntMultiRowGroupFile(500);
        StorageObject so = createStorageObject(data);
        ParquetMetadata fullFooter = loadFooter(so);
        assertTrue("expected multiple row groups", fullFooter.getBlocks().size() >= 2);

        long firstNullInSecondRowGroup = nextRowDivisibleByFive(fullFooter.getBlocks().get(0).getRowCount());
        long[] positions = { 0, firstNullInSecondRowGroup, firstNullInSecondRowGroup, 0 };
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        try (
            ColumnExtractor extractor = new ParquetColumnExtractor(so, reader, fullFooter, ErrorPolicy.PERMISSIVE);
            Block block = extractor.extract("v", positions, blockFactory)
        ) {
            IntBlock ints = (IntBlock) block;
            assertEquals(positions.length, ints.getPositionCount());
            for (int i = 0; i < positions.length; i++) {
                assertTrue("slot " + i + " should be null", ints.isNull(i));
            }
        }
    }

    public void testExtractMixedTypes() throws IOException {
        byte[] data = writeMixedTypeFile(30);
        StorageObject so = createStorageObject(data);
        try (ColumnExtractor extractor = newFullFileExtractor(so)) {
            long[] positions = { 0, 7, 15, 29 };

            try (Block intBlock = extractor.extract("v_int", positions, blockFactory)) {
                IntBlock ints = (IntBlock) intBlock;
                for (int i = 0; i < positions.length; i++) {
                    assertEquals((int) positions[i], ints.getInt(i));
                }
            }

            try (Block longBlock = extractor.extract("v_long", positions, blockFactory)) {
                LongBlock longs = (LongBlock) longBlock;
                for (int i = 0; i < positions.length; i++) {
                    assertEquals(positions[i] * 1_000L, longs.getLong(i));
                }
            }

            try (Block strBlock = extractor.extract("v_str", positions, blockFactory)) {
                BytesRefBlock strs = (BytesRefBlock) strBlock;
                for (int i = 0; i < positions.length; i++) {
                    assertEquals("row-" + positions[i], strs.getBytesRef(i, new org.apache.lucene.util.BytesRef()).utf8ToString());
                }
            }
        }
    }

    public void testDeferredInferredIntegerOverInt64NullFillsWholeColumn() throws IOException {
        // The deferred-extraction twin of ParquetFormatReaderTests.testInt64InferredIntegerNullFillsWholeColumn. After a
        // TopN, an INFERRED INTEGER target over an int64 column must null-fill via coerceToTarget — never downcast —
        // otherwise the column reads differently depending on whether extraction was deferred. supports(LONG, INTEGER) is
        // true, so a plain (non-declared) reader here pins the deferred branch of the gate split: dropping the
        // isDeclaredTypeColumn guard in coerceToTarget coerces here and this fails.
        byte[] data = writeSingleInt64File(new long[] { 5L, 7L, 9L });
        StorageObject so = createStorageObject(data);
        try (ColumnExtractor extractor = newFullFileExtractor(so)) { // plain reader => "v" is inferred
            long[] positions = { 0, 1, 2 };
            Block[] blocks = extractor.extract(new String[] { "v" }, new DataType[] { DataType.INTEGER }, positions, blockFactory);
            try (Block block = blocks[0]) {
                assertEquals(3, block.getPositionCount());
                for (int i = 0; i < positions.length; i++) {
                    assertTrue("inferred int64->integer must null-fill on the deferred path, never downcast", block.isNull(i));
                }
            }
        }
        List<String> warnings = drainWarnings();
        assertFalse("deferred inferred incompatibility must emit a response Warning", warnings.isEmpty());
        assertTrue(
            "warning must name the incompatibility, got: " + warnings,
            warnings.toString().contains("incompatible with planner type")
        );
    }

    public void testDeferredDeclaredIntegerOverInt64Coerces() throws IOException {
        // The declared contrast to the above on the SAME deferred path: when "v"'s INTEGER target is declared, the escape
        // is licensed and coerceToTarget narrows per value (values in range => Integer results, not null) — proving the
        // deferred gate distinguishes declared from inferred, exactly like the eager pair.
        byte[] data = writeSingleInt64File(new long[] { 5L, 7L, 9L });
        StorageObject so = createStorageObject(data);
        ParquetFormatReader reader = (ParquetFormatReader) new ParquetFormatReader(blockFactory).withDeclaredTypeColumns(Set.of("v"));
        try (ColumnExtractor extractor = new ParquetColumnExtractor(so, reader, loadFooter(so), ErrorPolicy.PERMISSIVE)) {
            long[] positions = { 0, 1, 2 };
            Block[] blocks = extractor.extract(new String[] { "v" }, new DataType[] { DataType.INTEGER }, positions, blockFactory);
            try (Block block = blocks[0]) {
                IntBlock ints = (IntBlock) block;
                assertEquals(3, ints.getPositionCount());
                assertEquals(5, ints.getInt(0));
                assertEquals(7, ints.getInt(1));
                assertEquals(9, ints.getInt(2));
            }
        }
    }

    public void testDeferredDeclaredCoercionWarningsRouteToSuppliedSink() throws IOException {
        // A declared long over unparseable string tokens null-fills each bad value on the deferred path and
        // records a per-value coercion Warning. When a driver-thread sink is supplied, every such warning must
        // reach that sink (the budget-gated relay) and not this thread's HeaderWarning context.
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("v")
            .named("strings");
        byte[] data = writeFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                Group g = factory.newGroup();
                g.add("v", "notanumber" + i);
                groups.add(g);
            }
            return groups;
        }, /* rowGroupBytes = */ 1024L);
        StorageObject so = createStorageObject(data);
        ParquetFormatReader reader = (ParquetFormatReader) new ParquetFormatReader(blockFactory).withDeclaredTypeColumns(Set.of("v"));
        List<String> sink = new ArrayList<>();
        try (ColumnExtractor extractor = new ParquetColumnExtractor(so, reader, loadFooter(so), ErrorPolicy.PERMISSIVE, sink::add)) {
            long[] positions = { 0, 1, 2 };
            Block[] blocks = extractor.extract(new String[] { "v" }, new DataType[] { DataType.LONG }, positions, blockFactory);
            try (Block block = blocks[0]) {
                assertEquals(3, block.getPositionCount());
                for (int i = 0; i < positions.length; i++) {
                    assertTrue("an unparseable long token null-fills its cell", block.isNull(i));
                }
            }
        }
        assertTrue(
            "per-value coercion warnings must reach the supplied sink, got: " + sink,
            sink.stream().anyMatch(w -> w.contains("cannot coerce value"))
        );
        List<String> leaked = drainWarnings();
        assertTrue(
            "no coercion warning may leak to this thread's HeaderWarning context when a sink is supplied, got: " + leaked,
            leaked.stream().noneMatch(w -> w.contains("cannot coerce value"))
        );
    }

    private byte[] writeSingleInt64File(long[] values) throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("v").named("longs");
        return writeFile(schema, factory -> {
            List<Group> groups = new ArrayList<>(values.length);
            for (long v : values) {
                Group g = factory.newGroup();
                g.add("v", v);
                groups.add(g);
            }
            return groups;
        }, /* rowGroupBytes = */ 1024L);
    }

    public void testExtractEmptyPositionsReturnsEmptyBlock() throws IOException {
        byte[] data = writeIntFile(10);
        StorageObject so = createStorageObject(data);
        try (ColumnExtractor extractor = newFullFileExtractor(so)) {
            Block block = extractor.extract("v", new long[0], blockFactory);
            try {
                assertEquals(0, block.getPositionCount());
            } finally {
                block.close();
            }
        }
    }

    public void testExtractRejectsRowPositionColumn() throws IOException {
        byte[] data = writeIntFile(5);
        StorageObject so = createStorageObject(data);
        try (ColumnExtractor extractor = newFullFileExtractor(so)) {
            expectThrows(
                IllegalArgumentException.class,
                () -> extractor.extract(ColumnExtractor.ROW_POSITION_COLUMN, new long[] { 0 }, blockFactory)
            );
        }
    }

    public void testExtractRejectsOutOfRangePositions() throws IOException {
        byte[] data = writeIntFile(5);
        StorageObject so = createStorageObject(data);
        try (ColumnExtractor extractor = newFullFileExtractor(so)) {
            expectThrows(IllegalArgumentException.class, () -> extractor.extract("v", new long[] { -1 }, blockFactory));
            expectThrows(IllegalArgumentException.class, () -> extractor.extract("v", new long[] { 5 }, blockFactory));
        }
    }

    /**
     * The packed-sort scheme inside {@code Bucket.sortAndDedup} packs each input slot index into
     * the low {@code INDEX_BITS} of a long; if the input batch ever exceeds that capacity, slot
     * collisions silently corrupt the sort. The extractor enforces the local invariant on entry
     * to {@link ColumnExtractor#extract} so the failure mode is a clear exception rather than
     * miscompacted output.
     */
    public void testExtractRejectsBatchAboveSortCapacity() throws IOException {
        byte[] data = writeIntFile(5);
        StorageObject so = createStorageObject(data);
        try (ColumnExtractor extractor = newFullFileExtractor(so)) {
            // 1 << 14 == 16_384, the per-batch capacity of the packed sort. Anything above must be
            // rejected without ever reaching {@code sortAndDedup}.
            long[] tooMany = new long[(1 << 14) + 1];
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> extractor.extract("v", tooMany, blockFactory));
            assertThat(e.getMessage(), containsString("packed-sort capacity"));
        }
    }

    /**
     * List columns (Parquet {@code maxRepetitionLevel > 0}, ESQL multi-value blocks) take the
     * repetition-aware sparse path. The extractor must (a) skip all rows preceding each survivor
     * via {@link ParquetColumnDecoding#skipListValues}, (b) materialize the surviving rows via
     * {@link ParquetColumnDecoding#readListColumn}, and (c) preserve list values, including
     * empty and null lists, exactly as written.
     */
    public void testExtractListColumn() throws IOException {
        byte[] data = writeIntListFile(60);
        StorageObject so = createStorageObject(data);
        try (ColumnExtractor extractor = newFullFileExtractor(so)) {
            assertEquals(60, extractor.rowCount());
            // Pick out-of-order, non-adjacent positions covering: a row with an empty list, a
            // row with a multi-element list, and a row with a singleton list. The skip/read
            // loop has to advance past several null / empty rows in between to reach the
            // requested ones; getting any of them wrong (off-by-one in the rep-level walk)
            // would shift the values.
            // First sanity-check: read a single non-trivial row in isolation. Row 17 has listLen = 2.
            try (Block block = extractor.extract("vals", new long[] { 17 }, blockFactory)) {
                IntBlock ints = (IntBlock) block;
                assertEquals(1, ints.getPositionCount());
                assertEquals("row 17 valueCount", 2, ints.getValueCount(0));
                int fv = ints.getFirstValueIndex(0);
                assertEquals("row 17 element 0", 170, ints.getInt(fv));
                assertEquals("row 17 element 1", 171, ints.getInt(fv + 1));
            }
            long[] positions = { 30, 5, 17 };
            try (Block block = extractor.extract("vals", positions, blockFactory)) {
                IntBlock ints = (IntBlock) block;
                assertEquals(positions.length, ints.getPositionCount());
                for (int i = 0; i < positions.length; i++) {
                    int row = (int) positions[i];
                    int listLen = row % 5;
                    assertEquals("position count mismatch at output slot " + i, listLen, ints.getValueCount(i));
                    int firstValue = ints.getFirstValueIndex(i);
                    for (int k = 0; k < listLen; k++) {
                        assertEquals("value at row " + row + " element " + k, row * 10 + k, ints.getInt(firstValue + k));
                    }
                }
            }
        }
    }

    public void testExtractListAcrossRowGroupsWithDuplicates() throws IOException {
        byte[] data = writeMultiRowGroupIntListFile(500);
        StorageObject so = createStorageObject(data);
        ParquetMetadata fullFooter = loadFooter(so);
        assertTrue("expected multiple row groups", fullFooter.getBlocks().size() >= 2);

        long rowInSecondGroup = fullFooter.getBlocks().get(0).getRowCount() + 1;
        long emptyRowInSecondGroup = nextRowDivisibleByFive(fullFooter.getBlocks().get(0).getRowCount());
        long[] positions = { 1, rowInSecondGroup, rowInSecondGroup, 1, 0, emptyRowInSecondGroup, emptyRowInSecondGroup, 0 };
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        try (
            ColumnExtractor extractor = new ParquetColumnExtractor(so, reader, fullFooter, ErrorPolicy.PERMISSIVE);
            Block block = extractor.extract("vals", positions, blockFactory)
        ) {
            IntBlock ints = (IntBlock) block;
            assertEquals(positions.length, ints.getPositionCount());
            for (int i = 0; i < positions.length; i++) {
                assertIntListRow(ints, i, (int) positions[i]);
            }
        }
    }

    /**
     * List-column variant of the null-leading concat coverage. Non-adjacent survivor positions
     * whose leading run(s) are empty lists ({@code i % 5 == 0}) force {@code decodeList} to emit
     * multiple run chunks with a null/empty-leading chunk first, then route them through
     * {@link BlockChunks#concat}. Unlike the flat and gather paths the list decoder never emits a
     * {@code ConstantNullBlock} (its runs are always typed multi-value blocks), so this guards the
     * list concat path's correctness rather than the exact null-first-chunk crash shape.
     */
    public void testExtractListColumnNullLeadingRun() throws IOException {
        byte[] data = writeIntListFile(60);
        StorageObject so = createStorageObject(data);
        try (ColumnExtractor extractor = newFullFileExtractor(so)) {
            // Rows 0 and 5 are empty lists (listLen == 0); row 17 has listLen 2. Three
            // non-adjacent positions => three separate runs => concat with two empty-leading chunks.
            long[] positions = { 0, 5, 17 };
            try (Block block = extractor.extract("vals", positions, blockFactory)) {
                IntBlock ints = (IntBlock) block;
                assertEquals(3, ints.getPositionCount());
                assertEquals("row 0 empty list", 0, ints.getValueCount(0));
                assertEquals("row 5 empty list", 0, ints.getValueCount(1));
                assertEquals("row 17 valueCount", 2, ints.getValueCount(2));
                int fv = ints.getFirstValueIndex(2);
                assertEquals("row 17 element 0", 170, ints.getInt(fv));
                assertEquals("row 17 element 1", 171, ints.getInt(fv + 1));
            }
        }
    }

    /**
     * Performance regression guard for the targeted-extraction path. The extractor must only
     * fetch bytes from row groups that actually own a surviving position; visiting all row
     * groups was the catastrophic forward-scan regression observed on S3 (see PR description).
     * A tracking {@link StorageObject} records the byte ranges read on every {@code newStream}
     * call, and we assert the read window is contained in the spans of the targeted row groups
     * plus any small footer/metadata reads, rather than spanning the whole file.
     */
    public void testExtractTouchesOnlyTargetedRowGroups() throws IOException {
        // Use a larger row count so the small-budget writer produces ≥3 row groups; with 500
        // rows the writer typically emits 2 groups, which doesn't exercise the "third group is
        // never touched" assertion.
        byte[] data = writeMultiRowGroupFile(2000);
        TrackingStorageObject so = new TrackingStorageObject(data);
        ParquetMetadata fullFooter = loadFooter(so);
        // Sanity check: the writer's small budget produces multiple row groups so there is a
        // real opportunity to skip groups.
        assertTrue("expected multiple row groups, got " + fullFooter.getBlocks().size(), fullFooter.getBlocks().size() >= 3);

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        try (ColumnExtractor extractor = new ParquetColumnExtractor(so, reader, fullFooter, ErrorPolicy.PERMISSIVE)) {
            // Pick a single survivor — the whole point of the targeted path is "1 survivor → 1
            // row group visited". The chosen position is in the second row group; we then check
            // no read range overlaps the first or third group's chunk space.
            BlockMetaData firstGroup = fullFooter.getBlocks().get(0);
            BlockMetaData secondGroup = fullFooter.getBlocks().get(1);
            BlockMetaData thirdGroup = fullFooter.getBlocks().get(2);
            long survivorLocal = firstGroup.getRowCount() + 1;
            so.reads.clear();
            try (Block block = extractor.extract("v", new long[] { survivorLocal }, blockFactory)) {
                IntBlock ints = (IntBlock) block;
                assertEquals(1, ints.getPositionCount());
                assertEquals((int) survivorLocal, ints.getInt(0));
            }
            long firstStart = firstGroup.getStartingPos();
            long firstEnd = firstStart + firstGroup.getCompressedSize();
            long thirdStart = thirdGroup.getStartingPos();
            long thirdEnd = thirdStart + thirdGroup.getCompressedSize();
            // Allow footer / dictionary header reads (they sit outside any row group's
            // compressed-chunk window). Any read that lands inside the first or third group's
            // chunk window is a regression — we should never touch those groups.
            for (long[] r : so.reads) {
                long readStart = r[0];
                long readEnd = readStart + r[1];
                assertFalse(
                    "extractor read overlapped first row group: read=["
                        + readStart
                        + ","
                        + readEnd
                        + "), group=["
                        + firstStart
                        + ","
                        + firstEnd
                        + ")",
                    overlaps(readStart, readEnd, firstStart, firstEnd)
                );
                assertFalse(
                    "extractor read overlapped third row group: read=["
                        + readStart
                        + ","
                        + readEnd
                        + "), group=["
                        + thirdStart
                        + ","
                        + thirdEnd
                        + ")",
                    overlaps(readStart, readEnd, thirdStart, thirdEnd)
                );
            }
            // Sanity: at least one read must have hit the second row group's chunk window.
            long secondStart = secondGroup.getStartingPos();
            long secondEnd = secondStart + secondGroup.getCompressedSize();
            boolean touchedSecond = false;
            for (long[] r : so.reads) {
                if (overlaps(r[0], r[0] + r[1], secondStart, secondEnd)) {
                    touchedSecond = true;
                    break;
                }
            }
            assertTrue("expected at least one read inside second row group window", touchedSecond);
        }
    }

    private static boolean overlaps(long aStart, long aEnd, long bStart, long bEnd) {
        return aStart < bEnd && bStart < aEnd;
    }

    /**
     * F-2 invariant: a multi-column {@code extract} call must coalesce I/O across columns rather
     * than fanning out one batch per column. With three sibling columns in the same row groups,
     * the per-(row-group, column) chunks are physically adjacent, so a single coalesced fetch
     * should resolve to roughly the same number of read calls as a single-column extract on the
     * same row groups — never N× more (where N is the column count). A regression that called
     * the single-column overload N times would show roughly N× the read calls of the
     * single-column baseline; this test hard-bounds the multi-column read count to be no more
     * than 2× the single-column count, which is far below the "one batch per column" failure
     * mode while still tolerating any necessary metadata-style follow-up reads.
     */
    public void testExtractMultipleColumnsCoalescesIO() throws IOException {
        byte[] data = writeMixedTypeMultiRowGroupFile(2000);
        ParquetMetadata fullFooter = loadFooter(new TrackingStorageObject(data));
        assertTrue("expected multiple row groups, got " + fullFooter.getBlocks().size(), fullFooter.getBlocks().size() >= 3);

        // Pick three survivors spread across row groups so the multi-column path has to visit
        // more than one bucket. The actual chosen positions don't matter beyond "land in three
        // different row groups" — the test asserts read-count behaviour, not data correctness
        // (which is covered by the other extractor tests).
        long[] survivors = new long[] {
            0L,                                                        // first row group
            fullFooter.getBlocks().get(0).getRowCount() + 1,           // second row group
            fullFooter.getBlocks().get(0).getRowCount() + fullFooter.getBlocks().get(1).getRowCount() + 1 };

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        // Baseline: single-column extract, count the reads.
        TrackingStorageObject baselineSo = new TrackingStorageObject(data);
        int singleColumnReads;
        try (ColumnExtractor extractor = new ParquetColumnExtractor(baselineSo, reader, fullFooter, ErrorPolicy.PERMISSIVE)) {
            baselineSo.reads.clear();
            try (Block block = extractor.extract("v_int", survivors, blockFactory)) {
                assertEquals(survivors.length, block.getPositionCount());
            }
            singleColumnReads = baselineSo.reads.size();
        }
        assertTrue("baseline single-column extract should issue at least one read", singleColumnReads > 0);

        // Multi-column extract: the F-2 path. Three columns in one call.
        TrackingStorageObject multiSo = new TrackingStorageObject(data);
        int multiColumnReads;
        try (ColumnExtractor extractor = new ParquetColumnExtractor(multiSo, reader, fullFooter, ErrorPolicy.PERMISSIVE)) {
            multiSo.reads.clear();
            Block[] blocks = extractor.extract(new String[] { "v_int", "v_long", "v_str" }, null, survivors, blockFactory);
            try {
                assertEquals(3, blocks.length);
                for (Block b : blocks) {
                    assertEquals(survivors.length, b.getPositionCount());
                }
            } finally {
                org.elasticsearch.core.Releasables.closeExpectNoException(blocks);
            }
            multiColumnReads = multiSo.reads.size();
        }

        // F-2 invariant: multi-column read count must NOT scale with the column count. A
        // regression that called extract per column would show ~3× the baseline. Allow up to 2×
        // to absorb any incidental coalescing-edge differences (e.g., a separator that splits a
        // merged range), well below the "fan out per column" failure mode.
        assertTrue(
            "multi-column extract issued "
                + multiColumnReads
                + " reads, expected <= 2 × single-column baseline ("
                + singleColumnReads
                + ")",
            multiColumnReads <= 2 * singleColumnReads
        );
    }

    /**
     * Verifies that per-row-group prefetches are dispatched in parallel: bucket B's
     * {@code readBytesAsync} must be issued before bucket A's read returns. This is the parallel
     * dispatch property that lets the slowest GET no longer dominate end-to-end latency.
     *
     * <p>The fixture: three survivors in three different row groups, fed to an async storage
     * object that holds the first response until both dispatches have been observed. If the
     * extractor still serialised on one read at a time (e.g., {@code prefetch().join()} inside a
     * loop) the test would hang on the latch and time out.
     */
    public void testExtractDispatchesPrefetchesInParallel() throws Exception {
        byte[] data = writeMultiRowGroupFile(2000);
        ParquetMetadata fullFooter = loadFooter(new TrackingStorageObject(data));
        assertTrue("expected at least 3 row groups", fullFooter.getBlocks().size() >= 3);

        // Survivors landing in three distinct row groups → three buckets → three prefetches.
        long rg0Rows = fullFooter.getBlocks().get(0).getRowCount();
        long rg1Rows = fullFooter.getBlocks().get(1).getRowCount();
        long[] survivors = new long[] { 0L, rg0Rows + 1, rg0Rows + rg1Rows + 1 };

        // Compute the on-disk byte windows for each row group's column chunks; those are the
        // requests we want to gate on the latch (other reads — footer probe / metadata — must
        // pass through immediately so the extractor can boot up).
        long[][] rgWindows = new long[3][2];
        for (int i = 0; i < 3; i++) {
            BlockMetaData rg = fullFooter.getBlocks().get(i);
            rgWindows[i][0] = rg.getStartingPos();
            rgWindows[i][1] = rg.getStartingPos() + rg.getCompressedSize();
        }

        ExecutorService ioExecutor = Executors.newFixedThreadPool(4);
        try {
            BlockingChunkStorageObject blocking = new BlockingChunkStorageObject(data, ioExecutor, rgWindows);
            ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
            try (ColumnExtractor extractor = new ParquetColumnExtractor(blocking, reader, fullFooter, ErrorPolicy.PERMISSIVE)) {
                blocking.armLatch();
                Thread t = new Thread(() -> {
                    try (Block b = extractor.extract("v", survivors, blockFactory)) {
                        assertEquals(survivors.length, b.getPositionCount());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, "extract-driver");
                t.setDaemon(true);
                t.start();

                // Wait until at least 3 chunk-window dispatches have been observed without
                // releasing any. If the extractor were serialising prefetches we would only ever
                // see one in-flight at a time and this would time out.
                assertTrue(
                    "expected 3 in-flight chunk prefetches; observed "
                        + blocking.observedDispatches.get()
                        + " — extractor likely serialised the prefetch loop",
                    blocking.dispatchedAtLeast.await(15, TimeUnit.SECONDS)
                );

                // All three are concurrently in flight: now release them and let extract finish.
                blocking.release();
                t.join(30_000);
                assertFalse("extract did not complete after release", t.isAlive());
            }
        } finally {
            ioExecutor.shutdownNow();
        }
    }

    /**
     * Struct-leaf columns (e.g. {@code "event.action"}) must be extractable via the deferred
     * TopN path. The two bugs this exercises: (1) {@code resolveColumnInfo} previously returned
     * {@code null} for dotted names because it only checked top-level fields; (2)
     * {@code decodeBucketsAsTheyArrive} called {@code schema.getType(columnName)} which throws
     * for dotted names. The multi-row-group setup forces the cross-bucket code path.
     */
    public void testExtractStructLeafColumns() throws IOException {
        byte[] data = writeStructFile(200);
        StorageObject so = createStorageObject(data);
        try (ColumnExtractor extractor = newFullFileExtractor(so)) {
            assertEquals(200, extractor.rowCount());
            long[] positions = { 0, 50, 99, 100, 150, 199 };

            try (Block strBlock = extractor.extract("event.action", positions, blockFactory)) {
                BytesRefBlock strs = (BytesRefBlock) strBlock;
                assertEquals(positions.length, strs.getPositionCount());
                for (int i = 0; i < positions.length; i++) {
                    assertEquals("action-" + positions[i], strs.getBytesRef(i, new org.apache.lucene.util.BytesRef()).utf8ToString());
                }
            }

            try (Block intBlock = extractor.extract("event.id", positions, blockFactory)) {
                IntBlock ints = (IntBlock) intBlock;
                assertEquals(positions.length, ints.getPositionCount());
                for (int i = 0; i < positions.length; i++) {
                    assertEquals((int) positions[i], ints.getInt(i));
                }
            }

            // Also validate that the per-column schema and projection are built
            // correctly when multiple dotted names are resolved in one pass.
            Block[] blocks = extractor.extract(new String[] { "event.id", "event.action" }, null, positions, blockFactory);
            try {
                IntBlock ints = (IntBlock) blocks[0];
                BytesRefBlock strs = (BytesRefBlock) blocks[1];
                assertEquals(positions.length, ints.getPositionCount());
                assertEquals(positions.length, strs.getPositionCount());
                for (int i = 0; i < positions.length; i++) {
                    assertEquals((int) positions[i], ints.getInt(i));
                    assertEquals("action-" + positions[i], strs.getBytesRef(i, new org.apache.lucene.util.BytesRef()).utf8ToString());
                }
            } finally {
                Releasables.close(blocks);
            }
        }
    }

    /**
     * Gather-path regression for #152592: the first-visited bucket's decoded block is all-null for
     * a projected nullable column and a later bucket has values. {@code stitchAndGather} then
     * concatenates a {@code ConstantNullBlock} (from the null-leading bucket) with a typed block;
     * resolving the builder type from the first bucket would poison a {@code ConstantNullBlock}
     * builder and throw. This is the random-access (LOOKUP-style) path that only "worked" before by
     * luck of bucket ordering.
     */
    public void testExtractNullLeadingBucketGather() throws IOException {
        int rows = 2000;
        byte[] data = writeNullLeadingMultiRowGroupFile(rows);
        StorageObject so = createStorageObject(data);
        ParquetMetadata footer = loadFooter(so);
        // Multiple row groups are required so position 0 (a null row, first row group -> first
        // visited bucket) and the last valued row land in different buckets, producing the
        // null-leading concatenation the fix targets.
        assertTrue("expected multiple row groups, got " + footer.getBlocks().size(), footer.getBlocks().size() >= 2);

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        // localPositions ordered {null-first, valued-later} so the all-null bucket is visited (and
        // concatenated) before the valued bucket.
        long nullPos = 0;
        long valuePos = rows - 1;
        long[] positions = { nullPos, valuePos };
        try (ColumnExtractor extractor = new ParquetColumnExtractor(so, reader, footer, ErrorPolicy.PERMISSIVE)) {
            try (Block intBlock = extractor.extract("opt_int", positions, blockFactory)) {
                assertEquals(2, intBlock.getPositionCount());
                assertTrue("null-leading position must decode null", intBlock.isNull(0));
                IntBlock ints = (IntBlock) intBlock;
                assertFalse(ints.isNull(1));
                assertEquals((int) valuePos, ints.getInt(ints.getFirstValueIndex(1)));
            }
            try (Block strBlock = extractor.extract("opt_str", positions, blockFactory)) {
                assertEquals(2, strBlock.getPositionCount());
                assertTrue("null-leading position must decode null", strBlock.isNull(0));
                BytesRefBlock strs = (BytesRefBlock) strBlock;
                assertFalse(strs.isNull(1));
                assertEquals(
                    "row-" + valuePos,
                    strs.getBytesRef(strs.getFirstValueIndex(1), new org.apache.lucene.util.BytesRef()).utf8ToString()
                );
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // Fixtures
    // -----------------------------------------------------------------------------------------

    /** Single-int-column file with sequential values 0..rows-1. One row group. */
    private byte[] writeIntFile(int rows) throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("v").named("ints");
        return writeFile(schema, factory -> {
            List<Group> groups = new ArrayList<>(rows);
            for (int i = 0; i < rows; i++) {
                groups.add(factory.newGroup().append("v", i));
            }
            return groups;
        }, /* rowGroupBytes = */ 64 * 1024 * 1024L);
    }

    /**
     * Two optional columns (int, string) where the first half of the rows are null and the rest
     * carry values ({@code opt_int = r}, {@code opt_str = "row-r"}). A small row-group budget forces
     * multiple row groups so a null row (position 0) and a valued row (last position) land in
     * different buckets — the setup {@link #testExtractNullLeadingBucketGather} needs to produce a
     * null-leading concatenation.
     */
    private byte[] writeNullLeadingMultiRowGroupFile(int rows) throws IOException {
        MessageType schema = Types.buildMessage()
            .optional(PrimitiveType.PrimitiveTypeName.INT32)
            .named("opt_int")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("opt_str")
            .named("null_leading");
        return writeFile(schema, factory -> {
            List<Group> groups = new ArrayList<>(rows);
            for (int i = 0; i < rows; i++) {
                Group g = factory.newGroup();
                if (i >= rows / 2) {
                    g.add("opt_int", i);
                    g.add("opt_str", Binary.fromString("row-" + i));
                }
                groups.add(g);
            }
            return groups;
        }, /* rowGroupBytes = */ 1024L);
    }

    /** Same shape as {@link #writeIntFile} but with a small row-group budget so the writer
     *  splits into multiple groups, exercising the extractor's cross-row-group scan. */
    private byte[] writeMultiRowGroupFile(int rows) throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("v").named("ints");
        // 1 KiB row-group budget: each int row is 4 bytes uncompressed plus parquet overhead, so
        // 500 rows split comfortably into multiple row groups.
        return writeFile(schema, factory -> {
            List<Group> groups = new ArrayList<>(rows);
            for (int i = 0; i < rows; i++) {
                groups.add(factory.newGroup().append("v", i));
            }
            return groups;
        }, /* rowGroupBytes = */ 1024L);
    }

    private byte[] writeNullableIntMultiRowGroupFile(int rows) throws IOException {
        MessageType schema = Types.buildMessage().optional(PrimitiveType.PrimitiveTypeName.INT32).named("v").named("ints");
        return writeFile(schema, factory -> {
            List<Group> groups = new ArrayList<>(rows);
            for (int i = 0; i < rows; i++) {
                Group g = factory.newGroup();
                if (i % 5 != 0) {
                    g.append("v", i);
                }
                groups.add(g);
            }
            return groups;
        }, /* rowGroupBytes = */ 1024L);
    }

    /**
     * Single-int-list-column file with {@code rows} rows; row {@code r} has list length
     * {@code r % 5} and values {@code r*10, r*10+1, ...}. Empty lists at every fifth row exercise
     * the rep-level walk; {@code rows = 60} keeps the file small (single row group) but covers
     * enough rep-level transitions to catch off-by-one bugs in the sparse skip loop.
     */
    private byte[] writeIntListFile(int rows) throws IOException {
        return writeIntListFile(rows, /* rowGroupBytes = */ 64 * 1024 * 1024L);
    }

    private byte[] writeMultiRowGroupIntListFile(int rows) throws IOException {
        return writeIntListFile(rows, /* rowGroupBytes = */ 1024L);
    }

    private byte[] writeIntListFile(int rows, long rowGroupBytes) throws IOException {
        org.apache.parquet.schema.Type intList = Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.INT32).named("vals");
        MessageType schema = new MessageType("ints_list", intList);
        return writeFile(schema, factory -> {
            List<Group> groups = new ArrayList<>(rows);
            for (int i = 0; i < rows; i++) {
                Group g = factory.newGroup();
                int listLen = i % 5;
                if (listLen > 0) {
                    Group vals = g.addGroup("vals");
                    for (int k = 0; k < listLen; k++) {
                        vals.addGroup("list").append("element", i * 10 + k);
                    }
                }
                // listLen == 0 → leave the "vals" group absent so the row materialises as an
                // empty list. This is the rep-level transition the sparse skip path has to walk.
                groups.add(g);
            }
            return groups;
        }, rowGroupBytes);
    }

    private static long nextRowDivisibleByFive(long row) {
        long remainder = row % 5;
        return remainder == 0 ? row : row + (5 - remainder);
    }

    private static void assertIntListRow(IntBlock ints, int outputSlot, int row) {
        int listLen = row % 5;
        assertEquals("position count mismatch at output slot " + outputSlot, listLen, ints.getValueCount(outputSlot));
        int firstValue = ints.getFirstValueIndex(outputSlot);
        for (int k = 0; k < listLen; k++) {
            assertEquals("value at row " + row + " element " + k, row * 10 + k, ints.getInt(firstValue + k));
        }
    }

    /**
     * Three-column file (int, long, string) with a small row-group budget so the writer emits
     * multiple row groups. Used by the multi-column I/O coalescing test to verify that a
     * multi-column extract issues O(row groups) HTTP-equivalent reads rather than
     * O(row groups × columns).
     */
    private byte[] writeMixedTypeMultiRowGroupFile(int rows) throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("v_int")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("v_long")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("v_str")
            .named("mixed");
        return writeFile(schema, factory -> {
            List<Group> groups = new ArrayList<>(rows);
            for (int i = 0; i < rows; i++) {
                Group g = factory.newGroup();
                g.add("v_int", i);
                g.add("v_long", (long) i * 1_000L);
                g.add("v_str", Binary.fromString("row-" + i));
                groups.add(g);
            }
            return groups;
        }, /* rowGroupBytes = */ 1024L);
    }

    /** Three-column file: int, long, string. Used to exercise per-column extraction. */
    private byte[] writeMixedTypeFile(int rows) throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("v_int")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("v_long")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("v_str")
            .named("mixed");
        return writeFile(schema, factory -> {
            List<Group> groups = new ArrayList<>(rows);
            for (int i = 0; i < rows; i++) {
                Group g = factory.newGroup();
                g.add("v_int", i);
                g.add("v_long", (long) i * 1_000L);
                g.add("v_str", Binary.fromString("row-" + i));
                groups.add(g);
            }
            return groups;
        }, /* rowGroupBytes = */ 64 * 1024 * 1024L);
    }

    /**
     * File with an {@code event} STRUCT containing {@code id} (int) and {@code action} (string)
     * sub-fields. Small row-group budget forces multiple row groups so the cross-bucket path is
     * exercised. Row {@code r} has {@code event.id = r} and {@code event.action = "action-r"}.
     */
    private byte[] writeStructFile(int rows) throws IOException {
        MessageType schema = Types.buildMessage()
            .requiredGroup()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("action")
            .named("event")
            .named("events");
        return writeFile(schema, factory -> {
            List<Group> groups = new ArrayList<>(rows);
            for (int i = 0; i < rows; i++) {
                Group g = factory.newGroup();
                Group event = g.addGroup("event");
                event.add("id", i);
                event.add("action", Binary.fromString("action-" + i));
                groups.add(g);
            }
            return groups;
        }, /* rowGroupBytes = */ 1024L);
    }

    private byte[] writeFile(MessageType schema, Function<SimpleGroupFactory, List<Group>> generator, long rowGroupBytes)
        throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputFile outputFile = inMemoryOutputFile(out);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(rowGroupBytes)
                .build()
        ) {
            for (Group g : generator.apply(factory)) {
                writer.write(g);
            }
        }
        return out.toByteArray();
    }

    private static OutputFile inMemoryOutputFile(ByteArrayOutputStream out) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) throws IOException {
                        out.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        out.write(b, off, len);
                        position += len;
                    }

                    @Override
                    public void close() throws IOException {
                        out.close();
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
                return "memory://test.parquet";
            }
        };
    }

    private static StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://test.parquet");
            }
        };
    }

    /**
     * In-memory {@link StorageObject} that records every {@code newStream(position, length)}
     * call. Used by {@link #testExtractTouchesOnlyTargetedRowGroups} to verify the targeted
     * extraction path only fetches bytes from row groups that actually own a surviving
     * position. The {@code newStream()} (no-arg, full-file) call is also recorded so the test
     * can detect a "scan the whole file" regression even if it sneaks in via that path.
     */
    private static final class TrackingStorageObject implements StorageObject {
        private final byte[] data;
        final List<long[]> reads = new ArrayList<>();

        TrackingStorageObject(byte[] data) {
            this.data = data;
        }

        @Override
        public InputStream newStream() {
            reads.add(new long[] { 0L, data.length });
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            int pos = (int) position;
            int len = (int) Math.min(length, data.length - position);
            reads.add(new long[] { position, len });
            return new ByteArrayInputStream(data, pos, len);
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public Instant lastModified() {
            return Instant.now();
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of("memory://test-tracking.parquet");
        }
    }

    /**
     * Async {@link StorageObject} that lets the test gate the completion of in-flight reads.
     * Reads whose byte window overlaps any of the configured "chunk windows" (the row group
     * column-chunk byte ranges from the parquet footer) are blocked on a latch until the test
     * calls {@link #release()}. All other reads (footer probes, dictionary metadata) pass
     * through immediately so the extractor can boot up. Used by
     * {@link #testExtractDispatchesPrefetchesInParallel} to assert per-row-group prefetches
     * are dispatched concurrently.
     */
    private static final class BlockingChunkStorageObject implements StorageObject {
        private final byte[] data;
        private final Executor executor;
        private final long[][] chunkWindows;
        private final CountDownLatch releaseLatch = new CountDownLatch(1);
        private final CountDownLatch dispatchedAtLeast = new CountDownLatch(3);
        private final AtomicInteger observedDispatches = new AtomicInteger();

        BlockingChunkStorageObject(byte[] data, Executor executor, long[][] chunkWindows) {
            this.data = data;
            this.executor = executor;
            this.chunkWindows = chunkWindows;
        }

        void armLatch() {
            // No-op: latch is set up at construction time. The method exists to make test intent
            // explicit at the call site.
        }

        void release() {
            releaseLatch.countDown();
        }

        private boolean overlapsAnyChunk(long position, long length) {
            long start = position;
            long end = position + length;
            for (long[] w : chunkWindows) {
                if (start < w[1] && w[0] < end) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            int pos = (int) position;
            int len = (int) Math.min(length, data.length - position);
            return new ByteArrayInputStream(data, pos, len);
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public Instant lastModified() {
            return Instant.now();
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of("memory://test-blocking.parquet");
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
            Executor unused,
            ActionListener<DirectReadBuffer> listener
        ) {
            boolean blocking = overlapsAnyChunk(position, length);
            if (blocking) {
                observedDispatches.incrementAndGet();
                dispatchedAtLeast.countDown();
            }
            executor.execute(() -> {
                try {
                    if (blocking) {
                        if (releaseLatch.await(20, TimeUnit.SECONDS) == false) {
                            listener.onFailure(new IOException("blocking read was not released within timeout"));
                            return;
                        }
                    }
                    int pos = (int) position;
                    int len = (int) Math.min(length, data.length - position);
                    byte[] copy = new byte[len];
                    System.arraycopy(data, pos, copy, 0, len);
                    listener.onResponse(new DirectReadBuffer(ByteBuffer.wrap(copy), () -> {}));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    listener.onFailure(new IOException("interrupted while waiting for release", e));
                }
            });
        }
    }
}
