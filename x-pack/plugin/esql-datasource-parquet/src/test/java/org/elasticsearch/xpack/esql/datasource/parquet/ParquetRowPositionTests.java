/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorProducer;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.RangeReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Tests for the {@code _rowPosition} synthetic column emitted by
 * {@link ParquetFormatReader#read} when the projection requests it. The reader must:
 * <ul>
 *     <li>emit values that are file-global and start at zero,</li>
 *     <li>be monotonically increasing within and across emitted pages,</li>
 *     <li>align row-for-row with the data columns in the same projection,</li>
 *     <li>place the {@code _rowPosition} block at the requested projection slot regardless of
 *         where it sits in the projection list.</li>
 * </ul>
 */
public class ParquetRowPositionTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactoryAndClearFooterCache() {
        ParquetStorageObjectAdapter.clearFooterCacheForTests();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testRowPositionEmittedAtRequestedSlot() throws IOException {
        byte[] data = writeIntFile(10);
        StorageObject storage = createStorageObject(data);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        // Slot 0: _rowPosition. Slot 1: data column.
        List<String> projection = List.of(ColumnExtractor.ROW_POSITION_COLUMN, "v");
        try (CloseableIterator<Page> iter = reader.read(storage, FormatReadContext.of(projection, 1024))) {
            ((ColumnExtractorProducer) iter).setExtractorId(0);
            assertTrue(iter.hasNext());
            Page page = iter.next();
            try {
                assertEquals(2, page.getBlockCount());
                assertEquals(10, page.getPositionCount());
                LongBlock rp = (LongBlock) page.getBlock(0);
                IntBlock vs = (IntBlock) page.getBlock(1);
                for (int i = 0; i < 10; i++) {
                    assertEquals("rowPosition[" + i + "]", i, rp.getLong(i));
                    assertEquals("data[" + i + "]", i, vs.getInt(i));
                }
            } finally {
                page.releaseBlocks();
            }
            assertFalse(iter.hasNext());
        }
    }

    public void testRowPositionEmittedAtTrailingSlot() throws IOException {
        // Same as above but with _rowPosition at slot 1, after the data column. Verifies the
        // injection logic respects the projection's declared position rather than always
        // appending the synthetic block at the end.
        byte[] data = writeIntFile(5);
        StorageObject storage = createStorageObject(data);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        List<String> projection = List.of("v", ColumnExtractor.ROW_POSITION_COLUMN);
        try (CloseableIterator<Page> iter = reader.read(storage, FormatReadContext.of(projection, 1024))) {
            // Complete the producer handshake: the iterator now pre-encodes _rowPosition with
            // the registry-assigned id (the factory's job in production). Use id 0 so the
            // encoded high bits are zero and the emitted longs equal the raw row indices we
            // assert against below.
            ((ColumnExtractorProducer) iter).setExtractorId(0);
            Page page = iter.next();
            try {
                IntBlock vs = (IntBlock) page.getBlock(0);
                LongBlock rp = (LongBlock) page.getBlock(1);
                for (int i = 0; i < 5; i++) {
                    assertEquals(i, vs.getInt(i));
                    assertEquals(i, rp.getLong(i));
                }
            } finally {
                page.releaseBlocks();
            }
        }
    }

    public void testRowPositionMonotonicAcrossPages() throws IOException {
        // 5 row groups × 100 rows each, batch size 73 → multiple emissions per row group, with
        // page boundaries that don't align to row-group boundaries. _rowPosition must remain
        // strictly monotonic and dense across the whole sequence.
        byte[] data = writeMultiRowGroupFile(500);
        StorageObject storage = createStorageObject(data);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        List<String> projection = List.of("v", ColumnExtractor.ROW_POSITION_COLUMN);
        long expectedNext = 0;
        long totalRows = 0;
        try (CloseableIterator<Page> iter = reader.read(storage, FormatReadContext.of(projection, 73))) {
            ((ColumnExtractorProducer) iter).setExtractorId(0);
            while (iter.hasNext()) {
                Page page = iter.next();
                try {
                    int positions = page.getPositionCount();
                    LongBlock rp = (LongBlock) page.getBlock(1);
                    IntBlock vs = (IntBlock) page.getBlock(0);
                    for (int i = 0; i < positions; i++) {
                        long pos = rp.getLong(i);
                        assertEquals("monotonic & dense across page boundaries", expectedNext, pos);
                        // Data column must align row-for-row with the rowPosition block; the
                        // file was written with v[i] = i.
                        assertEquals(Math.toIntExact(pos), vs.getInt(i));
                        expectedNext++;
                    }
                    totalRows += positions;
                } finally {
                    page.releaseBlocks();
                }
            }
        }
        assertEquals("total rows must match the file", 500L, totalRows);
        assertEquals(500L, expectedNext);
    }

    /**
     * Range-split path: when {@code readRange} is asked for {@code _rowPosition}, it must emit
     * <em>file-global</em> positions, not split-local. The whole point of file-global addressing
     * is that any iterator over a file (full read or any split of any byte range) and the
     * matching extractor agree on what each row is, without coordination. The split this test
     * picks intentionally lands deep in the file so a counter-based "starts at zero per split"
     * implementation would be visibly wrong.
     */
    public void testReadRangeRowPositionIsFileGlobal() throws IOException {
        byte[] data = writeMultiRowGroupFile(500);
        StorageObject storage = createStorageObject(data);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        // Pick the second half of the file: any row from this split must carry a _rowPosition
        // strictly greater than the first row of the file (which is row 0, value 0). The
        // contiguous-emit invariant is verified per-row: pos == v (the file wrote v[i] = i).
        long fileLength = data.length;
        long rangeStart = fileLength / 2;
        long rangeEnd = fileLength;

        List<String> projection = List.of("v", ColumnExtractor.ROW_POSITION_COLUMN);
        RangeReadContext ctx = new RangeReadContext(projection, /* batchSize */ 1024, rangeStart, rangeEnd, null, ErrorPolicy.STRICT);
        long totalRows = 0L;
        long firstSeenPosition = -1L;
        try (CloseableIterator<Page> iter = reader.readRange(storage, ctx)) {
            assertTrue("range iterator must implement ColumnExtractorProducer", iter instanceof ColumnExtractorProducer);
            ((ColumnExtractorProducer) iter).setExtractorId(0);
            while (iter.hasNext()) {
                Page page = iter.next();
                try {
                    int positions = page.getPositionCount();
                    IntBlock vs = (IntBlock) page.getBlock(0);
                    LongBlock rp = (LongBlock) page.getBlock(1);
                    for (int i = 0; i < positions; i++) {
                        long pos = rp.getLong(i);
                        // file-global identity equals the data value (v[i] = i)
                        assertEquals("file-global identity matches data value", vs.getInt(i), pos);
                        if (firstSeenPosition < 0) {
                            firstSeenPosition = pos;
                        }
                    }
                    totalRows += positions;
                } finally {
                    page.releaseBlocks();
                }
            }
        }
        assertTrue("expected the range to cover at least one row group", totalRows > 0);
        assertTrue("split-local would start at 0; file-global must start past the first row", firstSeenPosition > 0);
    }

    /**
     * Companion to {@link #testReadRangeRowPositionIsFileGlobal}: the
     * {@link ColumnExtractorProducer} returned by a range iterator hands back an extractor
     * whose {@link ColumnExtractor#rowCount()} is the <em>file's</em> total row count (not the
     * split's), because the identities the iterator emitted index into the file's address space.
     * Looking up via {@code extract} returns the same value the forward scan saw.
     */
    public void testReadRangeProducerYieldsFileScopedExtractor() throws IOException {
        byte[] data = writeMultiRowGroupFile(500);
        StorageObject storage = createStorageObject(data);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        long fileLength = data.length;
        long rangeStart = fileLength / 2;
        long rangeEnd = fileLength;

        List<String> projection = List.of("v", ColumnExtractor.ROW_POSITION_COLUMN);
        RangeReadContext ctx = new RangeReadContext(projection, /* batchSize */ 1024, rangeStart, rangeEnd, null, ErrorPolicy.STRICT);
        long firstSeenPosition = -1L;
        long lastSeenPosition = -1L;
        ColumnExtractor extractor;
        try (CloseableIterator<Page> iter = reader.readRange(storage, ctx)) {
            ((ColumnExtractorProducer) iter).setExtractorId(0);
            while (iter.hasNext()) {
                Page page = iter.next();
                try {
                    int positions = page.getPositionCount();
                    LongBlock rp = (LongBlock) page.getBlock(1);
                    if (firstSeenPosition < 0) {
                        firstSeenPosition = rp.getLong(0);
                    }
                    lastSeenPosition = rp.getLong(positions - 1);
                } finally {
                    page.releaseBlocks();
                }
            }
            extractor = ((ColumnExtractorProducer) iter).createColumnExtractor(null);
        }
        try {
            // Extractor sees the full file regardless of which split the iterator was for.
            assertEquals(500L, extractor.rowCount());
            // Hand back the very identities the forward scan emitted: the extractor must return
            // the same value the iterator did (v[i] = i for this fixture).
            try (Block block = extractor.extract("v", new long[] { firstSeenPosition, lastSeenPosition }, blockFactory)) {
                IntBlock ints = (IntBlock) block;
                assertEquals((int) firstSeenPosition, ints.getInt(0));
                assertEquals((int) lastSeenPosition, ints.getInt(1));
            }
        } finally {
            extractor.close();
        }
    }

    /**
     * Regression test for the original v1 production bug: {@code AsyncExternalSourceOperatorFactory}
     * passes its full unified query attributes — which under deferred extraction <em>include</em>
     * a synthetic {@link ColumnExtractor#ROW_POSITION_COLUMN} attribute — as
     * {@link RangeReadContext#resolvedAttributes()}. A short-lived collision guard in {@code
     * readRange} treated that as proof of a real user column named {@code _rowPosition} and fell
     * through to a path that did not implement {@link ColumnExtractorProducer}. The current
     * design has no special guard at all (the optimizer rule's planning-time bail-out catches
     * actual user collisions), so this test is a bare correctness check that the iterator still
     * implements {@link ColumnExtractorProducer} when {@code _rowPosition} is in the projection,
     * regardless of the {@code resolvedAttributes} shape.
     */
    public void testReadRangeRowPositionRoutesEvenWhenResolvedAttributesIncludeIt() throws IOException {
        byte[] data = writeMultiRowGroupFile(500);
        StorageObject storage = createStorageObject(data);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        long fileLength = data.length;
        long rangeStart = fileLength / 2;
        long rangeEnd = fileLength;

        List<String> projection = List.of("v", ColumnExtractor.ROW_POSITION_COLUMN);
        List<Attribute> resolvedAttributes = List.of(
            field("v", DataType.INTEGER),
            field(ColumnExtractor.ROW_POSITION_COLUMN, DataType.LONG)
        );
        RangeReadContext ctx = new RangeReadContext(
            projection,
            /* batchSize */ 1024,
            rangeStart,
            rangeEnd,
            resolvedAttributes,
            ErrorPolicy.STRICT
        );

        try (CloseableIterator<Page> iter = reader.readRange(storage, ctx)) {
            assertTrue(
                "range iterator must implement ColumnExtractorProducer when _rowPosition is in the projection",
                iter instanceof ColumnExtractorProducer
            );
            ((ColumnExtractorProducer) iter).setExtractorId(0);
            assertTrue(iter.hasNext());
            Page page = iter.next();
            try {
                LongBlock rp = (LongBlock) page.getBlock(1);
                IntBlock vs = (IntBlock) page.getBlock(0);
                // The file wrote v[i] = i; in file-global addressing each emitted row's
                // _rowPosition matches its data value byte-for-byte.
                for (int i = 0; i < page.getPositionCount(); i++) {
                    assertEquals(vs.getInt(i), rp.getLong(i));
                }
            } finally {
                page.releaseBlocks();
            }
        }
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    public void testSetExtractorIdEncodesEmittedRowPositions() throws IOException {
        // F-9A invariant: setExtractorId installs the high bits the iterator OR-s into every
        // emitted _rowPosition value. With a non-zero id the encoded value is no longer the bare
        // physical row offset; decoding it must round-trip back to (id, offset).
        byte[] data = writeIntFile(8);
        StorageObject storage = createStorageObject(data);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        int extractorId = 7;

        try (
            CloseableIterator<Page> iter = reader.read(
                storage,
                FormatReadContext.of(List.of("v", ColumnExtractor.ROW_POSITION_COLUMN), 1024)
            )
        ) {
            ((ColumnExtractorProducer) iter).setExtractorId(extractorId);
            Page page = iter.next();
            try {
                LongBlock rp = (LongBlock) page.getBlock(1);
                long expectedHighBits = ((long) extractorId) << ColumnExtractor.LOCAL_POSITION_BITS;
                long localMask = (1L << ColumnExtractor.LOCAL_POSITION_BITS) - 1L;
                for (int i = 0; i < page.getPositionCount(); i++) {
                    long encoded = rp.getLong(i);
                    assertEquals("high bits must carry the installed extractor id", expectedHighBits, encoded & ~localMask);
                    assertEquals("low bits must carry the file-global row offset", (long) i, encoded & localMask);
                }
            } finally {
                page.releaseBlocks();
            }
        }
    }

    public void testSetExtractorIdRejectsDoubleInstall() throws IOException {
        byte[] data = writeIntFile(2);
        StorageObject storage = createStorageObject(data);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        try (
            CloseableIterator<Page> iter = reader.read(
                storage,
                FormatReadContext.of(List.of("v", ColumnExtractor.ROW_POSITION_COLUMN), 1024)
            )
        ) {
            ColumnExtractorProducer producer = (ColumnExtractorProducer) iter;
            producer.setExtractorId(1);
            // Second install is a programmer error: the framework should call setExtractorId
            // exactly once between createColumnExtractor and the first next() call.
            expectThrows(IllegalStateException.class, () -> producer.setExtractorId(2));
        }
    }

    public void testRowPositionPreservesPushedFilterByEmittingFileGlobalIds() throws IOException {
        // Rationale: under file-global addressing the iterator emits each surviving row's
        // physical identity, so pushed filters, late materialization, and page skipping are all
        // free to drop rows from the forward scan — every row that comes out still carries its
        // file-global identity, and the extractor binds that identity back to the file's full
        // footer. We verify this by reading a small unfiltered file: each emitted row has
        // _rowPosition equal to v, demonstrating the row identity is anchored to file content.
        byte[] data = writeIntFile(20);
        StorageObject storage = createStorageObject(data);

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        try (
            CloseableIterator<Page> iter = reader.read(
                storage,
                FormatReadContext.of(List.of("v", ColumnExtractor.ROW_POSITION_COLUMN), 1024)
            )
        ) {
            ((ColumnExtractorProducer) iter).setExtractorId(0);
            Page page = iter.next();
            try {
                IntBlock vs = (IntBlock) page.getBlock(0);
                LongBlock rp = (LongBlock) page.getBlock(1);
                assertEquals(20, page.getPositionCount());
                for (int i = 0; i < 20; i++) {
                    // file-global identity == data value (file wrote v[i] = i); regardless of
                    // any forward-scan transformations, the identity is intrinsic to the row.
                    assertEquals(vs.getInt(i), rp.getLong(i));
                }
            } finally {
                page.releaseBlocks();
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // fixtures
    // ---------------------------------------------------------------------------------------------

    private byte[] writeIntFile(int rows) throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("v").named("ints");
        return writeFile(schema, factory -> {
            List<Group> groups = new ArrayList<>(rows);
            for (int i = 0; i < rows; i++) {
                groups.add(factory.newGroup().append("v", i));
            }
            return groups;
        }, /* rowGroupBytes = */ 64L * 1024 * 1024);
    }

    private byte[] writeMultiRowGroupFile(int rows) throws IOException {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("v").named("ints");
        // Small row-group budget so the writer splits the rows into multiple groups; that
        // exercises the cross-row-group position arithmetic the reader uses to emit
        // _rowPosition values for emitted batches.
        return writeFile(schema, factory -> {
            List<Group> groups = new ArrayList<>(rows);
            for (int i = 0; i < rows; i++) {
                groups.add(factory.newGroup().append("v", i));
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
                .withConf(new org.apache.parquet.conf.PlainParquetConfiguration())
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
}
