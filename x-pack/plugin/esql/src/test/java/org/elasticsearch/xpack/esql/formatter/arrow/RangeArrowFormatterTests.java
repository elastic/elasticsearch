/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.formatter.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleRangeBlock;
import org.elasticsearch.compute.data.DoubleRangeBlockBuilder.DoubleRange;
import org.elasticsearch.compute.data.LongRangeBlock;
import org.elasticsearch.compute.data.LongRangeBlockBuilder.LongRange;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.After;
import org.junit.AfterClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class RangeArrowFormatterTests extends ESTestCase {

    private static final RootAllocator ALLOCATOR = new RootAllocator();

    private final MockBigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), ByteSizeValue.ofGb(1));
    private final BlockFactory blockFactory = BlockFactory.builder(bigArrays).build();

    @After
    public void assertBreakerClean() {
        assertEquals(0L, blockFactory.breaker().getUsed());
    }

    @AfterClass
    public static void afterClass() {
        ALLOCATOR.close();
    }

    public void testArrowFields() {
        assertRangeField(
            BlockArrowFormatter.AsRange.dateRange(DataType.DATE_RANGE).arrowField("dr"),
            "dr",
            DataType.DATE_RANGE,
            Types.MinorType.TIMESTAMPMILLI
        );
        assertRangeField(
            BlockArrowFormatter.AsRange.doubleRange(DataType.DOUBLE_RANGE).arrowField("dblr"),
            "dblr",
            DataType.DOUBLE_RANGE,
            Types.MinorType.FLOAT8
        );
    }

    public void testConstantNullBlockSingleValued() throws IOException {
        for (int positions : new int[] { 0, 1, 5, 9 }) {
            for (BlockArrowFormatter formatter : List.of(
                BlockArrowFormatter.AsRange.dateRange(DataType.DATE_RANGE),
                BlockArrowFormatter.AsRange.doubleRange(DataType.DOUBLE_RANGE)
            )) {
                try (Block block = blockFactory.newConstantNullBlock(positions)) {
                    Converted converted = convertAndWrite(formatter, block, false);
                    assertEquals(3, converted.nodes.size());
                    assertFieldNode(converted.nodes.get(0), positions, positions);
                    assertFieldNode(converted.nodes.get(1), positions, positions);
                    assertFieldNode(converted.nodes.get(2), positions, positions);

                    // Buffers: struct validity, from validity, from data, to validity, to data
                    long validityBytes = BlockArrowFormatter.bitSetLength(positions);
                    long dataBytes = (long) positions * Long.BYTES;
                    assertEquals(List.of(validityBytes, validityBytes, dataBytes, validityBytes, dataBytes), converted.sizes);
                    for (byte[] buf : converted.writtenBytes) {
                        for (byte b : buf) {
                            assertEquals(0, b);
                        }
                    }
                }
            }
        }
    }

    public void testConstantNullBlockAsList() throws IOException {
        for (int positions : new int[] { 0, 1, 5 }) {
            for (BlockArrowFormatter formatter : List.of(
                BlockArrowFormatter.AsRange.dateRange(DataType.DATE_RANGE),
                BlockArrowFormatter.AsRange.doubleRange(DataType.DOUBLE_RANGE)
            )) {
                try (Block block = blockFactory.newConstantNullBlock(positions)) {
                    Converted converted = convertAndWrite(formatter, block, true);
                    assertEquals(4, converted.nodes.size());
                    assertFieldNode(converted.nodes.get(0), positions, positions);
                    assertFieldNode(converted.nodes.get(1), 0, 0);
                    assertFieldNode(converted.nodes.get(2), 0, 0);
                    assertFieldNode(converted.nodes.get(3), 0, 0);

                    long validityBytes = BlockArrowFormatter.bitSetLength(positions);
                    long offsetsBytes = (long) (positions + 1) * Integer.BYTES;
                    assertEquals(List.of(validityBytes, offsetsBytes, 0L, 0L, 0L, 0L, 0L), converted.sizes);
                }
            }
        }
    }

    public void testDenseSingleValuedUsesEmptyValidity() throws IOException {
        try (LongRangeBlock.Builder builder = blockFactory.newLongRangeBlockBuilder(2)) {
            builder.appendLongRange(10L, 20L);
            builder.appendLongRange(30L, 40L);
            try (LongRangeBlock block = builder.build()) {
                assertDenseSingleValuedLayout(BlockArrowFormatter.AsRange.dateRange(DataType.DATE_RANGE), block);
            }
        }

        try (DoubleRangeBlock.Builder builder = blockFactory.newDoubleRangeBlockBuilder(2)) {
            builder.appendDoubleRange(1.5, 2.5);
            builder.appendDoubleRange(3.5, 4.5);
            try (DoubleRangeBlock block = builder.build()) {
                assertDenseSingleValuedLayout(BlockArrowFormatter.AsRange.doubleRange(DataType.DOUBLE_RANGE), block);
            }
        }
    }

    public void testMultiPageDateRangeWithMultivaluedConstantNullAndSingleValued() throws IOException {
        try (
            Page p1 = new Page(buildMultivaluedDateRangeBlock());
            Page p2 = new Page(blockFactory.newConstantNullBlock(2));
            Page p3 = new Page(blockFactory.newConstantLongRangeBlockWith(new LongRange(700L, 800L), 2));
            Page p4 = new Page(buildEmptyDateRangeBlock())
        ) {
            assertMultiPageListRoundTrip(
                DataType.DATE_RANGE,
                "dr",
                List.of(p1, p2, p3, p4),
                (vec, i) -> ((TimeStampMilliVector) vec).get(i),
                List.of(100L, 300L, 500L),
                List.of(200L, 400L, 600L),
                700L,
                800L,
                0
            );
        }
    }

    public void testMultiPageDoubleRangeWithMultivaluedConstantNullAndSingleValued() throws IOException {
        try (
            Page p1 = new Page(buildMultivaluedDoubleRangeBlock());
            Page p2 = new Page(blockFactory.newConstantNullBlock(2));
            Page p3 = new Page(blockFactory.newConstantDoubleRangeBlockWith(new DoubleRange(7.5, 8.5), 2));
            Page p4 = new Page(buildAllNullDoubleRangeBlock(2))
        ) {
            assertMultiPageListRoundTrip(
                DataType.DOUBLE_RANGE,
                "dblr",
                List.of(p1, p2, p3, p4),
                (vec, i) -> ((Float8Vector) vec).get(i),
                List.of(-1.5, 3.25, 5.0),
                List.of(2.5, 4.75, 6.0),
                7.5,
                8.5,
                2
            );
        }
    }

    public void testSingleValuedRoundTripWithNullsAndConstantNullPage() throws IOException {
        try (
            LongRangeBlock drBlock = buildSparseDateRangeBlock();
            DoubleRangeBlock dblrBlock = buildSparseDoubleRangeBlock();
            Block drNulls = blockFactory.newConstantNullBlock(2);
            Block dblrNulls = blockFactory.newConstantNullBlock(2)
        ) {
            drBlock.incRef();
            dblrBlock.incRef();
            drNulls.incRef();
            dblrNulls.incRef();
            try (Page p1 = new Page(drBlock, dblrBlock); Page p2 = new Page(drNulls, dblrNulls)) {
                List<ArrowResponse.Column> columns = List.of(
                    new ArrowResponse.Column(DataType.DATE_RANGE, "dr"),
                    new ArrowResponse.Column(DataType.DOUBLE_RANGE, "dblr")
                );
                try (
                    ReleasableBytesReference encoded = encodeResponse(columns, List.of(p1, p2));
                    ArrowStreamReader reader = new ArrowStreamReader(encoded.streamInput(), ALLOCATOR);
                    VectorSchemaRoot root = reader.getVectorSchemaRoot()
                ) {
                    assertTrue(reader.loadNextBatch());
                    assertEquals(3, root.getRowCount());
                    StructVector dr = (StructVector) root.getVector("dr");
                    TimeStampMilliVector drFrom = (TimeStampMilliVector) dr.getChild("from");
                    TimeStampMilliVector drTo = (TimeStampMilliVector) dr.getChild("to");
                    assertFalse(dr.isNull(0));
                    assertEquals(1L, drFrom.get(0));
                    assertEquals(2L, drTo.get(0));
                    assertTrue(dr.isNull(1));
                    assertTrue(drFrom.isNull(1));
                    assertTrue(drTo.isNull(1));
                    assertFalse(dr.isNull(2));
                    assertEquals(3L, drFrom.get(2));
                    assertEquals(4L, drTo.get(2));

                    StructVector dblr = (StructVector) root.getVector("dblr");
                    Float8Vector dblrFrom = (Float8Vector) dblr.getChild("from");
                    Float8Vector dblrTo = (Float8Vector) dblr.getChild("to");
                    assertTrue(dblr.isNull(0));
                    assertTrue(dblrFrom.isNull(0));
                    assertTrue(dblrTo.isNull(0));
                    assertFalse(dblr.isNull(1));
                    assertEquals(1.25, dblrFrom.get(1), 0.0);
                    assertEquals(2.75, dblrTo.get(1), 0.0);
                    assertTrue(dblr.isNull(2));
                    assertTrue(dblrFrom.isNull(2));
                    assertTrue(dblrTo.isNull(2));

                    assertTrue(reader.loadNextBatch());
                    assertEquals(2, root.getRowCount());
                    for (int row = 0; row < 2; row++) {
                        assertTrue(dr.isNull(row));
                        assertTrue(drFrom.isNull(row));
                        assertTrue(drTo.isNull(row));
                        assertTrue(dblr.isNull(row));
                        assertTrue(dblrFrom.isNull(row));
                        assertTrue(dblrTo.isNull(row));
                    }

                    assertFalse(reader.loadNextBatch());
                }
            }
        }
    }

    private <T> void assertMultiPageListRoundTrip(
        DataType type,
        String columnName,
        List<Page> pages,
        BiFunction<FieldVector, Integer, T> getter,
        List<T> batch1From,
        List<T> batch1To,
        T batch3From,
        T batch3To,
        int batch4NullRows
    ) throws IOException {
        List<ArrowResponse.Column> columns = List.of(new ArrowResponse.Column(type, columnName));
        try (
            ReleasableBytesReference encoded = encodeResponse(columns, pages);
            ArrowStreamReader reader = new ArrowStreamReader(encoded.streamInput(), ALLOCATOR);
            VectorSchemaRoot root = reader.getVectorSchemaRoot()
        ) {
            // Batch 1: [[(f0,t0), (f1,t1)], null, [(f2,t2)]]
            assertTrue(reader.loadNextBatch());
            assertEquals(3, root.getRowCount());
            ListVector list = (ListVector) root.getVector(columnName);
            StructVector struct = (StructVector) list.getDataVector();
            FieldVector from = struct.getChild("from");
            FieldVector to = struct.getChild("to");
            assertFalse(list.isNull(0));
            assertEquals(0, list.getElementStartIndex(0));
            assertEquals(2, list.getElementEndIndex(0));
            assertEquals(batch1From.get(0), getter.apply(from, 0));
            assertEquals(batch1To.get(0), getter.apply(to, 0));
            assertEquals(batch1From.get(1), getter.apply(from, 1));
            assertEquals(batch1To.get(1), getter.apply(to, 1));
            assertTrue(list.isNull(1));
            assertFalse(list.isNull(2));
            assertEquals(2, list.getElementStartIndex(2));
            assertEquals(3, list.getElementEndIndex(2));
            assertEquals(batch1From.get(2), getter.apply(from, 2));
            assertEquals(batch1To.get(2), getter.apply(to, 2));

            // Batch 2: ConstantNullBlock of 2 nulls
            assertTrue(reader.loadNextBatch());
            assertEquals(2, root.getRowCount());
            assertTrue(list.isNull(0));
            assertTrue(list.isNull(1));

            // Batch 3: constant single-valued block encoded as list
            assertTrue(reader.loadNextBatch());
            assertEquals(2, root.getRowCount());
            for (int row = 0; row < 2; row++) {
                assertFalse(list.isNull(row));
                assertEquals(row, list.getElementStartIndex(row));
                assertEquals(row + 1, list.getElementEndIndex(row));
                assertEquals(batch3From, getter.apply(from, row));
                assertEquals(batch3To, getter.apply(to, row));
            }

            // Batch 4: empty or all-null builder block
            assertTrue(reader.loadNextBatch());
            assertEquals(batch4NullRows, root.getRowCount());
            for (int row = 0; row < batch4NullRows; row++) {
                assertTrue(list.isNull(row));
            }

            assertFalse(reader.loadNextBatch());
        }
    }

    private void assertDenseSingleValuedLayout(BlockArrowFormatter formatter, Block block) throws IOException {
        Converted converted = convertAndWrite(formatter, block, false);
        assertEquals(3, converted.nodes.size());
        assertFieldNode(converted.nodes.get(0), 2, 0);
        assertFieldNode(converted.nodes.get(1), 2, 0);
        assertFieldNode(converted.nodes.get(2), 2, 0);
        assertEquals(List.of(0L, 0L, 16L, 0L, 16L), converted.sizes);
    }

    private LongRangeBlock buildMultivaluedDateRangeBlock() {
        try (LongRangeBlock.Builder builder = blockFactory.newLongRangeBlockBuilder(3)) {
            builder.beginPositionEntry();
            builder.appendLongRange(100L, 200L);
            builder.appendLongRange(300L, 400L);
            builder.endPositionEntry();
            builder.appendNull();
            builder.appendLongRange(500L, 600L);
            return builder.build();
        }
    }

    private LongRangeBlock buildEmptyDateRangeBlock() {
        try (LongRangeBlock.Builder builder = blockFactory.newLongRangeBlockBuilder(0)) {
            return builder.build();
        }
    }

    private LongRangeBlock buildSparseDateRangeBlock() {
        try (LongRangeBlock.Builder builder = blockFactory.newLongRangeBlockBuilder(3)) {
            builder.appendLongRange(1L, 2L);
            builder.appendNull();
            builder.appendLongRange(3L, 4L);
            return builder.build();
        }
    }

    private DoubleRangeBlock buildMultivaluedDoubleRangeBlock() {
        try (DoubleRangeBlock.Builder builder = blockFactory.newDoubleRangeBlockBuilder(3)) {
            builder.beginPositionEntry();
            builder.appendDoubleRange(-1.5, 2.5);
            builder.appendDoubleRange(3.25, 4.75);
            builder.endPositionEntry();
            builder.appendNull();
            builder.appendDoubleRange(5.0, 6.0);
            return builder.build();
        }
    }

    private DoubleRangeBlock buildAllNullDoubleRangeBlock(int positions) {
        try (DoubleRangeBlock.Builder builder = blockFactory.newDoubleRangeBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendNull();
            }
            return builder.build();
        }
    }

    private DoubleRangeBlock buildSparseDoubleRangeBlock() {
        try (DoubleRangeBlock.Builder builder = blockFactory.newDoubleRangeBlockBuilder(3)) {
            builder.appendNull();
            builder.appendDoubleRange(1.25, 2.75);
            builder.appendNull();
            return builder.build();
        }
    }

    private record Converted(List<ArrowFieldNode> nodes, List<Long> sizes, List<byte[]> writtenBytes) {}

    private Converted convertAndWrite(BlockArrowFormatter formatter, Block block, boolean asList) throws IOException {
        List<ArrowFieldNode> nodes = new ArrayList<>();
        List<ArrowBuf> bufs = new ArrayList<>();
        List<BlockArrowFormatter.BufWriter> writers = new ArrayList<>();
        List<Releasable> releasables = new ArrayList<>();
        try {
            formatter.convert(block, asList, nodes, bufs, writers, releasables);
            assertEquals(bufs.size(), writers.size());
            List<Long> sizes = new ArrayList<>(bufs.size());
            List<byte[]> writtenBytes = new ArrayList<>(bufs.size());
            for (int i = 0; i < bufs.size(); i++) {
                long expectedSize = bufs.get(i).writerIndex();
                sizes.add(expectedSize);
                try (RecyclerBytesStreamOutput out = new RecyclerBytesStreamOutput(bigArrays.bytesRefRecycler())) {
                    long written = writers.get(i).write(out);
                    assertEquals(expectedSize, written);
                    assertEquals(expectedSize, out.size());
                    writtenBytes.add(BytesReference.toBytes(out.bytes()));
                }
            }
            return new Converted(nodes, sizes, writtenBytes);
        } finally {
            Releasables.close(releasables);
        }
    }

    private ReleasableBytesReference encodeResponse(List<ArrowResponse.Column> columns, List<Page> pages) throws IOException {
        List<ReleasableBytesReference> chunks = new ArrayList<>();
        boolean success = false;
        try (ArrowResponse response = new ArrowResponse(columns, pages)) {
            while (response.isPartComplete() == false) {
                chunks.add(response.encodeChunk(1024, bigArrays.bytesRefRecycler()));
            }
            BytesReference composite = CompositeBytesReference.of(chunks.toArray(BytesReference[]::new));
            success = true;
            return new ReleasableBytesReference(composite, () -> Releasables.close(chunks));
        } finally {
            if (success == false) {
                Releasables.close(chunks);
            }
        }
    }

    private static void assertRangeField(Field field, String name, DataType esqlType, Types.MinorType childType) {
        assertEquals(name, field.getName());
        assertTrue(field.isNullable());
        assertEquals(Types.MinorType.STRUCT.getType(), field.getType());
        assertEquals(Map.of(BlockArrowFormatter.ESQL_TYPE_METADATA, esqlType.outputType()), field.getMetadata());
        assertEquals(2, field.getChildren().size());
        assertChildField(field.getChildren().get(0), "from", childType);
        assertChildField(field.getChildren().get(1), "to", childType);
    }

    private static void assertChildField(Field child, String expectedName, Types.MinorType expectedType) {
        assertEquals(expectedName, child.getName());
        assertTrue(child.isNullable());
        assertEquals(expectedType.getType(), child.getType());
    }

    private static void assertFieldNode(ArrowFieldNode node, int expectedLength, int expectedNullCount) {
        assertEquals(expectedLength, node.getLength());
        assertEquals(expectedNullCount, node.getNullCount());
    }
}
