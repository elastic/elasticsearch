/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xpack.versionfield.Version;
import org.junit.After;
import org.junit.AfterClass;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.instanceOf;

public class ArrowResponseTests extends ESTestCase {

    private static final RootAllocator ALLOCATOR = new RootAllocator();

    private final MockPageCacheRecycler pageCacheRecycler = new MockPageCacheRecycler(Settings.EMPTY);
    private final MockBigArrays bigArrays = new MockBigArrays(pageCacheRecycler, ByteSizeValue.ofGb(1));
    private final BlockFactory blockFactory = BlockFactory.getInstance(
        bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST),
        bigArrays
    );

    @After
    public void assertBreakerClean() {
        assertEquals(0L, blockFactory.breaker().getUsed());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        ALLOCATOR.close();
    }

    // ---------------------------------------------------------------------------------------------
    // Value creation, getters for ESQL and Arrow

    static final ValueType INTEGER_VALUES = new ValueTypeImpl<IntBlock.Builder, IntBlock, IntVector>(
        "integer",
        factory -> factory.newIntBlockBuilder(0),
        block -> block.appendInt(randomInt()),
        (block, i, scratch) -> block.getInt(i),
        IntVector::get
    );

    static final ValueType LONG_VALUES = new ValueTypeImpl<LongBlock.Builder, LongBlock, BigIntVector>(
        "long",
        factory -> factory.newLongBlockBuilder(0),
        block -> block.appendLong(randomLong()),
        (block, i, scratch) -> block.getLong(i),
        BigIntVector::get
    );

    static final ValueType ULONG_VALUES = new ValueTypeImpl<LongBlock.Builder, LongBlock, UInt8Vector>(
        "ulong",
        factory -> factory.newLongBlockBuilder(0),
        block -> block.appendLong(randomLong()),
        (block, i, scratch) -> block.getLong(i),
        UInt8Vector::get
    );

    static final ValueType DATE_VALUES = new ValueTypeImpl<LongBlock.Builder, LongBlock, TimeStampMilliVector>(
        "date",
        factory -> factory.newLongBlockBuilder(0),
        block -> block.appendLong(randomLong()),
        (block, i, scratch) -> block.getLong(i),
        TimeStampMilliVector::get
    );

    static final ValueType DOUBLE_VALUES = new ValueTypeImpl<DoubleBlock.Builder, DoubleBlock, Float8Vector>(
        "double",
        factory -> factory.newDoubleBlockBuilder(0),
        block -> block.appendDouble(randomDouble()),
        (block, i, scratch) -> block.getDouble(i),
        Float8Vector::get
    );

    static final ValueType BOOLEAN_VALUES = new ValueTypeImpl<BooleanBlock.Builder, BooleanBlock, BitVector>(
        "boolean",
        factory -> factory.newBooleanBlockBuilder(0),
        block -> block.appendBoolean(randomBoolean()),
        (b, i, s) -> b.getBoolean(i),
        (v, i) -> v.get(i) != 0 // Arrow's BitVector returns 0 or 1
    );

    static final ValueType TEXT_VALUES = new ValueTypeImpl<BytesRefBlock.Builder, BytesRefBlock, VarCharVector>(
        "text",
        factory -> factory.newBytesRefBlockBuilder(0),
        block -> block.appendBytesRef(new BytesRef(randomUnicodeOfLengthBetween(1, 20))),
        (b, i, s) -> b.getBytesRef(i, s).utf8ToString(),
        (v, i) -> new String(v.get(i), StandardCharsets.UTF_8)
    );

    static final ValueType SOURCE_VALUES = new ValueTypeImpl<BytesRefBlock.Builder, BytesRefBlock, VarCharVector>(
        "source",
        factory -> factory.newBytesRefBlockBuilder(0),
        block -> block.appendBytesRef(new BytesRef("{\"foo\": " + randomIntBetween(-42, 42) + "}")),
        (b, i, s) -> b.getBytesRef(i, s).utf8ToString(),
        (v, i) -> new String(v.get(i), StandardCharsets.UTF_8)
    );

    static final ValueType IP_VALUES = new ValueTypeImpl<BytesRefBlock.Builder, BytesRefBlock, VarBinaryVector>(
        "ip",
        factory -> factory.newBytesRefBlockBuilder(0),
        block -> {
            byte[] addr = InetAddressPoint.encode(randomIp(randomBoolean()));
            assertEquals(16, addr.length); // Make sure all is ipv6-mapped
            block.appendBytesRef(new BytesRef(addr));
        },
        (b, i, s) -> ValueConversions.shortenIpV4Addresses(b.getBytesRef(i, s), new BytesRef()),
        (v, i) -> new BytesRef(v.get(i))
    );

    static final ValueType BINARY_VALUES = new ValueTypeImpl<BytesRefBlock.Builder, BytesRefBlock, VarBinaryVector>(
        "binary",
        factory -> factory.newBytesRefBlockBuilder(0),
        block -> block.appendBytesRef(new BytesRef(randomByteArrayOfLength(randomIntBetween(1, 100)))),
        BytesRefBlock::getBytesRef,
        (v, i) -> new BytesRef(v.get(i))
    );

    static final ValueType VERSION_VALUES = new ValueTypeImpl<BytesRefBlock.Builder, BytesRefBlock, VarCharVector>(
        "version",
        factory -> factory.newBytesRefBlockBuilder(0),
        block -> block.appendBytesRef(new Version(between(0, 100) + "." + between(0, 100) + "." + between(0, 100)).toBytesRef()),
        (b, i, s) -> new Version(b.getBytesRef(i, s)).toString(),
        (v, i) -> new String(v.get(i), StandardCharsets.UTF_8)
    );

    static final ValueType NULL_VALUES = new ValueTypeImpl<Block.Builder, Block, FieldVector>(
        "null",
        factory -> factory.newBytesRefBlockBuilder(0),
        Block.Builder::appendNull,
        (b, i, s) -> b.isNull(i) ? null : "non-null in block",
        (v, i) -> v.isNull(i) ? null : "non-null in vector"
    );

    static final Map<String, ValueType> VALUE_TYPES = Map.ofEntries(
        Map.entry("integer", INTEGER_VALUES),
        Map.entry("counter_integer", INTEGER_VALUES),
        Map.entry("long", LONG_VALUES),
        Map.entry("counter_long", LONG_VALUES),
        Map.entry("unsigned_long", ULONG_VALUES),
        Map.entry("double", DOUBLE_VALUES),
        Map.entry("counter_double", DOUBLE_VALUES),

        Map.entry("text", TEXT_VALUES),
        Map.entry("keyword", TEXT_VALUES),

        Map.entry("boolean", BOOLEAN_VALUES),
        Map.entry("date", DATE_VALUES),
        Map.entry("ip", IP_VALUES),
        Map.entry("version", VERSION_VALUES),
        Map.entry("_source", SOURCE_VALUES),

        Map.entry("null", NULL_VALUES),
        Map.entry("unsupported", NULL_VALUES),

        // All geo types just pass-through WKB, use random binary data
        Map.entry("geo_point", BINARY_VALUES),
        Map.entry("geo_shape", BINARY_VALUES),
        Map.entry("cartesian_point", BINARY_VALUES),
        Map.entry("cartesian_shape", BINARY_VALUES)
    );

    // ---------------------------------------------------------------------------------------------
    // Tests

    public void testTestHarness() {
        TestColumn testColumn = TestColumn.create("foo", "integer");
        TestBlock denseBlock = TestBlock.create(blockFactory, testColumn, Density.Dense, 3);
        TestBlock sparseBlock = TestBlock.create(blockFactory, testColumn, Density.Sparse, 5);
        TestBlock emptyBlock = TestBlock.create(blockFactory, testColumn, Density.Empty, 7);

        // Test that density works as expected
        assertFalse(denseBlock.block.mayHaveNulls());
        assertTrue(sparseBlock.block.mayHaveNulls());
        assertFalse(sparseBlock.block.areAllValuesNull());
        assertTrue(emptyBlock.block.areAllValuesNull());

        // Test that values iterator scans all pages
        List<TestPage> pages = Stream.of(denseBlock, sparseBlock, emptyBlock).map(b -> new TestPage(List.of(b))).toList();
        try (TestCase tc = new TestCase(List.of(testColumn), pages)) {
            EsqlValuesIterator valuesIterator = new EsqlValuesIterator(tc, 0);
            int count = 0;
            while (valuesIterator.hasNext()) {
                valuesIterator.next();
                count++;
            }
            assertEquals(3 + 5 + 7, count);
        }

        // Test that we have value types for all types
        List<String> converters = new ArrayList<>(ArrowResponse.ESQL_CONVERTERS.keySet());
        Collections.sort(converters);
        List<String> valueTypes = new ArrayList<>(VALUE_TYPES.keySet());
        Collections.sort(valueTypes);
        assertEquals("Missing test value types", converters, valueTypes);
    }

    /**
     * Test single-column for all types with a mix of dense/sparse/empty pages
     */
    public void testSingleColumn() throws IOException {
        for (var type : VALUE_TYPES.keySet()) {
            TestColumn testColumn = new TestColumn("foo", type, VALUE_TYPES.get(type), false);
            List<TestPage> pages = new ArrayList<>();

            for (var density : Density.values()) {
                TestBlock testBlock = TestBlock.create(blockFactory, testColumn, density, 10);
                TestPage testPage = new TestPage(List.of(testBlock));
                pages.add(testPage);
            }
            TestCase testCase = new TestCase(List.of(testColumn), pages);

            compareEsqlAndArrow(testCase);
        }
    }

    public void testSingleBlock() throws IOException {
        // Simple test to easily focus on a specific type & density
        String type = "text";
        Density density = Density.Dense;

        TestColumn testColumn = new TestColumn("foo", type, VALUE_TYPES.get(type), false);
        List<TestPage> pages = new ArrayList<>();

        TestBlock testBlock = TestBlock.create(blockFactory, testColumn, density, 10);
        TestPage testPage = new TestPage(List.of(testBlock));
        pages.add(testPage);

        TestCase testCase = new TestCase(List.of(testColumn), pages);

        compareEsqlAndArrow(testCase);
    }

    /**
     * Test a multivalued field with fixed size values.
     */
    public void testMultivaluedInteger() throws IOException {
        IntBlock block;
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(0)) {
            builder.beginPositionEntry();
            builder.appendInt(42);
            builder.appendInt(43);
            builder.endPositionEntry();

            // The multivalue can be null, but a multivalue cannot contain nulls.
            // Calling appendNull within a begin/endEntry causes consistency checks to fail in build()
            // See also https://github.com/elastic/elasticsearch/issues/114324
            builder.appendNull();

            builder.beginPositionEntry();
            builder.appendInt(44);
            builder.appendInt(45);
            builder.endPositionEntry();

            // single value
            builder.appendInt(46);

            block = builder.build();
        }

        // Consistency check.
        // AbstractArrayBlock.assertInvariants does some of these consistency checks, but those below
        // specifically verify the assumptions on which the conversion to Arrow is built.
        assertTrue(block.mayHaveMultivaluedFields());
        assertEquals(4, block.getPositionCount()); // counts null entries
        assertEquals(5, block.getTotalValueCount()); // nulls aren't counted

        // Value 0
        assertEquals(2, block.getValueCount(0));
        assertEquals(0, block.getFirstValueIndex(0));
        assertEquals(42, block.getInt(block.getFirstValueIndex(0)));
        assertEquals(43, block.getInt(block.getFirstValueIndex(0) + 1));

        // Value 1
        assertEquals(0, block.getValueCount(1));
        assertTrue(block.isNull(1)); // This is the position index, not value index
        // No value, but still occupies a value slot with zero
        assertEquals(2, block.getFirstValueIndex(1));
        assertEquals(0, block.getInt(block.getFirstValueIndex(1)));
        assertEquals(3, block.getFirstValueIndex(2));

        // Value 2
        assertEquals(2, block.getValueCount(2));
        assertEquals(3, block.getFirstValueIndex(2));
        assertEquals(44, block.getInt(block.getFirstValueIndex(2)));
        assertEquals(45, block.getInt(block.getFirstValueIndex(2) + 1));

        // Value 3
        assertEquals(1, block.getValueCount(3));
        assertEquals(5, block.getFirstValueIndex(3));
        assertEquals(46, block.getInt(block.getFirstValueIndex(3)));

        // End of block
        assertEquals(6, block.getFirstValueIndex(4));

        var column = TestColumn.create("some-field", "integer", true);
        TestCase testCase = new TestCase(List.of(column), List.of(new TestPage(List.of(TestBlock.create(column, block)))));

        try (testCase; VectorSchemaRoot root = toArrowVectors(testCase)) {
            compareEsqlAndArrow(testCase, root);
            ListVector vector = (ListVector) root.getVector("some-field");
            assertListLayout(vector, 0, 2, 2, 4, 5);
        }
    }

    public void testAllNullMultivaluedIntegerPageLayout() throws IOException {
        IntBlock multivaluedBlock;
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(1)) {
            builder.beginPositionEntry();
            builder.appendInt(1);
            builder.appendInt(2);
            builder.endPositionEntry();
            multivaluedBlock = builder.build();
        }
        Block nullBlock = blockFactory.newConstantNullBlock(2);
        var column = TestColumn.create("some-field", "integer", true);
        TestCase testCase = new TestCase(
            List.of(column),
            List.of(
                new TestPage(List.of(TestBlock.create(column, multivaluedBlock))),
                new TestPage(List.of(TestBlock.create(column, nullBlock)))
            )
        );

        try (testCase; VectorSchemaRoot root = toArrowVectors(testCase)) {
            compareEsqlAndArrow(testCase, root);
            assertListLayout((ListVector) root.getVector("some-field"), 0, 2, 2, 2);
        }
    }

    /**
     * Test a multivalued field with variable size values.
     */
    public void testMultivalueString() throws IOException {
        BytesRefBlock block;
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(0)) {
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("a"));
            builder.appendBytesRef(new BytesRef("b"));
            builder.endPositionEntry();

            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("c"));
            builder.appendBytesRef(new BytesRef("d"));
            builder.endPositionEntry();

            block = builder.build();
        }

        var column = TestColumn.create("some-field", "text");
        TestCase testCase = new TestCase(List.of(column), List.of(new TestPage(List.of(TestBlock.create(column, block)))));

        compareEsqlAndArrow(testCase);
    }

    // Test exercising Arrow's multivalue API
    public void testMultiValueArrow() throws IOException {

        byte[] bytes;

        try (ListVector listVector = ListVector.empty("some-field", ALLOCATOR)) {
            UnionListWriter writer = listVector.getWriter();

            writer.startList();
            writer.writeInt(42); // 0x2A
            writer.writeInt(43); // 0x2A
            writer.endList();

            writer.startList();
            // Size is zero without a writeNull()
            writer.writeNull(); // Adds a null value in that list
            writer.endList();

            writer.startList();
            writer.writeInt(44); // 0x2C
            writer.writeInt(45); // 0x2D
            writer.endList();

            writer.startList();
            writer.writeInt(46); // 0x2E
            writer.endList();

            listVector.setValueCount(4);
            bytes = getBytes(listVector);
        }

        try (var reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), ALLOCATOR)) {
            var root = reader.getVectorSchemaRoot();
            reader.loadNextBatch();

            ListVector listVector = (ListVector) root.getVector("some-field");

            assertEquals(4, listVector.getValueCount());
            assertEquals(List.of(42, 43), listVector.getObject(0));
            assertEquals(Collections.singletonList((Integer) null), listVector.getObject(1));
            assertEquals(List.of(44, 45), listVector.getObject(2));
            assertEquals(List.of(46), listVector.getObject(3));
        }
    }

    private static byte[] getBytes(ListVector listVector) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        var fields = List.of(listVector.getField());
        List<FieldVector> vectors = List.of(listVector);

        try (
            VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);
            ArrowStreamWriter arrowWriter = new ArrowStreamWriter(root, null, baos);
        ) {
            arrowWriter.start();
            arrowWriter.writeBatch();
            arrowWriter.end();
        }
        return baos.toByteArray();
    }

    /**
     * Test a random set of types/columns/pages/densities
     */
    public void testRandomTypesAndSize() throws IOException {

        // Shuffle types to randomize their succession in the Arrow stream
        List<String> types = new ArrayList<>(VALUE_TYPES.keySet());
        Collections.shuffle(types, random());

        List<TestColumn> columns = types.stream().map(type -> TestColumn.create("col-" + type, type)).toList();

        List<TestPage> pages = IntStream
            // 1 to 10 pages of random density and 1 to 1000 values
            .range(0, randomIntBetween(1, 100))
            .mapToObj(i -> TestPage.create(blockFactory, columns))
            .toList();

        TestCase testCase = new TestCase(columns, pages);

        compareEsqlAndArrow(testCase);
    }

    public void testTransformedColumnsWithRecycledPages() throws IOException {
        int positions = (PageCacheRecycler.INT_PAGE_SIZE / 2) / Integer.BYTES;
        List<TestColumn> columns = Stream.of("ip", "version", "_source").map(type -> TestColumn.create("col-" + type, type)).toList();
        List<TestBlock> blocks = columns.stream()
            .map(col -> TestBlock.create(blockFactory, col, randomFrom(Density.Dense, Density.Sparse), positions))
            .toList();
        compareEsqlAndArrow(new TestCase(columns, List.of(new TestPage(blocks))));
    }

    public void testTransformedValuesAreConvertedOnce() throws IOException {
        AtomicInteger conversionCount = new AtomicInteger();
        var formatter = new BlockConverter.TransformedBytesRef("keyword", Types.MinorType.VARCHAR, (value, scratch) -> {
            conversionCount.incrementAndGet();
            return value;
        });
        var bufs = new ArrayList<ArrowBuf>();
        var bufWriters = new ArrayList<BlockConverter.BufWriter>();
        var releasables = new ArrayList<Releasable>();
        try (
            BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(3);
            BytesRefBlock block = builder.appendBytesRef(new BytesRef("one")).appendNull().appendBytesRef(new BytesRef("two")).build();
            RecyclerBytesStreamOutput out = new RecyclerBytesStreamOutput(new BytesRefRecycler(pageCacheRecycler))
        ) {
            formatter.convert(block, false, bufs, bufWriters, releasables);
            assertEquals(2, conversionCount.get());

            for (BlockConverter.BufWriter writer : bufWriters) {
                writer.write(out);
            }
            assertEquals(2, conversionCount.get());
        } finally {
            Releasables.close(releasables);
        }
    }

    public void testSparseOrdinalBytesRefBlock() throws IOException {
        String version = "1.2.3";
        BytesRefBlock ordinalBlock;
        try (
            BytesRefVector constVersion = blockFactory.newConstantBytesRefVector(new Version(version).toBytesRef(), 2);
            BooleanVector.Builder maskBuilder = blockFactory.newBooleanVectorBuilder(2);
            BooleanVector mask = maskBuilder.appendBoolean(true).appendBoolean(false).build()
        ) {
            ordinalBlock = constVersion.keepMask(mask);
        }
        assertThat(ordinalBlock, instanceOf(OrdinalBytesRefBlock.class));
        assertEquals(0, ((OrdinalBytesRefBlock) ordinalBlock).getOrdinalsBlock().getInt(1));

        var bufs = new ArrayList<ArrowBuf>();
        var releasables = new ArrayList<Releasable>();
        try {
            ArrowResponse.ESQL_CONVERTERS.get("version").convert(ordinalBlock, false, bufs, new ArrayList<>(), releasables);
            assertEquals(version.length(), bufs.get(bufs.size() - 1).writerIndex());
        } finally {
            Releasables.close(releasables);
        }

        TestColumn col = TestColumn.create("col-version", "version", false);
        compareEsqlAndArrow(new TestCase(List.of(col), List.of(new TestPage(List.of(TestBlock.create(col, ordinalBlock))))));
    }

    public void testMultivaluedTransformedBytesRefBlocksWithNulls() throws IOException {
        for (String type : List.of("ip", "version", "_source")) {
            var column = TestColumn.create("col-" + type, type, true);
            Block block;
            try (Block.Builder builder = column.valueType.createBlockBuilder(blockFactory)) {
                builder.beginPositionEntry();
                column.valueType.addValue(builder, Density.Dense);
                column.valueType.addValue(builder, Density.Dense);
                builder.endPositionEntry();
                builder.appendNull();
                column.valueType.addValue(builder, Density.Dense);
                block = builder.build();
            }
            assertTrue(block.mayHaveMultivaluedFields());
            assertTrue(block.isNull(1));

            var testCase = new TestCase(List.of(column), List.of(new TestPage(List.of(TestBlock.create(column, block)))));
            try (testCase; VectorSchemaRoot root = toArrowVectors(testCase)) {
                compareEsqlAndArrow(testCase, root);
                assertListLayout((ListVector) root.getVector(column.name), 0, 2, 2, 3);
            }
        }
    }

    public void testMultivaluedOrdinalBytesRefBlockLayout() throws IOException {
        BytesRefBlock ordinalBlock;
        try (
            IntBlock.Builder ordinals = blockFactory.newIntBlockBuilder(3);
            BytesRefVector.Builder dictionary = blockFactory.newBytesRefVectorBuilder(3)
        ) {
            ordinals.beginPositionEntry();
            ordinals.appendInt(0);
            ordinals.appendInt(1);
            ordinals.endPositionEntry();
            ordinals.appendNull();
            ordinals.appendInt(2);
            dictionary.appendBytesRef(new Version("1.2.3").toBytesRef());
            dictionary.appendBytesRef(new Version("2.0.0").toBytesRef());
            dictionary.appendBytesRef(new Version("3.0.0").toBytesRef());
            ordinalBlock = new OrdinalBytesRefBlock(ordinals.build(), dictionary.build());
        }

        var column = TestColumn.create("some-field", "version", true);
        TestCase testCase = new TestCase(List.of(column), List.of(new TestPage(List.of(TestBlock.create(column, ordinalBlock)))));
        try (testCase; VectorSchemaRoot root = toArrowVectors(testCase)) {
            compareEsqlAndArrow(testCase, root);
            assertListLayout((ListVector) root.getVector("some-field"), 0, 2, 2, 3);
        }
    }

    private static void assertListLayout(ListVector vector, int... expectedOffsets) {
        assertEquals(expectedOffsets.length - 1, vector.getValueCount());
        for (int row = 0; row < vector.getValueCount(); row++) {
            assertEquals(expectedOffsets[row], vector.getElementStartIndex(row));
            assertEquals(expectedOffsets[row + 1], vector.getElementEndIndex(row));
        }
        assertEquals(expectedOffsets[expectedOffsets.length - 1], vector.getDataVector().getValueCount());
    }

    // ---------------------------------------------------------------------------------------------
    // Test harness

    private void compareEsqlAndArrow(TestCase testCase) throws IOException {
        try (testCase; VectorSchemaRoot arrowVectors = toArrowVectors(testCase)) {
            compareEsqlAndArrow(testCase, arrowVectors);
        }
    }

    private void compareEsqlAndArrow(TestCase testCase, VectorSchemaRoot root) {
        for (int i = 0; i < testCase.columns.size(); i++) {

            // Check esql type in the metadata
            var metadata = root.getSchema().getFields().get(i).getMetadata();
            assertEquals(testCase.columns.get(i).type, metadata.get("elastic:type"));

            // Check values
            var esqlValuesIterator = new EsqlValuesIterator(testCase, i);
            var arrowValuesIterator = new ArrowValuesIterator(testCase, root, i);

            int line = 0;

            while (esqlValuesIterator.hasNext() && arrowValuesIterator.hasNext()) {
                Object esqlValue = esqlValuesIterator.next();
                Object arrowValue = arrowValuesIterator.next();
                assertEquals(("line " + line), esqlValue, arrowValue);
                line++;
            }

            // Make sure we entirely consumed both sides.
            assertFalse(esqlValuesIterator.hasNext());
            assertFalse(arrowValuesIterator.hasNext());
        }
    }

    private VectorSchemaRoot toArrowVectors(TestCase testCase) throws IOException {
        ArrowResponse response = new ArrowResponse(
            testCase.columns.stream().map(c -> new ArrowResponse.Column(c.type, c.name)).toList(),
            testCase.pages.stream().map(p -> new Page(p.blocks.stream().map(b -> b.block).toArray(Block[]::new))).toList()
        );

        assertEquals("application/vnd.apache.arrow.stream", response.getResponseContentTypeString());

        try (
            ReleasableBytesReference bytes = serializeBlocksDirectly(response);
            ArrowStreamReader reader = new ArrowStreamReader(bytes.streamInput(), ALLOCATOR);
            VectorSchemaRoot readerRoot = reader.getVectorSchemaRoot();
        ) {
            VectorSchemaRoot root = VectorSchemaRoot.create(readerRoot.getSchema(), ALLOCATOR);
            root.allocateNew();

            while (reader.loadNextBatch()) {
                VectorSchemaRootAppender.append(root, readerRoot);
            }

            return root;
        }
    }

    /**
     * An iterator over values of a column across all pages.
     */
    static class EsqlValuesIterator implements Iterator<Object> {
        private final int fieldPos;
        private final ValueType type;
        private final Iterator<TestPage> pages;

        private TestPage page;
        private int position;

        EsqlValuesIterator(TestCase testCase, int column) {
            this.fieldPos = column;
            this.type = testCase.columns.get(column).valueType;
            this.position = 0;
            this.pages = testCase.pages.iterator();
            this.page = pages.next();
        }

        @Override
        public boolean hasNext() {
            return page != null;
        }

        @Override
        public Object next() {
            if (page == null) {
                throw new NoSuchElementException();
            }
            Block block = page.blocks.get(fieldPos).block;
            Object result = block.isNull(position) ? null : type.valueAt(block, position, new BytesRef());
            position++;
            if (position >= block.getPositionCount()) {
                position = 0;
                page = pages.hasNext() ? pages.next() : null;
            }
            return result;
        }
    }

    static class ArrowValuesIterator implements Iterator<Object> {
        private final ValueType type;
        private ValueVector vector;
        private int position;

        ArrowValuesIterator(TestCase testCase, VectorSchemaRoot root, int column) {
            this(root.getVector(column), testCase.columns.get(column).valueType);
        }

        ArrowValuesIterator(ValueVector vector, ValueType type) {
            this.vector = vector;
            this.type = type;
        }

        @Override
        public boolean hasNext() {
            return vector != null;
        }

        @Override
        public Object next() {
            if (vector == null) {
                throw new NoSuchElementException();
            }
            Object result = vector.isNull(position) ? null : type.valueAt(vector, position);
            position++;
            if (position >= vector.getValueCount()) {
                vector = null;
            }
            return result;
        }
    }

    private ReleasableBytesReference serializeBlocksDirectly(ArrowResponse body) throws IOException {
        // Ensure there's a single part, this will fail if we ever change it.
        assertTrue(body.isLastPart());

        List<ReleasableBytesReference> ourEncoding = new ArrayList<>();
        boolean success = false;
        try {
            while (body.isPartComplete() == false) {
                ourEncoding.add(body.encodeChunk(1500, new BytesRefRecycler(pageCacheRecycler)));
            }
            BytesReference composite = CompositeBytesReference.of(ourEncoding.toArray(BytesReference[]::new));
            success = true;
            return new ReleasableBytesReference(composite, () -> Releasables.close(ourEncoding));
        } finally {
            if (success == false) {
                Releasables.close(ourEncoding);
            }
        }
    }

    record TestCase(List<TestColumn> columns, List<TestPage> pages) implements Releasable {
        @Override
        public void close() {
            Releasables.close(pages);
        }

        @Override
        public String toString() {
            return pages.size() + " pages of " + columns.stream().map(TestColumn::type).collect(Collectors.joining("|"));
        }
    }

    record TestColumn(String name, String type, ValueType valueType, boolean multivalue) {
        static TestColumn create(String name, String type) {
            return create(name, type, randomBoolean());
        }

        static TestColumn create(String name, String type, boolean multivalue) {
            return new TestColumn(name, type, VALUE_TYPES.get(type), multivalue);
        }
    }

    record TestPage(List<TestBlock> blocks) implements Releasable {

        @Override
        public void close() {
            Releasables.close(blocks);
        }

        static TestPage create(BlockFactory factory, List<TestColumn> columns) {
            int size = randomIntBetween(1, 1000);
            return new TestPage(columns.stream().map(column -> TestBlock.create(factory, column, size)).toList());
        }

        @Override
        public String toString() {
            return blocks.get(0).block.getPositionCount()
                + " items - "
                + blocks.stream().map(b -> b.density.toString()).collect(Collectors.joining("|"));
        }
    }

    record TestBlock(TestColumn column, Block block, Density density) implements Releasable {

        @Override
        public void close() {
            block.close();
        }

        static TestBlock create(TestColumn column, Block block) {
            Density density;
            if (block.areAllValuesNull()) {
                density = Density.Empty;
            } else if (block.mayHaveNulls()) {
                density = Density.Sparse;
            } else {
                density = Density.Dense;
            }
            return new TestBlock(column, block, density);
        }

        static TestBlock create(BlockFactory factory, TestColumn column, int positions) {
            return create(factory, column, randomFrom(Density.values()), positions);
        }

        static TestBlock create(BlockFactory factory, TestColumn column, Density density, int positions) {
            ValueType valueType = column.valueType();
            Block block;
            if (density == Density.Empty) {
                block = factory.newConstantNullBlock(positions);
            } else {
                try (Block.Builder builder = valueType.createBlockBuilder(factory)) {
                    int start = 0;
                    if (density == Density.Sparse && positions >= 2) {
                        // Make sure it's really sparse even if randomness of values may decide otherwise
                        valueType.addValue(builder, Density.Dense);
                        valueType.addValue(builder, Density.Empty);
                        start = 2;
                    }
                    for (int i = start; i < positions; i++) {
                        // If multivalued, randomly insert a series of values if the type isn't null (nulls are not allowed in multivalues)
                        if (column.multivalue && column.valueType != NULL_VALUES && randomBoolean()) {
                            builder.beginPositionEntry();
                            int numEntries = randomIntBetween(2, 5);
                            for (int j = 0; j < numEntries; j++) {
                                valueType.addValue(builder, Density.Dense);
                            }
                            builder.endPositionEntry();
                        } else {
                            valueType.addValue(builder, density);
                        }
                    }
                    // Will create an ArrayBlock if there are null values, VectorBlock otherwise
                    block = builder.build();
                }
                assertEquals(positions, block.getPositionCount());
            }
            return new TestBlock(column, block, density);
        }
    }

    public enum Density {
        Empty,
        Sparse,
        Dense;

        boolean nextIsNull() {
            return switch (this) {
                case Empty -> true;
                case Sparse -> randomBoolean();
                case Dense -> false;
            };
        }
    }

    interface ValueType {
        Block.Builder createBlockBuilder(BlockFactory factory);

        void addValue(Block.Builder builder, Density density);

        Object valueAt(Block block, int position, BytesRef scratch);

        Object valueAt(ValueVector arrowVec, int position);
    }

    public static class ValueTypeImpl<BlockBT extends Block.Builder, BlockT extends Block, VectorT extends ValueVector>
        implements
            ValueType {
        private final String name;
        private final Function<BlockFactory, BlockBT> builderCreator;
        private final Consumer<BlockBT> valueAdder;
        private final TriFunction<BlockT, Integer, BytesRef, Object> blockGetter;
        private final BiFunction<VectorT, Integer, Object> vectorGetter;

        public ValueTypeImpl(
            String name,
            Function<BlockFactory, BlockBT> builderCreator,
            Consumer<BlockBT> valueAdder,
            TriFunction<BlockT, Integer, BytesRef, Object> blockGetter,
            BiFunction<VectorT, Integer, Object> vectorGetter
        ) {
            this.name = name;
            this.builderCreator = builderCreator;
            this.valueAdder = valueAdder;
            this.blockGetter = blockGetter;
            this.vectorGetter = vectorGetter;
        }

        @Override
        public Block.Builder createBlockBuilder(BlockFactory factory) {
            return builderCreator.apply(factory);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void addValue(Block.Builder builder, Density density) {
            if (density.nextIsNull()) {
                builder.appendNull();
            } else {
                valueAdder.accept((BlockBT) builder);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object valueAt(Block block, int position, BytesRef scratch) {
            // Build the list of values
            var values = new ArrayList<>();
            for (int i = block.getFirstValueIndex(position); i < block.getFirstValueIndex(position + 1); i++) {
                values.add(blockGetter.apply((BlockT) block, i, scratch));
                scratch = new BytesRef(); // do not overwrite previous value
            }
            return values.size() == 1 ? values.get(0) : values;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object valueAt(ValueVector arrowVec, int position) {
            if (arrowVec instanceof ListVector listVector) {
                var type = listVector.getField().getMetadata().get("elastic:type");
                // Build the list of values
                var valueVec = listVector.getDataVector();
                var values = new ArrayList<>();
                for (int i = listVector.getElementStartIndex(position); i < listVector.getElementEndIndex(position); i++) {
                    values.add(vectorGetter.apply((VectorT) valueVec, i));
                }
                return values.size() == 1 ? values.get(0) : values;
            } else {
                return vectorGetter.apply((VectorT) arrowVec, position);
            }
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
