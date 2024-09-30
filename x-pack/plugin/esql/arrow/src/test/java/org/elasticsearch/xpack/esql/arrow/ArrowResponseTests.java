/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.arrow;

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
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVectorBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xpack.versionfield.Version;
import org.junit.AfterClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ArrowResponseTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.getInstance(
        new NoopCircuitBreaker("test-noop"),
        BigArrays.NON_RECYCLING_INSTANCE
    );

    private static final RootAllocator ALLOCATOR = new RootAllocator();

    @AfterClass
    public static void afterClass() throws Exception {
        ALLOCATOR.close();
    }

    // ---------------------------------------------------------------------------------------------
    // Value creation, getters for ESQL and Arrow

    static final ValueType INTEGER_VALUES = new ValueTypeImpl<IntBlock.Builder, IntBlock, IntVector>(
        factory -> factory.newIntBlockBuilder(0),
        block -> block.appendInt(randomInt()),
        (block, i, scratch) -> block.getInt(i),
        IntVector::get
    );

    static final ValueType LONG_VALUES = new ValueTypeImpl<LongBlock.Builder, LongBlock, BigIntVector>(
        factory -> factory.newLongBlockBuilder(0),
        block -> block.appendLong(randomLong()),
        (block, i, scratch) -> block.getLong(i),
        BigIntVector::get
    );

    static final ValueType ULONG_VALUES = new ValueTypeImpl<LongBlock.Builder, LongBlock, UInt8Vector>(
        factory -> factory.newLongBlockBuilder(0),
        block -> block.appendLong(randomLong()),
        (block, i, scratch) -> block.getLong(i),
        UInt8Vector::get
    );

    static final ValueType DATE_VALUES = new ValueTypeImpl<LongBlock.Builder, LongBlock, TimeStampMilliVector>(
        factory -> factory.newLongBlockBuilder(0),
        block -> block.appendLong(randomLong()),
        (block, i, scratch) -> block.getLong(i),
        TimeStampMilliVector::get
    );

    static final ValueType DOUBLE_VALUES = new ValueTypeImpl<DoubleBlock.Builder, DoubleBlock, Float8Vector>(
        factory -> factory.newDoubleBlockBuilder(0),
        block -> block.appendDouble(randomDouble()),
        (block, i, scratch) -> block.getDouble(i),
        Float8Vector::get
    );

    static final ValueType BOOLEAN_VALUES = new ValueTypeImpl<BooleanBlock.Builder, BooleanBlock, BitVector>(
        factory -> factory.newBooleanBlockBuilder(0),
        block -> block.appendBoolean(randomBoolean()),
        (b, i, s) -> b.getBoolean(i),
        (v, i) -> v.get(i) != 0 // Arrow's BitVector returns 0 or 1
    );

    static final ValueType TEXT_VALUES = new ValueTypeImpl<BytesRefBlock.Builder, BytesRefBlock, VarCharVector>(
        factory -> factory.newBytesRefBlockBuilder(0),
        block -> block.appendBytesRef(new BytesRef("🚀" + randomAlphaOfLengthBetween(1, 20))),
        (b, i, s) -> b.getBytesRef(i, s).utf8ToString(),
        (v, i) -> new String(v.get(i), StandardCharsets.UTF_8)
    );

    static final ValueType SOURCE_VALUES = new ValueTypeImpl<BytesRefBlock.Builder, BytesRefBlock, VarCharVector>(
        factory -> factory.newBytesRefBlockBuilder(0),
        // Use a constant value, conversion is tested separately
        block -> block.appendBytesRef(new BytesRef("{\"foo\": 42}")),
        (b, i, s) -> b.getBytesRef(i, s).utf8ToString(),
        (v, i) -> new String(v.get(i), StandardCharsets.UTF_8)
    );

    static final ValueType IP_VALUES = new ValueTypeImpl<BytesRefBlock.Builder, BytesRefBlock, VarBinaryVector>(
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
        factory -> factory.newBytesRefBlockBuilder(0),
        block -> block.appendBytesRef(new BytesRef(randomByteArrayOfLength(randomIntBetween(1, 100)))),
        BytesRefBlock::getBytesRef,
        (v, i) -> new BytesRef(v.get(i))
    );

    static final ValueType VERSION_VALUES = new ValueTypeImpl<BytesRefBlock.Builder, BytesRefBlock, VarCharVector>(
        factory -> factory.newBytesRefBlockBuilder(0),
        block -> block.appendBytesRef(new Version(between(0, 100) + "." + between(0, 100) + "." + between(0, 100)).toBytesRef()),
        (b, i, s) -> new Version(b.getBytesRef(i, s)).toString(),
        (v, i) -> new String(v.get(i), StandardCharsets.UTF_8)
    );

    static final ValueType NULL_VALUES = new ValueTypeImpl<Block.Builder, Block, FieldVector>(
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
        TestBlock denseBlock = TestBlock.create(BLOCK_FACTORY, testColumn, Density.Dense, 3);
        TestBlock sparseBlock = TestBlock.create(BLOCK_FACTORY, testColumn, Density.Sparse, 5);
        TestBlock emptyBlock = TestBlock.create(BLOCK_FACTORY, testColumn, Density.Empty, 7);

        // Test that density works as expected
        assertTrue(denseBlock.block instanceof IntVectorBlock);
        assertEquals("IntArrayBlock", sparseBlock.block.getClass().getSimpleName()); // non-public class
        assertEquals("ConstantNullBlock", emptyBlock.block.getClass().getSimpleName());

        // Test that values iterator scans all pages
        List<TestPage> pages = Stream.of(denseBlock, sparseBlock, emptyBlock).map(b -> new TestPage(List.of(b))).toList();
        TestCase tc = new TestCase(List.of(testColumn), pages);
        EsqlValuesIterator valuesIterator = new EsqlValuesIterator(tc, 0);
        int count = 0;
        while (valuesIterator.hasNext()) {
            valuesIterator.next();
            count++;
        }
        assertEquals(3 + 5 + 7, count);

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
            TestColumn testColumn = new TestColumn("foo", type, VALUE_TYPES.get(type));
            List<TestPage> pages = new ArrayList<>();

            for (var density : Density.values()) {
                TestBlock testBlock = TestBlock.create(BLOCK_FACTORY, testColumn, density, 10);
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

        TestColumn testColumn = new TestColumn("foo", type, VALUE_TYPES.get(type));
        List<TestPage> pages = new ArrayList<>();

        TestBlock testBlock = TestBlock.create(BLOCK_FACTORY, testColumn, density, 10);
        TestPage testPage = new TestPage(List.of(testBlock));
        pages.add(testPage);

        TestCase testCase = new TestCase(List.of(testColumn), pages);

        compareEsqlAndArrow(testCase);
    }

    /**
     * Test that multivalued arrays are rejected
     */
    public void testMultivaluedField() throws IOException {
        IntBlock.Builder builder = BLOCK_FACTORY.newIntBlockBuilder(0);
        builder.appendInt(42);
        builder.appendNull();
        builder.beginPositionEntry();
        builder.appendInt(44);
        builder.appendInt(45);
        builder.endPositionEntry();
        builder.appendInt(46);
        IntBlock block = builder.build();

        // Consistency check
        assertTrue(block.mayHaveMultivaluedFields());
        assertEquals(0, block.getFirstValueIndex(0));
        assertEquals(1, block.getValueCount(0));

        // null values still use one position in the array
        assertEquals(0, block.getValueCount(1));
        assertEquals(1, block.getFirstValueIndex(1));
        assertTrue(block.isNull(1));
        assertEquals(0, block.getInt(1));

        assertEquals(2, block.getFirstValueIndex(2));
        assertEquals(2, block.getValueCount(2));
        assertEquals(2, block.getFirstValueIndex(2));
        assertEquals(45, block.getInt(block.getFirstValueIndex(2) + 1));

        assertEquals(4, block.getFirstValueIndex(3));

        var column = TestColumn.create("some-field", "integer");
        TestCase testCase = new TestCase(List.of(column), List.of(new TestPage(List.of(new TestBlock(column, block, Density.Dense)))));

        IllegalArgumentException exc = assertThrows(IllegalArgumentException.class, () -> compareEsqlAndArrow(testCase));

        assertEquals("ES|QL response field [some-field] is multi-valued. This isn't supported yet by the Arrow format", exc.getMessage());

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
            .mapToObj(i -> TestPage.create(BLOCK_FACTORY, columns))
            .toList();

        TestCase testCase = new TestCase(columns, pages);
        // System.out.println(testCase);
        // for (TestPage page: pages) {
        // System.out.println(page);
        // }

        compareEsqlAndArrow(testCase);
    }

    // ---------------------------------------------------------------------------------------------
    // Test harness

    private void compareEsqlAndArrow(TestCase testCase) throws IOException {
        try (VectorSchemaRoot arrowVectors = toArrowVectors(testCase)) {
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

            while (esqlValuesIterator.hasNext() && arrowValuesIterator.hasNext()) {
                assertEquals(esqlValuesIterator.next(), arrowValuesIterator.next());
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

        BytesReference bytes = serializeBlocksDirectly(response);
        try (
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
        private final BytesRef scratch = new BytesRef();
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
            Object result = block.isNull(position) ? null : type.valueAt(block, position, scratch);
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

    private BytesReference serializeBlocksDirectly(ArrowResponse body) throws IOException {
        // Ensure there's a single part, this will fail if we ever change it.
        assertTrue(body.isLastPart());

        List<BytesReference> ourEncoding = new ArrayList<>();
        int page = 0;
        while (body.isPartComplete() == false) {
            ourEncoding.add(body.encodeChunk(1500, BytesRefRecycler.NON_RECYCLING_INSTANCE));
            page++;
        }
        return CompositeBytesReference.of(ourEncoding.toArray(BytesReference[]::new));
    }

    record TestCase(List<TestColumn> columns, List<TestPage> pages) {
        @Override
        public String toString() {
            return pages.size() + " pages of " + columns.stream().map(TestColumn::type).collect(Collectors.joining("|"));
        }
    }

    record TestColumn(String name, String type, ValueType valueType) {
        static TestColumn create(String name, String type) {
            return new TestColumn(name, type, VALUE_TYPES.get(type));
        }
    }

    record TestPage(List<TestBlock> blocks) {

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

    record TestBlock(TestColumn column, Block block, Density density) {

        static TestBlock create(BlockFactory factory, TestColumn column, int positions) {
            return create(factory, column, randomFrom(Density.values()), positions);
        }

        static TestBlock create(BlockFactory factory, TestColumn column, Density density, int positions) {
            ValueType valueType = column.valueType();
            Block block;
            if (density == Density.Empty) {
                block = factory.newConstantNullBlock(positions);
            } else {
                Block.Builder builder = valueType.createBlockBuilder(factory);
                int start = 0;
                if (density == Density.Sparse && positions >= 2) {
                    // Make sure it's really sparse even if randomness of values may decide otherwise
                    valueType.addValue(builder, Density.Dense);
                    valueType.addValue(builder, Density.Empty);
                    start = 2;
                }
                for (int i = start; i < positions; i++) {
                    valueType.addValue(builder, density);
                }
                // Will create an ArrayBlock if there are null values, VectorBlock otherwise
                block = builder.build();
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
        private final Function<BlockFactory, BlockBT> builderCreator;
        private final Consumer<BlockBT> valueAdder;
        private final TriFunction<BlockT, Integer, BytesRef, Object> blockGetter;
        private final BiFunction<VectorT, Integer, Object> vectorGetter;

        public ValueTypeImpl(
            Function<BlockFactory, BlockBT> builderCreator,
            Consumer<BlockBT> valueAdder,
            TriFunction<BlockT, Integer, BytesRef, Object> blockGetter,
            BiFunction<VectorT, Integer, Object> vectorGetter
        ) {
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
            return blockGetter.apply((BlockT) block, position, scratch);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object valueAt(ValueVector arrowVec, int position) {
            return vectorGetter.apply((VectorT) arrowVec, position);
        }
    }
}
