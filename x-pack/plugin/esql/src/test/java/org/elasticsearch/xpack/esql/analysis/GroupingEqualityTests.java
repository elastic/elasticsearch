/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.RowKeyHash;
import org.elasticsearch.compute.aggregation.table.RowInTableLookup;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleRangeBlockBuilder.DoubleRange;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.expression.LoadFromPageEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Compares runtime {@code ==} with grouping, following the contract on {@link DataType#isGroupable()}.
 * A disagreement is a known exception, pinned here, and is not a change to the hash. Types with no
 * {@code ==} evaluator are covered for byte identity by the lookup round trip, and for eligibility by
 * {@link GroupableTypesTests}.
 */
public class GroupingEqualityTests extends ESTestCase {

    /**
     * Groupable types whose {@code ==} result can be the oracle. {@code null}, {@code _source} and {@code _tsid}
     * have no evaluator. {@code dense_vector} is a multivalue {@code ==} returns null for, and single-key
     * {@code STATS} cannot group it.
     */
    private static List<DataType> equalityTypes() {
        return Arrays.stream(DataType.values())
            .filter(DataType::isGroupable)
            .filter(type -> type != DataType.NULL && type != DataType.SOURCE && type != DataType.TSID_DATA_TYPE)
            .filter(type -> type != DataType.DENSE_VECTOR)
            .toList();
    }

    public void testSingleKeyBlockHashAgreesWithEquals() {
        agrees(BlockHashKind.SINGLE);
    }

    public void testRowKeyHashAgreesWithEquals() {
        agrees(BlockHashKind.ROW);
    }

    public void testPackedBlockHashAgreesWithEquals() {
        agrees(BlockHashKind.PACKED);
    }

    /** Infinities are ordinary values: {@code +Infinity == +Infinity}, and it is not {@code -Infinity}. */
    public void testInfinitiesAgreeWithEquals() {
        for (BlockHashKind kind : BlockHashKind.values()) {
            assertAgrees(DataType.DOUBLE, kind, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
            assertAgrees(DataType.DOUBLE, kind, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        }
    }

    /**
     * {@code version}, spatial types and {@code histogram} compare encoded bytes, so a second byte sequence is a
     * different key. A copy of the same bytes is the same key.
     */
    public void testEncodedBytesStayDistinct() {
        for (DataType type : List.of(DataType.VERSION, DataType.GEO_POINT, DataType.HISTOGRAM)) {
            BytesRef value = (BytesRef) ordinary(type);
            BytesRef copy = BytesRef.deepCopyOf(value);
            BytesRef flipped = BytesRef.deepCopyOf(value);
            assertThat(type.typeName(), flipped.length, not(equalTo(0)));
            flipped.bytes[flipped.offset] ^= 0x01;
            for (BlockHashKind kind : BlockHashKind.values()) {
                assertAgrees(type, kind, value, copy);
                assertAgrees(type, kind, value, flipped);
            }
            assertFalse(type.typeName(), valuesEqual(type, value, flipped));
        }
    }

    /**
     * A {@code double_range} bound uses {@link Double#compare(double, double)}, so {@code ==} matches single-key
     * {@code STATS}. The other hashes keep the raw bits and can split a bound those two treat as one value.
     * Tracked by https://github.com/elastic/elasticsearch/issues/167086
     */
    public void testDoubleRangeBoundsStayAsTheyAre() {
        DoubleRange positive = new DoubleRange(0.0d, 1.0d);
        DoubleRange negative = new DoubleRange(-0.0d, 1.0d);
        boolean sameBits = Double.doubleToLongBits(-0.0d) == Double.doubleToLongBits(0.0d);
        assertThat(valuesEqual(DataType.DOUBLE_RANGE, positive, negative), equalTo(Double.compare(0.0d, -0.0d) == 0));
        assertThat(sameGroup(DataType.DOUBLE_RANGE, positive, negative, BlockHashKind.SINGLE), equalTo(sameBits));
        assertFalse(sameGroup(DataType.DOUBLE_RANGE, positive, negative, BlockHashKind.ROW));
        assertFalse(sameGroup(DataType.DOUBLE_RANGE, positive, negative, BlockHashKind.PACKED));

        double otherNan = Double.longBitsToDouble(Double.doubleToRawLongBits(Double.NaN) | 1L);
        DoubleRange nan = new DoubleRange(Double.NaN, 1.0d);
        DoubleRange other = new DoubleRange(otherNan, 1.0d);
        assertTrue(valuesEqual(DataType.DOUBLE_RANGE, nan, other));
        assertTrue(sameGroup(DataType.DOUBLE_RANGE, nan, other, BlockHashKind.SINGLE));
        assertFalse(sameGroup(DataType.DOUBLE_RANGE, nan, other, BlockHashKind.ROW));
        assertFalse(sameGroup(DataType.DOUBLE_RANGE, nan, other, BlockHashKind.PACKED));
    }

    /**
     * Composite keys group when every column matches. {@code (1, a)} matches itself and not {@code (1, b)} or
     * {@code (2, a)}.
     */
    public void testCompositeKeyGroupsByEveryColumn() {
        BlockFactory factory = blockFactory();
        BytesRef a = new BytesRef("a");
        BytesRef b = new BytesRef("b");
        try (
            Block numbers = factory.newIntBlockBuilder(4).appendInt(1).appendInt(1).appendInt(1).appendInt(2).build();
            Block names = factory.newBytesRefBlockBuilder(4)
                .appendBytesRef(a)
                .appendBytesRef(a)
                .appendBytesRef(b)
                .appendBytesRef(a)
                .build();
            BlockHash hash = BlockHash.buildPackedValuesBlockHash(
                List.of(new BlockHash.GroupSpec(0, ElementType.INT), new BlockHash.GroupSpec(1, ElementType.BYTES_REF)),
                factory,
                1024
            )
        ) {
            int[][] ids = collect(hash, new Page(numbers, names), false);
            assertThat(ids[1][0], equalTo(ids[0][0]));
            assertThat(ids[2][0], not(equalTo(ids[0][0])));
            assertThat(ids[3][0], not(equalTo(ids[0][0])));
        }
    }

    /**
     * Multi-key {@code STATS} keeps {@code NaN} payloads and the sign of zero apart. {@code NaN == NaN} is false, so
     * the payloads agree with {@code ==}. {@code 0.0 == -0.0} is true, so the sign does not. Tracked by
     * https://github.com/elastic/elasticsearch/issues/167086
     */
    public void testCompositeKeyFloatBitsStayRaw() {
        BlockFactory factory = blockFactory();
        double otherNan = Double.longBitsToDouble(Double.doubleToRawLongBits(Double.NaN) | 1L);
        try (
            Block ones = factory.newIntBlockBuilder(2).appendInt(1).appendInt(1).build();
            Block nans = factory.newDoubleBlockBuilder(2).appendDouble(Double.NaN).appendDouble(otherNan).build();
            BlockHash nanHash = BlockHash.buildPackedValuesBlockHash(
                List.of(new BlockHash.GroupSpec(0, ElementType.INT), new BlockHash.GroupSpec(1, ElementType.DOUBLE)),
                factory,
                1024
            )
        ) {
            int[][] ids = collect(nanHash, new Page(ones, nans), false);
            assertThat(ids[1][0], not(equalTo(ids[0][0])));
        }
        assertFalse(valuesEqual(DataType.DOUBLE, Double.NaN, otherNan));
        try (
            Block ones = factory.newIntBlockBuilder(2).appendInt(1).appendInt(1).build();
            Block zeros = factory.newDoubleBlockBuilder(2).appendDouble(0.0d).appendDouble(-0.0d).build();
            BlockHash zeroHash = BlockHash.buildPackedValuesBlockHash(
                List.of(new BlockHash.GroupSpec(0, ElementType.INT), new BlockHash.GroupSpec(1, ElementType.DOUBLE)),
                factory,
                1024
            )
        ) {
            int[][] ids = collect(zeroHash, new Page(ones, zeros), false);
            assertThat(ids[1][0], not(equalTo(ids[0][0])));
        }
        assertTrue(valuesEqual(DataType.DOUBLE, 0.0d, -0.0d));
    }

    /**
     * {@code null == null} and {@code null == v} fold to null. A multivalue is not a boolean result either:
     * {@code ==} returns null, and the evaluator warns that it saw more than one value.
     */
    public void testNullAndMultivalueEquality() {
        for (DataType type : List.of(DataType.BOOLEAN, DataType.INTEGER, DataType.LONG, DataType.DOUBLE, DataType.KEYWORD)) {
            assertNull(type.typeName(), foldEquals(type, null, null));
            assertNull(type.typeName(), foldEquals(type, ordinary(type), null));
        }
        assertNull(foldEquals(DataType.INTEGER, List.of(1, 2), 1));
        assertWarnings(MULTIVALUE_EQUALS_WARNINGS);
        assertNull(foldEquals(DataType.INTEGER, List.of(1, 2), List.of(2, 1)));
        assertWarnings(MULTIVALUE_EQUALS_WARNINGS);
    }

    /**
     * Nulls share a group on every hash. {@code ==} of null is null, so this is not the equality oracle.
     */
    public void testNullsShareAGroup() {
        for (DataType type : List.of(DataType.BOOLEAN, DataType.INTEGER, DataType.LONG, DataType.DOUBLE, DataType.KEYWORD)) {
            assertNull(type.typeName(), evaluateEquals(type, null, null));
            Object value = ordinary(type);
            assertNull(type.typeName(), evaluateEquals(type, value, null));
            for (BlockHashKind kind : BlockHashKind.values()) {
                assertTrue(type.typeName() + " " + kind, sameGroup(type, null, null, kind));
                assertFalse(type.typeName() + " " + kind, sameGroup(type, value, null, kind));
            }
        }
    }

    /**
     * {@code STATS BY} gives a multivalue one group per distinct value. The position belongs to each of those
     * groups, which a single group id cannot represent. {@code ==} of a multivalue is null.
     */
    public void testStatsSplitsMultivalue() {
        for (BlockHashKind kind : List.of(BlockHashKind.SINGLE, BlockHashKind.PACKED)) {
            assertSplit(kind, intMultivalue(blockFactory()));
            assertSplit(kind, keywordMultivalue(blockFactory()));
        }
    }

    /**
     * {@code LIMIT BY} and {@code DEDUP} keep a multivalue whole and in order. {@code [1, 2]} matches itself and
     * not {@code [2, 1]} or {@code 1}.
     */
    public void testRowKeyKeepsMultivalueOrder() {
        assertWhole(intMultivalue(blockFactory()));
        assertWhole(keywordMultivalue(blockFactory()));
    }

    /**
     * {@code dense_vector} compares element by element, in order. {@link BlockHash} cannot group it, so
     * {@code STATS} and {@code INLINE STATS} reject it. {@code LIMIT BY} and {@code DEDUP} encode that order.
     */
    public void testDenseVectorRowKeyUsesElementOrder() {
        BlockFactory factory = blockFactory();
        List<Float> forward = List.of(1f, 2f);
        List<Float> reversed = List.of(2f, 1f);
        assertTrue(valuesEqual(DataType.DENSE_VECTOR, forward, forward));
        assertFalse(valuesEqual(DataType.DENSE_VECTOR, forward, reversed));
        try (Block block = floatVectors(factory); RowKeyHash hash = new RowKeyHash(new int[] { 0 }, List.of(ElementType.FLOAT), factory)) {
            int[] ids = hash.add(new Page(block));
            assertThat(ids[1], equalTo(ids[0]));
            assertThat(ids[2], not(equalTo(ids[0])));
        }
        expectThrows(
            IllegalArgumentException.class,
            () -> BlockHash.build(List.of(new BlockHash.GroupSpec(0, ElementType.FLOAT)), factory, 16, false).close()
        );
    }

    /**
     * The {@code INLINE STATS} join looks keys up in a {@link RowInTableLookup} built from the grouped output.
     * Probing the original rows, including a repeated key, finds the group that hashing assigned. The lookup
     * rejects a multivalued build key; {@code STATS} splits a multivalue before that join.
     */
    public void testLookupRoundTripFindsOriginalRows() {
        BlockFactory factory = blockFactory();
        for (DataType type : equalityTypes()) {
            ElementType elementType = PlannerUtils.toElementType(type);
            try (Block input = repeatedKeyBlock(elementType, factory)) {
                assertRoundTrip(
                    type.typeName() + " single",
                    input,
                    BlockHash.build(List.of(new BlockHash.GroupSpec(0, elementType)), factory, 1024, false)
                );
                assertRoundTrip(
                    type.typeName() + " packed",
                    input,
                    BlockHash.buildPackedValuesBlockHash(List.of(new BlockHash.GroupSpec(0, elementType)), factory, 1024)
                );
            }
        }
        for (DataType type : List.of(DataType.NULL, DataType.SOURCE, DataType.TSID_DATA_TYPE)) {
            ElementType elementType = PlannerUtils.toElementType(type);
            try (Block input = repeatedKeyBlock(elementType, factory)) {
                assertRoundTrip(
                    type.typeName() + " single",
                    input,
                    BlockHash.build(List.of(new BlockHash.GroupSpec(0, elementType)), factory, 1024, false)
                );
                assertRoundTrip(
                    type.typeName() + " packed",
                    input,
                    BlockHash.buildPackedValuesBlockHash(List.of(new BlockHash.GroupSpec(0, elementType)), factory, 1024)
                );
            }
        }
    }

    /**
     * Records the current {@code NaN} and signed-zero grouping, which disagrees with {@code ==}.
     * Single-key {@code STATS} hashes with {@link Double#doubleToLongBits}, so {@code NaN} payloads share a group.
     * {@code LIMIT BY}, {@code DEDUP} and the packed hash keep the raw bits, so those payloads do not. The sign of
     * zero follows the same split. Tracked by https://github.com/elastic/elasticsearch/issues/167086
     */
    public void testNanAndSignedZeroStayAsTheyAre() {
        double otherNan = Double.longBitsToDouble(Double.doubleToRawLongBits(Double.NaN) | 1L);
        assertFalse(valuesEqual(DataType.DOUBLE, Double.NaN, otherNan));
        assertTrue(sameGroup(BlockHashKind.SINGLE, Double.NaN, otherNan));
        assertFalse(sameGroup(BlockHashKind.ROW, Double.NaN, otherNan));
        assertFalse(sameGroup(BlockHashKind.PACKED, Double.NaN, otherNan));

        assertTrue(valuesEqual(DataType.DOUBLE, 0.0d, -0.0d));
        assertThat(
            "single-key STATS follows doubleToLongBits",
            sameGroup(BlockHashKind.SINGLE, 0.0d, -0.0d),
            equalTo(Double.doubleToLongBits(0.0d) == Double.doubleToLongBits(-0.0d))
        );
        assertFalse(sameGroup(BlockHashKind.ROW, 0.0d, -0.0d));
        assertFalse(sameGroup(BlockHashKind.PACKED, 0.0d, -0.0d));
    }

    private void agrees(BlockHashKind kind) {
        for (DataType type : equalityTypes()) {
            for (int i = 0; i < 8; i++) {
                Object left = ordinary(type);
                Object right = ordinary(type);
                assertAgrees(type, kind, left, left);
                assertAgrees(type, kind, left, right);
            }
        }
    }

    /** A value {@code ==} can judge. {@code NaN} and {@code -0.0} are pinned separately. */
    private static Object ordinary(DataType type) {
        for (int i = 0; i < 20; i++) {
            Object value = EsqlTestUtils.randomLiteralValue(type);
            if (value instanceof Double d
                && (Double.isFinite(d) == false || Double.doubleToRawLongBits(d) == Double.doubleToRawLongBits(-0.0d))) {
                continue;
            }
            return value;
        }
        throw new AssertionError(type);
    }

    private static void assertAgrees(DataType type, BlockHashKind kind, Object left, Object right) {
        boolean equal = valuesEqual(type, left, right);
        assertThat(type.typeName() + " " + kind + " [" + left + "] [" + right + "]", sameGroup(type, left, right, kind), equalTo(equal));
    }

    private static final String[] MULTIVALUE_EQUALS_WARNINGS = new String[] {
        "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
        "Line -1:-1: java.lang.IllegalArgumentException: single-value function encountered multi-value" };

    private enum BlockHashKind {
        /** Single-key {@code STATS}, {@link BlockHash}. */
        SINGLE,
        /** {@code LIMIT BY} and {@code DEDUP}, {@link RowKeyHash}. */
        ROW,
        /** Multi-key {@code STATS} and the {@code INLINE STATS} join, {@link BlockHash#buildPackedValuesBlockHash}. */
        PACKED
    }

    /** Runtime {@code ==} on one page of blocks. Constant folding must report the same boolean. */
    private static boolean valuesEqual(DataType type, Object left, Object right) {
        Boolean runtime = evaluateEquals(type, left, right);
        assertNotNull(type.typeName(), runtime);
        assertThat(type.typeName() + " fold", foldEquals(type, left, right), equalTo(runtime));
        return runtime;
    }

    private static Boolean evaluateEquals(DataType type, Object left, Object right) {
        BlockFactory factory = blockFactory();
        DriverContext context = new DriverContext(factory.bigArrays(), factory, null);
        Equals equals = new Equals(Source.EMPTY, new ChannelValue(type, 0), new ChannelValue(type, 1));
        try (
            Block lhs = valueBlock(factory, left);
            Block rhs = valueBlock(factory, right);
            ExpressionEvaluator evaluator = equals.toEvaluator(new EvaluatorMapper.ToEvaluator() {
                @Override
                public ExpressionEvaluator.Factory apply(Expression expression) {
                    return ((EvaluatorMapper) expression).toEvaluator(this);
                }

                @Override
                public FoldContext foldCtx() {
                    return FoldContext.small();
                }
            }).get(context);
            Block result = evaluator.eval(new Page(lhs, rhs))
        ) {
            BooleanBlock booleans = (BooleanBlock) result;
            if (booleans.isNull(0) || booleans.getValueCount(0) != 1) {
                return null;
            }
            return booleans.getBoolean(booleans.getFirstValueIndex(0));
        }
    }

    private static Object foldEquals(DataType type, Object left, Object right) {
        return new Equals(Source.EMPTY, new Literal(Source.EMPTY, left, type), new Literal(Source.EMPTY, right, type)).fold(
            FoldContext.small()
        );
    }

    private static boolean sameGroup(BlockHashKind kind, double left, double right) {
        return sameGroup(DataType.DOUBLE, left, right, kind);
    }

    private static boolean sameGroup(DataType type, Object left, Object right, BlockHashKind kind) {
        BlockFactory factory = blockFactory();
        ElementType elementType = PlannerUtils.toElementType(type);
        try (Block block = pairBlock(factory, left, right)) {
            if (kind == BlockHashKind.ROW) {
                try (RowKeyHash hash = new RowKeyHash(new int[] { 0 }, List.of(elementType), factory)) {
                    int[] ids = hash.add(new Page(block));
                    return ids[0] == ids[1];
                }
            }
            BlockHash hash = kind == BlockHashKind.SINGLE
                ? BlockHash.build(List.of(new BlockHash.GroupSpec(0, elementType)), factory, 1024, false)
                : BlockHash.buildPackedValuesBlockHash(List.of(new BlockHash.GroupSpec(0, elementType)), factory, 1024);
            int[][] ids = collect(hash, new Page(block), true);
            assertThat(ids[0].length, equalTo(1));
            assertThat(ids[1].length, equalTo(1));
            return ids[0][0] == ids[1][0];
        }
    }

    /** Every group id of every position. A {@code STATS} multivalue has more than one. */
    private static int[][] collect(BlockHash hash, Page page, boolean closeHash) {
        int count = page.getPositionCount();
        List<List<Integer>> found = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            found.add(new ArrayList<>());
        }
        try {
            hash.add(page, new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {
                    take(positionOffset, groupIds, found);
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {
                    take(positionOffset, groupIds, found);
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    for (int i = 0; i < groupIds.getPositionCount(); i++) {
                        found.get(positionOffset + i).add(groupIds.getInt(i));
                    }
                }

                @Override
                public void close() {}
            });
            int[][] ids = new int[count][];
            for (int i = 0; i < count; i++) {
                ids[i] = found.get(i).stream().mapToInt(Integer::intValue).toArray();
            }
            return ids;
        } finally {
            if (closeHash) {
                hash.close();
            }
        }
    }

    private static void take(int positionOffset, IntBlock groupIds, List<List<Integer>> found) {
        for (int p = 0; p < groupIds.getPositionCount(); p++) {
            if (groupIds.isNull(p)) {
                continue;
            }
            int start = groupIds.getFirstValueIndex(p);
            int values = groupIds.getValueCount(p);
            for (int v = 0; v < values; v++) {
                found.get(positionOffset + p).add(groupIds.getInt(start + v));
            }
        }
    }

    private static BlockFactory blockFactory() {
        return TestBlockFactory.getNonBreakingInstance();
    }

    /**
     * Positions are {@code [1, 2]}, {@code [2, 1]}, {@code 1}, {@code 2}, {@code [1, 1]}. {@code STATS} splits the
     * lists. The repeated {@code 1} is one group.
     */
    private static Block intMultivalue(BlockFactory factory) {
        return factory.newIntBlockBuilder(6)
            .beginPositionEntry()
            .appendInt(1)
            .appendInt(2)
            .endPositionEntry()
            .beginPositionEntry()
            .appendInt(2)
            .appendInt(1)
            .endPositionEntry()
            .appendInt(1)
            .appendInt(2)
            .beginPositionEntry()
            .appendInt(1)
            .appendInt(1)
            .endPositionEntry()
            .beginPositionEntry()
            .appendInt(1)
            .appendInt(2)
            .endPositionEntry()
            .build();
    }

    /** The byte-key counterpart of {@link #intMultivalue}. */
    private static Block keywordMultivalue(BlockFactory factory) {
        BytesRef a = new BytesRef("a");
        BytesRef b = new BytesRef("b");
        return factory.newBytesRefBlockBuilder(6)
            .beginPositionEntry()
            .appendBytesRef(a)
            .appendBytesRef(b)
            .endPositionEntry()
            .beginPositionEntry()
            .appendBytesRef(b)
            .appendBytesRef(a)
            .endPositionEntry()
            .appendBytesRef(a)
            .appendBytesRef(b)
            .beginPositionEntry()
            .appendBytesRef(a)
            .appendBytesRef(a)
            .endPositionEntry()
            .beginPositionEntry()
            .appendBytesRef(a)
            .appendBytesRef(b)
            .endPositionEntry()
            .build();
    }

    private static void assertSplit(BlockHashKind kind, Block block) {
        ElementType elementType = block.elementType();
        BlockFactory factory = blockFactory();
        try (block) {
            BlockHash hash = kind == BlockHashKind.SINGLE
                ? BlockHash.build(List.of(new BlockHash.GroupSpec(0, elementType)), factory, 1024, false)
                : BlockHash.buildPackedValuesBlockHash(List.of(new BlockHash.GroupSpec(0, elementType)), factory, 1024);
            int[][] ids = collect(hash, new Page(block), true);
            assertThat(set(ids[0]), equalTo(set(ids[2][0], ids[3][0])));
            assertThat(set(ids[1]), equalTo(set(ids[0])));
            assertThat(set(ids[5]), equalTo(set(ids[0])));
            assertThat(ids[2][0], not(equalTo(ids[3][0])));
            assertThat(ids[4].length, equalTo(1));
            assertThat(ids[4][0], equalTo(ids[2][0]));
        }
    }

    private static void assertWhole(Block block) {
        try (block; RowKeyHash hash = new RowKeyHash(new int[] { 0 }, List.of(block.elementType()), blockFactory())) {
            int[] ids = hash.add(new Page(block));
            assertThat(ids[5], equalTo(ids[0]));
            assertThat(ids[0], not(equalTo(ids[1])));
            assertThat(ids[0], not(equalTo(ids[2])));
            assertThat(ids[4], not(equalTo(ids[2])));
        }
    }

    /** {@code [1, 2]}, the same vector again, then {@code [2, 1]}. */
    private static Block floatVectors(BlockFactory factory) {
        return factory.newFloatBlockBuilder(6)
            .beginPositionEntry()
            .appendFloat(1)
            .appendFloat(2)
            .endPositionEntry()
            .beginPositionEntry()
            .appendFloat(1)
            .appendFloat(2)
            .endPositionEntry()
            .beginPositionEntry()
            .appendFloat(2)
            .appendFloat(1)
            .endPositionEntry()
            .build();
    }

    private static void assertRoundTrip(String label, Block input, BlockHash hash) {
        try {
            int[][] ids = collect(hash, new Page(input), false);
            try (IntVector nonEmpty = hash.nonEmpty()) {
                Block[] grouped = hash.getKeys(nonEmpty);
                try {
                    input.incRef();
                    Page probe = new Page(input);
                    try (RowInTableLookup lookup = RowInTableLookup.build(blockFactory(), grouped)) {
                        try (
                            ReleasableIterator<IntBlock> rows = lookup.lookup(probe, blockFactory(), ByteSizeValue.ofMb(1));
                            IntBlock matches = rows.next()
                        ) {
                            assertThat(label, matches.getPositionCount(), equalTo(input.getPositionCount()));
                            for (int p = 0; p < matches.getPositionCount(); p++) {
                                assertFalse(label + " position " + p, matches.isNull(p));
                                assertThat(label, ids[p].length, equalTo(1));
                                int ordinal = matches.getInt(matches.getFirstValueIndex(p));
                                assertThat(label + " position " + p, nonEmpty.getInt(ordinal), equalTo(ids[p][0]));
                            }
                            assertFalse(rows.hasNext());
                        } finally {
                            probe.releaseBlocks();
                        }
                    }
                } finally {
                    Releasables.close(grouped);
                }
            }
        } finally {
            hash.close();
        }
    }

    /**
     * Three positions, the first value repeated, and not an ascending int run. Null is three nulls.
     * {@code _source} and {@code _tsid} are bytes.
     */
    private static Block repeatedKeyBlock(ElementType elementType, BlockFactory blockFactory) {
        return switch (elementType) {
            case NULL -> blockFactory.newConstantNullBlock(3);
            case BOOLEAN -> blockFactory.newBooleanBlockBuilder(3).appendBoolean(false).appendBoolean(true).appendBoolean(false).build();
            case INT -> blockFactory.newIntBlockBuilder(3).appendInt(2).appendInt(1).appendInt(2).build();
            case LONG -> blockFactory.newLongBlockBuilder(3).appendLong(2L).appendLong(1L).appendLong(2L).build();
            case DOUBLE -> blockFactory.newDoubleBlockBuilder(3).appendDouble(2.5).appendDouble(1.5).appendDouble(2.5).build();
            case BYTES_REF -> blockFactory.newBytesRefBlockBuilder(3)
                .appendBytesRef(new BytesRef("b"))
                .appendBytesRef(new BytesRef("a"))
                .appendBytesRef(new BytesRef("b"))
                .build();
            case DOUBLE_RANGE -> blockFactory.newDoubleRangeBlockBuilder(3)
                .appendDoubleRange(1, 3)
                .appendDoubleRange(1, 2)
                .appendDoubleRange(1, 3)
                .build();
            case FLOAT, DOC, COMPOSITE, UNKNOWN, AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, TDIGEST, LONG_RANGE ->
                throw new AssertionError("not an INLINE STATS key [" + elementType + "]");
        };
    }

    private static Block valueBlock(BlockFactory factory, Object value) {
        if (value == null) {
            return factory.newConstantNullBlock(1);
        }
        return BlockUtils.fromList(factory, List.of(List.of(value)))[0];
    }

    private static Block pairBlock(BlockFactory factory, Object left, Object right) {
        if (left == null && right == null) {
            return factory.newConstantNullBlock(2);
        }
        List<Object> leftRow = left == null ? Arrays.asList(new Object[] { null }) : List.of(left);
        List<Object> rightRow = right == null ? Arrays.asList(new Object[] { null }) : List.of(right);
        return BlockUtils.fromList(factory, List.of(leftRow, rightRow))[0];
    }

    private static Set<Integer> set(int... values) {
        Set<Integer> set = new HashSet<>();
        for (int value : values) {
            set.add(value);
        }
        return set;
    }

    /** Reads one channel of the page, so {@code ==} runs on the block rather than a folded literal. */
    private static final class ChannelValue extends Expression implements EvaluatorMapper {
        private final DataType dataType;
        private final int channel;

        private ChannelValue(DataType dataType, int channel) {
            this(Source.EMPTY, dataType, channel);
        }

        private ChannelValue(Source source, DataType dataType, int channel) {
            super(source, List.of());
            this.dataType = dataType;
            this.channel = channel;
        }

        @Override
        public DataType dataType() {
            return dataType;
        }

        @Override
        public Nullability nullable() {
            return Nullability.TRUE;
        }

        @Override
        protected NodeInfo<ChannelValue> info() {
            return NodeInfo.create(this, ChannelValue::new, dataType, channel);
        }

        @Override
        public Expression replaceChildren(List<Expression> newChildren) {
            return this;
        }

        @Override
        public String getWriteableName() {
            return "channel-for-test";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
            return new LoadFromPageEvaluator.Factory(channel);
        }
    }
}
