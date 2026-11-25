/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.RowKeyHash;
import org.elasticsearch.compute.aggregation.table.RowInTableLookup;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Compares {@code ==} with the three grouping hashes. A disagreement is a known exception, pinned here, and is not
 * a change to the hash. Types with no {@code ==} evaluator are covered by {@link GroupableTypesTests}.
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
     * The {@code INLINE STATS} join looks keys up in a {@link RowInTableLookup} built from the grouped output
     * ({@code PackedValuesBlockHash}). Probing that output finds each grouped row again.
     */
    public void testInlineStatsLookupFindsGroupedRow() {
        BlockFactory blockFactory = blockFactory();
        for (DataType type : equalityTypes()) {
            ElementType elementType = PlannerUtils.toElementType(type);
            try (
                Block input = distinctKeyBlock(elementType, blockFactory);
                BlockHash hash = BlockHash.buildPackedValuesBlockHash(List.of(new BlockHash.GroupSpec(0, elementType)), blockFactory, 1024)
            ) {
                hash.add(new Page(input), ignoringGroups());
                try (IntVector nonEmpty = hash.nonEmpty()) {
                    Block[] grouped = hash.getKeys(nonEmpty);
                    try {
                        assertLookupFindsItsRows(type, blockFactory, grouped);
                    } finally {
                        Releasables.close(grouped);
                    }
                }
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

    private static boolean valuesEqual(DataType type, Object left, Object right) {
        Boolean result = (Boolean) foldEquals(type, left, right);
        assertNotNull(type.typeName(), result);
        return result;
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
        try (Block block = BlockUtils.fromList(factory, List.of(List.of(left), List.of(right)))[0]) {
            int[] ids = switch (kind) {
                case SINGLE -> groups(BlockHash.build(List.of(new BlockHash.GroupSpec(0, elementType)), factory, 1024, false), block);
                case ROW -> {
                    try (RowKeyHash hash = new RowKeyHash(new int[] { 0 }, List.of(elementType), factory)) {
                        yield hash.add(new Page(block));
                    }
                }
                case PACKED -> groups(
                    BlockHash.buildPackedValuesBlockHash(List.of(new BlockHash.GroupSpec(0, elementType)), factory, 1024),
                    block
                );
            };
            assertThat(ids[0], not(equalTo(-1)));
            assertThat(ids[1], not(equalTo(-1)));
            return ids[0] == ids[1];
        }
    }

    private static int[] groups(BlockHash hash, Block block) {
        try {
            int[] ids = new int[block.getPositionCount()];
            Arrays.fill(ids, -1);
            hash.add(new Page(block), new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {
                    copyGroups(positionOffset, groupIds, ids);
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {
                    copyGroups(positionOffset, groupIds, ids);
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    for (int i = 0; i < groupIds.getPositionCount(); i++) {
                        ids[positionOffset + i] = groupIds.getInt(i);
                    }
                }

                @Override
                public void close() {}
            });
            return ids;
        } finally {
            hash.close();
        }
    }

    private static void copyGroups(int positionOffset, IntBlock groupIds, int[] ids) {
        for (int p = 0; p < groupIds.getPositionCount(); p++) {
            if (groupIds.isNull(p)) {
                continue;
            }
            ids[positionOffset + p] = groupIds.getInt(groupIds.getFirstValueIndex(p));
        }
    }

    private static BlockFactory blockFactory() {
        return TestBlockFactory.getNonBreakingInstance();
    }

    private static void assertLookupFindsItsRows(DataType type, BlockFactory blockFactory, Block[] grouped) {
        Block keys = grouped[0];
        keys.incRef();
        Page probe = new Page(keys);
        try (RowInTableLookup lookup = RowInTableLookup.build(blockFactory, grouped)) {
            try (
                ReleasableIterator<IntBlock> rows = lookup.lookup(probe, blockFactory, ByteSizeValue.ofMb(1));
                IntBlock matches = rows.next()
            ) {
                assertThat(type.typeName(), matches.getPositionCount(), equalTo(keys.getPositionCount()));
                for (int p = 0; p < matches.getPositionCount(); p++) {
                    assertFalse(type.typeName() + " position " + p, matches.isNull(p));
                    assertThat(type.typeName(), matches.getValueCount(p), equalTo(1));
                    assertThat(type.typeName(), matches.getInt(matches.getFirstValueIndex(p)), equalTo(p));
                }
                assertFalse(rows.hasNext());
            } finally {
                probe.releaseBlocks();
            }
        }
    }

    private static GroupingAggregatorFunction.AddInput ignoringGroups() {
        return new GroupingAggregatorFunction.AddInput() {
            @Override
            public void add(int positionOffset, IntArrayBlock groupIds) {}

            @Override
            public void add(int positionOffset, IntBigArrayBlock groupIds) {}

            @Override
            public void add(int positionOffset, IntVector groupIds) {}

            @Override
            public void close() {}
        };
    }

    /**
     * Two different single values, or one null. The int pair is not an ascending run, so the lookup takes the
     * packed-values path rather than the ascending-int shortcut.
     */
    private static Block distinctKeyBlock(ElementType elementType, BlockFactory blockFactory) {
        return switch (elementType) {
            case NULL -> blockFactory.newConstantNullBlock(1);
            case BOOLEAN -> blockFactory.newBooleanBlockBuilder(2).appendBoolean(false).appendBoolean(true).build();
            case INT -> blockFactory.newIntBlockBuilder(2).appendInt(2).appendInt(1).build();
            case LONG -> blockFactory.newLongBlockBuilder(2).appendLong(2L).appendLong(1L).build();
            case DOUBLE -> blockFactory.newDoubleBlockBuilder(2).appendDouble(2.5).appendDouble(1.5).build();
            case BYTES_REF -> blockFactory.newBytesRefBlockBuilder(2)
                .appendBytesRef(new BytesRef("b"))
                .appendBytesRef(new BytesRef("a"))
                .build();
            case DOUBLE_RANGE -> blockFactory.newDoubleRangeBlockBuilder(2).appendDoubleRange(1, 3).appendDoubleRange(1, 2).build();
            case FLOAT, DOC, COMPOSITE, UNKNOWN, AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, TDIGEST, LONG_RANGE ->
                throw new AssertionError("not an INLINE STATS key [" + elementType + "]");
        };
    }
}
