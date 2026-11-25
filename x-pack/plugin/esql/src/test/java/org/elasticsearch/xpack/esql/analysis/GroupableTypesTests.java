/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.RowKeyHash;
import org.elasticsearch.compute.aggregation.table.RowInTableLookup;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Enforces that which types can be a grouping key is decided by {@link DataType#isGroupable} alone: for every
 * data type, the verifier outcome of STATS BY, INLINE STATS BY, LIMIT BY and DEDUP must match the property, and
 * the compute engine must be able to build a grouping for every groupable type, so the property can never claim
 * comparability the engine cannot deliver.
 */
public class GroupableTypesTests extends AnalyzerTestCase {

    public GroupableTypesTests(VersionMode versionMode) {
        super(versionMode);
    }

    /**
     * Types that cannot reach a grouping as themselves: unsupported and object fields resolve to
     * {@code UnsupportedAttribute} or to their subfields, small numeric types are widened on load, and
     * date_period, time_duration and null exist only as literals (pinned by the literal tests in
     * {@code VerifierTests} and {@link #testNullIsGroupable}).
     */
    private static final Set<DataType> NEVER_A_GROUPING = Set.of(
        DataType.UNSUPPORTED,
        DataType.OBJECT,
        DataType.SHORT,
        DataType.BYTE,
        DataType.FLOAT,
        DataType.HALF_FLOAT,
        DataType.SCALED_FLOAT,
        DataType.NULL,
        DataType.DATE_PERIOD,
        DataType.TIME_DURATION
    );

    public void testStatsByTypeMatchesIsGroupable() {
        for (DataType type : matrixTypes()) {
            boolean accepted = type.isGroupable() && type != DataType.DENSE_VECTOR;
            check(type, "FROM t | STATS c = count(*) BY f", accepted, groupingError(type));
        }
    }

    public void testInlineStatsByTypeMatchesIsGroupable() {
        assumeTrue("INLINE STATS must be enabled", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        for (DataType type : matrixTypes()) {
            boolean accepted = type.isGroupable() && type != DataType.DENSE_VECTOR;
            check(type, "FROM t | INLINE STATS c = count(*) BY f", accepted, groupingError(type));
        }
    }

    public void testLimitByTypeMatchesIsGroupable() {
        for (DataType type : matrixTypes()) {
            check(type, "FROM t | LIMIT 1 BY f", type.isGroupable(), groupingError(type));
        }
    }

    public void testDedupTypeMatchesIsGroupable() {
        assumeTrue("requires DEDUP", EsqlCapabilities.Cap.DEDUP_COMMAND.isEnabled());
        for (DataType type : matrixTypes()) {
            // _doc is never a DEDUP key, so DEDUP ignores it instead of rejecting it.
            boolean accepted = type.isGroupable() || type == DataType.DOC_DATA_TYPE;
            check(type, "FROM t | DEDUP", accepted, dedupError(type));
        }
    }

    /**
     * A counter field is not groupable; {@link #testStatsByTypeMatchesIsGroupable} and the other matrix tests
     * reject the direct field. A counter reached indirectly is still accepted: {@code RENAME} and
     * {@code BY c = <counter>} are not a direct field, and those queries group today. Making counters
     * groupable is a follow-up of <a href="https://github.com/elastic/elasticsearch/issues/163024">#163024</a>.
     */
    public void testIndirectCounterStaysAccepted() {
        for (DataType type : List.of(DataType.COUNTER_LONG, DataType.COUNTER_INTEGER, DataType.COUNTER_DOUBLE)) {
            check(type, "FROM t | RENAME f AS c | STATS x = count(*) BY c", true, "");
            check(type, "FROM t | STATS x = count(*) BY c = f", true, "");
            // LIMIT BY keys are expressions, not aliases, so RENAME is the indirect form.
            check(type, "FROM t | RENAME f AS c | LIMIT 1 BY c", true, "");
            if (EsqlCapabilities.Cap.INLINE_STATS.isEnabled()) {
                check(type, "FROM t | RENAME f AS c | INLINE STATS x = count(*) BY c", true, "");
                check(type, "FROM t | INLINE STATS x = count(*) BY c = f", true, "");
            }
            if (EsqlCapabilities.Cap.DEDUP_COMMAND.isEnabled()) {
                check(type, "FROM t | RENAME f AS c | DEDUP", true, "");
            }
        }
    }

    /**
     * LIMIT BY is GA, so a type that is groupable today must stay groupable: this pins the set as a lower
     * bound. Adding types is fine; removing one rejects queries that work today.
     */
    public void testGroupableSetOnlyGrows() {
        Set<DataType> groupable = Set.of(
            DataType.NULL,
            DataType.BOOLEAN,
            DataType.LONG,
            DataType.INTEGER,
            DataType.UNSIGNED_LONG,
            DataType.DOUBLE,
            DataType.KEYWORD,
            DataType.TEXT,
            DataType.DATETIME,
            DataType.DATE_NANOS,
            DataType.DOUBLE_RANGE,
            DataType.IP,
            DataType.VERSION,
            DataType.SOURCE,
            DataType.GEO_POINT,
            DataType.CARTESIAN_POINT,
            DataType.CARTESIAN_SHAPE,
            DataType.GEO_SHAPE,
            DataType.GEOHASH,
            DataType.GEOTILE,
            DataType.GEOHEX,
            DataType.TSID_DATA_TYPE,
            DataType.HISTOGRAM,
            DataType.DENSE_VECTOR,
            DataType.FLATTENED
        );
        for (DataType type : groupable) {
            assertTrue("[" + type.typeName() + "] was groupable and the set can only grow", type.isGroupable());
        }
    }

    /**
     * Groupable without {@code ==}. An indirect counter is accepted as a grouping expression even though the
     * counter types themselves are not groupable; that exception is {@link #testIndirectCounterStaysAccepted}.
     */
    private static final Set<DataType> GROUPABLE_WITHOUT_EQUALS = Set.of(DataType.NULL, DataType.SOURCE, DataType.TSID_DATA_TYPE);

    /**
     * {@code ==} exists, and the type stays non-groupable. Making {@code date_range} groupable is a follow-up.
     */
    private static final Set<DataType> EQUALS_WITHOUT_GROUPING = Set.of(
        DataType.DATE_RANGE,
        DataType.EXPONENTIAL_HISTOGRAM,
        DataType.TDIGEST
    );

    /**
     * Scalars whose {@code ==} result is the oracle for the single-key {@link BlockHash}, {@link RowKeyHash}
     * and the packed hash. {@code NaN} and {@code -0.0} are kept out of here and pinned by
     * {@link #testNanAndSignedZeroStayAsTheyAre}.
     */
    private static final List<DataType> EQUALS_ORACLE_TYPES = List.of(
        DataType.BOOLEAN,
        DataType.INTEGER,
        DataType.LONG,
        DataType.DOUBLE,
        DataType.KEYWORD
    );

    /**
     * Every groupable type has an {@code ==} evaluator, except the documented ones, and every type that has
     * {@code ==} but is not groupable is one of the pinned exceptions. Type resolution is not enough:
     * {@code NULL} resolves and still has no evaluator.
     */
    public void testGroupableTypesSupportEquality() {
        for (DataType type : DataType.values()) {
            if (GROUPABLE_WITHOUT_EQUALS.contains(type)) {
                assertTrue(type.typeName(), type.isGroupable());
                if (type == DataType.NULL) {
                    // TypeResolutions.isType accepts NULL for every operator, so this resolves with no evaluator.
                    // null == null folds to null.
                    assertTrue(type.typeName(), equalsSupports(type));
                    assertNull(type.typeName(), foldEquals(type, null, null));
                    expectThrows(EsqlIllegalArgumentException.class, () -> equalsEvaluator(type));
                    continue;
                }
                assertFalse(type.typeName() + " " + equalsResolution(type).message(), equalsSupports(type));
                expectThrows(EsqlIllegalArgumentException.class, () -> equalsEvaluator(type));
            } else if (type.isGroupable()) {
                assertTrue(type.typeName() + " " + equalsResolution(type).message(), equalsSupports(type));
                assertNotNull(type.typeName(), equalsEvaluator(type));
            } else if (equalsSupports(type)) {
                assertTrue(
                    type.typeName() + " supports == and is not groupable; pin it in EQUALS_WITHOUT_GROUPING or make it groupable",
                    EQUALS_WITHOUT_GROUPING.contains(type)
                );
            }
        }
        for (DataType type : EQUALS_WITHOUT_GROUPING) {
            assertFalse(type.typeName(), type.isGroupable());
            assertTrue(type.typeName() + " " + equalsResolution(type).message(), equalsSupports(type));
            assertNotNull(type.typeName(), equalsEvaluator(type));
        }
    }

    /**
     * For ordinary scalars, single-key {@code STATS} ({@link BlockHash}), {@code LIMIT BY} / {@code DEDUP}
     * ({@link RowKeyHash}) and the packed hash ({@link BlockHash#buildPackedValuesBlockHash}) put two values
     * in one group exactly when {@code ==} does. {@code NaN} and {@code -0.0} are pinned separately.
     * This does not change the hashes.
     */
    public void testGroupingAgreesWithEqualsForScalars() {
        for (DataType type : EQUALS_ORACLE_TYPES) {
            Object left = scalar(type, false);
            Object right = scalar(type, true);
            assertGroupingAgrees(type, left, left);
            assertGroupingAgrees(type, left, right);
            for (int i = 0; i < 16; i++) {
                assertGroupingAgrees(type, randomScalar(type), randomScalar(type));
            }
        }
    }

    /**
     * {@code null == null} and {@code null == v} fold to null. A multivalue is not a boolean result either:
     * {@code ==} returns null, and the evaluator warns that it saw more than one value.
     */
    public void testNullAndMultivalueEquality() {
        for (DataType type : EQUALS_ORACLE_TYPES) {
            assertNull(type.typeName(), foldEquals(type, null, null));
            assertNull(type.typeName(), foldEquals(type, scalar(type, false), null));
        }
        assertNull(foldEquals(DataType.INTEGER, List.of(1, 2), 1));
        assertWarnings(MULTIVALUE_EQUALS_WARNINGS);
        assertNull(foldEquals(DataType.INTEGER, List.of(1, 2), List.of(2, 1)));
        assertWarnings(MULTIVALUE_EQUALS_WARNINGS);
    }

    /**
     * Records the current {@code NaN} and signed-zero grouping, which disagrees with the contract.
     * Single-key {@code STATS} hashes with {@link Double#doubleToLongBits}, so {@code NaN} payloads share a group.
     * {@code LIMIT BY}, {@code DEDUP} and the packed hash used by multi-key {@code STATS} and {@code INLINE STATS}
     * keep the raw bits, so those payloads do not. The sign of zero follows the same split: {@code ==} treats
     * {@code 0.0} and {@code -0.0} as equal, and the raw-bit hashes do not. Tracked by
     * https://github.com/elastic/elasticsearch/issues/167086
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

    /**
     * The {@code INLINE STATS} join looks keys up in a {@link RowInTableLookup} built from the grouped output
     * ({@code PackedValuesBlockHash}). Probing that output finds each grouped row again.
     */
    public void testInlineStatsLookupFindsItsInputRow() {
        BlockFactory blockFactory = blockFactory();
        for (DataType type : DataType.values()) {
            if (type.isGroupable() == false || type == DataType.DENSE_VECTOR) {
                continue;
            }
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

    public void testNullIsGroupable() {
        assertTrue(DataType.NULL.isGroupable());
        analyzer().query("ROW x = 1 | EVAL f = null | STATS c = count(*) BY f");
        analyzer().query("ROW f = null | LIMIT 1 BY f");
        if (EsqlCapabilities.Cap.DEDUP_COMMAND.isEnabled()) {
            analyzer().query("ROW f = null | DEDUP");
        }
    }

    /**
     * Every groupable type must build the {@link BlockHash} STATS BY groups with: the build throws for an
     * element type without grouping support, which is the failure. The exception is dense_vector:
     * {@link BlockHash} cannot group it, which is why STATS rejects it, while LIMIT BY and DEDUP encode
     * their keys row by row and accept it, covered by the dense_vector and dedup csv-spec tests.
     */
    public void testGroupableTypesCanBuildBlockHash() {
        BlockFactory blockFactory = blockFactory();
        for (DataType type : DataType.values()) {
            if (type.isGroupable() == false || type == DataType.DENSE_VECTOR) {
                continue;
            }
            ElementType elementType = PlannerUtils.toElementType(type);
            BlockHash.build(List.of(new BlockHash.GroupSpec(0, elementType)), blockFactory, 16 * 1024, false).close();
        }
    }

    /**
     * The LIMIT BY and DEDUP key path must group every groupable type: {@link RowKeyHash} must put equal
     * values into one group and different values into different ones, with a multivalue compared as an
     * ordered list. This is the row-key counterpart of {@link #testGroupableTypesCanBuildBlockHash};
     * dense_vector, which {@link BlockHash} cannot group, is covered here through its multivalued FLOAT keys.
     */
    public void testGroupableTypesGroupRowKeys() {
        BlockFactory blockFactory = blockFactory();
        for (DataType type : DataType.values()) {
            if (type.isGroupable() == false) {
                continue;
            }
            ElementType elementType = PlannerUtils.toElementType(type);
            try (
                Block block = keyBlock(elementType, blockFactory);
                RowKeyHash hash = new RowKeyHash(new int[] { 0 }, List.of(elementType), blockFactory)
            ) {
                int[] ids = hash.add(new Page(block));
                assertThat("equal [" + type.typeName() + "] values must share a group", ids[1], equalTo(ids[0]));
                if (elementType != ElementType.NULL) {
                    assertThat("different [" + type.typeName() + "] values must not share a group", ids[2], not(equalTo(ids[0])));
                }
            }
        }
    }

    /**
     * A block whose positions 0 and 1 hold equal values and position 2 a different one. FLOAT keys only occur
     * for dense_vector, so its positions are whole vectors, with {@code [2, 1]} differing from {@code [1, 2]}
     * only in value order.
     */
    private static Block keyBlock(ElementType elementType, BlockFactory blockFactory) {
        return switch (elementType) {
            case NULL -> blockFactory.newConstantNullBlock(3);
            case BOOLEAN -> blockFactory.newBooleanBlockBuilder(3).appendBoolean(true).appendBoolean(true).appendBoolean(false).build();
            case INT -> blockFactory.newIntBlockBuilder(3).appendInt(1).appendInt(1).appendInt(2).build();
            case LONG -> blockFactory.newLongBlockBuilder(3).appendLong(1).appendLong(1).appendLong(2).build();
            case DOUBLE -> blockFactory.newDoubleBlockBuilder(3).appendDouble(1.5).appendDouble(1.5).appendDouble(2.5).build();
            case BYTES_REF -> blockFactory.newBytesRefBlockBuilder(3)
                .appendBytesRef(new BytesRef("a"))
                .appendBytesRef(new BytesRef("a"))
                .appendBytesRef(new BytesRef("b"))
                .build();
            case DOUBLE_RANGE -> blockFactory.newDoubleRangeBlockBuilder(3)
                .appendDoubleRange(1, 2)
                .appendDoubleRange(1, 2)
                .appendDoubleRange(1, 3)
                .build();
            case FLOAT -> blockFactory.newFloatBlockBuilder(6)
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
            default -> throw new AssertionError("no key values for groupable element type [" + elementType + "]");
        };
    }

    private static List<DataType> matrixTypes() {
        return Arrays.stream(DataType.values())
            .filter(t -> NEVER_A_GROUPING.contains(t) == false)
            .filter(t -> DataType.UNDER_CONSTRUCTION.contains(t) == false || Build.current().isSnapshot())
            .toList();
    }

    private void check(DataType type, String query, boolean accepted, String error) {
        Map<String, EsField> mapping = Map.of("f", new EsField("f", type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        var analyzer = analyzer().addIndex(EsIndexGenerator.esIndex("t", mapping));
        if (accepted) {
            analyzer.query(query);
        } else {
            analyzer.error(query, containsString(error));
        }
    }

    private static String groupingError(DataType type) {
        // The dense_vector STATS exception does not advise converting, since the type is groupable elsewhere.
        if (type == DataType.DENSE_VECTOR) {
            return "cannot group by on [dense_vector] type for grouping [f]";
        }
        return "cannot group by on [" + type.typeName() + "] type for grouping [f]; convert it to a groupable type or drop it";
    }

    private static String dedupError(DataType type) {
        return "cannot dedup on [" + type.typeName() + "] type for column [f]; convert it to a groupable type or drop it";
    }

    private static BlockFactory blockFactory() {
        return TestBlockFactory.getNonBreakingInstance();
    }

    private enum BlockHashKind {
        /** Single-key {@code STATS}, {@link BlockHash}. */
        SINGLE,
        /** {@code LIMIT BY} and {@code DEDUP}, {@link RowKeyHash}. */
        ROW,
        /** Multi-key {@code STATS} and the {@code INLINE STATS} join, {@link BlockHash#buildPackedValuesBlockHash}. */
        PACKED
    }

    private static boolean equalsSupports(DataType type) {
        return equalsResolution(type).resolved();
    }

    private static Expression.TypeResolution equalsResolution(DataType type) {
        EsField field = new EsField("f", type, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        FieldAttribute attribute = new FieldAttribute(Source.EMPTY, "f", field);
        return new Equals(Source.EMPTY, attribute, attribute).typeResolved();
    }

    private static boolean valuesEqual(DataType type, Object left, Object right) {
        Boolean result = (Boolean) foldEquals(type, left, right);
        assertNotNull(type.typeName(), result);
        return result;
    }

    private static final String[] MULTIVALUE_EQUALS_WARNINGS = new String[] {
        "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
        "Line -1:-1: java.lang.IllegalArgumentException: single-value function encountered multi-value" };

    /** Builds the {@code ==} evaluator. Fails when the type has none, which type resolution does not always report. */
    private static ExpressionEvaluator.Factory equalsEvaluator(DataType type) {
        Equals equals = new Equals(Source.EMPTY, new Literal(Source.EMPTY, null, type), new Literal(Source.EMPTY, null, type));
        return equals.toEvaluator(new EvaluatorMapper.ToEvaluator() {
            @Override
            public ExpressionEvaluator.Factory apply(Expression expression) {
                return ((EvaluatorMapper) expression).toEvaluator(this);
            }

            @Override
            public FoldContext foldCtx() {
                return FoldContext.small();
            }
        });
    }

    private static Object foldEquals(DataType type, Object left, Object right) {
        return new Equals(Source.EMPTY, new Literal(Source.EMPTY, left, type), new Literal(Source.EMPTY, right, type)).fold(
            FoldContext.small()
        );
    }

    /** A scalar that is not {@code NaN} or {@code -0.0}. Those two are pinned separately. */
    private Object randomScalar(DataType type) {
        return switch (type) {
            case BOOLEAN -> randomBoolean();
            case INTEGER -> randomInt();
            case LONG -> randomLong();
            case DOUBLE -> {
                double value = randomBoolean() ? randomDouble() : -randomDouble();
                yield value == 0.0d ? 0.0d : value;
            }
            case KEYWORD -> new BytesRef(randomRealisticUnicodeOfCodepointLengthBetween(0, 8));
            default -> throw new AssertionError(type);
        };
    }

    private static void assertGroupingAgrees(DataType type, Object left, Object right) {
        boolean equal = valuesEqual(type, left, right);
        String label = type.typeName() + " [" + left + "] [" + right + "]";
        assertThat(label, sameGroup(type, left, right, BlockHashKind.SINGLE), equalTo(equal));
        assertThat(label, sameGroup(type, left, right, BlockHashKind.ROW), equalTo(equal));
        assertThat(label, sameGroup(type, left, right, BlockHashKind.PACKED), equalTo(equal));
    }

    private static boolean sameGroup(BlockHashKind kind, double left, double right) {
        return sameGroup(DataType.DOUBLE, left, right, kind);
    }

    private static boolean sameGroup(DataType type, Object left, Object right, BlockHashKind kind) {
        BlockFactory factory = blockFactory();
        ElementType elementType = PlannerUtils.toElementType(type);
        try (Block block = twoValues(type, left, right, factory)) {
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

    /** A fixed pair, {@code other == false} the first value and {@code true} a different one. */
    private static Object scalar(DataType type, boolean other) {
        return switch (type) {
            case BOOLEAN -> other;
            case INTEGER -> other ? 2 : 1;
            case LONG -> other ? 2L : 1L;
            case DOUBLE -> other ? 2.5d : 1.5d;
            case KEYWORD -> new BytesRef(other ? "b" : "a");
            default -> throw new AssertionError(type);
        };
    }

    private static Block twoValues(DataType type, Object left, Object right, BlockFactory factory) {
        return switch (type) {
            case BOOLEAN -> factory.newBooleanBlockBuilder(2).appendBoolean((Boolean) left).appendBoolean((Boolean) right).build();
            case INTEGER -> factory.newIntBlockBuilder(2).appendInt((Integer) left).appendInt((Integer) right).build();
            case LONG -> factory.newLongBlockBuilder(2).appendLong((Long) left).appendLong((Long) right).build();
            case DOUBLE -> factory.newDoubleBlockBuilder(2).appendDouble((Double) left).appendDouble((Double) right).build();
            case KEYWORD -> factory.newBytesRefBlockBuilder(2).appendBytesRef((BytesRef) left).appendBytesRef((BytesRef) right).build();
            default -> throw new AssertionError(type);
        };
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

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
