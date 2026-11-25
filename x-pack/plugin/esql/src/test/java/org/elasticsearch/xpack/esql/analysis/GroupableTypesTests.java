/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.RowKeyHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.test.TestBlockFactory;
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
 * comparability the engine cannot deliver. Whether two values land in one group is {@link GroupingEqualityTests}.
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

    private static boolean equalsSupports(DataType type) {
        return equalsResolution(type).resolved();
    }

    private static Expression.TypeResolution equalsResolution(DataType type) {
        EsField field = new EsField("f", type, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        FieldAttribute attribute = new FieldAttribute(Source.EMPTY, "f", field);
        return new Equals(Source.EMPTY, attribute, attribute).typeResolved();
    }

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

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
