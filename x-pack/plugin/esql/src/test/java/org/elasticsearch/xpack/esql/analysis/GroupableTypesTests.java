/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;

/**
 * Enforces that which types can be a grouping key is decided by {@link DataType#isGroupable} alone: for every
 * data type, the verifier outcome of STATS BY, INLINE STATS BY, LIMIT BY and DEDUP must match the property, and
 * the compute engine must be able to build a grouping for every groupable type, so the property can never claim
 * comparability the engine cannot deliver.
 */
public class GroupableTypesTests extends ESTestCase {

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
     * LIMIT BY is GA, so a type that is groupable today must stay groupable: this pins the set as a lower
     * bound. Adding types is fine; removing one rejects queries that work today.
     */
    public void testGroupableSetOnlyGrows() {
        Set<DataType> groupable = Set.of(
            DataType.NULL,
            DataType.BOOLEAN,
            DataType.COUNTER_LONG,
            DataType.COUNTER_INTEGER,
            DataType.COUNTER_DOUBLE,
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
     * A type SORT accepts should be a legal grouping key too, since both need stable value comparisons.
     * The pinned violations are types {@code isSortable} accepts although no sort key encoding exists for
     * them either; they must not grow.
     */
    public void testSortableImpliesGroupable() {
        Set<DataType> knownViolations = Set.of(DataType.TDIGEST, DataType.EXPONENTIAL_HISTOGRAM, DataType.PARTIAL_AGG);
        for (DataType type : DataType.values()) {
            if (NEVER_A_GROUPING.contains(type) || type == DataType.DOC_DATA_TYPE) {
                continue;
            }
            if (DataType.isSortable(type) && knownViolations.contains(type) == false) {
                assertTrue("sortable [" + type.typeName() + "] must be groupable", type.isGroupable());
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

    private static List<DataType> matrixTypes() {
        return Arrays.stream(DataType.values())
            .filter(t -> NEVER_A_GROUPING.contains(t) == false)
            .filter(t -> DataType.UNDER_CONSTRUCTION.contains(t) == false || Build.current().isSnapshot())
            .toList();
    }

    private static void check(DataType type, String query, boolean accepted, String error) {
        Map<String, EsField> mapping = Map.of("f", new EsField("f", type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        var analyzer = analyzer().addIndex(EsIndexGenerator.esIndex("t", mapping)).minimumTransportVersion(TransportVersion.current());
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

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
