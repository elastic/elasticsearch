/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.GroupKeyEncoder;
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
import static org.hamcrest.Matchers.notNullValue;

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

    public void testNullIsGroupable() {
        assertTrue(DataType.NULL.isGroupable());
        analyzer().query("ROW x = 1 | EVAL f = null | STATS c = count(*) BY f");
        analyzer().query("ROW f = null | LIMIT 1 BY f");
        if (EsqlCapabilities.Cap.DEDUP_COMMAND.isEnabled()) {
            analyzer().query("ROW f = null | DEDUP");
        }
    }

    /**
     * Every groupable type must build the {@link BlockHash} STATS BY groups with. The exception is dense_vector:
     * {@link BlockHash} cannot group it, which is why STATS rejects it, while LIMIT BY and DEDUP encode their keys
     * row by row and accept it, covered by {@link #testDenseVectorHasARowKeyEncoding}.
     */
    public void testGroupableTypesCanBuildBlockHash() {
        BlockFactory blockFactory = blockFactory();
        for (DataType type : DataType.values()) {
            if (type.isGroupable() == false || type == DataType.DENSE_VECTOR) {
                continue;
            }
            ElementType elementType = PlannerUtils.toElementType(type);
            try (BlockHash hash = BlockHash.build(List.of(new BlockHash.GroupSpec(0, elementType)), blockFactory, 16 * 1024, false)) {
                assertThat("grouping by [" + type.typeName() + "] must be supported by BlockHash", hash, notNullValue());
            }
        }
    }

    public void testDenseVectorHasARowKeyEncoding() {
        assertTrue(DataType.DENSE_VECTOR.isGroupable());
        BlockFactory blockFactory = blockFactory();
        ElementType elementType = PlannerUtils.toElementType(DataType.DENSE_VECTOR);
        PagedBytesBuilder row = new PagedBytesBuilder(blockFactory.bigArrays().recycler(), blockFactory.breaker(), "test", 64);
        try (
            GroupKeyEncoder encoder = new GroupKeyEncoder(new int[] { 0 }, List.of(elementType), row);
            FloatBlock block = blockFactory.newFloatArrayVector(new float[] { 1F, 2F }, 2).asBlock()
        ) {
            assertThat(encoder.encode(new Page(block), 0), notNullValue());
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
