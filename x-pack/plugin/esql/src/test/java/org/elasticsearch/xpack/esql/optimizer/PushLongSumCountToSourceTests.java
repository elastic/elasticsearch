/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SumLongAsDouble;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests that the data node reads the sum and the count of a {@code long} with {@code LuceneSumCountLongOperator}, which is what
 * {@code PushStatsToSource} turns the {@code AVG} of a {@code long} into. {@code long_noidx} is the only {@code long} in the test
 * mapping.
 */
public class PushLongSumCountToSourceTests extends AbstractLocalPhysicalPlanOptimizerTests {

    /** Doc values only, like a field mapped with {@code index: false}. */
    private static final SearchStats DOC_VALUES_ONLY = new EsqlTestUtils.TestSearchStats() {
        @Override
        public boolean isIndexed(FieldAttribute.FieldName field) {
            return false;
        }

        @Override
        public boolean hasDocValues(FieldAttribute.FieldName field) {
            return true;
        }
    };

    /** Indexed too, which is what a default {@code long} field is. */
    private static final SearchStats INDEXED_WITH_DOC_VALUES = new EsqlTestUtils.TestSearchStats() {
        @Override
        public boolean hasDocValues(FieldAttribute.FieldName field) {
            return true;
        }
    };

    private static final SearchStats MULTI_VALUED = new EsqlTestUtils.TestSearchStats() {
        @Override
        public boolean isSingleValue(FieldAttribute.FieldName field) {
            return false;
        }

        @Override
        public boolean hasDocValues(FieldAttribute.FieldName field) {
            return true;
        }
    };

    public PushLongSumCountToSourceTests(String name, Configuration config) {
        super(name, config);
    }

    /**
     * Expects, on the data node, instead of the {@code AggregateExec} that reads the field:
     * <pre>{@code
     * ExchangeExec[[$$SUM$a$0$value, $$SUM$a$0$delta, $$SUM$a$0$seen, $$COUNT$a$1$count, $$COUNT$a$1$seen],true]
     * \_EsStatsQueryExec[test], stats[LongSumCountStat[field=long_noidx, ...]]
     * }</pre>
     */
    public void testAvgOfLong() {
        var plan = plannerOptimizer.plan("FROM test | STATS a = AVG(long_noidx)", DOC_VALUES_ONLY);

        EsStatsQueryExec stats = singleStatsQuery(plan);
        var stat = as(stats.stat(), EsStatsQueryExec.LongSumCountStat.class);
        assertThat(stat.field().fieldName().string(), equalTo("long_noidx"));
        assertThat(stat.queryBuilderAndTags(), hasSize(1));
        assertThat(stat.queryBuilderAndTags().get(0).query(), equalTo(null));
        // The intermediate state of SUM of a double followed by the one of COUNT, what the coordinator expects.
        assertThat(
            stats.output().stream().map(Attribute::dataType).toList(),
            contains(DataType.DOUBLE, DataType.DOUBLE, DataType.BOOLEAN, DataType.LONG, DataType.BOOLEAN)
        );
        assertThat(stats.limit(), equalTo(null));
    }

    /**
     * For a field that is indexed, {@code AVG} also filters out the docs without a value, which is pushed to the source as an
     * {@code exists} query. So the operator gets a query that isn't match-all, and has to look at the doc ids. That is a limitation
     * of the prototype: such a query could be recognized as match-all for the segments in which every doc has the field.
     */
    public void testIndexedFieldHasAnExistsQuery() {
        var plan = plannerOptimizer.plan("FROM test | STATS a = AVG(long_noidx)", INDEXED_WITH_DOC_VALUES);

        var stat = as(singleStatsQuery(plan).stat(), EsStatsQueryExec.LongSumCountStat.class);
        assertThat(stat.queryBuilderAndTags(), hasSize(1));
        assertThat(stat.queryBuilderAndTags().get(0).query(), notNullValue());
    }

    /** The filter is the query the operator collects the docs of. */
    public void testFilterIsTheQueryOfTheOperator() {
        var plan = plannerOptimizer.plan("FROM test | WHERE emp_no > 10 | STATS a = AVG(long_noidx)", DOC_VALUES_ONLY);

        var stat = as(singleStatsQuery(plan).stat(), EsStatsQueryExec.LongSumCountStat.class);
        assertThat(stat.queryBuilderAndTags(), hasSize(1));
        assertThat(stat.queryBuilderAndTags().get(0).query(), notNullValue());
    }

    /** The operator adds up every value of a multivalued field, as the aggregations do. */
    public void testMultiValuedFieldIsPushed() {
        var plan = plannerOptimizer.plan("FROM test | STATS a = AVG(long_noidx)", MULTI_VALUED);

        as(singleStatsQuery(plan).stat(), EsStatsQueryExec.LongSumCountStat.class);
    }

    public void testGroupedAvgIsNotPushed() {
        assertNotPushed("FROM test | STATS a = AVG(long_noidx) BY emp_no", DOC_VALUES_ONLY, 0);
    }

    public void testAvgOfIntegerIsNotPushed() {
        assertNotPushed("FROM test | STATS a = AVG(salary)", DOC_VALUES_ONLY, 0);
    }

    public void testOtherAggregatesAreNotPushed() {
        assertNotPushed("FROM test | STATS a = AVG(long_noidx), m = MAX(salary)", DOC_VALUES_ONLY, 1);
    }

    /**
     * A limit below the aggregation stops it early, so the aggregation, which the operator would run without a limit, isn't on
     * the data node.
     */
    public void testLimitBelowTheAggregationIsNotPushed() {
        assertNotPushed("FROM test | LIMIT 10 | STATS a = AVG(long_noidx)", DOC_VALUES_ONLY, 0);
    }

    private void assertNotPushed(String query, SearchStats stats, int expectedSumLongAsDoubles) {
        var plan = plannerOptimizer.plan(query, stats);
        assertThat(statsQueries(plan).stream().filter(s -> s.stat() instanceof EsStatsQueryExec.LongSumCountStat).toList(), hasSize(0));
        long sums = plan.collect(AggregateExec.class::isInstance)
            .stream()
            .map(AggregateExec.class::cast)
            .flatMap(a -> a.aggregates().stream())
            .map(Alias::unwrap)
            .filter(SumLongAsDouble.class::isInstance)
            .count();
        assertThat(sums, equalTo((long) expectedSumLongAsDoubles));
    }

    private static List<EsStatsQueryExec> statsQueries(PhysicalPlan plan) {
        return plan.collect(EsStatsQueryExec.class::isInstance).stream().map(EsStatsQueryExec.class::cast).toList();
    }

    private static EsStatsQueryExec singleStatsQuery(PhysicalPlan plan) {
        List<EsStatsQueryExec> statsQueries = statsQueries(plan);
        assertThat(statsQueries, hasSize(1));
        assertThat(statsQueries.get(0).stat(), instanceOf(EsStatsQueryExec.Stat.class));
        return statsQueries.get(0);
    }

    private static <T> T as(Object o, Class<T> type) {
        return EsqlTestUtils.as(o, type);
    }
}
