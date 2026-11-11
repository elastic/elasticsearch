/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SumLongAsDouble;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.AbstractPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction.NO_WINDOW;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests the gates of {@link PushStatsToSource} that turn the sum and the count of a {@code long} into the fused
 * {@code LuceneSumCountLongOperator}. Every gate has a case that has to leave the aggregation alone. The plans are built by hand,
 * because that is the only way to get some of them: nothing splits a query by tag before this rule runs.
 */
public class PushStatsToSourceLongSumCountTests extends ESTestCase {

    private static final SearchStats WITH_DOC_VALUES = new EsqlTestUtils.TestSearchStats() {
        @Override
        public boolean hasDocValues(FieldAttribute.FieldName field) {
            return true;
        }
    };

    private static final FieldAttribute LONG_FIELD = EsqlTestUtils.getFieldAttribute("x", DataType.LONG);

    public void testPushed() {
        PhysicalPlan pushed = apply(aggregate(LONG_FIELD, LONG_FIELD, queryExec(null, null, singleQuery())));

        EsStatsQueryExec stats = as(pushed, EsStatsQueryExec.class);
        EsStatsQueryExec.LongSumCountStat stat = as(stats.stat(), EsStatsQueryExec.LongSumCountStat.class);
        assertThat(stat.field(), equalTo(LONG_FIELD));
        assertThat(stat.queryBuilderAndTags(), equalTo(singleQuery()));
        // five columns, the intermediate state of the sum and of the count
        assertThat(stats.output().size(), equalTo(5));
    }

    public void testPushedWithAFilterOnTheSource() {
        var filtered = List.of(new EsQueryExec.QueryBuilderAndTags(QueryBuilders.rangeQuery("y").gt(10), List.of()));
        assertThat(apply(aggregate(LONG_FIELD, LONG_FIELD, queryExec(null, null, filtered))), instanceOf(EsStatsQueryExec.class));
    }

    /** The operator makes one row out of everything it reads: it can't report per tag. */
    public void testSourceWithTagsIsNotPushed() {
        var tagged = List.of(new EsQueryExec.QueryBuilderAndTags(QueryBuilders.matchAllQuery(), List.of(1L)));
        assertNotPushed(aggregate(LONG_FIELD, LONG_FIELD, queryExec(null, null, tagged)));
    }

    public void testSourceWithSeveralQueriesIsNotPushed() {
        var several = List.of(
            new EsQueryExec.QueryBuilderAndTags(QueryBuilders.termQuery("y", 1), List.of()),
            new EsQueryExec.QueryBuilderAndTags(QueryBuilders.termQuery("y", 2), List.of())
        );
        assertNotPushed(aggregate(LONG_FIELD, LONG_FIELD, queryExec(null, null, several)));
        assertNotPushed(aggregate(LONG_FIELD, LONG_FIELD, queryExec(null, null, List.of())));
    }

    /** The operator reads every doc, it can't stop at a limit. */
    public void testLimitIsNotPushed() {
        assertNotPushed(aggregate(LONG_FIELD, LONG_FIELD, queryExec(Literal.integer(Source.EMPTY, 10), null, singleQuery())));
    }

    /** And it reads them in index order. */
    public void testSortIsNotPushed() {
        var sort = new EsQueryExec.FieldSort(LONG_FIELD, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        assertNotPushed(aggregate(LONG_FIELD, LONG_FIELD, queryExec(null, List.of(sort), singleQuery())));
    }

    public void testOnlyTheInitialModeIsPushed() {
        for (AggregatorMode mode : List.of(AggregatorMode.SINGLE, AggregatorMode.INTERMEDIATE, AggregatorMode.FINAL)) {
            assertNotPushed(aggregate(mode, List.of(), sum(LONG_FIELD), count(LONG_FIELD), queryExec(null, null, singleQuery())));
        }
    }

    public void testGroupedIsNotPushed() {
        assertNotPushed(
            aggregate(
                AggregatorMode.INITIAL,
                List.of(EsqlTestUtils.getFieldAttribute("g", DataType.KEYWORD)),
                sum(LONG_FIELD),
                count(LONG_FIELD),
                queryExec(null, null, singleQuery())
            )
        );
    }

    public void testAFilterOnTheAggregateIsNotPushed() {
        var filteredSum = new SumLongAsDouble(Source.EMPTY, LONG_FIELD, Literal.FALSE, NO_WINDOW);
        var filteredCount = new Count(Source.EMPTY, LONG_FIELD, Literal.FALSE, NO_WINDOW);
        assertNotPushed(aggregate(AggregatorMode.INITIAL, List.of(), filteredSum, count(LONG_FIELD), queryExec(null, null, singleQuery())));
        assertNotPushed(aggregate(AggregatorMode.INITIAL, List.of(), sum(LONG_FIELD), filteredCount, queryExec(null, null, singleQuery())));
    }

    public void testSomethingElseThanTheSumAndTheCountIsNotPushed() {
        // another aggregation next to them
        var extra = new Count(Source.EMPTY, EsqlTestUtils.getFieldAttribute("y", DataType.LONG));
        AggregateExec three = aggregate(
            AggregatorMode.INITIAL,
            List.of(),
            List.of(alias("s", sum(LONG_FIELD)), alias("c", count(LONG_FIELD)), alias("e", extra)),
            queryExec(null, null, singleQuery())
        );
        assertNotPushed(three);
        // two fields
        assertNotPushed(aggregate(LONG_FIELD, EsqlTestUtils.getFieldAttribute("y", DataType.LONG), queryExec(null, null, singleQuery())));
        // the count of everything
        assertNotPushed(
            aggregate(
                AggregatorMode.INITIAL,
                List.of(),
                List.of(alias("s", sum(LONG_FIELD)), alias("c", new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*")))),
                queryExec(null, null, singleQuery())
            )
        );
    }

    public void testFieldWithoutDocValuesIsNotPushed() {
        var noDocValues = new EsqlTestUtils.TestSearchStats() {
            @Override
            public boolean hasDocValues(FieldAttribute.FieldName field) {
                return false;
            }

        };
        assertNotPushed(aggregate(LONG_FIELD, LONG_FIELD, queryExec(null, null, singleQuery())), noDocValues);
    }

    /** The operator adds up every value of a multivalued field, as the aggregations do. */
    public void testMultiValuedFieldIsPushed() {
        var multiValued = new EsqlTestUtils.TestSearchStats() {
            @Override
            public boolean hasDocValues(FieldAttribute.FieldName field) {
                return true;
            }

            @Override
            public boolean isSingleValue(FieldAttribute.FieldName field) {
                return false;
            }
        };
        assertThat(
            apply(aggregate(LONG_FIELD, LONG_FIELD, queryExec(null, null, singleQuery())), multiValued),
            instanceOf(EsStatsQueryExec.class)
        );
    }

    private static List<EsQueryExec.QueryBuilderAndTags> singleQuery() {
        return List.of(new EsQueryExec.QueryBuilderAndTags(null, List.of()));
    }

    private static EsQueryExec queryExec(
        org.elasticsearch.xpack.esql.core.expression.Expression limit,
        List<EsQueryExec.Sort> sorts,
        List<EsQueryExec.QueryBuilderAndTags> queries
    ) {
        return new EsQueryExec(Source.EMPTY, "test", IndexMode.STANDARD, List.of(), limit, sorts, null, queries);
    }

    private static SumLongAsDouble sum(FieldAttribute field) {
        return new SumLongAsDouble(Source.EMPTY, field);
    }

    private static Count count(FieldAttribute field) {
        return new Count(Source.EMPTY, field);
    }

    private static Alias alias(String name, AggregateFunction function) {
        return new Alias(Source.EMPTY, name, function);
    }

    private static AggregateExec aggregate(FieldAttribute sumField, FieldAttribute countField, EsQueryExec source) {
        return aggregate(AggregatorMode.INITIAL, List.of(), sum(sumField), count(countField), source);
    }

    private static AggregateExec aggregate(
        AggregatorMode mode,
        List<org.elasticsearch.xpack.esql.core.expression.Expression> groupings,
        SumLongAsDouble sum,
        Count count,
        EsQueryExec source
    ) {
        return aggregate(mode, groupings, List.of(alias("s", sum), alias("c", count)), source);
    }

    private static AggregateExec aggregate(
        AggregatorMode mode,
        List<org.elasticsearch.xpack.esql.core.expression.Expression> groupings,
        List<? extends NamedExpression> aggregates,
        EsQueryExec source
    ) {
        return new AggregateExec(
            Source.EMPTY,
            source,
            groupings,
            aggregates,
            mode,
            AbstractPhysicalOperationProviders.intermediateAttributes(aggregates, groupings),
            null
        );
    }

    private static PhysicalPlan apply(AggregateExec aggregate) {
        return apply(aggregate, WITH_DOC_VALUES);
    }

    private static PhysicalPlan apply(AggregateExec aggregate, SearchStats stats) {
        var context = new LocalPhysicalOptimizerContext(PlannerSettings.DEFAULTS, new EsqlFlags(true), null, FoldContext.small(), stats);
        return new PushStatsToSource().apply(aggregate, context);
    }

    private static void assertNotPushed(AggregateExec aggregate) {
        assertNotPushed(aggregate, WITH_DOC_VALUES);
    }

    private static void assertNotPushed(AggregateExec aggregate, SearchStats stats) {
        assertThat("the aggregation is left alone", apply(aggregate, stats), sameInstance(aggregate));
    }

    private static <T> T as(Object o, Class<T> type) {
        return EsqlTestUtils.as(o, type);
    }
}
