/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PackDimsAgg;
import org.elasticsearch.xpack.esql.expression.promql.function.NaturalSortKey;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesCollapse;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class PromqlPlanSortByLabelTests extends AbstractPromqlPlanOptimizerTests {

    public PromqlPlanSortByLabelTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void assumeSortByLabelEnabled() {
        assumeTrue("Requires PROMQL_SORT_BY_LABEL capability", EsqlCapabilities.Cap.PROMQL_SORT_BY_LABEL.isEnabled());
    }

    public void testOpenHeaderGroupsBySortLabelWithoutExposingIt() {
        LogicalPlan plan = planPromql("PROMQL index=k8s step=1h result=(sort_by_label(network.bytes_in, \"pod\"))", false);

        assertThat(outputColumns(plan), not(hasItem("pod")));
        assertThat(outputColumns(plan), hasItem(MetadataAttribute.TIMESERIES));

        List<Attribute> collapseDimensions = plan.collect(TimeSeriesCollapse.class)
            .stream()
            .flatMap(collapse -> collapse.dimensions().stream())
            .toList();
        if (collapseDimensions.isEmpty() == false) {
            assertThat(collapseDimensions.stream().map(Attribute::name).toList(), hasItem("pod"));
            assertThat(collapseDimensions.stream().map(Attribute::name).toList(), hasItem(MetadataAttribute.TIMESERIES));
        }

        List<String> dimensions = aggregatedDimensions(plan);
        assertThat(dimensions, hasItem("pod"));
        assertThat(dimensions, hasItem(MetadataAttribute.TIMESERIES));
        assertWarnings("sort_by_label: ordering is discarded for range queries");
    }

    /**
     * Names of the dimensions the time-series aggregates group by, in either representation: one
     * {@link DimensionValues} per dimension, or a single {@link PackDimsAgg} collecting them all.
     */
    private static List<String> aggregatedDimensions(LogicalPlan plan) {
        List<Expression> fields = new ArrayList<>();
        for (TimeSeriesAggregate aggregate : plan.collect(TimeSeriesAggregate.class)) {
            for (NamedExpression agg : aggregate.aggregates()) {
                agg.forEachDown(DimensionValues.class, dimension -> fields.add(dimension.field()));
                agg.forEachDown(PackDimsAgg.class, packed -> fields.addAll(packed.fields()));
            }
        }
        return fields.stream().map(field -> field instanceof Attribute attribute ? attribute.name() : field.toString()).toList();
    }

    public void testClosedHeaderSkipsSortLabels() {
        LogicalPlan withSort = planPromql("PROMQL index=k8s step=1h result=(sort_by_label(avg by (cluster) (network.bytes_in), \"pod\"))");
        LogicalPlan without = planPromql("PROMQL index=k8s step=1h result=(avg by (cluster) (network.bytes_in))");
        assertEquals(outputColumns(without), outputColumns(withSort));
        assertThat(outputColumns(withSort), not(hasItem("pod")));
        assertWarnings("sort_by_label: ordering is discarded for range queries");
    }

    public void testOpenHeaderSortDoesNotChangeOutputSchema() {
        LogicalPlan withSort = planPromql(
            "PROMQL index=k8s time=\"2024-05-10T00:03:00.000Z\" result=(sort_by_label(network.bytes_in, \"pod\"))",
            false
        );
        LogicalPlan without = planPromql("PROMQL index=k8s time=\"2024-05-10T00:03:00.000Z\" result=(network.bytes_in)", false);
        assertEquals(outputColumns(without), outputColumns(withSort));
    }

    public void testRangeQueryDoesNotInjectOrderBy() {
        LogicalPlan plan = planPromql("PROMQL index=k8s step=1h result=(sort_by_label(network.bytes_in, \"pod\"))", false);
        assertTrue(plan.collect(OrderBy.class).isEmpty());
        assertWarnings("sort_by_label: ordering is discarded for range queries");
    }

    public void testInstantQueryInjectsNaturalSortKeyAndTimeseriesTieBreak() {
        LogicalPlan plan = planPromql(
            "PROMQL index=k8s time=\"2024-05-10T00:03:00.000Z\" result=(sort_by_label(network.bytes_in, \"pod\"))",
            false
        );
        assertThat(outputColumns(plan), not(hasItem("pod")));
        assertThat(outputColumns(plan), hasItem(MetadataAttribute.TIMESERIES));
        OrderBy orderBy = as(plan.collect(OrderBy.class).getFirst(), OrderBy.class);
        Eval eval = as(orderBy.child(), Eval.class);
        assertEquals(1, eval.fields().size());
        Alias key = eval.fields().getFirst();
        assertTrue(key.synthetic());
        assertTrue(key.child() instanceof NaturalSortKey);
        assertEquals(2, orderBy.order().size());
        assertEquals(Order.OrderDirection.ASC, orderBy.order().getFirst().direction());
        assertTrue(orderBy.order().getFirst().child().semanticEquals(key.toAttribute()));
        assertEquals(Order.OrderDirection.ASC, orderBy.order().get(1).direction());
        Attribute tieBreak = as(orderBy.order().get(1).child(), Attribute.class);
        assertEquals(MetadataAttribute.TIMESERIES, tieBreak.name());
    }

    public void testInstantQueryDescReversesKeysAndTieBreak() {
        LogicalPlan plan = planPromql(
            "PROMQL index=k8s time=\"2024-05-10T00:03:00.000Z\" result=(sort_by_label_desc(network.bytes_in, \"pod\"))",
            false
        );
        OrderBy orderBy = as(plan.collect(OrderBy.class).getFirst(), OrderBy.class);
        assertEquals(2, orderBy.order().size());
        assertEquals(Order.OrderDirection.DESC, orderBy.order().getFirst().direction());
        assertEquals(Order.OrderDirection.DESC, orderBy.order().get(1).direction());
        Attribute tieBreak = as(orderBy.order().get(1).child(), Attribute.class);
        assertEquals(MetadataAttribute.TIMESERIES, tieBreak.name());
    }

    public void testClosedHeaderInstantOrdersByRemainingIdentityWithoutEval() {
        LogicalPlan plan = planPromql(
            "PROMQL index=k8s time=\"2024-05-10T00:03:00.000Z\" result=(sort_by_label(avg by (cluster) (network.bytes_in), \"pod\"))",
            false
        );
        OrderBy orderBy = as(plan.collect(OrderBy.class).getFirst(), OrderBy.class);
        assertFalse(orderBy.child() instanceof Eval);
        assertEquals(1, orderBy.order().size());
        assertEquals(Order.OrderDirection.ASC, orderBy.order().getFirst().direction());
        Attribute tieBreak = as(orderBy.order().getFirst().child(), Attribute.class);
        assertEquals("cluster", tieBreak.name());
    }

    public void testClosedHeaderInstantOrdersByPresentSortLabel() {
        LogicalPlan plan = planPromql(
            "PROMQL index=k8s time=\"2024-05-10T00:03:00.000Z\" result=(sort_by_label(avg by (cluster) (network.bytes_in), \"cluster\"))",
            false
        );
        OrderBy orderBy = as(plan.collect(OrderBy.class).getFirst(), OrderBy.class);
        Eval eval = as(orderBy.child(), Eval.class);
        assertEquals(1, eval.fields().size());
        assertTrue(eval.fields().getFirst().child() instanceof NaturalSortKey);
        assertEquals(1, orderBy.order().size());
        assertTrue(orderBy.order().getFirst().child().semanticEquals(eval.fields().getFirst().toAttribute()));
    }

    /**
     * The injected {@code OrderBy} must still fuse into the {@code TopN} above the collapse, and neither the
     * materialized sort label nor the synthetic sort key may survive into the result.
     */
    public void testInstantQueryFusesToTopNWithoutLeakingSortColumns() {
        LogicalPlan optimized = planPromql(
            "PROMQL index=k8s time=\"2024-05-10T00:03:00.000Z\" value=(sort_by_label(network.bytes_in, \"pod\")) | TS_COLLAPSE"
        );

        List<TopN> topNsAboveCollapse = optimized.collect(TopN.class)
            .stream()
            .filter(topN -> topN.collect(TimeSeriesCollapse.class).isEmpty() == false)
            .toList();
        assertEquals("expected a TopN above TimeSeriesCollapse", 1, topNsAboveCollapse.size());
        assertEquals(List.of("value", PromqlCommand.STEP, MetadataAttribute.TIMESERIES), outputColumns(optimized));
    }

    private static List<String> outputColumns(LogicalPlan plan) {
        return plan.output().stream().map(Attribute::name).toList();
    }
}
