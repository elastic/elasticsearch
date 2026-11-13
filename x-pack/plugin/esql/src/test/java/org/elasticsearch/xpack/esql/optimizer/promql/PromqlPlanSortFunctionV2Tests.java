/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.expression.promql.function.NaturalSortKey;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/** {@code sort} and {@code sort_by_label} translated over a label container. */
public class PromqlPlanSortFunctionV2Tests extends AbstractPromqlPlanOptimizerTests {

    private static final String INSTANT = "PROMQL index=k8s time=\"2024-05-10T00:03:00.000Z\" ";

    public PromqlPlanSortFunctionV2Tests(VersionMode versionMode) {
        super(versionMode);
    }

    /** An instant {@code sort} orders the series, which keep their labels. */
    public void testInstantSortOrdersTheSeries() {
        LogicalPlan plan = planPromql(INSTANT + "sort(network.bytes_in)", false);

        assertThat(plan.collect(OrderBy.class), hasSize(1));
        assertThat(names(plan.output()), hasItem("_timeseries"));
    }

    /** A range {@code sort} has no order, like Prometheus. */
    public void testRangeSortHasNoOrder() {
        LogicalPlan plan = planPromql("PROMQL index=k8s step=1h sort(network.bytes_in)", false);

        assertThat(plan.collect(OrderBy.class), empty());
        assertWarnings("sort: ordering is discarded for range queries");
    }

    /**
     * An instant {@code sort_by_label} orders by a label of the series, read from its field; the label stays in {@code _timeseries}
     * and is not exposed as a column of its own.
     */
    public void testSortByLabelReadsTheLabel() {
        assumeTrue("Requires PROMQL_SORT_BY_LABEL capability", EsqlCapabilities.Cap.PROMQL_SORT_BY_LABEL.isEnabled());
        LogicalPlan plan = planPromql(INSTANT + "sort_by_label(network.bytes_in, \"pod\")", false);

        assertThat(plan.collect(OrderBy.class), hasSize(1));
        var keys = new ArrayList<NaturalSortKey>();
        plan.forEachExpressionDown(NaturalSortKey.class, keys::add);
        assertThat(keys, hasSize(1));
        assertThat(names(new ArrayList<>(keys.getFirst().references())), hasItem("pod"));
        assertThat(names(plan.output()), not(hasItem("pod")));
        assertThat(names(plan.output()), hasItem("_timeseries"));
    }

    /** The translated sorts plan through the optimizer. */
    public void testSortsOptimize() {
        var queries = new ArrayList<>(List.of("sort(network.bytes_in)", "sort_desc(sum by (cluster) (network.bytes_in))"));
        if (EsqlCapabilities.Cap.PROMQL_SORT_BY_LABEL.isEnabled()) {
            queries.add("sort_by_label(network.bytes_in, \"pod\")");
        }
        for (String query : queries) {
            LogicalPlan plan = planPromql(INSTANT + query, true);
            assertThat(query, plan.collect(TimeSeriesAggregate.class), hasSize(1));
        }
    }

    private static List<String> names(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::name).toList();
    }
}
