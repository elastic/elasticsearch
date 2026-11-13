/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.promql.function.HashOffset;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

/** PromQL reductions across series translated over a label container. */
public class PromqlPlanAcrossSeriesReductionV2Tests extends AbstractPromqlPlanOptimizerTests {

    public PromqlPlanAcrossSeriesReductionV2Tests(VersionMode versionMode) {
        super(versionMode);
    }

    /** {@code topk} ranks the series of each step and keeps their labels. */
    public void testTopkRanksEachStepAndKeepsTheLabels() {
        LogicalPlan plan = planPromql("PROMQL index=k8s step=1h topk(3, network.bytes_in)", false);

        TopNBy topN = plan.collect(TopNBy.class).getFirst();
        assertThat(topN.groupings(), hasSize(1));
        assertThat(topN.child().collect(TimeSeriesAggregate.class), hasSize(1));
        assertThat(names(plan.output()), hasItem("_timeseries"));
    }

    /** {@code topk by (...)} partitions each step by the listed labels of each series. */
    public void testTopkByPartitionsByTheListedLabels() {
        LogicalPlan plan = planPromql("PROMQL index=k8s step=1h topk by (cluster) (3, network.bytes_in)", false);

        TopNBy topN = plan.collect(TopNBy.class).getFirst();
        assertThat(topN.groupings(), hasSize(2));
        PackDims partitions = as(topN.child(), PackDims.class);
        assertThat(names(partitions.dims()), contains("cluster"));
        assertThat(names(plan.output()), hasItem("_timeseries"));
    }

    /** {@code limit_ratio} samples each series by a hash of its labels. */
    public void testLimitRatioSamplesByTheLabels() {
        LogicalPlan plan = planPromql("PROMQL index=k8s step=1h limit_ratio(0.5, network.bytes_in)", false);
        assertThat(expressions(plan, HashOffset.class), hasSize(1));
    }

    /** The translated reductions plan through the optimizer. */
    public void testReductionsOptimize() {
        for (String query : List.of(
            "topk(3, network.bytes_in)",
            "bottomk by (cluster) (2, rate(network.total_bytes_in[5m]))",
            "topk(1, sum by (cluster) (network.bytes_in))",
            "limit_ratio(0.5, network.bytes_in)"
        )) {
            LogicalPlan plan = planPromql("PROMQL index=k8s step=1h " + query, true);
            assertThat(query, plan.collect(TimeSeriesAggregate.class), hasSize(1));
        }
    }

    private static List<String> names(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::name).toList();
    }

    private static <E extends Expression> List<E> expressions(LogicalPlan plan, Class<E> type) {
        var found = new ArrayList<E>();
        plan.forEachExpressionDown(type, found::add);
        return found;
    }
}
