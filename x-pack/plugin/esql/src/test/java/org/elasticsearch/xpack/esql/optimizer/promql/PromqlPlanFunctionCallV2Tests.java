/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Scalar;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.ToTimeSeriesMetadata;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/** PromQL functions translated over a label container. */
public class PromqlPlanFunctionCallV2Tests extends AbstractPromqlPlanOptimizerTests {

    public PromqlPlanFunctionCallV2Tests(VersionMode versionMode) {
        super(versionMode);
    }

    /** A function over a range keeps the labels of each series: the collapse is keyed by the source's label container. */
    public void testRangeFunctionKeepsTheSourceLabels() {
        LogicalPlan plan = planPromqlV2("PROMQL index=k8s step=5m rate(network.total_bytes_in[5m])", false);

        TimeSeriesAggregate collapse = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(expressions(collapse, Rate.class), hasSize(1));
        List<ToTimeSeriesMetadata> containers = expressions(collapse, ToTimeSeriesMetadata.class);
        assertThat(containers, hasSize(1));
        TimeSeriesWithout source = as(containers.getFirst().field(), TimeSeriesWithout.class);
        assertThat(source.children(), empty());
        assertThat(names(plan.output()), hasItem("_timeseries"));
    }

    /** A function over an instant vector keeps the labels of each series. */
    public void testInstantFunctionKeepsTheSourceLabels() {
        LogicalPlan plan = planPromqlV2("PROMQL index=k8s step=5m abs(network.bytes_in)", false);

        assertThat(expressions(plan, Abs.class), hasSize(1));
        assertThat(expressions(plan, ToTimeSeriesMetadata.class), hasSize(1));
        assertThat(names(plan.output()), hasItem("_timeseries"));
    }

    /** The translated function plans through the optimizer. */
    public void testRangeFunctionOptimizes() {
        LogicalPlan plan = planPromqlV2("PROMQL index=k8s step=5m rate(network.total_bytes_in[5m])", true);
        assertThat(plan.collect(TimeSeriesAggregate.class), hasSize(1));
    }

    /** {@code scalar} returns one value per step, without labels. */
    public void testScalarDropsTheLabels() {
        LogicalPlan plan = planPromqlV2("PROMQL index=k8s step=1h scalar(network.bytes_in)", false);

        assertThat(expressions(plan, Scalar.class), hasSize(1));
        assertThat(expressions(plan, ToTimeSeriesMetadata.class), empty());
        assertThat(names(plan.output()), not(hasItem("_timeseries")));
    }

    /** A scalar function has no labels. */
    public void testTimeHasNoLabels() {
        LogicalPlan plan = planPromqlV2("PROMQL index=k8s step=1h time()", false);
        assertThat(expressions(plan, ToTimeSeriesMetadata.class), empty());
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
