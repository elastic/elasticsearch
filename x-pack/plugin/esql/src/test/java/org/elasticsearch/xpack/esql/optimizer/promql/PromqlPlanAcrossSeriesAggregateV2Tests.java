/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.AbstractTimeSeriesMetadataFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataFilter;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/** PromQL aggregations across series translated over a label container. */
public class PromqlPlanAcrossSeriesAggregateV2Tests extends AbstractPromqlPlanOptimizerTests {

    public PromqlPlanAcrossSeriesAggregateV2Tests(VersionMode versionMode) {
        super(versionMode);
    }

    /** {@code by(...)} keys each group by the listed labels of its series, read from their fields. */
    public void testByKeepsTheListedLabels() {
        LogicalPlan plan = planPromqlV2("PROMQL index=k8s step=1h sum by (cluster) (network.bytes_in)", false);

        TimeSeriesAggregate collapse = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(names(collapse.aggregates().stream().flatMap(a -> a.references().stream()).toList()), hasItem("cluster"));
        assertThat(expressions(plan, AbstractTimeSeriesMetadataFunction.class), empty());
        assertThat(names(plan.output()), hasItem("cluster"));
        assertThat(names(plan.output()), not(hasItem("_timeseries")));
    }

    /** {@code without(...)} keys each group by the labels of its series but the listed ones: the source leaves them out. */
    public void testWithoutLeavesTheListedLabelsOut() {
        LogicalPlan plan = planPromqlV2("PROMQL index=k8s step=1h sum without (pod) (network.bytes_in)", false);

        TimeSeriesAggregate collapse = plan.collect(TimeSeriesAggregate.class).getFirst();
        TimeSeriesWithout source = expressions(collapse, TimeSeriesWithout.class).getFirst();
        assertThat(names(source.children()), contains("pod"));
        assertThat(names(plan.output()), hasItem("_timeseries"));
    }

    /** An aggregation without grouping has no labels. */
    public void testAggregationWithoutGroupingHasNoLabels() {
        LogicalPlan plan = planPromqlV2("PROMQL index=k8s step=1h sum(network.bytes_in)", false);

        assertThat(expressions(plan, TimeSeriesMetadataFilter.class), empty());
        assertThat(names(plan.output()), not(hasItem("_timeseries")));
    }

    /** An aggregation over an aggregation regroups the collapsed table. */
    public void testAggregationOverAggregationRegroups() {
        LogicalPlan plan = planPromqlV2("PROMQL index=k8s step=1h max(sum by (cluster) (network.bytes_in))", false);

        assertThat(plan.collect(TimeSeriesAggregate.class), hasSize(1));
        Aggregate regroup = plan.collect(Aggregate.class)
            .stream()
            .filter(a -> a instanceof TimeSeriesAggregate == false)
            .findFirst()
            .orElseThrow();
        assertThat(regroup.child().collect(TimeSeriesAggregate.class), hasSize(1));
    }

    /** The translated aggregations plan through the optimizer. */
    public void testAggregationsOptimize() {
        for (String query : List.of(
            "sum by (cluster) (network.bytes_in)",
            "sum without (pod) (rate(network.total_bytes_in[5m]))",
            "count(network.bytes_in)",
            "max(sum by (cluster) (network.bytes_in))"
        )) {
            LogicalPlan plan = planPromqlV2("PROMQL index=k8s step=1h " + query, true);
            assertThat(query, plan.collect(TimeSeriesAggregate.class), hasSize(1));
        }
    }

    /**
     * A label the source's container does not hold by its name - a passthrough alias of {@code attributes.cpu} - is read from its
     * field, which the shard resolves.
     */
    public void testPassthroughLabelReadsItsField() {
        LogicalPlan plan = planPromqlV2("PROMQL index=otel-metrics step=1h sum by (cpu) (metrics.system.cpu.time)", false);

        TimeSeriesAggregate collapse = plan.collect(TimeSeriesAggregate.class).getFirst();
        FieldAttribute field = collapse.aggregates()
            .stream()
            .flatMap(a -> a.references().stream())
            .filter(a -> a.name().equals("cpu"))
            .map(a -> as(a, FieldAttribute.class))
            .findFirst()
            .orElseThrow();
        assertThat(field.fieldName().string(), equalTo("cpu"));
        assertThat(expressions(plan, AbstractTimeSeriesMetadataFunction.class), empty());
        assertThat(names(plan.output()), hasItem("cpu"));
    }

    /** A label that is not a dimension, which the source's container does not hold, is read from its field. */
    public void testLabelOutsideTheContainerReadsItsField() {
        LogicalPlan plan = planPromqlV2("PROMQL index=k8s step=1h sum by (event) (network.bytes_in)", false);

        TimeSeriesAggregate collapse = plan.collect(TimeSeriesAggregate.class).getFirst();
        Values values = expressions(collapse, Values.class).stream()
            .filter(v -> v.field() instanceof FieldAttribute field && field.name().equals("event"))
            .findFirst()
            .orElseThrow();
        assertThat(as(values.field(), FieldAttribute.class).isDimension(), is(false));
        assertThat(expressions(plan, AbstractTimeSeriesMetadataFunction.class), empty());
        assertThat(names(plan.output()), hasItem("event"));
    }

    private static List<String> names(List<? extends Expression> expressions) {
        return expressions.stream()
            .map(e -> e instanceof Attribute attribute ? attribute.name() : BytesRefs.toString(((Literal) e).value()))
            .toList();
    }

    private static <E extends Expression> List<E> expressions(LogicalPlan plan, Class<E> type) {
        var found = new ArrayList<E>();
        plan.forEachExpressionDown(type, found::add);
        return found;
    }
}
