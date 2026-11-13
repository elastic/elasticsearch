/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.promql.function.RegexExpand;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/** {@code label_replace} and {@code label_join} translated over a label container. */
public class PromqlPlanMetadataManipulationFunctionV2Tests extends AbstractPromqlPlanOptimizerTests {

    public PromqlPlanMetadataManipulationFunctionV2Tests(VersionMode versionMode) {
        super(versionMode);
    }

    /** {@code label_replace} writes the expanded source label under the destination. */
    public void testLabelReplaceWritesTheDestination() {
        LogicalPlan plan = planPromql(
            "PROMQL index=k8s step=1h label_replace(sum by (pod) (network.bytes_in), \"dst\", \"$1\", \"pod\", \"(.*)\")",
            false
        );

        assertThat(expressions(destination(plan), RegexExpand.class), not(empty()));
        assertThat(names(plan.output()), hasItems("pod", "dst"));
    }

    /** {@code label_join} writes the joined source labels under the destination. */
    public void testLabelJoinWritesTheDestination() {
        LogicalPlan plan = planPromql(
            "PROMQL index=k8s step=1h label_join(sum by (cluster, pod) (network.bytes_in), \"dst\", \"-\", \"cluster\", \"pod\")",
            false
        );

        assertThat(expressions(destination(plan), Concat.class), not(empty()));
        assertThat(names(plan.output()), hasItems("cluster", "pod", "dst"));
    }

    /** {@code label_join} without source labels writes an empty value, which removes the destination like Prometheus. */
    public void testLabelJoinWithoutSourcesWritesAnEmptyValue() {
        LogicalPlan plan = planPromql("PROMQL index=k8s step=1h label_join(sum by (cluster) (network.bytes_in), \"dst\", \"-\")", false);

        assertThat(as(destination(plan), Literal.class).value(), nullValue());
    }

    /** The translated label functions plan through the optimizer. */
    public void testLabelFunctionsOptimize() {
        for (String query : List.of(
            "label_replace(sum by (pod) (network.bytes_in), \"dst\", \"$1\", \"pod\", \"(.*)\")",
            "label_join(sum by (cluster, pod) (network.bytes_in), \"dst\", \"-\", \"cluster\", \"pod\")",
            "sum by (dst) (label_replace(sum by (pod) (network.bytes_in), \"dst\", \"$1\", \"pod\", \"(.*)\"))",
            // the series id keys the collapse under the label it writes
            "sum by (dst) (label_replace(network.bytes_in, \"dst\", \"$1\", \"region\", \"(.+)\"))"
        )) {
            LogicalPlan plan = planPromql("PROMQL index=k8s step=1h " + query, true);
            assertThat(query, plan.collect(TimeSeriesAggregate.class), hasSize(1));
        }
    }

    /** The value the plan writes under the destination label {@code dst}. */
    private static Expression destination(LogicalPlan plan) {
        return expressions(plan, Alias.class).stream().filter(alias -> alias.name().equals("dst")).findFirst().orElseThrow().child();
    }

    private static List<String> names(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::name).toList();
    }

    private static <E extends Expression> List<E> expressions(LogicalPlan plan, Class<E> type) {
        var found = new ArrayList<E>();
        plan.forEachExpressionDown(type, found::add);
        return found;
    }

    private static <E extends Expression> List<E> expressions(Expression expression, Class<E> type) {
        var found = new ArrayList<E>();
        expression.forEachDown(type, found::add);
        return found;
    }
}
