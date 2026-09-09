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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesCollapse;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;

/**
 * Optimizer coverage for PromQL result ordering: an {@code OrderBy} above
 * {@link TimeSeriesCollapse} fuses to {@link TopN}, and synthetic sort-key
 * columns are stripped from the final output.
 */
public class PromqlSortSpikeTests extends AbstractPromqlPlanOptimizerTests {

    public void testSortInstantQueryOptimizesToTopNAboveCollapse() {
        assumeTrue("Requires PROMQL_SORT capability", EsqlCapabilities.Cap.PROMQL_SORT.isEnabled());
        LogicalPlan optimized = planPromql(
            "PROMQL index=k8s time=\"2024-05-10T00:03:00.000Z\" value=(sort(network.bytes_in)) | TS_COLLAPSE"
        );

        List<TopN> topNsAboveCollapse = optimized.collect(TopN.class)
            .stream()
            .filter(topN -> topN.collect(TimeSeriesCollapse.class).isEmpty() == false)
            .toList();
        assertEquals("expected a TopN above TimeSeriesCollapse. Optimized plan:\n" + planString(optimized), 1, topNsAboveCollapse.size());
        List<String> outputNames = optimized.output().stream().map(Attribute::name).toList();
        assertEquals(List.of("value", PromqlCommand.STEP, MetadataAttribute.TIMESERIES), outputNames);
    }

    public void testSortDescInstantQueryDoesNotLeakSyntheticNanKey() {
        assumeTrue("Requires PROMQL_SORT_DESC capability", EsqlCapabilities.Cap.PROMQL_SORT_DESC.isEnabled());
        LogicalPlan optimized = planPromql(
            "PROMQL index=k8s time=\"2024-05-10T00:03:00.000Z\" value=(sort_desc(network.bytes_in)) | TS_COLLAPSE"
        );

        List<TopN> topNsAboveCollapse = optimized.collect(TopN.class)
            .stream()
            .filter(topN -> topN.collect(TimeSeriesCollapse.class).isEmpty() == false)
            .toList();
        assertEquals("expected a TopN above TimeSeriesCollapse. Optimized plan:\n" + planString(optimized), 1, topNsAboveCollapse.size());
        List<String> outputNames = optimized.output().stream().map(Attribute::name).toList();
        assertEquals(List.of("value", PromqlCommand.STEP, MetadataAttribute.TIMESERIES), outputNames);
        String keyName = Attribute.rawTemporaryName("promql_sort", "nan");
        assertFalse(
            "synthetic sort-key [" + keyName + "] leaked into output. Optimized plan:\n" + planString(optimized),
            outputNames.contains(keyName)
        );
    }

    public void testSyntheticSortKeyAboveCollapseOptimizesToTopNWithoutLeakingKey() {
        LogicalPlan parsed = TEST_PARSER.parseQuery(
            "PROMQL index=k8s time=\"2024-05-10T00:03:00.000Z\" value=(network.bytes_in) | TS_COLLAPSE"
        );
        TimeSeriesCollapse collapse = as(parsed, TimeSeriesCollapse.class);

        String keyName = Attribute.rawTemporaryName("promql_sort", "nan");
        LogicalPlan wrapped = wrapSortAboveCollapse(collapse, keyName);

        LogicalPlan analyzed = tsAnalyzer().buildAnalyzer().analyze(wrapped);
        LogicalPlan optimized = logicalOptimizer.optimize(analyzed);

        List<TopN> topNsAboveCollapse = optimized.collect(TopN.class)
            .stream()
            .filter(topN -> topN.collect(TimeSeriesCollapse.class).isEmpty() == false)
            .toList();
        assertEquals("expected a TopN above TimeSeriesCollapse. Optimized plan:\n" + planString(optimized), 1, topNsAboveCollapse.size());

        List<String> outputNames = optimized.output().stream().map(Attribute::name).toList();
        assertEquals(
            "synthetic sort-key must be absent from output. Optimized plan:\n" + planString(optimized),
            List.of("value", MetadataAttribute.TIMESERIES, PromqlCommand.STEP),
            outputNames
        );
        assertFalse(
            "synthetic sort-key [" + keyName + "] leaked into output. Optimized plan:\n" + planString(optimized),
            outputNames.contains(keyName)
        );
    }

    /**
     * Prometheus REST analyzed shape: {@code Limit > Eval[TO_LONG(step)] > OrderBy[$$k ASC, value DESC]
     * > Eval[$$k = value != value, synthetic] > TimeSeriesCollapse > PromqlCommand}.
     * {@code AddImplicitLimit} still wraps an outer Limit during analysis; that matches a user LIMIT.
     */
    private static LogicalPlan wrapSortAboveCollapse(TimeSeriesCollapse collapse, String keyName) {
        Attribute value = collapse.value();
        Alias keyAlias = new Alias(Source.EMPTY, keyName, new NotEquals(Source.EMPTY, value, value), null, true);
        Eval keyEval = new Eval(Source.EMPTY, collapse, List.of(keyAlias));
        OrderBy orderBy = new OrderBy(
            Source.EMPTY,
            keyEval,
            List.of(
                new Order(Source.EMPTY, keyAlias.toAttribute(), Order.OrderDirection.ASC, Order.NullsPosition.LAST),
                new Order(Source.EMPTY, value, Order.OrderDirection.DESC, Order.NullsPosition.LAST)
            )
        );
        Alias stepAlias = new Alias(Source.EMPTY, PromqlCommand.STEP, new ToLong(Source.EMPTY, collapse.step()));
        Eval stepEval = new Eval(Source.EMPTY, orderBy, List.of(stepAlias));
        return new Limit(Source.EMPTY, new Literal(Source.EMPTY, 10000, DataType.INTEGER), stepEval);
    }

    private static String planString(LogicalPlan plan) {
        return plan.toString(Node.NodeStringFormat.FULL);
    }
}
