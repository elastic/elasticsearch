/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;

public class InsertDefaultInnerTimeSeriesAggregateTests extends AbstractLogicalPlanOptimizerTests {

    private final InsertDefaultInnerTimeSeriesAggregate rule = new InsertDefaultInnerTimeSeriesAggregate();

    public void testTranslateSumWithImplicitLastOverTime() {
        var query = """
            TS k8s | STATS sum(network.bytes_in) BY bucket(@timestamp, 1 minute)
            | LIMIT 10
            """;
        var plan = rule.apply(metricsAnalyzer.analyze(parser.parseQuery(query)));
        TimeSeriesAggregate aggsByTsid = plan.collect(TimeSeriesAggregate.class).getFirst();

        Sum sum = as(Alias.unwrap(aggsByTsid.aggregates().getFirst()), Sum.class);
        LastOverTime lastOverTime = as(sum.field(), LastOverTime.class);
        assertThat(Expressions.attribute(lastOverTime.field()).name(), equalTo("network.bytes_in"));
    }

    public void testTranslateSumWithAdditionWithImplicitLastOverTime() {
        var query = """
            TS k8s
            | STATS %s BY bucket(@timestamp, 1 minute)
            | LIMIT 10
            """;
        for (String variant : List.of(
            "sum(network.eth0.tx + network.eth0.rx)",
            "sum(network.eth0.tx + last_over_time(network.eth0.rx))",
            "sum(last_over_time(network.eth0.tx) + network.eth0.rx)",
            "sum(last_over_time(network.eth0.tx) + last_over_time(network.eth0.rx))"
        )) {
            var plan = rule.apply(metricsAnalyzer.analyze(parser.parseQuery(String.format(Locale.ROOT, query, variant))));

            TimeSeriesAggregate aggsByTsid = plan.collect(TimeSeriesAggregate.class).getFirst();
            Sum sum = as(Alias.unwrap(aggsByTsid.aggregates().getFirst()), Sum.class);
            Add add = as(sum.field(), Add.class);
            LastOverTime lastOverTimeTx = as(add.left(), LastOverTime.class);
            assertThat(Expressions.attribute(lastOverTimeTx.field()).name(), equalTo("network.eth0.tx"));

            LastOverTime lastOverTimeRx = as(add.right(), LastOverTime.class);
            assertThat(Expressions.attribute(lastOverTimeRx.field()).name(), equalTo("network.eth0.rx"));
        }
    }

}
