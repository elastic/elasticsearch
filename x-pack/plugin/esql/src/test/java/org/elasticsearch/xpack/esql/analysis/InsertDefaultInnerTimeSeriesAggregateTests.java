/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;

import java.util.Locale;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.equalToIgnoringIds;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.ignoreIds;

public class InsertDefaultInnerTimeSeriesAggregateTests extends AbstractLogicalPlanOptimizerTests {

    private final InsertDefaultInnerTimeSeriesAggregate rule = new InsertDefaultInnerTimeSeriesAggregate();

    public void testSimpleImplicitOverTime() {
        assertStatsEqual("sum(network.bytes_in)", "sum(last_over_time(network.bytes_in))");
    }

    public void testBinaryWithImplicitAndExplicitOverTime() {
        var expected = "sum(last_over_time(network.eth0.tx) + last_over_time(network.eth0.rx))";
        assertStatsEqual("sum(network.eth0.tx + network.eth0.rx)", expected);
        assertStatsEqual("sum(network.eth0.tx + last_over_time(network.eth0.rx))", expected);
        assertStatsEqual("sum(last_over_time(network.eth0.tx) + network.eth0.rx)", expected);
        assertStatsEqual(expected, expected);
    }

    public void testComplexArithmetic() {
        var expected = "sum(last_over_time(network.eth0.tx) / 2 + last_over_time(network.eth0.rx) * 2)";
        assertStatsEqual("sum(network.eth0.tx / 2 + network.eth0.rx * 2)", expected);
        assertStatsEqual("sum(last_over_time(network.eth0.tx) / 2 + network.eth0.rx * 2)", expected);
        assertStatsEqual("sum(network.eth0.tx / 2 + last_over_time(network.eth0.rx) * 2)", expected);
        assertStatsEqual(expected, expected);
    }

    private void assertStatsEqual(String stats1, String stats2) {
        var baseQuery = """
            TS k8s
            | STATS %s BY bucket(@timestamp, 1 minute)
            | LIMIT 10
            """;
        var plan1 = rule.apply(metricsAnalyzer.analyze(parser.parseQuery(String.format(Locale.ROOT, baseQuery, stats1))));
        Function<Alias, Expression> ignoreAliasName = (Alias a) -> new Alias(a.source(), "dummy", a.child(), a.id());
        plan1 = plan1.transformExpressionsDown(Alias.class, ignoreAliasName);
        var plan2 = metricsAnalyzer.analyze(parser.parseQuery(String.format(Locale.ROOT, baseQuery, stats2)));
        plan2 = plan2.transformExpressionsDown(Alias.class, ignoreAliasName);
        assertThat(ignoreIds(plan1), equalToIgnoringIds(plan2));
    }

}
