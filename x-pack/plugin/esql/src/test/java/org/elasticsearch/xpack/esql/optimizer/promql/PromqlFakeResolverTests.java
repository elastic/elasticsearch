/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;

import java.util.List;

import static java.util.function.Predicate.not;
import static org.hamcrest.Matchers.contains;

public class PromqlFakeResolverTests extends AbstractLogicalPlanOptimizerTests {

    private final PromqlFakeResolver resolver = new PromqlFakeResolver();

    public void testSimpleQuery() {
        var attributes = extractAttributes("PROMQL step=1m foo");
        assertThat(gauges(attributes), contains("foo"));
    }

    public void testWithLabelFilter() {
        var attributes = extractAttributes("PROMQL step=1m foo{job=\"api-server\"}");
        assertThat(gauges(attributes), contains("foo"));
        assertThat(labels(attributes), contains("job"));
    }

    public void testWithRateFunction() {
        var attributes = extractAttributes("PROMQL step=1m rate(foo[5m])");
        assertThat(counters(attributes), contains("foo"));
    }

    public void testGroupingAggregate() {
        var attributes = extractAttributes("PROMQL step=1m sum by (job) (foo)");
        assertThat(gauges(attributes), contains("foo"));
        assertThat(labels(attributes), contains("job"));
    }

    private List<Attribute> extractAttributes(String query) {
        var plan = parser.parseQuery(query);
        plan = resolver.apply(plan);
        plan = analyzer.analyze(plan);
        plan = logicalOptimizer.optimize(plan);
        return plan.collect(LeafPlan.class).getFirst().output();
    }

    private List<String> gauges(List<Attribute> attributes) {
        return attributes.stream()
            .filter(Attribute::isMetric)
            .filter(attribute -> attribute.dataType().isNumeric())
            .map(Attribute::name)
            .toList();
    }

    private List<String> counters(List<Attribute> attributes) {
        return attributes.stream()
            .filter(Attribute::isMetric)
            .filter(attribute -> attribute.dataType().isCounter())
            .map(Attribute::name)
            .toList();
    }

    private List<String> labels(List<Attribute> attributes) {
        return attributes.stream()
            .filter(Attribute::isDimension)
            .filter(not(a -> a.name().equals("_timeseries")))
            .map(Attribute::name)
            .toList();
    }
}
