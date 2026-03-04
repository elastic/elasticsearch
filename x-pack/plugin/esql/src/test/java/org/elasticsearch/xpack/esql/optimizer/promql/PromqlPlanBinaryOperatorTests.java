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
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.time.Duration;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PromqlPlanBinaryOperatorTests extends AbstractPromqlPlanOptimizerTests {

    public void testConstantFoldingArithmeticOperators() {
        var plan = planPromqlExpectNoReferences("PROMQL index=k8s step=5m 1 + 1");
        var eval = plan.collect(Eval.class).getFirst();
        var literal = as(eval.fields().getFirst().child(), Literal.class);
        assertThat(literal.value(), equalTo(2.0));
    }

    public void testBinaryArithmeticScalarFunctions() {
        assertConstantResult("pi() - pi()", equalTo(0.0));
    }

    public void testScalarAndInstantVectorArithmeticOperators() {
        LogicalPlan plan;
        plan = planPromql("PROMQL index=k8s step=5m max(network.bytes_in / 1024) by (pod)");
        Div div = plan.collect(Eval.class)
            .stream()
            .map(e -> e.fields().getLast().child())
            .filter(Div.class::isInstance)
            .map(Div.class::cast)
            .findFirst()
            .get();
        assertThat(div.left().sourceText(), equalTo("network.bytes_in"));
        assertThat(as(div.right(), Literal.class).value(), equalTo(1024.0));
    }

    public void testBinaryInstantSelectorAndLiteral() {
        var plan = planPromql("PROMQL index=k8s step=1m bits=(network.bytes_in * 8)");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("bits", "step", "_timeseries")));

        Mul mul = plan.collect(Eval.class)
            .stream()
            .map(e -> e.fields().getLast().child())
            .filter(Mul.class::isInstance)
            .map(Mul.class::cast)
            .findFirst()
            .get();
        assertThat(as(as(mul.left(), ToDouble.class).field(), ReferenceAttribute.class).sourceText(), equalTo("network.bytes_in"));
        assertThat(as(mul.right(), Literal.class).fold(null), equalTo(8.0));

        TimeSeriesAggregate tsAgg = plan.collect(TimeSeriesAggregate.class).getFirst();
        LastOverTime last = as(Alias.unwrap(tsAgg.aggregates().getFirst()), LastOverTime.class);
        assertThat(as(last.field(), FieldAttribute.class).sourceText(), equalTo("network.bytes_in"));
    }

    public void testBinaryArithmeticInstantSelectorAndScalarFunction() {
        boolean piFirst = randomBoolean();
        LogicalPlan plan;
        if (piFirst) {
            plan = planPromql("PROMQL index=k8s step=1m bits=(pi() - network.bytes_in)");
        } else {
            plan = planPromql("PROMQL index=k8s step=1m bits=(network.bytes_in - pi())");
        }
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("bits", "step", "_timeseries")));

        Sub sub = plan.collect(Eval.class)
            .stream()
            .map(e -> e.fields().getLast().child())
            .filter(Sub.class::isInstance)
            .map(Sub.class::cast)
            .findFirst()
            .get();
        Expression piExpression = piFirst ? sub.left() : sub.right();
        assertThat((double) as(piExpression, Literal.class).fold(null), closeTo(Math.PI, 1e-9));

        Expression bytesInExpression = piFirst ? sub.right() : sub.left();
        assertThat(as(as(bytesInExpression, ToDouble.class).field(), ReferenceAttribute.class).sourceText(), equalTo("network.bytes_in"));

        TimeSeriesAggregate tsAgg = plan.collect(TimeSeriesAggregate.class).getFirst();
        LastOverTime last = as(Alias.unwrap(tsAgg.aggregates().getFirst()), LastOverTime.class);
        assertThat(as(last.field(), FieldAttribute.class).sourceText(), equalTo("network.bytes_in"));
    }

    public void testTopLevelBinaryArithmeticQuery() {
        var plan = planPromql("""
            PROMQL index=k8s step=1m in_n_out=(
                network.eth0.rx + network.eth0.tx
              )
            | SORT in_n_out""");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("in_n_out", "step", "_timeseries")));
        Add add = plan.collect(Eval.class)
            .stream()
            .map(e -> e.fields().getLast().child())
            .filter(Add.class::isInstance)
            .map(Add.class::cast)
            .findFirst()
            .get();
        assertThat(add.children().stream().map(Expression::sourceText).toList(), containsInAnyOrder("network.eth0.rx", "network.eth0.tx"));
    }

    public void testBinaryAcrossSeriesAndLiteral() {
        var plan = planPromql("PROMQL index=k8s step=1m bits=(max(network.total_bytes_in) * 8)");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("bits", "step")));

        Eval eval = plan.collect(Eval.class).getFirst();
        Mul mul = as(eval.fields().getFirst().child(), Mul.class);
        assertThat(mul.left().sourceText(), equalTo("max(network.total_bytes_in)"));
        assertThat(as(mul.right(), Literal.class).fold(null), equalTo(8.0));

        Aggregate agg = eval.collect(Aggregate.class).getFirst();
        Max max = as(Alias.unwrap(agg.aggregates().getFirst()), Max.class);
        assertThat(as(max.field(), ReferenceAttribute.class).sourceText(), equalTo("network.total_bytes_in"));

        TimeSeriesAggregate tsAgg = agg.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(tsAgg.timeBucket().buckets().fold(null), equalTo(Duration.ofMinutes(1)));
        LastOverTime last = as(Alias.unwrap(tsAgg.aggregates().getFirst()), LastOverTime.class);
        assertThat(as(last.field(), FieldAttribute.class).sourceText(), equalTo("network.total_bytes_in"));
    }

    public void testAcrossSeriesMultiplicationLiteral() {
        var plan = planPromql("PROMQL index=k8s step=1m bits=(max(network.total_bytes_in * 8))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("bits", "step")));

        Aggregate agg = plan.collect(Aggregate.class).getFirst();
        Max max = as(Alias.unwrap(agg.aggregates().getFirst()), Max.class);
        assertThat(as(max.field(), ReferenceAttribute.class).sourceText(), equalTo("network.total_bytes_in * 8"));

        Eval eval = agg.collect(Eval.class).getFirst();
        Mul mul = as(Alias.unwrap(eval.fields().getFirst().child()), Mul.class);
        assertThat(mul.left().sourceText(), equalTo("network.total_bytes_in"));
        assertThat(as(mul.right(), Literal.class).fold(null), equalTo(8.0));

        TimeSeriesAggregate tsAgg = eval.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(tsAgg.timeBucket().buckets().fold(null), equalTo(Duration.ofMinutes(1)));
        LastOverTime last = as(Alias.unwrap(tsAgg.aggregates().getFirst()), LastOverTime.class);
        assertThat(as(last.field(), FieldAttribute.class).sourceText(), equalTo("network.total_bytes_in"));
    }

    public void testBinaryAcrossSeriesAggregations() {
        var plan = planPromql("PROMQL index=k8s step=1m ratio=(sum(network.total_bytes_in) / max(network.total_bytes_in))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("ratio", "step")));

        // Find the outer Aggregate (not TimeSeriesAggregate) that should contain both sum and max
        var outerAggs = plan.collect(Aggregate.class).stream().filter(a -> a instanceof TimeSeriesAggregate == false).toList();
        assertThat("binary agg expressions should fold into a single outer Aggregate", outerAggs, hasSize(1));

        var aggregate = outerAggs.getFirst();
        // Aggregates should contain both sum and max
        assertThat(aggregate.aggregates().stream().filter(e -> e.anyMatch(Sum.class::isInstance)).count(), equalTo(1L));
        assertThat(aggregate.aggregates().stream().filter(e -> e.anyMatch(Max.class::isInstance)).count(), equalTo(1L));
    }

    public void testBinaryAcrossSeriesAggregationsDoNotLoseReferences() {
        // Verifies that both aggregate expressions are preserved when folding (using different fields)
        var plan = planPromql("PROMQL index=k8s step=1m ratio=(sum(network.total_bytes_in) / max(network.bytes_in))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("ratio", "step")));

        var outerAggs = plan.collect(Aggregate.class).stream().filter(a -> a instanceof TimeSeriesAggregate == false).toList();
        assertThat("both aggregations should be folded into single outer Aggregate", outerAggs, hasSize(1));

        var aggregate = outerAggs.getFirst();
        assertThat(aggregate.aggregates().stream().filter(e -> e.anyMatch(Sum.class::isInstance)).count(), equalTo(1L));
        assertThat(aggregate.aggregates().stream().filter(e -> e.anyMatch(Max.class::isInstance)).count(), equalTo(1L));
    }

    public void testNestedBinaryAggregationsWithScalar() {
        // Pattern: (agg op agg) op scalar
        var plan = planPromql("PROMQL index=k8s step=1m result=(sum(network.total_bytes_in) / max(network.total_bytes_in) * 100)");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step")));

        var outerAggs = plan.collect(Aggregate.class).stream().filter(a -> a instanceof TimeSeriesAggregate == false).toList();
        assertThat("all aggregations should fold into single outer Aggregate", outerAggs, hasSize(1));
    }

    public void testFunctionOnBinaryAggregations() {
        // Pattern: func(agg op agg) - tests that Eval nodes for function are preserved
        var plan = planPromql("PROMQL index=k8s step=1m result=(ceil(sum(network.total_bytes_in) / max(network.total_bytes_in)))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step")));

        var outerAggs = plan.collect(Aggregate.class).stream().filter(a -> a instanceof TimeSeriesAggregate == false).toList();
        assertThat("aggregations should fold into single outer Aggregate", outerAggs, hasSize(1));

        // Verify ceil is applied via Eval
        var evals = plan.collect(Eval.class);
        assertThat("should have Eval nodes for ceil and value conversion", evals.size(), org.hamcrest.Matchers.greaterThanOrEqualTo(1));
    }

    public void testBinaryAggregationsWithAddition() {
        // Two aggregates combined with addition
        var plan = planPromql("PROMQL index=k8s step=1m result=(sum(network.total_bytes_in) + max(network.total_bytes_in))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step")));

        var outerAggs = plan.collect(Aggregate.class).stream().filter(a -> a instanceof TimeSeriesAggregate == false).toList();
        assertThat("all aggregations should fold into single outer Aggregate", outerAggs, hasSize(1));
    }

    public void testComparisonAcrossSeriesWithScalar() {
        var plan = planPromql("PROMQL index=k8s step=1m max(network.eth0.rx) > 1000");
        GreaterThan gt = plan.collect(Filter.class)
            .stream()
            .map(Filter::condition)
            .filter(GreaterThan.class::isInstance)
            .map(GreaterThan.class::cast)
            .findAny()
            .get();
        assertThat(gt.left().sourceText(), equalTo("max(network.eth0.rx)"));
        assertThat(as(gt.right(), Literal.class).fold(null), equalTo(1000.0));

        Aggregate acrossSeries = plan.collect(Aggregate.class).getFirst();
        Max max = as(Alias.unwrap(acrossSeries.aggregates().getFirst()), Max.class);
        assertThat(as(max.field(), ReferenceAttribute.class).sourceText(), equalTo("network.eth0.rx"));
    }
}
