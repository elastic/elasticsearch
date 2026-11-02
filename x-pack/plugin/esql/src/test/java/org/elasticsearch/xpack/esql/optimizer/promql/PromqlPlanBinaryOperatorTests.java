/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Present;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesFromKeyword;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesCollapse;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.HashJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class PromqlPlanBinaryOperatorTests extends AbstractPromqlPlanOptimizerTests {

    public PromqlPlanBinaryOperatorTests(VersionMode versionMode) {
        super(versionMode);
    }

    public void testConstantFoldingArithmeticOperators() {
        var plan = planPromql("PROMQL index=k8s step=5m 1 + 1");
        var eval = plan.collect(Eval.class).getFirst();
        var literal = as(eval.fields().getFirst().child(), Literal.class);
        assertThat(literal.value(), equalTo(2.0));
    }

    public void testBinaryArithmeticScalarFunctions() {
        assertConstantResult("pi() - pi()", equalTo(0.0));
    }

    public void testFoldableScalarInstantQueryDoesNotTouchIndex() {
        var plan = planPromql("PROMQL index=empty_index time=\"2025-01-01T00:00:00Z\" result=(1 * 2 + 4 / 2)", false, false);

        assertInstantConstFolded(plan, List.of(1735689600000L));
    }

    public void testFoldableTimeInstantQueryDoesNotTouchIndex() {
        var plan = planPromql("PROMQL index=empty_index time=\"2025-01-01T00:00:00Z\" result=(time())", false, false);

        assertInstantConstFolded(plan, List.of(1735689600000L));
    }

    public void testFoldableTimeArithmeticInstantQueryDoesNotTouchIndex() {
        var plan = planPromql("PROMQL index=empty_index time=\"2025-01-01T00:00:00Z\" result=(time() + 60)", false, false);

        assertInstantConstFolded(plan, List.of(1735689600000L));
    }

    public void testFoldableTimeExtractionInstantQueryDoesNotTouchIndex() {
        var plan = planPromql("PROMQL index=empty_index time=\"2025-01-01T05:00:00Z\" result=(hour())", false, false);

        assertInstantConstFolded(plan, List.of(1735707600000L));
    }

    public void testFoldableValueTransformationInstantQueryDoesNotTouchIndex() {
        var plan = planPromql("PROMQL index=empty_index time=\"2025-01-01T00:00:00Z\" result=(round(vector(1.23), 0.1))", false, false);

        assertInstantConstFolded(plan, List.of(1735689600000L));
    }

    public void testFoldableBoolComparisonInstantQueryDoesNotTouchIndex() {
        var plan = planPromql("PROMQL index=empty_index time=\"2025-01-01T00:00:00Z\" result=(1 == bool 2)", false, false);

        assertInstantConstFolded(plan, List.of(1735689600000L));
    }

    public void testFoldableScalarRangeQueryDoesNotTouchIndex() {
        var plan = planPromql(
            "PROMQL index=empty_index start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T00:02:00Z\" step=1m result=(42)",
            false,
            false
        );

        Row row = plan.collect(Row.class).getFirst();
        assertThat(((Literal) row.fields().getLast().child()).value(), equalTo(List.of(1735689600000L, 1735689660000L, 1735689720000L)));
        assertThat(plan.collect(MvExpand.class), hasSize(1));
        assertNoIndexBackedPromqlPlan(plan);
    }

    public void testFoldableTimeRangeQueryDoesNotTouchIndex() {
        var plan = planPromql(
            "PROMQL index=empty_index start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T00:02:00Z\" step=1m result=(time())",
            false,
            false
        );

        Row row = plan.collect(Row.class).getFirst();
        assertThat(row.fields().getFirst().name(), equalTo("step"));
        assertThat(((Literal) row.fields().getFirst().child()).value(), equalTo(List.of(1735689600000L, 1735689660000L, 1735689720000L)));
        Eval eval = findEvalWithField(plan, "result");
        Div value = as(eval.fields().getFirst().child(), Div.class);
        assertThat(value.left(), instanceOf(ToDouble.class));
        assertThat(((Literal) value.right()).value(), equalTo(1000.0));
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step")));
        assertThat(plan.collect(MvExpand.class), hasSize(1));
        assertNoIndexBackedPromqlPlan(plan);
    }

    public void testFoldableTimeArithmeticRangeQueryDoesNotTouchIndex() {
        var plan = planPromql(
            "PROMQL index=empty_index start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T00:02:00Z\" step=1m result=(time() + 1)",
            false,
            false
        );

        Row row = plan.collect(Row.class).getFirst();
        assertThat(row.fields().getFirst().name(), equalTo("step"));
        assertThat(((Literal) row.fields().getFirst().child()).value(), equalTo(List.of(1735689600000L, 1735689660000L, 1735689720000L)));
        Eval eval = findEvalWithFieldAndExpression(plan, "result", Add.class);
        assertThat(eval.fields().getFirst().child(), instanceOf(Add.class));
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step")));
        assertThat(plan.collect(MvExpand.class), hasSize(1));
        assertNoIndexBackedPromqlPlan(plan);
    }

    public void testFoldableScalarRangeQueryWithBucketsDoesNotTouchIndex() {
        var plan = planPromql(
            "PROMQL index=empty_index start=\"2024-05-10T00:00:00Z\" end=\"2024-05-10T00:20:00Z\" buckets=4 result=(42)",
            false,
            false
        );

        Row row = plan.collect(Row.class).getFirst();
        assertThat(
            ((Literal) row.fields().getLast().child()).value(),
            equalTo(List.of(1715299200000L, 1715299500000L, 1715299800000L, 1715300100000L, 1715300400000L))
        );
        assertThat(plan.collect(MvExpand.class), hasSize(1));
        assertNoIndexBackedPromqlPlan(plan);
    }

    public void testFoldableScalarRangeQueryWithTsCollapseDoesNotTouchIndex() {
        var plan = planPromql(
            "PROMQL index=empty_index start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T00:02:00Z\" step=1m result=(1 + 1) | TS_COLLAPSE",
            false,
            false
        );

        TimeSeriesCollapse collapse = plan.collect(TimeSeriesCollapse.class).getFirst();
        Row row = collapse.child().collect(Row.class).getFirst();
        assertThat(((Literal) row.fields().getLast().child()).value(), equalTo(List.of(1735689600000L, 1735689660000L, 1735689720000L)));
        assertThat(plan.collect(MvExpand.class), hasSize(1));
        assertNoIndexBackedPromqlPlan(plan);
    }

    public void testFoldableTimeRangeQueryWithTsCollapseDoesNotTouchIndex() {
        var plan = planPromql(
            "PROMQL index=empty_index start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T00:02:00Z\" step=1m result=(time()) | TS_COLLAPSE",
            false,
            false
        );

        TimeSeriesCollapse collapse = plan.collect(TimeSeriesCollapse.class).getFirst();
        Row row = collapse.child().collect(Row.class).getFirst();
        assertThat(((Literal) row.fields().getFirst().child()).value(), equalTo(List.of(1735689600000L, 1735689660000L, 1735689720000L)));
        Eval eval = findEvalWithField(collapse.child(), "result");
        assertThat(eval.fields().getFirst().name(), equalTo("result"));
        assertThat(plan.collect(MvExpand.class), hasSize(1));
        assertNoIndexBackedPromqlPlan(plan);
    }

    public void testFoldableTimeArithmeticRangeQueryWithTsCollapseDoesNotTouchIndex() {
        var plan = planPromql(
            "PROMQL index=empty_index start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T00:02:00Z\" step=1m "
                + "result=(time() + 1) | TS_COLLAPSE",
            false,
            false
        );

        TimeSeriesCollapse collapse = plan.collect(TimeSeriesCollapse.class).getFirst();
        Row row = collapse.child().collect(Row.class).getFirst();
        assertThat(((Literal) row.fields().getFirst().child()).value(), equalTo(List.of(1735689600000L, 1735689660000L, 1735689720000L)));
        Eval eval = findEvalWithFieldAndExpression(collapse.child(), "result", Add.class);
        assertThat(eval.fields().getFirst().child(), instanceOf(Add.class));
        assertThat(plan.collect(MvExpand.class), hasSize(1));
        assertNoIndexBackedPromqlPlan(plan);
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

    public void testBinaryWithDifferentSelectorsPreserveDistinctAggregates() {
        // for mixed selectors, optimizer must not merge both sides into one selector.
        var plan = planPromql("PROMQL index=k8s step=1m result=(sum(avg_over_time(network.cost[1m]) + avg_over_time(network.cost[10m])))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step")));

        TimeSeriesAggregate tsAgg = plan.collect(TimeSeriesAggregate.class).getFirst();
        // Both aggregate components should survive with their original windows (1m and 10m).
        var sumWindows = tsAgg.aggregates()
            .stream()
            .map(Alias::unwrap)
            .flatMap(agg -> agg.collect(Sum.class).stream())
            .map(agg -> agg.window().fold(FoldContext.small()))
            .toList();
        var countWindows = tsAgg.aggregates()
            .stream()
            .map(Alias::unwrap)
            .flatMap(agg -> agg.collect(Count.class).stream())
            .map(agg -> agg.window().fold(FoldContext.small()))
            .toList();
        assertThat(sumWindows, hasSize(2));
        assertThat(countWindows, hasSize(2));
        assertThat(sumWindows, containsInAnyOrder(Duration.ofMinutes(1), Duration.ofMinutes(10)));
        assertThat(countWindows, containsInAnyOrder(Duration.ofMinutes(1), Duration.ofMinutes(10)));

        // The binary add must reference two distinct aggregate outputs, not the same ref twice (x + x).
        Add add = plan.collect(Eval.class)
            .stream()
            .flatMap(e -> e.fields().stream())
            .map(Alias::unwrap)
            .filter(Add.class::isInstance)
            .map(Add.class::cast)
            .findFirst()
            .orElseThrow();
        ReferenceAttribute leftRef = as(as(add.left(), ToDouble.class).field(), ReferenceAttribute.class);
        ReferenceAttribute rightRef = as(as(add.right(), ToDouble.class).field(), ReferenceAttribute.class);
        assertFalse(leftRef.semanticEquals(rightRef));
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

    public void testBinaryScalarAndNestedAggregationBroadcastsOnStep() {
        // Nested count(count by ...) next to scalar() cannot fold into one aggregate; the scalar broadcasts via a step join.
        var plan = planPromql(
            "PROMQL index=k8s step=1m result=(scalar(network.bytes_in) * 100 / count(count by (pod) (network.total_bytes_in)))"
        );
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step")));
        assertThat(plan.collect(HashJoin.class).isEmpty(), equalTo(false));
    }

    public void testNestedBinaryAggregationsWithScalar() {
        // Pattern: (agg op agg) op scalar
        var plan = planPromql("PROMQL index=k8s step=1m result=(sum(network.total_bytes_in) / max(network.total_bytes_in) * 100)");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step")));

        var outerAggs = plan.collect(Aggregate.class).stream().filter(a -> a instanceof TimeSeriesAggregate == false).toList();
        assertThat("all aggregations should fold into single outer Aggregate", outerAggs, hasSize(1));
    }

    public void testBinaryFilteredRateAggregationsDoNotLoseReferences() {
        var plan = planPromql(
            "PROMQL index=k8s step=10m value=("
                + "sum(rate(network.total_bytes_in{network.bytes_in =~\"1..\"}[10m])) / sum(rate(network.total_bytes_in[10m])) * 100)"
        );
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("value", "step")));

        var outerAggs = plan.collect(Aggregate.class).stream().filter(a -> a instanceof TimeSeriesAggregate == false).toList();
        assertThat("both rate sums should fold into single outer Aggregate", outerAggs, hasSize(1));

        var aggregate = outerAggs.getFirst();
        assertThat(aggregate.aggregates().stream().filter(e -> e.anyMatch(Sum.class::isInstance)).count(), equalTo(2L));
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
            .flatMap(f -> f.condition().collect(GreaterThan.class).stream())
            .findFirst()
            .orElseThrow();
        assertThat(gt.left().sourceText(), equalTo("max(network.eth0.rx)"));
        assertThat(as(gt.right(), Literal.class).fold(null), equalTo(1000.0));

        Aggregate acrossSeries = plan.collect(Aggregate.class).getFirst();
        Max max = as(Alias.unwrap(acrossSeries.aggregates().getFirst()), Max.class);
        assertThat(as(max.field(), ReferenceAttribute.class).sourceText(), equalTo("network.eth0.rx"));
    }

    private static void assertInstantConstFolded(LogicalPlan plan, List<Long> expectedSteps) {
        Row row = plan.collect(Row.class).getFirst();
        assertThat(((Literal) row.fields().getLast().child()).value(), equalTo(expectedSteps));
        assertThat(plan.collect(MvExpand.class), hasSize(1));
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step")));
        assertNoIndexBackedPromqlPlan(plan);
    }

    public void testVectorMatchOnProducesHashJoin() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        // `on (cluster)` matches 1:1 on cluster + step; no group_left/right so the join enforces uniqueness.
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum by (cluster) (network.eth0.tx) / on (cluster) sum by (cluster) (network.eth0.rx))"
        );
        var joins = plan.collect(HashJoin.class);
        assertThat(joins, hasSize(1));
        HashJoin join = joins.getFirst();
        assertThat(join.unique(), equalTo(true));
        assertThat(join.leftFields().getFirst().name(), equalTo("step"));
        assertThat(keyedLabels(join.left()), containsInAnyOrder("cluster"));
        assertThat(keyedLabels(join.right()), containsInAnyOrder("cluster"));
    }

    public void testNestedVectorMatchUsesCurrentOperandLabels() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=((sum by (cluster) (network.eth0.tx) / on (cluster) "
                + "sum by (cluster) (network.eth0.rx)) / ignoring (pod) sum by (cluster, region) (network.eth0.rx))"
        );
        var joins = plan.collect(HashJoin.class);
        assertThat(joins, hasSize(2));
        joins.forEach(join -> assertFalse(join.leftFields().stream().map(Attribute::name).toList().contains("region")));
    }

    public void testVectorMatchGroupLeftIsManyToOne() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        // group_left: LHS is the "many"/probe side, RHS the "one"/build side; the join is not unique.
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum by (cluster, pod) (network.eth0.tx) "
                + "/ on (cluster) group_left sum by (cluster) (network.eth0.rx))"
        );
        var joins = plan.collect(HashJoin.class);
        assertThat(joins, hasSize(1));
        HashJoin join = joins.getFirst();
        assertThat(join.unique(), equalTo(false));
        assertThat(keyedLabels(join.left()), containsInAnyOrder("cluster"));
        assertThat(keyedLabels(join.right()), containsInAnyOrder("cluster"));
    }

    public void testGroupLeftDoesNotExposeUnlistedBuildLabels() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum by (cluster) (network.eth0.tx) "
                + "/ ignoring (pod) group_left sum by (cluster, region) (network.eth0.rx))"
        );
        assertFalse(plan.output().stream().map(Attribute::name).toList().contains("region"));
    }

    public void testVectorMatchGroupRightSwapsInputs() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        // group_right: RHS is the "many" side, so the inputs are swapped to keep the "one" side as the build (join right).
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum by (cluster) (network.eth0.tx) "
                + "/ on (cluster) group_right sum by (cluster, pod) (network.eth0.rx))"
        );
        var joins = plan.collect(HashJoin.class);
        assertThat(joins, hasSize(1));
        HashJoin join = joins.getFirst();
        assertThat(join.unique(), equalTo(false));
        // After the swap the probe (left of the join) is the RHS "many" side, grouped by (cluster, pod).
        assertThat(keyedLabels(join.left()), containsInAnyOrder("cluster"));
    }

    /**
     * group_left over an opaque "many" side keeps each of its series without the metric name, like Prometheus. The
     * match label stays in that side's metadata rather than becoming a column of its own.
     */
    public void testGroupLeftOverOpaqueManyKeepsItsSeries() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(network.eth0.tx / on (cluster) group_left sum by (cluster) (network.eth0.rx))"
        );
        assertThat(plan.collect(HashJoin.class), hasSize(1));
        assertThat(plan.output().stream().map(Attribute::name).toList(), containsInAnyOrder("result", "step", "_timeseries"));
        assertEquals(Set.of(LabelMatcher.NAME), viewExclusions(plan));
    }

    /**
     * A group_left label comes from the "one" side: the join carries it over, and the opaque "many" side's metadata leaves
     * out the value it stores itself.
     */
    public void testGroupLeftLabelLeavesOpaqueManyMetadata() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(network.eth0.tx "
                + "/ on (cluster) group_left (region) sum by (cluster, region) (network.eth0.rx))"
        );
        HashJoin join = plan.collect(HashJoin.class).getFirst();
        assertThat(join.addedFields().stream().map(Attribute::name).toList(), hasItem("region"));
        assertThat(plan.output().stream().map(Attribute::name).toList(), containsInAnyOrder("result", "step", "region", "_timeseries"));
        assertEquals(Set.of("region", LabelMatcher.NAME), viewExclusions(plan));
    }

    /** group_right mirrors group_left: the right operand is the opaque "many" side and keeps its series. */
    public void testGroupRightLabelLeavesOpaqueManyMetadata() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum by (cluster, region) (network.eth0.tx) "
                + "/ on (cluster) group_right (region) network.eth0.rx)"
        );
        HashJoin join = plan.collect(HashJoin.class).getFirst();
        assertThat(join.addedFields().stream().map(Attribute::name).toList(), hasItem("region"));
        assertThat(plan.output().stream().map(Attribute::name).toList(), containsInAnyOrder("result", "step", "region", "_timeseries"));
        assertEquals(Set.of("region", LabelMatcher.NAME), viewExclusions(plan));
    }

    /** A filtering comparison keeps the metric name of the opaque "many" side's series, like Prometheus. */
    public void testGroupLeftComparisonKeepsMetricName() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(network.eth0.tx > on (cluster) group_left sum by (cluster) (network.eth0.rx))"
        );
        assertEquals(Set.of(), viewExclusions(plan));
    }

    /** A {@code bool} comparison drops the metric name, like arithmetic. */
    public void testGroupLeftBoolComparisonDropsMetricName() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(network.eth0.tx > bool on (cluster) group_left sum by (cluster) (network.eth0.rx))"
        );
        assertEquals(Set.of(LabelMatcher.NAME), viewExclusions(plan));
    }

    /** Arithmetic with a scalar drops the metric name: the source reads its series without it. */
    public void testScalarArithmeticDropsMetricName() {
        var plan = planPromql("PROMQL index=k8s step=5m result=(network.eth0.tx * 2)");
        assertEquals(Set.of(LabelMatcher.NAME), viewExclusions(plan));
    }

    /**
     * Above {@code topk} the metric name cannot be pushed down to the source: typed metadata leaves it out with
     * {@code ts_unset}, while keyword metadata keeps it, as before the operator dropped it.
     */
    public void testScalarArithmeticOverTopk() {
        var plan = planPromql("PROMQL index=k8s step=5m result=(topk(1, network.eth0.tx) * 2)");
        boolean unsets = plan.anyMatch(node -> node.expressions().stream().anyMatch(e -> e.anyMatch(TimeSeriesUnset.class::isInstance)));
        assertEquals(typedTimeSeriesMetadata(), unsets);
        assertEquals(Set.of(), viewExclusions(plan));
    }

    /**
     * A {@code group_left} label over an aggregated opaque "many" side: typed metadata leaves the label out of the result's
     * metadata, keyword metadata keeps it there, as before.
     */
    public void testGroupLeftLabelOverAggregatedOpaqueMany() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(topk(5, network.eth0.tx) "
                + "/ on (cluster) group_left (region) sum by (cluster, region) (network.eth0.rx))"
        );
        boolean unsets = plan.anyMatch(node -> node.expressions().stream().anyMatch(e -> e.anyMatch(TimeSeriesUnset.class::isInstance)));
        assertEquals(typedTimeSeriesMetadata(), unsets);
        assertThat(plan.output().stream().map(Attribute::name).toList(), containsInAnyOrder("result", "step", "region", "_timeseries"));
    }

    public void testVectorMatchComparisonBoolProducesHashJoin() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        // `> bool on (cluster)` compares two vectors and yields 1.0/0.0 for each matched pair (no rows dropped).
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum by (cluster) (network.eth0.tx) > bool on (cluster) sum by (cluster) (network.eth0.rx))"
        );
        var joins = plan.collect(HashJoin.class);
        assertThat(joins, hasSize(1));
        assertThat(keyedLabels(joins.getFirst().left()), containsInAnyOrder("cluster"));
    }

    public void testVectorMatchComparisonFilterProducesHashJoinAndFilter() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        // `> on (cluster)` (no bool) keeps the LHS series where the comparison holds; the comparison becomes a Filter.
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum by (cluster) (network.eth0.tx) > on (cluster) sum by (cluster) (network.eth0.rx))"
        );
        assertThat(plan.collect(HashJoin.class), hasSize(1));
        boolean hasGreaterThanFilter = plan.collect(Filter.class)
            .stream()
            .anyMatch(f -> f.condition().anyMatch(GreaterThan.class::isInstance));
        assertTrue("expected a Filter carrying the > comparison", hasGreaterThanFilter);
    }

    public void testBinaryOperatorWithDifferentGroupingKeysTranslatesAsJoin() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        // sum by (cluster) (...) + sum by (pod) (...) can't fold into one aggregate; it translates as a default-match
        // join whose full-label-set keys never coincide, so it evaluates to the empty vector like Prometheus.
        LogicalPlan plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum by (cluster) (network.eth0.tx) + sum by (pod) (network.eth0.rx))"
        );
        assertThat(plan.collect(HashJoin.class), hasSize(1));
    }

    public void testVectorMatchOnLabelAbsentFromBothOperandsJoinsOnStep() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        // on (pod) references a label neither operand exposes (both are sum by (cluster)). PromQL matches an absent
        // label as the empty string on both sides, so it cannot discriminate: the key set degrades to step only, and a
        // resulting many-to-many match surfaces as the runtime's unique-build-key error, exactly like Prometheus.
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum by (cluster) (network.eth0.tx) / on (pod) sum by (cluster) (network.eth0.rx))"
        );
        var joins = plan.collect(HashJoin.class);
        assertThat(joins, hasSize(1));
        assertThat(
            joins.getFirst().leftFields().stream().map(Attribute::name).toList(),
            containsInAnyOrder("step", PackDims.PACKED_FIELD_NAME)
        );
    }

    /**
     * Shape 3 (asymmetric): concrete {@code by} × opaque selector with {@code on (cluster)}. Match keys are declared by
     * {@code on}; Resolve materializes {@code cluster} from the selector's rest.
     */
    public void testVectorMatchOnAllowsOpaqueSelectorOperand() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        var plan = planPromql("PROMQL index=k8s step=5m result=(sum by (cluster) (network.eth0.tx) / on (cluster) network.eth0.rx)");
        assertThat(plan.collect(HashJoin.class), hasSize(1));
        assertThat(plan.output().stream().map(Attribute::name).toList(), containsInAnyOrder("result", "step", "cluster"));
    }

    /**
     * Shape 2 (opaque × opaque) with {@code on (cluster)}: both sides expose only rest; keys are unresolved label refs.
     */
    public void testVectorMatchOnAllowsOpaqueOpaqueOperands() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        var plan = planPromql("PROMQL index=k8s step=5m result=(network.eth0.tx / on (cluster) network.eth0.rx)");
        assertThat(plan.collect(HashJoin.class), hasSize(1));
        assertThat(plan.output().stream().map(Attribute::name).toList(), containsInAnyOrder("result", "step", "cluster"));
    }

    /**
     * {@code without} still yields an open rest identity; {@code on} declares the keys the same way as a bare selector.
     */
    public void testVectorMatchOnAllowsOpaqueWithoutOperand() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum without (pod) (network.eth0.tx) / on (cluster) sum by (cluster) (network.eth0.rx))"
        );
        assertThat(plan.collect(HashJoin.class), hasSize(1));
        assertThat(plan.output().stream().map(Attribute::name).toList(), containsInAnyOrder("result", "step", "cluster"));
    }

    /** Shape 7a: nested WITHOUT over WITHOUT — typed metadata only. */
    public void testNestedWithoutOverWithoutPlans() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        assumeTrue("nested without needs typed metadata", typedTimeSeriesMetadata());
        var plan = planPromql("PROMQL index=k8s step=5m result=(sum without (pod) (sum without (region) (network.eth0.tx)))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), containsInAnyOrder("result", "step", "_timeseries"));
    }

    /** Shape 7b: topk over without — typed metadata only. */
    public void testTopkOverWithoutPlans() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        assumeTrue("topk over without needs typed metadata", typedTimeSeriesMetadata());
        var plan = planPromql("PROMQL index=k8s step=5m result=(topk(3, sum without (pod) (network.eth0.tx)))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), containsInAnyOrder("result", "step", "_timeseries"));
    }

    /**
     * Shape 4: {@code ignoring} over opaque/opaque. Match keys pack rest minus the ignored labels and {@code __name__}; the
     * result keeps that refined rest, which arithmetic leaves {@code __name__} out of too, so both read one view.
     */
    public void testVectorMatchIgnoringAllowsOpaqueOpaqueOperands() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        var plan = planPromql("PROMQL index=k8s step=5m result=(network.eth0.tx / ignoring (region) network.eth0.rx)");
        assertThat(plan.collect(HashJoin.class), hasSize(1));
        assertThat(plan.output().stream().map(Attribute::name).toList(), containsInAnyOrder("result", "step", "_timeseries"));
    }

    /**
     * {@code ignoring(pod) group_left(region)} over raw operands: the LHS source serves two metadata views that neither
     * contain each other, the match key leaving {@code pod} out and the result leaving {@code region} out. Typed metadata
     * reads what both leave out at the source and derives each; the keyword column cannot, so it stays rejected there.
     */
    public void testIgnoringGroupLeftIncludeOverRawOperands() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        for (String expr : List.of(
            "network.eth0.tx * ignoring (pod) group_left (region) network.eth0.rx",
            "network.eth0.tx * ignoring (pod) group_right (region) network.eth0.rx",
            "network.eth0.tx * ignoring (pod) group_left (region) sum by (region, cluster) (network.eth0.rx)"
        )) {
            String query = "PROMQL index=k8s step=5m result=(" + expr + ")";
            if (typedTimeSeriesMetadata()) {
                var plan = planPromql(query, false);
                assertTrue("must resolve for [" + expr + "]: " + plan, plan.resolved());
                assertThat("expected join for [" + expr + "]", plan.collect(HashJoin.class), hasSize(1));
                assertThat(
                    plan.output().stream().map(Attribute::name).toList(),
                    containsInAnyOrder("result", "step", "_timeseries", "region")
                );
                assertTrue("the match key derives from the shared read for [" + expr + "]", leavesOut(plan, "pod"));
                assertTrue("the result derives from the shared read for [" + expr + "]", leavesOut(plan, "region"));
            } else {
                var e = expectThrows(VerificationException.class, () -> planPromql(query));
                assertThat(e.getMessage(), containsString("regrouping the series of an aggregated input is not supported"));
            }
        }
    }

    /**
     * An opaque operand next to a concrete or promoted-label sibling under {@code ignoring(...)} / default matching: the
     * named key labels compare in their own slots, so the opaque side's signature slot leaves them out and the side without
     * metadata packs the empty signature. Typed metadata only: the keyword slot stays null there and never matches.
     */
    public void testOpaqueSignatureSlotLeavesNamedKeysOut() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        assumeTrue("requires typed _timeseries for the empty signature", typedTimeSeriesMetadata());
        for (var shape : Map.of(
            "network.eth0.tx / ignoring (pod, region) group_left sum by (cluster) (network.eth0.rx)",
            "cluster",
            "sum by (cluster) (network.eth0.tx) + network.eth0.rx",
            "cluster",
            "(network.eth0.tx * on (pod) group_left (region) network.eth0.rx) / ignoring (pod) network.eth0.rx",
            "region"
        ).entrySet()) {
            String expr = shape.getKey();
            var plan = planPromql("PROMQL index=k8s step=5m result=(" + expr + ")", false);
            assertTrue("must resolve for [" + expr + "]: " + plan, plan.resolved());
            assertThat("expected join for [" + expr + "]", plan.collect(HashJoin.class).isEmpty(), equalTo(false));
            assertTrue("the opaque side's slot leaves the named key out for [" + expr + "]", leavesOut(plan, shape.getValue()));
        }
        var concrete = planPromql("PROMQL index=k8s step=5m result=(sum by (cluster) (network.eth0.tx) + network.eth0.rx)", false);
        boolean emptySignature = concrete.anyMatch(
            node -> node.expressions()
                .stream()
                .anyMatch(
                    e -> e.anyMatch(
                        x -> x instanceof TimeSeriesFromKeyword from
                            && "{}".equals(BytesRefs.toString(from.children().getFirst().fold(FoldContext.small())))
                    )
                )
        );
        assertTrue("the concrete side packs the empty signature: " + concrete, emptySignature);
    }

    /** Whether some metadata view of {@code plan} leaves {@code label} out: read so at a source, or derived with ts_unset. */
    private static boolean leavesOut(LogicalPlan plan, String label) {
        boolean readWithout = plan.collect(EsRelation.class)
            .stream()
            .flatMap(relation -> relation.output().stream())
            .anyMatch(a -> a instanceof TimeSeriesMetadataAttribute m && m.excludedFields().contains(label));
        boolean unset = plan.anyMatch(
            node -> node.expressions()
                .stream()
                .anyMatch(
                    e -> e.anyMatch(
                        x -> x instanceof TimeSeriesUnset u
                            && u.children().stream().anyMatch(c -> c instanceof Literal l && label.equals(BytesRefs.toString(l.value())))
                    )
                )
        );
        return readWithout || unset;
    }

    /**
     * Unmatched {@code +} over a prior {@code on(...)} match joins by default matching: the match result is concrete
     * ({@code cluster}), the raw selector is opaque. Same {@code joinKey} path as concrete×opaque without nesting —
     * packs a metadata signature on the opaque side and named keys on the concrete side.
     */
    public void testVectorMatchComposedWithOpaqueOperandJoins() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        for (String expr : List.of(
            "(sum by (cluster) (network.eth0.tx) / on (cluster) sum by (cluster) (network.eth0.rx)) + network.eth0.rx",
            "network.eth0.rx + (sum by (cluster) (network.eth0.tx) / on (cluster) sum by (cluster) (network.eth0.rx))",
            "(sum by (cluster) (network.eth0.tx) / on (cluster) sum by (cluster) (network.eth0.rx))" + " / ignoring (pod) network.eth0.rx"
        )) {
            var plan = planPromql("PROMQL index=k8s step=5m result=(" + expr + ")");
            assertThat("expected join for [" + expr + "]", plan.collect(HashJoin.class).isEmpty(), equalTo(false));
            assertTrue("must resolve for [" + expr + "]: " + plan, plan.resolved());
        }
    }

    /**
     * Two opaque {@code group_right} results composed under default matching join on metadata (same path as cross-metric
     * selectors), so they no longer need every label named up front.
     */
    public void testComposedGroupRightOpaqueOpaqueAllowed() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=("
                + "(network.eth0.tx * on (cluster) group_right (pod) network.eth0.rx)"
                + " / (network.cost * on (cluster) group_right (pod) network.eth0.rx))"
        );
        assertThat(plan.collect(HashJoin.class).isEmpty(), equalTo(false));
    }

    /**
     * Labelless aggregate {@code + ignoring(...)} opaque selector: both sides pack a metadata signature slot so join-key
     * arity matches. The labelless side binds that slot to the empty Prometheus signature, which an opaque series equals
     * only when it has no label left — plans as a join, does not unique-HashJoin on {@code step} alone ("found a
     * duplicate row") or IndexOOB on asymmetric keys.
     */

    public void testLabeledTimesLabellessWithoutPlansJoinNotFold() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        assumeTrue("requires typed _timeseries", typedTimeSeriesMetadata());
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=((max without (cluster) (network.eth0.tx)) * (sum without (cluster, pod) (network.eth0.tx)))"
        );
        assertThat("must join asymmetric without grains", plan.collect(HashJoin.class).isEmpty(), equalTo(false));
        assertTrue(plan.resolved());
    }

    public void testIgnoringAsymmetricLabellessVsOpaquePlansJoin() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        assumeTrue("requires typed _timeseries for opaque ignoring join keys", typedTimeSeriesMetadata());
        for (String expr : List.of(
            "sum(network.eth0.tx) + ignoring (pod) network.eth0.tx",
            "network.eth0.tx + ignoring (pod) sum(network.eth0.tx)",
            "sum(network.eth0.tx{cluster=\"prod\"}) + ignoring (pod) network.eth0.tx",
            "(sum(network.eth0.tx) + ignoring (pod) network.eth0.tx) * on (cluster) group_left network.eth0.rx"
        )) {
            var plan = planPromql("PROMQL index=k8s step=5m result=(" + expr + ")");
            assertThat("expected join for [" + expr + "]", plan.collect(HashJoin.class).isEmpty(), equalTo(false));
            assertTrue("must resolve for [" + expr + "]: " + plan, plan.resolved());
            for (HashJoin join : plan.collect(HashJoin.class)) {
                assertThat(
                    "join fields must share arity (step+packed metadata) for [" + expr + "]",
                    join.leftFields().size(),
                    equalTo(join.rightFields().size())
                );
            }
        }
    }

    /**
     * Nested many-to-one: {@code a * on(pod) group_left(node) b * on(pod) group_left(namespace) b} (left-assoc).
     * Stand-ins: {@code region} for node, {@code cluster} for namespace; both joins must survive.
     */
    public void testNestedGroupLeftIncludeLabels() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=("
                + "network.eth0.tx * on (pod) group_left (region) network.eth0.rx"
                + " * on (pod) group_left (cluster) network.eth0.rx)"
        );
        assertThat("two nested group_left joins", plan.collect(HashJoin.class), hasSize(2));
        assertTrue("group_left(region) include", plan.output().stream().anyMatch(a -> "region".equals(a.name())));
        assertTrue("group_left(cluster) include", plan.output().stream().anyMatch(a -> "cluster".equals(a.name())));
        assertTrue(
            "opaque many-side rest retained: " + plan.output(),
            plan.output().stream().anyMatch(a -> a.name().startsWith("_timeseries"))
        );
        // Outer build must pack a real pod key (not null): a pin Project under PackDims used to drop the join-key
        // need so every build row shared (step, null) and HashJoin threw "found a duplicate row".
        HashJoin outer = plan.collect(HashJoin.class).get(0);
        assertFalse(
            "outer build must not null-fill the on() key: " + outer.right(),
            outer.right().anyMatch(p -> p instanceof Eval eval && eval.fields().stream().anyMatch(a -> {
                if ("pod".equals(a.name()) == false) {
                    return false;
                }
                return a.child() instanceof Literal lit && lit.value() == null;
            }))
        );
    }

    /**
     * Join by default, fold as the optimisation: same-metric pending binaries share a frame and fold; explicit
     * {@code group_left} and {@code label_replace}×vector (a relabel leaves the frame) always join.
     */
    public void testSameMetricFoldsLabelReplaceAndGroupLeftJoin() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        var folded = planPromql("PROMQL index=k8s step=5m result=(network.eth0.tx + network.eth0.tx)");
        assertThat("same-metric pending binary folds into one frame", folded.collect(HashJoin.class), hasSize(0));

        var groupLeft = planPromql("PROMQL index=k8s step=5m result=(network.eth0.tx * on (pod) group_left (region) network.eth0.rx)");
        assertThat("group_left always joins", groupLeft.collect(HashJoin.class), hasSize(1));

        assumeTrue("requires PromQL label functions", EsqlCapabilities.Cap.PROMQL_LABEL_FUNCTIONS.isEnabled());
        assumeTrue("requires typed _timeseries for opaque label_replace as a binary operand", typedTimeSeriesMetadata());
        var relabel = planPromql(
            "PROMQL index=k8s step=5m result=(label_replace(network.eth0.tx, \"dst\", \"$1\", \"pod\", \"(.+)\") + network.eth0.tx)"
        );
        assertThat("label_replace × vector always joins", relabel.collect(HashJoin.class).isEmpty(), equalTo(false));
    }

    public void testGroupLabelMissingFromBuildDoesNotLeakFromProbe() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum by (cluster, pod) (network.eth0.tx) "
                + "/ on (cluster) group_left (pod) sum by (cluster) (network.eth0.rx))"
        );
        Alias pod = plan.collect(Eval.class)
            .stream()
            .flatMap(eval -> eval.fields().stream())
            .filter(alias -> alias.name().equals("pod") && alias.child() instanceof Literal)
            .findFirst()
            .orElseThrow();
        assertNull(as(pod.child(), Literal.class).value());
    }

    public void testVectorMatchNestedInScalarArithmetic() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        // The vector match is nested as the right operand of `1 + (...)`; the join is built and the scalar op wraps its value.
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(1 + (sum by (cluster) (network.eth0.tx) "
                + "/ on (cluster) sum by (cluster) (network.eth0.rx)))"
        );
        assertThat(plan.collect(HashJoin.class), hasSize(1));
    }

    public void testVectorMatchNestedInAggregation() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        // sum(...) aggregates over the vector-match result.
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum(sum by (cluster) (network.eth0.tx) "
                + "/ on (cluster) sum by (cluster) (network.eth0.rx)))"
        );
        assertThat(plan.collect(HashJoin.class), hasSize(1));
    }

    public void testVectorMatchNestedInFunction() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        // abs(...) applies over the vector-match result value.
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(abs(sum by (cluster) (network.eth0.tx) "
                + "/ on (cluster) sum by (cluster) (network.eth0.rx)))"
        );
        assertThat(plan.collect(HashJoin.class), hasSize(1));
    }

    public void testVectorMatchComparisonInsideUnion() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        // A filter-mode comparison vector match composes as a union branch.
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=((sum by (cluster) (network.eth0.tx) "
                + "> on (cluster) sum by (cluster) (network.eth0.rx)) or sum by (cluster) (network.eth0.rx))"
        );
        assertThat(plan.collect(HashJoin.class), hasSize(1));
    }

    public void testVectorMatchComposedWithPlainVectorOperand() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        // The vector-match result is itself an operand of an unmatched binary operator.
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=((sum by (cluster) (network.eth0.tx) "
                + "/ on (cluster) sum by (cluster) (network.eth0.rx)) + sum by (cluster) (network.eth0.rx))"
        );
        assertThat(plan.collect(HashJoin.class), hasSize(2));
    }

    public void testTopKOverVectorMatchInsideUnion() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.isEnabled());
        // topk over a vector match inside a union branch: the branch-local step id must survive the join.
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(topk(2, sum by (cluster) (network.eth0.tx) "
                + "/ on (cluster) sum by (cluster) (network.eth0.rx)) or sum by (cluster) (network.eth0.rx))"
        );
        assertThat(plan.collect(HashJoin.class), hasSize(1));
    }

    /**
     * A count is at least 1 for an element and a group with none is no element. Fused with the other operand's aggregate,
     * an operand's count reads 0 in a group that only the other operand's rows create; the plan turns that 0 into null so
     * the pair drops with the unmatched ones, for the series count and for the sample count alike.
     */
    public void testEmptyCountOperandIsNull() {
        for (String promql : List.of(
            "count by (cluster) (network.bytes_in) - count by (cluster) (network.cost{pod=\"one\"})",
            "count_over_time(network.bytes_in[5m]) - count_over_time(network.cost{pod=\"one\"}[5m])",
            "sum by (cluster) (count_over_time(network.bytes_in[5m])) - sum by (cluster) (count_over_time(network.cost{pod=\"one\"}[5m]))",
            "count(network.bytes_in{pod=~\"nope\"}) - count(network.cost{pod=~\"nope\"})"
        )) {
            LogicalPlan plan = planPromql("PROMQL index=k8s step=1m result=(" + promql + ")");
            assertThat(promql + ": one null-when-empty count per operand\n" + plan, zeroCountGuards(plan), hasSize(2));
        }
    }

    /**
     * A range function over a series without a sample in the window is no element. Fused with the other operand's
     * aggregate, an operand's {@code count_over_time} reads 0 and its {@code present_over_time} false for every series its
     * matchers reject; each maps to null before the across-series {@code count} sees it, so the count covers the matching
     * series only.
     */
    public void testEmptyRangeOperandIsNull() {
        Eval countOverTime = acrossSeriesInput("count_over_time");
        assertThat(countOverTime.toString(), zeroCountGuards(countOverTime), hasSize(2));
        Eval presentOverTime = acrossSeriesInput("present_over_time");
        List<Case> falseAsNull = new ArrayList<>();
        presentOverTime.forEachExpressionDown(Case.class, c -> {
            if (c.children().size() == 3 && Literal.TRUE.equals(c.children().get(1)) && Literal.NULL.equals(c.children().get(2))) {
                falseAsNull.add(c);
            }
        });
        assertThat(presentOverTime.toString(), falseAsNull, hasSize(2));
    }

    /**
     * Fused with another operand's aggregate, an operand's {@code absent_over_time} reads true for every series its
     * matchers reject. True stays only for a series the operand selects: one with a row passing its matchers in the window.
     */
    public void testFusedAbsentOverTimeStaysOnlyForSelectedSeries() {
        LogicalPlan plan = planPromql(
            "PROMQL index=k8s step=1m result=(max by (cluster) (absent_over_time(network.bytes_in[5m])) "
                + "- max by (cluster) (absent_over_time(network.cost{pod=\"one\"}[5m])))"
        );
        List<Present> selected = timestampPresence(plan);
        assertThat(plan.toString(), selected, hasSize(2));
        List<Expression> selectedFilters = selected.stream().filter(Present::hasFilter).map(Present::filter).toList();
        assertThat(plan.toString(), selectedFilters, hasSize(1));
        assertEquals(List.of("pod"), selectedFilters.getFirst().references().stream().map(Attribute::name).toList());
    }

    /**
     * A series a metric selector selects has a sample of the metric, so a metric's {@code absent_over_time} never has an
     * element, fused or not: no row-presence check of the series is planned for it.
     */
    public void testFusedAbsentOverTimeOfMetricsChecksNoRowPresence() {
        LogicalPlan plan = planPromql(
            "PROMQL index=k8s step=1m result=(max by (cluster) (absent_over_time(network.eth0.tx[5m])) "
                + "- max by (cluster) (absent_over_time(network.eth0.rx{pod=\"one\"}[5m])))"
        );
        assertThat(plan.toString(), timestampPresence(plan), hasSize(0));
    }

    /** The {@code present(@timestamp)} checks of whether an operand reads a row of the series in the window. */
    private static List<Present> timestampPresence(LogicalPlan plan) {
        List<Present> presence = new ArrayList<>();
        plan.forEachExpressionDown(Present.class, p -> {
            if (p.field() instanceof Attribute field && field.name().equals("@timestamp")) {
                presence.add(p);
            }
        });
        return presence;
    }

    /**
     * The input of the across-series aggregate of two fused operands, each counting {@code rangeFunction} over one metric:
     * the per-series range values the outer {@code count}s read.
     */
    private Eval acrossSeriesInput(String rangeFunction) {
        LogicalPlan plan = planPromql(
            "PROMQL index=k8s step=1m result=(count by (cluster) ("
                + rangeFunction
                + "(network.bytes_in[5m])) - count by (cluster) ("
                + rangeFunction
                + "(network.cost{pod=\"one\"}[5m])))"
        );
        List<Aggregate> outer = plan.collect(Aggregate.class).stream().filter(a -> a instanceof TimeSeriesAggregate == false).toList();
        assertThat(plan.toString(), outer, hasSize(1));
        assertThat(plan.toString(), outer.getFirst().child(), instanceOf(Eval.class));
        return (Eval) outer.getFirst().child();
    }

    /** A range function maps to null once, however many operators compose over it. */
    public void testChainedRangeCountsMapOnce() {
        LogicalPlan plan = planPromql(
            "PROMQL index=k8s step=1m result=(count_over_time(network.bytes_in[5m]) - count_over_time(network.cost[5m]) "
                + "- count_over_time(network.total_bytes_in[5m]))"
        );
        assertThat(plan.toString(), zeroCountGuards(plan), hasSize(3));
    }

    /**
     * The operator leaves a null value for a series without a partner; the enclosing {@code count} counts no element for a
     * group of such series, so the group is no element either rather than a count of 0.
     */
    public void testCountOverUnmatchedPairsIsNoElement() {
        for (String promql : List.of(
            "count by (pod) (network.bytes_in / network.cost)",
            "count by (cluster) (sum by (pod, cluster) (network.bytes_in) / sum by (pod, cluster) (network.cost{pod!=\"one\"}))"
        )) {
            LogicalPlan plan = planPromql("PROMQL index=k8s step=1m result=(" + promql + ")");
            assertThat(promql + ": the enclosing count maps 0 to no element\n" + plan, zeroCountGuards(plan), hasSize(1));
        }
    }

    /** The {@code CASE(count == 0, null, count)} guards that map a count of 0 to no element. */
    private static List<Case> zeroCountGuards(LogicalPlan plan) {
        List<Case> guards = new ArrayList<>();
        plan.forEachExpressionDown(Case.class, c -> {
            if (c.children().getFirst() instanceof Equals eq && eq.right() instanceof Literal l && Long.valueOf(0L).equals(l.value())) {
                guards.add(c);
            }
        });
        return guards;
    }

    private static void assertNoIndexBackedPromqlPlan(LogicalPlan plan) {
        assertThat(plan.collect(PromqlCommand.class), hasSize(0));
        assertThat(plan.collect(UnresolvedRelation.class), hasSize(0));
        assertThat(plan.collect(EsRelation.class), hasSize(0));
    }

    private static Eval findEvalWithField(LogicalPlan plan, String fieldName) {
        return plan.collect(Eval.class).stream().filter(e -> e.fields().getFirst().name().equals(fieldName)).findFirst().orElseThrow();
    }

    private static Eval findEvalWithFieldAndExpression(LogicalPlan plan, String fieldName, Class<? extends Expression> expressionClass) {
        return plan.collect(Eval.class)
            .stream()
            .filter(e -> e.fields().getFirst().name().equals(fieldName) && expressionClass.isInstance(e.fields().getFirst().child()))
            .findFirst()
            .orElseThrow();
    }

    /** The labels a join side is keyed on: the join keys are {@code [step, pack]} and the pack carries the labels. */
    private static List<String> keyedLabels(LogicalPlan joinSide) {
        LogicalPlan packed = joinSide instanceof Project project ? project.child() : joinSide;
        return as(packed, PackDims.class).dims().stream().map(Attribute::name).toList();
    }

    /**
     * A binary operator between two closed aggregates names every label of its result, so a {@code without} over it is a
     * emitAggregate over known columns: the result exposes those labels minus the dropped ones, never a packed identity the plan
     * does not produce.
     */
    public void testWithoutOverAClosedBinaryOperatorKeepsTheRemainingLabels() {
        assertThat(
            outputNames("sum without (pod) (sum by (pod, cluster) (requests) / sum by (pod, cluster) (errors))"),
            equalTo(List.of("result", "step", "cluster"))
        );
        assertThat(outputNames("sum without (pod) (sum by (pod) (requests) / sum by (pod) (errors))"), equalTo(List.of("result", "step")));
        assertThat(outputNames("count without (cluster) (sum(requests) + sum(errors))"), equalTo(List.of("result", "step")));
    }

    /**
     * When {@code __name__} is a dimension, every metric lives in its own series. Default matching over opaque operands
     * is implicit {@code ignoring(__name__)}: each side packs metadata excluding the metric name and the operator
     * translates as an {@link HashJoin}.
     */
    public void testDifferentMetricsInSeparateSeriesDefaultMatchJoins() {
        for (String promql : List.of(
            "node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes",
            "rate(node_cpu_seconds_total[5m]) / node_memory_MemTotal_bytes",
            "(node_memory_MemAvailable_bytes + 1) * node_memory_MemTotal_bytes",
            "sum by (instance) (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)",
            // Across-series sum of a cross-metric binary (promcheck grouping_key_empty bucket).
            "sum(node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)",
            "sum(node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes)",
            "sum(node_memory_MemAvailable_bytes + node_memory_MemTotal_bytes)"
        )) {
            var plan = planPrometheusLayout(promql);
            assertFalse(promql, plan.collect(HashJoin.class).isEmpty());
            assertTrue(promql, plan.resolved());
        }
    }

    /**
     * Cross-metric binary under sum, then divided by another sum — the promcheck {@code grouping_key_empty} shape
     * ({@code sum(a-b)/sum(a)}, {@code sum(a+b)/sum(c)}).
     */
    public void testSumOfCrossMetricBinaryDividedBySum() {
        // Baseline: two labelless aggregates over different metrics already join.
        assertTrue(planPrometheusLayout("sum(node_memory_MemAvailable_bytes) / count(node_memory_MemAvailable_bytes)").resolved());
        assertTrue(planPrometheusLayout("sum(node_memory_MemAvailable_bytes) / sum(node_memory_MemTotal_bytes)").resolved());
        for (String promql : List.of(
            "sum(node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes) / count(node_memory_MemAvailable_bytes)",
            "sum(node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes) / sum(node_memory_MemTotal_bytes)",
            "sum(node_memory_MemAvailable_bytes + node_memory_MemTotal_bytes) / sum(node_memory_MemTotal_bytes)"
        )) {
            var plan = planPrometheusLayout(promql);
            assertTrue(promql, plan.resolved());
        }
    }

    /**
     * Grafana empty-denominator idiom on a {@code __name__}-dimension index: nested {@code or on ()} keeps a sticky
     * value name above UnionAll so the outer division's build-side reidentify does not rewrite union branches.
     */
    public void testOrOnEmptyUnderDivisionPrometheusLayout() {
        var plan = planPrometheusLayout("sum(node_memory_MemAvailable_bytes) / (sum(node_memory_MemAvailable_bytes) or on () vector(1))");
        assertTrue(plan.resolved());
        assertFalse(plan.collect(UnionAll.class).isEmpty());
        assertFalse(plan.collect(HashJoin.class).isEmpty());
    }

    /**
     * {@code TS_COLLAPSE} must group the command's public {@code _timeseries} column. Using the inner plan's unresolved
     * rest (refined to drop {@code __name__}) would PathTrace-rename it to {@code _timeseries$__name__} and leave the
     * projected {@code _timeseries} channel uncovered at runtime.
     */
    public void testDifferentMetricsDefaultMatchTsCollapseUsesPublicTimeseries() {
        var plan = planPrometheusLayout("node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes", true);
        TimeSeriesCollapse collapse = plan.collect(TimeSeriesCollapse.class).getFirst();
        assertThat(collapse.dimensions().stream().map(Attribute::name).toList(), equalTo(List.of(MetadataAttribute.TIMESERIES)));
        assertThat(plan.collect(HashJoin.class), hasSize(1));
    }

    /** Shapes over a {@code __name__}-dimension index that evaluate correctly and must keep planning. */
    public void testSameMetricOrAggregatedOperandsAllowedInSeparateSeries() {
        for (String promql : List.of(
            "node_memory_MemTotal_bytes / node_memory_MemTotal_bytes",
            "node_memory_MemTotal_bytes{job=\"node\"} - node_memory_MemTotal_bytes{instance=\"a:9100\"}",
            "rate(node_cpu_seconds_total[5m]) / node_cpu_seconds_total",
            "node_memory_MemTotal_bytes * 8",
            "node_memory_MemTotal_bytes > 5",
            "sum by (instance) (node_memory_MemAvailable_bytes) / sum by (instance) (node_memory_MemTotal_bytes)",
            "sum(node_memory_MemAvailable_bytes) / sum(node_memory_MemTotal_bytes)",
            "node_memory_MemAvailable_bytes or node_memory_MemTotal_bytes",
            // Matching WITHOUT exclusions share remaining labels.
            "sum without (job) (node_memory_MemAvailable_bytes) - sum without (job) (node_memory_MemTotal_bytes)"
        )) {
            assertNotNull(promql, planPrometheusLayout(promql));
        }
    }

    /**
     * On a {@code __name__}-dimension index, {@code sum(a/b) by / count by} cannot fold (opaque cross-metric child joins
     * into an Aggregate while the other side is a fused TSA). Joining avoids the mismatched-grouping verification error.
     */
    public void testMismatchedAggregateGroupingsJoin() {
        var plan = planPrometheusLayout(
            "sum by (instance) (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)"
                + " / count by (instance) (node_memory_MemAvailable_bytes)"
        );
        assertThat(plan.collect(HashJoin.class).isEmpty(), equalTo(false));
    }

    /**
     * Labelless nested across-series ({@code max(sum by)/min(sum by)}) join on step — the promcheck nested_agg bucket.
     */
    public void testNestedAcrossSeriesBinaryJoins() {
        var plan = planPrometheusLayout(
            "max(sum by (instance) (node_memory_MemAvailable_bytes))" + " / min(sum by (instance) (node_memory_MemTotal_bytes))"
        );
        assertTrue(plan.resolved());
        assertThat(plan.collect(HashJoin.class).isEmpty(), equalTo(false));
    }

    /**
     * Different {@code offset}s refuse a shared fold frame and join independent pipelines (#158184).
     * Parentheses are required so {@code offset} does not consume the following binary (limitations row 7).
     */
    public void testHeterogeneousOffsetBinaryJoins() {
        assumeTrue("requires independent binary join", EsqlCapabilities.Cap.PROMQL_BINARY_INDEPENDENT_JOIN.isEnabled());
        assumeTrue("requires offset modifier", EsqlCapabilities.Cap.PROMQL_OFFSET_MODIFIER.isEnabled());
        for (String promql : List.of(
            "node_memory_MemAvailable_bytes - (node_memory_MemAvailable_bytes offset 1h)",
            "sum(node_memory_MemAvailable_bytes) / sum(node_memory_MemAvailable_bytes offset 5m)"
        )) {
            var plan = planPrometheusLayout(promql);
            assertTrue(promql, plan.resolved());
            assertThat(promql, plan.collect(HashJoin.class).isEmpty(), equalTo(false));
        }
        // Same metric + same offset shares one evaluation timestamp and still folds.
        var sameOffset = planPrometheusLayout("(node_memory_MemAvailable_bytes offset 5m) - (node_memory_MemAvailable_bytes offset 5m)");
        assertThat(sameOffset.collect(HashJoin.class), hasSize(0));
    }

    /**
     * Aggregated×raw, nested across-series×source, and {@code scalar()}×labeled join
     * ({@link EsqlCapabilities.Cap#PROMQL_BINARY_INDEPENDENT_JOIN}). Opaque reductions stay rejected at verify/bind.
     */
    public void testAsymmetricBinaryOperandsJoin() {
        assumeTrue("requires independent binary join", EsqlCapabilities.Cap.PROMQL_BINARY_INDEPENDENT_JOIN.isEnabled());
        for (String promql : List.of(
            "sum(node_memory_MemAvailable_bytes) / node_memory_MemTotal_bytes",
            "sum by (instance) (node_memory_MemAvailable_bytes) / node_memory_MemTotal_bytes",
            "count(count by (instance) (node_memory_MemAvailable_bytes)) / sum(node_memory_MemTotal_bytes)",
            "scalar(sum(node_memory_MemAvailable_bytes)) * node_memory_MemTotal_bytes",
            "node_memory_MemTotal_bytes / scalar(node_memory_MemAvailable_bytes)"
        )) {
            var plan = planPrometheusLayout(promql);
            assertTrue(promql, plan.resolved());
            assertThat(promql, plan.collect(HashJoin.class).isEmpty(), equalTo(false));
        }
        // Same shape on the k8s fixture (concrete dimensions, no __name__).
        var k8s = planPromql("PROMQL index=k8s step=5m result=(sum by (cluster, pod) (network.eth0.tx) / network.eth0.rx)");
        assertThat(k8s.collect(HashJoin.class).isEmpty(), equalTo(false));

        assumeTrue("opaque topk binaries need typed _timeseries", typedTimeSeriesMetadata());
        for (String promql : List.of(
            "topk(1, node_memory_MemAvailable_bytes) / topk(1, node_memory_MemTotal_bytes)",
            "topk(2, node_memory_MemAvailable_bytes) * node_memory_MemTotal_bytes"
        )) {
            var plan = planPrometheusLayout(promql);
            assertTrue(promql, plan.resolved());
            assertThat(promql, plan.collect(HashJoin.class).isEmpty(), equalTo(false));
        }
    }

    /** WITHOUT-grouped operands join, including asymmetric raw × without (promcheck without_bin). */
    public void testWithoutBinaryJoins() {
        for (String promql : List.of(
            "sum without (job) (node_memory_MemAvailable_bytes) - sum without (job) (node_memory_MemTotal_bytes)",
            "sum without (job) (node_memory_MemAvailable_bytes) - sum without (instance) (node_memory_MemTotal_bytes)",
            "rate(node_cpu_seconds_total[5m]) - sum(rate(node_cpu_seconds_total[5m])) without (job)"
        )) {
            var plan = planPrometheusLayout(promql);
            assertTrue(promql, plan.resolved());
            assertThat(promql, plan.collect(HashJoin.class).isEmpty(), equalTo(false));
        }
    }

    /**
     * Nested {@code or} under division must plan a join without unresolved layout (promcheck project_block /
     * Cannot project block). Typed metadata can refine the opaque match key after the union; keyword metadata cannot.
     */
    public void testOrUnderDivisionPlansJoin() {
        assumeTrue("requires typed _timeseries", typedTimeSeriesMetadata());
        var plan = planPrometheusLayout(
            "(rate(node_cpu_seconds_total[30s]) or irate(node_cpu_seconds_total[5m]))"
                + " / (rate(node_cpu_seconds_total[30s]) or irate(node_cpu_seconds_total[5m]))"
        );
        assertTrue(plan.resolved());
        assertThat(plan.collect(UnionAll.class).isEmpty(), equalTo(false));
        assertThat(plan.collect(HashJoin.class).isEmpty(), equalTo(false));
    }

    /**
     * {@code by (value)} collides with the sample value column — rejected like {@code by (step)} (promcheck plan_dump).
     */
    public void testByValueLabelRejectedAsCollision() {
        EsField name = dimension("__name__");
        EsField job = dimension("job");
        EsField instance = dimension("instance");
        EsField valueLabel = dimension("value");
        EsField flag = metric("flag", DataType.DOUBLE);
        Map<String, EsField> labels = Map.of(
            name.getName(),
            name,
            job.getName(),
            job,
            instance.getName(),
            instance,
            valueLabel.getName(),
            valueLabel
        );
        Map<String, EsField> metrics = Map.of(flag.getName(), flag);
        Map<String, EsField> mapping = new HashMap<>(labels);
        mapping.putAll(metrics);
        mapping.put("@timestamp", DateEsField.dateEsField("@timestamp", Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        mapping.put("labels", new EsField("labels", DataType.OBJECT, labels, false, EsField.TimeSeriesFieldType.NONE));
        mapping.put("metrics", new EsField("metrics", DataType.OBJECT, metrics, false, EsField.TimeSeriesFieldType.NONE));
        var index = new EsIndex(
            "metrics-node.prometheus-default",
            mapping,
            Map.of("metrics-node.prometheus-default", new IndexProperties(IndexMode.TIME_SERIES, 0)),
            Map.of(),
            Map.of()
        );
        var e = expectThrows(
            VerificationException.class,
            () -> tsAnalyzer().addIndex(index)
                .query("PROMQL index=metrics-node.prometheus-default step=1m value=(sum(flag) by (job, instance, value))")
        );
        assertThat(e.getMessage(), containsString("label [value] collides with the built-in [value] output column"));
    }

    /**
     * Plans against the layout of {@code metrics-prometheus@template}: {@code labels} and {@code metrics} are passthrough
     * objects, so each sub-field is also exposed under its bare name, and {@code labels.__name__} is a dimension.
     */
    private LogicalPlan planPrometheusLayout(String promql) {
        return planPrometheusLayout(promql, false);
    }

    private LogicalPlan planPrometheusLayout(String promql, boolean tsCollapse) {
        assumeTrue("requires typed _timeseries", typedTimeSeriesMetadata());
        EsField name = dimension("__name__");
        EsField instance = dimension("instance");
        EsField job = dimension("job");
        EsField available = metric("node_memory_MemAvailable_bytes", DataType.DOUBLE);
        EsField total = metric("node_memory_MemTotal_bytes", DataType.DOUBLE);
        EsField cpu = metric("node_cpu_seconds_total", DataType.COUNTER_DOUBLE);
        Map<String, EsField> labels = Map.of(name.getName(), name, instance.getName(), instance, job.getName(), job);
        Map<String, EsField> metrics = Map.of(available.getName(), available, total.getName(), total, cpu.getName(), cpu);
        Map<String, EsField> mapping = new HashMap<>(labels);
        mapping.putAll(metrics);
        mapping.put("@timestamp", DateEsField.dateEsField("@timestamp", Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        mapping.put("labels", new EsField("labels", DataType.OBJECT, labels, false, EsField.TimeSeriesFieldType.NONE));
        mapping.put("metrics", new EsField("metrics", DataType.OBJECT, metrics, false, EsField.TimeSeriesFieldType.NONE));
        var index = new EsIndex(
            "metrics-node.prometheus-default",
            mapping,
            Map.of("metrics-node.prometheus-default", new IndexProperties(IndexMode.TIME_SERIES, 0)),
            Map.of(),
            Map.of()
        );
        String query = tsCollapse
            ? "PROMQL index=metrics-node.prometheus-default start=\"2024-05-10T00:00:00Z\" end=\"2024-05-10T00:02:00Z\" "
                + "step=1m result=("
                + promql
                + ") | TS_COLLAPSE"
            : "PROMQL index=metrics-node.prometheus-default step=1m result=(" + promql + ")";
        var analyzed = tsAnalyzer().addIndex(index).query(query);
        return logicalOptimizerWithLatestVersion.optimize(analyzed);
    }

    private static EsField dimension(String name) {
        return new KeywordEsField(name, Map.of(), true, 0, false, false, EsField.TimeSeriesFieldType.DIMENSION);
    }

    private static EsField metric(String name, DataType type) {
        return new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.METRIC);
    }

    private List<String> outputNames(String promql) {
        return planMetricNameIndex(promql).output().stream().map(Attribute::name).toList();
    }

    /** Plans against a remote-write shaped index: `__name__` is a dimension, every metric its own field. */
    private LogicalPlan planMetricNameIndex(String promql) {
        assumeTrue("requires typed _timeseries", typedTimeSeriesMetadata());
        var index = new EsIndex(
            "remote_write",
            Map.of(
                "@timestamp",
                new EsField("@timestamp", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
                "__name__",
                new EsField("__name__", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION),
                "cluster",
                new EsField("cluster", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION),
                "pod",
                new EsField("pod", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION),
                "requests",
                new EsField("requests", DataType.COUNTER_LONG, Map.of(), true, EsField.TimeSeriesFieldType.METRIC),
                "errors",
                new EsField("errors", DataType.COUNTER_LONG, Map.of(), true, EsField.TimeSeriesFieldType.METRIC)
            ),
            Map.of("remote_write", new IndexProperties(IndexMode.TIME_SERIES, 0)),
            Map.of(),
            Map.of()
        );
        var analyzed = analyzerWithEnrichPolicies().addIndex(index)
            .unmappedResolution(UnmappedResolution.NULLIFY)
            .query("PROMQL index=remote_write step=1h result=(" + promql + ")");
        return logicalOptimizer.optimize(analyzed);
    }
}
