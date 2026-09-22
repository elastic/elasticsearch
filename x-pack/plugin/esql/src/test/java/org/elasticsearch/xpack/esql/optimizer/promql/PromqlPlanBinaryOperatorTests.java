/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

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
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
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
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.InnerJoin;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

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

    public void testBinaryScalarAndNestedAggregationFailsCleanly() {
        VerificationException e = assertThrows(
            VerificationException.class,
            () -> planPromql(
                "PROMQL index=k8s step=1m result=(scalar(network.bytes_in) * 100 / count(count by (pod) (network.total_bytes_in)))"
            )
        );
        assertThat(e.getMessage(), containsString("binary expressions with nested aggregations are not supported at this time"));
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

    public void testNestedBinaryPairingGroupsByFullIdentity() {
        // An enclosing aggregate must not narrow the pairing identity. The packed carrier covers every label except
        // the metric name, so separately grouping by each source dimension would only add redundant work.
        var plan = planPromql("PROMQL index=k8s step=1m ratio=(sum(network.eth0.rx / network.eth0.tx))");

        List<UnpackDims> unpacks = plan.collect(UnpackDims.class);
        assertThat(unpacks, hasSize(1));
        assertThat(
            unpacks.getFirst().dims().stream().map(e -> e instanceof Attribute a ? a.name() : e.toString()).toList(),
            equalTo(List.of("_timeseries$__name__"))
        );
        var carriers = plan.collect(EsRelation.class)
            .stream()
            .flatMap(relation -> relation.output().stream())
            .filter(TimeSeriesMetadataAttribute.class::isInstance)
            .map(TimeSeriesMetadataAttribute.class::cast)
            .toList();
        assertThat(carriers, hasSize(1));
        assertThat(carriers.getFirst().excludedFields(), equalTo(Set.of("__name__")));
    }

    public void testScalarOnEitherSideRetainsVectorCarriers() {
        for (String expression : List.of("network.bytes_in * 8", "8 * network.bytes_in")) {
            var plan = planPromql("PROMQL index=k8s step=1m result=(sum by (cluster) (" + expression + "))");
            assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step", "cluster")));
        }
    }

    public void testNestedPairingRetainsRequiredExplicitLabel() {
        var plan = planPromql("PROMQL index=k8s step=1m result=(sum by (cluster) ((network.eth0.rx + network.eth0.tx) / network.eth0.tx))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step", "cluster")));
        assertThat(
            plan.collect(UnpackDims.class).getFirst().dims().stream().map(dimension -> as(dimension, Attribute.class).name()).toList(),
            containsInAnyOrder("_timeseries$__name__", "cluster")
        );
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

    public void testFilterComparisonKeepsMetricNameCarrier() {
        var plan = planPromql("PROMQL index=k8s step=1m network.bytes_in > 1");
        var carriers = plan.collect(EsRelation.class)
            .stream()
            .flatMap(relation -> relation.output().stream())
            .filter(TimeSeriesMetadataAttribute.class::isInstance)
            .map(TimeSeriesMetadataAttribute.class::cast)
            .toList();
        assertThat(carriers, hasSize(1));
        assertThat(carriers.getFirst().excludedFields(), equalTo(Set.of()));
    }

    private static void assertInstantConstFolded(LogicalPlan plan, List<Long> expectedSteps) {
        Row row = plan.collect(Row.class).getFirst();
        assertThat(((Literal) row.fields().getLast().child()).value(), equalTo(expectedSteps));
        assertThat(plan.collect(MvExpand.class), hasSize(1));
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step")));
        assertNoIndexBackedPromqlPlan(plan);
    }

    public void testVectorMatchOnProducesInnerJoin() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        // `on (cluster)` matches 1:1 on cluster + step; no group_left/right so the join enforces uniqueness.
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum by (cluster) (network.eth0.tx) / on (cluster) sum by (cluster) (network.eth0.rx))"
        );
        var joins = plan.collect(InnerJoin.class);
        assertThat(joins, hasSize(1));
        InnerJoin join = joins.getFirst();
        assertThat(join.unique(), equalTo(true));
        assertThat(join.leftFields().getFirst().name(), equalTo("step"));
        assertThat(keyedLabels(join.left()), containsInAnyOrder("cluster"));
        assertThat(keyedLabels(join.right()), containsInAnyOrder("cluster"));
    }

    public void testNestedVectorMatchUsesCurrentOperandLabels() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=((sum by (cluster) (network.eth0.tx) / on (cluster) "
                + "sum by (cluster) (network.eth0.rx)) / ignoring (pod) sum by (cluster, region) (network.eth0.rx))"
        );
        var joins = plan.collect(InnerJoin.class);
        assertThat(joins, hasSize(2));
        joins.forEach(join -> assertFalse(join.leftFields().stream().map(Attribute::name).toList().contains("region")));
    }

    public void testVectorMatchGroupLeftIsManyToOne() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        // group_left: LHS is the "many"/probe side, RHS the "one"/build side; the join is not unique.
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum by (cluster, pod) (network.eth0.tx) "
                + "/ on (cluster) group_left sum by (cluster) (network.eth0.rx))"
        );
        var joins = plan.collect(InnerJoin.class);
        assertThat(joins, hasSize(1));
        InnerJoin join = joins.getFirst();
        assertThat(join.unique(), equalTo(false));
        assertThat(keyedLabels(join.left()), containsInAnyOrder("cluster"));
        assertThat(keyedLabels(join.right()), containsInAnyOrder("cluster"));
    }

    public void testGroupLeftDoesNotExposeUnlistedBuildLabels() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum by (cluster) (network.eth0.tx) "
                + "/ ignoring (pod) group_left sum by (cluster, region) (network.eth0.rx))"
        );
        assertFalse(plan.output().stream().map(Attribute::name).toList().contains("region"));
    }

    public void testVectorMatchGroupRightSwapsInputs() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        // group_right: RHS is the "many" side, so the inputs are swapped to keep the "one" side as the build (join right).
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum by (cluster) (network.eth0.tx) "
                + "/ on (cluster) group_right sum by (cluster, pod) (network.eth0.rx))"
        );
        var joins = plan.collect(InnerJoin.class);
        assertThat(joins, hasSize(1));
        InnerJoin join = joins.getFirst();
        assertThat(join.unique(), equalTo(false));
        // After the swap the probe (left of the join) is the RHS "many" side, grouped by (cluster, pod).
        assertThat(keyedLabels(join.left()), containsInAnyOrder("cluster"));
    }

    public void testVectorMatchComparisonBoolProducesInnerJoin() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        // `> bool on (cluster)` compares two vectors and yields 1.0/0.0 for each matched pair (no rows dropped).
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum by (cluster) (network.eth0.tx) > bool on (cluster) sum by (cluster) (network.eth0.rx))"
        );
        var joins = plan.collect(InnerJoin.class);
        assertThat(joins, hasSize(1));
        assertThat(keyedLabels(joins.getFirst().left()), containsInAnyOrder("cluster"));
    }

    public void testVectorMatchComparisonFilterProducesInnerJoinAndFilter() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        // `> on (cluster)` (no bool) keeps the LHS series where the comparison holds; the comparison becomes a Filter.
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum by (cluster) (network.eth0.tx) > on (cluster) sum by (cluster) (network.eth0.rx))"
        );
        assertThat(plan.collect(InnerJoin.class), hasSize(1));
        boolean hasGreaterThanFilter = plan.collect(Filter.class)
            .stream()
            .anyMatch(f -> f.condition().anyMatch(GreaterThan.class::isInstance));
        assertTrue("expected a Filter carrying the > comparison", hasGreaterThanFilter);
    }

    public void testBinaryOperatorWithDifferentGroupingKeysTranslatesAsJoin() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        // sum by (cluster) (...) + sum by (pod) (...) can't fold into one aggregate; it translates as a default-match
        // join whose full-label-set keys never coincide, so it evaluates to the empty vector like Prometheus.
        LogicalPlan plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum by (cluster) (network.eth0.tx) + sum by (pod) (network.eth0.rx))"
        );
        assertThat(plan.collect(InnerJoin.class), hasSize(1));
    }

    public void testVectorMatchOnLabelAbsentFromBothOperandsJoinsOnStep() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        // on (pod) references a label neither operand exposes (both are sum by (cluster)). PromQL matches an absent
        // label as the empty string on both sides, so it cannot discriminate: the key set degrades to step only, and a
        // resulting many-to-many match surfaces as the runtime's unique-build-key error, exactly like Prometheus.
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum by (cluster) (network.eth0.tx) / on (pod) sum by (cluster) (network.eth0.rx))"
        );
        var joins = plan.collect(InnerJoin.class);
        assertThat(joins, hasSize(1));
        assertThat(
            joins.getFirst().leftFields().stream().map(Attribute::name).toList(),
            containsInAnyOrder("step", PackDims.PACKED_FIELD_NAME)
        );
    }

    public void testVectorMatchRejectsOpaqueWithoutOperand() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        VerificationException e = assertThrows(
            VerificationException.class,
            () -> planPromql(
                "PROMQL index=k8s step=5m result=(sum without (pod) (network.eth0.tx) "
                    + "/ on (cluster) sum by (cluster) (network.eth0.rx))"
            )
        );
        assertThat(e.getMessage(), containsString("vector matching requires operands with concrete label sets"));
    }

    public void testVectorMatchRejectsOpaqueSelectorOperand() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        VerificationException e = assertThrows(
            VerificationException.class,
            () -> planPromql("PROMQL index=k8s step=5m result=(sum by (cluster) (network.eth0.tx) / on (cluster) network.eth0.rx)")
        );
        assertThat(e.getMessage(), containsString("vector matching requires operands with concrete label sets"));
    }

    public void testVectorMatchComposedWithOpaqueOperandRejected() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        // The unmatched + composes over a vector match and therefore translates as another join: it inherits the
        // concrete-label requirement, which the bare selector operand does not satisfy.
        VerificationException e = assertThrows(
            VerificationException.class,
            () -> planPromql(
                "PROMQL index=k8s step=5m result=((sum by (cluster) (network.eth0.tx) "
                    + "/ on (cluster) sum by (cluster) (network.eth0.rx)) + network.eth0.rx)"
            )
        );
        assertThat(e.getMessage(), containsString("vector matching requires operands with concrete label sets"));
    }

    public void testGroupLabelMissingFromBuildDoesNotLeakFromProbe() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
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
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        // The vector match is nested as the right operand of `1 + (...)`; the join is built and the scalar op wraps its value.
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(1 + (sum by (cluster) (network.eth0.tx) "
                + "/ on (cluster) sum by (cluster) (network.eth0.rx)))"
        );
        assertThat(plan.collect(InnerJoin.class), hasSize(1));
    }

    public void testVectorMatchNestedInAggregation() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        // sum(...) aggregates over the vector-match result.
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(sum(sum by (cluster) (network.eth0.tx) "
                + "/ on (cluster) sum by (cluster) (network.eth0.rx)))"
        );
        assertThat(plan.collect(InnerJoin.class), hasSize(1));
    }

    public void testVectorMatchNestedInFunction() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        // abs(...) applies over the vector-match result value.
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(abs(sum by (cluster) (network.eth0.tx) "
                + "/ on (cluster) sum by (cluster) (network.eth0.rx)))"
        );
        assertThat(plan.collect(InnerJoin.class), hasSize(1));
    }

    public void testVectorMatchComparisonInsideUnion() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        // A filter-mode comparison vector match composes as a union branch.
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=((sum by (cluster) (network.eth0.tx) "
                + "> on (cluster) sum by (cluster) (network.eth0.rx)) or sum by (cluster) (network.eth0.rx))"
        );
        assertThat(plan.collect(InnerJoin.class), hasSize(1));
    }

    public void testVectorMatchComposedWithPlainVectorOperand() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        // The vector-match result is itself an operand of an unmatched binary operator.
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=((sum by (cluster) (network.eth0.tx) "
                + "/ on (cluster) sum by (cluster) (network.eth0.rx)) + sum by (cluster) (network.eth0.rx))"
        );
        assertThat(plan.collect(InnerJoin.class), hasSize(2));
    }

    public void testTopKOverVectorMatchInsideUnion() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        // topk over a vector match inside a union branch: the branch-local step id must survive the join.
        var plan = planPromql(
            "PROMQL index=k8s step=5m result=(topk(2, sum by (cluster) (network.eth0.tx) "
                + "/ on (cluster) sum by (cluster) (network.eth0.rx)) or sum by (cluster) (network.eth0.rx))"
        );
        assertThat(plan.collect(InnerJoin.class), hasSize(1));
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

    // -- operands exposing the metric name --
    // Only an index with a `__name__` dimension (the remote-write layout) can expose it; `by (__name__, ...)` keeps it on
    // an operand, and the operator must still treat it the way Prometheus does: never part of the default match
    // signature, dropped from the result of arithmetic and `bool` comparisons, kept by filter comparisons.

    public void testArithmeticOverNamedOperandsDropsTheMetricNameFromTheResult() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        for (String query : List.of(
            // fused aggregates
            "sum by (__name__, cluster) (requests) / sum by (__name__, cluster) (errors)",
            // scalar operand
            "sum by (__name__, cluster) (requests) * 2",
            // many-to-one: the many side carries the name
            "sum by (__name__, cluster) (requests) / on (cluster) group_left sum by (cluster) (errors)",
            // `bool` drops the name like arithmetic
            "sum by (__name__, cluster) (requests) > bool 5"
        )) {
            LogicalPlan plan = planMetricNameIndex(query);
            assertThat(query, plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step", "cluster")));
        }
    }

    public void testFilterComparisonOverNamedOperandsKeepsTheMetricName() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        LogicalPlan plan = planMetricNameIndex("sum by (__name__, cluster) (requests) > 5");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step", "__name__", "cluster")));
    }

    public void testFusedAggregatesDoNotGroupOnTheMetricName() {
        // Each operand is one metric, so over the remote-write layout the sides never share a `__name__`: grouping the
        // fused aggregate on it would put them in disjoint groups and pair nothing. Prometheus pairs on the other labels.
        LogicalPlan plan = planMetricNameIndex("sum by (__name__, cluster) (requests) / sum by (__name__, cluster) (errors)");
        // Both operands fold into one aggregation pipeline (phase 1 and 2 of one collapse), which reads `cluster` from the
        // relation but not `__name__`, however the transport version lays the dimensions out.
        List<Aggregate> aggregates = plan.collect(Aggregate.class);
        assertThat(aggregates.stream().filter(a -> a instanceof TimeSeriesAggregate).toList(), hasSize(1));
        Set<String> fields = new TreeSet<>();
        for (Aggregate aggregate : aggregates) {
            aggregate.groupings().forEach(g -> g.forEachDown(FieldAttribute.class, f -> fields.add(f.name())));
            aggregate.aggregates().forEach(a -> a.forEachDown(FieldAttribute.class, f -> fields.add(f.name())));
        }
        assertThat(fields, hasItem("cluster"));
        assertThat(fields, not(hasItem("__name__")));
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step", "cluster")));
    }

    public void testDefaultMatchKeyExcludesTheMetricName() {
        assumeTrue("PromQL vector matching is required", EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V1.isEnabled());
        // Different label sets force the join; `ignoring (pod)` leaves `__name__` in both label sets, and the signature
        // must still leave it out or operands of different metrics would never match.
        LogicalPlan plan = planMetricNameIndex(
            "sum by (__name__, cluster, pod) (requests) / ignoring (pod) sum by (__name__, cluster) (errors)"
        );
        InnerJoin join = plan.collect(InnerJoin.class).getFirst();
        assertThat(keyedLabels(join.left()), equalTo(List.of("cluster")));
        assertThat(keyedLabels(join.right()), equalTo(List.of("cluster")));
        // one-to-one `ignoring (pod)` also drops `pod` from the result, like Prometheus
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step", "cluster")));
    }

    /** Plans against a remote-write shaped index: `__name__` is a dimension, every metric its own field. */
    private LogicalPlan planMetricNameIndex(String promql) {
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
