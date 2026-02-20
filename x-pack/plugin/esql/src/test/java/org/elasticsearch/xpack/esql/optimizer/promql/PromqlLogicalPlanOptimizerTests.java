/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.hamcrest.Matcher;
import org.junit.BeforeClass;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.plan.QuerySettings.UNMAPPED_FIELDS;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

// @TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug tests")
public class PromqlLogicalPlanOptimizerTests extends AbstractLogicalPlanOptimizerTests {

    private static Analyzer tsAnalyzer;

    @BeforeClass
    public static void initTest() {
        var timeSeriesMapping = loadMapping("k8s-mappings.json");
        var timeSeriesIndex = IndexResolution.valid(
            new EsIndex("k8s", timeSeriesMapping, Map.of("k8s", IndexMode.TIME_SERIES), Map.of(), Map.of(), Set.of())
        );
        tsAnalyzer = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                Map.of(new IndexPattern(Source.EMPTY, "k8s"), timeSeriesIndex),
                emptyMap(),
                enrichResolution,
                emptyInferenceResolution(),
                TransportVersion.current(),
                UNMAPPED_FIELDS.defaultValue()
            ),
            TEST_VERIFIER
        );
    }

    public void testAvgAvgOverTimeOutput() {
        var plan = planPromql("""
            PROMQL index=k8s step=1h ( avg by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[1h])) )
            | LIMIT 1000
            """);

        var project = as(plan, Project.class);
        assertThat(project.projections(), hasSize(3));

        var aggregate = plan.collect(Aggregate.class).getFirst();
        assertThat(aggregate.groupings(), hasSize(2));

        var evalMiddle = as(aggregate.child(), Eval.class);

        var tsAggregate = as(evalMiddle.child(), TimeSeriesAggregate.class);
        assertThat(tsAggregate.groupings(), hasSize(2));

        // verify TBUCKET duration plus reuse
        var evalBucket = as(tsAggregate.child(), Eval.class);
        assertThat(evalBucket.fields(), hasSize(1));
        var bucketAlias = as(evalBucket.fields().get(0), Alias.class);
        var bucket = as(bucketAlias.child(), Bucket.class);

        var bucketSpan = bucket.buckets();
        assertThat(bucketSpan.fold(FoldContext.small()), equalTo(Duration.ofHours(1)));

        var tbucketId = bucketAlias.toAttribute().id();
        assertThat(Expressions.attribute(tsAggregate.groupings().get(1)).id(), equalTo(tbucketId));
        assertThat(Expressions.attribute(aggregate.groupings().get(0)).id(), equalTo(tbucketId));
        assertThat(Expressions.attribute(project.projections().get(1)).id(), equalTo(tbucketId));

        // Filter should contain: IN(host-0, host-1, host-2, pod)
        var filter = as(evalBucket.child(), Filter.class);
        var in = as(filter.condition(), In.class);
        assertThat(in.list(), hasSize(3));

        as(filter.child(), EsRelation.class);
    }

    public void testPromqlTrailingSpaces() {
        planPromql("PROMQL index=k8s step=1h (max(network.bytes_in)) ");
        planPromql("PROMQL index=k8s step=1h (max(network.bytes_in)) | SORT step");
    }

    public void testPromqlMaxOfLongField() {
        var plan = planPromql("PROMQL index=k8s step=1h max(network.bytes_in)");
        // In PromQL, the output is always double
        assertThat(plan.output().getFirst().dataType(), equalTo(DataType.DOUBLE));
        assertThat(plan.output().getFirst().name(), equalTo("max(network.bytes_in)"));
    }

    public void testPromqlExplicitOutputName() {
        var plan = planPromql("PROMQL index=k8s step=1h max_bytes=(max(network.bytes_in))");
        assertThat(plan.output().getFirst().name(), equalTo("max_bytes"));
    }

    public void testSort() {
        var plan = planPromql("""
            PROMQL index=k8s step=1h (
                avg(network.bytes_in) by (pod)
              )
            | SORT step, pod, `avg(network.bytes_in) by (pod)`
            """);
        List<String> order = plan.collect(TopN.class)
            .getFirst()
            .order()
            .stream()
            .map(o -> as(o.child(), NamedExpression.class).name())
            .toList();
        assertThat(order, hasSize(3));
        assertThat(order, equalTo(List.of("step", "pod", "avg(network.bytes_in) by (pod)")));
    }

    public void testRangeSelector() {
        var plan = planPromql("""
            PROMQL index=k8s step=1h ( max by (pod) (last_over_time(network.bytes_in[1h])) )
            """);
        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        LastOverTime lastOverTime = tsAggregate.aggregates().getFirst().collect(LastOverTime.class).getFirst();
        assertThat(lastOverTime.window().fold(FoldContext.small()), equalTo(Duration.ofHours(1)));
    }

    /**
     * Expect the logical plan structure:
     * Project
     * \_Eval
     *   \_Limit
     *     \_Aggregate
     *       \_Eval
     *         \_TimeSeriesAggregate[[...],[SUM(...,PT10M,...), COUNT(...,PT10M,...), ...], BUCKET(@timestamp,PT5M)]
     */
    public void testRangeSelectorWithDifferentStep() {
        var plan = planPromql("""
            PROMQL index=k8s step=5m sum by (pod) (avg_over_time(events_received[10m]))
            """);

        var tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();

        // Verify bucket is 5 minutes
        assertThat(tsAggregate.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(5)));

        // Verify window is 10 minutes
        var sum = tsAggregate.aggregates().getFirst().collect(Sum.class).getFirst();
        assertThat(sum.window().fold(FoldContext.small()), equalTo(Duration.ofMinutes(10)));
    }

    public void testImplicitRangeSelectorUsesStepWindow() {
        var plan = planPromql("""
            PROMQL index=k8s step=5m rate=(rate(network.total_bytes_in))
            """);

        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        Rate rate = tsAggregate.aggregates().getFirst().collect(Rate.class).getFirst();
        assertThat(rate.window().fold(FoldContext.small()), equalTo(Duration.ofMinutes(5)));
    }

    public void testImplicitRangeSelectorUsesScrapeIntervalWhenStepIsSmaller() {
        var plan = planPromql("""
            PROMQL index=k8s step=15s rate=(rate(network.total_bytes_in))
            """);

        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        Rate rate = tsAggregate.aggregates().getFirst().collect(Rate.class).getFirst();
        assertThat(rate.window().fold(FoldContext.small()), equalTo(Duration.ofMinutes(1)));
    }

    public void testImplicitRangeSelectorRoundsWindowToStepMultiple() {
        var plan = planPromql("""
            PROMQL index=k8s step=20s scrape_interval=1m rate=(rate(network.total_bytes_in))
            """);

        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        Rate rate = tsAggregate.aggregates().getFirst().collect(Rate.class).getFirst();
        assertThat(rate.window().fold(FoldContext.small()), equalTo(Duration.ofMinutes(1)));
    }

    public void testImplicitRangeSelectorUsesInferredStepFromDefaultBuckets() {
        var plan = planPromql("""
            PROMQL index=k8s start="2024-05-10T00:00:00.000Z" end="2024-05-10T01:00:00.000Z" rate=(rate(network.total_bytes_in))
            """);

        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(tsAggregate.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(1)));

        Rate rate = tsAggregate.aggregates().getFirst().collect(Rate.class).getFirst();
        assertThat(rate.window().fold(FoldContext.small()), equalTo(Duration.ofMinutes(1)));
    }

    public void testImplicitRangeSelectorUsesInferredStepFromBuckets() {
        var plan = planPromql("""
            PROMQL index=k8s start="2024-05-10T00:00:00.000Z" end="2024-05-10T01:00:00.000Z" buckets=6 rate=(rate(network.total_bytes_in))
            """);

        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(tsAggregate.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(10)));

        Rate rate = tsAggregate.aggregates().getFirst().collect(Rate.class).getFirst();
        assertThat(rate.window().fold(FoldContext.small()), equalTo(Duration.ofMinutes(10)));
    }

    public void testStartEndStep() {
        String testQuery = """
            PROMQL index=k8s start=$now-1h end=$now step=5m (
                avg(avg_over_time(network.bytes_in[5m]))
                )
            """;

        var plan = planPromql(testQuery);
        var filters = plan.collect(Filter.class);
        assertThat(
            filters.stream()
                .map(Filter::condition)
                .flatMap(c -> c.collect(FieldAttribute.class).stream())
                .map(FieldAttribute::name)
                .filter("@timestamp"::equals)
                .count(),
            equalTo(2L)
        );
    }

    public void testInferredStepUsesDefaultBuckets() {
        var plan = planPromql("""
            PROMQL index=k8s start="2024-05-10T00:00:00.000Z" end="2024-05-10T01:00:00.000Z" (
                avg(avg_over_time(network.bytes_in[6m]))
              )
            """);
        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(tsAggregate.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(1)));
    }

    public void testInferredStepMinStepIsUnknownParameter() {
        ParsingException e = assertThrows(ParsingException.class, () -> planPromql("""
            PROMQL index=k8s start="2024-05-10T00:00:00.000Z" end="2024-05-10T01:00:00.000Z" min_step=1s (
                avg(avg_over_time(network.bytes_in[6m]))
              )
            """));
        assertThat(e.getMessage(), containsString("Unknown parameter [min_step]"));
    }

    public void testInferredStepUsesBuckets() {
        var plan = planPromql("""
            PROMQL index=k8s start="2024-05-10T00:00:00.000Z" end="2024-05-10T01:00:00.000Z" buckets=6 (
                avg(avg_over_time(network.bytes_in[1h]))
              )
            """);
        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(tsAggregate.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(10)));
    }

    public void testLabelSelector() {
        var plan = planPromql("""
            PROMQL index=k8s step=1m (
                max by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[5m]))
              )
            """);
        var filters = plan.collect(Filter.class);
        Optional<In> in = filters.stream().map(Filter::condition).filter(In.class::isInstance).map(In.class::cast).findAny();
        assertThat(in.isPresent(), equalTo(true));
        assertThat(in.get().value().sourceText(), equalTo("pod"));
        assertThat(in.get().list().stream().map(Expression::toString).toList(), containsInAnyOrder("host-0", "host-1", "host-2"));
    }

    public void testLabelSelectorPrefix() {
        String testQuery = """
            PROMQL index=k8s step=1m (
                avg by (pod) (avg_over_time(network.bytes_in{pod=~"host-.*"}[5m]))
                )
            """;

        var plan = planPromql(testQuery);
        var filters = plan.collect(Filter.class);
        assertThat(filters.stream().map(Filter::condition).anyMatch(StartsWith.class::isInstance), equalTo(true));
        assertThat(filters.stream().map(Filter::condition).anyMatch(NotEquals.class::isInstance), equalTo(false));
    }

    public void testLabelSelectorProperPrefix() {
        var plan = planPromql("""
            PROMQL index=k8s step=1m (
                avg(avg_over_time(network.bytes_in{pod=~"host-.+"}[1h]))
              )
            """);

        var filters = plan.collect(Filter.class);
        assertThat(filters.stream().anyMatch(f -> f.condition().anyMatch(StartsWith.class::isInstance)), equalTo(true));
        assertThat(filters.stream().anyMatch(f -> f.condition().anyMatch(NotEquals.class::isInstance)), equalTo(true));
    }

    public void testLabelSelectorRegex() {
        var plan = planPromql("""
            PROMQL index=k8s step=1m (
                avg(avg_over_time(network.bytes_in{pod=~"[a-z]+"}[1h]))
              )
            """);

        var filters = plan.collect(Filter.class);
        assertThat(filters.stream().map(Filter::condition).anyMatch(RegexMatch.class::isInstance), equalTo(true));
    }

    public void testLabelSelectorNotEquals() {
        var plan = planPromql("PROMQL index=k8s step=1m avg(network.bytes_in{pod!=\"foo\"})");

        var not = plan.collect(Filter.class)
            .stream()
            .map(Filter::condition)
            .filter(Not.class::isInstance)
            .map(Not.class::cast)
            .findFirst()
            .get();
        var in = as(not.field(), In.class);
        assertThat(as(in.value(), FieldAttribute.class).name(), equalTo("pod"));
        assertThat(in.list(), hasSize(1));
        assertThat(as(as(in.list().getFirst(), Literal.class).value(), BytesRef.class).utf8ToString(), equalTo("foo"));
    }

    public void testLabelSelectorRegexNegation() {
        var plan = planPromql("PROMQL index=k8s step=1m avg(network.bytes_in{pod!~\"f.o\"})");

        var filters = plan.collect(Filter.class);
        var not = filters.stream().map(Filter::condition).filter(Not.class::isInstance).map(Not.class::cast).findFirst().get();
        var rLike = as(not.field(), RLike.class);
        assertThat(as(rLike.field(), FieldAttribute.class).name(), equalTo("pod"));
        assertThat(rLike.pattern().pattern(), equalTo("f.o"));
    }

    public void testLabelSelectors() {
        var plan = planPromql("PROMQL index=k8s step=1m avg(network.bytes_in{pod!=\"foo\",cluster=~\"bar|baz\",region!~\"us-.*\"})");

        var filters = plan.collect(Filter.class);
        var and = filters.stream().map(Filter::condition).filter(And.class::isInstance).map(And.class::cast).findFirst().get();
        if (and.left() instanceof IsNotNull) {
            and = as(and.right(), And.class);
        }
        var left = as(and.left(), And.class);
        var podNotFoo = as(as(left.left(), Not.class).field(), In.class);
        assertThat(podNotFoo.list(), hasSize(1));
        assertThat(as(podNotFoo.list().getFirst(), Literal.class).value(), equalTo(new BytesRef("foo")));

        var clusterInBarBaz = as(left.right(), In.class);
        assertThat(clusterInBarBaz.list(), hasSize(2));
        assertThat(as(clusterInBarBaz.list().get(0), Literal.class).value(), equalTo(new BytesRef("bar")));
        assertThat(as(clusterInBarBaz.list().get(1), Literal.class).value(), equalTo(new BytesRef("baz")));

        var regionNotUs = as(as(and.right(), Not.class).field(), StartsWith.class);
        assertThat(as(regionNotUs.prefix(), Literal.class).value(), equalTo(new BytesRef("us-")));
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

    public void testConstantFoldingArithmeticOperators() {
        var plan = planPromqlExpectNoReferences("PROMQL index=k8s step=5m 1 + 1");
        var eval = plan.collect(Eval.class).getFirst();
        var literal = as(eval.fields().getFirst().child(), Literal.class);
        assertThat(literal.value(), equalTo(2.0));
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

    public void testGroupByAllWithinSeriesAggregate() {
        var plan = planPromql("PROMQL index=k8s step=1m count=(count_over_time(network.bytes_in[1m]))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("count", "step", "_timeseries")));
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

    public void testBinaryArithmeticScalarFunctions() {
        assertConstantResult("pi() - pi()", equalTo(0.0));
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

    public void testGroupByAllInstantSelector() {
        var plan = planPromql("PROMQL index=k8s step=1m network.bytes_in");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("network.bytes_in", "step", "_timeseries")));
    }

    public void testGroupByAllInstantSelectorRate() {
        var plan = planPromql("PROMQL index=k8s step=1m rate=(rate(network.total_bytes_in[1m]))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("rate", "step", "_timeseries")));
    }

    public void testConstantResults() {
        assertConstantResult("ceil(vector(3.14159))", equalTo(4.0));
        assertConstantResult("pi()", equalTo(Math.PI));
        assertConstantResult("abs(vector(-1))", equalTo(1.0));
        assertConstantResult("quantile(0.5, vector(1))", equalTo(1.0));
    }

    public void testRound() {
        assertConstantResult("round(vector(pi()))", equalTo(3.0)); // round down to nearest integer
        assertConstantResult("round(vector(pi()), 1)", equalTo(3.0)); // same as above but with explicit argument
        assertConstantResult("round(vector(pi()), 0.01)", equalTo(3.14)); // round down 2 decimal places
        assertConstantResult("round(vector(pi()), 0.001)", equalTo(3.142)); // round up 3 decimal places
        assertConstantResult("round(vector(pi()), 0.15)", equalTo(3.15)); // rounds up to nearest
        assertConstantResult("round(vector(pi()), 0.5)", equalTo(3.0)); // rounds down to nearest
    }

    public void testClamp() {
        assertConstantResult("clamp(vector(5), 0, 10)", equalTo(5.0));
        assertConstantResult("clamp(vector(-5), 0, 10)", equalTo(0.0));
        assertConstantResult("clamp(vector(15), 0, 10)", equalTo(10.0));
        assertConstantResult("clamp(vector(0), 0, 10)", equalTo(0.0));
        assertConstantResult("clamp(vector(10), 0, 10)", equalTo(10.0));
    }

    public void testClampMin() {
        assertConstantResult("clamp_min(vector(5), 0)", equalTo(5.0));
        assertConstantResult("clamp_min(vector(-5), 0)", equalTo(0.0));
        assertConstantResult("clamp_min(vector(0), 0)", equalTo(0.0));
    }

    public void testClampMax() {
        assertConstantResult("clamp_max(vector(5), 10)", equalTo(5.0));
        assertConstantResult("clamp_max(vector(15), 10)", equalTo(10.0));
        assertConstantResult("clamp_max(vector(10), 10)", equalTo(10.0));
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

    public void testNonExistentFieldsOptimizesToEmptyPlan() {
        List.of("non_existent_metric", "network.eth0.rx{non_existent_label=\"value\"}", "avg(non_existent_metric)"
        // TODO because we wrap group-by-all aggregates into Values, this does not optimize away yet
        // "rate(non_existent_metric[5m])"
        ).forEach(query -> {
            var plan = planPromql("PROMQL index=k8s step=1m " + query);
            assertThat(as(plan, LocalRelation.class).supplier(), equalTo(EmptyLocalSupplier.EMPTY));
        });
    }

    public void testGroupByNonExistentLabel() {
        var plan = planPromql("PROMQL index=k8s step=1m result=(sum by (non_existent_label) (network.eth0.rx))");
        // equivalent to avg(network.eth0.rx) since the label does not exist
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step")));
        // the non-existent label should not appear in the groupings
        plan.collect(Aggregate.class)
            .forEach(
                agg -> assertThat(
                    agg.groupings().stream().map(Attribute.class::cast).map(Attribute::name).toList(),
                    not(hasItem("non_existent_label"))
                )
            );
    }

    private void assertConstantResult(String query, Matcher<Double> matcher) {
        var plan = planPromqlExpectNoReferences("PROMQL index=k8s step=1m " + query);
        Eval eval = plan.collect(Eval.class).getFirst();
        Literal literal = as(eval.fields().getFirst().child(), Literal.class);
        assertThat(as(literal.value(), Double.class), matcher);

        Aggregate aggregate = eval.collect(Aggregate.class).getFirst();
        ReferenceAttribute step = as(aggregate.groupings().getFirst(), ReferenceAttribute.class);
        assertThat(step.name(), equalTo("step"));

        TimeSeriesAggregate tsAgg = aggregate.collect(TimeSeriesAggregate.class).getFirst();
        ReferenceAttribute stepInTsAgg = as(Alias.unwrap(tsAgg.aggregates().getFirst()), ReferenceAttribute.class);
        assertThat(stepInTsAgg.name(), equalTo("step"));

        Eval stepEval = tsAgg.collect(Eval.class).getFirst();
        Alias bucketAlias = as(stepEval.fields().getFirst(), Alias.class);
        assertThat(bucketAlias.id(), equalTo(stepInTsAgg.id()));
        assertThat(bucketAlias.id(), equalTo(step.id()));
    }

    /**
     * Project[[result, step]]
     * \_Limit
     *   \_Filter[ISNOTNULL(result)]
     *     \_Eval[[CASE(count == 1, TODOUBLE(max), NaN) AS result, TODOUBLE(result) AS result]]
     *       \_Aggregate[[step],[COUNT(result) AS $$COUNT$result$0, MAX(result) AS $$MAX$result$1, step]]
     *         \_Aggregate[[step, pack_cluster],[SUM(...) AS result, step]]
     *           \_Eval[[PACKDIMENSION(cluster) AS pack_cluster]]
     *             \_TimeSeriesAggregate
     *               \_Eval[[BUCKET(@timestamp, PT1H) AS step]]
     *                 \_EsRelation[k8s]
     */
    public void testScalarInnerAggregate() {
        var plan = planPromql("PROMQL index=k8s step=1h result=(scalar(sum by (cluster) (network.bytes_in)))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step")));

        var project = as(plan, Project.class);
        var filter = project.collect(Filter.class).getFirst();

        var eval = as(filter.child(), Eval.class);
        assertThat(eval.fields(), hasSize(2));

        var scalarAgg = as(eval.child(), Aggregate.class);
        assertThat(scalarAgg.groupings(), hasSize(1));
        assertThat(Expressions.attribute(scalarAgg.groupings().getFirst()).name(), equalTo("step"));

        assertThat(scalarAgg.aggregates(), hasSize(3));

        var sumAgg = as(scalarAgg.child(), Aggregate.class);
        assertThat(sumAgg.groupings(), hasSize(2));
        assertThat(sumAgg.aggregates().getFirst().collect(Sum.class), not(empty()));

        var tsAgg = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(tsAgg.aggregates().getFirst().collect(LastOverTime.class), not(empty()));
    }

    /**
     * Project[[result, step]]
     * \_Limit
     *   \_Filter[ISNOTNULL(result)]
     *     \_Eval[[CASE(count == 1, TODOUBLE(max), NaN) AS result, TODOUBLE(result) AS result]]
     *       \_Aggregate[[step],[COUNT(...) AS $$COUNT$result$0, MAX(...) AS $$MAX$result$1, step]]
     *         \_TimeSeriesAggregate[[_tsid, step],[LASTOVERTIME(...) AS LASTOVERTIME_$1, step], BUCKET(@timestamp, PT1H)]
     *           \_Eval[[BUCKET(@timestamp, PT1H) AS step]]
     *             \_EsRelation[k8s]
     */
    public void testScalar() {
        var plan = planPromql("PROMQL index=k8s step=1h result=(scalar(network.bytes_in))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step")));

        var project = as(plan, Project.class);
        var filter = project.collect(Filter.class).getFirst();
        as(filter.condition(), IsNotNull.class);

        var eval = as(filter.child(), Eval.class);
        assertThat(eval.fields(), hasSize(2));

        var scalarAgg = as(eval.child(), Aggregate.class);
        assertThat(scalarAgg.groupings(), hasSize(1));
        assertThat(Expressions.attribute(scalarAgg.groupings().getFirst()).name(), equalTo("step"));
        assertThat(scalarAgg.aggregates(), hasSize(3));

        var tsAgg = as(scalarAgg.child(), TimeSeriesAggregate.class);
        assertThat(tsAgg.aggregates().getFirst().collect(LastOverTime.class), not(empty()));

        var bucketEval = as(tsAgg.child(), Eval.class);
        var bucketAlias = as(bucketEval.fields().getFirst(), Alias.class);
        var bucket = as(bucketAlias.child(), Bucket.class);
        assertThat(bucket.buckets().fold(FoldContext.small()), equalTo(Duration.ofHours(1)));
    }

    protected LogicalPlan planPromql(String query) {
        return planPromql(query, false);
    }

    protected LogicalPlan planPromqlExpectNoReferences(String query) {
        return planPromql(query, true);
    }

    protected LogicalPlan planPromql(String query, boolean allowEmptyReferences) {
        var now = Instant.now();
        query = query.replace("$now-1h", "\"" + now.minus(1, ChronoUnit.HOURS) + "\"");
        query = query.replace("$now", "\"" + now + "\"");
        var analyzed = tsAnalyzer.analyze(parser.parseQuery(query));
        AttributeSet.Builder references = AttributeSet.builder();
        analyzed.forEachDown(lp -> references.addAll(lp.references()));
        if (allowEmptyReferences) {
            assertThat(references.build(), empty());
        } else {
            assertThat(references.build(), not(empty()));
        }
        logger.trace("analyzed plan:\n{}", analyzed);
        var optimized = logicalOptimizer.optimize(analyzed);
        logger.trace("optimized plan:\n{}", optimized);
        return optimized;
    }
}
