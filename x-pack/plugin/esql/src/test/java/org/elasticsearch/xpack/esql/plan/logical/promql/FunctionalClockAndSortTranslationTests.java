/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateExtract;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesSet;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.hamcrest.Matchers.containsString;

/** Source-free functions use evaluation clocks, while sorting preserves identity and discards histogram samples. */
public class FunctionalClockAndSortTranslationTests extends ESTestCase {
    private static final String BOUNDS = "start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T00:02:00Z\" step=1m";
    private static final String INSTANT = "time=\"2025-01-01T00:00:00Z\"";

    public void testBoundedFunctionsDoNotDependOnSourceRows() {
        for (String expression : List.of("time()", "pi()", "hour()", "day_of_month()", "vector(time())", "vector(pi())")) {
            for (String index : List.of("k8s", "empty_index")) {
                TestAnalyzer analyzer = prototype().addK8s().addEmptyIndex().unmappedResolution(UnmappedResolution.NULLIFY);
                LogicalPlan plan = analyze(analyzer, index, expression, BOUNDS);
                assertTrue(expression, plan.collect(EsRelation.class).isEmpty());
                assertEquals(expression, 1, plan.collect(MvExpand.class).size());
                Row grid = plan.collect(Row.class).getFirst();
                long first = Instant.parse("2025-01-01T00:00:00Z").toEpochMilli();
                assertEquals(List.of(first, first + 60_000L, first + 120_000L), ((Literal) grid.fields().getFirst().child()).value());
                if (expression.contains("time()")) {
                    Attribute clock = plan.collect(MvExpand.class).getFirst().expanded();
                    for (Div time : timeExpressions(plan)) {
                        assertTrue(expression, time.references().stream().allMatch(attribute -> attribute.semanticEquals(clock)));
                    }
                }
            }
        }
    }

    public void testDateFunctionsReturnVectorsAndTimeAndPiReturnScalars() {
        for (String name : List.of("time", "pi")) {
            assertEquals(
                PromqlDataType.SCALAR,
                new ScalarFunction(Source.EMPTY, PromqlFunctionRegistry.INSTANCE.functionMetadata(name)).returnType()
            );
            analyze("vector(" + name + "())", BOUNDS);
        }
        for (String name : List.of("hour", "day_of_month", "month", "year")) {
            assertEquals(
                PromqlDataType.INSTANT_VECTOR,
                new ScalarFunction(Source.EMPTY, PromqlFunctionRegistry.INSTANCE.functionMetadata(name)).returnType()
            );
            analyze("scalar(" + name + "())", BOUNDS);
            VerificationException error = expectThrows(VerificationException.class, () -> analyze("vector(" + name + "())", BOUNDS));
            assertThat(error.getMessage(), containsString("expected type scalar"));
            assertThat(error.getMessage(), containsString("got instant_vector"));
        }
    }

    public void testZeroArgumentDateUsesTheGridStep() {
        LogicalPlan plan = analyze("hour()", BOUNDS);
        Attribute clock = plan.collect(MvExpand.class).getFirst().expanded();
        var dates = new ArrayList<DateExtract>();
        plan.forEachExpressionDown(DateExtract.class, dates::add);
        assertFalse(dates.isEmpty());
        for (DateExtract date : dates) {
            assertEquals(1, date.references().size());
            assertTrue(date.references().stream().allMatch(attribute -> attribute.semanticEquals(clock)));
        }
    }

    /** Scalar arithmetic must stay defined on every requested step, independently of source rows and operand order. */
    public void testScalarArithmeticUsesTheRequestedClock() {
        for (String expression : List.of(
            "time() + time()",
            "time() + 1",
            "1 + time()",
            "time() * 2",
            "2 * time()",
            "(time() + 1) * pi()",
            "vector(time() + time())"
        )) {
            for (String index : List.of("k8s", "empty_index")) {
                LogicalPlan plan = analyze(
                    prototype().addK8s().addEmptyIndex().unmappedResolution(UnmappedResolution.NULLIFY),
                    index,
                    expression,
                    BOUNDS
                );
                assertTrue(expression, plan.collect(EsRelation.class).isEmpty());
                assertEquals(DataType.DOUBLE, plan.output().getFirst().dataType());
                assertTimeReferencesProduced(plan);
            }
        }
    }

    /** Constant vector and date-function arithmetic must not require a source aggregate when the source is empty. */
    public void testLiteralOperandsPreserveSourceFreeVectorClocks() {
        Map<String, Double> expected = Map.of(
            "1 + 2",
            3.0,
            "vector(1) + vector(2)",
            3.0,
            "hour() * 2",
            0.0,
            "2 * hour()",
            0.0,
            "vector(1) * 2",
            2.0,
            "2 * vector(1)",
            2.0,
            "abs(hour()) + 1",
            1.0
        );
        for (var entry : expected.entrySet()) {
            String expression = entry.getKey();
            for (String index : List.of("k8s", "empty_index")) {
                LogicalPlan plan = analyze(
                    prototype().addK8s().addEmptyIndex().unmappedResolution(UnmappedResolution.NULLIFY),
                    index,
                    expression,
                    BOUNDS
                );
                assertTrue(expression, plan.collect(EsRelation.class).isEmpty());
                assertEquals(DataType.DOUBLE, plan.output().getFirst().dataType());
                assertEquals(expression, entry.getValue(), foldSourceFreeResult(plan));
            }
        }
    }

    public void testUnboundedFunctionsUseFiniteSourceClocks() {
        for (String expression : List.of("time()", "pi()", "hour()", "vector(time())")) {
            LogicalPlan plan = analyze(expression, "step=1m");
            assertTrue(expression, plan.collect(MvExpand.class).isEmpty());
            assertEquals(expression, 1, plan.collect(EsRelation.class).size());
            assertFalse(expression, plan.collect(TimeSeriesAggregate.class).isEmpty());
            if (expression.contains("time()")) {
                assertTimeReferencesProduced(plan);
            }
        }
    }

    public void testTimeSharesRelabeledVectorsOffsetClock() {
        LogicalPlan plan = analyze("time() + label_join(network.bytes_in offset 1h, \"copy\", \"\", \"cluster\")", "step=1m");
        assertTrue(plan.collect(MvExpand.class).isEmpty());
        assertTimeReferencesProduced(plan);
        assertHasRelabel(plan);
        List<TimeSeriesAggregate> aggregates = plan.collect(TimeSeriesAggregate.class);
        assertTrue(aggregates.size() > 1);
        for (TimeSeriesAggregate aggregate : aggregates) {
            var aliases = new ArrayList<Alias>();
            aggregate.child().forEachExpressionDown(Alias.class, aliases::add);
            assertTrue(
                aggregate.toString(),
                aliases.stream()
                    .anyMatch(
                        alias -> alias.toAttribute().semanticEquals(aggregate.timestamp())
                            && alias.child() instanceof Add add
                            && add.children()
                                .stream()
                                .anyMatch(child -> child instanceof Literal literal && Duration.ofHours(1).equals(literal.value()))
                    )
            );
        }
    }

    public void testTimeBindsAfterDistinctClocksAreDeduplicated() {
        LogicalPlan plan = analyze("vector(time()) or (network.bytes_in offset 1h) or network.bytes_in", "step=1m");
        assertTrue(plan.collect(MvExpand.class).isEmpty());
        List<UnionAll> clocks = plan.collect(UnionAll.class)
            .stream()
            .filter(union -> union.output().size() == 1 && union.output().getFirst().dataType() == DataType.DATETIME)
            .toList();
        assertEquals(plan.toString(), 1, clocks.size());
        UnionAll clock = clocks.getFirst();
        assertEquals(2, clock.children().size());
        assertTrue(
            plan.collect(Aggregate.class)
                .stream()
                .anyMatch(aggregate -> aggregate.child() == clock && aggregate.groupings().equals(clock.output()))
        );
        for (Div time : timeExpressions(plan)) {
            assertEquals(1, time.references().size());
            assertTrue(time.references().stream().allMatch(attribute -> attribute.semanticEquals(clock.output().getFirst())));
        }
        assertTimeReferencesProduced(plan);
    }

    public void testInstantSortRetainsRelabelAndSetIdentity() {
        for (String function : List.of("sort", "sort_desc")) {
            for (String operand : List.of(
                "label_join(network.bytes_in, \"copy\", \"\", \"cluster\")",
                "label_join(network.bytes_in, \"copy\", \"\", \"cluster\") or network.total_bytes_out"
            )) {
                LogicalPlan plan = analyze(function + "(" + operand + ")", INSTANT);
                assertHasRelabel(plan);
                assertEquals(1, plan.collect(OrderBy.class).size());
                OrderBy order = plan.collect(OrderBy.class).getFirst();
                Order valueOrder = order.order().getLast();
                assertEquals(function.equals("sort") ? Order.OrderDirection.ASC : Order.OrderDirection.DESC, valueOrder.direction());
                assertTrue(valueOrder.child().semanticEquals(plan.output().getFirst()));
                if (operand.contains(" or ")) {
                    assertTrue(
                        plan.collect(UnionAll.class)
                            .stream()
                            .anyMatch(
                                union -> union.children().size() == 2
                                    && union.output().stream().anyMatch(attribute -> attribute.dataType() == DataType.TIMESERIES_DATA_TYPE)
                            )
                    );
                }
            }
        }
    }

    public void testHistogramSortDiscardsSamples() {
        for (String function : List.of("sort", "sort_desc")) {
            LogicalPlan plan = analyze(
                prototype().addIndex("histograms", "exp_histo_sample-mappings.json", IndexMode.TIME_SERIES),
                "histograms",
                function + "(label_replace(responseTime, \"instance\", \"\", \"instance\", \".*\"))",
                INSTANT
            );
            assertTrue(plan.collect(Filter.class).stream().anyMatch(filter -> Literal.FALSE.equals(filter.condition())));
            assertEquals(DataType.DOUBLE, plan.output().getFirst().dataType());
            plan.forEachExpressionDown(ToDouble.class, cast -> assertFalse(cast.field().dataType().isHistogram()));
        }
    }

    public void testNestedSortRemainsRejected() {
        for (String expression : List.of("abs(sort(network.bytes_in))", "sort(network.bytes_in) + 1", "sum(sort_desc(network.bytes_in))")) {
            VerificationException error = expectThrows(VerificationException.class, () -> analyze(expression, INSTANT));
            assertThat(error.getMessage(), containsString("only supported at the top-level"));
        }
    }

    public void testDisabledAndOlderClustersKeepLegacyFunctionsAndSort() {
        for (TestAnalyzer analyzer : List.of(
            prototype().flags(EsqlFlags.DEFAULTS),
            prototype().minimumTransportVersion(TransportVersionUtils.getPreviousVersion(featureVersion()))
        )) {
            analyzer.addK8s();
            for (String expression : List.of("time()", "pi()", "hour()", "vector(time())", "sort(network.bytes_in)")) {
                LogicalPlan plan = analyze(analyzer, "k8s", expression, INSTANT);
                plan.forEachExpressionDown(TimeSeriesFunction.class, function -> fail("Legacy query uses typed metadata: " + expression));
                plan.forEachExpressionDown(
                    Attribute.class,
                    attribute -> assertNotEquals(expression, DataType.TIMESERIES_DATA_TYPE, attribute.dataType())
                );
            }
        }
    }

    private static void assertTimeReferencesProduced(LogicalPlan plan) {
        List<Div> expressions = timeExpressions(plan);
        for (Div time : expressions) {
            assertEquals(1, time.references().size());
            Attribute reference = time.references().iterator().next();
            assertEquals(DataType.DATETIME, reference.dataType());
            assertTrue(
                time.toString(),
                plan.collect(Eval.class)
                    .stream()
                    .anyMatch(
                        eval -> eval.fields().stream().anyMatch(field -> field.anyMatch(expression -> expression == time))
                            && eval.child().output().stream().anyMatch(attribute -> attribute.semanticEquals(reference))
                    )
            );
        }
    }

    /** Resolve the emitted value through its actual attribute identities at the first bounded evaluation step. */
    private static Object foldSourceFreeResult(LogicalPlan plan) {
        Map<NameId, Expression> definitions = new HashMap<>();
        plan.forEachExpressionDown(Alias.class, alias -> definitions.put(alias.id(), alias.child()));
        for (MvExpand expand : plan.collect(MvExpand.class)) {
            Row row = expand.collect(Row.class).getFirst();
            Literal grid = (Literal) row.fields().getFirst().child();
            definitions.put(expand.expanded().id(), new Literal(Source.EMPTY, ((List<?>) grid.value()).getFirst(), DataType.DATETIME));
        }
        Expression value = resolveAliases(plan.output().getFirst(), definitions, new HashSet<>());
        assertTrue(value.toString(), value.foldable());
        return value.fold(FoldContext.small());
    }

    private static Expression resolveAliases(Expression expression, Map<NameId, Expression> definitions, Set<NameId> active) {
        if (expression instanceof Attribute attribute) {
            assertTrue("Cyclic value definition: " + attribute, active.add(attribute.id()));
            Expression definition = definitions.get(attribute.id());
            assertNotNull("Missing value definition: " + attribute, definition);
            Expression resolved = resolveAliases(definition, definitions, active);
            active.remove(attribute.id());
            return resolved;
        }
        return expression.children().isEmpty()
            ? expression
            : expression.replaceChildren(expression.children().stream().map(child -> resolveAliases(child, definitions, active)).toList());
    }

    private static List<Div> timeExpressions(LogicalPlan plan) {
        var expressions = new ArrayList<Div>();
        plan.forEachExpressionDown(Div.class, division -> {
            if (division.right() instanceof Literal literal && Double.valueOf(1000.0).equals(literal.value())) {
                expressions.add(division);
            }
        });
        assertFalse(plan.toString(), expressions.isEmpty());
        return expressions;
    }

    private static void assertHasRelabel(LogicalPlan plan) {
        var functions = new ArrayList<ToString>();
        plan.forEachExpressionDown(ToString.class, function -> {
            if (function.field().dataType() == DataType.TIMESERIES_DATA_TYPE) {
                functions.add(function);
            }
        });
        assertTrue(
            functions.stream()
                .anyMatch(
                    function -> function.field()
                        .anyMatch(
                            child -> child instanceof TimeSeriesSet set
                                && set.children().get(1).semanticEquals(Literal.keyword(Source.EMPTY, "copy"))
                        )
                )
        );
    }

    private static LogicalPlan analyze(String expression, String bounds) {
        return analyze(prototype().addK8s(), "k8s", expression, bounds);
    }

    private static LogicalPlan analyze(TestAnalyzer analyzer, String index, String expression, String bounds) {
        LogicalPlan plan = analyzer.query("PROMQL index=" + index + " " + bounds + " result=(" + expression + ") | LIMIT 10");
        Failures failures = new Failures();
        plan.forEachUp(node -> PlanConsistencyChecker.checkPlan(node, failures));
        assertFalse(expression + ": " + failures, failures.hasFailures());
        assertTrue(expression, plan.output().stream().allMatch(Attribute::resolved));
        return plan;
    }

    private static TestAnalyzer prototype() {
        return analyzer().minimumTransportVersion(featureVersion()).flags(EsqlFlags.DEFAULTS.withPromqlFunctionalTimeSeries(true));
    }

    private static TransportVersion featureVersion() {
        return TransportVersion.fromName("promql_functional_timeseries");
    }
}
