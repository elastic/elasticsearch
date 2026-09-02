/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DefaultTimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.First;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Last;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.equalToIgnoringIds;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.ignoreIds;
import static org.hamcrest.Matchers.sameInstance;

public class InsertDefaultInnerTimeSeriesAggregateTests extends AbstractLogicalPlanOptimizerTests {

    private final InsertDefaultInnerTimeSeriesAggregate rule = new InsertDefaultInnerTimeSeriesAggregate();

    public void testSimpleImplicitOverTime() {
        assertStatsEqual("network.bytes_in", "last_over_time(network.bytes_in)");
        assertStatsEqual("sum(network.bytes_in)", "sum(last_over_time(network.bytes_in))");
    }

    public void testBinaryWithImplicitAndExplicitOverTime() {
        var expected = "last_over_time(network.eth0.tx) + last_over_time(network.eth0.rx)";
        assertStatsEqual("network.eth0.tx + network.eth0.rx", expected);
        assertStatsEqual("network.eth0.tx + last_over_time(network.eth0.rx)", expected);
        assertStatsEqual("last_over_time(network.eth0.tx) + network.eth0.rx", expected);
        assertStatsEqual(expected, expected);
    }

    public void testSumBinaryWithImplicitAndExplicitOverTime() {
        var expected = "sum(last_over_time(network.eth0.tx) + last_over_time(network.eth0.rx))";
        assertStatsEqual("sum(network.eth0.tx + network.eth0.rx)", expected);
        assertStatsEqual("sum(network.eth0.tx + last_over_time(network.eth0.rx))", expected);
        assertStatsEqual("sum(last_over_time(network.eth0.tx) + network.eth0.rx)", expected);
        assertStatsEqual(expected, expected);
    }

    public void testComplexArithmetic() {
        var expected = "last_over_time(network.eth0.tx) / 2 + last_over_time(network.eth0.rx) * 2";
        assertStatsEqual("network.eth0.tx / 2 + network.eth0.rx * 2", expected);
        assertStatsEqual("last_over_time(network.eth0.tx) / 2 + network.eth0.rx * 2", expected);
        assertStatsEqual("network.eth0.tx / 2 + last_over_time(network.eth0.rx) * 2", expected);
        assertStatsEqual(expected, expected);
    }

    public void testConversionFunctions() {
        // Basic casting case should happen before the inner aggregation
        assertStatsEqual("Avg(to_double(network.eth0.tx))", "Avg(last_over_time(to_double(network.eth0.tx)))");

        // Casting with an additional computation should be pulled after the inner aggregation
        assertStatsEqual("Avg(to_double(network.eth0.tx + 5))", "Avg(to_double(last_over_time(network.eth0.tx) + 5))");

        // Even complex casts should happen first, if they only cast
        assertStatsEqual("Avg(to_double(to_int(network.eth0.tx)))", "Avg(last_over_time(to_double(to_int(network.eth0.tx))))");
    }

    public void testFirstWithTimestampSort() {
        assertStatsEqual("FIRST(network.bytes_in, @timestamp)", "FIRST(first_over_time(network.bytes_in), min_over_time(@timestamp))");
    }

    public void testLastWithTimestampSort() {
        assertStatsEqual("LAST(network.bytes_in, @timestamp)", "LAST(last_over_time(network.bytes_in), max_over_time(@timestamp))");
    }

    /**
     * Regression test for a bug where {@link InsertDefaultInnerTimeSeriesAggregate} would throw
     * {@code unresolved_exception} instead of leaving the plan untouched when the sort argument of
     * {@code LAST}/{@code FIRST} was still unresolved (e.g. an {@code @timestamp} reference that hadn't
     * been resolved yet). This can happen mid-analysis, such as during the CCS retry path in
     * {@code EsqlSession#maybeRetryConcreteTimeSeriesResolution}, before the Verifier gets a chance to
     * report a normal resolution failure.
     */
    public void testLastWithUnresolvedSortIsLeftUntouched() {
        FieldAttribute timestamp = new FieldAttribute(
            Source.EMPTY,
            "@timestamp",
            new EsField("@timestamp", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        FieldAttribute field = new FieldAttribute(
            Source.EMPTY,
            "network.bytes_in",
            new EsField("network.bytes_in", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.METRIC)
        );
        UnresolvedAttribute unresolvedSort = new UnresolvedAttribute(Source.EMPTY, "@timestamp");
        NamedExpression aggregate = new Alias(Source.EMPTY, "last", new Last(Source.EMPTY, field, unresolvedSort));

        EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES).withAttributes(List.of(timestamp, field));
        TimeSeriesAggregate plan = new TimeSeriesAggregate(
            Source.EMPTY,
            relation,
            List.of(),
            List.of(aggregate),
            null,
            timestamp,
            TimeSeriesAggregate.Origin.TS_COMMAND
        );

        // Must not throw; the unresolved sort should be left untouched for the Verifier to report.
        TimeSeriesAggregate result = as(rule.rule(plan), TimeSeriesAggregate.class);
        assertThat(result, sameInstance(plan));
        Alias resultAlias = as(result.aggregates().getFirst(), Alias.class);
        Last resultLast = as(resultAlias.child(), Last.class);
        assertThat(resultLast.field(), sameInstance(field));
        assertThat(resultLast.sort(), sameInstance(unresolvedSort));
    }

    /**
     * Sister test to {@link #testLastWithUnresolvedSortIsLeftUntouched} covering the symmetric {@code FIRST}
     * path: an unresolved sort argument must leave the plan untouched rather than crashing in
     * {@code wrapSortedAgg}.
     */
    public void testFirstWithUnresolvedSortIsLeftUntouched() {
        FieldAttribute timestamp = new FieldAttribute(
            Source.EMPTY,
            "@timestamp",
            new EsField("@timestamp", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        FieldAttribute field = new FieldAttribute(
            Source.EMPTY,
            "network.bytes_in",
            new EsField("network.bytes_in", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.METRIC)
        );
        UnresolvedAttribute unresolvedSort = new UnresolvedAttribute(Source.EMPTY, "@timestamp");
        NamedExpression aggregate = new Alias(Source.EMPTY, "first", new First(Source.EMPTY, field, unresolvedSort));

        EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES).withAttributes(List.of(timestamp, field));
        TimeSeriesAggregate plan = new TimeSeriesAggregate(
            Source.EMPTY,
            relation,
            List.of(),
            List.of(aggregate),
            null,
            timestamp,
            TimeSeriesAggregate.Origin.TS_COMMAND
        );

        // Must not throw; the unresolved sort should be left untouched for the Verifier to report.
        TimeSeriesAggregate result = as(rule.rule(plan), TimeSeriesAggregate.class);
        assertThat(result, sameInstance(plan));
        Alias resultAlias = as(result.aggregates().getFirst(), Alias.class);
        First resultFirst = as(resultAlias.child(), First.class);
        assertThat(resultFirst.field(), sameInstance(field));
        assertThat(resultFirst.sort(), sameInstance(unresolvedSort));
    }

    private void assertStatsEqual(String stats1, String stats2) {
        var baseQuery = """
            TS k8s
            | STATS s=%s BY bucket(@timestamp, 1 minute)
            | SORT s
            | LIMIT 10
            """;
        var analyzer = metricsAnalyzer();
        var plan1 = analyzer.query(String.format(Locale.ROOT, baseQuery, stats1));
        var plan2 = analyzer.query(String.format(Locale.ROOT, baseQuery, stats2));
        assertThat(ignoreIds(normalize(plan1)), equalToIgnoringIds(normalize(plan2)));
    }

    /**
     * Normalizes a plan for comparison by:
     * <ol>
     *   <li>Renaming all aliases to {@code "dummy"} — internal first-pass alias names differ between the
     *       implicit form (e.g. {@code $DEFAULTTIMESERIESAGGREGATEFUNCTION_1}) and the explicit form
     *       (e.g. {@code $LASTOVERTIME_1}).</li>
     *   <li>Renaming all {@link ReferenceAttribute}s to {@code "dummy"} — the second-pass {@code Aggregate}
     *       and {@code SORT} reference the first-pass aliases by name, so those names differ too.</li>
     *   <li>Replacing {@link DefaultTimeSeriesAggregateFunction} with its delegate ({@link
     *       org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime}).</li>
     * </ol>
     */
    private static LogicalPlan normalize(LogicalPlan plan) {
        plan = plan.transformExpressionsDown(Alias.class, a -> new Alias(a.source(), "dummy", a.child(), a.id()));
        plan = plan.transformExpressionsDown(
            ReferenceAttribute.class,
            ra -> new ReferenceAttribute(ra.source(), ra.qualifier(), "dummy", ra.dataType(), ra.nullable(), ra.id(), ra.synthetic())
        );
        plan = plan.transformExpressionsDown(DefaultTimeSeriesAggregateFunction.class, DefaultTimeSeriesAggregateFunction::surrogate);
        return plan;
    }
}
