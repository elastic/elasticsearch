/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PackDimsAgg;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.AbstractLocalLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.TransportVersionUtils.getPreviousVersion;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for the {@link IgnoreNullMetrics} transformation rule.  Like most rule tests, this runs the entire analysis chain.
 */
public class IgnoreNullMetricsTests extends AbstractLocalLogicalPlanOptimizerTests {

    public void testSimple() {
        LogicalPlan actual = metrics().localPlan("""
            TS test
            | STATS max(max_over_time(metric_1)) BY BUCKET(@timestamp, 1 min)
            | LIMIT 10
            """);
        Limit limit = as(actual, Limit.class);
        Aggregate agg = as(limit.child(), Aggregate.class);
        // The optimizer expands the STATS out into two STATS steps
        Aggregate tsAgg = as(agg.child(), Aggregate.class);
        Eval bucketEval = as(tsAgg.child(), Eval.class);
        Filter filter = as(bucketEval.child(), Filter.class);
        IsNotNull condition = as(filter.condition(), IsNotNull.class);
        FieldAttribute attribute = as(condition.field(), FieldAttribute.class);
        assertEquals("metric_1", attribute.fieldName().string());
    }

    public void testRuleDoesNotApplyInNonTSMode() {
        // NOTE: it is necessary to have the `BY dimension 1` grouping here, otherwise the InfernonNullAggConstraint rule
        // will add the same filter IgnoreNullMetrics would have.
        LogicalPlan actual = metrics().localPlan("""
            FROM test
            | STATS max(metric_1) BY dimension_1
            | LIMIT 10
            """);
        Limit limit = as(actual, Limit.class);
        Aggregate agg = as(limit.child(), Aggregate.class);
        EsRelation relation = as(agg.child(), EsRelation.class);
    }

    public void testDimensionsAreNotFiltered() {
        // Dimension packing has two representations: a standalone PackDims/UnpackDims pair (older versions) or a PACKDIMSAGG aggregate
        // folded into the time-series aggregate (pack_dims_agg and later). The analyzer picks a random compatible version by default, so
        // pin to the version just below pack_dims_agg to assert the standalone PackDims/UnpackDims shape deterministically.
        LogicalPlan actual = metrics().minimumTransportVersion(getPreviousVersion(PackDimsAgg.PACK_DIMS_AGG_VERSION)).localPlan("""
            TS test
            | STATS max(max_over_time(metric_1)) BY dimension_1
            | LIMIT 10
            """);
        Project project = as(actual, Project.class);
        Limit limit = as(project.child(), Limit.class);
        UnpackDims unpack = as(limit.child(), UnpackDims.class);
        assertThat(unpack.dims(), hasSize(1));
        Aggregate agg = as(unpack.child(), Aggregate.class);
        PackDims pack = as(agg.child(), PackDims.class);
        assertThat(pack.dims(), hasSize(1));
        // The optimizer expands the STATS out into two STATS steps
        Aggregate tsAgg = as(pack.child(), Aggregate.class);
        Filter filter = as(tsAgg.child(), Filter.class);
        IsNotNull condition = as(filter.condition(), IsNotNull.class);
        FieldAttribute attribute = as(condition.field(), FieldAttribute.class);
        assertEquals("metric_1", attribute.fieldName().string());
    }

    public void testFiltersAreJoinedWithOr() {
        LogicalPlan actual = metrics().localPlan("""
            TS test
            | STATS max(max_over_time(metric_1)), min(min_over_time(metric_2))
            | LIMIT 10
            """);
        Limit limit = as(actual, Limit.class);
        Aggregate agg = as(limit.child(), Aggregate.class);
        // The optimizer expands the STATS out into two STATS steps
        Aggregate tsAgg = as(agg.child(), Aggregate.class);
        Filter filter = as(tsAgg.child(), Filter.class);
        Or or = as(filter.expressions().getFirst(), Or.class);

        // Null checks follow the order of metrics in the aggregate expressions.
        IsNotNull left = as(or.left(), IsNotNull.class);
        assertEquals("metric_1", as(left.field(), FieldAttribute.class).fieldName().string());
        IsNotNull right = as(or.right(), IsNotNull.class);
        assertEquals("metric_2", as(right.field(), FieldAttribute.class).fieldName().string());
    }

    /**
     * {@code COALESCE} can be non-null when its inputs are null, so its result can't be traced back to the metric it reads. Filtering
     * on metric_1 alone would drop the rows that only have a value for the computed input, so no filter is added.
     */
    public void testSkipFilterForComputedInputs() {
        LogicalPlan actual = metrics().localPlan("""
            TS test
            | EVAL metric_2 = coalesce(metric_2, 0)
            | STATS max(max_over_time(metric_1)), min(min_over_time(metric_2))
            | LIMIT 10
            """);
        Limit limit = as(actual, Limit.class);
        Aggregate agg = as(limit.child(), Aggregate.class);
        // The optimizer expands the STATS out into two STATS steps
        Aggregate tsAgg = as(agg.child(), Aggregate.class);
        Eval eval = as(tsAgg.child(), Eval.class);
        as(eval.child(), EsRelation.class);
    }

    /**
     * Inputs computed from metrics through null-propagating expressions are filtered on the metrics they read.
     */
    public void testFilterNullPropagatingExpressions() {
        for (String computed : new String[] {
            "metric_2::double",
            "(metric_2 + 1)::double",
            "(metric_2 + 1)::gauge",
            "(metric_2 + 1)::counter::long",
            "metric_2::string::double",
            "to_degrees(metric_2::double)",
            "metric_2 * 8 / 1000" }) {
            LogicalPlan actual = new IgnoreNullMetrics().apply(metrics().coordinatorPlan(Strings.format("""
                TS test
                | EVAL converted = metric_1::double::long, computed = %s
                | STATS max(max_over_time(converted)), min(min_over_time(computed)) BY BUCKET(@timestamp, 1 min)
                | LIMIT 10
                """, computed)));
            assertEquals(computed + "\n" + actual, List.of("metric_1", "metric_2"), filteredFields(actual));
        }
    }

    /**
     * An input reading several metrics is null as soon as one of them is null. Each metric it reads is filtered on once, in the order
     * the input reads them, so every row with a value for the input is kept.
     */
    public void testFilterExpressionsOverMultipleMetrics() {
        Map<String, List<String>> evals = Map.of(
            "total = metric_1 + metric_2",
            List.of("metric_1", "metric_2"),
            "x = metric_1::double, total = x + metric_2 * 2",
            List.of("metric_1", "metric_2"),
            "total = metric_2 + metric_1::double",
            List.of("metric_2", "metric_1"),
            "total = metric_1 + metric_1::double",
            List.of("metric_1")
        );
        for (var eval : evals.entrySet()) {
            LogicalPlan actual = new IgnoreNullMetrics().apply(metrics().coordinatorPlan(Strings.format("""
                TS test
                | EVAL %s
                | STATS max(max_over_time(total)) BY BUCKET(@timestamp, 1 min)
                | LIMIT 10
                """, eval.getKey())));
            assertEquals(eval.getKey() + "\n" + actual, eval.getValue(), filteredFields(actual));
        }
    }

    public void testSkipFilterForNonNullPropagatingExpressions() {
        for (String computed : new String[] {
            "coalesce(metric_2, 0)::double",
            "coalesce(metric_2::double, 0)",
            "(coalesce(metric_2, 0) + 1)::gauge",
            "case(metric_2 > 0, metric_2, 0)" }) {
            LogicalPlan actual = metrics().localPlan(Strings.format("""
                TS test
                | EVAL converted = metric_1::double::long, computed = %s
                | STATS max(max_over_time(converted)), min(min_over_time(computed)) BY BUCKET(@timestamp, 1 min)
                | LIMIT 10
                """, computed));
            assertFalse(computed + "\n" + actual, actual.anyMatch(Filter.class::isInstance));
        }
    }

    /**
     * Fields that are neither metrics, dimensions nor the timestamp have never been null-filtered. Inputs reading them disable the filter.
     */
    public void testSkipFilterForExpressionsReadingNonMetricFields() {
        EsIndex index = new EsIndex(
            "test",
            Map.of(
                "@timestamp",
                new EsField("@timestamp", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
                "metric",
                new EsField("metric", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.METRIC),
                "plain",
                new EsField("plain", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
            ),
            Map.of("test", new IndexProperties(IndexMode.TIME_SERIES, 0)),
            Map.of(),
            Map.of()
        );
        for (String computed : new String[] { "metric + plain", "plain::double" }) {
            LogicalPlan actual = EsqlTestUtils.optimizer().addIndex(index).localPlan(Strings.format("""
                TS test
                | EVAL computed = %s
                | STATS max(max_over_time(metric)), max(max_over_time(computed)) BY BUCKET(@timestamp, 1 min)
                | LIMIT 10
                """, computed));
            assertFalse(computed + "\n" + actual, actual.anyMatch(Filter.class::isInstance));
        }
    }

    /**
     * Every aggregation, apart from the ones carrying the grouping keys, must read at least one metric and nothing but metrics.
     * Otherwise the filter would drop rows that aggregation counts, like the ones without metrics for a count over a dimension or a
     * constant.
     */
    public void testSkipFilterForAggregationsNotReadingOnlyMetrics() {
        for (String eval : new String[] { "d = length(dimension_1)", "d = 1", "d = dimension_1" }) {
            LogicalPlan actual = metrics().localPlan(Strings.format("""
                TS test
                | EVAL %s
                | STATS max(max_over_time(metric_1)), n = max(count_over_time(d)) BY dimension_1
                | LIMIT 10
                """, eval));
            assertFalse(eval + "\n" + actual, actual.anyMatch(Filter.class::isInstance));
        }
    }

    public void testFilterAliasedAndOverwrittenMetrics() {
        for (String eval : new String[] {
            "converted = metric_1::double",
            "intermediate = metric_1::double, converted = intermediate::long",
            "metric_1 = metric_1::double, converted = metric_1" }) {
            LogicalPlan actual = new IgnoreNullMetrics().apply(metrics().coordinatorPlan(Strings.format("""
                TS test
                | EVAL %s
                | STATS max(max_over_time(converted)) BY BUCKET(@timestamp, 1 min)
                | LIMIT 10
                """, eval)));
            assertEquals(eval + "\n" + actual, List.of("metric_1"), filteredFields(actual));
        }
    }

    /**
     * A {@code WHERE} between the {@code EVAL} and the {@code STATS} doesn't hide the metric an input is computed from.
     */
    public void testFilterThroughWhere() {
        LogicalPlan actual = new IgnoreNullMetrics().apply(metrics().coordinatorPlan("""
            TS test
            | EVAL converted = metric_1::double
            | WHERE dimension_1 == "a"
            | STATS max(max_over_time(converted)) BY BUCKET(@timestamp, 1 min)
            | LIMIT 10
            """));
        assertEquals(List.of("metric_1"), filteredFields(actual));
    }

    public void testFilterAcrossUnionConversions() {
        for (EsField.TimeSeriesFieldType fieldType : List.of(EsField.TimeSeriesFieldType.METRIC, EsField.TimeSeriesFieldType.NONE)) {
            EsIndex index = unionIndex(DataType.LONG, DataType.DOUBLE, fieldType);
            for (String conversion : List.of(
                "value::double",
                "value::string::double",
                "value::double::gauge",
                "value::double::counter::double",
                "value::double * 2"
            )) {
                LogicalPlan actual = EsqlTestUtils.optimizer().addIndex(index).localPlan(Strings.format("""
                    TS test-1,test-2
                    | EVAL converted = %s
                    | STATS max(max_over_time(converted)) BY BUCKET(@timestamp, 1 min)
                    | LIMIT 10
                    """, conversion));
                assertEquals(
                    conversion + "\n" + actual,
                    fieldType == EsField.TimeSeriesFieldType.METRIC,
                    actual.anyMatch(Filter.class::isInstance)
                );
            }
        }
    }

    /**
     * The names of the fields in the null filter this rule added below the time-series aggregation, in order.
     */
    private static List<String> filteredFields(LogicalPlan plan) {
        TimeSeriesAggregate tsAgg = as(plan.collect(TimeSeriesAggregate.class).getFirst(), TimeSeriesAggregate.class);
        Filter filter = as(tsAgg.child(), Filter.class);
        List<String> fields = new ArrayList<>();
        filter.condition().forEachDown(IsNotNull.class, isNotNull -> fields.add(as(isNotNull.field(), FieldAttribute.class).name()));
        return fields;
    }

    private static EsIndex unionIndex(DataType first, DataType second, EsField.TimeSeriesFieldType fieldType) {
        return new EsIndex(
            "test-1,test-2",
            Map.of(
                "@timestamp",
                new EsField("@timestamp", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
                "value",
                new InvalidMappedField("value", Map.of(first.typeName(), Set.of("test-1"), second.typeName(), Set.of("test-2")), fieldType)
            ),
            Map.of("test-1", new IndexProperties(IndexMode.TIME_SERIES, 0), "test-2", new IndexProperties(IndexMode.TIME_SERIES, 0)),
            Map.of(),
            Map.of()
        );
    }

    public void testFilterMixedAggregateMetricDoubleAndDoubleMetric() {
        assumeTrue("requires implicit aggregate_metric_double casting", EsqlCapabilities.Cap.AGGREGATE_METRIC_DOUBLE_V0.isEnabled());
        Map<String, EsField> mapping = Map.of(
            "@timestamp",
            new EsField("@timestamp", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "pod",
            new EsField("pod", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION),
            "network.eth0.rx",
            new InvalidMappedField(
                "network.eth0.rx",
                Map.of("aggregate_metric_double", Set.of("k8s-downsampled"), "double", Set.of("k8s")),
                EsField.TimeSeriesFieldType.METRIC
            )
        );
        EsIndex esIndex = new EsIndex(
            "k8s,k8s-downsampled",
            mapping,
            Map.of("k8s", new IndexProperties(IndexMode.TIME_SERIES, 0), "k8s-downsampled", new IndexProperties(IndexMode.TIME_SERIES, 0)),
            Map.of(),
            Map.of()
        );
        LogicalPlan actual = EsqlTestUtils.optimizer().addIndex(esIndex).localPlan("""
            TS k8s,k8s-downsampled
            | STATS avg(avg_over_time(network.eth0.rx)) BY BUCKET(@timestamp, 1 min)
            | LIMIT 10
            """);
        Filter filter = as(actual.collect(Filter.class::isInstance).getFirst(), Filter.class);
        // avg_over_time is split into sum_over_time and count_over_time, each reading its own union-typed attribute over the field
        Or or = as(filter.condition(), Or.class);
        for (Expression condition : List.of(or.left(), or.right())) {
            FieldAttribute attribute = as(as(condition, IsNotNull.class).field(), FieldAttribute.class);
            assertThat(attribute.field(), instanceOf(UnionTypeEsField.class));
            assertTrue(attribute.isMetric());
            assertEquals("network.eth0.rx", attribute.fieldName().string());
        }
    }

    public void testFilterAggregateMetricDoubleMetric() {
        assumeTrue("requires implicit aggregate_metric_double casting", EsqlCapabilities.Cap.AGGREGATE_METRIC_DOUBLE_V0.isEnabled());
        LogicalPlan actual = ts().localPlan("""
            TS k8s-downsampled
            | STATS avg(avg_over_time(network.eth0.rx)) BY BUCKET(@timestamp, 1 min)
            | LIMIT 10
            """);
        Filter filter = as(actual.collect(Filter.class::isInstance).getFirst(), Filter.class);
        FieldAttribute attribute = as(as(filter.condition(), IsNotNull.class).field(), FieldAttribute.class);
        assertEquals(DataType.AGGREGATE_METRIC_DOUBLE, attribute.dataType());
        assertTrue(attribute.isMetric());
        assertEquals("network.eth0.rx", attribute.fieldName().string());
    }

    /**
     * check that stats blocks after the first are not sourced for adding metrics to the filter
     */
    public void testMultipleStats() {
        LogicalPlan actual = metrics().localPlan("""
            TS test
            | STATS m = max(max_over_time(metric_1))
            | STATS sum(m)
            | LIMIT 10
            """);
        Limit limit = as(actual, Limit.class);
        Aggregate sumAgg = as(limit.child(), Aggregate.class);
        Aggregate outerAgg = as(sumAgg.child(), Aggregate.class);
        Aggregate tsAgg = as(outerAgg.child(), Aggregate.class);
        Filter filter = as(tsAgg.child(), Filter.class);
        IsNotNull condition = as(filter.condition(), IsNotNull.class);
        FieldAttribute attribute = as(condition.field(), FieldAttribute.class);
        assertEquals("metric_1", attribute.fieldName().string());
    }
}
