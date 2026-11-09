/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

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
import org.elasticsearch.xpack.esql.plan.logical.UnpackDims;

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

        // For reasons beyond my comprehension, the ordering of the conditionals inside the OR is nondeterministic.
        IsNotNull condition;
        FieldAttribute attribute;

        condition = as(or.left(), IsNotNull.class);
        attribute = as(condition.field(), FieldAttribute.class);
        if (attribute.fieldName().string().equals("metric_1")) {
            condition = as(or.right(), IsNotNull.class);
            attribute = as(condition.field(), FieldAttribute.class);
            assertEquals("metric_2", attribute.fieldName().string());
        } else if (attribute.fieldName().string().equals("metric_2")) {
            condition = as(or.right(), IsNotNull.class);
            attribute = as(condition.field(), FieldAttribute.class);
            assertEquals("metric_1", attribute.fieldName().string());
        } else {
            // something weird happened
            assert false;
        }
    }

    /**
     * The reference attribute metric_2 doesn't inherit the metric property from the original field. Filtering on metric_1 alone would
     * drop the rows that only have a value for metric_2, so no filter is added.
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
     * A converted metric doesn't carry the metric property. Filtering on metric_1 alone would drop the rows that only have a value for
     * metric_2, so no filter is added.
     */
    public void testSkipFilterForConvertedMetrics() {
        LogicalPlan actual = metrics().localPlan("""
            TS test
            | STATS max(max_over_time(metric_1)), max(max_over_time(metric_2::double))
            | LIMIT 10
            """);
        Limit limit = as(actual, Limit.class);
        Aggregate agg = as(limit.child(), Aggregate.class);
        // The optimizer expands the STATS out into two STATS steps
        Aggregate tsAgg = as(agg.child(), Aggregate.class);
        Eval eval = as(tsAgg.child(), Eval.class);
        as(eval.child(), EsRelation.class);
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
