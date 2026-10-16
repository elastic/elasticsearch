/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.core.Strings;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PackDimsAgg;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
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

import static org.elasticsearch.test.TransportVersionUtils.getPreviousVersion;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.hasSize;

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
     * Casts retain metric identity for null filtering, independently of their output data types.
     */
    public void testFilterConvertedMetrics() {
        LogicalPlan actual = metrics().localPlan("""
            TS test
            | STATS max(max_over_time(metric_1)), max(max_over_time(metric_2::double))
            | LIMIT 10
            """);
        Limit limit = as(actual, Limit.class);
        Aggregate agg = as(limit.child(), Aggregate.class);
        // The optimizer expands the STATS out into two STATS steps
        Aggregate tsAgg = as(agg.child(), Aggregate.class);
        assertTrue(actual.toString(), tsAgg.anyMatch(Filter.class::isInstance));
    }

    /**
     * Casts preserve metric identity only when their input is a metric. Computed inputs must still disable null filtering.
     */
    public void testSkipFilterForCastsWithComputedInputs() {
        for (String computed : new String[] {
            "coalesce(metric_2, 0)::double",
            "coalesce(metric_2::double, 0)",
            "(metric_2 + 1)::double" }) {
            LogicalPlan actual = metrics().localPlan(Strings.format("""
                TS test
                | EVAL converted = metric_1::double::long, computed = %s
                | STATS max(max_over_time(converted)), min(min_over_time(computed)) BY BUCKET(@timestamp, 1 min)
                | LIMIT 10
                """, computed));
            assertFalse(actual.toString(), actual.anyMatch(Filter.class::isInstance));
        }
    }

    /**
     * Absence and presence still need null rows when a computed input is aggregated alongside a cast metric.
     */
    public void testSkipFilterForComputedPresenceWithCastMetric() {
        for (String function : new String[] { "absent_over_time", "present_over_time" }) {
            LogicalPlan actual = metrics().localPlan(Strings.format("""
                TS test
                | EVAL converted = metric_1::double, computed = (metric_2 + 0)::double
                | STATS max(max_over_time(converted)), max(%s(computed)) BY BUCKET(@timestamp, 1 min)
                | LIMIT 10
                """, function));
            assertFalse(actual.toString(), actual.anyMatch(Filter.class::isInstance));
        }
    }

    public void testFilterOnlyConvertedMetric() {
        LogicalPlan actual = metrics().localPlan("""
            TS test | STATS max(max_over_time(metric_1::double)) BY BUCKET(@timestamp, 1 min) | LIMIT 10
            """);
        assertTrue(actual.toString(), actual.anyMatch(Filter.class::isInstance));
    }

    public void testFilterChainedCastsAndAliases() {
        LogicalPlan actual = metrics().localPlan("""
            TS test
            | EVAL converted = metric_2::double, chained = converted::long
            | STATS max(max_over_time(metric_1)), max(max_over_time(chained))
            | LIMIT 10
            """);
        assertTrue(actual.toString(), actual.anyMatch(Filter.class::isInstance));
    }

    public void testFilterOverwrittenMetric() {
        LogicalPlan actual = metrics().localPlan("""
            TS test
            | EVAL metric_2 = metric_2::double
            | STATS max(max_over_time(metric_1)), max(max_over_time(metric_2))
            | LIMIT 10
            """);
        assertTrue(actual.toString(), actual.anyMatch(Filter.class::isInstance));
    }

    public void testFilterGaugeAndCounterCasts() {
        LogicalPlan actual = ts().localPlan("""
            TS k8s
            | STATS average = avg(avg_over_time(network.total_bytes_in::gauge)), rate = max(rate(network.total_bytes_out::gauge::counter))
            | LIMIT 10
            """);
        assertTrue(actual.toString(), actual.anyMatch(Filter.class::isInstance));
    }

    public void testCastStillStripsCounterSemantics() {
        LogicalPlan actual = ts().localPlan("""
            TS k8s
            | STATS average = avg(avg_over_time(network.total_bytes_in::long)) BY BUCKET(@timestamp, 1 min)
            | LIMIT 10
            """);
        assertEquals(DataType.DOUBLE, actual.output().getFirst().dataType());
        assertTrue(actual.toString(), actual.anyMatch(Filter.class::isInstance));
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
