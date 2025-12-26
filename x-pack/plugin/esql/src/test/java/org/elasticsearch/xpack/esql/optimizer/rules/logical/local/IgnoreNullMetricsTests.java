/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.expression.function.scalar.internal.PackDimension;
import org.elasticsearch.xpack.esql.expression.function.scalar.internal.UnpackDimension;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.optimizer.AbstractLocalLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for the {@link IgnoreNullMetrics} transformation rule.  Like most rule tests, this runs the entire analysis chain.
 */
public class IgnoreNullMetricsTests extends AbstractLocalLogicalPlanOptimizerTests {

    public void testSimple() {
        LogicalPlan actual = localPlan("""
            TS test
            | STATS max(max_over_time(metric_1)) BY BUCKET(@timestamp, 1 min)
            | LIMIT 10
            """, metricsAnalyzer);
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
        LogicalPlan actual = localPlan("""
            FROM test
            | STATS max(metric_1) BY dimension_1
            | LIMIT 10
            """, metricsAnalyzer);
        Limit limit = as(actual, Limit.class);
        Aggregate agg = as(limit.child(), Aggregate.class);
        EsRelation relation = as(agg.child(), EsRelation.class);
    }

    public void testDimensionsAreNotFiltered() {
        LogicalPlan actual = localPlan("""
            TS test
            | STATS max(max_over_time(metric_1)) BY dimension_1
            | LIMIT 10
            """, metricsAnalyzer);
        Project project = as(actual, Project.class);
        Eval unpack = as(project.child(), Eval.class);
        assertThat(unpack.fields(), hasSize(1));
        assertThat(Alias.unwrap(unpack.fields().get(0)), instanceOf(UnpackDimension.class));
        Limit limit = as(unpack.child(), Limit.class);
        Aggregate agg = as(limit.child(), Aggregate.class);
        Eval pack = as(agg.child(), Eval.class);
        assertThat(pack.fields(), hasSize(1));
        assertThat(Alias.unwrap(pack.fields().get(0)), instanceOf(PackDimension.class));
        // The optimizer expands the STATS out into two STATS steps
        Aggregate tsAgg = as(pack.child(), Aggregate.class);
        Filter filter = as(tsAgg.child(), Filter.class);
        IsNotNull condition = as(filter.condition(), IsNotNull.class);
        FieldAttribute attribute = as(condition.field(), FieldAttribute.class);
        assertEquals("metric_1", attribute.fieldName().string());
    }

    public void testFiltersAreJoinedWithOr() {
        LogicalPlan actual = localPlan("""
            TS test
            | STATS max(max_over_time(metric_1)), min(min_over_time(metric_2))
            | LIMIT 10
            """, metricsAnalyzer);
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

    public void testSkipCoalescedMetrics() {
        // Note: this test is passing because the reference attribute metric_2 in the stats block does not inherit the
        // metric property from the original field.
        LogicalPlan actual = localPlan("""
            TS test
            | EVAL metric_2 = coalesce(metric_2, 0)
            | STATS max(max_over_time(metric_1)), min(min_over_time(metric_2))
            | LIMIT 10
            """, metricsAnalyzer);
        Limit limit = as(actual, Limit.class);
        Aggregate agg = as(limit.child(), Aggregate.class);
        // The optimizer expands the STATS out into two STATS steps
        Aggregate tsAgg = as(agg.child(), Aggregate.class);
        Eval eval = as(tsAgg.child(), Eval.class);
        Filter filter = as(eval.child(), Filter.class);
        IsNotNull condition = as(filter.condition(), IsNotNull.class);
        FieldAttribute attribute = as(condition.field(), FieldAttribute.class);
        assertEquals("metric_1", attribute.fieldName().string());
    }

    /**
     * check that stats blocks after the first are not sourced for adding metrics to the filter
     */
    public void testMultipleStats() {
        LogicalPlan actual = localPlan("""
            TS test
            | STATS m = max(max_over_time(metric_1))
            | STATS sum(m)
            | LIMIT 10
            """, metricsAnalyzer);
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
