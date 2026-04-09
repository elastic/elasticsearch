/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.time.Duration;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class PromqlPlanFunctionCallTests extends AbstractPromqlPlanOptimizerTests {

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
}
