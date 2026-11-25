/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TsInfo;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

/**
 * TS_INFO is more expensive than METRICS_INFO so translate queries with TS_INFO to METRICS_INFO when possible. For example:
 * {@code
 * TS metrics-prometheusreceiver.otel-default
 * | TS_INFO
 * | WHERE metric_name LIKE "metrics.litellm_*"
 * | STATS dims = VALUES (dimension_fields) BY metric_name, metric_type
 * }
 * to
 * {@code
 * TS metrics-prometheusreceiver.otel-default
 * | METRICS_INFO
 * | WHERE metric_name LIKE "metrics.litellm_*"
 * | STATS dims = VALUES (dimension_fields) BY metric_name, metric_type
 * }
 * as both should yield the same results
 */
public final class ReplaceTsInfoWithMetricsInfo extends OptimizerRules.OptimizerRule<Aggregate> {

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        if (aggregate.aggregates().stream().anyMatch(ReplaceTsInfoWithMetricsInfo::needsTimeSeriesRows)) {
            return aggregate;
        }
        LogicalPlan newChild = replaceTsInfo(aggregate.child());
        if (newChild == null || newChild.outputSet().containsAll(aggregate.references()) == false) {
            return aggregate;
        }
        return aggregate.replaceChild(newChild);
    }

    private static LogicalPlan replaceTsInfo(LogicalPlan plan) {
        if (plan instanceof TsInfo tsInfo) {
            return MetricsInfo.fromTsInfo(tsInfo);
        }
        if (plan instanceof Filter == false && plan instanceof Project == false) {
            return null;
        }
        UnaryPlan unary = (UnaryPlan) plan;
        LogicalPlan newChild = replaceTsInfo(unary.child());
        if (newChild == null || newChild.outputSet().containsAll(unary.references()) == false) {
            return null;
        }
        return unary.replaceChild(newChild);
    }

    /** True for aggregates whose result changes with the number of time_series rows */
    private static boolean needsTimeSeriesRows(Expression expression) {
        return expression.anyMatch(
            e -> e instanceof AggregateFunction f
                && (f instanceof Values || f instanceof Min || f instanceof Max || f instanceof CountDistinct) == false
        );
    }
}
