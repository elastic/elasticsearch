/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

/**
 * Represents a PromQL aggregate function call that operates on range vectors.
 *
 * This is a surrogate logical plan for PromQL range vector functions that translate
 * to ESQL TimeSeriesAggregate operations. It extends PromqlFunctionCall and implements
 * the surrogate pattern to transform PromQL aggregations into ESQL time-series aggregates.
 *
 * Range vector functions supported:
 * - Counter functions: rate(), irate(), increase(), delta(), idelta()
 * - Aggregation functions: avg_over_time(), sum_over_time(), min_over_time(), max_over_time(), count_over_time()
 * - Selection functions: first_over_time(), last_over_time()
 * - Presence functions: present_over_time(), absent_over_time()
 * - Cardinality functions: count_distinct_over_time()
 *
 * During planning, surrogate() transforms this node into:
 *   TimeSeriesAggregate(
 *     child: RangeSelector (or other time-series source),
 *     groupings: [_tsid],
 *     aggregates: [function_result, _tsid]
 *   )
 *
 * Example transformations:
 *   rate(http_requests[5m])
 *     → TimeSeriesAggregate(groupBy: _tsid, agg: Rate(value, @timestamp))
 *
 *   avg_over_time(cpu_usage[1h])
 *     → TimeSeriesAggregate(groupBy: _tsid, agg: AvgOverTime(value))
 */
public class WithinSeriesAggregate extends PromqlFunctionCall {

    public WithinSeriesAggregate(Source source, LogicalPlan child, String functionName, List<Expression> parameters) {
        super(source, child, functionName, parameters);
    }

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(this, WithinSeriesAggregate::new, child(), functionName(), parameters());
    }

    @Override
    public WithinSeriesAggregate replaceChild(LogicalPlan newChild) {
        return new WithinSeriesAggregate(source(), newChild, functionName(), parameters());
    }

    // @Override
    // public String telemetryLabel() {
    // return "PROMQL_WITHIN_SERIES_AGGREGATION";
    // }
}
