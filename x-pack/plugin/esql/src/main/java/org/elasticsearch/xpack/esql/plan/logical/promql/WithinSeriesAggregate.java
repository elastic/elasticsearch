/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SurrogateLogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.ArrayList;
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
public class WithinSeriesAggregate extends PromqlFunctionCall implements SurrogateLogicalPlan {

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

    @Override
    public LogicalPlan surrogate() {
        LogicalPlan childPlan = child();

        ReferenceAttribute timestampField = new ReferenceAttribute(source(), "@timestamp", DataType.DATETIME);
        ReferenceAttribute valueField = new ReferenceAttribute(source(), "value", DataType.DOUBLE);
        ReferenceAttribute tsidField = new ReferenceAttribute(source(), "_tsid", DataType.KEYWORD);

        List<Expression> functionParams = new ArrayList<>();
        functionParams.add(valueField);
        functionParams.add(timestampField);
        functionParams.addAll(parameters());

        Function esqlFunction = PromqlFunctionRegistry.INSTANCE.buildEsqlFunction(functionName(), source(), functionParams);

        String internalName = functionName() + "_$result";
        Alias functionAlias = new Alias(source(), internalName, esqlFunction);

        List<Expression> groupings = new ArrayList<>();
        groupings.add(tsidField);

        List<NamedExpression> aggregates = new ArrayList<>();
        aggregates.add(functionAlias);
        aggregates.add(tsidField);

        return new TimeSeriesAggregate(source(), childPlan, groupings, aggregates, null);
    }

    // @Override
    // public String telemetryLabel() {
    // return "PROMQL_WITHIN_SERIES_AGGREGATION";
    // }
}
