/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

/**
 * Represents a PromQL aggregate function call that operates on range vectors.
 * <p>
 * These functions take a range vector as input and aggregate the values within each series
 * over the specified time range, returning an instant vector.
 * This corresponds to PromQL syntax:
 * <pre>
 * function_name(range_vector)
 * </pre>
 *
 * Examples:
 * <pre>
 * rate(http_requests_total[5m])
 * increase(errors_total[1h])
 * delta(cpu_temp_celsius[30m])
 * </pre>
 *
 * These functions operate independently on each time series selected by the range vector.
 * The result contains one sample per series at the evaluation timestamp.
 */
public final class WithinSeriesAggregate extends PromqlFunctionCall {

    private List<Attribute> output;

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
    public List<Attribute> output() {
        if (output == null) {
            // returns values grouped per time series
            output = List.of(FieldAttribute.timeSeriesAttribute(source()));
        }
        return output;
    }

    @Override
    public FunctionType functionType() {
        return FunctionType.WITHIN_SERIES_AGGREGATION;
    }
}
