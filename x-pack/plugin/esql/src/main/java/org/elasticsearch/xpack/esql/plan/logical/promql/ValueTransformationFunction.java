/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

/**
 * Represents a PromQL function call that performs element-wise transformations on an instant vector.
 * <p>
 * These functions transform each sample in the input instant vector independently, preserving the labels.
 * The result is an instant vector with the same cardinality as the input, containing the transformed values.
 * This corresponds to PromQL syntax:
 * <pre>
 * function_name(instant_vector)
 * </pre>
 *
 * Examples:
 * <pre>
 * abs(http_requests_total)
 * ceil(cpu_usage)
 * sqrt(variance)
 * </pre>
 *
 * These functions operate on the value of each time series without changing the set of series or their labels.
 */
public final class ValueTransformationFunction extends PromqlFunctionCall {
    public ValueTransformationFunction(Source source, LogicalPlan child, String functionName, List<Expression> parameters) {
        super(source, child, functionName, parameters);
    }

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(this, ValueTransformationFunction::new, child(), functionName(), parameters());
    }

    @Override
    public ValueTransformationFunction replaceChild(LogicalPlan newChild) {
        return new ValueTransformationFunction(source(), newChild, functionName(), parameters());
    }

    @Override
    public List<Attribute> output() {
        return child().output();
    }

    @Override
    public FunctionType functionType() {
        return FunctionType.VALUE_TRANSFORMATION;
    }
}
