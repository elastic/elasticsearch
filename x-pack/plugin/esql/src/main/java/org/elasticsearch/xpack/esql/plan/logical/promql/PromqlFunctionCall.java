/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.promql.function.FunctionType;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Represents a PromQL function call in the logical plan.
 *
 * This is a surrogate logical plan that encapsulates a PromQL function invocation
 * and delegates to the PromqlFunctionRegistry for validation and ESQL function construction.
 */
public abstract sealed class PromqlFunctionCall extends UnaryPlan implements PromqlPlan permits AcrossSeriesAggregate,
    ScalarConversionFunction, WithinSeriesAggregate, ValueTransformationFunction, VectorConversionFunction {
    // implements TelemetryAware {

    private final String functionName;
    private final List<Expression> parameters;

    public PromqlFunctionCall(Source source, LogicalPlan child, String functionName, List<Expression> parameters) {
        super(source, child);
        this.functionName = functionName;
        this.parameters = parameters != null ? parameters : List.of();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("PromqlFunctionCall does not support serialization");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("PromqlFunctionCall does not support serialization");
    }

    public String functionName() {
        return functionName;
    }

    public List<Expression> parameters() {
        return parameters;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(parameters);
    }

    // @Override
    // public String telemetryLabel() {
    // return "PROMQL_FUNCTION_CALL";
    // }

    @Override
    public int hashCode() {
        return Objects.hash(child(), functionName, parameters);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        PromqlFunctionCall other = (PromqlFunctionCall) obj;
        return Objects.equals(child(), other.child())
            && Objects.equals(functionName, other.functionName)
            && Objects.equals(parameters, other.parameters);
    }

    @Override
    public List<Attribute> output() {
        return List.of();
    }

    public abstract FunctionType functionType();

    @Override
    public final PromqlDataType returnType() {
        return functionType().outputType();
    }
}
