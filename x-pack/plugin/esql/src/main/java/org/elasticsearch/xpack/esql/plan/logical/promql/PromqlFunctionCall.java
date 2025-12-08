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
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
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
public class PromqlFunctionCall extends UnaryPlan {
    // implements TelemetryAware {

    private final String functionName;
    private final List<Expression> parameters;
    private List<Attribute> output;

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

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(this, PromqlFunctionCall::new, child(), functionName, parameters);
    }

    @Override
    public PromqlFunctionCall replaceChild(LogicalPlan newChild) {
        return new PromqlFunctionCall(source(), newChild, functionName, parameters);
    }

    public String functionName() {
        return functionName;
    }

    public List<Expression> parameters() {
        return parameters;
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            output = List.of(
                new ReferenceAttribute(source(), "promql$labels", DataType.KEYWORD),
                new ReferenceAttribute(source(), "promql$timestamp", DataType.DATETIME),
                new ReferenceAttribute(source(), "promql$value", DataType.DOUBLE)
            );
        }
        return output;
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
}
