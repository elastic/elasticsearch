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
 * Represents the {@code vector(s scalar)} PromQL function call that creates a vector from a scalar.
 * <p>
 * The result is a vector with a single element, with the given scalar value and no labels.
 */
public final class VectorConversionFunction extends PromqlFunctionCall {

    public VectorConversionFunction(Source source, LogicalPlan child, String functionName, List<Expression> parameters) {
        super(source, child, functionName, parameters);
    }

    @Override
    public FunctionType functionType() {
        return FunctionType.VECTOR_CONVERSION;
    }

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(this, VectorConversionFunction::new, child(), functionName(), parameters());
    }

    @Override
    public VectorConversionFunction replaceChild(LogicalPlan newChild) {
        return new VectorConversionFunction(source(), newChild, functionName(), parameters());
    }

    @Override
    public List<Attribute> output() {
        return child().output();
    }
}
