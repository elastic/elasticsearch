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
 * Represents the {@code scalar(v instant-vector)} PromQL function call that converts a vector to a scalar.
 */
public final class ScalarConversionFunction extends PromqlFunctionCall {

    public ScalarConversionFunction(Source source, LogicalPlan child, String functionName, List<Expression> parameters) {
        super(source, child, functionName, parameters);
    }

    @Override
    public FunctionType functionType() {
        return FunctionType.SCALAR_CONVERSION;
    }

    @Override
    protected NodeInfo<PromqlFunctionCall> info() {
        return NodeInfo.create(this, ScalarConversionFunction::new, child(), functionName(), parameters());
    }

    @Override
    public ScalarConversionFunction replaceChild(LogicalPlan newChild) {
        return new ScalarConversionFunction(source(), newChild, functionName(), parameters());
    }

    @Override
    public List<Attribute> output() {
        return List.of();
    }
}
