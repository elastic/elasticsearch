/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

/**
 * Tangent trigonometric function.
 */
public class Tan extends AbstractTrigonometricFunction {
    @FunctionInfo(returnType = "double")
    public Tan(Source source, @Param(name = "n", type = { "integer", "long", "double", "unsigned_long" }) Expression n) {
        super(source, n);
    }

    @Override
    protected EvalOperator.ExpressionEvaluator.Factory doubleEvaluator(EvalOperator.ExpressionEvaluator.Factory field) {
        return new TanEvaluator.Factory(field);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Tan(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Tan::new, field());
    }

    @Evaluator
    static double process(double val) {
        return Math.tan(val);
    }
}
