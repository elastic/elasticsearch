/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.function.Named;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

/**
 * Tangent trigonometric function.
 */
public class Tan extends AbstractTrigonometricFunction {
    public Tan(Source source, @Named("n") Expression n) {
        super(source, n);
    }

    @Override
    protected EvalOperator.ExpressionEvaluator doubleEvaluator(EvalOperator.ExpressionEvaluator field, DriverContext dvrCtx) {
        return new TanEvaluator(field, dvrCtx);
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
