/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.UnaryExpression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.function.Function;

/**
 * Expression that makes a deep copy of the block it receives.
 */
public class DeepCopy extends UnaryExpression implements EvaluatorMapper {
    public DeepCopy(Source source, Expression child) {
        super(source, child);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        EvalOperator.ExpressionEvaluator.Factory childEval = toEvaluator.apply(child());
        return ctx -> new EvalOperator.ExpressionEvaluator() {
            private final EvalOperator.ExpressionEvaluator child = childEval.get(ctx);

            @Override
            public Block eval(Page page) {
                try (Block block = child.eval(page)) {
                    return BlockUtils.deepCopyOf(block, ctx.blockFactory());
                }
            }

            @Override
            public void close() {
                Releasables.closeExpectNoException(child);
            }
        };
    }

    @Override
    protected UnaryExpression replaceChild(Expression newChild) {
        return new DeepCopy(source(), newChild);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, DeepCopy::new, child());
    }
}
