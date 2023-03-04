/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class IsInfinite extends RationalUnaryPredicate {
    public IsInfinite(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected boolean fold(Object val) {
        return Double.isInfinite((Double) val);
    }

    @Override
    public Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        Supplier<EvalOperator.ExpressionEvaluator> field = toEvaluator.apply(field());
        return () -> new IsInfiniteEvaluator(field.get());
    }

    private record IsInfiniteEvaluator(EvalOperator.ExpressionEvaluator field) implements EvalOperator.ExpressionEvaluator {
        @Override
        public Object computeRow(Page page, int pos) {
            Object v = field.computeRow(page, pos);
            if (v == null) {
                return null;
            }
            return Double.isInfinite((Double) v);
        }
    }

    @Override
    public final Expression replaceChildren(List<Expression> newChildren) {
        return new IsInfinite(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, IsInfinite::new, field());
    }
}
