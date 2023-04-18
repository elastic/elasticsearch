/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanArrayVector;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.planner.Mappable;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class IsNull extends UnaryScalarFunction implements Mappable {
    public IsNull(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new Expression.TypeResolution("Unresolved children");
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public Object fold() {
        return field().fold() == null;
    }

    @Override
    public Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        Supplier<EvalOperator.ExpressionEvaluator> field = toEvaluator.apply(field());
        return () -> new IsNullEvaluator(field.get());
    }

    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new IsNull(source(), newChildren.get(0));
    }

    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, IsNull::new, field());
    }

    private record IsNullEvaluator(EvalOperator.ExpressionEvaluator field) implements EvalOperator.ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            Block fieldBlock = field.eval(page);
            if (fieldBlock.asVector() != null) {
                return BooleanBlock.newConstantBlockWith(false, page.getPositionCount());
            }
            boolean[] result = new boolean[page.getPositionCount()];
            for (int p = 0; p < page.getPositionCount(); p++) {
                result[p] = fieldBlock.isNull(p);
            }
            return new BooleanArrayVector(result, result.length).asBlock();
        }
    }
}
