/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.planner.Mappable;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class Abs extends UnaryScalarFunction implements Mappable {
    public Abs(Source source, Expression field) {
        super(source, field);
    }

    @Override
    public Object fold() {
        return Mappable.super.fold();
    }

    @Evaluator(extraName = "Double")
    static double process(double fieldVal) {
        return Math.abs(fieldVal);
    }

    @Evaluator(extraName = "Long")
    static long process(long fieldVal) {
        return Math.absExact(fieldVal);
    }

    @Evaluator(extraName = "Int")
    static int process(int fieldVal) {
        return Math.absExact(fieldVal);
    }

    @Override
    public Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        Supplier<EvalOperator.ExpressionEvaluator> field = toEvaluator.apply(field());
        if (dataType() == DataTypes.DOUBLE) {
            return () -> new AbsDoubleEvaluator(field.get());
        }
        if (dataType() == DataTypes.LONG) {
            return () -> new AbsLongEvaluator(field.get());
        }
        if (dataType() == DataTypes.INTEGER) {
            return () -> new AbsIntEvaluator(field.get());
        }
        throw new UnsupportedOperationException("unsupported data type [" + dataType() + "]");
    }

    @Override
    public final Expression replaceChildren(List<Expression> newChildren) {
        return new Abs(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Abs::new, field());
    }
}
