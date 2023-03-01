/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
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
        Object fieldVal = field().fold();
        if (fieldVal == null) {
            return null;
        }
        if (dataType() == DataTypes.DOUBLE) {
            return transform((Double) fieldVal);
        }
        if (dataType() == DataTypes.LONG) {
            return transform((Long) fieldVal);
        }
        if (dataType() == DataTypes.INTEGER) {
            return transform((Integer) fieldVal);
        }
        throw new UnsupportedOperationException("unsupported data type [" + dataType() + "]");
    }

    static double transform(double fieldVal) {
        return Math.abs(fieldVal);
    }

    static long transform(long fieldVal) {
        return Math.absExact(fieldVal);
    }

    static int transform(int fieldVal) {
        return Math.absExact(fieldVal);
    }

    @Override
    public Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        Supplier<EvalOperator.ExpressionEvaluator> field = toEvaluator.apply(field());
        if (dataType() == DataTypes.DOUBLE) {
            return () -> new DoubleEvaluator(field.get());
        }
        if (dataType() == DataTypes.LONG) {
            return () -> new LongEvaluator(field.get());
        }
        if (dataType() == DataTypes.INTEGER) {
            return () -> new IntEvaluator(field.get());
        }
        throw new UnsupportedOperationException("unsupported data type [" + dataType() + "]");
    }

    private record DoubleEvaluator(EvalOperator.ExpressionEvaluator field) implements EvalOperator.ExpressionEvaluator {
        @Override
        public Object computeRow(Page page, int pos) {
            Object v = field.computeRow(page, pos);
            if (v == null) {
                return null;
            }
            return transform((Double) v);
        }
    }

    private record LongEvaluator(EvalOperator.ExpressionEvaluator field) implements EvalOperator.ExpressionEvaluator {
        @Override
        public Object computeRow(Page page, int pos) {
            Object v = field.computeRow(page, pos);
            if (v == null) {
                return null;
            }
            return transform((Long) v);
        }
    }

    private record IntEvaluator(EvalOperator.ExpressionEvaluator field) implements EvalOperator.ExpressionEvaluator {
        @Override
        public Object computeRow(Page page, int pos) {
            Object v = field.computeRow(page, pos);
            if (v == null) {
                return null;
            }
            return transform((Integer) v);
        }
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
