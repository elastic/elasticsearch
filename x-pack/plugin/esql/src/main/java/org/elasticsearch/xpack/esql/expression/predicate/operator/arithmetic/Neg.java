/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

public class Neg extends UnaryScalarFunction implements EvaluatorMapper {

    public Neg(Source source, Expression field) {
        super(source, field);
    }

    @Override
    public Supplier<ExpressionEvaluator> toEvaluator(Function<Expression, Supplier<ExpressionEvaluator>> toEvaluator) {
        DataType type = dataType();

        if (type.isNumeric()) {
            var f = toEvaluator.apply(field());
            Supplier<ExpressionEvaluator> supplier = null;

            if (type == DataTypes.INTEGER) {
                supplier = () -> new NegIntsEvaluator(source(), f.get());
            }
            // Unsigned longs are unsupported by choice; negating them would require implicitly converting to long.
            else if (type == DataTypes.LONG) {
                supplier = () -> new NegLongsEvaluator(source(), f.get());
            } else if (type == DataTypes.DOUBLE) {
                supplier = () -> new NegDoublesEvaluator(f.get());
            }

            if (supplier != null) {
                return supplier;
            }
        }
        throw new EsqlIllegalArgumentException("arithmetic negation operator with unsupported data type [" + type + "]");
    }

    @Override
    public final Object fold() {
        return EvaluatorMapper.super.fold();
    }

    @Override
    protected TypeResolution resolveType() {
        return isNumeric(field(), sourceText(), DEFAULT);
    }

    @Override
    protected NodeInfo<Neg> info() {
        return NodeInfo.create(this, Neg::new, field());
    }

    @Override
    public Neg replaceChildren(List<Expression> newChildren) {
        return new Neg(source(), newChildren.get(0));
    }

    @Evaluator(extraName = "Ints", warnExceptions = { ArithmeticException.class })
    static int processInts(int v) {
        return Math.negateExact(v);
    }

    @Evaluator(extraName = "Longs", warnExceptions = { ArithmeticException.class })
    static long processLongs(long v) {
        return Math.negateExact(v);
    }

    @Evaluator(extraName = "Doubles")
    static double processDoubles(double v) {
        // This can never fail (including when `v` is +/- infinity or NaN) since negating a double is just a bit flip.
        return -v;
    }
}
