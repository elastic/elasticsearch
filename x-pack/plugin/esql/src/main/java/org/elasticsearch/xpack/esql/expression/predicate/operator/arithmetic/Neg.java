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
import org.elasticsearch.xpack.esql.expression.function.Warnings;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.Duration;
import java.time.Period;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isTemporalAmount;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

public class Neg extends UnaryScalarFunction implements EvaluatorMapper {

    private final Warnings warnings;

    public Neg(Source source, Expression field) {
        super(source, field);
        warnings = new Warnings(source);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        DataType type = dataType();

        if (type.isNumeric()) {
            var f = toEvaluator.apply(field());
            ExpressionEvaluator.Factory supplier = null;

            if (type == DataTypes.INTEGER) {
                supplier = dvrCtx -> new NegIntsEvaluator(source(), f.get(dvrCtx), dvrCtx);
            }
            // Unsigned longs are unsupported by choice; negating them would require implicitly converting to long.
            else if (type == DataTypes.LONG) {
                supplier = dvrCtx -> new NegLongsEvaluator(source(), f.get(dvrCtx), dvrCtx);
            } else if (type == DataTypes.DOUBLE) {
                supplier = dvrCtx -> new NegDoublesEvaluator(f.get(dvrCtx), dvrCtx);
            }

            if (supplier != null) {
                return supplier;
            }
        } else if (isTemporalAmount(type)) {
            return toEvaluator.apply(field());
        }
        throw new EsqlIllegalArgumentException("arithmetic negation operator with unsupported data type [" + type + "]");
    }

    @Override
    public final Object fold() {
        if (isTemporalAmount(field().dataType()) && field() instanceof Literal literal) {
            return foldTemporalAmount(literal);
        }
        return EvaluatorMapper.super.fold();
    }

    private Object foldTemporalAmount(Literal literal) {
        try {
            Object value = literal.fold();
            if (value instanceof Period period) {
                return period.negated();
            }
            if (value instanceof Duration duration) {
                return duration.negated();
            }
        } catch (ArithmeticException ae) {
            warnings.registerException(ae);
            return null;
        }

        throw new EsqlIllegalArgumentException(
            "unexpected non-temporal amount literal [" + literal.sourceText() + "] of type [" + literal.dataType() + "]"
        );
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(
            field(),
            dt -> dt.isNumeric() || isTemporalAmount(dt),
            sourceText(),
            DEFAULT,
            "numeric",
            "date_period",
            "time_duration"
        );
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
