/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.NumericUtils;

import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

public class Sqrt extends UnaryScalarFunction implements EvaluatorMapper {
    public Sqrt(Source source, @Param(name = "n", type = { "integer", "long", "double", "unsigned_long" }) Expression n) {
        super(source, n);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        var field = toEvaluator.apply(field());
        var fieldType = field().dataType();

        if (fieldType == DataTypes.DOUBLE) {
            return new SqrtDoubleEvaluator.Factory(source(), field);
        }
        if (fieldType == DataTypes.INTEGER) {
            return new SqrtIntEvaluator.Factory(source(), field);
        }
        if (fieldType == DataTypes.LONG) {
            return new SqrtLongEvaluator.Factory(source(), field);
        }
        if (fieldType == DataTypes.UNSIGNED_LONG) {
            return new SqrtUnsignedLongEvaluator.Factory(field);
        }

        throw EsqlIllegalArgumentException.illegalDataType(fieldType);
    }

    @Evaluator(extraName = "Double", warnExceptions = ArithmeticException.class)
    static double process(double val) {
        if (val < 0) {
            throw new ArithmeticException("Square root of negative");
        }
        return Math.sqrt(val);
    }

    @Evaluator(extraName = "Long", warnExceptions = ArithmeticException.class)
    static double process(long val) {
        if (val < 0) {
            throw new ArithmeticException("Square root of negative");
        }
        return Math.sqrt(val);
    }

    @Evaluator(extraName = "UnsignedLong")
    static double processUnsignedLong(long val) {
        return Math.sqrt(NumericUtils.unsignedLongToDouble(val));
    }

    @Evaluator(extraName = "Int", warnExceptions = ArithmeticException.class)
    static double process(int val) {
        if (val < 0) {
            throw new ArithmeticException("Square root of negative");
        }
        return Math.sqrt(val);
    }

    @Override
    public final Expression replaceChildren(List<Expression> newChildren) {
        return new Sqrt(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Sqrt::new, field());
    }

    @Override
    public DataType dataType() {
        return DataTypes.DOUBLE;
    }

    @Override
    public Object fold() {
        return EvaluatorMapper.super.fold();
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isNumeric(field, sourceText(), DEFAULT);
    }
}
