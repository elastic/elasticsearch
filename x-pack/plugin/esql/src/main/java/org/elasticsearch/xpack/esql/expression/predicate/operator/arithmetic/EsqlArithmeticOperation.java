/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.ArithmeticOperation;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.BinaryArithmeticOperation;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.io.IOException;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;

abstract class EsqlArithmeticOperation extends ArithmeticOperation implements EvaluatorMapper {

    /**
     * The only role of this enum is to fit the super constructor that expects a BinaryOperation which is
     * used just for its symbol.
     * The rest of the methods should not be triggered hence the UOE.
     */
    enum OperationSymbol implements BinaryArithmeticOperation {
        ADD("+"),
        SUB("-"),
        MUL("*"),
        DIV("/"),
        MOD("%");

        private final String symbol;

        OperationSymbol(String symbol) {
            this.symbol = symbol;
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object doApply(Object o, Object o2) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String symbol() {
            return symbol;
        }
    }

    /** Arithmetic (quad) function. */
    interface ArithmeticEvaluator {
        ExpressionEvaluator.Factory apply(Source source, ExpressionEvaluator.Factory lhs, ExpressionEvaluator.Factory rhs);
    }

    private final ArithmeticEvaluator ints;
    private final ArithmeticEvaluator longs;
    private final ArithmeticEvaluator ulongs;
    private final ArithmeticEvaluator doubles;

    private DataType dataType;

    EsqlArithmeticOperation(
        Source source,
        Expression left,
        Expression right,
        OperationSymbol op,
        ArithmeticEvaluator ints,
        ArithmeticEvaluator longs,
        ArithmeticEvaluator ulongs,
        ArithmeticEvaluator doubles
    ) {
        super(source, left, right, op);
        this.ints = ints;
        this.longs = longs;
        this.ulongs = ulongs;
        this.doubles = doubles;
    }

    @Override
    public Object fold() {
        return EvaluatorMapper.super.fold();
    }

    public DataType dataType() {
        if (dataType == null) {
            dataType = EsqlDataTypeRegistry.INSTANCE.commonType(left().dataType(), right().dataType());
        }
        return dataType;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        var commonType = dataType();
        var leftType = left().dataType();
        if (leftType.isNumeric()) {

            var lhs = Cast.cast(left().dataType(), commonType, toEvaluator.apply(left()));
            var rhs = Cast.cast(right().dataType(), commonType, toEvaluator.apply(right()));

            ArithmeticEvaluator eval;
            if (commonType == INTEGER) {
                eval = ints;
            } else if (commonType == LONG) {
                eval = longs;
            } else if (commonType == UNSIGNED_LONG) {
                eval = ulongs;
            } else if (commonType == DOUBLE) {
                eval = doubles;
            } else {
                throw new EsqlIllegalArgumentException("Unsupported type " + commonType);
            }
            return eval.apply(source(), lhs, rhs);
        }
        throw new EsqlIllegalArgumentException("Unsupported type " + leftType);
    }
}
