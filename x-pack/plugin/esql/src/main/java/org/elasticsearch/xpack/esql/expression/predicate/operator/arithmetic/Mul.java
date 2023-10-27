/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.BinaryComparisonInversible;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation.OperationSymbol.MUL;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongMultiplyExact;

public class Mul extends EsqlArithmeticOperation implements BinaryComparisonInversible {

    public Mul(Source source, Expression left, Expression right) {
        super(
            source,
            left,
            right,
            MUL,
            MulIntsEvaluator.Factory::new,
            MulLongsEvaluator.Factory::new,
            MulUnsignedLongsEvaluator.Factory::new,
            (s, lhs, rhs) -> new MulDoublesEvaluator.Factory(lhs, rhs)
        );
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        DataType l = left().dataType();
        DataType r = right().dataType();

        // 1. both are numbers
        if (DataTypes.isNullOrNumeric(l) && DataTypes.isNullOrNumeric(r)) {
            return TypeResolution.TYPE_RESOLVED;
        }

        return new TypeResolution(format(null, "[{}] has arguments with incompatible types [{}] and [{}]", symbol(), l, r));
    }

    @Override
    public ArithmeticOperationFactory binaryComparisonInverse() {
        return Div::new;
    }

    @Override
    protected boolean isCommutative() {
        return true;
    }

    @Override
    public Mul swapLeftAndRight() {
        return new Mul(source(), right(), left());
    }

    @Override
    protected NodeInfo<Mul> info() {
        return NodeInfo.create(this, Mul::new, left(), right());
    }

    @Override
    protected Mul replaceChildren(Expression left, Expression right) {
        return new Mul(source(), left, right);
    }

    @Evaluator(extraName = "Ints", warnExceptions = { ArithmeticException.class })
    static int processInts(int lhs, int rhs) {
        return Math.multiplyExact(lhs, rhs);
    }

    @Evaluator(extraName = "Longs", warnExceptions = { ArithmeticException.class })
    static long processLongs(long lhs, long rhs) {
        return Math.multiplyExact(lhs, rhs);
    }

    @Evaluator(extraName = "UnsignedLongs", warnExceptions = { ArithmeticException.class })
    static long processUnsignedLongs(long lhs, long rhs) {
        return unsignedLongMultiplyExact(lhs, rhs);
    }

    @Evaluator(extraName = "Doubles")
    static double processDoubles(double lhs, double rhs) {
        return lhs * rhs;
    }

}
