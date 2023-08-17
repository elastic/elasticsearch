/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation.OperationSymbol.DIV;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.util.NumericUtils.asLongUnsigned;

public class Div extends EsqlArithmeticOperation {

    Div(Source source, Expression left, Expression right) {
        super(
            source,
            left,
            right,
            DIV,
            DivIntsEvaluator::new,
            DivLongsEvaluator::new,
            DivUnsignedLongsEvaluator::new,
            (s, l, r) -> new DivDoublesEvaluator(l, r)
        );
    }

    @Override
    public DataType dataType() {
        return DOUBLE;
    }

    @Override
    protected NodeInfo<Div> info() {
        return NodeInfo.create(this, Div::new, left(), right());
    }

    protected Div replaceChildren(Expression newLeft, Expression newRight) {
        return new Div(source(), newLeft, newRight);
    }

    @Override
    public ArithmeticOperationFactory binaryComparisonInverse() {
        return Mul::new;
    }

    @Evaluator(extraName = "Ints", warnExceptions = { ArithmeticException.class })
    static int processInts(int lhs, int rhs) {
        return lhs / rhs;
    }

    @Evaluator(extraName = "Longs", warnExceptions = { ArithmeticException.class })
    static long processLongs(long lhs, long rhs) {
        return lhs / rhs;
    }

    @Evaluator(extraName = "UnsignedLongs", warnExceptions = { ArithmeticException.class })
    static long processUnsignedLongs(long lhs, long rhs) {
        return asLongUnsigned(Long.divideUnsigned(asLongUnsigned(lhs), asLongUnsigned(rhs)));
    }

    @Evaluator(extraName = "Doubles")
    static double processDoubles(double lhs, double rhs) {
        return lhs / rhs;
    }
}
