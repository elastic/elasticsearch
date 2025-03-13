/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongMultiplyExact;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation.OperationSymbol.MUL;

public class Mul extends EsqlArithmeticOperation implements BinaryComparisonInversible {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Mul", Mul::new);

    @FunctionInfo(
        operator = "*",
        returnType = { "double", "integer", "long", "unsigned_long" },
        description = "Multiply two numbers together. "
            + "If either field is <<esql-multivalued-fields,multivalued>> then the result is `null`."
    )
    public Mul(
        Source source,
        @Param(name = "lhs", description = "A numeric value.", type = { "double", "integer", "long", "unsigned_long" }) Expression left,
        @Param(name = "rhs", description = "A numeric value.", type = { "double", "integer", "long", "unsigned_long" }) Expression right
    ) {
        super(
            source,
            left,
            right,
            MUL,
            MulIntsEvaluator.Factory::new,
            MulLongsEvaluator.Factory::new,
            MulUnsignedLongsEvaluator.Factory::new,
            MulDoublesEvaluator.Factory::new
        );
    }

    private Mul(StreamInput in) throws IOException {
        super(
            in,
            MUL,
            MulIntsEvaluator.Factory::new,
            MulLongsEvaluator.Factory::new,
            MulUnsignedLongsEvaluator.Factory::new,
            MulDoublesEvaluator.Factory::new
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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

    @Evaluator(extraName = "Doubles", warnExceptions = { ArithmeticException.class })
    static double processDoubles(double lhs, double rhs) {
        return NumericUtils.asFiniteNumber(lhs * rhs);
    }

}
