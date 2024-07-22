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
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.BinaryComparisonInversible;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation.OperationSymbol.DIV;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.longToUnsignedLong;

public class Div extends EsqlArithmeticOperation implements BinaryComparisonInversible {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Div", Div::new);

    private DataType type;

    @FunctionInfo(
        returnType = { "double", "integer", "long", "unsigned_long" },
        description = "Divide one number by another. "
            + "If either field is <<esql-multivalued-fields,multivalued>> then the result is `null`.",
        note = "Division of two integer types will yield an integer result, rounding towards 0. "
            + "If you need floating point division, <<esql-cast-operator>> one of the arguments to a `DOUBLE`."
    )
    public Div(
        Source source,
        @Param(name = "lhs", description = "A numeric value.", type = { "double", "integer", "long", "unsigned_long" }) Expression left,
        @Param(name = "rhs", description = "A numeric value.", type = { "double", "integer", "long", "unsigned_long" }) Expression right
    ) {
        this(source, left, right, null);
    }

    public Div(Source source, Expression left, Expression right, DataType type) {
        super(
            source,
            left,
            right,
            DIV,
            DivIntsEvaluator.Factory::new,
            DivLongsEvaluator.Factory::new,
            DivUnsignedLongsEvaluator.Factory::new,
            DivDoublesEvaluator.Factory::new
        );
        this.type = type;
    }

    private Div(StreamInput in) throws IOException {
        super(
            in,
            DIV,
            DivIntsEvaluator.Factory::new,
            DivLongsEvaluator.Factory::new,
            DivUnsignedLongsEvaluator.Factory::new,
            DivDoublesEvaluator.Factory::new
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        if (type == null) {
            type = super.dataType();
        }
        return type;
    }

    @Override
    protected NodeInfo<Div> info() {
        return NodeInfo.create(this, Div::new, left(), right(), type);
    }

    protected Div replaceChildren(Expression newLeft, Expression newRight) {
        return new Div(source(), newLeft, newRight, type);
    }

    @Override
    public ArithmeticOperationFactory binaryComparisonInverse() {
        return Mul::new;
    }

    @Evaluator(extraName = "Ints", warnExceptions = { ArithmeticException.class })
    static int processInts(int lhs, int rhs) {
        if (rhs == 0) {
            throw new ArithmeticException("/ by zero");
        }
        return lhs / rhs;
    }

    @Evaluator(extraName = "Longs", warnExceptions = { ArithmeticException.class })
    static long processLongs(long lhs, long rhs) {
        if (rhs == 0L) {
            throw new ArithmeticException("/ by zero");
        }
        return lhs / rhs;
    }

    @Evaluator(extraName = "UnsignedLongs", warnExceptions = { ArithmeticException.class })
    static long processUnsignedLongs(long lhs, long rhs) {
        if (rhs == NumericUtils.ZERO_AS_UNSIGNED_LONG) {
            throw new ArithmeticException("/ by zero");
        }
        return longToUnsignedLong(Long.divideUnsigned(longToUnsignedLong(lhs, true), longToUnsignedLong(rhs, true)), true);
    }

    @Evaluator(extraName = "Doubles", warnExceptions = { ArithmeticException.class })
    static double processDoubles(double lhs, double rhs) {
        double value = lhs / rhs;
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new ArithmeticException("/ by zero");
        }
        return value;
    }
}
