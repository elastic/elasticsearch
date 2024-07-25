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

import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation.OperationSymbol.MOD;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.longToUnsignedLong;

public class Mod extends EsqlArithmeticOperation {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Mod", Mod::new);

    @FunctionInfo(
        returnType = { "double", "integer", "long", "unsigned_long" },
        description = "Divide one number by another and return the remainder. "
            + "If either field is <<esql-multivalued-fields,multivalued>> then the result is `null`."
    )
    public Mod(
        Source source,
        @Param(name = "lhs", description = "A numeric value.", type = { "double", "integer", "long", "unsigned_long" }) Expression left,
        @Param(name = "rhs", description = "A numeric value.", type = { "double", "integer", "long", "unsigned_long" }) Expression right
    ) {
        super(
            source,
            left,
            right,
            MOD,
            ModIntsEvaluator.Factory::new,
            ModLongsEvaluator.Factory::new,
            ModUnsignedLongsEvaluator.Factory::new,
            ModDoublesEvaluator.Factory::new
        );
    }

    private Mod(StreamInput in) throws IOException {
        super(
            in,
            MOD,
            ModIntsEvaluator.Factory::new,
            ModLongsEvaluator.Factory::new,
            ModUnsignedLongsEvaluator.Factory::new,
            ModDoublesEvaluator.Factory::new
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Mod> info() {
        return NodeInfo.create(this, Mod::new, left(), right());
    }

    @Override
    protected Mod replaceChildren(Expression left, Expression right) {
        return new Mod(source(), left, right);
    }

    @Evaluator(extraName = "Ints", warnExceptions = { ArithmeticException.class })
    static int processInts(int lhs, int rhs) {
        if (rhs == 0) {
            throw new ArithmeticException("/ by zero");
        }
        return lhs % rhs;
    }

    @Evaluator(extraName = "Longs", warnExceptions = { ArithmeticException.class })
    static long processLongs(long lhs, long rhs) {
        if (rhs == 0L) {
            throw new ArithmeticException("/ by zero");
        }
        return lhs % rhs;
    }

    @Evaluator(extraName = "UnsignedLongs", warnExceptions = { ArithmeticException.class })
    static long processUnsignedLongs(long lhs, long rhs) {
        if (rhs == NumericUtils.ZERO_AS_UNSIGNED_LONG) {
            throw new ArithmeticException("/ by zero");
        }
        return longToUnsignedLong(Long.remainderUnsigned(longToUnsignedLong(lhs, true), longToUnsignedLong(rhs, true)), true);
    }

    @Evaluator(extraName = "Doubles", warnExceptions = { ArithmeticException.class })
    static double processDoubles(double lhs, double rhs) {
        double value = lhs % rhs;
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new ArithmeticException("/ by zero");
        }
        return value;
    }
}
