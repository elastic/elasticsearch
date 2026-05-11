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
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation.OperationSymbol.MOD;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.longToUnsignedLong;

public class Mod extends EsqlArithmeticOperation {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Mod", Mod::new);

    @FunctionInfo(
        operator = "%",
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

    /**
     * Fast path for a foldable scalar divisor: skip the per-row block fetch
     * for the right-hand side and bake the divisor into the evaluator via {@code @Fixed}.
     * Common in ClickBench-style aggregations like {@code STATS ... BY EventDate % 30}.
     * Falls back to the column-vs-column path for non-foldable, unsigned_long,
     * zero-valued, or non-numeric divisors — the zero case is kept on the slow
     * path so that the existing per-row warning / null behavior is preserved
     * instead of failing the query at plan time.
     */
    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        Expression right = right();
        if (right.foldable() == false) {
            return super.toEvaluator(toEvaluator);
        }
        Object folded = right.fold(toEvaluator.foldCtx());
        if (folded instanceof Number == false) {
            return super.toEvaluator(toEvaluator);
        }
        Number divisor = (Number) folded;
        DataType commonType = dataType();
        if (commonType == INTEGER) {
            int d = divisor.intValue();
            if (d == 0) {
                return super.toEvaluator(toEvaluator);
            }
            return new ModIntsByConstantEvaluator.Factory(
                source(),
                Cast.cast(source(), left().dataType(), commonType, toEvaluator.apply(left())),
                d
            );
        }
        if (commonType == LONG) {
            long d = divisor.longValue();
            if (d == 0L) {
                return super.toEvaluator(toEvaluator);
            }
            return new ModLongsByConstantEvaluator.Factory(
                source(),
                Cast.cast(source(), left().dataType(), commonType, toEvaluator.apply(left())),
                d
            );
        }
        if (commonType == DOUBLE) {
            double d = divisor.doubleValue();
            if (d == 0.0d) {
                return super.toEvaluator(toEvaluator);
            }
            return new ModDoublesByConstantEvaluator.Factory(
                source(),
                Cast.cast(source(), left().dataType(), commonType, toEvaluator.apply(left())),
                d
            );
        }
        // unsigned_long or anything else: stick with the binary evaluator
        return super.toEvaluator(toEvaluator);
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

    @Evaluator(extraName = "IntsByConstant")
    static int processIntsByConstant(int lhs, @Fixed int rhs) {
        return lhs % rhs;
    }

    @Evaluator(extraName = "LongsByConstant")
    static long processLongsByConstant(long lhs, @Fixed long rhs) {
        return lhs % rhs;
    }

    @Evaluator(extraName = "DoublesByConstant", warnExceptions = { ArithmeticException.class })
    static double processDoublesByConstant(double lhs, @Fixed double rhs) {
        double value = lhs % rhs;
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new ArithmeticException("/ by zero");
        }
        return value;
    }
}
