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
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.ExceptionUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;

import java.io.IOException;
import java.time.Duration;
import java.time.Period;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TIME_DURATION;
import static org.elasticsearch.xpack.esql.core.type.DataType.isTemporalAmount;

public class Neg extends UnaryScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Neg", Neg::new);

    @FunctionInfo(
        operator = "-",
        returnType = { "double", "integer", "long", "date_period", "time_duration" },
        description = "Returns the negation of the argument."
    )
    public Neg(
        Source source,
        @Param(
            name = "field",
            description = "A numeric value or a date time interval.",
            type = { "double", "integer", "long", "date_period", "time_duration" }
        ) Expression field
    ) {
        super(source, field);
    }

    public Neg(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        DataType type = dataType();

        if (type.isNumeric()) {
            var f = toEvaluator.apply(field());
            ExpressionEvaluator.Factory factory = null;

            if (type == DataType.INTEGER) {
                factory = new NegIntsEvaluator.Factory(source(), f);
            }
            // Unsigned longs are unsupported by choice; negating them would require implicitly converting to long.
            else if (type == DataType.LONG) {
                factory = new NegLongsEvaluator.Factory(source(), f);
            } else if (type == DataType.DOUBLE) {
                factory = new NegDoublesEvaluator.Factory(source(), f);
            }

            if (factory != null) {
                return factory;
            }
        } else if (isTemporalAmount(type)) {
            return toEvaluator.apply(field());
        }
        throw new EsqlIllegalArgumentException("arithmetic negation operator with unsupported data type [" + type + "]");
    }

    @Override
    public final Object fold(FoldContext ctx) {
        DataType dataType = field().dataType();
        // For date periods and time durations, we need to treat folding differently. These types are unrepresentable, so there is no
        // evaluator for them - but the default folding requires an evaluator.
        if (dataType == DATE_PERIOD) {
            Period fieldValue = (Period) field().fold(ctx);
            try {
                return fieldValue.negated();
            } catch (ArithmeticException e) {
                // Folding will be triggered before the plan is sent to the compute service, so we have to handle arithmetic exceptions
                // manually and provide a user-friendly error message.
                throw ExceptionUtils.math(source(), e);
            }
        }
        if (dataType == TIME_DURATION) {
            Duration fieldValue = (Duration) field().fold(ctx);
            try {
                return fieldValue.negated();
            } catch (ArithmeticException e) {
                // Folding will be triggered before the plan is sent to the compute service, so we have to handle arithmetic exceptions
                // manually and provide a user-friendly error message.
                throw ExceptionUtils.math(source(), e);
            }
        }
        return super.fold(ctx);
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(
            field(),
            dt -> dt != DataType.UNSIGNED_LONG && (dt.isNumeric() || isTemporalAmount(dt)),
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
