/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.ExceptionUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.Collection;

import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TIME_DURATION;
import static org.elasticsearch.xpack.esql.core.type.DataType.isDateTimeOrNanosOrTemporal;
import static org.elasticsearch.xpack.esql.core.type.DataType.isMillisOrNanos;
import static org.elasticsearch.xpack.esql.core.type.DataType.isNull;
import static org.elasticsearch.xpack.esql.core.type.DataType.isTemporalAmount;

public abstract class DateTimeArithmeticOperation extends EsqlArithmeticOperation {
    /** Arithmetic (quad) function. */
    interface DatetimeArithmeticEvaluator {
        ExpressionEvaluator.Factory apply(Source source, ExpressionEvaluator.Factory expressionEvaluator, TemporalAmount temporalAmount);
    }

    private final DatetimeArithmeticEvaluator millisEvaluator;
    private final DatetimeArithmeticEvaluator nanosEvaluator;

    DateTimeArithmeticOperation(
        Source source,
        Expression left,
        Expression right,
        OperationSymbol op,
        BinaryEvaluator ints,
        BinaryEvaluator longs,
        BinaryEvaluator ulongs,
        BinaryEvaluator doubles,
        DatetimeArithmeticEvaluator millisEvaluator,
        DatetimeArithmeticEvaluator nanosEvaluator
    ) {
        super(source, left, right, op, ints, longs, ulongs, doubles);
        this.millisEvaluator = millisEvaluator;
        this.nanosEvaluator = nanosEvaluator;
    }

    DateTimeArithmeticOperation(
        StreamInput in,
        OperationSymbol op,
        BinaryEvaluator ints,
        BinaryEvaluator longs,
        BinaryEvaluator ulongs,
        BinaryEvaluator doubles,
        DatetimeArithmeticEvaluator millisEvaluator,
        DatetimeArithmeticEvaluator nanosEvaluator
    ) throws IOException {
        super(in, op, ints, longs, ulongs, doubles);
        this.millisEvaluator = millisEvaluator;
        this.nanosEvaluator = nanosEvaluator;
    }

    @Override
    protected TypeResolution resolveInputType(Expression e, TypeResolutions.ParamOrdinal paramOrdinal) {
        return TypeResolutions.isType(
            e,
            t -> t.isNumeric() || DataType.isDateTimeOrNanosOrTemporal(t) || DataType.isNull(t),
            sourceText(),
            paramOrdinal,
            "date_nanos",
            "datetime",
            "numeric"
        );
    }

    @Override
    protected TypeResolution checkCompatibility() {
        DataType leftType = left().dataType();
        DataType rightType = right().dataType();

        // Date math is only possible if either
        // - one argument is a DATETIME and the other a (foldable) TemporalValue, or
        // - both arguments are TemporalValues (so we can fold them), or
        // - one argument is NULL and the other one a DATETIME.
        if (isDateTimeOrNanosOrTemporal(leftType) || isDateTimeOrNanosOrTemporal(rightType)) {
            if (isNull(leftType) || isNull(rightType)) {
                return TypeResolution.TYPE_RESOLVED;
            }
            if ((isMillisOrNanos(leftType) && isTemporalAmount(rightType)) || (isTemporalAmount(leftType) && isMillisOrNanos(rightType))) {
                return TypeResolution.TYPE_RESOLVED;
            }
            if (isTemporalAmount(leftType) && isTemporalAmount(rightType) && leftType == rightType) {
                return TypeResolution.TYPE_RESOLVED;
            }

            return new TypeResolution(formatIncompatibleTypesMessage(symbol(), leftType, rightType));
        }
        return super.checkCompatibility();
    }

    /**
     * Override this to allow processing literals of type {@link DataType#DATE_PERIOD} when folding constants.
     * Used in {@link DateTimeArithmeticOperation#fold}.
     * @param left the left period
     * @param right the right period
     * @return the result of the evaluation
     */
    abstract Period fold(Period left, Period right);

    /**
     * Override this to allow processing literals of type {@link DataType#TIME_DURATION} when folding constants.
     * Used in {@link DateTimeArithmeticOperation#fold}.
     * @param left the left duration
     * @param right the right duration
     * @return the result of the evaluation
     */
    abstract Duration fold(Duration left, Duration right);

    @Override
    public final Object fold(FoldContext ctx) {
        DataType leftDataType = left().dataType();
        DataType rightDataType = right().dataType();
        if (leftDataType == DATE_PERIOD && rightDataType == DATE_PERIOD) {
            // Both left and right expressions are temporal amounts; we can assume they are both foldable.
            var l = left().fold(ctx);
            var r = right().fold(ctx);
            if (l instanceof Collection<?> || r instanceof Collection<?>) {
                return null;
            }
            try {
                return fold((Period) l, (Period) r);
            } catch (ArithmeticException e) {
                // Folding will be triggered before the plan is sent to the compute service, so we have to handle arithmetic exceptions
                // manually and provide a user-friendly error message.
                throw ExceptionUtils.math(source(), e);
            }
        }
        if (leftDataType == TIME_DURATION && rightDataType == TIME_DURATION) {
            // Both left and right expressions are temporal amounts; we can assume they are both foldable.
            Duration l = (Duration) left().fold(ctx);
            Duration r = (Duration) right().fold(ctx);
            try {
                return fold(l, r);
            } catch (ArithmeticException e) {
                // Folding will be triggered before the plan is sent to the compute service, so we have to handle arithmetic exceptions
                // manually and provide a user-friendly error message.
                throw ExceptionUtils.math(source(), e);
            }
        }
        if (isNull(leftDataType) || isNull(rightDataType)) {
            return null;
        }
        return super.fold(ctx);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (dataType() == DATETIME) {
            // One of the arguments has to be a datetime and the other a temporal amount.
            Expression datetimeArgument;
            Expression temporalAmountArgument;
            if (left().dataType() == DATETIME) {
                datetimeArgument = left();
                temporalAmountArgument = right();
            } else {
                datetimeArgument = right();
                temporalAmountArgument = left();
            }

            return millisEvaluator.apply(
                source(),
                toEvaluator.apply(datetimeArgument),
                (TemporalAmount) temporalAmountArgument.fold(toEvaluator.foldCtx())
            );
        } else if (dataType() == DATE_NANOS) {
            // One of the arguments has to be a date_nanos and the other a temporal amount.
            Expression dateNanosArgument;
            Expression temporalAmountArgument;
            if (left().dataType() == DATE_NANOS) {
                dateNanosArgument = left();
                temporalAmountArgument = right();
            } else {
                dateNanosArgument = right();
                temporalAmountArgument = left();
            }

            return nanosEvaluator.apply(
                source(),
                toEvaluator.apply(dateNanosArgument),
                (TemporalAmount) temporalAmountArgument.fold(toEvaluator.foldCtx())
            );
        } else {
            return super.toEvaluator(toEvaluator);
        }
    }
}
