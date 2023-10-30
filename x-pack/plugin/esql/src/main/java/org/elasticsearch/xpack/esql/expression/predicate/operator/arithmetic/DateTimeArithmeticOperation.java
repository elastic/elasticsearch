/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.ExceptionUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.Collection;
import java.util.function.Function;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.TIME_DURATION;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isDateTimeOrTemporal;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isTemporalAmount;

abstract class DateTimeArithmeticOperation extends EsqlArithmeticOperation {
    /** Arithmetic (quad) function. */
    interface DatetimeArithmeticEvaluator {
        ExpressionEvaluator.Factory apply(Source source, ExpressionEvaluator.Factory expressionEvaluator, TemporalAmount temporalAmount);
    }

    private final DatetimeArithmeticEvaluator datetimes;

    DateTimeArithmeticOperation(
        Source source,
        Expression left,
        Expression right,
        OperationSymbol op,
        ArithmeticEvaluator ints,
        ArithmeticEvaluator longs,
        ArithmeticEvaluator ulongs,
        ArithmeticEvaluator doubles,
        DatetimeArithmeticEvaluator datetimes
    ) {
        super(source, left, right, op, ints, longs, ulongs, doubles);
        this.datetimes = datetimes;
    }

    @Override
    protected TypeResolution resolveType() {
        DataType leftType = left().dataType();
        DataType rightType = right().dataType();

        // Date math is only possible if either
        // - one argument is a DATETIME and the other a (foldable) TemporalValue, or
        // - both arguments are TemporalValues (so we can fold them).
        if (isDateTimeOrTemporal(leftType) || isDateTimeOrTemporal(rightType)) {
            if ((leftType == DataTypes.DATETIME && isTemporalAmount(rightType))
                || (rightType == DataTypes.DATETIME && isTemporalAmount(leftType))) {
                return TypeResolution.TYPE_RESOLVED;
            }
            if (leftType == TIME_DURATION && rightType == TIME_DURATION) {
                return TypeResolution.TYPE_RESOLVED;
            }
            if (leftType == DATE_PERIOD && rightType == DATE_PERIOD) {
                return TypeResolution.TYPE_RESOLVED;
            }

            return new TypeResolution(
                format(null, "[{}] has arguments with incompatible types [{}] and [{}]", symbol(), leftType, rightType)
            );
        }
        return super.resolveType();
    }

    /**
     * Override this to allow processing literals of type {@link EsqlDataTypes#DATE_PERIOD} when folding constants.
     * Used in {@link DateTimeArithmeticOperation#fold()}.
     * @param left the left period
     * @param right the right period
     * @return the result of the evaluation
     */
    abstract Period fold(Period left, Period right);

    /**
     * Override this to allow processing literals of type {@link EsqlDataTypes#TIME_DURATION} when folding constants.
     * Used in {@link DateTimeArithmeticOperation#fold()}.
     * @param left the left duration
     * @param right the right duration
     * @return the result of the evaluation
     */
    abstract Duration fold(Duration left, Duration right);

    @Override
    public final Object fold() {
        DataType leftDataType = left().dataType();
        DataType rightDataType = right().dataType();
        if (leftDataType == DATE_PERIOD && rightDataType == DATE_PERIOD) {
            // Both left and right expressions are temporal amounts; we can assume they are both foldable.
            var l = left().fold();
            var r = right().fold();
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
            Duration l = (Duration) left().fold();
            Duration r = (Duration) right().fold();
            try {
                return fold(l, r);
            } catch (ArithmeticException e) {
                // Folding will be triggered before the plan is sent to the compute service, so we have to handle arithmetic exceptions
                // manually and provide a user-friendly error message.
                throw ExceptionUtils.math(source(), e);
            }
        }
        return super.fold();
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        if (dataType() == DataTypes.DATETIME) {
            // One of the arguments has to be a datetime and the other a temporal amount.
            Expression datetimeArgument;
            Expression temporalAmountArgument;
            if (left().dataType() == DataTypes.DATETIME) {
                datetimeArgument = left();
                temporalAmountArgument = right();
            } else {
                datetimeArgument = right();
                temporalAmountArgument = left();
            }

            return datetimes.apply(source(), toEvaluator.apply(datetimeArgument), (TemporalAmount) temporalAmountArgument.fold());
        } else {
            return super.toEvaluator(toEvaluator);
        }
    }
}
