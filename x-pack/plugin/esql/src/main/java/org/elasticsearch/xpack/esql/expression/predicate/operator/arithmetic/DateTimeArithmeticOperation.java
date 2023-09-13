/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.common.TriFunction;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.TIME_DURATION;

abstract class DateTimeArithmeticOperation extends EsqlArithmeticOperation {

    interface DatetimeArithmeticEvaluator extends TriFunction<Source, ExpressionEvaluator, TemporalAmount, ExpressionEvaluator> {};

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

        if (hasArgumentWhich(EsqlDataTypes::isDateTimeOrTemporal)) {
            // Date math is only possible if either
            // - one argument is a DATETIME and the other a (foldable) TemporalValue, or
            // - both arguments are TemporalValues (so we can fold them).
            if (hasArgumentWhich(DataTypes::isDateTime) && hasArgumentWhich(EsqlDataTypes::isTemporalAmount)) {
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
     * Used in {@link DateTimeArithmeticOperation#fold}.
     * @param left the left period
     * @param right the right period
     * @return the result of the evaluation
     */
    protected Period processTimePeriods(Period left, Period right) {
        throw new UnsupportedOperationException("processing of time periods is unsupported for [" + op().symbol() + "]");
    }

    /**
     * Override this to allow processing literals of type {@link EsqlDataTypes#TIME_DURATION} when folding constants.
     * Used in {@link DateTimeArithmeticOperation#fold}.
     * @param left the left duration
     * @param right the right duration
     * @return the result of the evaluation
     */
    protected Duration processDateDurations(Duration left, Duration right) {
        throw new UnsupportedOperationException("processing of date durations is unsupported for [" + op().symbol() + "]");
    }

    @Override
    public final Object fold() {
        DataType leftDataType = left().dataType();
        DataType rightDataType = right().dataType();
        if (leftDataType == DATE_PERIOD && rightDataType == DATE_PERIOD) {
            // Both left and right expressions are temporal amounts; we can assume they are both foldable.
            Period l = (Period) left().fold();
            Period r = (Period) right().fold();
            return processTimePeriods(l, r);
        }
        if (leftDataType == TIME_DURATION && rightDataType == TIME_DURATION) {
            // Both left and right expressions are temporal amounts; we can assume they are both foldable.
            Duration l = (Duration) left().fold();
            Duration r = (Duration) right().fold();
            return processDateDurations(l, r);
        }
        return super.fold();
    }

    @Override
    public Supplier<ExpressionEvaluator> toEvaluator(Function<Expression, Supplier<ExpressionEvaluator>> toEvaluator) {
        return dataType() == DataTypes.DATETIME
            ? () -> datetimes.apply(
                source(),
                toEvaluator.apply(argumentWhich(DataTypes::isDateTime)).get(),
                (TemporalAmount) argumentWhich(EsqlDataTypes::isTemporalAmount).fold()
            )
            : super.toEvaluator(toEvaluator);
    }

    private Expression argumentWhich(Predicate<DataType> filter) {
        return filter.test(left().dataType()) ? left() : filter.test(right().dataType()) ? right() : null;
    }

    private boolean hasArgumentWhich(Predicate<DataType> filter) {
        return argumentWhich(filter) != null;
    }
}
