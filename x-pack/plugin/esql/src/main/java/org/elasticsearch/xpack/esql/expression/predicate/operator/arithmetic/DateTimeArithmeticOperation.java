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

import java.time.temporal.TemporalAmount;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isDateTimeOrTemporal;

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
        // date math is only possible if one argument is a DATETIME and the other a (foldable) TemporalValue
        if (isDateTimeOrTemporal(leftType) || isDateTimeOrTemporal(rightType)) {
            if (argumentOfType(DataTypes::isDateTime) == null || argumentOfType(EsqlDataTypes::isTemporalAmount) == null) {
                return new TypeResolution(
                    format(null, "[{}] has arguments with incompatible types [{}] and [{}]", symbol(), leftType, rightType)
                );
            }
            return TypeResolution.TYPE_RESOLVED;
        }
        return super.resolveType();
    }

    @Override
    public Supplier<ExpressionEvaluator> toEvaluator(Function<Expression, Supplier<ExpressionEvaluator>> toEvaluator) {
        return dataType() == DataTypes.DATETIME
            ? () -> datetimes.apply(
                source(),
                toEvaluator.apply(argumentOfType(DataTypes::isDateTime)).get(),
                (TemporalAmount) argumentOfType(EsqlDataTypes::isTemporalAmount).fold()
            )
            : super.toEvaluator(toEvaluator);
    }

    private Expression argumentOfType(Predicate<DataType> filter) {
        return filter.test(left().dataType()) ? left() : filter.test(right().dataType()) ? right() : null;
    }
}
