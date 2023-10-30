/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.BinaryComparisonInversible;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation.OperationSymbol.SUB;
import static org.elasticsearch.xpack.ql.type.DateUtils.asDateTime;
import static org.elasticsearch.xpack.ql.type.DateUtils.asMillis;
import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongSubtractExact;

public class Sub extends DateTimeArithmeticOperation implements BinaryComparisonInversible {

    public Sub(Source source, Expression left, Expression right) {
        super(
            source,
            left,
            right,
            SUB,
            SubIntsEvaluator.Factory::new,
            SubLongsEvaluator.Factory::new,
            SubUnsignedLongsEvaluator.Factory::new,
            (s, lhs, rhs) -> new SubDoublesEvaluator.Factory(lhs, rhs),
            SubDatetimesEvaluator.Factory::new
        );
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = super.resolveType();
        // As opposed to general date time arithmetics, we cannot subtract a datetime from something else.
        if (resolution.resolved() && EsqlDataTypes.isDateTimeOrTemporal(dataType()) && DataTypes.isDateTime(right().dataType())) {
            return new TypeResolution(
                format(
                    null,
                    "[{}] arguments are in unsupported order: cannot subtract a [{}] value [{}] from a [{}] amount [{}]",
                    symbol(),
                    right().dataType(),
                    right().sourceText(),
                    left().dataType(),
                    left().sourceText()
                )
            );
        }
        return resolution;
    }

    @Override
    protected NodeInfo<Sub> info() {
        return NodeInfo.create(this, Sub::new, left(), right());
    }

    @Override
    protected Sub replaceChildren(Expression left, Expression right) {
        return new Sub(source(), left, right);
    }

    @Override
    public ArithmeticOperationFactory binaryComparisonInverse() {
        return Add::new;
    }

    @Evaluator(extraName = "Ints", warnExceptions = { ArithmeticException.class })
    static int processInts(int lhs, int rhs) {
        return Math.subtractExact(lhs, rhs);
    }

    @Evaluator(extraName = "Longs", warnExceptions = { ArithmeticException.class })
    static long processLongs(long lhs, long rhs) {
        return Math.subtractExact(lhs, rhs);
    }

    @Evaluator(extraName = "UnsignedLongs", warnExceptions = { ArithmeticException.class })
    static long processUnsignedLongs(long lhs, long rhs) {
        return unsignedLongSubtractExact(lhs, rhs);
    }

    @Evaluator(extraName = "Doubles")
    static double processDoubles(double lhs, double rhs) {
        return lhs - rhs;
    }

    @Evaluator(extraName = "Datetimes", warnExceptions = { ArithmeticException.class, DateTimeException.class })
    static long processDatetimes(long datetime, @Fixed TemporalAmount temporalAmount) {
        // using a UTC conversion since `datetime` is always a UTC-Epoch timestamp, either read from ES or converted through a function
        return asMillis(asDateTime(datetime).minus(temporalAmount));
    }

    @Override
    public Period fold(Period left, Period right) {
        return left.minus(right);
    }

    @Override
    public Duration fold(Duration left, Duration right) {
        return left.minus(right);
    }
}
