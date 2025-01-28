/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;

import static org.elasticsearch.xpack.esql.core.util.DateUtils.asDateTime;
import static org.elasticsearch.xpack.esql.core.util.DateUtils.asMillis;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongAddExact;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation.OperationSymbol.ADD;

public class Add extends DateTimeArithmeticOperation implements BinaryComparisonInversible {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Add", Add::new);

    @FunctionInfo(
        operator = "+",
        returnType = { "double", "integer", "long", "date_nanos", "date_period", "datetime", "time_duration", "unsigned_long" },
        description = "Add two numbers together. " + "If either field is <<esql-multivalued-fields,multivalued>> then the result is `null`."
    )
    public Add(
        Source source,
        @Param(
            name = "lhs",
            description = "A numeric value or a date time value.",
            type = { "double", "integer", "long", "date_nanos", "date_period", "datetime", "time_duration", "unsigned_long" }
        ) Expression left,
        @Param(
            name = "rhs",
            description = "A numeric value or a date time value.",
            type = { "double", "integer", "long", "date_nanos", "date_period", "datetime", "time_duration", "unsigned_long" }
        ) Expression right
    ) {
        super(
            source,
            left,
            right,
            ADD,
            AddIntsEvaluator.Factory::new,
            AddLongsEvaluator.Factory::new,
            AddUnsignedLongsEvaluator.Factory::new,
            AddDoublesEvaluator.Factory::new,
            AddDatetimesEvaluator.Factory::new,
            AddDateNanosEvaluator.Factory::new
        );
    }

    private Add(StreamInput in) throws IOException {
        super(
            in,
            ADD,
            AddIntsEvaluator.Factory::new,
            AddLongsEvaluator.Factory::new,
            AddUnsignedLongsEvaluator.Factory::new,
            AddDoublesEvaluator.Factory::new,
            AddDatetimesEvaluator.Factory::new,
            AddDateNanosEvaluator.Factory::new
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Add> info() {
        return NodeInfo.create(this, Add::new, left(), right());
    }

    @Override
    protected Add replaceChildren(Expression left, Expression right) {
        return new Add(source(), left, right);
    }

    @Override
    public Add swapLeftAndRight() {
        return new Add(source(), right(), left());
    }

    @Override
    public ArithmeticOperationFactory binaryComparisonInverse() {
        return Sub::new;
    }

    @Override
    protected boolean isCommutative() {
        return true;
    }

    @Evaluator(extraName = "Ints", warnExceptions = { ArithmeticException.class })
    static int processInts(int lhs, int rhs) {
        return Math.addExact(lhs, rhs);
    }

    @Evaluator(extraName = "Longs", warnExceptions = { ArithmeticException.class })
    static long processLongs(long lhs, long rhs) {
        return Math.addExact(lhs, rhs);
    }

    @Evaluator(extraName = "UnsignedLongs", warnExceptions = { ArithmeticException.class })
    public static long processUnsignedLongs(long lhs, long rhs) {
        return unsignedLongAddExact(lhs, rhs);
    }

    @Evaluator(extraName = "Doubles", warnExceptions = { ArithmeticException.class })
    static double processDoubles(double lhs, double rhs) {
        return NumericUtils.asFiniteNumber(lhs + rhs);
    }

    @Evaluator(extraName = "Datetimes", warnExceptions = { ArithmeticException.class, DateTimeException.class })
    static long processDatetimes(long datetime, @Fixed TemporalAmount temporalAmount) {
        // using a UTC conversion since `datetime` is always a UTC-Epoch timestamp, either read from ES or converted through a function
        return asMillis(asDateTime(datetime).plus(temporalAmount));
    }

    @Evaluator(extraName = "DateNanos", warnExceptions = { ArithmeticException.class, DateTimeException.class })
    static long processDateNanos(long dateNanos, @Fixed TemporalAmount temporalAmount) {
        // Instant.plus behaves differently from ZonedDateTime.plus, but DateUtils generally works with instants.
        try {
            return DateUtils.toLong(
                Instant.from(
                    ZonedDateTime.ofInstant(DateUtils.toInstant(dateNanos), org.elasticsearch.xpack.esql.core.util.DateUtils.UTC)
                        .plus(temporalAmount)
                )
            );
        } catch (IllegalArgumentException e) {
            /*
             toLong will throw IllegalArgumentException for out of range dates, but that includes the actual value which we want
             to avoid returning here.
            */
            throw new DateTimeException("Date nanos out of range.  Must be between 1970-01-01T00:00:00Z and 2262-04-11T23:47:16.854775807");
        }
    }

    @Override
    public Period fold(Period left, Period right) {
        return left.plus(right);
    }

    @Override
    public Duration fold(Duration left, Duration right) {
        return left.plus(right);
    }
}
