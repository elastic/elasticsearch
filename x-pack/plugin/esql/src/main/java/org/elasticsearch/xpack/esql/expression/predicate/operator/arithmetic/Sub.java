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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.BinaryComparisonInversible;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.type.DateUtils.asDateTime;
import static org.elasticsearch.xpack.esql.core.type.DateUtils.asMillis;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongSubtractExact;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation.OperationSymbol.SUB;

public class Sub extends DateTimeArithmeticOperation implements BinaryComparisonInversible {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Sub", Sub::new);

    @FunctionInfo(
        returnType = { "double", "integer", "long", "date_period", "datetime", "time_duration", "unsigned_long" },
        description = "Subtract one number from another. "
            + "If either field is <<esql-multivalued-fields,multivalued>> then the result is `null`."
    )
    public Sub(
        Source source,
        @Param(
            name = "lhs",
            description = "A numeric value or a date time value.",
            type = { "double", "integer", "long", "date_period", "datetime", "time_duration", "unsigned_long" }
        ) Expression left,
        @Param(
            name = "rhs",
            description = "A numeric value or a date time value.",
            type = { "double", "integer", "long", "date_period", "datetime", "time_duration", "unsigned_long" }
        ) Expression right
    ) {
        super(
            source,
            left,
            right,
            SUB,
            SubIntsEvaluator.Factory::new,
            SubLongsEvaluator.Factory::new,
            SubUnsignedLongsEvaluator.Factory::new,
            SubDoublesEvaluator.Factory::new,
            SubDatetimesEvaluator.Factory::new
        );
    }

    private Sub(StreamInput in) throws IOException {
        super(
            in,
            SUB,
            SubIntsEvaluator.Factory::new,
            SubLongsEvaluator.Factory::new,
            SubUnsignedLongsEvaluator.Factory::new,
            SubDoublesEvaluator.Factory::new,
            SubDatetimesEvaluator.Factory::new
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = super.resolveType();
        // As opposed to general date time arithmetics, we cannot subtract a datetime from something else.
        if (resolution.resolved() && DataType.isDateTimeOrTemporal(dataType()) && DataType.isDateTime(right().dataType())) {
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

    @Evaluator(extraName = "Doubles", warnExceptions = { ArithmeticException.class })
    static double processDoubles(double lhs, double rhs) {
        return NumericUtils.asFiniteNumber(lhs - rhs);
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
