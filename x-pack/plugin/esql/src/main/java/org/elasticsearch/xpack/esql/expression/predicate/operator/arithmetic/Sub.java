/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.capabilities.NonFiniteSupport;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneId;
import java.time.temporal.TemporalAmount;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.util.DateUtils.asDateTime;
import static org.elasticsearch.xpack.esql.core.util.DateUtils.asMillis;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongSubtractExact;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation.OperationSymbol.SUB;

public class Sub extends DateTimeArithmeticOperation implements BinaryComparisonInversible, NonFiniteSupport {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Sub", Sub::new);
    public static final String OP_NAME = "Sub";

    private final Configuration configuration;

    /**
     * When {@code true}, a non-finite scalar difference ({@code NaN}/{@code ±Inf}) is returned as-is instead of being
     * rejected to {@code null}. Set only by the PromQL translation so that arithmetic follows IEEE-754; the default
     * is {@code false}, preserving ES|QL's finite-only contract. Only the scalar double path honors this flag.
     */
    private final boolean allowNonFinite;

    @FunctionInfo(
        operator = "-",
        returnType = { "double", "integer", "long", "date_period", "datetime", "time_duration", "unsigned_long", "dense_vector" },
        description = """
            Subtract one value from another. In case of numeric fields, if either field is <<esql-multivalued-fields,multivalued>>
            then the result is `null`. For dense_vector fields, both arguments should be dense_vectors. Inequal vector dimensions generate
            null result.
            """
    )
    public Sub(
        Source source,
        @Param(
            name = "lhs",
            description = "A numeric value, dense_vector or a date time value.",
            type = { "double", "integer", "long", "date_period", "datetime", "time_duration", "unsigned_long", "dense_vector" }
        ) Expression left,
        @Param(
            name = "rhs",
            description = "A numeric value, dense_vector or a date time value.",
            type = { "double", "integer", "long", "date_period", "datetime", "time_duration", "unsigned_long", "dense_vector" }
        ) Expression right,
        Configuration configuration
    ) {
        this(source, left, right, configuration, false);
    }

    public Sub(Source source, Expression left, Expression right, Configuration configuration, boolean allowNonFinite) {
        super(
            source,
            left,
            right,
            SUB,
            SubIntsEvaluator.Factory::new,
            SubLongsEvaluator.Factory::new,
            SubUnsignedLongsEvaluator.Factory::new,
            (s, lhs, rhs) -> new SubDoublesEvaluator.Factory(s, lhs, rhs, allowNonFinite),
            SUB_DENSE_VECTOR_EVALUATOR,
            SubDatetimesEvaluator.Factory::new,
            SubDateNanosEvaluator.Factory::new
        );
        this.configuration = configuration;
        this.allowNonFinite = allowNonFinite;
    }

    private Sub(StreamInput in) throws IOException {
        // Children are serialized by BinaryScalarFunction#writeTo (source, left, right); the non-finite flag, when
        // present, follows them, so read it last to match. Configuration is read from the PlanStreamInput context,
        // not the byte stream. Bypassing the base StreamInput constructor lets the flag reach the double-evaluator factory.
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            ((PlanStreamInput) in).configuration(),
            NonFiniteSupport.readNonFinite(in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeNonFinite(out);
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
        return NodeInfo.create(this, Sub::new, left(), right(), configuration, allowNonFinite);
    }

    @Override
    public boolean allowNonFinite() {
        return allowNonFinite;
    }

    @Override
    public Expression toStrictVariant() {
        return new Sub(source(), left(), right(), configuration, false);
    }

    @Override
    protected Sub replaceChildren(Expression left, Expression right) {
        return new Sub(source(), left, right, configuration, allowNonFinite);
    }

    @Override
    public ArithmeticOperationFactory binaryComparisonInverse() {
        return (source, left, right) -> new Add(source, left, right, configuration);
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
    static double processDoubles(double lhs, double rhs, @Fixed(includeInToString = false) boolean allowNonFinite) {
        double result = lhs - rhs;
        return allowNonFinite ? result : NumericUtils.asFiniteNumber(result);
    }

    @Evaluator(extraName = "Datetimes", warnExceptions = { ArithmeticException.class, DateTimeException.class })
    static long processDatetimes(long datetime, @Fixed TemporalAmount temporalAmount, @Fixed ZoneId zoneId) {
        // using a UTC conversion since `datetime` is always a UTC-Epoch timestamp, either read from ES or converted through a function
        return asMillis(asDateTime(datetime, zoneId).minus(temporalAmount));
    }

    @Evaluator(extraName = "DateNanos", warnExceptions = { ArithmeticException.class, DateTimeException.class })
    static long processDateNanos(long dateNanos, @Fixed TemporalAmount temporalAmount, @Fixed ZoneId zoneId) {
        // Instant.plus behaves differently from ZonedDateTime.plus, but DateUtils generally works with instants.
        try {
            return DateUtils.toLong(Instant.from(asDateTime(DateUtils.toInstant(dateNanos), zoneId).minus(temporalAmount)));
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
        return left.minus(right);
    }

    @Override
    public Duration fold(Duration left, Duration right) {
        return left.minus(right);
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    @Override
    public Sub withConfiguration(Configuration configuration) {
        return new Sub(source(), left(), right(), configuration, allowNonFinite);
    }

    private static float subDenseVectorElements(float lhs, float rhs) {
        return NumericUtils.asFiniteNumber(lhs - rhs);
    }

    private static final DenseVectorBinaryEvaluator SUB_DENSE_VECTOR_EVALUATOR = new DenseVectorBinaryEvaluator() {
        @Override
        public ExpressionEvaluator.Factory vectorsOperation(
            Source source,
            ExpressionEvaluator.Factory lhs,
            ExpressionEvaluator.Factory rhs
        ) {
            return new DenseVectorsEvaluator.Factory(source, lhs, rhs, Sub::subDenseVectorElements, OP_NAME);
        }

        @Override
        public ExpressionEvaluator.Factory scalarVectorOperation(Source source, float lhs, ExpressionEvaluator.Factory rhs) {
            return new DenseVectorScalarEvaluator.Factory(source, lhs, rhs, Sub::subDenseVectorElements, OP_NAME);
        }

        @Override
        public ExpressionEvaluator.Factory vectorScalarOperation(Source source, ExpressionEvaluator.Factory lhs, float rhs) {
            return new DenseVectorScalarEvaluator.Factory(source, lhs, rhs, Sub::subDenseVectorElements, OP_NAME);
        }
    };
}
