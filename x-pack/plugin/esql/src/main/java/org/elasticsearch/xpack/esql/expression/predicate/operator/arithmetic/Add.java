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

import static org.elasticsearch.xpack.esql.core.util.DateUtils.asDateTime;
import static org.elasticsearch.xpack.esql.core.util.DateUtils.asMillis;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongAddExact;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.EsqlArithmeticOperation.OperationSymbol.ADD;

public class Add extends DateTimeArithmeticOperation implements BinaryComparisonInversible, NonFiniteSupport {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Add", Add::new);
    public static final String OP_NAME = "Add";

    private final Configuration configuration;

    /**
     * When {@code true}, a non-finite scalar sum ({@code NaN}/{@code ±Inf}) is returned as-is instead of being
     * rejected to {@code null}. Set only by the PromQL translation so that arithmetic follows IEEE-754; the default
     * is {@code false}, preserving ES|QL's finite-only contract. Only the scalar double path honors this flag.
     */
    private final boolean allowNonFinite;

    @FunctionInfo(
        operator = "+",
        returnType = {
            "double",
            "integer",
            "long",
            "date_nanos",
            "date_period",
            "datetime",
            "time_duration",
            "unsigned_long",
            "dense_vector" },
        description = """
            Add two values. In case of numeric fields, if either field is <<esql-multivalued-fields,multivalued>> then the result is `null`.
            For dense_vector operations, both arguments should be dense_vectors. Inequal vector dimensions generate null result.
            """
    )
    public Add(
        Source source,
        @Param(
            name = "lhs",
            description = "A numeric value, dense_vector or a date time value.",
            type = {
                "double",
                "integer",
                "long",
                "date_nanos",
                "date_period",
                "datetime",
                "time_duration",
                "unsigned_long",
                "dense_vector" }
        ) Expression left,
        @Param(
            name = "rhs",
            description = "A numeric value, dense_vector or a date time value.",
            type = {
                "double",
                "integer",
                "long",
                "date_nanos",
                "date_period",
                "datetime",
                "time_duration",
                "unsigned_long",
                "dense_vector" }
        ) Expression right,
        Configuration configuration
    ) {
        this(source, left, right, configuration, false);
    }

    public Add(Source source, Expression left, Expression right, Configuration configuration, boolean allowNonFinite) {
        super(
            source,
            left,
            right,
            ADD,
            AddIntsEvaluator.Factory::new,
            AddLongsEvaluator.Factory::new,
            AddUnsignedLongsEvaluator.Factory::new,
            (s, lhs, rhs) -> new AddDoublesEvaluator.Factory(s, lhs, rhs, allowNonFinite),
            ADD_DENSE_VECTOR_EVALUATOR,
            AddDatetimesEvaluator.Factory::new,
            AddDateNanosEvaluator.Factory::new
        );
        this.configuration = configuration;
        this.allowNonFinite = allowNonFinite;
    }

    private Add(StreamInput in) throws IOException {
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
    protected NodeInfo<Add> info() {
        return NodeInfo.create(this, Add::new, left(), right(), configuration, allowNonFinite);
    }

    @Override
    public boolean allowNonFinite() {
        return allowNonFinite;
    }

    @Override
    public Expression toStrictVariant() {
        return new Add(source(), left(), right(), configuration, false);
    }

    @Override
    protected Add replaceChildren(Expression left, Expression right) {
        return new Add(source(), left, right, configuration, allowNonFinite);
    }

    @Override
    public Add swapLeftAndRight() {
        return new Add(source(), right(), left(), configuration, allowNonFinite);
    }

    @Override
    public ArithmeticOperationFactory binaryComparisonInverse() {
        return (source, left, right) -> new Sub(source, left, right, configuration);
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
    static double processDoubles(double lhs, double rhs, @Fixed(includeInToString = false) boolean allowNonFinite) {
        double result = lhs + rhs;
        return allowNonFinite ? result : NumericUtils.asFiniteNumber(result);
    }

    @Evaluator(extraName = "Datetimes", warnExceptions = { ArithmeticException.class, DateTimeException.class })
    static long processDatetimes(long datetime, @Fixed TemporalAmount temporalAmount, @Fixed ZoneId zoneId) {
        // using a UTC conversion since `datetime` is always a UTC-Epoch timestamp, either read from ES or converted through a function
        return asMillis(asDateTime(datetime, zoneId).plus(temporalAmount));
    }

    @Evaluator(extraName = "DateNanos", warnExceptions = { ArithmeticException.class, DateTimeException.class })
    static long processDateNanos(long dateNanos, @Fixed TemporalAmount temporalAmount, @Fixed ZoneId zoneId) {
        // Instant.plus behaves differently from ZonedDateTime.plus, but DateUtils generally works with instants.
        try {
            return DateUtils.toLong(Instant.from(asDateTime(DateUtils.toInstant(dateNanos), zoneId).plus(temporalAmount)));
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

    @Override
    public Configuration configuration() {
        return configuration;
    }

    @Override
    public Add withConfiguration(Configuration configuration) {
        return new Add(source(), left(), right(), configuration, allowNonFinite);
    }

    private static float addDenseVectorElements(float lhs, float rhs) {
        return NumericUtils.asFiniteNumber(lhs + rhs);
    }

    private static final DenseVectorBinaryEvaluator ADD_DENSE_VECTOR_EVALUATOR = new DenseVectorBinaryEvaluator() {
        @Override
        public ExpressionEvaluator.Factory vectorsOperation(
            Source source,
            ExpressionEvaluator.Factory lhs,
            ExpressionEvaluator.Factory rhs
        ) {
            return new DenseVectorsEvaluator.Factory(source, lhs, rhs, Add::addDenseVectorElements, OP_NAME);
        }

        @Override
        public ExpressionEvaluator.Factory scalarVectorOperation(Source source, float lhs, ExpressionEvaluator.Factory rhs) {
            return new DenseVectorScalarEvaluator.Factory(source, lhs, rhs, Add::addDenseVectorElements, OP_NAME);
        }

        @Override
        public ExpressionEvaluator.Factory vectorScalarOperation(Source source, ExpressionEvaluator.Factory lhs, float rhs) {
            return new DenseVectorScalarEvaluator.Factory(source, lhs, rhs, Add::addDenseVectorElements, OP_NAME);
        }
    };
}
