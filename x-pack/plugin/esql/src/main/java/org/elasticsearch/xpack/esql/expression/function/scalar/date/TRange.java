/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.List;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.isTemporalAmount;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.commonType;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

/**
 * In a single-parameter mode, the function always uses the current time as the end of the range.
 * <br/>
 * Supported single parameter mode formats:
 * <ul>
 *     <li>TRANGE(1h) - [now - 1h; now] - supports time_duration (1h, 1min, etc) and period (1day, 1month, etc.)</li>
 *     <li>TRANGE(2024-05-12T12:00:00) - [explicit start; now]</li>
 *     <li>TRANGE(1715504400000) - [explicit start in millis; now]</li>
 * </ul>
 * Supported two parameter mode formats:
 * <ul>
 *     <li>TRANGE(2024-05-12T12:00:00, 2024-05-12T15:30:00) - [explicit start; explicit end]</li>
 *     <li>TRANGE(1715504400000, 1715517000000) - [explicit start in millis; explicit end in millis]</li>
 *     <li>TRANGE(2024-05-12T12:00:00, 1h) - [explicit start; start + 1h] - supports time_duration and period</li>
 *     <li>TRANGE(2024-05-12T12:00:00, -1h) - [explicit start; start - 1h] - supports time_duration and period</li>
 *     <li>TRANGE(1715504400000, 1h) - [explicit start in millis; start + 1h] - supports time_duration and period</li>
 *     <li>TRANGE(1715504400000, -1h) - [explicit start in millis; start - 1h] - supports time_duration and period</li>
 * </ul>
 */
public class TRange extends EsqlConfigurationFunction implements OptionalArgument, SurrogateExpression, PostAnalysisVerificationAware {
    public static final String NAME = "TRange";

    public static final String START_TIME_OR_OFFSET_PARAMETER = "start_time_or_offset";
    public static final String END_TIME_OR_OFFSET_PARAMETER = "end_time_or_offset";

    private final Expression first;
    private final Expression second;
    private final Expression timestamp;

    @FunctionInfo(
        returnType = "boolean",
        description = "Filters time-series data for the given time range.",
        examples = {
            @Example(file = "trange", tag = "docsTRangeOffsetFromNow"),
            @Example(file = "trange", tag = "docsTRangeFromExplicitTimeString"),
            @Example(file = "trange", tag = "docsTRangeFromExplicitTimeEpochMillis"),
            @Example(file = "trange", tag = "docsTRangeAbsoluteTimeString"),
            @Example(file = "trange", tag = "docsTRangeAbsoluteTimeEpochMillis") }
    )
    public TRange(
        Source source,
        @Param(name = START_TIME_OR_OFFSET_PARAMETER, type = { "time_duration", "date_period", "keyword", "long" }, description = """
             Time interval or start time value for single parameter mode. Start time for two parameter mode.
             In two parameter mode, the start time value can be a date string or epoch milliseconds.
            """) Expression first,
        @Param(name = END_TIME_OR_OFFSET_PARAMETER, type = { "time_duration", "date_period", "keyword", "long" }, description = """
            Explicit end time that can be a date string or epoch milliseconds, or a modifier for the first parameter (offset).
            The offset can be a time duration or date period and can be either positive or negative.""", optional = true) Expression second,
        Configuration configuration
    ) {
        this(source, new UnresolvedAttribute(source, MetadataAttribute.TIMESTAMP_FIELD), first, second, configuration);
    }

    public TRange(Source source, Expression timestamp, Expression first, Expression second, Configuration configuration) {
        super(source, second != null ? List.of(timestamp, first, second) : List.of(timestamp, first), configuration);
        this.timestamp = timestamp;
        this.first = first;
        this.second = second;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        throw new UnsupportedOperationException("should be rewritten");
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public boolean foldable() {
        return timestamp.foldable() && first.foldable() && (second == null || second.foldable());
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        String operationName = sourceText();
        TypeResolution resolution = isType(timestamp, DataType::isMillisOrNanos, operationName, DEFAULT, true, "date_nanos", "datetime");

        if (resolution.unresolved()) {
            return resolution;
        }

        // Single parameter mode
        if (second == null) {
            return isNotNull(first, operationName, FIRST).and(isFoldable(first, operationName, FIRST))
                .and(
                    isType(
                        first,
                        dt -> isTemporalAmount(dt) || dt == KEYWORD || dt == LONG,
                        operationName,
                        FIRST,
                        "time_duration",
                        "date_period",
                        "string",
                        "long"
                    )
                );
        }

        // Two parameter mode
        resolution = isNotNull(first, operationName, FIRST).and(isFoldable(first, operationName, FIRST))
            .and(isNotNull(second, operationName, SECOND))
            .and(isFoldable(second, operationName, SECOND));

        if (resolution.unresolved()) {
            return resolution;
        }

        // the 2nd parameter is an absolute time in string or long format: (<string>, <string>) or (<long>, <long>)
        if (isTemporalAmount(second.dataType()) == false) {
            resolution = isType(first, dt -> dt == KEYWORD || dt == LONG, operationName, FIRST, "string", "long").and(
                isType(second, dt -> dt == first.dataType(), operationName, SECOND, first.dataType().esType())
            );

            if (resolution.unresolved()) {
                return resolution;
            }
        } else {
            // the 2nd parameter is a temporal amount (offset): (<string|long>, <time_duration|date_period>)
            resolution = isType(first, dt -> dt == KEYWORD || dt == LONG, operationName, FIRST, "string", "long").and(
                isType(second, DataType::isTemporalAmount, operationName, SECOND, "time_duration", "date_period")
            );

            if (resolution.unresolved()) {
                return resolution;
            }
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() == 3) {
            return new TRange(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), configuration());
        }
        return new TRange(source(), newChildren.get(0), newChildren.get(1), null, configuration());
    }

    @Override
    public Expression surrogate() {
        long[] range = getRange(FoldContext.small());

        Expression startLiteral;
        Expression endLiteral;

        if (timestamp.dataType() == DataType.DATE_NANOS) {
            startLiteral = new Literal(source(), range[0], DataType.DATE_NANOS);
            endLiteral = new Literal(source(), range[1], DataType.DATE_NANOS);
        } else {
            startLiteral = new Literal(source(), range[0], DataType.DATETIME);
            endLiteral = new Literal(source(), range[1], DataType.DATETIME);
        }

        return new And(
            source(),
            new GreaterThanOrEqual(source(), timestamp, startLiteral),
            new LessThanOrEqual(source(), timestamp, endLiteral)
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, TRange::new, timestamp, first, second, configuration());
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    private long[] getRange(FoldContext foldContext) {
        Instant rangeStart;
        Instant rangeEnd;

        try {
            Object foldFirst = first.fold(foldContext);
            if (second == null) {
                rangeEnd = configuration().now().toInstant();
                if (DataType.isTemporalAmount(first.dataType())) {
                    rangeStart = timeWithOffset(foldFirst, rangeEnd);
                } else {
                    rangeStart = parseToInstant(foldFirst, START_TIME_OR_OFFSET_PARAMETER);
                }
            } else {
                Object foldSecond = second.fold(foldContext);
                if (DataType.isTemporalAmount(second.dataType())) {
                    // offset for base time
                    Instant base = parseToInstant(first, START_TIME_OR_OFFSET_PARAMETER);
                    Instant withOffset = timeWithOffset(foldSecond, base);

                    if (base.isBefore(withOffset)) {
                        rangeStart = base;
                        rangeEnd = withOffset;
                    } else {
                        rangeStart = withOffset;
                        rangeEnd = base;
                    }
                } else {
                    // absolute time
                    rangeStart = parseToInstant(foldFirst, START_TIME_OR_OFFSET_PARAMETER);
                    rangeEnd = parseToInstant(foldSecond, END_TIME_OR_OFFSET_PARAMETER);
                }
            }
        } catch (InvalidArgumentException e) {
            throw new InvalidArgumentException(e, "invalid time range for [{}]: {}", sourceText(), e.getMessage());
        }

        if (rangeStart.isAfter(rangeEnd)) {
            throw new InvalidArgumentException("TRANGE rangeStart time [{}] must be before rangeEnd time [{}]", rangeStart, rangeEnd);
        }

        boolean nanos = timestamp.dataType() == DataType.DATE_NANOS;
        return new long[] {
            nanos == false ? rangeStart.toEpochMilli() : DateUtils.toNanoSeconds(rangeStart.toEpochMilli()),
            nanos == false ? rangeEnd.toEpochMilli() : DateUtils.toNanoSeconds(rangeEnd.toEpochMilli()) };
    }

    private Instant timeWithOffset(Object offset, Instant base) {
        if (offset instanceof TemporalAmount amount) {
            return base.minus(amount);
        }
        throw new InvalidArgumentException("Unsupported offset type [{}]", offset.getClass().getSimpleName());
    }

    private Instant parseToInstant(Object value, String paramName) {
        if (value instanceof Literal literal) {
            value = literal.fold(FoldContext.small());
        }

        if (value instanceof BytesRef bytesRef) {
            try {
                long millis = dateTimeToLong(bytesRef.utf8ToString());
                return Instant.ofEpochMilli(millis);
            } catch (Exception e) {
                throw new InvalidArgumentException("TRANGE {} parameter must be a valid datetime string, got: {}", paramName, value);
            }
        }

        if (value instanceof Long longValue) {
            return Instant.ofEpochMilli(longValue);
        }

        throw new InvalidArgumentException(
            "Unsupported time value type [{}] for parameter [{}]",
            value.getClass().getSimpleName(),
            paramName
        );
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (first.resolved() && first.dataType() == DataType.NULL) {
            failures.add(fail(first, "{} cannot be null", START_TIME_OR_OFFSET_PARAMETER));
        }

        // single parameter mode
        if (second == null && DataType.isTemporalAmount(first.dataType())) {
            Object rangeStartValue = first.fold(FoldContext.small());
            if (rangeStartValue instanceof Duration duration && duration.isNegative()) {
                failures.add(fail(first, "{} cannot be negative", START_TIME_OR_OFFSET_PARAMETER));
            }

            if (rangeStartValue instanceof Period period && period.isNegative()) {
                failures.add(fail(first, "{} cannot be negative", START_TIME_OR_OFFSET_PARAMETER));
            }
        }

        // two parameter mode
        if (second != null) {
            if (second.resolved() && second.dataType() == DataType.NULL) {
                failures.add(fail(second, "{} cannot be null", END_TIME_OR_OFFSET_PARAMETER));
            }

            if (comparableTypes(first, second) == false) {
                failures.add(
                    fail(second, "{} must have the same type as {}", END_TIME_OR_OFFSET_PARAMETER, START_TIME_OR_OFFSET_PARAMETER)
                );
            }
        }
    }

    private static boolean comparableTypes(Expression left, Expression right) {
        DataType leftType = left.dataType();
        DataType rightType = right.dataType();
        if (leftType.isNumeric() && rightType.isNumeric()) {
            return commonType(leftType, rightType) != null;
        }
        return leftType.noText() == rightType.noText();
    }
}
