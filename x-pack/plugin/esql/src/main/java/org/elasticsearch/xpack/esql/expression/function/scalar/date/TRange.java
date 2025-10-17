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

    public static final String START_TIME_OR_INTERVAL_PARAM_NAME = "start_time_or_interval";
    public static final String END_TIME_PARAM_NAME = "end_time";

    private final Expression startTime;
    private final Expression endTime;
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
        @Param(name = START_TIME_OR_INTERVAL_PARAM_NAME, type = { "time_duration", "date_period", "keyword", "long" }, description = """
             Time interval or start time value for single parameter mode. Start time for two parameter mode.
             In two parameter mode, the start time value can be a date string or epoch milliseconds.
            """) Expression startTime,
        @Param(name = END_TIME_PARAM_NAME, type = { "keyword", "long" }, description = """
            Explicit end time that can be a date string or epoch milliseconds, or a modifier for the first parameter.
            The modifier can be a time duration or date period.""", optional = true) Expression endTime,
        Configuration configuration
    ) {
        this(source, new UnresolvedAttribute(source, MetadataAttribute.TIMESTAMP_FIELD), startTime, endTime, configuration);
    }

    public TRange(Source source, Expression timestamp, Expression startTime, Expression endTime, Configuration configuration) {
        super(source, endTime != null ? List.of(timestamp, startTime, endTime) : List.of(timestamp, startTime), configuration);
        this.timestamp = timestamp;
        this.startTime = startTime;
        this.endTime = endTime;
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
        return timestamp.foldable() && startTime.foldable() && (endTime == null || endTime.foldable());
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        String operationName = sourceText();

        TypeResolution resolution = isType(
            timestamp,
            dt -> dt == DataType.DATETIME || dt == DataType.DATE_NANOS,
            operationName,
            DEFAULT,
            true,
            "date_nanos",
            "datetime"
        );

        if (resolution.unresolved()) {
            return resolution;
        }

        // Single parameter mode
        if (endTime == null) {
            return isNotNull(startTime, operationName, FIRST).and(isFoldable(startTime, operationName, FIRST))
                .and(
                    isType(
                        startTime,
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
        resolution = isNotNull(startTime, operationName, FIRST).and(isFoldable(startTime, operationName, FIRST))
            .and(isNotNull(endTime, operationName, SECOND))
            .and(isFoldable(endTime, operationName, SECOND));

        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isType(startTime, dt -> dt == KEYWORD || dt == LONG, operationName, FIRST, "string", "long").and(
            isType(endTime, dt -> dt == startTime.dataType(), operationName, SECOND, startTime.dataType().esType())
        );

        if (resolution.unresolved()) {
            return resolution;
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
        return NodeInfo.create(this, TRange::new, timestamp, startTime, endTime, configuration());
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    public Expression getStartTime() {
        return startTime;
    }

    public Expression getEndTime() {
        return endTime;
    }

    private long[] getRange(FoldContext foldContext) {
        Instant end;
        Instant start;

        try {
            if (this.endTime == null) {
                end = configuration().now().toInstant();
                start = calculateStartTime(this.startTime.fold(foldContext), end, START_TIME_OR_INTERVAL_PARAM_NAME);
            } else {
                start = parseToInstant(this.startTime.fold(foldContext), START_TIME_OR_INTERVAL_PARAM_NAME);
                end = parseToInstant(this.endTime.fold(foldContext), END_TIME_PARAM_NAME);
            }
        } catch (InvalidArgumentException e) {
            throw new InvalidArgumentException(e, "invalid time range for [{}]: {}", sourceText(), e.getMessage());
        }

        if (start.isAfter(end)) {
            throw new InvalidArgumentException("TRANGE start time [{}] must be before end time [{}]", start, end);
        }

        boolean nanos = timestamp.dataType() == DataType.DATE_NANOS;
        return new long[] {
            nanos == false ? start.toEpochMilli() : DateUtils.toNanoSeconds(start.toEpochMilli()),
            nanos == false ? end.toEpochMilli() : DateUtils.toNanoSeconds(end.toEpochMilli()) };
    }

    private Instant calculateStartTime(Object value, Instant endOfRange, String paramName) {
        if (value instanceof Duration duration) {
            return endOfRange.minus(duration.abs());
        }
        if (value instanceof Period period) {
            period = period.isNegative() ? period.negated() : period;
            return endOfRange.minus(period);
        }
        return parseToInstant(value, paramName);
    }

    private Instant parseToInstant(Object value, String paramName) {
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
        if (startTime.resolved() && startTime.dataType() == DataType.NULL) {
            failures.add(fail(startTime, "{} cannot be null", START_TIME_OR_INTERVAL_PARAM_NAME));
        }

        // two parameter mode
        if (endTime != null) {
            if (endTime.resolved() && endTime.dataType() == DataType.NULL) {
                failures.add(fail(endTime, "{} cannot be null", END_TIME_PARAM_NAME));
            }

            if (comparableTypes(startTime, endTime) == false) {
                failures.add(fail(endTime, "{} must have the same type as {}", END_TIME_PARAM_NAME, START_TIME_OR_INTERVAL_PARAM_NAME));
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
