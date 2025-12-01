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
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.isMillisOrNanos;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

/**
 * In a single-parameter mode, the function always uses the current time as the end of the range.
 * <br/>
 * Supported single parameter mode formats:
 * <ul>
 *     <li>TRANGE(1h) - [now - 1h; now] - supports time_duration (1h, 1min, etc) and period (1day, 1month, etc.)</li>
 * </ul>
 * Supported two parameter mode formats:
 * <ul>
 *     <li>TRANGE(2024-05-12T12:00:00, 2024-05-12T15:30:00) - [explicit start; explicit end]</li>
 *     <li>TRANGE(1715504400000, 1715517000000) - [explicit start in millis; explicit end in millis]</li>
 * </ul>
 */
public class TRange extends EsqlConfigurationFunction
    implements
        OptionalArgument,
        SurrogateExpression,
        PostAnalysisPlanVerificationAware,
        TimestampAware {
    public static final String NAME = "TRange";

    public static final String START_TIME_OR_OFFSET_PARAMETER = "start_time_or_offset";
    public static final String END_TIME_PARAMETER = "end_time";

    private final Expression first;
    private final Expression second;
    private final Expression timestamp;

    @FunctionInfo(
        returnType = "boolean",
        description = "Filters data for the given time range using the @timestamp attribute.",
        examples = {
            @Example(file = "trange", tag = "docsTRangeOffsetFromNow"),
            @Example(file = "trange", tag = "docsTRangeAbsoluteTimeString"),
            @Example(file = "trange", tag = "docsTRangeAbsoluteTimeDateTime"),
            @Example(file = "trange", tag = "docsTRangeAbsoluteTimeDateTimeNanos"),
            @Example(file = "trange", tag = "docsTRangeAbsoluteTimeEpochMillis") }
    )
    public TRange(
        Source source,
        @Param(
            name = START_TIME_OR_OFFSET_PARAMETER,
            type = { "time_duration", "date_period", "date", "date_nanos", "keyword", "long" },
            description = """
                 Offset from NOW for the single parameter mode. Start time for two parameter mode.
                 In two parameter mode, the start time value can be a date string, date, date_nanos or epoch milliseconds.
                """
        ) Expression first,
        @Param(name = END_TIME_PARAMETER, type = { "keyword", "long", "date", "date_nanos" }, description = """
            Explicit end time that can be a date string, date, date_nanos or epoch milliseconds.""", optional = true) Expression second,
        Expression timestamp,
        Configuration configuration
    ) {
        super(source, second != null ? List.of(first, second, timestamp) : List.of(first, timestamp), configuration);
        this.first = first;
        this.second = second;
        this.timestamp = timestamp;
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
    public Expression timestamp() {
        return timestamp;
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
        TypeResolution resolution = isType(timestamp, DataType::isMillisOrNanos, operationName, DEFAULT, true, "date_nanos", "date");

        if (resolution.unresolved()) {
            return resolution;
        }

        // Single parameter mode
        if (second == null) {
            return isNotNull(first, operationName, FIRST).and(isFoldable(first, operationName, FIRST))
                .and(isType(first, DataType::isTemporalAmount, operationName, FIRST, "time_duration", "date_period"));
        }

        // Two parameter mode
        resolution = isNotNull(first, operationName, FIRST).and(isFoldable(first, operationName, FIRST))
            .and(isNotNull(second, operationName, SECOND))
            .and(isFoldable(second, operationName, SECOND));

        if (resolution.unresolved()) {
            return resolution;
        }

        // the 2nd parameter has the same type as the 1st, which can be string (datetime), long (epoch millis), date or date_nanos
        resolution = isType(
            first,
            dt -> isMillisOrNanos(dt) || dt == KEYWORD || dt == LONG,
            operationName,
            FIRST,
            "string",
            "long",
            "date",
            "date_nanos"
        ).and(isType(second, dt -> dt == first.dataType(), operationName, SECOND, first.dataType().esType()));

        if (resolution.unresolved()) {
            return resolution;
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new TRange(
            source(),
            newChildren.getFirst(),
            newChildren.size() == 3 ? newChildren.get(1) : null,
            newChildren.getLast(),
            configuration()
        );
    }

    @Override
    public Expression surrogate() {
        long[] range = getRange(FoldContext.small());

        Expression startLiteral = new Literal(source(), range[0], timestamp.dataType());
        Expression endLiteral = new Literal(source(), range[1], timestamp.dataType());

        return new And(source(), new GreaterThan(source(), timestamp, startLiteral), new LessThanOrEqual(source(), timestamp, endLiteral));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, TRange::new, first, second, timestamp, configuration());
    }

    @Override
    public Nullability nullable() {
        return timestamp.nullable();
    }

    private long[] getRange(FoldContext foldContext) {
        Instant rangeStart;
        Instant rangeEnd;

        try {
            Object foldFirst = first.fold(foldContext);
            if (second == null) {
                rangeEnd = configuration().now().toInstant();
                rangeStart = timeWithOffset(foldFirst, rangeEnd);
            } else {
                Object foldSecond = second.fold(foldContext);
                rangeStart = parseToInstant(foldFirst, START_TIME_OR_OFFSET_PARAMETER);
                rangeEnd = parseToInstant(foldSecond, END_TIME_PARAMETER);
            }
        } catch (InvalidArgumentException e) {
            throw new InvalidArgumentException(e, "invalid time range for [{}]: {}", sourceText(), e.getMessage());
        }

        if (rangeStart.isAfter(rangeEnd)) {
            throw new InvalidArgumentException("TRANGE rangeStart time [{}] must be before rangeEnd time [{}]", rangeStart, rangeEnd);
        }

        if (timestamp.dataType() == DataType.DATE_NANOS && first.dataType() == DataType.DATE_NANOS) {
            return new long[] { DateUtils.toLong(rangeStart), DateUtils.toLong(rangeEnd) };
        }

        boolean convertToNanos = timestamp.dataType() == DataType.DATE_NANOS;
        return new long[] {
            convertToNanos ? DateUtils.toNanoSeconds(rangeStart.toEpochMilli()) : rangeStart.toEpochMilli(),
            convertToNanos ? DateUtils.toNanoSeconds(rangeEnd.toEpochMilli()) : rangeEnd.toEpochMilli() };
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

        if (value instanceof Instant instantValue) {
            return instantValue;
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
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return (logicalPlan, failures) -> {
            // single parameter mode
            if (second == null) {
                Object rangeStartValue = first.fold(FoldContext.small());
                if (rangeStartValue instanceof Duration duration && duration.isNegative()
                    || rangeStartValue instanceof Period period && period.isNegative()) {
                    failures.add(fail(first, "{} cannot be negative", START_TIME_OR_OFFSET_PARAMETER));
                }
            }

            // two parameter mode
            if (second != null) {
                Object rangeEndValue = second.fold(FoldContext.small());
                if (rangeEndValue == null) {
                    failures.add(fail(second, "{} cannot be null", END_TIME_PARAMETER));
                }
            }
        };
    }
}
