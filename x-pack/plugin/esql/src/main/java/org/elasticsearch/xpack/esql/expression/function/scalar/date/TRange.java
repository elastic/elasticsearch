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
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

public class TRange extends EsqlScalarFunction implements OptionalArgument, SurrogateExpression {
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
            @Example(file = "trange", tag = "docsTRangeInterval"),
            @Example(file = "trange", tag = "docsTRangeAbsolute"),
            @Example(file = "trange", tag = "docsTRangeEpochMillis") }
    )
    public TRange(
        Source source,
        @Param(
            name = START_TIME_OR_INTERVAL_PARAM_NAME,
            type = { "time_duration", "date_period", "keyword", "long", "integer" },
            description = """
                 Time interval (positive only) for single parameter mode, or start time for two parameter mode.
                 In two parameter mode, the start time value can be a date string or epoch milliseconds.
                """
        ) Expression startTime,
        @Param(
            name = END_TIME_PARAM_NAME,
            type = { "keyword", "long", "integer" },
            description = "End time for two parameter mode (optional). The end time value can be a date string or epoch milliseconds.",
            optional = true
        ) Expression endTime
    ) {
        this(source, new UnresolvedAttribute(source, MetadataAttribute.TIMESTAMP_FIELD), startTime, endTime);
    }

    public TRange(Source source, Expression timestamp, Expression startTime, Expression endTime) {
        super(source, endTime != null ? List.of(startTime, endTime, timestamp) : List.of(startTime, timestamp));
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
                .and(isType(startTime, DataType::isTemporalAmount, operationName, FIRST, "time_duration", "date_period"));
        }

        // Two parameter mode
        resolution = isNotNull(startTime, operationName, FIRST).and(isFoldable(startTime, operationName, FIRST))
            .and(isNotNull(endTime, operationName, SECOND))
            .and(isFoldable(endTime, operationName, SECOND));

        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isType(
            startTime,
            dt -> dt == KEYWORD || dt == LONG || dt == INTEGER,
            operationName,
            FIRST,
            "string",
            "long",
            "integer"
        ).and(isType(endTime, dt -> dt == startTime.dataType(), operationName, SECOND, startTime.dataType().esType()));

        if (resolution.unresolved()) {
            return resolution;
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() == 3) {
            return new TRange(source(), newChildren.get(2), newChildren.get(0), newChildren.get(1));
        }
        return new TRange(source(), newChildren.get(1), newChildren.get(0), null);
    }

    @Override
    public Expression surrogate() {
        long[] range = getRange(FoldContext.small());

        // Create literal expressions for the bounds based on timestamp data type
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
        return NodeInfo.create(this, TRange::new, timestamp, startTime, endTime);
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
        Instant endOfRange;
        Instant beginningOfRange;

        if (endTime == null) {
            // Single parameter mode - fold the startTime to get its value
            Object start = startTime.fold(foldContext);
            if (start == null) {
                throw new InvalidArgumentException("start_time_or_interval can not be null in [{}]", sourceText());
            }

            endOfRange = Instant.now();
            beginningOfRange = calculateStartTime(start, endOfRange, START_TIME_OR_INTERVAL_PARAM_NAME);
        } else {
            // Two parameter mode - fold both startTime and endTime to get their values
            Object start = startTime.fold(foldContext);
            if (start == null) {
                throw new InvalidArgumentException("start_time_or_interval can not be null in [{}]", sourceText());
            }

            Object end = endTime.fold(foldContext);
            if (end == null) {
                throw new InvalidArgumentException("end_time can not be null in [{}]", sourceText());
            }

            if (start.getClass() != end.getClass()) {
                throw new InvalidArgumentException("TRANGE start_time_or_interval and end_time parameters must be of the same type.");
            }

            try {
                beginningOfRange = parseToInstant(start, START_TIME_OR_INTERVAL_PARAM_NAME);
                endOfRange = parseToInstant(end, END_TIME_PARAM_NAME);
            } catch (InvalidArgumentException e) {
                throw new InvalidArgumentException(e, "invalid time range for [{}]: {}", sourceText(), e.getMessage());
            }
        }

        if (beginningOfRange.isAfter(endOfRange) || beginningOfRange.equals(endOfRange)) {
            throw new InvalidArgumentException("TRANGE start time [{}] must be before end time [{}]", beginningOfRange, endOfRange);
        }

        long beginningOfRangeValue = beginningOfRange.toEpochMilli();
        long endOfRangeValue = endOfRange.toEpochMilli();

        if (timestamp.dataType() == DataType.DATE_NANOS) {
            beginningOfRangeValue = DateUtils.toNanoSeconds(beginningOfRangeValue);
            endOfRangeValue = DateUtils.toNanoSeconds(endOfRangeValue);
        }

        return new long[] { beginningOfRangeValue, endOfRangeValue };
    }

    private Instant calculateStartTime(Object value, Instant endOfRange, String paramName) {
        if (value instanceof Duration duration) {
            return endOfRange.minus(duration.abs());
        }
        if (value instanceof Period period) {
            period = period.isNegative() ? period.negated() : period;
            return endOfRange.minus(period);
        }
        throw new InvalidArgumentException(
            "Unsupported time value type [{}] for parameter [{}]",
            value.getClass().getSimpleName(),
            paramName
        );
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

        if (value instanceof Integer intValue) {
            return Instant.ofEpochMilli(intValue.longValue());
        }

        throw new InvalidArgumentException(
            "Unsupported time value type [{}] for parameter [{}]",
            value.getClass().getSimpleName(),
            paramName
        );
    }
}
