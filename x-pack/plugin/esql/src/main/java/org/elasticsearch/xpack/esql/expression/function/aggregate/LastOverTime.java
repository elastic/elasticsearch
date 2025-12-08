/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LastBytesRefByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LastDoubleByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LastFloatByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LastIntByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LastLongByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class LastOverTime extends TimeSeriesAggregateFunction implements OptionalArgument, ToAggregator, TimestampAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "LastOverTime",
        LastOverTime::new
    );

    private final Expression timestamp;

    // TODO: support all types
    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "long", "integer", "double", "_tsid" },
        description = "Calculates the latest value of a field, where recency determined by the `@timestamp` field.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0") },
        preview = true,
        examples = { @Example(file = "k8s-timeseries", tag = "last_over_time") }
    )
    public LastOverTime(
        Source source,
        @Param(
            name = "field",
            type = { "counter_long", "counter_integer", "counter_double", "long", "integer", "double", "_tsid" },
            description = "the field to calculate the latest value for"
        ) Expression field,
        @Param(
            name = "window",
            type = { "time_duration" },
            description = "the time window over which to find the latest value",
            optional = true
        ) Expression window,
        Expression timestamp
    ) {
        this(source, field, Literal.TRUE, Objects.requireNonNullElse(window, NO_WINDOW), timestamp);
    }

    public LastOverTime(Source source, Expression field, Expression filter, Expression window, Expression timestamp) {
        super(source, field, filter, window, List.of(timestamp));
        this.timestamp = timestamp;
    }

    public LastOverTime(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            readWindow(in),
            in.readNamedWriteableCollectionAsList(Expression.class).getFirst()
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<LastOverTime> info() {
        return NodeInfo.create(this, LastOverTime::new, field(), filter(), window(), timestamp);
    }

    @Override
    public LastOverTime replaceChildren(List<Expression> newChildren) {
        return new LastOverTime(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }

    @Override
    public LastOverTime withFilter(Expression filter) {
        return new LastOverTime(source(), field(), filter, window(), timestamp);
    }

    @Override
    public DataType dataType() {
        return field().dataType().noCounter();
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(
            field(),
            dt -> (dt.noCounter().isNumeric() && dt != DataType.UNSIGNED_LONG) || dt == DataType.TSID_DATA_TYPE,
            sourceText(),
            DEFAULT,
            "numeric except unsigned_long"
        ).and(
            isType(timestamp, dt -> dt == DataType.DATETIME || dt == DataType.DATE_NANOS, sourceText(), SECOND, "date_nanos or datetime")
        );
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        // TODO: When processing TSDB data_streams they are sorted by `_tsid` and timestamp in descending order,
        // we can read the first encountered value for each group of `_tsid` and time bucket.
        final DataType type = field().dataType();
        return switch (type) {
            case LONG, COUNTER_LONG -> new LastLongByTimestampAggregatorFunctionSupplier();
            case INTEGER, COUNTER_INTEGER -> new LastIntByTimestampAggregatorFunctionSupplier();
            case DOUBLE, COUNTER_DOUBLE -> new LastDoubleByTimestampAggregatorFunctionSupplier();
            case FLOAT -> new LastFloatByTimestampAggregatorFunctionSupplier();
            case TSID_DATA_TYPE -> new LastBytesRefByTimestampAggregatorFunctionSupplier();
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public LastOverTime perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public String toString() {
        return "last_over_time(" + field() + ", " + timestamp() + ")";
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }
}
