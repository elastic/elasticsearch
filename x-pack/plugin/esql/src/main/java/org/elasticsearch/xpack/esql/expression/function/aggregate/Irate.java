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
import org.elasticsearch.compute.aggregation.IrateDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.IrateIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.IrateLongAggregatorFunctionSupplier;
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

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class Irate extends TimeSeriesAggregateFunction implements OptionalArgument, ToAggregator, TimestampAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Irate", Irate::new);

    private final Expression timestamp;

    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "double" },
        description = "Calculates the irate of a counter field. irate is the per-second rate of increase between the last two data points ("
            + "it ignores all but the last two data points in each time period). "
            + "This function is very similar to rate, but is more responsive to recent changes in the rate of increase.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0") },
        preview = true,
        examples = { @Example(file = "k8s-timeseries-irate", tag = "irate") }
    )
    public Irate(
        Source source,
        @Param(name = "field", type = { "counter_long", "counter_integer", "counter_double" }) Expression field,
        Expression timestamp
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW, timestamp);
    }

    private Irate(Source source, Expression field, Expression filter, Expression window, Expression timestamp) {
        super(source, field, filter, window, List.of(timestamp));
        this.timestamp = timestamp;
    }

    public Irate(StreamInput in) throws IOException {
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
    protected NodeInfo<Irate> info() {
        return NodeInfo.create(this, Irate::new, field(), timestamp);
    }

    @Override
    public Irate replaceChildren(List<Expression> newChildren) {
        return new Irate(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }

    @Override
    public Irate withFilter(Expression filter) {
        return new Irate(source(), field(), filter, window(), timestamp);
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(field(), dt -> DataType.isCounter(dt), sourceText(), FIRST, "counter_long", "counter_integer", "counter_double");
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        final DataType type = field().dataType();
        return switch (type) {
            case COUNTER_LONG -> new IrateLongAggregatorFunctionSupplier(false);
            case COUNTER_INTEGER -> new IrateIntAggregatorFunctionSupplier(false);
            case COUNTER_DOUBLE -> new IrateDoubleAggregatorFunctionSupplier(false);
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public Irate perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public String toString() {
        return "irate(" + field() + ", " + timestamp() + ")";
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }
}
