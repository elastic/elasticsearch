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
import org.elasticsearch.compute.aggregation.DeltaDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.DeltaIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.DeltaLongAggregatorFunctionSupplier;
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

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;

public class Delta extends TimeSeriesAggregateFunction implements OptionalArgument, ToAggregator, TimestampAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Delta", Delta::new);

    private final Expression timestamp;

    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "double" },
        description = "Calculates the absolute change of a gauge field in a time window.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0") },
        preview = true,
        examples = { @Example(file = "k8s-timeseries-delta", tag = "delta") }
    )
    public Delta(Source source, @Param(name = "field", type = { "long", "integer", "double" }) Expression field, Expression timestamp) {
        this(source, field, Literal.TRUE, NO_WINDOW, timestamp);
    }

    private Delta(Source source, Expression field, Expression filter, Expression window, Expression timestamp) {
        super(source, field, filter, window, List.of(timestamp));
        this.timestamp = timestamp;
    }

    public Delta(StreamInput in) throws IOException {
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
    protected NodeInfo<Delta> info() {
        return NodeInfo.create(this, Delta::new, field(), timestamp);
    }

    @Override
    public Delta replaceChildren(List<Expression> newChildren) {
        return new Delta(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }

    @Override
    public Delta withFilter(Expression filter) {
        return new Delta(source(), field(), filter, window(), timestamp);
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(
            field(),
            dt -> dt.isNumeric() && dt != AGGREGATE_METRIC_DOUBLE,
            sourceText(),
            DEFAULT,
            "numeric except counter types"
        );
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        final DataType type = field().dataType();
        return switch (type) {
            case LONG -> new DeltaLongAggregatorFunctionSupplier();
            case INTEGER -> new DeltaIntAggregatorFunctionSupplier();
            case DOUBLE -> new DeltaDoubleAggregatorFunctionSupplier();
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public Delta perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public String toString() {
        return "delta(" + field() + ", " + timestamp() + ")";
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }
}
