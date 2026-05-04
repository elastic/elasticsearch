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
import org.elasticsearch.compute.aggregation.LegacyIrateDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LegacyIrateIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LegacyIrateLongAggregatorFunctionSupplier;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.TemporalityAware;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Legacy irate implementation that does not support delta temporality.
 * This is used for backwards compatibility with older cluster nodes.
 * New code should use {@link Irate} instead.
 */
public class LegacyIrate extends TimeSeriesAggregateFunction implements OptionalArgument, ToAggregator, TemporalityAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Irate",
        LegacyIrate::readFrom
    );

    private final Expression timestamp;
    private final Expression temporality;

    public LegacyIrate(
        Source source,
        Expression field,
        Expression filter,
        Expression window,
        Expression timestamp,
        @Nullable Expression temporality
    ) {
        super(source, field, filter, window, temporality == null ? List.of(timestamp) : List.of(timestamp, temporality));
        this.timestamp = timestamp;
        this.temporality = temporality;
    }

    private static LegacyIrate readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression filter = in.readNamedWriteable(Expression.class);
        Expression window = readWindow(in);
        List<Expression> parameters = in.readNamedWriteableCollectionAsList(Expression.class);
        return new LegacyIrate(source, field, filter, window, parameters.getFirst(), parameters.size() > 1 ? parameters.get(1) : null);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<LegacyIrate> info() {
        if (temporality != null) {
            return NodeInfo.create(this, LegacyIrate::new, field(), filter(), window(), timestamp, temporality);
        } else {
            return NodeInfo.create(
                this,
                (source, field, filter, window, timestamp) -> new LegacyIrate(source, field, filter, window, timestamp, null),
                field(),
                filter(),
                window(),
                timestamp
            );
        }
    }

    @Override
    public LegacyIrate replaceChildren(List<Expression> newChildren) {
        return new LegacyIrate(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.get(2),
            newChildren.get(3),
            newChildren.size() > 4 ? newChildren.get(4) : null
        );
    }

    @Override
    public LegacyIrate withFilter(Expression filter) {
        return new LegacyIrate(source(), field(), filter, window(), timestamp, temporality);
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
        final DataType tsType = timestamp().dataType();
        final boolean isDateNanos = tsType == DataType.DATE_NANOS;
        return switch (type) {
            case COUNTER_LONG -> new LegacyIrateLongAggregatorFunctionSupplier(source(), isDateNanos);
            case COUNTER_INTEGER -> new LegacyIrateIntAggregatorFunctionSupplier(source(), isDateNanos);
            case COUNTER_DOUBLE -> new LegacyIrateDoubleAggregatorFunctionSupplier(source(), isDateNanos);
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public LegacyIrate perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public String toString() {
        return "irate(" + field() + ", " + timestamp() + ", " + temporality() + ")";
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }

    @Override
    public Expression temporality() {
        return temporality;
    }

    @Override
    public LegacyIrate withTemporality(Expression newTemporality) {
        return new LegacyIrate(source(), field(), filter(), window(), timestamp, newTemporality);
    }
}
