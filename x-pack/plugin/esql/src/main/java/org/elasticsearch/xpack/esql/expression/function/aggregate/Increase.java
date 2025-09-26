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
import org.elasticsearch.compute.aggregation.RateDoubleGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.RateIntGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.RateLongGroupingAggregatorFunction;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * The {@code increase()} function calculates the absolute increase of a counter field in a time window.
 *
 * It is similar to the {@code rate()} function, but instead of calculating the per-second average rate of increase,
 * it calculates the total increase over the time window.
 */
public class Increase extends TimeSeriesAggregateFunction implements OptionalArgument, ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Increase", Increase::new);

    private final Expression timestamp;

    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "double" },
        description = "The absolute increase of a counter field in a time window.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.UNAVAILABLE) },
        note = "Available with the [TS](/reference/query-languages/esql/commands/source-commands.md#esql-ts) command"
    )
    public Increase(
        Source source,
        @Param(name = "field", type = { "counter_long", "counter_integer", "counter_double" }) Expression field
    ) {
        this(source, field, new UnresolvedAttribute(source, "@timestamp"));
    }

    public Increase(
        Source source,
        @Param(name = "field", type = { "counter_long", "counter_integer", "counter_double" }) Expression field,
        Expression timestamp
    ) {
        this(source, field, Literal.TRUE, timestamp);
    }

    // compatibility constructor used when reading from the stream
    private Increase(Source source, Expression field, Expression filter, List<Expression> children) {
        this(source, field, filter, children.getFirst());
    }

    private Increase(Source source, Expression field, Expression filter, Expression timestamp) {
        super(source, field, filter, List.of(timestamp));
        this.timestamp = timestamp;
    }

    public Increase(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Increase> info() {
        return NodeInfo.create(this, Increase::new, field(), timestamp);
    }

    @Override
    public Increase replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 3) {
            assert false : "expected 3 children for field, filter, @timestamp; got " + newChildren;
            throw new IllegalArgumentException("expected 3 children for field, filter, @timestamp; got " + newChildren);
        }
        return new Increase(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public Increase withFilter(Expression filter) {
        return new Increase(source(), field(), filter, timestamp);
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
            case COUNTER_LONG -> new RateLongGroupingAggregatorFunction.FunctionSupplier(false);
            case COUNTER_INTEGER -> new RateIntGroupingAggregatorFunction.FunctionSupplier(false);
            case COUNTER_DOUBLE -> new RateDoubleGroupingAggregatorFunction.FunctionSupplier(false);
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public Increase perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public String toString() {
        return "increase(" + field() + ")";
    }

    Expression timestamp() {
        return timestamp;
    }

    @Override
    public boolean requiredTimeSeriesSource() {
        return true;
    }
}
