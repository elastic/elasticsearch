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
import org.elasticsearch.compute.aggregation.RateDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.RateIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.RateLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
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

public class Rate extends TimeSeriesAggregateFunction implements OptionalArgument, ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Rate", Rate::new);

    private final Expression timestamp;

    @FunctionInfo(
        returnType = { "double" },
        description = "compute the rate of a counter field. Available in METRICS command only",
        type = FunctionType.AGGREGATE
    )
    public Rate(
        Source source,
        @Param(name = "field", type = { "counter_long|counter_integer|counter_double" }, description = "counter field") Expression field,
        Expression timestamp
    ) {
        this(source, field, Literal.TRUE, timestamp);
    }

    // compatibility constructor used when reading from the stream
    private Rate(Source source, Expression field, Expression filter, List<Expression> children) {
        this(source, field, filter, children.getFirst());
    }

    private Rate(Source source, Expression field, Expression filter, Expression timestamp) {
        super(source, field, filter, List.of(timestamp));
        this.timestamp = timestamp;
    }

    public Rate(StreamInput in) throws IOException {
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

    public static Rate withUnresolvedTimestamp(Source source, Expression field) {
        return new Rate(source, field, new UnresolvedAttribute(source, "@timestamp"));
    }

    @Override
    protected NodeInfo<Rate> info() {
        return NodeInfo.create(this, Rate::new, field(), timestamp);
    }

    @Override
    public Rate replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 3) {
            assert false : "expected 3 children for field, filter, @timestamp; got " + newChildren;
            throw new IllegalArgumentException("expected 3 children for field, filter, @timestamp; got " + newChildren);
        }
        return new Rate(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public Rate withFilter(Expression filter) {
        return new Rate(source(), field(), filter, timestamp);
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
            case COUNTER_LONG -> new RateLongAggregatorFunctionSupplier();
            case COUNTER_INTEGER -> new RateIntAggregatorFunctionSupplier();
            case COUNTER_DOUBLE -> new RateDoubleAggregatorFunctionSupplier();
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public Rate perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public String toString() {
        return "rate(" + field() + ")";
    }

    Expression timestamp() {
        return timestamp;
    }
}
