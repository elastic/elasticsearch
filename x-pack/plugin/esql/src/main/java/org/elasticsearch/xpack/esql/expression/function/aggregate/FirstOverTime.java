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
import org.elasticsearch.compute.aggregation.FirstOverTimeDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.FirstOverTimeFloatAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.FirstOverTimeIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.FirstOverTimeLongAggregatorFunctionSupplier;
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

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class FirstOverTime extends TimeSeriesAggregateFunction implements OptionalArgument, ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "FirstOverTime",
        FirstOverTime::new
    );

    private final Expression timestamp;

    @FunctionInfo(
        returnType = { "int", "double", "integer", "long" },
        description = "Collect the first occurrence value of a time-series in the specified interval. Available with TS command only",
        type = FunctionType.AGGREGATE
    )
    public FirstOverTime(
        Source source,
        @Param(name = "field", type = { "long|int|double|float" }, description = "field") Expression field,
        Expression timestamp
    ) {
        this(source, field, Literal.TRUE, timestamp);
    }

    // compatibility constructor used when reading from the stream
    private FirstOverTime(Source source, Expression field, Expression filter, List<Expression> children) {
        this(source, field, filter, children.getFirst());
    }

    private FirstOverTime(Source source, Expression field, Expression filter, Expression timestamp) {
        super(source, field, filter, List.of(timestamp));
        this.timestamp = timestamp;
    }

    public FirstOverTime(StreamInput in) throws IOException {
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

    public static FirstOverTime withUnresolvedTimestamp(Source source, Expression field) {
        return new FirstOverTime(source, field, new UnresolvedAttribute(source, "@timestamp"));
    }

    @Override
    protected NodeInfo<FirstOverTime> info() {
        return NodeInfo.create(this, FirstOverTime::new, field(), timestamp);
    }

    @Override
    public FirstOverTime replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 3) {
            assert false : "expected 3 children for field, filter, @timestamp; got " + newChildren;
            throw new IllegalArgumentException("expected 3 children for field, filter, @timestamp; got " + newChildren);
        }
        return new FirstOverTime(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public FirstOverTime withFilter(Expression filter) {
        return new FirstOverTime(source(), field(), filter, timestamp);
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(field(), dt -> dt.isNumeric() && dt != DataType.UNSIGNED_LONG, sourceText(), DEFAULT, "numeric except unsigned_long");
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        final DataType type = field().dataType();
        return switch (type) {
            case LONG -> new FirstOverTimeLongAggregatorFunctionSupplier();
            case INTEGER -> new FirstOverTimeIntAggregatorFunctionSupplier();
            case DOUBLE -> new FirstOverTimeDoubleAggregatorFunctionSupplier();
            case FLOAT -> new FirstOverTimeFloatAggregatorFunctionSupplier();
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public FirstOverTime perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public String toString() {
        return "first_over_time(" + field() + ")";
    }

    Expression timestamp() {
        return timestamp;
    }
}
