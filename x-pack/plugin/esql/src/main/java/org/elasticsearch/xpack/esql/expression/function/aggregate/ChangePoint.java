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
import org.elasticsearch.compute.aggregation.ChangePointLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class ChangePoint extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ChangePoint",
        ChangePoint::new
    );

    @FunctionInfo(returnType = { "string" }, description = "...", isAggregation = true)
    public ChangePoint(
        Source source,
        @Param(name = "field", type = { "double", "integer", "long" }, description = "field") Expression field,
        @Param(name = "timestamp", type = { "date_nanos", "datetime", "double", "integer", "long" }) Expression timestamp
    ) {
        this(source, field, Literal.TRUE, timestamp);
    }

    public ChangePoint(Source source, Expression field, Expression filter, Expression timestamp) {
        super(source, field, filter, List.of(timestamp));
    }

    private ChangePoint(StreamInput in) throws IOException {
        super(
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

    Expression timestamp() {
        return parameters().get(0);
    }

    @Override
    protected NodeInfo<ChangePoint> info() {
        return NodeInfo.create(this, ChangePoint::new, field(), timestamp());
    }

    @Override
    public ChangePoint replaceChildren(List<Expression> newChildren) {
        return new ChangePoint(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(field(), dt -> dt.isNumeric(), sourceText(), FIRST, "numeric").and(
            isType(timestamp(), dt -> dt.isDate() || dt.isNumeric(), sourceText(), SECOND, "date_nanos or datetime or numeric")
        );
    }

    @Override
    public AggregateFunction withFilter(Expression filter) {
        return new ChangePoint(source(), field(), filter, timestamp());
    }

    @Override
    public AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        // if (inputChannels.size() != 2 && inputChannels.size() != 3) {
        // throw new IllegalArgumentException("change point requires two for raw input or three channels for partial input; got " +
        // inputChannels);
        // }
        final DataType type = field().dataType();
        return switch (type) {
            case LONG -> new ChangePointLongAggregatorFunctionSupplier(inputChannels);
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public String toString() {
        return "change_point{field=" + field() + ",timestamp=" + timestamp() + "}";
    }
}
