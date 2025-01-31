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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;

public class FirstValue extends AggregateFunction implements OptionalArgument, ToAggregator, SurrogateExpression {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "First", FirstValue::new);

    // TODO @FunctionInfo
    public FirstValue(Source source, Expression field, Expression by) {
        this(source, field, Literal.TRUE, by != null ? List.of(by) : List.of());
    }

    private FirstValue(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    private FirstValue(Source source, Expression field, Expression filter, List<? extends Expression> params) {
        super(source, field, filter, params);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FirstValue::new, field(), filter(), parameters());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new FirstValue(source(), newChildren.get(0), newChildren.get(1), newChildren.subList(2, newChildren.size()));
    }

    @Override
    public AggregateFunction withFilter(Expression filter) {
        return new FirstValue(source(), field(), filter, parameters());
    }

    @Override
    public DataType dataType() {
        return field().dataType().noText();
    }

    @Override
    protected TypeResolution resolveType() {
        return TypeResolutions.isType(
            field(),
            dt -> dt == DataType.KEYWORD, // TODO implement for all types
            sourceText(),
            DEFAULT,
            "representable except unsigned_long and spatial types"
        );
    }

    @Override
    public Expression surrogate() {
        return null;
    }

    @Override
    public AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        return null;
    }

    public Expression by() {
        return parameters().isEmpty() ? null : parameters().getFirst();
    }
}
