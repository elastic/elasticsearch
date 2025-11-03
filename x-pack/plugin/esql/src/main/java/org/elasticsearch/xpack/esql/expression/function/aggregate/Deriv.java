/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.DerivDoubleGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.DerivIntGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.DerivLongGroupingAggregatorFunction;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.util.List;

/**
 * Calculates the derivative over time of a numeric field using linear regression.
 */
public class Deriv extends TimeSeriesAggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Deriv", Deriv::new);
    private final Expression timestamp;

    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "double" },
        description = "Calculates the derivative over time of a numeric field using linear regression."
    )
    public Deriv(Source source, @Param(name = "field", type = { "long", "integer", "double" }) Expression field) {
        this(source, field, new UnresolvedAttribute(source, "@timestamp"));
    }

    public Deriv(Source source, Expression field, Expression timestamp) {
        this(source, field, Literal.TRUE, timestamp, NO_WINDOW);
    }

    public Deriv(Source source, Expression field, Expression filter, Expression window, Expression timestamp) {
        super(source, field, filter, window, List.of(timestamp));
        this.timestamp = timestamp;
    }

    private Deriv(org.elasticsearch.common.io.stream.StreamInput in) throws java.io.IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public AggregateFunction perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public AggregateFunction withFilter(Expression filter) {
        return new Deriv(source(), field(), filter, timestamp, window());
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() == 4) {
            return new Deriv(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
        } else if (newChildren.size() == 3) {
            return new Deriv(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), timestamp);
        } else {
            assert newChildren.size() == 2 : "Expected 2, 3, 4 children but got " + newChildren.size();
            return new Deriv(source(), newChildren.get(0), newChildren.get(1));
        }
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Deriv::new, field(), filter(), window(), timestamp);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        final DataType type = field().dataType();
        return switch (type) {
            case INTEGER -> new DerivIntGroupingAggregatorFunction.Supplier();
            case LONG -> new DerivLongGroupingAggregatorFunction.Supplier();
            case DOUBLE -> new DerivDoubleGroupingAggregatorFunction.Supplier();
            default -> throw new IllegalArgumentException("Unsupported data type for deriv aggregation: " + type);
        };
    }
}
