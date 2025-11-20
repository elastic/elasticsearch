/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.DerivDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.DerivIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.DerivLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;

/**
 * Calculates the derivative over time of a numeric field using linear regression.
 */
public class Deriv extends TimeSeriesAggregateFunction implements ToAggregator, TimestampAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Deriv", Deriv::new);
    private final Expression timestamp;

    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "double" },
        description = "Calculates the derivative over time of a numeric field using linear regression.",
        examples = { @Example(file = "k8s-timeseries", tag = "deriv") }
    )
    public Deriv(Source source, @Param(name = "field", type = { "long", "integer", "double" }) Expression field, Expression timestamp) {
        this(source, field, Literal.TRUE, NO_WINDOW, timestamp);
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
            in.readNamedWriteableCollectionAsList(Expression.class).getFirst()
        );
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }

    @Override
    public AggregateFunction perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public AggregateFunction withFilter(Expression filter) {
        return new Deriv(source(), field(), filter, window(), timestamp);
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    public TypeResolution resolveType() {
        return TypeResolutions.isType(
            field(),
            dt -> dt.isNumeric() && dt != AGGREGATE_METRIC_DOUBLE,
            sourceText(),
            DEFAULT,
            "numeric except counter types"
        );
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Deriv(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
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
            case DOUBLE -> new DerivDoubleAggregatorFunctionSupplier();
            case LONG -> new DerivLongAggregatorFunctionSupplier();
            case INTEGER -> new DerivIntAggregatorFunctionSupplier();
            default -> throw new IllegalArgumentException("Unsupported data type for deriv aggregation: " + type);
        };
    }
}
