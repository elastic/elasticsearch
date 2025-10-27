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
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
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
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;

public class Idelta extends TimeSeriesAggregateFunction implements OptionalArgument, ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Idelta", Idelta::new);

    private final Expression timestamp;

    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "double" },
        description = "Calculates the idelta of a gauge. idelta is the absolute change between the last two data points ("
            + "it ignores all but the last two data points in each time period). "
            + "This function is very similar to delta, but is more responsive to recent changes.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.2.0") },
        preview = true,
        examples = { @Example(file = "k8s-timeseries-idelta", tag = "idelta") }
    )
    public Idelta(Source source, @Param(name = "field", type = { "long", "integer", "double" }) Expression field) {
        this(
            source,
            field,
            new UnresolvedTimestamp(source, "IDelta aggregation requires @timestamp field, but @timestamp was renamed or dropped")
        );
    }

    public Idelta(Source source, @Param(name = "field", type = { "long", "integer", "double" }) Expression field, Expression timestamp) {
        this(source, field, Literal.TRUE, timestamp);
    }

    // compatibility constructor used when reading from the stream
    private Idelta(Source source, Expression field, Expression filter, List<Expression> children) {
        this(source, field, filter, children.getFirst());
    }

    private Idelta(Source source, Expression field, Expression filter, Expression timestamp) {
        super(source, field, filter, List.of(timestamp));
        this.timestamp = timestamp;
    }

    public Idelta(StreamInput in) throws IOException {
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
    protected NodeInfo<Idelta> info() {
        return NodeInfo.create(this, Idelta::new, field(), timestamp);
    }

    @Override
    public Idelta replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 3) {
            assert false : "expected 3 children for field, filter, @timestamp; got " + newChildren;
            throw new IllegalArgumentException("expected 3 children for field, filter, @timestamp; got " + newChildren);
        }
        return new Idelta(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public Idelta withFilter(Expression filter) {
        return new Idelta(source(), field(), filter, timestamp);
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
            case LONG -> new IrateLongAggregatorFunctionSupplier(true);
            case INTEGER -> new IrateIntAggregatorFunctionSupplier(true);
            case DOUBLE -> new IrateDoubleAggregatorFunctionSupplier(true);
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public Idelta perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public String toString() {
        return "idelta(" + field() + ")";
    }
}
