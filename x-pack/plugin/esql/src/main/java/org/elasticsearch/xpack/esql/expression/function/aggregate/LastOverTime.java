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
import org.elasticsearch.compute.aggregation.LastOverTimeDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LastOverTimeFloatAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LastOverTimeIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LastOverTimeLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
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
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class LastOverTime extends TimeSeriesAggregateFunction implements OptionalArgument, ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "LastOverTime",
        LastOverTime::new
    );

    private final Expression timestamp;

    // TODO: support all types
    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "long", "integer", "double" },
        description = "The latest value of a field, where recency determined by the `@timestamp` field.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.UNAVAILABLE) },
        note = "Available with the [TS](/reference/query-languages/esql/commands/source-commands.md#esql-ts) command in snapshot builds",
        examples = { @Example(file = "k8s-timeseries", tag = "last_over_time") }
    )
    public LastOverTime(Source source, @Param(name = "field", type = { "long", "integer", "double" }) Expression field) {
        this(source, field, new UnresolvedAttribute(source, "@timestamp"));
    }

    public LastOverTime(Source source, Expression field, Expression timestamp) {
        this(source, field, Literal.TRUE, timestamp);
    }

    // compatibility constructor used when reading from the stream
    private LastOverTime(Source source, Expression field, Expression filter, List<Expression> children) {
        this(source, field, filter, children.getFirst());
    }

    private LastOverTime(Source source, Expression field, Expression filter, Expression timestamp) {
        super(source, field, filter, List.of(timestamp));
        this.timestamp = timestamp;
    }

    public LastOverTime(StreamInput in) throws IOException {
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
    protected NodeInfo<LastOverTime> info() {
        return NodeInfo.create(this, LastOverTime::new, field(), timestamp);
    }

    @Override
    public LastOverTime replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 3) {
            assert false : "expected 3 children for field, filter, @timestamp; got " + newChildren;
            throw new IllegalArgumentException("expected 3 children for field, filter, @timestamp; got " + newChildren);
        }
        return new LastOverTime(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public LastOverTime withFilter(Expression filter) {
        return new LastOverTime(source(), field(), filter, timestamp);
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(field(), dt -> dt.isNumeric() && dt != DataType.UNSIGNED_LONG, sourceText(), DEFAULT, "numeric except unsigned_long")
            .and(
                isType(
                    timestamp,
                    dt -> dt == DataType.DATETIME || dt == DataType.DATE_NANOS,
                    sourceText(),
                    SECOND,
                    "date_nanos or datetime"
                )
            );
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        final DataType type = field().dataType();
        return switch (type) {
            case LONG -> new LastOverTimeLongAggregatorFunctionSupplier();
            case INTEGER -> new LastOverTimeIntAggregatorFunctionSupplier();
            case DOUBLE -> new LastOverTimeDoubleAggregatorFunctionSupplier();
            case FLOAT -> new LastOverTimeFloatAggregatorFunctionSupplier();
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public LastOverTime perTimeSeriesAggregation() {
        return this;
    }

    @Override
    public String toString() {
        return "last_over_time(" + field() + ")";
    }

    Expression timestamp() {
        return timestamp;
    }
}
