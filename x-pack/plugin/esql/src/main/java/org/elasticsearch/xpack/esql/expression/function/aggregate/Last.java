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
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.compute.aggregation.LastOverTimeDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LastOverTimeFloatAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LastOverTimeIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LastOverTimeLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class Last extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Last", Last::new);

    private final Expression sort;

    // TODO: support all types
    @FunctionInfo(
        type = FunctionType.TIME_SERIES_AGGREGATE,
        returnType = { "long", "integer", "double" },
        description = "The latest value of a field.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.UNAVAILABLE) }
        // examples = { @Example(file = "k8s-timeseries", tag = "last_over_time") } NOCOMMIT
    )
    public Last(
        Source source,
        @Param(name = "field", type = { "long", "integer", "double" }) Expression field,
        @Param(name = "field", type = { "long", "datetime", "date_nanos" }) Expression sort
    ) {
        this(source, field, Literal.TRUE, sort);
    }

    private Last(Source source, Expression field, Expression filter, Expression sort) {
        super(source, field, filter, List.of(sort));
        this.sort = sort;
    }

    public Last(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Last> info() {
        return NodeInfo.create(this, Last::new, field(), sort);
    }

    @Override
    public Last replaceChildren(List<Expression> newChildren) {
        return new Last(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public Last withFilter(Expression filter) {
        return new Last(source(), field(), filter, sort);
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
                    sort,
                    dt -> dt == DataType.LONG || dt == DataType.DATETIME || dt == DataType.DATE_NANOS,
                    sourceText(),
                    SECOND,
                    "long or date_nanos or datetime"
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
    public String toString() {
        return "last(" + field() + ", " + sort + ")";
    }
}
