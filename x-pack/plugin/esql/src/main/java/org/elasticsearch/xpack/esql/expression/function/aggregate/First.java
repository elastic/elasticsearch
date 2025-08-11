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
import org.elasticsearch.compute.aggregation.FirstDoubleByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.FirstFloatByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.FirstIntByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.FirstLongByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
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

public class First extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "First", First::readFrom);

    private final Expression sort;

    // TODO: support all types of values
    @FunctionInfo(
        type = FunctionType.AGGREGATE,
        returnType = { "long", "integer", "double" },
        description = "The earliest value of a field.",
        appliesTo = @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.UNAVAILABLE),
        examples = @Example(file = "stats_first", tag = "first")
    )
    public First(
        Source source,
        @Param(name = "value", type = { "long", "integer", "double" }, description = "Values to return") Expression field,
        @Param(name = "sort", type = { "date", "date_nanos" }, description = "Sort key") Expression sort
    ) {
        this(source, field, Literal.TRUE, sort);
    }

    private First(Source source, Expression field, Expression filter, Expression sort) {
        super(source, field, filter, List.of(sort));
        this.sort = sort;
    }

    private static First readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression filter = in.readNamedWriteable(Expression.class);
        List<Expression> params = in.readNamedWriteableCollectionAsList(Expression.class);
        Expression sort = params.getFirst();
        return new First(source, field, filter, sort);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<First> info() {
        return NodeInfo.create(this, First::new, field(), sort);
    }

    @Override
    public First replaceChildren(List<Expression> newChildren) {
        return new First(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public First withFilter(Expression filter) {
        return new First(source(), field(), filter, sort);
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
            case LONG -> new FirstLongByTimestampAggregatorFunctionSupplier();
            case INTEGER -> new FirstIntByTimestampAggregatorFunctionSupplier();
            case DOUBLE -> new FirstDoubleByTimestampAggregatorFunctionSupplier();
            case FLOAT -> new FirstFloatByTimestampAggregatorFunctionSupplier();
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public String toString() {
        return "first(" + field() + ", " + sort + ")";
    }
}
