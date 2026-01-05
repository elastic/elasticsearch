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
import org.elasticsearch.compute.aggregation.LastBytesRefByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LastDoubleByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LastFloatByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LastIntByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.LastLongByTimestampAggregatorFunctionSupplier;
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

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class Last extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Last", Last::readFrom);

    private final Expression sort;

    // TODO: support all types
    @FunctionInfo(
        type = FunctionType.AGGREGATE,
        returnType = { "long", "integer", "double", "keyword" },
        description = "Calculates the latest value of a field.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.3.0") },
        examples = @Example(file = "stats_last", tag = "last")
    )
    public Last(
        Source source,
        @Param(
            name = "value",
            type = { "long", "integer", "double", "keyword", "text" },
            description = "Values to return"
        ) Expression field,
        @Param(name = "sort", type = { "date", "date_nanos" }, description = "Sort key") Expression sort
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW, sort);
    }

    private Last(Source source, Expression field, Expression filter, Expression window, Expression sort) {
        super(source, field, filter, window, List.of(sort));
        this.sort = sort;
    }

    private static Last readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression filter = in.readNamedWriteable(Expression.class);
        Expression window = readWindow(in);
        List<Expression> params = in.readNamedWriteableCollectionAsList(Expression.class);
        Expression sort = params.getFirst();
        return new Last(source, field, filter, window, sort);
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
        return new Last(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }

    @Override
    public Last withFilter(Expression filter) {
        return new Last(source(), field(), filter, window(), sort);
    }

    public Expression sort() {
        return sort;
    }

    @Override
    public DataType dataType() {
        return field().dataType().noText();
    }

    @Override
    protected TypeResolution resolveType() {
        return isType(
            field(),
            dt -> dt == DataType.BOOLEAN
                || dt == DataType.DATETIME
                || DataType.isString(dt)
                || (dt.isNumeric() && dt != DataType.UNSIGNED_LONG),
            sourceText(),
            FIRST,
            "boolean",
            "date",
            "ip",
            "string",
            "numeric except unsigned_long or counter types"
        ).and(
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
            case LONG -> new LastLongByTimestampAggregatorFunctionSupplier();
            case INTEGER -> new LastIntByTimestampAggregatorFunctionSupplier();
            case DOUBLE -> new LastDoubleByTimestampAggregatorFunctionSupplier();
            case FLOAT -> new LastFloatByTimestampAggregatorFunctionSupplier();
            case KEYWORD, TEXT -> new LastBytesRefByTimestampAggregatorFunctionSupplier();
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public String toString() {
        return "last(" + field() + ", " + sort + ")";
    }
}
