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
import org.elasticsearch.compute.aggregation.AllFirstBytesRefByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AllFirstDoubleByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AllFirstFloatByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AllFirstIntByTimestampAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.AllFirstLongByTimestampAggregatorFunctionSupplier;
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

public class AllFirst extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "AllFirst",
        AllFirst::readFrom
    );

    private final Expression sort;

    @FunctionInfo(
        type = FunctionType.AGGREGATE,
        preview = true,
        returnType = { "long", "integer", "double", "keyword" },
        description = "Calculates the earliest value of a field, and can operate on null values.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.DEVELOPMENT) },
        examples = @Example(file = "stats_all_first_all_last", tag = "all_first")
    )
    public AllFirst(
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

    private AllFirst(Source source, Expression field, Expression filter, Expression window, Expression sort) {
        super(source, field, filter, window, List.of(sort));
        this.sort = sort;
    }

    private static AllFirst readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression filter = in.readNamedWriteable(Expression.class);
        Expression window = readWindow(in);
        List<Expression> params = in.readNamedWriteableCollectionAsList(Expression.class);
        Expression sort = params.getFirst();
        return new AllFirst(source, field, filter, window, sort);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<AllFirst> info() {
        return NodeInfo.create(this, AllFirst::new, field(), sort);
    }

    @Override
    public AllFirst replaceChildren(List<Expression> newChildren) {
        return new AllFirst(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }

    @Override
    public AllFirst withFilter(Expression filter) {
        return new AllFirst(source(), field(), filter, window(), sort);
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
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

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
            case LONG -> new AllFirstLongByTimestampAggregatorFunctionSupplier();
            case INTEGER -> new AllFirstIntByTimestampAggregatorFunctionSupplier();
            case DOUBLE -> new AllFirstDoubleByTimestampAggregatorFunctionSupplier();
            case FLOAT -> new AllFirstFloatByTimestampAggregatorFunctionSupplier();
            case KEYWORD, TEXT -> new AllFirstBytesRefByTimestampAggregatorFunctionSupplier();
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public String toString() {
        return "all_first(" + field() + ", " + sort + ")";
    }
}
