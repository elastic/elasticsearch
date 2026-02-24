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
import org.elasticsearch.compute.aggregation.AllFirstBooleanByTimestampAggregatorFunctionSupplier;
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

public class First extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "First", First::readFrom);

    private final Expression sort;

    @FunctionInfo(
        type = FunctionType.AGGREGATE,
        preview = true,
        returnType = { "long", "integer", "double", "keyword", "ip", "boolean", "date", "date_nanos" },
        description = """
            This function calculates the earliest occurrence of the search field
            (the first parameter), where sorting order is determined by the sort
            field (the second parameter). This sorting order is always ascending
            and null values always sort last. Both fields support null,
            single-valued, and multi-valued input. If the earliest sort field
            value appears in multiple documents, this function is allowed to
            return any corresponding search field value.""",
        appendix = """
            ::::{warning}
            This can use a significant amount of memory and ES|QL doesn’t yet
            grow aggregations beyond the memory available. This function will
            continue to work until it is used to collect more values than can
            fit into memory, in which case it will fail the query with a
            [Circuit Breaker Error](docs-content://troubleshoot/elasticsearch/circuit-breaker-errors.md).
            This is especially the case when grouping on a field with a large
            number of unique values, and even more so if the search field
            has multi-values of high cardinality.
            ::::""",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW) },
        examples = @Example(file = "stats_first_last", tag = "first")
    )
    public First(
        Source source,
        @Param(
            name = "field",
            type = { "long", "integer", "double", "keyword", "text", "ip", "boolean", "date", "date_nanos" },
            description = "The search field"
        ) Expression field,
        @Param(name = "sortField", type = { "long", "date", "date_nanos" }, description = "The sort field") Expression sort
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW, sort);
    }

    private First(Source source, Expression field, Expression filter, Expression window, Expression sort) {
        super(source, field, filter, window, List.of(sort));
        this.sort = sort;
    }

    private static First readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression filter = in.readNamedWriteable(Expression.class);
        Expression window = readWindow(in);
        List<Expression> params = in.readNamedWriteableCollectionAsList(Expression.class);
        Expression sort = params.getFirst();
        return new First(source, field, filter, window, sort);
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
        return new First(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }

    @Override
    public First withFilter(Expression filter) {
        return new First(source(), field(), filter, window(), sort);
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
                || dt == DataType.DATE_NANOS
                || DataType.isString(dt)
                || dt == DataType.IP
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
            case LONG, DATETIME, DATE_NANOS -> new AllFirstLongByTimestampAggregatorFunctionSupplier();
            case INTEGER -> new AllFirstIntByTimestampAggregatorFunctionSupplier();
            case DOUBLE -> new AllFirstDoubleByTimestampAggregatorFunctionSupplier();
            case FLOAT -> new AllFirstFloatByTimestampAggregatorFunctionSupplier();
            case KEYWORD, TEXT, IP -> new AllFirstBytesRefByTimestampAggregatorFunctionSupplier();
            case BOOLEAN -> new AllFirstBooleanByTimestampAggregatorFunctionSupplier();
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }

    @Override
    public String toString() {
        return "first(" + field() + ", " + sort + ")";
    }
}
