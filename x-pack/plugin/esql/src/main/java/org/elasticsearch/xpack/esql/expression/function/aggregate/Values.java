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
import org.elasticsearch.compute.aggregation.ValuesBooleanAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.ValuesLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;

public class Values extends AggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Values", Values::new);

    private static final Map<DataType, Function<List<Integer>, AggregatorFunctionSupplier>> SUPPLIERS = Map.ofEntries(
        Map.entry(DataType.INTEGER, ValuesIntAggregatorFunctionSupplier::new),
        Map.entry(DataType.LONG, ValuesLongAggregatorFunctionSupplier::new),
        Map.entry(DataType.DATETIME, ValuesLongAggregatorFunctionSupplier::new),
        Map.entry(DataType.DATE_NANOS, ValuesLongAggregatorFunctionSupplier::new),
        Map.entry(DataType.DOUBLE, ValuesDoubleAggregatorFunctionSupplier::new),
        Map.entry(DataType.KEYWORD, ValuesBytesRefAggregatorFunctionSupplier::new),
        Map.entry(DataType.TEXT, ValuesBytesRefAggregatorFunctionSupplier::new),
        Map.entry(DataType.IP, ValuesBytesRefAggregatorFunctionSupplier::new),
        Map.entry(DataType.VERSION, ValuesBytesRefAggregatorFunctionSupplier::new),
        Map.entry(DataType.BOOLEAN, ValuesBooleanAggregatorFunctionSupplier::new)
    );

    @FunctionInfo(
        returnType = { "boolean", "date", "double", "integer", "ip", "keyword", "long", "text", "version" },
        preview = true,
        description = "Returns all values in a group as a multivalued field. The order of the returned values isn't guaranteed. "
            + "If you need the values returned in order use <<esql-mv_sort>>.",
        appendix = """
            [WARNING]
            ====
            This can use a significant amount of memory and ES|QL doesn't yet
            grow aggregations beyond memory. So this aggregation will work until
            it is used to collect more values than can fit into memory. Once it
            collects too many values it will fail the query with
            a <<circuit-breaker-errors, Circuit Breaker Error>>.
            ====""",
        isAggregation = true,
        examples = @Example(file = "string", tag = "values-grouped")
    )
    public Values(
        Source source,
        @Param(name = "field", type = { "boolean", "date", "double", "integer", "ip", "keyword", "long", "text", "version" }) Expression v
    ) {
        this(source, v, Literal.TRUE);
    }

    public Values(Source source, Expression field, Expression filter) {
        super(source, field, filter, emptyList());
    }

    private Values(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Values> info() {
        return NodeInfo.create(this, Values::new, field(), filter());
    }

    @Override
    public Values replaceChildren(List<Expression> newChildren) {
        return new Values(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    public Values withFilter(Expression filter) {
        return new Values(source(), field(), filter);
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    protected TypeResolution resolveType() {
        return TypeResolutions.isType(
            field(),
            SUPPLIERS::containsKey,
            sourceText(),
            DEFAULT,
            "any type except unsigned_long and spatial types"
        );
    }

    @Override
    public AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        DataType type = field().dataType();
        if (SUPPLIERS.containsKey(type) == false) {
            // If the type checking did its job, this should never happen
            throw EsqlIllegalArgumentException.illegalDataType(type);
        }
        return SUPPLIERS.get(type).apply(inputChannels);
    }
}
