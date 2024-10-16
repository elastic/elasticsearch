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
import org.elasticsearch.compute.aggregation.MaxBooleanAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxIpAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MaxLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMax;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;

public class Max extends AggregateFunction implements ToAggregator, SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Max", Max::new);

    private static final Map<DataType, Function<List<Integer>, AggregatorFunctionSupplier>> SUPPLIERS = Map.ofEntries(
        Map.entry(DataType.BOOLEAN, MaxBooleanAggregatorFunctionSupplier::new),
        Map.entry(DataType.LONG, MaxLongAggregatorFunctionSupplier::new),
        Map.entry(DataType.DATETIME, MaxLongAggregatorFunctionSupplier::new),
        Map.entry(DataType.DATE_NANOS, MaxLongAggregatorFunctionSupplier::new),
        Map.entry(DataType.INTEGER, MaxIntAggregatorFunctionSupplier::new),
        Map.entry(DataType.DOUBLE, MaxDoubleAggregatorFunctionSupplier::new),
        Map.entry(DataType.IP, MaxIpAggregatorFunctionSupplier::new),
        Map.entry(DataType.KEYWORD, MaxBytesRefAggregatorFunctionSupplier::new),
        Map.entry(DataType.TEXT, MaxBytesRefAggregatorFunctionSupplier::new),
        Map.entry(DataType.VERSION, MaxBytesRefAggregatorFunctionSupplier::new)
    );

    @FunctionInfo(
        returnType = { "boolean", "double", "integer", "long", "date", "ip", "keyword", "text", "long", "version" },
        description = "The maximum value of a field.",
        isAggregation = true,
        examples = {
            @Example(file = "stats", tag = "max"),
            @Example(
                description = "The expression can use inline functions. For example, to calculate the maximum "
                    + "over an average of a multivalued column, use `MV_AVG` to first average the "
                    + "multiple values per row, and use the result with the `MAX` function",
                file = "stats",
                tag = "docsStatsMaxNestedExpression"
            ) }
    )
    public Max(
        Source source,
        @Param(
            name = "field",
            type = { "boolean", "double", "integer", "long", "date", "ip", "keyword", "text", "long", "version" }
        ) Expression field
    ) {
        this(source, field, Literal.TRUE);
    }

    public Max(Source source, Expression field, Expression filter) {
        super(source, field, filter, emptyList());
    }

    private Max(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public Max withFilter(Expression filter) {
        return new Max(source(), field(), filter);
    }

    @Override
    protected NodeInfo<Max> info() {
        return NodeInfo.create(this, Max::new, field(), filter());
    }

    @Override
    public Max replaceChildren(List<Expression> newChildren) {
        return new Max(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected TypeResolution resolveType() {
        return TypeResolutions.isType(
            field(),
            SUPPLIERS::containsKey,
            sourceText(),
            DEFAULT,
            "representable except unsigned_long and spatial types"
        );
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    public final AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        DataType type = field().dataType();
        if (SUPPLIERS.containsKey(type) == false) {
            // If the type checking did its job, this should never happen
            throw EsqlIllegalArgumentException.illegalDataType(type);
        }
        return SUPPLIERS.get(type).apply(inputChannels);
    }

    @Override
    public Expression surrogate() {
        return field().foldable() ? new MvMax(source(), field()) : null;
    }
}
