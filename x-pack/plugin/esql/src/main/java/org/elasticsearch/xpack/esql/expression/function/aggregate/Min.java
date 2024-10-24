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
import org.elasticsearch.compute.aggregation.MinBooleanAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinIpAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.MinLongAggregatorFunctionSupplier;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMin;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;

public class Min extends AggregateFunction implements ToAggregator, SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Min", Min::new);

    private static final Map<DataType, Function<List<Integer>, AggregatorFunctionSupplier>> SUPPLIERS = Map.ofEntries(
        Map.entry(DataType.BOOLEAN, MinBooleanAggregatorFunctionSupplier::new),
        Map.entry(DataType.LONG, MinLongAggregatorFunctionSupplier::new),
        Map.entry(DataType.DATETIME, MinLongAggregatorFunctionSupplier::new),
        Map.entry(DataType.DATE_NANOS, MinLongAggregatorFunctionSupplier::new),
        Map.entry(DataType.INTEGER, MinIntAggregatorFunctionSupplier::new),
        Map.entry(DataType.DOUBLE, MinDoubleAggregatorFunctionSupplier::new),
        Map.entry(DataType.IP, MinIpAggregatorFunctionSupplier::new),
        Map.entry(DataType.VERSION, MinBytesRefAggregatorFunctionSupplier::new),
        Map.entry(DataType.KEYWORD, MinBytesRefAggregatorFunctionSupplier::new),
        Map.entry(DataType.TEXT, MinBytesRefAggregatorFunctionSupplier::new)
    );

    @FunctionInfo(
        returnType = { "boolean", "double", "integer", "long", "date", "ip", "keyword", "text", "long", "version" },
        description = "The minimum value of a field.",
        isAggregation = true,
        examples = {
            @Example(file = "stats", tag = "min"),
            @Example(
                description = "The expression can use inline functions. For example, to calculate the minimum "
                    + "over an average of a multivalued column, use `MV_AVG` to first average the "
                    + "multiple values per row, and use the result with the `MIN` function",
                file = "stats",
                tag = "docsStatsMinNestedExpression"
            ) }
    )
    public Min(
        Source source,
        @Param(
            name = "field",
            type = { "boolean", "double", "integer", "long", "date", "ip", "keyword", "text", "long", "version" }
        ) Expression field
    ) {
        this(source, field, Literal.TRUE);
    }

    public Min(Source source, Expression field, Expression filter) {
        super(source, field, filter, emptyList());
    }

    private Min(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Min> info() {
        return NodeInfo.create(this, Min::new, field(), filter());
    }

    @Override
    public Min replaceChildren(List<Expression> newChildren) {
        return new Min(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    public Min withFilter(Expression filter) {
        return new Min(source(), field(), filter);
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
        return field().foldable() ? new MvMin(source(), field()) : null;
    }
}
