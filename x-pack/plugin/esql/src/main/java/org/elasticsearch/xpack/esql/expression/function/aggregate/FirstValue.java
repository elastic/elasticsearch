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
import org.elasticsearch.compute.aggregation.FirstValueBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.FirstValueLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;

public class FirstValue extends AggregateFunction implements OptionalArgument, ToAggregator, SurrogateExpression {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "First", FirstValue::new);

    private static final Map<DataType, Function<List<Integer>, AggregatorFunctionSupplier>> SUPPLIERS = Map.ofEntries(
        Map.entry(DataType.LONG, FirstValueLongAggregatorFunctionSupplier::new),
        Map.entry(DataType.KEYWORD, FirstValueBytesRefAggregatorFunctionSupplier::new),
        Map.entry(DataType.TEXT, FirstValueBytesRefAggregatorFunctionSupplier::new),
        Map.entry(DataType.SEMANTIC_TEXT, FirstValueBytesRefAggregatorFunctionSupplier::new),
        Map.entry(DataType.VERSION, FirstValueBytesRefAggregatorFunctionSupplier::new)
    );

    @FunctionInfo(returnType = "keyword", description = "TBD", type = FunctionType.AGGREGATE, examples = {})
    public FirstValue(Source source, @Param(name = "field", type = "keyword", description = "TBD") Expression field, Expression timestamp) {
        this(source, field, Literal.TRUE, timestamp != null ? List.of(timestamp) : List.of(new UnresolvedAttribute(source, "@timestamp")));
    }

    private FirstValue(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    private FirstValue(Source source, Expression field, Expression filter, List<? extends Expression> params) {
        super(source, field, filter, params);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, FirstValue::new, field(), filter(), parameters());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new FirstValue(source(), newChildren.get(0), newChildren.get(1), newChildren.subList(2, newChildren.size()));
    }

    @Override
    public AggregateFunction withFilter(Expression filter) {
        return new FirstValue(source(), field(), filter, parameters());
    }

    // TODO ensure BY is always timestamp/long

    @Override
    public DataType dataType() {
        return field().dataType().noText();
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
    public Expression surrogate() {
        // TODO can this be optimized even further?
        return field().foldable() ? field() : null;
    }

    @Override
    public AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        var type = field().dataType();
        var supplier = SUPPLIERS.get(type);
        if (supplier == null) {
            // If the type checking did its job, this should never happen
            throw EsqlIllegalArgumentException.illegalDataType(type);
        }
        return supplier.apply(inputChannels);
    }

    public Expression by() {
        return parameters().isEmpty() ? null : parameters().getFirst();
    }
}
