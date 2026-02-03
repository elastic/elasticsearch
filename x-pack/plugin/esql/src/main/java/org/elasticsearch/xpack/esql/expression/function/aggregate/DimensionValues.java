/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.DimensionValuesByteRefGroupingAggregatorFunction;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;

/**
 * A specialization of {@link Values} for collecting dimension fields in time-series queries.
 */
public class DimensionValues extends AggregateFunction implements ToAggregator {
    private static final Map<DataType, Supplier<AggregatorFunctionSupplier>> SUPPLIERS = Map.ofEntries(
        Map.entry(DataType.KEYWORD, DimensionValuesByteRefGroupingAggregatorFunction.FunctionSupplier::new),
        Map.entry(DataType.TEXT, DimensionValuesByteRefGroupingAggregatorFunction.FunctionSupplier::new),
        Map.entry(DataType.IP, DimensionValuesByteRefGroupingAggregatorFunction.FunctionSupplier::new),
        Map.entry(DataType.VERSION, DimensionValuesByteRefGroupingAggregatorFunction.FunctionSupplier::new),
        Map.entry(DataType.GEO_POINT, DimensionValuesByteRefGroupingAggregatorFunction.FunctionSupplier::new),
        Map.entry(DataType.CARTESIAN_POINT, DimensionValuesByteRefGroupingAggregatorFunction.FunctionSupplier::new),
        Map.entry(DataType.GEO_SHAPE, DimensionValuesByteRefGroupingAggregatorFunction.FunctionSupplier::new),
        Map.entry(DataType.CARTESIAN_SHAPE, DimensionValuesByteRefGroupingAggregatorFunction.FunctionSupplier::new)
    );

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "DimensionValues",
        DimensionValues::new
    );

    public static final TransportVersion DIMENSION_VALUES_VERSION = TransportVersion.fromName("dimension_values");

    public DimensionValues(Source source, Expression field) {
        super(source, field, Literal.TRUE, NO_WINDOW, emptyList());
    }

    private DimensionValues(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<DimensionValues> info() {
        return NodeInfo.create(this, DimensionValues::new, field());
    }

    @Override
    public DimensionValues replaceChildren(List<Expression> newChildren) {
        return new DimensionValues(source(), newChildren.get(0));
    }

    @Override
    public DimensionValues withFilter(Expression filter) {
        if (filter instanceof Literal l && l.value() == Boolean.TRUE) {
            return this;
        }
        throw new UnsupportedOperationException("Dimension values do not support filters");
    }

    @Override
    public DataType dataType() {
        return field().dataType().noText();
    }

    @Override
    protected TypeResolution resolveType() {
        return new Values(source(), field(), filter(), window()).resolveType();
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        Supplier<AggregatorFunctionSupplier> supplier = SUPPLIERS.get(field().dataType());
        if (supplier != null) {
            return supplier.get();
        }
        return new Values(source(), field(), filter(), window()).supplier();
    }
}
