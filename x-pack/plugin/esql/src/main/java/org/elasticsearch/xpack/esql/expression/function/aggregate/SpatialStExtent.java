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
import org.elasticsearch.compute.aggregation.spatial.SpatialStExtentCartesianPointDocValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialStExtentCartesianPointSourceValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialStExtentCartesianShapeAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialStExtentGeoPointDocValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialStExtentGeoPointSourceValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialStExtentGeoShapeAggregatorFunctionSupplier;
import org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatial;

/** Calculate spatial extent of all values of a field in matching documents. */
public final class SpatialStExtent extends SpatialAggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "SpatialStExtent",
        SpatialStExtent::new
    );

    @FunctionInfo(
        returnType = { "geo_point", "cartesian_point", "geo_shape", "cartesian_shape" },
        description = "Calculate the extent over a field with spatial point geometry type.",
        isAggregation = true,
        examples = @Example(file = "spatial", tag = "st_extent_agg-airports")
    )
    public SpatialStExtent(
        Source source,
        @Param(name = "field", type = { "geo_point", "cartesian_point", "geo_shape", "cartesian_shape" }) Expression field
    ) {
        this(source, field, Literal.TRUE, FieldExtractPreference.NONE);
    }

    private SpatialStExtent(Source source, Expression field, Expression filter, FieldExtractPreference preference) {
        super(source, field, filter, preference);
    }

    private SpatialStExtent(StreamInput in) throws IOException {
        super(in, FieldExtractPreference.NONE);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public SpatialStExtent withFilter(Expression filter) {
        return new SpatialStExtent(source(), field(), filter, preference);
    }

    @Override
    public SpatialStExtent withDocValues() {
        return new SpatialStExtent(source(), field(), filter(), FieldExtractPreference.DOC_VALUES);
    }

    @Override
    protected TypeResolution resolveType() {
        return isSpatial(field(), sourceText(), DEFAULT);
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    protected NodeInfo<SpatialStExtent> info() {
        return NodeInfo.create(this, SpatialStExtent::new, field());
    }

    @Override
    public SpatialStExtent replaceChildren(List<Expression> newChildren) {
        return new SpatialStExtent(source(), newChildren.get(0));
    }

    @Override
    public AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        return switch (field().dataType()) {
            case DataType.GEO_POINT -> switch (preference) {
                case DOC_VALUES -> new SpatialStExtentGeoPointDocValuesAggregatorFunctionSupplier(inputChannels);
                case NONE -> new SpatialStExtentGeoPointSourceValuesAggregatorFunctionSupplier(inputChannels);
            };
            case DataType.CARTESIAN_POINT -> switch (preference) {
                case DOC_VALUES -> new SpatialStExtentCartesianPointDocValuesAggregatorFunctionSupplier(inputChannels);
                case NONE -> new SpatialStExtentCartesianPointSourceValuesAggregatorFunctionSupplier(inputChannels);
            };
            // Shapes don't differentiate between source and doc values.
            case DataType.GEO_SHAPE -> new SpatialStExtentGeoShapeAggregatorFunctionSupplier(inputChannels);
            case DataType.CARTESIAN_SHAPE -> new SpatialStExtentCartesianShapeAggregatorFunctionSupplier(inputChannels);
            default -> throw EsqlIllegalArgumentException.illegalDataType(field().dataType());
        };
    }
}
