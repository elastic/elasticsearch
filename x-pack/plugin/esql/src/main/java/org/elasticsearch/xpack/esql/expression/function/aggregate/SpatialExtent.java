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
import org.elasticsearch.compute.aggregation.spatial.SpatialExtentCartesianPointDocValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialExtentCartesianPointSourceValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialExtentCartesianShapeDocValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialExtentCartesianShapeSourceValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialExtentGeoPointDocValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialExtentGeoPointSourceValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialExtentGeoShapeDocValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialExtentGeoShapeSourceValuesAggregatorFunctionSupplier;
import org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatial;

/**
 * Calculate spatial extent of all values of a field in matching documents.
 */
public final class SpatialExtent extends SpatialAggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "SpatialExtent",
        SpatialExtent::new
    );

    @FunctionInfo(
        returnType = { "geo_shape", "cartesian_shape" },
        description = "Calculate the spatial extent over a field with geometry type. Returns a bounding box for all values of the field.",
        type = FunctionType.AGGREGATE,
        examples = @Example(file = "spatial", tag = "st_extent_agg-airports")
    )
    public SpatialExtent(
        Source source,
        @Param(name = "field", type = { "geo_point", "cartesian_point", "geo_shape", "cartesian_shape" }) Expression field
    ) {
        this(source, field, Literal.TRUE, FieldExtractPreference.NONE);
    }

    private SpatialExtent(Source source, Expression field, Expression filter, FieldExtractPreference preference) {
        super(source, field, filter, preference);
    }

    private SpatialExtent(StreamInput in) throws IOException {
        super(in, FieldExtractPreference.NONE);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public SpatialExtent withFilter(Expression filter) {
        return new SpatialExtent(source(), field(), filter, fieldExtractPreference);
    }

    @Override
    public SpatialExtent withFieldExtractPreference(FieldExtractPreference preference) {
        return new SpatialExtent(source(), field(), filter(), preference);
    }

    @Override
    protected TypeResolution resolveType() {
        return isSpatial(field(), sourceText(), DEFAULT);
    }

    @Override
    public DataType dataType() {
        return DataType.isSpatialGeo(field().dataType()) ? DataType.GEO_SHAPE : DataType.CARTESIAN_SHAPE;
    }

    @Override
    protected NodeInfo<SpatialExtent> info() {
        return NodeInfo.create(this, SpatialExtent::new, field());
    }

    @Override
    public SpatialExtent replaceChildren(List<Expression> newChildren) {
        return new SpatialExtent(source(), newChildren.get(0));
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        DataType type = field().dataType();
        return switch (type) {
            case DataType.GEO_POINT -> switch (fieldExtractPreference) {
                case DOC_VALUES -> new SpatialExtentGeoPointDocValuesAggregatorFunctionSupplier();
                case NONE, EXTRACT_SPATIAL_BOUNDS, STORED -> new SpatialExtentGeoPointSourceValuesAggregatorFunctionSupplier();
            };
            case DataType.CARTESIAN_POINT -> switch (fieldExtractPreference) {
                case DOC_VALUES -> new SpatialExtentCartesianPointDocValuesAggregatorFunctionSupplier();
                case NONE, EXTRACT_SPATIAL_BOUNDS, STORED -> new SpatialExtentCartesianPointSourceValuesAggregatorFunctionSupplier();
            };
            case DataType.GEO_SHAPE -> switch (fieldExtractPreference) {
                case EXTRACT_SPATIAL_BOUNDS -> new SpatialExtentGeoShapeDocValuesAggregatorFunctionSupplier();
                case NONE, STORED -> new SpatialExtentGeoShapeSourceValuesAggregatorFunctionSupplier();
                case DOC_VALUES -> throw new EsqlIllegalArgumentException("Illegal field extract preference: " + fieldExtractPreference);
            };
            case DataType.CARTESIAN_SHAPE -> switch (fieldExtractPreference) {
                case EXTRACT_SPATIAL_BOUNDS -> new SpatialExtentCartesianShapeDocValuesAggregatorFunctionSupplier();
                case NONE, STORED -> new SpatialExtentCartesianShapeSourceValuesAggregatorFunctionSupplier();
                case DOC_VALUES -> throw new EsqlIllegalArgumentException("Illegal field extract preference: " + fieldExtractPreference);
            };
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }
}
