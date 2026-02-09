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
import org.elasticsearch.compute.aggregation.spatial.SpatialCentroidCartesianPointDocValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialCentroidGeoPointDocValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialCentroidPointSourceValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialCentroidShapeSourceValuesAggregatorFunctionSupplier;
import org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference;
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
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.NONE;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatial;

/**
 * Calculate spatial centroid of all geo_point, cartesian_point, geo_shape, or cartesian_shape values of a field in matching documents.
 */
public class SpatialCentroid extends SpatialAggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "SpatialCentroid",
        SpatialCentroid::new
    );

    @FunctionInfo(
        returnType = { "geo_point", "cartesian_point" },
        preview = true,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW) },
        description = """
            Calculate the spatial centroid over a field with spatial geometry type.
            Supports `geo_point` and `cartesian_point`, as well as `geo_shape` and `cartesian_shape` {applies_to}`stack: preview 9.4`.""",
        type = FunctionType.AGGREGATE,
        examples = @Example(file = "spatial", tag = "st_centroid_agg-airports")
    )
    public SpatialCentroid(
        Source source,
        @Param(name = "field", type = { "geo_point", "cartesian_point", "geo_shape", "cartesian_shape" }) Expression field
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW, NONE);
    }

    private SpatialCentroid(Source source, Expression field, Expression filter, Expression window, FieldExtractPreference preference) {
        super(source, field, filter, window, preference);
    }

    private SpatialCentroid(StreamInput in) throws IOException {
        super(in, NONE);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public SpatialCentroid withFilter(Expression filter) {
        return new SpatialCentroid(source(), field(), filter, window(), fieldExtractPreference);
    }

    @Override
    public SpatialCentroid withFieldExtractPreference(FieldExtractPreference preference) {
        return new SpatialCentroid(source(), field(), filter(), window(), preference);
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        return isSpatial(field(), sourceText(), DEFAULT);
    }

    @Override
    public DataType dataType() {
        // We aggregate incoming spatial types into a single point of the corresponding type
        // (geo types return geo_point, cartesian types return cartesian_point)
        return DataType.isSpatialGeo(field().dataType()) ? DataType.GEO_POINT : DataType.CARTESIAN_POINT;
    }

    @Override
    protected NodeInfo<SpatialCentroid> info() {
        return NodeInfo.create(this, SpatialCentroid::new, field());
    }

    @Override
    public SpatialCentroid replaceChildren(List<Expression> newChildren) {
        return new SpatialCentroid(source(), newChildren.get(0));
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        DataType type = field().dataType();
        return switch (type) {
            case DataType.GEO_POINT -> switch (fieldExtractPreference) {
                case DOC_VALUES -> new SpatialCentroidGeoPointDocValuesAggregatorFunctionSupplier();
                case NONE, EXTRACT_SPATIAL_BOUNDS, STORED -> new SpatialCentroidPointSourceValuesAggregatorFunctionSupplier();
            };
            case DataType.CARTESIAN_POINT -> switch (fieldExtractPreference) {
                case DOC_VALUES -> new SpatialCentroidCartesianPointDocValuesAggregatorFunctionSupplier();
                case NONE, EXTRACT_SPATIAL_BOUNDS, STORED -> new SpatialCentroidPointSourceValuesAggregatorFunctionSupplier();
            };
            case DataType.GEO_SHAPE, DataType.CARTESIAN_SHAPE -> switch (fieldExtractPreference) {
                case NONE, STORED -> new SpatialCentroidShapeSourceValuesAggregatorFunctionSupplier();
                case DOC_VALUES, EXTRACT_SPATIAL_BOUNDS -> throw new EsqlIllegalArgumentException(
                    "Unsupported field extraction preference [" + fieldExtractPreference + "] for shape type [" + type + "]"
                );
            };
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }
}
