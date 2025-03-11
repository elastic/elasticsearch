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
import org.elasticsearch.compute.aggregation.spatial.SpatialCentroidCartesianPointSourceValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialCentroidGeoPointDocValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialCentroidGeoPointSourceValuesAggregatorFunctionSupplier;
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

import static org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference.NONE;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatialPoint;

/**
 * Calculate spatial centroid of all geo_point or cartesian point values of a field in matching documents.
 */
public class SpatialCentroid extends SpatialAggregateFunction implements ToAggregator {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "SpatialCentroid",
        SpatialCentroid::new
    );

    @FunctionInfo(
        returnType = { "geo_point", "cartesian_point" },
        description = "Calculate the spatial centroid over a field with spatial point geometry type.",
        type = FunctionType.AGGREGATE,
        examples = @Example(file = "spatial", tag = "st_centroid_agg-airports")
    )
    public SpatialCentroid(Source source, @Param(name = "field", type = { "geo_point", "cartesian_point" }) Expression field) {
        this(source, field, Literal.TRUE, NONE);
    }

    private SpatialCentroid(Source source, Expression field, Expression filter, FieldExtractPreference preference) {
        super(source, field, filter, preference);
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
        return new SpatialCentroid(source(), field(), filter, fieldExtractPreference);
    }

    @Override
    public SpatialCentroid withFieldExtractPreference(FieldExtractPreference preference) {
        return new SpatialCentroid(source(), field(), filter(), preference);
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        // TODO: Support geo_shape and cartesian_shape
        return isSpatialPoint(field(), sourceText(), DEFAULT);
    }

    @Override
    public DataType dataType() {
        // We aggregate incoming GEO_POINTs into a single GEO_POINT, or incoming CARTESIAN_POINTs into a single CARTESIAN_POINT.
        return field().dataType();
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
                case NONE, EXTRACT_SPATIAL_BOUNDS, STORED -> new SpatialCentroidGeoPointSourceValuesAggregatorFunctionSupplier();
            };
            case DataType.CARTESIAN_POINT -> switch (fieldExtractPreference) {
                case DOC_VALUES -> new SpatialCentroidCartesianPointDocValuesAggregatorFunctionSupplier();
                case NONE, EXTRACT_SPATIAL_BOUNDS, STORED -> new SpatialCentroidCartesianPointSourceValuesAggregatorFunctionSupplier();
            };
            default -> throw EsqlIllegalArgumentException.illegalDataType(type);
        };
    }
}
