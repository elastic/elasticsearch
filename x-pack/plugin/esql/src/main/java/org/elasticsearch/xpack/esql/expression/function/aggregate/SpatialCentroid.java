/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialCentroidCartesianPointDocValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialCentroidCartesianPointSourceValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialCentroidGeoPointDocValuesAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.spatial.SpatialCentroidGeoPointSourceValuesAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypes;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatialPoint;

/**
 * Calculate spatial centroid of all geo_point or cartesian point values of a field in matching documents.
 */
public class SpatialCentroid extends SpatialAggregateFunction implements ToAggregator {

    @FunctionInfo(returnType = { "geo_point", "cartesian_point" }, description = "The centroid of a spatial field.", isAggregation = true)
    public SpatialCentroid(Source source, @Param(name = "field", type = { "geo_point", "cartesian_point" }) Expression field) {
        super(source, field, false);
    }

    private SpatialCentroid(Source source, Expression field, boolean useDocValues) {
        super(source, field, useDocValues);
    }

    @Override
    public SpatialCentroid withDocValues() {
        return new SpatialCentroid(source(), field(), true);
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
    public AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        DataType type = field().dataType();
        if (useDocValues) {
            // When the points are read as doc-values (eg. from the index), feed them into the doc-values aggregator
            if (type == DataTypes.GEO_POINT) {
                return new SpatialCentroidGeoPointDocValuesAggregatorFunctionSupplier(inputChannels);
            }
            if (type == DataTypes.CARTESIAN_POINT) {
                return new SpatialCentroidCartesianPointDocValuesAggregatorFunctionSupplier(inputChannels);
            }
        } else {
            // When the points are read as WKB from source or as point literals, feed them into the source-values aggregator
            if (type == DataTypes.GEO_POINT) {
                return new SpatialCentroidGeoPointSourceValuesAggregatorFunctionSupplier(inputChannels);
            }
            if (type == DataTypes.CARTESIAN_POINT) {
                return new SpatialCentroidCartesianPointSourceValuesAggregatorFunctionSupplier(inputChannels);
            }
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }
}
