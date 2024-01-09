/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SpatialCentroidCartesianPointAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SpatialCentroidGeoPointAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.planner.ToAggregator;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;

import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatial;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;

/**
 * Calculate spatial centroid of all geo_point or cartesian point values of a field in matching documents.
 */
public class SpatialCentroid extends AggregateFunction implements ToAggregator {

    public SpatialCentroid(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        return isSpatial(field(), sourceText(), DEFAULT);
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
        if (type == EsqlDataTypes.GEO_POINT) {
            return new SpatialCentroidGeoPointAggregatorFunctionSupplier(inputChannels);
        }
        if (type == EsqlDataTypes.CARTESIAN_POINT) {
            return new SpatialCentroidCartesianPointAggregatorFunctionSupplier(inputChannels);
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }
}
