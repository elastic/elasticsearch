/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.GeoCentroidGeoPointAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.planner.ToAggregator;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;

import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isGeoPoint;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_POINT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;

/**
 * Calculate geographic centroid of all GeoPoint values of a field in matching documents.
 */
public class GeoCentroid extends AggregateFunction implements ToAggregator {

    public GeoCentroid(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        return isGeoPoint(field(), sourceText(), DEFAULT);
    }

    @Override
    public DataType dataType() {
        return GEO_POINT;
    }

    @Override
    protected NodeInfo<GeoCentroid> info() {
        return NodeInfo.create(this, GeoCentroid::new, field());
    }

    @Override
    public GeoCentroid replaceChildren(List<Expression> newChildren) {
        return new GeoCentroid(source(), newChildren.get(0));
    }

    @Override
    public AggregatorFunctionSupplier supplier(BigArrays bigArrays, List<Integer> inputChannels) {
        return new GeoCentroidGeoPointAggregatorFunctionSupplier(bigArrays, inputChannels);
    }
}
