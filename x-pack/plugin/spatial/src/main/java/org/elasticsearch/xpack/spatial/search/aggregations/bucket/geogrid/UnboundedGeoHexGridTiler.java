/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.h3.H3;
import org.elasticsearch.xpack.spatial.common.H3CartesianUtil;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;

import java.io.IOException;

/**
 * Unbounded geohex aggregation. It accepts any hash.
 */
public class UnboundedGeoHexGridTiler extends AbstractGeoHexGridTiler {

    private final long maxAddresses;

    private final GeoHexVisitor visitor;

    public UnboundedGeoHexGridTiler(int precision) {
        super(precision);
        this.visitor = new GeoHexVisitor();
        maxAddresses = calcMaxAddresses(precision);
    }

    @Override
    protected boolean h3IntersectsBounds(long h3) {
        return true;
    }

    @Override
    protected GeoRelation relateTile(GeoShapeValues.GeoShapeValue geoValue, long h3) throws IOException {
        visitor.reset(h3);
        final int resolution = H3.getResolution(h3);
        if (resolution != precision
            && (visitor.getMaxY() > H3CartesianUtil.getNorthPolarBound(resolution)
                || visitor.getMinY() < H3CartesianUtil.getSouthPolarBound(resolution))) {
            // close to the poles, the properties of the H3 grid are lost because of the equirectangular projection,
            // therefore we cannot ensure that the relationship at this level make any sense in the next level.
            // Therefore, we just return CROSSES which just mean keep recursing.
            return GeoRelation.QUERY_CROSSES;
        }
        geoValue.visit(visitor);
        return visitor.relation();
    }

    @Override
    protected boolean valueInsideBounds(GeoShapeValues.GeoShapeValue geoValue) {
        return true;
    }

    @Override
    protected long getMaxCells() {
        return maxAddresses;
    }

    public static long calcMaxAddresses(int precision) {
        // TODO: Verify this (and perhaps move the calculation into H3 and based on NUM_BASE_CELLS and others)
        final int baseHexagons = 110;
        final int basePentagons = 12;
        return baseHexagons * (long) Math.pow(7, precision) + basePentagons * (long) Math.pow(6, precision);
    }
}
