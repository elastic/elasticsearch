/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.h3.H3;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.query.H3LatLonGeometry;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;

public class GeoShapeGeoHexGridAggregatorTests extends GeoShapeGeoGridTestCase<InternalGeoHexGridBucket> {

    @Override
    protected int randomPrecision() {
        return randomIntBetween(1, 12);
    }

    @Override
    protected String hashAsString(double lng, double lat, int precision) {
        return H3.geoToH3Address(lat, lng, precision);
    }

    @Override
    protected Point randomPoint() {
        return GeometryTestUtils.randomPoint(false);
    }

    @Override
    protected GeoBoundingBox randomBBox() {
        return GeoTestUtils.randomBBox();
    }

    @Override
    protected boolean intersects(double lng, double lat, int precision, GeoShapeValues.GeoShapeValue value) throws IOException {
        H3LatLonGeometry geometry = new H3LatLonGeometry(H3.geoToH3Address(lat, lng, precision));
        return value.relate(geometry) != GeoRelation.QUERY_DISJOINT;
    }

    @Override
    protected boolean intersectsBounds(double lng, double lat, int precision, GeoBoundingBox box) {
        GeoHexBoundedPredicate predicate = new GeoHexBoundedPredicate(precision, box);
        return predicate.validAddress(hashAsString(lng, lat, precision));
    }

    @Override
    protected GeoGridAggregationBuilder createBuilder(String name) {
        return new GeoHexGridAggregationBuilder(name);
    }
}
