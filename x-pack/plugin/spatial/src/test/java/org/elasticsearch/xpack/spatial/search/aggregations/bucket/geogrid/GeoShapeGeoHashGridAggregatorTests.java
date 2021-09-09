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
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoHashGridBucket;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import static org.elasticsearch.geometry.utils.Geohash.stringEncode;

public class GeoShapeGeoHashGridAggregatorTests extends GeoShapeGeoGridTestCase<InternalGeoHashGridBucket> {

    @Override
    protected int randomPrecision() {
        return randomIntBetween(1, 12);
    }

    @Override
    protected String hashAsString(double lng, double lat, int precision) {
        return stringEncode(lng, lat, precision);
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
    protected Rectangle getTile(double lng, double lat, int precision) {
        return Geohash.toBoundingBox(stringEncode(lng, lat, precision));
    }

    @Override
    protected GeoGridAggregationBuilder createBuilder(String name) {
        return new GeoHashGridAggregationBuilder(name);
    }
}
