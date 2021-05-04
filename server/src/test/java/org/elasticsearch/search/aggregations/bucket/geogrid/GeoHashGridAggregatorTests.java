/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;

import static org.elasticsearch.geometry.utils.Geohash.stringEncode;

public class GeoHashGridAggregatorTests extends GeoGridAggregatorTestCase<InternalGeoHashGridBucket> {

    @Override
    protected int randomPrecision() {
        return randomIntBetween(1, 12);
    }

    @Override
    protected Point randomPoint() {
        return GeometryTestUtils.randomPoint(false);
    }

    @Override
    protected GeoBoundingBox randomBBox() {
        Rectangle rectangle = GeometryTestUtils.randomRectangle();
        return new GeoBoundingBox(new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
            new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon()));
    }

    @Override
    protected Rectangle getTile(double lng, double lat, int precision) {
        return Geohash.toBoundingBox(stringEncode(lng, lat, precision));
    }

    @Override
    protected String hashAsString(double lng, double lat, int precision) {
        return stringEncode(lng, lat, precision);
    }

    @Override
    protected GeoGridAggregationBuilder createBuilder(String name) {
        return new GeoHashGridAggregationBuilder(name);
    }
}
