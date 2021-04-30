/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;

public class GeoTileGridAggregatorTests extends GeoGridAggregatorTestCase<InternalGeoTileGridBucket> {

    @Override
    protected int randomPrecision() {
        return randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
    }

    @Override
    protected String hashAsString(double lng, double lat, int precision) {
        return GeoTileUtils.stringEncode(GeoTileUtils.longEncode(lng, lat, precision));
    }

    @Override
    protected Point randomPoint() {
        return new Point(randomDoubleBetween(GeoUtils.MIN_LON, GeoUtils.MAX_LON, true),
            randomDoubleBetween(-GeoTileUtils.LATITUDE_MASK, GeoTileUtils.LATITUDE_MASK, false));
    }

    @Override
    protected GeoBoundingBox randomBBox() {
        GeoBoundingBox bbox = randomValueOtherThanMany(
            (b) -> b.top() > GeoTileUtils.LATITUDE_MASK || b.bottom() < -GeoTileUtils.LATITUDE_MASK,
            () -> {
                Rectangle rectangle = GeometryTestUtils.randomRectangle();
                return new GeoBoundingBox(new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
                    new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon()));
            });
        // Avoid numerical errors for sub-atomic values
        double left = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(bbox.left()));
        double right = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(bbox.right()));
        double top = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(bbox.top()));
        double bottom = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(bbox.bottom()));
        bbox.topLeft().reset(top, left);
        bbox.bottomRight().reset(bottom, right);
        return bbox;
    }

    @Override
    protected Rectangle getTile(double lng, double lat, int precision) {
        long tiles =  1 << precision;
        int x = GeoTileUtils.getXTile(lng, tiles);
        int y = GeoTileUtils.getYTile(lat, tiles);
        Rectangle r1 = GeoTileUtils.toBoundingBox(x, y, precision);
        Rectangle r2 = GeoTileUtils.toBoundingBox(GeoTileUtils.longEncode(lng, lat, precision));
        if (r1.equals(r2) == false) {
            int a =0;
        }
        return GeoTileUtils.toBoundingBox(GeoTileUtils.longEncode(lng, lat, precision));
    }

    @Override
    protected GeoGridAggregationBuilder createBuilder(String name) {
        return new GeoTileGridAggregationBuilder(name);
    }

    public void testPrecision() {
        final GeoGridAggregationBuilder builder = createBuilder("_name");

        expectThrows(IllegalArgumentException.class, () -> builder.precision(-1));
        expectThrows(IllegalArgumentException.class, () -> builder.precision(30));

        int precision = randomIntBetween(0, 29);
        builder.precision(precision);
        assertEquals(precision, builder.precision());
    }
}
