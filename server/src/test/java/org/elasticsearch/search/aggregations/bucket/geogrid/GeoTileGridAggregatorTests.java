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
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

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
        return new Point(
            randomDoubleBetween(GeoUtils.MIN_LON, GeoUtils.MAX_LON, true),
            randomDoubleBetween(-GeoTileUtils.LATITUDE_MASK, GeoTileUtils.LATITUDE_MASK, false)
        );
    }

    @Override
    protected GeoBoundingBox randomBBox(int precision) {
        GeoBoundingBox bbox = randomValueOtherThanMany(
            (b) -> b.top() > GeoTileUtils.LATITUDE_MASK || b.bottom() < -GeoTileUtils.LATITUDE_MASK,
            () -> {
                Rectangle rectangle = GeometryTestUtils.randomRectangle();
                return new GeoBoundingBox(
                    new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
                    new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
                );
            }
        );
        final int tiles = 1 << precision;

        // Due to the way GeoTileBoundedPredicate works that adjust the given bounding box when it is touching the tiles, we need to
        // adjust here in order not to generate bounding boxes touching tiles or the test will fail

        // compute tile at the top left
        final Rectangle minTile = GeoTileUtils.toBoundingBox(
            GeoTileUtils.getXTile(bbox.left(), tiles),
            GeoTileUtils.getYTile(bbox.top(), tiles),
            precision
        );
        // adjust if it is touching the tile
        final int encodedLeft = encodeLongitude(bbox.left());
        final double left = encodeLongitude(minTile.getMaxX()) == encodedLeft
            ? decodeLongitude(encodedLeft + 1)
            : decodeLongitude(encodedLeft);
        final int encodedTop = encodeLatitude(bbox.top());
        final double bottom = encodeLatitude(minTile.getMinY()) == encodedTop ? decodeLatitude(encodedTop + 1) : decodeLatitude(encodedTop);
        // compute tile at the bottom right
        final Rectangle maxTile = GeoTileUtils.toBoundingBox(
            GeoTileUtils.getXTile(bbox.right(), tiles),
            GeoTileUtils.getYTile(bbox.bottom(), tiles),
            precision
        );
        // adjust if it is touching the tile
        final int encodedRight = encodeLongitude(bbox.right());
        final double right = encodeLongitude(maxTile.getMinX()) == encodedRight
            ? decodeLongitude(encodedRight)
            : decodeLongitude(encodedRight + 1);
        final int encodedBottom = encodeLatitude(bbox.bottom());
        final double top = encodeLatitude(maxTile.getMaxY()) == encodedBottom
            ? decodeLatitude(encodedBottom)
            : decodeLatitude(encodedBottom + 1);

        bbox.topLeft().reset(top, left);
        bbox.bottomRight().reset(bottom, right);
        return bbox;
    }

    @Override
    protected Rectangle getTile(double lng, double lat, int precision) {
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
