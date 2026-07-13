/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile.rest;

import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.test.ESTestCase;

public class GridAggregationTests extends ESTestCase {

    /** Make sure that buffering covers all points it should be covering */
    public void testGeoHexBufferTile() {
        final Point point = randomValueOtherThanMany(
            p -> Math.abs(p.getLat()) > GeoTileUtils.LATITUDE_MASK,
            GeometryTestUtils::randomPoint
        );
        final int z = randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
        final int x = GeoTileUtils.getXTile(point.getLon(), 1 << z);
        final int y = GeoTileUtils.getYTile(point.getLat(), 1 << z);
        // current tile
        final Rectangle tile = GeoTileUtils.toBoundingBox(x, y, z);
        for (int i = 1; i <= 8; i++) {
            final int geoHexPrecision = GridAggregation.GEOHEX.gridPrecisionToAggPrecision(z, i);
            // buffered tile
            final Rectangle bufferedTile = GridAggregation.GEOHEX.bufferTile(tile, z, i);
            // Hex bin of the original point
            final long l = H3.geoToH3(point.getLat(), point.getLon(), geoHexPrecision);
            final CellBoundary boundary = H3.h3ToGeoBoundary(l);
            // Check that all points of the hex bin are inside our buffered tile
            for (int j = 0; j < boundary.numPoints(); j++) {
                final double lat = boundary.getLatLon(j).getLatDeg();
                if (Math.abs(lat) <= GeoTileUtils.LATITUDE_MASK) { // We only consider points inside the mercator projection
                    assertTrue(bufferedTile.getMinLat() <= lat && bufferedTile.getMaxLat() >= lat);
                    final double lon = boundary.getLatLon(j).getLonDeg();
                    if (bufferedTile.getMinLon() < bufferedTile.getMaxLon()) {
                        assertTrue(bufferedTile.getMinLon() <= lon && bufferedTile.getMaxLon() >= lon);
                    } else {
                        assertTrue(bufferedTile.getMinLon() <= lon || bufferedTile.getMaxLon() >= lon);
                    }
                }
            }
        }
    }
}
