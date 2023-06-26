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
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.test.ESTestCase;

public class GeoTileBoundedPredicateTests extends ESTestCase {

    public void testValidTile() {
        int precision = 4;
        int x = 1;
        int y = 1;
        Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, precision);
        GeoBoundingBox bbox = new GeoBoundingBox(
            new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
            new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
        );

        GeoTileBoundedPredicate predicate = new GeoTileBoundedPredicate(precision, bbox);
        // the same tile should be valid
        assertEquals(true, predicate.validTile(x, y, precision));
        // neighbour tiles only touching should not be valid
        assertEquals(false, predicate.validTile(x + 1, y, precision));
        assertEquals(false, predicate.validTile(x - 1, y, precision));
        assertEquals(false, predicate.validTile(x, y + 1, precision));
        assertEquals(false, predicate.validTile(x, y - 1, precision));
        assertEquals(false, predicate.validTile(x + 1, y + 1, precision));
        assertEquals(false, predicate.validTile(x - 1, y - 1, precision));
        assertEquals(false, predicate.validTile(x + 1, y - 1, precision));
        assertEquals(false, predicate.validTile(x - 1, y + 1, precision));
    }

    public void testRandomValidTile() {
        int precision = randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
        int tiles = 1 << precision;
        int x = randomIntBetween(0, tiles - 1);
        int y = randomIntBetween(0, tiles - 1);
        Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, precision);
        GeoBoundingBox bbox = new GeoBoundingBox(
            new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
            new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
        );
        long hash = GeoTileUtils.longEncodeTiles(precision, x, y);
        GeoTileBoundedPredicate predicate = new GeoTileBoundedPredicate(precision, bbox);

        int minX = GeoTileUtils.getXTile(bbox.left(), tiles);
        int maxX = GeoTileUtils.getXTile(bbox.right(), tiles);
        int minY = GeoTileUtils.getYTile(bbox.bottom(), tiles);
        int maxY = GeoTileUtils.getYTile(bbox.top(), tiles);

        assertPredicates(hash, predicate, minX, minY, precision);
        assertPredicates(hash, predicate, maxX, minY, precision);
        assertPredicates(hash, predicate, minX, maxY, precision);
        assertPredicates(hash, predicate, maxX, maxY, precision);

        for (int i = 0; i < 1000; i++) {
            assertPredicates(hash, predicate, randomIntBetween(0, (1 << i) - 1), randomIntBetween(0, (1 << i) - 1), precision);
        }
    }

    public void testMaxTiles() {
        int precision = randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
        int x = randomIntBetween(0, (1 << precision) - 1);
        int y = randomIntBetween(0, (1 << precision) - 1);
        Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, precision);
        GeoBoundingBox bbox = new GeoBoundingBox(
            new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
            new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
        );
        for (int i = precision; i < GeoTileUtils.MAX_ZOOM; i++) {
            GeoTileBoundedPredicate predicate = new GeoTileBoundedPredicate(i, bbox);
            long l = 1L << (i - precision);
            assertEquals(predicate.getMaxTiles(), l * l);
        }
    }

    private void assertPredicates(long encoded, GeoTileBoundedPredicate predicate, int x, int y, int p) {
        assertEquals(GeoTileUtils.longEncodeTiles(p, x, y) == encoded, predicate.validTile(x, y, p));
    }
}
