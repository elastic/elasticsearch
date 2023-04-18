/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

public class GeoHashBoundedPredicateTests extends ESTestCase {

    public void testValidTile() {
        int precision = 3;
        String hash = "bcd";
        Rectangle rectangle = Geohash.toBoundingBox(hash);
        GeoBoundingBox bbox = new GeoBoundingBox(
            new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
            new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
        );
        GeoHashBoundedPredicate predicate = new GeoHashBoundedPredicate(precision, bbox);
        // the same tile should be valid
        assertEquals(true, predicate.validHash(hash));
        // neighbour tiles only touching should not be valid
        assertEquals(false, predicate.validHash("bcc"));
        assertEquals(false, predicate.validHash("bcf"));
        assertEquals(false, predicate.validHash("bcg"));
        assertEquals(false, predicate.validHash("bc9"));
        assertEquals(false, predicate.validHash("bce"));
        assertEquals(false, predicate.validHash("bc3"));
        assertEquals(false, predicate.validHash("bc6"));
        assertEquals(false, predicate.validHash("bc7"));
    }

    public void testRandomValidTile() {
        int precision = randomIntBetween(1, Geohash.PRECISION);
        String hash = Geohash.stringEncode(GeoTestUtil.nextLongitude(), GeoTestUtil.nextLatitude(), precision);
        Rectangle rectangle = Geohash.toBoundingBox(hash);
        GeoBoundingBox bbox = new GeoBoundingBox(
            new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
            new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
        );
        GeoHashBoundedPredicate predicate = new GeoHashBoundedPredicate(precision, bbox);

        assertPredicates(hash, predicate, bbox.left(), bbox.bottom());
        assertPredicates(hash, predicate, bbox.left(), bbox.top());
        assertPredicates(hash, predicate, bbox.right(), bbox.top());
        assertPredicates(hash, predicate, bbox.right(), bbox.bottom());

        for (int i = 0; i < 10000; i++) {
            assertPredicates(hash, predicate, GeoTestUtil.nextLongitude(), GeoTestUtil.nextLatitude());
        }
    }

    public void testMaxHash() {
        int precision = randomIntBetween(1, Geohash.PRECISION);
        String hash = Geohash.stringEncode(GeoTestUtil.nextLongitude(), GeoTestUtil.nextLatitude(), precision);
        Rectangle rectangle = Geohash.toBoundingBox(hash);
        GeoBoundingBox bbox = new GeoBoundingBox(
            new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
            new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
        );
        for (int i = precision; i < Geohash.PRECISION; i++) {
            GeoHashBoundedPredicate predicate = new GeoHashBoundedPredicate(i, bbox);
            // not exact due to numerical errors
            assertThat(predicate.getMaxHashes(), Matchers.greaterThanOrEqualTo((long) Math.pow(32, (i - precision))));
        }
    }

    private void assertPredicates(String hash, GeoHashBoundedPredicate predicate, double lon, double lat) {
        String newhash = Geohash.stringEncode(lon, lat, hash.length());
        assertEquals(newhash.equals(hash), predicate.validHash(newhash));
    }
}
