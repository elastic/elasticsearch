/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.h3.LatLng;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.geom.TestGeometryCollector;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class GeoHexBoundedPredicateTests extends ESTestCase {

    private final TestGeometryCollector testGeometryCollector = TestGeometryCollector.createGeometryCollector();
    // Uncomment the following line to get export of WKT geometries for visual inspection
    // private final TestGeometryCollector testGeometryCollector = TestGeometryCollector.createWKTExporter("GeoHexBoundedPredicateTests");

    public void testValidTile() {
        testGeometryCollector.start("testValidTile");
        int precision = 3;
        double x = 25;
        double y = 25;
        String h3 = H3.h3ToString(H3.geoToH3(y, x, precision));
        GeoBoundingBox bbox = bboxOf(h3);
        testGeometryCollector.normal(c -> {
            c.addPoint(x, y);
            c.addBox(bbox);
        });
        GeoHexBoundedPredicate predicate = new GeoHexBoundedPredicate(precision, bbox);
        assertTrue("Original H3 cell should match predicate", predicate.validAddress(h3));
        // unlike with rectangular tiles, we cannot assert that immediate neighbours do not intersect
        // (bounds predicate does not match the cell itself)
        // instead we select a group of near neighbours, and if their bounds intersect our test bounds, we assert the predicate
        String grandparent = H3.h3ToParent(H3.h3ToParent(h3));
        for (String parent : H3.h3ToChildren(grandparent)) {
            for (String child : H3.h3ToChildren(parent)) {
                assertPredicate(bbox, child, predicate);
            }
        }
        testGeometryCollector.stop((failedPredicate, passedPredicate) -> {
            assertThat(
                "Should have more non-matching geometries than matching",
                failedPredicate.size(),
                greaterThan(passedPredicate.size())
            );
            assertThat("Should have at least the original cell and a few neighbours", passedPredicate.size(), greaterThan(4));
        });
    }

    private static GeoBoundingBox bboxOf(String h3) {
        CellBoundary boundary = H3.h3ToGeoBoundary(h3);
        double minX = Float.MAX_VALUE;
        double maxX = -Float.MAX_VALUE;
        double minY = Float.MAX_VALUE;
        double maxY = -Float.MAX_VALUE;
        for (int i = 0; i < boundary.numPoints(); i++) {
            LatLng vertex = boundary.getLatLon(i);
            minX = min(minX, vertex.getLonDeg());
            maxX = max(maxX, vertex.getLonDeg());
            minY = min(minY, vertex.getLatDeg());
            maxY = max(maxY, vertex.getLatDeg());
        }
        return new GeoBoundingBox(new GeoPoint(maxY, minX), new GeoPoint(minY, maxX));
    }

    private static boolean boundsIntersects(GeoBoundingBox bbox, String h3) {
        GeoBoundingBox other = bboxOf(h3);
        return bbox.pointInBounds(other.left(), other.bottom())
            || bbox.pointInBounds(other.right(), other.bottom())
            || bbox.pointInBounds(other.right(), other.top())
            || bbox.pointInBounds(other.left(), other.top());
    }

    public void testRandomValidTile() {
        testGeometryCollector.start("testRandomValidTile");
        int precision = randomIntBetween(0, H3.MAX_H3_RES);
        double x = GeoTestUtil.nextLongitude();
        double y = GeoTestUtil.nextLatitude();
        String h3 = H3.h3ToString(H3.geoToH3(y, x, precision));
        GeoBoundingBox bbox = bboxOf(h3);
        testGeometryCollector.normal(c -> {
            c.addPoint(x, y);
            c.addBox(bbox);
        });
        GeoHexBoundedPredicate predicate = new GeoHexBoundedPredicate(precision, bbox);
        assertPredicate(bbox, h3, predicate);
        // unlike with rectangular tiles, we cannot assert that immediate neighbours do not intersect
        // (bounds predicate does not match the cell itself)
        // instead if a cells bounds intersect our test bounds, we assert the predicate
        for (int i = 0; i < 1000; i++) {
            String h3Address = H3.geoToH3Address(GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude(), precision);
            assertPredicate(bbox, h3Address, predicate);
        }
        testGeometryCollector.stop((failedPredicate, passedPredicate) -> {
            assertThat(
                "Should have more non-matching geometries than matching",
                failedPredicate.size(),
                greaterThan(passedPredicate.size())
            );
            assertThat("Should have at least the original cell", passedPredicate.size(), greaterThan(0));
        });
    }

    private void assertPredicate(GeoBoundingBox bbox, String h3Address, GeoHexBoundedPredicate predicate) {
        boolean expected = boundsIntersects(bbox, h3Address);
        testGeometryCollector.highlighted(expected).addH3Cell(h3Address);
        assertThat(h3Address, predicate.validAddress(h3Address), is(expected));
    }
}
