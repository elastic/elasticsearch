/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.geo.geometry;

import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.geo.GeoTestUtil.nextLatitude;
import static org.apache.lucene.geo.GeoTestUtil.nextLongitude;
import static org.apache.lucene.geo.GeoTestUtil.nextPolygon;

/**
 * Test EdgeTree impl
 */
public class TestEdgeTree extends LuceneTestCase {

    /**
     * Three boxes, an island inside a hole inside a shape
     */
    public void testMultiPolygon() {
        Polygon hole = new Polygon(new double[]{-10, -10, 10, 10, -10}, new double[]{-10, 10, 10, -10, -10});
        Polygon outer = new Polygon(new double[]{-50, -50, 50, 50, -50}, new double[]{-50, 50, 50, -50, -50}, hole);
        Polygon island = new Polygon(new double[]{-5, -5, 5, 5, -5}, new double[]{-5, 5, 5, -5, -5});
        EdgeTree polygon = EdgeTree.create(outer, island);

        // contains(point)
        assertTrue(polygon.contains(-2, 2)); // on the island
        assertFalse(polygon.contains(-6, 6)); // in the hole
        assertTrue(polygon.contains(-25, 25)); // on the mainland
        assertFalse(polygon.contains(-51, 51)); // in the ocean

        // relate(box): this can conservatively return CELL_CROSSES_QUERY
        assertEquals(Relation.CELL_INSIDE_QUERY, polygon.relate(-2, 2, -2, 2)); // on the island
        assertEquals(Relation.CELL_OUTSIDE_QUERY, polygon.relate(6, 7, 6, 7)); // in the hole
        assertEquals(Relation.CELL_INSIDE_QUERY, polygon.relate(24, 25, 24, 25)); // on the mainland
        assertEquals(Relation.CELL_OUTSIDE_QUERY, polygon.relate(51, 52, 51, 52)); // in the ocean
        assertEquals(Relation.CELL_CROSSES_QUERY, polygon.relate(-60, 60, -60, 60)); // enclosing us completely
        assertEquals(Relation.CELL_CROSSES_QUERY, polygon.relate(49, 51, 49, 51)); // overlapping the mainland
        assertEquals(Relation.CELL_CROSSES_QUERY, polygon.relate(9, 11, 9, 11)); // overlapping the hole
        assertEquals(Relation.CELL_CROSSES_QUERY, polygon.relate(5, 6, 5, 6)); // overlapping the island
    }

    public void testPacMan() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5, 0};

        // candidate crosses cell
        double xMin = 2;//-5;
        double xMax = 11;//0.000001;
        double yMin = -1;//0;
        double yMax = 1;//5;

        // test cell crossing poly
        EdgeTree polygon = EdgeTree.create(new Polygon(py, px));
        assertEquals(Relation.CELL_CROSSES_QUERY, polygon.relate(yMin, yMax, xMin, xMax));
    }

    public void testBoundingBox() throws Exception {
        for (int i = 0; i < 100; i++) {
            EdgeTree polygon = EdgeTree.create(nextPolygon());

            for (int j = 0; j < 100; j++) {
                double latitude = nextLatitude();
                double longitude = nextLongitude();
                // if the point is within poly, then it should be in our bounding box
                if (polygon.contains(latitude, longitude)) {
                    assertTrue(latitude >= polygon.minLat && latitude <= polygon.maxLat);
                    assertTrue(longitude >= polygon.minLon && longitude <= polygon.maxLon);
                }
            }
        }
    }

    // targets the bounding box directly
    public void testBoundingBoxEdgeCases() throws Exception {
        for (int i = 0; i < 100; i++) {
            Polygon polygon = nextPolygon();
            EdgeTree impl = EdgeTree.create(polygon);

            for (int j = 0; j < 100; j++) {
                double point[] = GeoTestUtil.nextPointNear(polygon);
                double latitude = point[0];
                double longitude = point[1];
                // if the point is within poly, then it should be in our bounding box
                if (impl.contains(latitude, longitude)) {
                    assertTrue(latitude >= polygon.minLat() && latitude <= polygon.maxLat());
                    assertTrue(longitude >= polygon.minLon() && longitude <= polygon.maxLon());
                }
            }
        }
    }

    /**
     * If polygon.contains(box) returns true, then any point in that box should return true as well
     */
    public void testContainsRandom() throws Exception {
        int iters = atLeast(50);
        for (int i = 0; i < iters; i++) {
            Polygon polygon = nextPolygon();
            EdgeTree impl = EdgeTree.create(polygon);

            for (int j = 0; j < 100; j++) {
                Rectangle rectangle = GeoTestUtil.nextBoxNear(polygon);
                // allowed to conservatively return false
                if (impl.relate(rectangle.minLat, rectangle.maxLat, rectangle.minLon, rectangle.maxLon) == GeoShape.Relation.WITHIN) {
                    for (int k = 0; k < 500; k++) {
                        // this tests in our range but sometimes outside! so we have to double-check its really in other box
                        double point[] = GeoTestUtil.nextPointNear(rectangle);
                        double latitude = point[0];
                        double longitude = point[1];
                        // check for sure its in our box
                        if (latitude >= rectangle.minLat && latitude <= rectangle.maxLat && longitude >= rectangle.minLon && longitude <= rectangle.maxLon) {
                            assertTrue(impl.contains(latitude, longitude));
                        }
                    }
                    for (int k = 0; k < 100; k++) {
                        // this tests in our range but sometimes outside! so we have to double-check its really in other box
                        double point[] = GeoTestUtil.nextPointNear(polygon);
                        double latitude = point[0];
                        double longitude = point[1];
                        // check for sure its in our box
                        if (latitude >= rectangle.minLat && latitude <= rectangle.maxLat && longitude >= rectangle.minLon && longitude <= rectangle.maxLon) {
                            assertTrue(impl.contains(latitude, longitude));
                        }
                    }
                }
            }
        }
    }

    /**
     * If polygon.contains(box) returns true, then any point in that box should return true as well
     */
    // different from testContainsRandom in that its not a purely random test. we iterate the vertices of the polygon
    // and generate boxes near each one of those to try to be more efficient.
    public void testContainsEdgeCases() throws Exception {
        for (int i = 0; i < 1000; i++) {
            Polygon polygon = nextPolygon();
            EdgeTree impl = EdgeTree.create(polygon);

            for (int j = 0; j < 10; j++) {
                Rectangle rectangle = GeoTestUtil.nextBoxNear(polygon);
                // allowed to conservatively return false
                if (impl.relate(rectangle.minLat, rectangle.maxLat, rectangle.minLon, rectangle.maxLon) == GeoShape.Relation.WITHIN) {
                    for (int k = 0; k < 100; k++) {
                        // this tests in our range but sometimes outside! so we have to double-check its really in other box
                        double point[] = GeoTestUtil.nextPointNear(rectangle);
                        double latitude = point[0];
                        double longitude = point[1];
                        // check for sure its in our box
                        if (latitude >= rectangle.minLat && latitude <= rectangle.maxLat && longitude >= rectangle.minLon && longitude <= rectangle.maxLon) {
                            assertTrue(impl.contains(latitude, longitude));
                        }
                    }
                    for (int k = 0; k < 20; k++) {
                        // this tests in our range but sometimes outside! so we have to double-check its really in other box
                        double point[] = GeoTestUtil.nextPointNear(polygon);
                        double latitude = point[0];
                        double longitude = point[1];
                        // check for sure its in our box
                        if (latitude >= rectangle.minLat && latitude <= rectangle.maxLat && longitude >= rectangle.minLon && longitude <= rectangle.maxLon) {
                            assertTrue(impl.contains(latitude, longitude));
                        }
                    }
                }
            }
        }
    }

    /**
     * If polygon.intersects(box) returns false, then any point in that box should return false as well
     */
    public void testIntersectRandom() {
        int iters = atLeast(10);
        for (int i = 0; i < iters; i++) {
            Polygon polygon = nextPolygon();
            EdgeTree impl = EdgeTree.create(polygon);

            for (int j = 0; j < 100; j++) {
                Rectangle rectangle = GeoTestUtil.nextBoxNear(polygon);
                // allowed to conservatively return true.
                if (impl.relate(rectangle.minLat, rectangle.maxLat, rectangle.minLon, rectangle.maxLon) == GeoShape.Relation.DISJOINT) {
                    for (int k = 0; k < 1000; k++) {
                        double point[] = GeoTestUtil.nextPointNear(rectangle);
                        // this tests in our range but sometimes outside! so we have to double-check its really in other box
                        double latitude = point[0];
                        double longitude = point[1];
                        // check for sure its in our box
                        if (latitude >= rectangle.minLat && latitude <= rectangle.maxLat && longitude >= rectangle.minLon && longitude <= rectangle.maxLon) {
                            assertFalse(impl.contains(latitude, longitude));
                        }
                    }
                    for (int k = 0; k < 100; k++) {
                        double point[] = GeoTestUtil.nextPointNear(polygon);
                        // this tests in our range but sometimes outside! so we have to double-check its really in other box
                        double latitude = point[0];
                        double longitude = point[1];
                        // check for sure its in our box
                        if (latitude >= rectangle.minLat && latitude <= rectangle.maxLat && longitude >= rectangle.minLon && longitude <= rectangle.maxLon) {
                            assertFalse(impl.contains(latitude, longitude));
                        }
                    }
                }
            }
        }
    }

    /**
     * If polygon.intersects(box) returns false, then any point in that box should return false as well
     */
    // different from testIntersectsRandom in that its not a purely random test. we iterate the vertices of the polygon
    // and generate boxes near each one of those to try to be more efficient.
    public void testIntersectEdgeCases() {
        for (int i = 0; i < 100; i++) {
            Polygon polygon = nextPolygon();
            EdgeTree impl = EdgeTree.create(polygon);

            for (int j = 0; j < 10; j++) {
                Rectangle rectangle = GeoTestUtil.nextBoxNear(polygon);
                // allowed to conservatively return false.
                if (impl.relate(rectangle.minLat, rectangle.maxLat, rectangle.minLon, rectangle.maxLon) == GeoShape.Relation.DISJOINT) {
                    for (int k = 0; k < 100; k++) {
                        // this tests in our range but sometimes outside! so we have to double-check its really in other box
                        double point[] = GeoTestUtil.nextPointNear(rectangle);
                        double latitude = point[0];
                        double longitude = point[1];
                        // check for sure its in our box
                        if (latitude >= rectangle.minLat && latitude <= rectangle.maxLat && longitude >= rectangle.minLon && longitude <= rectangle.maxLon) {
                            assertFalse(impl.contains(latitude, longitude));
                        }
                    }
                    for (int k = 0; k < 50; k++) {
                        // this tests in our range but sometimes outside! so we have to double-check its really in other box
                        double point[] = GeoTestUtil.nextPointNear(polygon);
                        double latitude = point[0];
                        double longitude = point[1];
                        // check for sure its in our box
                        if (latitude >= rectangle.minLat && latitude <= rectangle.maxLat && longitude >= rectangle.minLon && longitude <= rectangle.maxLon) {
                            assertFalse(impl.contains(latitude, longitude));
                        }
                    }
                }
            }
        }
    }

    /**
     * Tests edge case behavior with respect to insideness
     */
    public void testEdgeInsideness() {
        EdgeTree poly = EdgeTree.create(new Polygon(new double[]{-2, -2, 2, 2, -2}, new double[]{-2, 2, 2, -2, -2}));
        assertTrue(poly.contains(-2, -2)); // bottom left corner: true
        assertFalse(poly.contains(-2, 2));  // bottom right corner: false
        assertFalse(poly.contains(2, -2));  // top left corner: false
        assertFalse(poly.contains(2, 2));  // top right corner: false
        assertTrue(poly.contains(-2, -1)); // bottom side: true
        assertTrue(poly.contains(-2, 0));  // bottom side: true
        assertTrue(poly.contains(-2, 1));  // bottom side: true
        assertFalse(poly.contains(2, -1));  // top side: false
        assertFalse(poly.contains(2, 0));   // top side: false
        assertFalse(poly.contains(2, 1));   // top side: false
        assertFalse(poly.contains(-1, 2));  // right side: false
        assertFalse(poly.contains(0, 2));   // right side: false
        assertFalse(poly.contains(1, 2));   // right side: false
        assertTrue(poly.contains(-1, -2)); // left side: true
        assertTrue(poly.contains(0, -2));  // left side: true
        assertTrue(poly.contains(1, -2));  // left side: true
    }

    /**
     * Tests current impl against original algorithm
     */
    public void testContainsAgainstOriginal() {
        int iters = atLeast(100);
        for (int i = 0; i < iters; i++) {
            Polygon polygon = nextPolygon();
            // currently we don't generate these, but this test does not want holes.
            while (polygon.getHoles().length > 0) {
                polygon = nextPolygon();
            }
            EdgeTree impl = EdgeTree.create(polygon);

            // random lat/lons against polygon
            for (int j = 0; j < 1000; j++) {
                double point[] = GeoTestUtil.nextPointNear(polygon);
                double latitude = point[0];
                double longitude = point[1];
                boolean expected = GeoTestUtil.containsSlowly(polygon, latitude, longitude);
                assertEquals(expected, impl.contains(latitude, longitude));
            }
        }
    }
}
