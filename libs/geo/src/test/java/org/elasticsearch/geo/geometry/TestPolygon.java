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
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.geometry.GeoShape.Relation;

import static com.carrotsearch.randomizedtesting.generators.RandomNumbers.randomIntBetween;
import static org.apache.lucene.geo.GeoTestUtil.nextBoxNotCrossingDateline;
import static org.apache.lucene.geo.GeoTestUtil.nextLatitudeIn;
import static org.apache.lucene.geo.GeoTestUtil.nextLongitudeIn;

/**
 * Tests relations and features of Polygon types
 */
public class TestPolygon extends BaseGeometryTestCase<Polygon> {

    @Override
    public Polygon getShape() {
        return GeoTestUtil.nextPolygon();
    }

    public Polygon getShape(boolean padded) {
        double minFactor = padded == true ? 20D : 0D;
        double maxFactor = padded == true ? -20D : 0D;

        // we can't have self crossing lines.
        // since polygons share this contract we create an unclosed polygon
        Polygon poly = GeoTestUtil.createRegularPolygon(
            nextLatitudeIn(GeoUtils.MIN_LAT_INCL + minFactor, GeoUtils.MAX_LAT_INCL + maxFactor),
            nextLongitudeIn(GeoUtils.MIN_LON_INCL + minFactor, GeoUtils.MAX_LON_INCL + maxFactor),
            10000D, randomIntBetween(random(), 4, 100));
        return poly;
    }

    /**
     * tests area of a Polygon type using a simple random box
     */
    @Override
    public void testArea() {
        // test with simple random box
        Rectangle box = nextBoxNotCrossingDateline();
        Polygon polygon = new Polygon(
            new double[]{box.minLat(), box.minLat(), box.maxLat(), box.maxLat(), box.minLat()},
            new double[]{box.minLon(), box.maxLon(), box.maxLon(), box.minLon(), box.minLon()});
        double width = box.maxLon() - box.minLon();
        double height = box.maxLat() - box.minLat();
        double area = width * height;

        assertEquals(area, polygon.getArea(), 1E-10D);
    }

    @Override
    public void testBoundingBox() {
        Polygon polygon = getShape();
        double minLat = Double.POSITIVE_INFINITY;
        double maxLat = Double.NEGATIVE_INFINITY;
        double minLon = Double.POSITIVE_INFINITY;
        double maxLon = Double.NEGATIVE_INFINITY;
        // shell of polygon
        Line shell = new Line(polygon.getPolyLats(), polygon.getPolyLons());
        for (int i = 0; i < shell.numPoints(); ++i) {
            minLat = StrictMath.min(minLat, shell.getLat(i));
            maxLat = StrictMath.max(maxLat, shell.getLat(i));
            minLon = StrictMath.min(minLon, shell.getLon(i));
            maxLon = StrictMath.max(maxLon, shell.getLon(i));
        }

        Rectangle bbox = new Rectangle(minLat, maxLat, minLon, maxLon);
        assertEquals(bbox, polygon.getBoundingBox());
    }

    @Override
    public void testWithin() {
        relationTest(getShape(true), Relation.WITHIN);
    }

    @Override
    public void testContains() {
        Polygon polygon = getShape(true);
        Point center = polygon.getCenter();
        Rectangle box = new Rectangle(center.lat() - 1E-3D, center.lat() + 1E-3D, center.lon() - 1E-3D, center.lon() + 1E-3D);
        assertEquals(Relation.CONTAINS, polygon.relate(box.minLat(), box.maxLat(), box.minLon(), box.maxLon()));
    }

    @Override
    public void testDisjoint() {
        relationTest(getShape(true), Relation.DISJOINT);
    }

    @Override
    public void testIntersects() {
        Polygon polygon = getShape(true);
        double minLat = StrictMath.min(polygon.getLat(0), polygon.getLat(1));
        double maxLat = StrictMath.max(polygon.getLat(0), polygon.getLat(1));
        double minLon = StrictMath.min(polygon.getLon(0), polygon.getLon(1));
        double maxLon = StrictMath.max(polygon.getLon(0), polygon.getLon(1));
        assertEquals(Relation.INTERSECTS, polygon.relate(minLat, maxLat, minLon, maxLon));
    }

    public void testPacMan() {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5, 0};

        Polygon polygon = new Polygon(py, px);

        // candidate crosses cell
        double minLon = 2;
        double maxLon = 11;
        double minLat = -1;
        double maxLat = 1;

        // test cell crossing poly
        assertEquals(Relation.INTERSECTS, polygon.relate(minLat, maxLat, minLon, maxLon));
    }

    /**
     * null polyLats not allowed
     */
    public void testPolygonNullPolyLats() {
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
            new Polygon(null, new double[]{-66, -65, -65, -66, -66});
        });
        assertTrue(expected.getMessage().contains("lats must not be null"));
    }

    /**
     * null polyLons not allowed
     */
    public void testPolygonNullPolyLons() {
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
            new Polygon(new double[]{18, 18, 19, 19, 18}, null);
        });
        assertTrue(expected.getMessage().contains("lons must not be null"));
    }

    /**
     * polygon needs at least 3 vertices
     */
    public void testPolygonLine() {
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
            new Polygon(new double[]{18, 18, 18}, new double[]{-66, -65, -66});
        });
        assertTrue(expected.getMessage().contains("at least 4 polygon points required"));
    }

    /**
     * polygon needs same number of latitudes as longitudes
     */
    public void testPolygonBogus() {
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
            new Polygon(new double[]{18, 18, 19, 19}, new double[]{-66, -65, -65, -66, -66});
        });
        assertTrue(expected.getMessage().contains("must be equal length"));
    }

    /**
     * polygon must be closed
     */
    public void testPolygonNotClosed() {
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
            new Polygon(new double[]{18, 18, 19, 19, 19}, new double[]{-66, -65, -65, -66, -67});
        });
        assertTrue(expected.getMessage(), expected.getMessage().contains("it must close itself"));
    }
}
