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
import org.junit.Ignore;

import java.util.Arrays;

import static com.carrotsearch.randomizedtesting.generators.RandomNumbers.randomIntBetween;
import static org.apache.lucene.geo.GeoTestUtil.nextLatitudeIn;
import static org.apache.lucene.geo.GeoTestUtil.nextLongitudeIn;

/**
 * Tests relations and features of Line types
 */
public class TestLine extends TestMultiPoint {
    @Override
    public Line getShape() {
        return getShape(false);
    }

    @Override
    public Line getShape(boolean padded) {
        double minFactor = padded == true ? 20D : 0D;
        double maxFactor = padded == true ? -20D : 0D;

        // we can't have self crossing lines.
        // since polygons share this contract we create an unclosed polygon
        Polygon poly = GeoTestUtil.createRegularPolygon(
            nextLatitudeIn(GeoUtils.MIN_LAT_INCL + minFactor, GeoUtils.MAX_LAT_INCL + maxFactor),
            nextLongitudeIn(GeoUtils.MIN_LON_INCL + minFactor, GeoUtils.MAX_LON_INCL + maxFactor),
            100D, randomIntBetween(random(), 4, 100));
        final double[] lats = poly.getPolyLats();
        final double[] lons = poly.getPolyLons();
        return new Line(Arrays.copyOfRange(lats, 0, lats.length - 1),
            Arrays.copyOfRange(lons, 0, lons.length - 1));
    }

    /**
     * tests the bounding box of the line
     */
    @Override
    public void testBoundingBox() {
        // lines are just MultiPoint with an edge tree; bounding box logic is the same
        super.testBoundingBox();
    }

    /**
     * tests the "center" of the line
     */
    @Override
    public void testCenter() {
        // lines are just MultiPoint with an edge tree; center logic is the same
        super.testCenter();
    }

    /**
     * tests MultiPoints are within a box
     */
    @Override
    public void testWithin() {
        relationTest(getShape(true), Relation.WITHIN);
    }

    /**
     * tests box is disjoint with a MultiPoint shape
     */
    @Override
    public void testDisjoint() {
        relationTest(getShape(true), Relation.DISJOINT);
    }

    /**
     * IGNORED: Line does not contain other shapes
     */
    @Ignore
    @Override
    public void testContains() {
    }

    /**
     * tests box intersection with a MultiPoint shape
     */
    @Override
    public void testIntersects() {
        MultiPoint points = getShape(true);
        Line line = new Line(points.getLats(), points.getLons());
        double minLat = StrictMath.min(line.getLat(0), line.getLat(1));
        double maxLat = StrictMath.max(line.getLat(0), line.getLat(1));
        double minLon = StrictMath.min(line.getLon(0), line.getLon(1));
        double maxLon = StrictMath.max(line.getLon(0), line.getLon(1));
        assertEquals(Relation.INTERSECTS, line.relate(minLat, maxLat, minLon, maxLon));
    }
}
