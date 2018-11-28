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

import static com.carrotsearch.randomizedtesting.generators.RandomNumbers.randomIntBetween;

/**
 * Tests relations and features of MultiPoint types
 */
public class TestMultiPoint extends BaseGeometryTestCase<MultiPoint> {
    /**
     * returns a MultiPoint shape
     */
    @Override
    public MultiPoint getShape() {
        return getShape(false);
    }

    /**
     * returns a MultiPoint shape within a small area;
     * ensures MultiPoints do not cover entire globe.
     * Used for testing INTERSECTS and DISJOINT queries
     */
    protected MultiPoint getShape(boolean padded) {
        int numPoints = randomIntBetween(random(), 2, 100);
        double[] lats = new double[numPoints];
        double[] lons = new double[numPoints];
        for (int i = 0; i < numPoints; ++i) {
            if (padded == true) {
                lats[i] = GeoTestUtil.nextLatitudeIn(GeoUtils.MIN_LAT_INCL + 20D, GeoUtils.MAX_LAT_INCL - 20D);
                lons[i] = GeoTestUtil.nextLongitudeIn(GeoUtils.MIN_LON_INCL + 20D, GeoUtils.MAX_LON_INCL - 20D);
            } else {
                lats[i] = GeoTestUtil.nextLatitude();
                lons[i] = GeoTestUtil.nextLongitude();
            }
        }
        return new MultiPoint(lats, lons);
    }

    @Override
    public void testWithin() {
        relationTest(getShape(true), Relation.WITHIN);
    }

    @Ignore
    @Override
    public void testContains() {
        // MultiPoint does not support CONTAINS
    }

    @Override
    public void testDisjoint() {
        // this is a simple test where we build a bounding box that is disjoint
        // from the MultiPoint bounding box
        // note: we should add a test where a box is between points
        relationTest(getShape(true), Relation.DISJOINT);
    }

    @Override
    public void testIntersects() {
        MultiPoint points = getShape(true);
        double minLat = StrictMath.min(points.getLat(0), points.getLat(1));
        double maxLat = StrictMath.max(points.getLat(0), points.getLat(1));
        double minLon = StrictMath.min(points.getLon(0), points.getLon(1));
        double maxLon = StrictMath.max(points.getLon(0), points.getLon(1));
        Relation r = Relation.DISJOINT;
        for (Point p : points) {
            if ((r = p.relate(minLat, maxLat, minLon, maxLon)) == Relation.INTERSECTS) {
                break;
            }
        }
        assertEquals(Relation.INTERSECTS, r);
    }

    /**
     * tests bounding box of MultiPoint shape type
     */
    @Override
    public void testBoundingBox() {
        MultiPoint points = getShape(true);
        double minLat = Double.POSITIVE_INFINITY;
        double maxLat = Double.NEGATIVE_INFINITY;
        double minLon = Double.POSITIVE_INFINITY;
        double maxLon = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < points.numPoints(); ++i) {
            minLat = StrictMath.min(minLat, points.getLat(i));
            maxLat = StrictMath.max(maxLat, points.getLat(i));
            minLon = StrictMath.min(minLon, points.getLon(i));
            maxLon = StrictMath.max(maxLon, points.getLon(i));
        }

        Rectangle bbox = new Rectangle(minLat, maxLat, minLon, maxLon);
        assertEquals(bbox, points.getBoundingBox());
    }
}
