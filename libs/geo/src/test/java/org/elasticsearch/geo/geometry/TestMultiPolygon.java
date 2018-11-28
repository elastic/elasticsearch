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
import static org.apache.lucene.geo.GeoTestUtil.nextPolygon;

/**
 * Tests relations and features of MultiPolygon types
 */
public class TestMultiPolygon extends BaseGeometryTestCase<MultiPolygon> {
    @Override
    public MultiPolygon getShape() {
        int numPolys = randomIntBetween(random(), 2, 100);
        Polygon[] polygons = new Polygon[numPolys];
        for (int i = 0; i < numPolys; ++i) {
            polygons[i] = nextPolygon();
        }
        return new MultiPolygon(polygons);
    }

    protected MultiPolygon getShape(boolean padded) {
        double minFactor = padded == true ? 20D : 0D;
        double maxFactor = padded == true ? -20D : 0D;
        int numPolys = randomIntBetween(random(), 1, 10);
        Polygon[] polygons = new Polygon[numPolys];

        // we can't have self crossing lines.
        // since polygons share this contract we create an unclosed polygon
        for (int i = 0; i < numPolys; ++i) {
            polygons[i] = GeoTestUtil.createRegularPolygon(
                nextLatitudeIn(GeoUtils.MIN_LAT_INCL + minFactor, GeoUtils.MAX_LAT_INCL + maxFactor),
                nextLongitudeIn(GeoUtils.MIN_LON_INCL + minFactor, GeoUtils.MAX_LON_INCL + maxFactor),
                10000D, randomIntBetween(random(), 4, 100));
        }

        return new MultiPolygon(polygons);
    }

    /**
     * tests area of MultiPolygon using simple random boxes
     */
    @Override
    public void testArea() {
        int numPolys = randomIntBetween(random(), 2, 10);
        Rectangle box;
        Polygon[] polygon = new Polygon[numPolys];
        double width, height;
        double area = 0;
        for (int i = 0; i < numPolys; ++i) {
            box = nextBoxNotCrossingDateline();
            polygon[i] = new Polygon(
                new double[]{box.minLat(), box.minLat(), box.maxLat(), box.maxLat(), box.minLat()},
                new double[]{box.minLon(), box.maxLon(), box.maxLon(), box.minLon(), box.minLon()});
            width = box.maxLon() - box.minLon();
            height = box.maxLat() - box.minLat();
            area += width * height;
        }
        MultiPolygon polygons = new MultiPolygon(polygon);
        assertEquals(area, polygons.getArea(), 1E-10D);
    }

    @Override
    public void testBoundingBox() {
        MultiPolygon polygons = getShape(true);
        double minLat = Double.POSITIVE_INFINITY;
        double maxLat = Double.NEGATIVE_INFINITY;
        double minLon = Double.POSITIVE_INFINITY;
        double maxLon = Double.NEGATIVE_INFINITY;
        Polygon p;
        for (int i = 0; i < polygons.length(); ++i) {
            p = polygons.get(i);
            for (int j = 0; j < p.numPoints(); ++j) {
                minLat = Math.min(p.getLat(j), minLat);
                maxLat = Math.max(p.getLat(j), maxLat);
                minLon = Math.min(p.getLon(j), minLon);
                maxLon = Math.max(p.getLon(j), maxLon);
            }
        }
        Rectangle bbox = new Rectangle(minLat, maxLat, minLon, maxLon);
        assertEquals(bbox, polygons.getBoundingBox());
    }

    @Override
    public void testWithin() {
        relationTest(getShape(true), Relation.WITHIN);
    }

    @Override
    public void testContains() {
        MultiPolygon polygons = getShape(true);
        Point center = polygons.get(0).getCenter();
        Rectangle box = new Rectangle(center.lat() - 1E-3D, center.lat() + 1E-3D, center.lon() - 1E-3D, center.lon() + 1E-3D);
        assertEquals(Relation.CONTAINS, polygons.relate(box.minLat(), box.maxLat(), box.minLon(), box.maxLon()));
    }

    @Override
    public void testDisjoint() {
        relationTest(getShape(true), Relation.DISJOINT);
    }

    @Override
    public void testIntersects() {
        MultiPolygon polygons = getShape(true);
        Polygon polygon = polygons.get(0);
        double minLat = StrictMath.min(polygon.getLat(0), polygon.getLat(1));
        double maxLat = StrictMath.max(polygon.getLat(0), polygon.getLat(1));
        double minLon = StrictMath.min(polygon.getLon(0), polygon.getLon(1));
        double maxLon = StrictMath.max(polygon.getLon(0), polygon.getLon(1));
        assertEquals(Relation.INTERSECTS, polygon.relate(minLat, maxLat, minLon, maxLon));
    }
}
