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
 * Tests relations and features of MultiLine types
 */
public class TestMultiLine extends BaseGeometryTestCase<MultiLine> {
    @Override
    public MultiLine getShape() {
        return getShape(false);
    }

    public MultiLine getShape(boolean padded) {
        double minFactor = padded == true ? 20D : 0D;
        double maxFactor = padded == true ? -20D : 0D;
        int numLines = randomIntBetween(random(), 1, 10);
        Line[] lines = new Line[numLines];

        // we can't have self crossing lines.
        // since polygons share this contract we create an unclosed polygon
        Polygon poly;
        double[] lats;
        double[] lons;

        for (int i = 0; i < numLines; ++i) {
            poly = GeoTestUtil.createRegularPolygon(
                nextLatitudeIn(GeoUtils.MIN_LAT_INCL + minFactor, GeoUtils.MAX_LAT_INCL + maxFactor),
                nextLongitudeIn(GeoUtils.MIN_LON_INCL + minFactor, GeoUtils.MAX_LON_INCL + maxFactor),
                100D, randomIntBetween(random(), 4, 100));
            lats = poly.getPolyLats();
            lons = poly.getPolyLons();
            lines[i] = new Line(Arrays.copyOfRange(lats, 0, lats.length - 1),
                Arrays.copyOfRange(lons, 0, lons.length - 1));
        }

        return new MultiLine(lines);
    }

    @Override
    public void testBoundingBox() {
        MultiLine lines = getShape(true);
        double minLat = Double.POSITIVE_INFINITY;
        double maxLat = Double.NEGATIVE_INFINITY;
        double minLon = Double.POSITIVE_INFINITY;
        double maxLon = Double.NEGATIVE_INFINITY;
        for (int l = 0; l < lines.length(); ++l) {
            Line line = lines.get(l);
            for (int j = 0; j < line.numPoints(); ++j) {
                minLat = StrictMath.min(minLat, line.getLat(j));
                maxLat = StrictMath.max(maxLat, line.getLat(j));
                minLon = StrictMath.min(minLon, line.getLon(j));
                maxLon = StrictMath.max(maxLon, line.getLon(j));
            }
        }

        Rectangle bbox = new Rectangle(minLat, maxLat, minLon, maxLon);
        assertEquals(bbox, lines.getBoundingBox());
    }

    @Override
    public void testWithin() {
        relationTest(getShape(true), Relation.WITHIN);
    }

    @Ignore
    @Override
    public void testContains() {
        // CONTAINS not supported with MultiLine
    }

    @Override
    public void testDisjoint() {
        relationTest(getShape(true), Relation.DISJOINT);
    }

    @Override
    public void testIntersects() {
        MultiLine lines = getShape(true);
        Line line = lines.get(0);
        double minLat = StrictMath.min(line.getLat(0), line.getLat(1));
        double maxLat = StrictMath.max(line.getLat(0), line.getLat(1));
        double minLon = StrictMath.min(line.getLon(0), line.getLon(1));
        double maxLon = StrictMath.max(line.getLon(0), line.getLon(1));
        assertEquals(Relation.INTERSECTS, lines.relate(minLat, maxLat, minLon, maxLon));
    }
}
