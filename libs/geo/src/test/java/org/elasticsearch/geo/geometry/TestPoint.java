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

import static org.apache.lucene.geo.GeoTestUtil.nextLatitudeIn;
import static org.apache.lucene.geo.GeoTestUtil.nextLongitudeIn;

/**
 * Tests relations and features of simple Point types
 */
public class TestPoint extends BaseGeometryTestCase<Point> {
    @Override
    public Point getShape() {
        return new Point(GeoTestUtil.nextLatitude(), GeoTestUtil.nextLongitude());
    }

    @Ignore
    @Override
    public void testWithin() {
        // for Point types WITHIN == INTERSECTS; so ingore this test
    }

    @Ignore
    @Override
    public void testContains() {
        // points do not contain other shapes; ignore this test
    }

    @Override
    public void testDisjoint() {
        Point pt = getShape();

        double minLat = pt.lat();
        double maxLat = pt.lat();
        double minLon = pt.lon();
        double maxLon = pt.lon();

        // ensure point latitude is outside of box (with pole as boundary)
        if (pt.lat() <= GeoUtils.MIN_LAT_INCL + 1D) {
            minLat = GeoUtils.MIN_LAT_INCL + 2D;
            maxLat = nextLatitudeIn(minLat, GeoUtils.MAX_LAT_INCL);
        } else if (pt.lat() >= GeoUtils.MAX_LAT_INCL - 1D) {
            maxLat -= 2D;
            minLat = nextLatitudeIn(GeoUtils.MIN_LAT_INCL, maxLat);
        } else {
            minLat += 1D;
            maxLat = nextLatitudeIn(minLat, GeoUtils.MAX_LAT_INCL);
        }

        // ensure point longitude is disjoint with box (with dateline as boundary)
        if (pt.lon() <= GeoUtils.MIN_LON_INCL + 1D) {
            minLon = GeoUtils.MIN_LON_INCL + 2D;
            maxLon = nextLongitudeIn(minLon, GeoUtils.MAX_LON_INCL);
        } else if (pt.lon() >= GeoUtils.MAX_LON_INCL - 1D) {
            maxLon -= 2D;
            minLon = nextLongitudeIn(GeoUtils.MIN_LON_INCL, maxLon);
        } else {
            minLon += 1D;
            maxLon = nextLongitudeIn(minLon, GeoUtils.MAX_LON_INCL);
        }

        Rectangle box = new Rectangle(minLat, maxLat, minLon, maxLon);
        assertEquals(Relation.DISJOINT, pt.relate(box.minLat(), box.maxLat(), box.minLon(), box.maxLon()));
    }

    @Override
    public void testIntersects() {
        Rectangle box = GeoTestUtil.nextBoxNotCrossingDateline();
        Point pt = new Point(nextLatitudeIn(box.minLat(), box.maxLat()), nextLongitudeIn(box.minLon(), box.maxLon()));
        assertEquals(Relation.INTERSECTS, pt.relate(box.minLat(), box.maxLat(), box.minLon(), box.maxLon()));
    }

    @Override
    public void testBoundingBox() {
        expectThrows(UnsupportedOperationException.class, () -> getShape().getBoundingBox());
    }

    @Override
    public void testCenter() {
        Point pt = getShape();
        assertEquals(pt, new Point(pt.lat(), pt.lon()));
    }
}
