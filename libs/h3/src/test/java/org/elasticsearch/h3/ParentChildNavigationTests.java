/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.h3;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.geo.Point;
import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.GeoPolygon;
import org.apache.lucene.spatial3d.geom.GeoPolygonFactory;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.tests.geo.GeoTestUtil;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ParentChildNavigationTests extends ESTestCase {

    public void testChildrenSize() {
        Point point = GeoTestUtil.nextPoint();
        int res = randomInt(H3.MAX_H3_RES - 1);
        String h3Address = H3.geoToH3Address(point.getLat(), point.getLon(), res);
        // check invalid resolutions
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> H3.h3ToChildrenSize(h3Address, res));
        assertThat(ex.getMessage(), Matchers.containsString("Invalid child resolution"));
        ex = expectThrows(IllegalArgumentException.class, () -> H3.h3ToChildrenSize(h3Address, H3.MAX_H3_RES + 1));
        assertThat(ex.getMessage(), Matchers.containsString("Invalid child resolution"));
        ex = expectThrows(
            IllegalArgumentException.class,
            () -> H3.h3ToChildrenSize(H3.geoToH3(point.getLat(), point.getLon(), H3.MAX_H3_RES))
        );
        assertThat(ex.getMessage(), Matchers.containsString("Invalid child resolution"));
        // check methods gives same answer
        assertEquals(H3.h3ToChildrenSize(h3Address), H3.h3ToChildrenSize(h3Address, res + 1));
        // check against brute force counting
        int childrenRes = Math.min(H3.MAX_H3_RES, res + randomIntBetween(2, 7));
        long numChildren = H3.h3ToChildrenSize(h3Address, childrenRes);
        assertEquals(numChildren(h3Address, childrenRes), numChildren);
    }

    private long numChildren(String h3Address, int finalRes) {
        if (H3.getResolution(h3Address) == finalRes) {
            return 1;
        }
        long result = 0;
        for (int i = 0; i < H3.h3ToChildrenSize(h3Address); i++) {
            result += numChildren(H3.childPosToH3(h3Address, i), finalRes);
        }
        return result;
    }

    public void testNoChildrenIntersectingSize() {
        Point point = GeoTestUtil.nextPoint();
        int res = randomInt(H3.MAX_H3_RES - 1);
        String h3Address = H3.geoToH3Address(point.getLat(), point.getLon(), res);
        // check invalid resolutions
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> H3.h3ToNotIntersectingChildrenSize(H3.geoToH3(point.getLat(), point.getLon(), H3.MAX_H3_RES))
        );
        assertThat(ex.getMessage(), Matchers.containsString("Invalid child resolution"));
        // check against brute force counting
        long numChildren = H3.h3ToNotIntersectingChildrenSize(h3Address);
        assertEquals(H3.h3ToNoChildrenIntersecting(h3Address).length, numChildren);
    }

    public void testParentChild() {
        String[] h3Addresses = H3.getStringRes0Cells();
        String h3Address = RandomPicks.randomFrom(random(), h3Addresses);
        String[] values = new String[H3.MAX_H3_RES];
        values[0] = h3Address;
        for (int i = 1; i < H3.MAX_H3_RES; i++) {
            h3Addresses = H3.h3ToChildren(h3Address);
            // check all elements are unique
            Set<String> mySet = Sets.newHashSet(h3Addresses);
            assertEquals(mySet.size(), h3Addresses.length);
            h3Address = RandomPicks.randomFrom(random(), h3Addresses);
            values[i] = h3Address;
        }
        h3Addresses = H3.h3ToChildren(h3Address);
        h3Address = RandomPicks.randomFrom(random(), h3Addresses);
        for (int i = H3.MAX_H3_RES - 1; i >= 0; i--) {
            h3Address = H3.h3ToParent(h3Address);
            assertEquals(values[i], h3Address);
        }
    }

    public void testHexRing() {
        String[] h3Addresses = H3.getStringRes0Cells();
        for (int i = 1; i < H3.MAX_H3_RES; i++) {
            String h3Address = RandomPicks.randomFrom(random(), h3Addresses);
            assertEquals(i - 1, H3.getResolution(h3Address));
            h3Addresses = H3.h3ToChildren(h3Address);
            assertHexRing(i, h3Address, h3Addresses);
        }
    }

    private static final int[] HEX_RING_POSITIONS = new int[] { 2, 0, 1, 4, 3, 5 };
    private static final int[] PENT_RING_POSITIONS = new int[] { 0, 1, 3, 2, 4 };

    private void assertHexRing(int res, String h3Address, String[] children) {
        LatLng latLng = H3.h3ToLatLng(h3Address);
        String centerChild = H3.geoToH3Address(latLng.getLatDeg(), latLng.getLonDeg(), res);
        assertEquals(children[0], centerChild);
        String[] ring = H3.hexRing(centerChild);
        int[] positions = H3.isPentagon(centerChild) ? PENT_RING_POSITIONS : HEX_RING_POSITIONS;
        for (int i = 1; i < children.length; i++) {
            assertEquals(children[i], ring[positions[i - 1]]);
        }
    }

    public void testNoChildrenIntersecting() {
        String[] h3Addresses = H3.getStringRes0Cells();
        String h3Address = RandomPicks.randomFrom(random(), h3Addresses);
        for (int i = 1; i <= H3.MAX_H3_RES; i++) {
            h3Addresses = H3.h3ToChildren(h3Address);
            assertIntersectingChildren(h3Address, h3Addresses);
            h3Address = RandomPicks.randomFrom(random(), h3Addresses);
        }
    }

    private void assertIntersectingChildren(String h3Address, String[] children) {
        int size = H3.h3ToNotIntersectingChildrenSize(h3Address);
        for (int i = 0; i < size; i++) {
            GeoPolygon p = getGeoPolygon(H3.noChildIntersectingPosToH3(h3Address, i));
            int intersections = 0;
            for (String o : children) {
                if (p.intersects(getGeoPolygon(o))) {
                    intersections++;
                }
            }
            assertEquals(2, intersections);
        }
    }

    private GeoPolygon getGeoPolygon(String h3Address) {
        CellBoundary cellBoundary = H3.h3ToGeoBoundary(h3Address);
        List<GeoPoint> points = new ArrayList<>(cellBoundary.numPoints());
        for (int i = 0; i < cellBoundary.numPoints(); i++) {
            LatLng latLng = cellBoundary.getLatLon(i);
            points.add(new GeoPoint(PlanetModel.SPHERE, latLng.getLatRad(), latLng.getLonRad()));
        }
        return GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
    }

    public void testHexRingPos() {
        String[] h3Addresses = H3.getStringRes0Cells();
        for (int i = 0; i < H3.MAX_H3_RES; i++) {
            String h3Address = RandomPicks.randomFrom(random(), h3Addresses);
            assertHexRing3(h3Address);
            h3Addresses = H3.h3ToChildren(h3Address);
        }
    }

    private void assertHexRing3(String h3Address) {
        String[] ring = H3.hexRing(h3Address);
        assertEquals(ring.length, H3.hexRingSize(h3Address));
        for (int i = 0; i < H3.hexRingSize(h3Address); i++) {
            assertEquals(ring[i], H3.hexRingPosToH3(h3Address, i));
        }
    }
}
