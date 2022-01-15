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

import org.elasticsearch.test.ESTestCase;

public class ParentChildNavigationTests extends ESTestCase {

    public void testParentChild() {
        String[] h3Addresses = H3.getStringRes0Cells();
        String h3Address = RandomPicks.randomFrom(random(), h3Addresses);
        String[] values = new String[H3.MAX_H3_RES];
        values[0] = h3Address;
        for (int i = 1; i < H3.MAX_H3_RES; i++) {
            h3Addresses = H3.h3ToChildren(h3Address);
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
        String h3Address = RandomPicks.randomFrom(random(), h3Addresses);
        for (int i = 1; i < H3.MAX_H3_RES; i++) {
            h3Addresses = H3.h3ToChildren(h3Address);
            assertHexRing(i, h3Address, h3Addresses);
            h3Address = RandomPicks.randomFrom(random(), h3Addresses);
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
}
