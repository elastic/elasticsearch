/*
 * Based on the h3 project by Uber (@uber)
 * https://github.com/uber/h3
 * Licensed to Elasticsearch B.V under the Apache 2.0 License.
 * Elasticsearch B.V licenses this file, including any modifications, to you under the Apache 2.0 License.
 * See the LICENSE file in the project root for more information.
 */

package org.elasticsearch.h3;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.elasticsearch.test.ESTestCase;

public class ParentChildNavigationTests extends ESTestCase {

    public void testParentChild() {
        String[] h3Addresses = H3.getStringRes0Cells();
        String h3Address = RandomPicks.randomFrom(random(), h3Addresses);
        String[] values = new String[Constants.MAX_H3_RES];
        values[0] = h3Address;
        for (int i = 1; i < Constants.MAX_H3_RES; i++) {
            h3Addresses = H3.h3ToChildren(h3Address);
            h3Address = RandomPicks.randomFrom(random(), h3Addresses);
            values[i] = h3Address;
        }
        h3Addresses = H3.h3ToChildren(h3Address);
        h3Address = RandomPicks.randomFrom(random(), h3Addresses);
        for (int i = Constants.MAX_H3_RES - 1; i >= 0; i--) {
            h3Address = H3.h3ToParent(h3Address);
            assertEquals(values[i], h3Address);
        }
    }
}
