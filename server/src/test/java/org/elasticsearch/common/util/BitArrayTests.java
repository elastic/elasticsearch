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

package org.elasticsearch.common.util;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BitArrayTests extends ESTestCase {
    public void testRandom() {
        try (BitArray bitArray = new BitArray(1, BigArrays.NON_RECYCLING_INSTANCE)) {
            int numBits = randomIntBetween(1000, 10000);
            for (int step = 0; step < 3; step++) {
                boolean[] bits = new boolean[numBits];
                List<Integer> slots = new ArrayList<>();
                for (int i = 0; i < numBits; i++) {
                    bits[i] = randomBoolean();
                    slots.add(i);
                }
                Collections.shuffle(slots, random());
                for (int i : slots) {
                    if (bits[i]) {
                        bitArray.set(i);
                    } else {
                        bitArray.clear(i);
                    }
                }
                for (int i = 0; i < numBits; i++) {
                    assertEquals(bitArray.get(i), bits[i]);
                }
            }
        }
    }
}
