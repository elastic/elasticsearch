/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
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

package org.elasticsearch.common.util;

import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;

public class BinarySearcherTests extends ESTestCase {

    private BigArrays randombigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    private BigArrays bigArrays;

    @Before
    public void init() {
        bigArrays = randombigArrays();
    }

    public void testDoubleBinarySearch() throws Exception {
        final int size = randomIntBetween(50, 10000);
        DoubleArray bigArray = new BigDoubleArray(size, bigArrays, false);
        double[] array = new double[size];

        // Fill array with sorted values
        double currentValue = randomDoubleBetween(-100, 100, true);
        for (int i = 0; i < size; ++i) {
            bigArray.set(i, currentValue);
            array[i] = currentValue;
            currentValue += randomDoubleBetween(0, 30, false);
        }

        // Pick a number to search for
        int index = randomIntBetween(0, size-1);
        double searchFor = bigArray.get(index);
        if (randomBoolean()){
            // Pick a number where there is no exact match, but that is closest to array.get(index)
            if(randomBoolean()){
                // Pick a number above array.get(index)
                if(index < size - 1){
                    searchFor += (bigArray.get(index + 1) - bigArray.get(index)) / 3; // Divide by 3 so that it's closer to array.get(index) than to array.get(index + 1)
                } else {
                    // There is nothing about index
                    searchFor += 0.1;
                }
            } else {
                // Pick one below array.get(index)
                if (index > 0) {
                    searchFor -= (bigArray.get(index) - bigArray.get(index - 1)) / 3;
                } else {
                    // There is nothing below index
                    searchFor -= 0.1;
                }
            }
        }

        BigArrays.DoubleBinarySearcher searcher = new BigArrays.DoubleBinarySearcher(bigArray);
        assertEquals(index, searcher.search(0, size - 1, searchFor));

        // Sanity check: confirm that ArrayUtils.binarySearch() returns the same index
        assertEquals(index, Arrays.binarySearch(array, searchFor));

        Releasables.close(bigArray);
    }
}
