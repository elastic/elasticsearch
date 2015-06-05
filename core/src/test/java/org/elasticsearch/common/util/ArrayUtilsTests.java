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

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.BitSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class ArrayUtilsTests extends ElasticsearchTestCase {

    @Test
    public void binarySearch() throws Exception {

        for (int j = 0; j < 100; j++) {

            int index = Math.min(randomInt(0, 10), 9);
            double tolerance = Math.random() * 0.01;
            double lookForValue = randomFreq(0.9) ? -1 : Double.NaN; // sometimes we'll look for NaN
            double[] array = new double[10];
            for (int i = 0; i < array.length; i++) {
                double value;
                if (randomFreq(0.9)) {
                    value = Math.random() * 10;
                    array[i] = value + ((randomFreq(0.5) ? 1 : -1) * Math.random() * tolerance);

                } else {                    // sometimes we'll have NaN in the array
                    value = Double.NaN;
                    array[i] = value;
                }
                if (i == index && lookForValue < 0) {
                    lookForValue = value;
                }
            }
            Arrays.sort(array);

            // pick up all the indices that fall within the range of [lookForValue - tolerance, lookForValue + tolerance]
            // we need to do this, since we choose the values randomly and we might end up having multiple values in the
            // array that will match the looked for value with the random tolerance. In such cases, the binary search will
            // return the first one that will match.
            BitSet bitSet = new BitSet(10);
            for (int i = 0; i < array.length; i++) {
                if (Double.isNaN(lookForValue) && Double.isNaN(array[i])) {
                    bitSet.set(i);
                } else if ((array[i] >= lookForValue - tolerance) && (array[i] <= lookForValue + tolerance)) {
                    bitSet.set(i);
                }
            }

            int foundIndex = ArrayUtils.binarySearch(array, lookForValue, tolerance);

            if (bitSet.cardinality() == 0) {
                assertThat(foundIndex, is(-1));
            } else {
                assertThat(bitSet.get(foundIndex), is(true));
            }
        }
    }

    private boolean randomFreq(double freq) {
        return Math.random() < freq;
    }

    private int randomInt(int min, int max) {
        int delta = (int) (Math.random() * (max - min));
        return min + delta;
    }

}
