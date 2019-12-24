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

package org.apache.lucene.util;

import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.test.ESTestCase;

public class CombinedBitSetTests extends ESTestCase {
    public void testEmpty() {
        for (float percent : new float[] {0f, 0.1f, 0.5f, 0.9f, 1f}) {
            testCase(randomIntBetween(1, 10000), 0f, percent);
            testCase(randomIntBetween(1, 10000), percent, 0f);
        }
    }

    public void testSparse() {
        for (float percent : new float[] {0f, 0.1f, 0.5f, 0.9f, 1f}) {
            testCase(randomIntBetween(1, 10000), 0.1f, percent);
            testCase(randomIntBetween(1, 10000), percent, 0.1f);
        }
    }

    public void testDense() {
        for (float percent : new float[] {0f, 0.1f, 0.5f, 0.9f, 1f}) {
            testCase(randomIntBetween(1, 10000), 0.9f, percent);
            testCase(randomIntBetween(1, 10000), percent, 0.9f);
        }
    }

    public void testRandom() {
        int iterations = atLeast(10);
        for (int i = 0; i < iterations; i++) {
            testCase(randomIntBetween(1, 10000), randomFloat(), randomFloat());
        }
    }

    private void testCase(int numBits, float percent1, float percent2) {
        BitSet first = randomSet(numBits, percent1);
        BitSet second = randomSet(numBits, percent2);
        CombinedBitSet actual = new CombinedBitSet(first, second);
        FixedBitSet expected = new FixedBitSet(numBits);
        or(expected, first);
        and(expected, second);
        assertEquals(expected.cardinality(), actual.cardinality());
        assertEquals(expected, actual, numBits);
        for (int i = 0; i < numBits; ++i) {
            assertEquals(expected.nextSetBit(i), actual.nextSetBit(i));
            assertEquals(Integer.toString(i), expected.prevSetBit(i), actual.prevSetBit(i));
        }
    }

    private void or(BitSet set1, BitSet set2) {
        int next = 0;
        while (next < set2.length() && (next = set2.nextSetBit(next)) != DocIdSetIterator.NO_MORE_DOCS) {
            set1.set(next);
            next += 1;
        }
    }

    private void and(BitSet set1, BitSet set2) {
        int next = 0;
        while (next < set1.length() && (next = set1.nextSetBit(next)) != DocIdSetIterator.NO_MORE_DOCS) {
            if (set2.get(next) == false) {
                set1.clear(next);
            }
            next += 1;
        }
    }

    private void assertEquals(BitSet set1, BitSet set2, int maxDoc) {
        for (int i = 0; i < maxDoc; ++i) {
            assertEquals("Different at " + i, set1.get(i), set2.get(i));
        }
    }

    private BitSet randomSet(int numBits, float percentSet) {
        return randomSet(numBits, (int) (percentSet * numBits));
    }

    private BitSet randomSet(int numBits, int numBitsSet) {
        assert numBitsSet <= numBits;
        final BitSet set = randomBoolean() ? new SparseFixedBitSet(numBits) : new FixedBitSet(numBits);
        for (int i = 0; i < numBitsSet; ++i) {
            while (true) {
                final int o = random().nextInt(numBits);
                if (set.get(o) == false) {
                    set.set(o);
                    break;
                }
            }
        }
        return set;
    }
}
