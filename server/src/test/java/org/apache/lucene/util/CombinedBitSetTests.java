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

public class CombinedBitSetTests extends BaseBitSetTestCase {
    @Override
    public BitSet copyOf(BitSet bs, int length) {
        final Bits bits = new Bits() {
            @Override
            public boolean get(int index) {
                if (bs.get(index)) {
                    return true;
                }
                return random().nextBoolean();
            }

            @Override
            public int length() {
                return bs.length();
            }
        };
        return new CombinedBitSet(bs, bits);
    }

    public void testCombinedSparse() {
        int numDocs = random().nextInt(1000) + 1;
        SparseFixedBitSet bitSet = new SparseFixedBitSet(numDocs);
        FixedBitSet bits = new FixedBitSet(numDocs);
        FixedBitSet expected = new FixedBitSet(numDocs);
        for (int i = 0; i < numDocs; i++) {
            boolean isSet = false;
            if (random().nextFloat() <= 0.1f) {
                bitSet.set(i);
                isSet = true;
            }
            if (random().nextFloat() >= 0.1f) {
                bits.set(i);
            } else {
                isSet = false;
            }
            if (isSet) {
                expected.set(i);
            }
        }
        CombinedBitSet combined = new CombinedBitSet(bitSet, bits);
        for (int i = 0; i < numDocs; i++) {
            assertEquals(expected.nextSetBit(i), combined.nextSetBit(i));
            assertEquals(expected.prevSetBit(i), combined.prevSetBit(i));
        }
        assertEquals(expected.length(), combined.length());
        assertEquals(expected.cardinality(), combined.cardinality());
    }
}
