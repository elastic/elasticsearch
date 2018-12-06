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

package org.elasticsearch.index.seqno;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class CountedBitSetTests extends ESTestCase {

    public void testCompareToFixedBitset() {
        int numBits = (short) randomIntBetween(8, 4096);
        final FixedBitSet fixedBitSet = new FixedBitSet(numBits);
        final CountedBitSet countedBitSet = new CountedBitSet((short) numBits);

        for (int i = 0; i < numBits; i++) {
            if (randomBoolean()) {
                fixedBitSet.set(i);
                countedBitSet.set(i);
            }
            assertThat(countedBitSet.cardinality(), equalTo(fixedBitSet.cardinality()));
            assertThat(countedBitSet.length(), equalTo(fixedBitSet.length()));
        }

        for (int i = 0; i < numBits; i++) {
            assertThat(countedBitSet.get(i), equalTo(fixedBitSet.get(i)));
        }
    }

    public void testReleaseInternalBitSet() {
        int numBits = (short) randomIntBetween(8, 4096);
        final CountedBitSet countedBitSet = new CountedBitSet((short) numBits);
        final List<Integer> values = IntStream.range(0, numBits).boxed().collect(Collectors.toList());

        for (int i = 1; i < numBits; i++) {
            final int value = values.get(i);
            assertThat(countedBitSet.get(value), equalTo(false));
            assertThat(countedBitSet.isInternalBitsetReleased(), equalTo(false));

            countedBitSet.set(value);

            assertThat(countedBitSet.get(value), equalTo(true));
            assertThat(countedBitSet.isInternalBitsetReleased(), equalTo(false));
            assertThat(countedBitSet.length(), equalTo(numBits));
            assertThat(countedBitSet.cardinality(), equalTo(i));
        }

        // The missing piece to fill all bits.
        {
            final int value = values.get(0);
            assertThat(countedBitSet.get(value), equalTo(false));
            assertThat(countedBitSet.isInternalBitsetReleased(), equalTo(false));

            countedBitSet.set(value);

            assertThat(countedBitSet.get(value), equalTo(true));
            assertThat(countedBitSet.isInternalBitsetReleased(), equalTo(true));
            assertThat(countedBitSet.length(), equalTo(numBits));
            assertThat(countedBitSet.cardinality(), equalTo(numBits));
        }

        // Tests with released internal bitset.
        final int iterations = iterations(1000, 10000);
        for (int i = 0; i < iterations; i++) {
            final int value = randomInt(numBits - 1);
            assertThat(countedBitSet.get(value), equalTo(true));
            assertThat(countedBitSet.isInternalBitsetReleased(), equalTo(true));
            assertThat(countedBitSet.length(), equalTo(numBits));
            assertThat(countedBitSet.cardinality(), equalTo(numBits));
            if (frequently()) {
                assertThat(countedBitSet.get(value), equalTo(true));
            }
        }
    }
}
