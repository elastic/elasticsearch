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

package org.elasticsearch.search.aggregations.metrics;

import com.carrotsearch.hppc.BitMixer;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLog.MAX_PRECISION;
import static org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLog.MIN_PRECISION;

public class HyperLogLogPlusPlusSparseTests extends ESTestCase {

    public void testEquivalence() throws IOException {
        final int p = randomIntBetween(MIN_PRECISION, MAX_PRECISION);
        final HyperLogLogPlusPlus single = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 0);
        final int numBuckets = randomIntBetween(2, 100);
        final int numValues = randomIntBetween(1, 100000);
        final int maxValue = randomIntBetween(1, randomBoolean() ? 1000: 1000000);
        for (int i = 0; i < numValues; ++i) {
            final int n = randomInt(maxValue);
            final long hash = BitMixer.mix64(n);
            single.collect(randomInt(numBuckets), hash);
        }
        for (int i = 0; i < numBuckets; i++) {
            // test clone
            AbstractHyperLogLogPlusPlus clone = single.clone(i, BigArrays.NON_RECYCLING_INSTANCE);
            if (single.getAlgorithm(i) == AbstractHyperLogLogPlusPlus.LINEAR_COUNTING) {
                assertTrue(clone instanceof HyperLogLogPlusPlusSparse);
            } else {
                assertTrue(clone instanceof HyperLogLogPlusPlus);
            }
            checkEquivalence(single, i, clone, 0);
            // test serialize
            BytesStreamOutput out = new BytesStreamOutput();
            single.writeTo(i, out);
            clone = AbstractHyperLogLogPlusPlus.readFrom(out.bytes().streamInput(), BigArrays.NON_RECYCLING_INSTANCE);
            if (single.getAlgorithm(i) == AbstractHyperLogLogPlusPlus.LINEAR_COUNTING) {
                assertTrue(clone instanceof HyperLogLogPlusPlusSparse);
            } else {
                assertTrue(clone instanceof HyperLogLogPlusPlus);
            }
            checkEquivalence(single, i, clone, 0);
            // test merge
            final HyperLogLogPlusPlus merge = new HyperLogLogPlusPlus(p, BigArrays.NON_RECYCLING_INSTANCE, 0);
            merge.merge(0, clone, 0);
            checkEquivalence(merge, 0, clone, 0);
        }
    }

    private void checkEquivalence(AbstractHyperLogLogPlusPlus first, int firstBucket,
                                  AbstractHyperLogLogPlusPlus second, int secondBucket) {
        assertEquals(first.hashCode(firstBucket), second.hashCode(secondBucket));
        assertEquals(first.cardinality(firstBucket), second.cardinality(0));
        assertTrue(first.equals(firstBucket, second, secondBucket));
        assertTrue(second.equals(secondBucket, first, firstBucket));
    }
}
