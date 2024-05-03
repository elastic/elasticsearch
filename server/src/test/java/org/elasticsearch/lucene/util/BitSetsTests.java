/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.lucene.util;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class BitSetsTests extends ESTestCase {

    public void testRandomBitset() throws Exception {
        int maxDocs = randomIntBetween(1, 1024);
        int numDocs = 0;
        FixedBitSet matches = new FixedBitSet(maxDocs);
        for (int i = 0; i < maxDocs; i++) {
            if (numDocs < maxDocs && randomBoolean()) {
                numDocs++;
                matches.set(i);
            }
        }
        DocIdSetIterator it = new BitSetIterator(matches, randomIntBetween(0, numDocs));
        BitSet bitSet = BitSets.of(it, maxDocs);
        assertThat(bitSet.cardinality(), equalTo(numDocs));
        assertThat(bitSet.length(), equalTo(maxDocs));
        for (int i = 0; i < maxDocs; i++) {
            assertThat(bitSet.get(i), equalTo(matches.get(i)));
            assertThat(bitSet.nextSetBit(i), equalTo(matches.nextSetBit(i)));
            assertThat(bitSet.prevSetBit(i), equalTo(matches.prevSetBit(i)));
        }
    }

    public void testMatchAllBitSet() throws Exception {
        int maxDocs = randomIntBetween(1, 128);
        FixedBitSet matches = new FixedBitSet(maxDocs);
        for (int i = 0; i < maxDocs; i++) {
            matches.set(i);
        }
        DocIdSetIterator it = new BitSetIterator(matches, randomNonNegativeLong());
        BitSet bitSet = BitSets.of(it, maxDocs);
        assertThat(bitSet, instanceOf(MatchAllBitSet.class));
        for (int i = 0; i < maxDocs; i++) {
            assertTrue(bitSet.get(i));
            assertThat(bitSet.nextSetBit(i), equalTo(matches.nextSetBit(i)));
            assertThat(bitSet.prevSetBit(i), equalTo(matches.prevSetBit(i)));
        }
    }
}
