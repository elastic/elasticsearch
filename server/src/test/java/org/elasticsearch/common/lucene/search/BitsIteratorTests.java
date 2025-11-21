/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.test.ESTestCase;

public class BitsIteratorTests extends ESTestCase {

    public void testEmpty() {
        Bits bits = new Bits.MatchNoBits(10_000);
        BitsIterator iterator = new BitsIterator((bits));
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
    }

    public void testSingleBit() {
        FixedBitSet bits = new FixedBitSet(10_000);
        bits.set(5000);

        BitsIterator iterator = new BitsIterator((bits));
        assertEquals(5000, iterator.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());

        iterator = new BitsIterator((bits));
        assertEquals(5000, iterator.advance(5000));

        iterator = new BitsIterator((bits));
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.advance(5001));
    }

    public void testEverySecondBit() {
        FixedBitSet bits = new FixedBitSet(10_000);
        for (int i = 0; i < bits.length(); i += 2) {
            bits.set(i);
        }
        BitsIterator iterator = new BitsIterator((bits));
        for (int i = 0; i < bits.length(); i += 2) {
            assertEquals(i, iterator.nextDoc());
        }
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
    }
}
