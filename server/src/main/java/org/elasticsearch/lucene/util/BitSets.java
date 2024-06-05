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

import java.io.IOException;

public final class BitSets {
    private BitSets() {}

    /**
     * Build a {@link BitSet} from the content of the provided {@link DocIdSetIterator}. If the iterator matches all documents,
     * then this method will wrap the returned Bitset as {@link MatchAllBitSet} to reduce memory usage.
     */
    public static BitSet of(DocIdSetIterator iter, int maxDocs) throws IOException {
        final BitSet bitSet = BitSet.of(iter, maxDocs);
        if (bitSet.cardinality() == maxDocs) {
            return new MatchAllBitSet(maxDocs);
        } else {
            return bitSet;
        }
    }
}
