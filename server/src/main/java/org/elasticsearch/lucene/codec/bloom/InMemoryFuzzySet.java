/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.lucene.codec.bloom;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;

final class InMemoryFuzzySet {
    private final HashFunction hashFunction;
    private final int bloomSize;
    private final FixedBitSet bitSet;

    InMemoryFuzzySet(IndexInput in) throws IOException {
        int version = in.readInt();
        if (version == FuzzySet.VERSION_SPI) {
            in.readString();
        }
        this.hashFunction = FuzzySet.hashFunctionForVersion(version);
        this.bloomSize = in.readInt();
        final long[] bits = new long[in.readInt()];
        in.readLongs(bits, 0, bits.length);
        bitSet = new FixedBitSet(bits, bloomSize + 1);
    }

    public FuzzySet.ContainsResult contains(BytesRef value) {
        int hash = hashFunction.hash(value);
        if (hash < 0) {
            hash = -hash;
        }
        // Bloom sizes are always base 2 and so can be ANDed for a fast modulo
        hash = hash & bloomSize;
        if (bitSet.get(hash)) {
            return FuzzySet.ContainsResult.MAYBE;
        } else {
            return FuzzySet.ContainsResult.NO;
        }
    }
}
