/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.lucene.codec.bloom;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

final class OnDiskFuzzySet {
    private final HashFunction hashFunction;
    private final RandomAccessInput in;
    private final int bloomSize;

    OnDiskFuzzySet(IndexInput in) throws IOException {
        int version = in.readInt();
        if (version == FuzzySet.VERSION_SPI) {
            in.readString();
        }
        this.hashFunction = FuzzySet.hashFunctionForVersion(version);
        this.bloomSize = in.readInt();
        final long numLongs = in.readInt();
        this.in = in.randomAccessSlice(in.getFilePointer(), numLongs * Long.BYTES);
    }

    public FuzzySet.ContainsResult contains(BytesRef value) throws IOException {
        int hash = hashFunction.hash(value);
        if (hash < 0) {
            hash = hash * -1;
        }
        // Bloom sizes are always base 2 and so can be ANDed for a fast modulo
        hash = hash & bloomSize;

        final int index = hash >> 3; // div 8
        final int mask = 1 << (hash & 0x7);
        final byte bits = in.readByte(index);
        if ((bits & mask) != 0) {
            return FuzzySet.ContainsResult.MAYBE;
        } else {
            return FuzzySet.ContainsResult.NO;
        }
    }
}
