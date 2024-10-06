/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.util;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;

/**
 * An optimized implementation of {@link BitSet} that matches all documents to reduce memory usage.
 */
public final class MatchAllBitSet extends BitSet {
    private static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MatchAllBitSet.class);

    private final int numBits;

    public MatchAllBitSet(int numBits) {
        this.numBits = numBits;
    }

    @Override
    public void set(int i) {

    }

    @Override
    public boolean getAndSet(int i) {
        return true;
    }

    @Override
    public void clear(int i) {
        assert false : "MatchAllBitSet doesn't support clear";
        throw new UnsupportedOperationException("MatchAllBitSet doesn't support clear");
    }

    @Override
    public void clear(int startIndex, int endIndex) {
        assert false : "MatchAllBitSet doesn't support clear";
        throw new UnsupportedOperationException("MatchAllBitSet doesn't support clear");
    }

    @Override
    public int cardinality() {
        return numBits;
    }

    @Override
    public int approximateCardinality() {
        return numBits;
    }

    @Override
    public int prevSetBit(int index) {
        return index;
    }

    @Override
    public int nextSetBit(int index) {
        return index;
    }

    @Override
    public int nextSetBit(int index, int upperBound) {
        assert index < upperBound;
        return index;
    }

    @Override
    public long ramBytesUsed() {
        return RAM_BYTES_USED;
    }

    @Override
    public boolean get(int index) {
        return true;
    }

    @Override
    public int length() {
        return numBits;
    }

    @Override
    public void or(DocIdSetIterator iter) throws IOException {

    }
}
