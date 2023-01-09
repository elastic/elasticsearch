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
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;

/**
 * An optimized implementation of {@link BitSet} that matches all documents to reduce memory usage.
 */
public final class MatchAllBitSet extends BitSet {
    private static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MatchAllBitSet.class);

    private FixedBitSet bits;
    private final int numBits;

    public MatchAllBitSet(int numBits) {
        this.numBits = numBits;
    }

    @Override
    public void set(int i) {
        if (bits != null) {
            bits.set(i);
        }
    }

    @Override
    public boolean getAndSet(int i) {
        if (bits != null) {
            return bits.getAndSet(i);
        } else {
            return true;
        }
    }

    private void initializeBitSet() {
        bits = new FixedBitSet(numBits);
        bits.set(0, bits.length());
    }

    @Override
    public void clear(int i) {
        if (bits == null) {
            initializeBitSet();
        }
        bits.clear(i);
    }

    @Override
    public void clear(int startIndex, int endIndex) {
        if (bits == null) {
            initializeBitSet();
        }
        bits.clear(startIndex, endIndex);
    }

    @Override
    public int cardinality() {
        if (bits != null) {
            return bits.cardinality();
        } else {
            return numBits;
        }
    }

    @Override
    public int approximateCardinality() {
        if (bits != null) {
            return bits.approximateCardinality();
        } else {
            return numBits;
        }
    }

    @Override
    public int prevSetBit(int index) {
        if (bits != null) {
            return bits.prevSetBit(index);
        } else {
            return index;
        }
    }

    @Override
    public int nextSetBit(int index) {
        if (bits != null) {
            return bits.nextSetBit(index);
        } else {
            return index;
        }
    }

    @Override
    public long ramBytesUsed() {
        return RAM_BYTES_USED + (bits != null ? bits.ramBytesUsed() : 0);
    }

    @Override
    public boolean get(int index) {
        if (bits != null) {
            return bits.get(index);
        } else {
            return true;
        }
    }

    @Override
    public int length() {
        return numBits;
    }

    @Override
    public void or(DocIdSetIterator iter) throws IOException {
        if (bits != null) {
            bits.or(iter);
        }
    }
}
