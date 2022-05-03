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
import org.apache.lucene.util.Bits;

/**
 * A {@link BitSet} implementation that combines two instances of {@link BitSet} and {@link Bits}
 * to provide a single merged view.
 */
public final class CombinedBitSet extends BitSet implements Bits {
    private final BitSet first;
    private final Bits second;
    private final int length;

    public CombinedBitSet(BitSet first, Bits second) {
        this.first = first;
        this.second = second;
        this.length = first.length();
    }

    public BitSet getFirst() {
        return first;
    }

    /**
     * This implementation is slow and requires to iterate over all bits to compute
     * the intersection. Use {@link #approximateCardinality()} for
     * a fast approximation.
     */
    @Override
    public int cardinality() {
        int card = 0;
        for (int i = 0; i < length; i++) {
            card += get(i) ? 1 : 0;
        }
        return card;
    }

    @Override
    public int approximateCardinality() {
        return first.cardinality();
    }

    @Override
    public int prevSetBit(int index) {
        assert index >= 0 && index < length : "index=" + index + ", numBits=" + length();
        int prev = first.prevSetBit(index);
        while (prev != -1 && second.get(prev) == false) {
            if (prev == 0) {
                return -1;
            }
            prev = first.prevSetBit(prev - 1);
        }
        return prev;
    }

    @Override
    public int nextSetBit(int index) {
        assert index >= 0 && index < length : "index=" + index + " numBits=" + length();
        int next = first.nextSetBit(index);
        while (next != DocIdSetIterator.NO_MORE_DOCS && second.get(next) == false) {
            if (next == length() - 1) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            next = first.nextSetBit(next + 1);
        }
        return next;
    }

    @Override
    public long ramBytesUsed() {
        return first.ramBytesUsed();
    }

    @Override
    public boolean get(int index) {
        return first.get(index) && second.get(index);
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public void set(int i) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void clear(int i) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void clear(int startIndex, int endIndex) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean getAndSet(int i) {
        throw new UnsupportedOperationException("not implemented");
    }
}
