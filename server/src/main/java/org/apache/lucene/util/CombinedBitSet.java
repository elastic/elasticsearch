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

package org.apache.lucene.util;

import org.apache.lucene.search.DocIdSetIterator;

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
            prev = first.prevSetBit(prev-1);
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
            next = first.nextSetBit(next+1);
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
}
