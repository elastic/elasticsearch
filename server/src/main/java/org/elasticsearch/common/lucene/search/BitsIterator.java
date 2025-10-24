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

import java.util.Objects;

/**
 * A {@link DocIdSetIterator} over set bits of a {@link Bits} instance.
 */
public final class BitsIterator extends DocIdSetIterator {

    private static final int WINDOW_SIZE = 1024;

    private final Bits bits;

    private int doc = -1;
    private final FixedBitSet bitSet;
    private int from = 0;
    private int to = 0;

    public BitsIterator(Bits bits) {
        this.bits = Objects.requireNonNull(bits);
        // 1024 bits may sound heavy at first sight but it's only a long[16] under the hood
        bitSet = new FixedBitSet(WINDOW_SIZE);
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() {
        return advance(docID() + 1);
    }

    @Override
    public int advance(int target) {
        for (;;) {
            if (target >= to) {
                if (target >= bits.length()) {
                    return doc = NO_MORE_DOCS;
                }
                refill(target);
            }

            int next = bitSet.nextSetBit(target - from);
            if (next != NO_MORE_DOCS) {
                return doc = from + next;
            } else {
                target = to;
            }
        }
    }

    private void refill(int target) {
        assert target >= to;
        from = target;
        bitSet.set(0, WINDOW_SIZE);
        if (bits.length() - from < WINDOW_SIZE) {
            to = bits.length();
            bitSet.clear(to - from, WINDOW_SIZE);
        } else {
            to = from + WINDOW_SIZE;
        }
        bits.applyMask(bitSet, from);
    }

    @Override
    public long cost() {
        // We have no better estimate
        return bits.length();
    }
}
