/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.seqno;

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * A {@link CountedBitSet} wraps a {@link FixedBitSet} but automatically releases the internal bitset
 * when all bits are set to reduce memory usage. This structure can work well for sequence numbers as
 * these numbers are likely to form contiguous ranges (eg. filling all bits).
 */
public final class CountedBitSet {
    static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(CountedBitSet.class);
    private short onBits; // Number of bits are set.
    private FixedBitSet bitset;

    public CountedBitSet(short numBits) {
        if (numBits <= 0) {
            throw new IllegalArgumentException("Number of bits must be positive. Given [" + numBits + "]");
        }
        this.onBits = 0;
        this.bitset = new FixedBitSet(numBits);
    }

    public boolean get(int index) {
        assert 0 <= index && index < this.length();
        assert bitset == null || onBits < bitset.length() : "Bitset should be released when all bits are set";
        return bitset == null ? true : bitset.get(index);
    }

    public void set(int index) {
        assert 0 <= index && index < this.length();
        assert bitset == null || onBits < bitset.length() : "Bitset should be released when all bits are set";

        // Ignore set when bitset is full.
        if (bitset != null) {
            final boolean wasOn = bitset.getAndSet(index);
            if (wasOn == false) {
                onBits++;
                // Once all bits are set, we can simply just return YES for all indexes.
                // This allows us to clear the internal bitset and use null check as the guard.
                if (onBits == bitset.length()) {
                    bitset = null;
                }
            }
        }
    }

    // Below methods are pkg-private for testing

    int cardinality() {
        return onBits;
    }

    int length() {
        return bitset == null ? onBits : bitset.length();
    }

    boolean isInternalBitsetReleased() {
        return bitset == null;
    }
}
