/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 * A bit array that is implemented using a growing {@link LongArray}
 * created from {@link BigArrays}.
 * The underlying long array grows lazily based on the biggest index
 * that needs to be set.
 */
public final class BitArray implements Releasable {
    private final BigArrays bigArrays;
    private LongArray bits;

    /**
     * Create the {@linkplain BitArray}.
     * @param initialSize the initial size of underlying storage expressed in bits.
     */
    public BitArray(long initialSize, BigArrays bigArrays) {
        this.bigArrays = bigArrays;
        this.bits = bigArrays.newLongArray(wordNum(initialSize) + 1, true);
    }

    /**
     * Set the {@code index}th bit.
     */
    public void set(long index) {
        long wordNum = wordNum(index);
        bits = bigArrays.grow(bits, wordNum + 1);
        bits.set(wordNum, bits.get(wordNum) | bitmask(index));
    }

    /** this = this OR other */
    public void or(BitArray other) {
        or(other.bits);
    }

    private void or(final LongArray otherArr) {
        long pos = otherArr.size();
        bits = bigArrays.grow(bits, pos + 1);
        final LongArray thisArr = this.bits;
        while (--pos >= 0) {
            thisArr.set(pos, thisArr.get(pos) | otherArr.get(pos));
        }
    }

    public long nextSetBit(long index) {
        long wordNum = wordNum(index);
        if (wordNum >= bits.size()) {
            return Long.MAX_VALUE;
        }
        long word = bits.get(wordNum) >> index;  // skip all the bits to the right of index

        if (word != 0) {
            return index + Long.numberOfTrailingZeros(word);
        }

        while (++wordNum < bits.size()) {
            word = bits.get(wordNum);
            if (word != 0) {
                return (wordNum << 6) + Long.numberOfTrailingZeros(word);
            }
        }
        return Long.MAX_VALUE;
    }

    public long cardinality() {
        long cardinality = 0;
        for (int i = 0; i < bits.size(); ++i) {
            cardinality += Long.bitCount(bits.get(i));
        }
        return cardinality;
    }

    /**
     * Clear the {@code index}th bit.
     */
    public void clear(long index) {
        long wordNum = wordNum(index);
        if (wordNum >= bits.size()) {
            /*
             * No need to resize the array just to clear the bit because we'll
             * initialize them to false when we grow the array anyway.
             */
            return;
        }
        bits.set(wordNum, bits.get(wordNum) & ~bitmask(index));
    }

    /**
     * Is the {@code index}th bit set?
     */
    public boolean get(long index) {
        long wordNum = wordNum(index);
        if (wordNum >= bits.size()) {
            /*
             * If the word is bigger than the array then it could *never* have
             * been set.
             */
            return false;
        }
        long bitmask = 1L << index;
        return (bits.get(wordNum) & bitmask) != 0;
    }

    private static long wordNum(long index) {
        return index >> 6;
    }

    private static long bitmask(long index) {
        return 1L << index;
    }

    @Override
    public void close() {
        Releasables.close(bits);
    }
}
