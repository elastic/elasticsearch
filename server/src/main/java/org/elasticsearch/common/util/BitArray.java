/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;

/**
 * A bit array that is implemented using a growing {@link LongArray}
 * created from {@link BigArrays}.
 * The underlying long array grows lazily based on the biggest index
 * that needs to be set.
 */
public final class BitArray implements Accountable, Releasable, Writeable {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BitArray.class);

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
     * Create a {@link BitArray} using {@link BigArrays} with bytes are written by {@link BitArray#writeTo}
     */
    public BitArray(BigArrays bigArrays, boolean readOnly, StreamInput in) throws IOException {
        this.bigArrays = bigArrays;
        final long numBits = in.readVLong();
        this.bits = bigArrays.newLongArray(wordNum(numBits), readOnly == false);
        boolean success = false;
        try {
            this.bits.fillWith(in);
            success = true;
        } finally {
            if (success == false) {
                this.bits.close();
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(size());
        bits.writeTo(out);
    }

    /**
     * Set or clear the {@code index}th bit based on the specified value.
     */
    public void set(long index, boolean value) {
        if (value) {
            set(index);
        } else {
            clear(index);
        }
    }

    /**
     * Set the {@code index}th bit.
     */
    public void set(long index) {
        long wordNum = wordNum(index);
        bits = bigArrays.grow(bits, wordNum + 1);
        bits.set(wordNum, bits.get(wordNum) | bitmask(index));
    }

    /**
     * Set the {@code index}th bit and return {@code true} if the bit was set already.
     */
    public boolean getAndSet(long index) {
        long wordNum = wordNum(index);
        bits = bigArrays.grow(bits, wordNum + 1);
        long word = bits.get(wordNum);
        long bitMask = bitmask(index);
        bits.set(wordNum, word | bitMask);
        return (word & bitMask) != 0;
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

    /**
     * Set or clear slots between {@code fromIndex} inclusive to {@code toIndex} based on {@code value}.
     */
    public void fill(long fromIndex, long toIndex, boolean value) {
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException("From should be less than or equal to toIndex");
        }
        long currentSize = size();
        if (value == false) {
            // There's no need to grow the array just to clear bits.
            toIndex = Math.min(toIndex, currentSize);
        }
        if (fromIndex >= toIndex) {
            // Empty range or false values after the end of the array.
            return;
        }

        if (toIndex > currentSize) {
            bits = bigArrays.grow(bits, wordNum(toIndex) + 1);
        }

        int wordLength = Long.BYTES * Byte.SIZE;
        long fullWord = 0xFFFFFFFFFFFFFFFFL;

        long firstWordIndex = fromIndex % wordLength;
        long lastWordIndex = toIndex % wordLength;

        long firstWordNum = wordNum(fromIndex);
        long lastWordNum = wordNum(toIndex - 1);

        // Mask first word
        if (firstWordIndex > 0) {
            long mask = fullWord << firstWordIndex;

            if (firstWordNum == lastWordNum) {
                mask &= fullWord >>> (wordLength - lastWordIndex);
            }

            if (value) {
                bits.set(firstWordNum, bits.get(firstWordNum) | mask);
            } else {
                bits.set(firstWordNum, bits.get(firstWordNum) & ~mask);
            }

            firstWordNum++;
        }

        // Mask last word
        if (firstWordNum <= lastWordNum) {
            long mask = fullWord >>> (wordLength - lastWordIndex);

            if (value) {
                bits.set(lastWordNum, bits.get(lastWordNum) | mask);
            } else {
                bits.set(lastWordNum, bits.get(lastWordNum) & ~mask);
            }
        }

        if (firstWordNum < lastWordNum) {
            bits.fill(firstWordNum, lastWordNum, value ? fullWord : 0L);
        }
    }

    public long size() {
        return bits.size() * (long) Long.BYTES * Byte.SIZE;
    }

    private static long wordNum(long index) {
        return index >> 6;
    }

    private static long bitmask(long index) {
        return 1L << index;
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(bits);
    }

    @Override
    public void close() {
        Releasables.close(bits);
    }
}
