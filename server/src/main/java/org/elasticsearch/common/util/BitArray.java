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

package org.elasticsearch.common.util;

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;

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
     * @param initialSize the initial size of underlying storage.
     */
    public BitArray(int initialSize, BigArrays bigArrays) {
        this.bigArrays = bigArrays;
        this.bits = bigArrays.newLongArray(initialSize, true);
    }

    /**
     * Set the {@code index}th bit.
     */
    public void set(int index) {
        int wordNum = wordNum(index);
        bits = bigArrays.grow(bits, wordNum + 1);
        bits.set(wordNum, bits.get(wordNum) | bitmask(index));
    }

    /**
     * Clear the {@code index}th bit.
     */
    public void clear(int index) {
        int wordNum = wordNum(index);
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
    public boolean get(int index) {
        int wordNum = wordNum(index);
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

    private static int wordNum(int index) {
        return index >> 6;
    }

    private static long bitmask(int index) {
        return 1L << index;
    }

    @Override
    public void close() {
        Releasables.close(bits);
    }
}
