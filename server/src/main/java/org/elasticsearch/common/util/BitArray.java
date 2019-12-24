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

    public BitArray(int initialSize, BigArrays bigArrays) {
        this.bigArrays = bigArrays;
        this.bits = bigArrays.newLongArray(initialSize, true);
    }

    public void set(int index) {
        fill(index, true);
    }

    public void clear(int index) {
        fill(index, false);
    }

    public boolean get(int index) {
        int wordNum = index >> 6;
        long bitmask = 1L << index;
        return (bits.get(wordNum) & bitmask) != 0;
    }

    private void fill(int index, boolean bit) {
        int wordNum = index >> 6;
        bits = bigArrays.grow(bits,wordNum+1);
        long bitmask = 1L << index;
        long value = bit ? bits.get(wordNum) | bitmask : bits.get(wordNum) & ~bitmask;
        bits.set(wordNum, value);
    }

    @Override
    public void close() {
        Releasables.close(bits);
    }
}
