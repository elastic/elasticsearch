/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.filter;

import org.apache.lucene.search.CheckedIntConsumer;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;

/** The docs of a window that passed the filter, handed to a {@code LeafCollector} in bulk. Bit {@code i} is doc {@code offset + i}. */
final class BitSetDocIdStream extends DocIdStream {
    private final FixedBitSet bitSet;
    private final int offset;
    private final int max;
    private int upTo;

    BitSetDocIdStream(FixedBitSet bitSet, int offset) {
        this.bitSet = bitSet;
        this.offset = offset;
        this.upTo = offset;
        this.max = offset + bitSet.length();
    }

    @Override
    public boolean mayHaveRemaining() {
        return upTo < max;
    }

    @Override
    public void forEach(int upTo, CheckedIntConsumer<IOException> consumer) throws IOException {
        if (upTo > this.upTo) {
            upTo = Math.min(upTo, max);
            bitSet.forEach(this.upTo - offset, upTo - offset, offset, consumer);
            this.upTo = upTo;
        }
    }

    @Override
    public int count(int upTo) {
        if (upTo > this.upTo) {
            upTo = Math.min(upTo, max);
            int count = bitSet.cardinality(this.upTo - offset, upTo - offset);
            this.upTo = upTo;
            return count;
        }
        return 0;
    }

    @Override
    public int intoArray(int upTo, int[] array) {
        if (upTo > this.upTo) {
            upTo = Math.min(upTo, max);
            int count = bitSet.intoArray(this.upTo - offset, upTo - offset, offset, array);
            if (count == array.length) {
                upTo = array[array.length - 1] + 1;
            }
            this.upTo = upTo;
            return count;
        }
        return 0;
    }
}
