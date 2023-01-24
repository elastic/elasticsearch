/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 * Lookup table to represent transactions as bit sets.
 */
public class TransactionsLookupTable implements Accountable, Releasable {

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(TransactionsLookupTable.class);

    private final BigArrays bigArrays;
    private LongArray startOffsets;
    private LongArray longs;
    private long size;

    public TransactionsLookupTable(long capacity, BigArrays bigArrays) {
        this.bigArrays = bigArrays;
        boolean success = false;
        try {
            startOffsets = bigArrays.newLongArray(capacity + 1, false);
            startOffsets.set(0, 0);
            longs = bigArrays.newLongArray(capacity * 3, false);
            success = true;
        } finally {
            if (false == success) {
                close();
            }
        }
        size = 0;
    }

    public void append(ItemSetBitSet itemSetBitSet) {
        final long startOffset = startOffsets.get(size);
        longs = bigArrays.grow(longs, startOffset + itemSetBitSet.wordsInUse);
        for (int i = 0; i < itemSetBitSet.wordsInUse; ++i) {
            longs.set(startOffset + i, itemSetBitSet.words[i]);
        }
        startOffsets = bigArrays.grow(startOffsets, size + 2);
        startOffsets.set(size + 1, startOffset + itemSetBitSet.wordsInUse);
        ++size;
    }

    boolean isSubsetOf(long row, ItemSetBitSet set) {
        final long startOffset = startOffsets.get(row);
        final int wordsInUse = (int) (startOffsets.get(row + 1) - startOffset);

        if (set.wordsInUse > wordsInUse) {
            return false;
        }

        for (int i = set.wordsInUse - 1; i >= 0; i--) {
            final long word = longs.get(startOffset + i);
            if ((word & set.words[i]) != set.words[i]) return false;
        }

        return true;
    }

    public long size() {
        return size;
    }

    @Override
    public void close() {
        Releasables.close(longs, startOffsets);
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + startOffsets.ramBytesUsed() + longs.ramBytesUsed();
    }
}
