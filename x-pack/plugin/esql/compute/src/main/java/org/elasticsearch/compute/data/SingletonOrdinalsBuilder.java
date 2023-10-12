/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.util.Arrays;

public class SingletonOrdinalsBuilder implements BlockLoader.SingletonOrdinalBuilder, Releasable {
    private final BlockFactory blockFactory;
    private final SortedDocValues docValues;
    private int[] ords;
    private int count;

    public SingletonOrdinalsBuilder(BlockFactory blockFactory, SortedDocValues docValues, int expectedCount) {
        this.blockFactory = blockFactory;
        this.docValues = docValues;
        blockFactory.adjustBreaker(ordsSize(), false);
        this.ords = new int[expectedCount];
    }

    @Override
    public SingletonOrdinalsBuilder appendNull() {
        ords[count++] = -1; // real ords can't be < 0, so we use -1 as null
        return this;
    }

    @Override
    public BlockLoader.IntBuilder appendInt(int value) {
        ords[count++] = value;
        return this;
    }

    @Override
    public BlockLoader.Builder beginPositionEntry() {
        throw new UnsupportedOperationException("should only have one value per doc");
    }

    @Override
    public BlockLoader.Builder endPositionEntry() {
        throw new UnsupportedOperationException("should only have one value per doc");
    }

    public BytesRefBlock build() throws IOException {
        // Increment breaker for sorted ords.
        blockFactory.adjustBreaker(ordsSize(), false);
        try {
            int[] sortedOrds = ords.clone();
            Arrays.sort(sortedOrds);
            int uniqueCount = compactToUnique(sortedOrds);

            try (BreakingBytesRefBuilder copies = new BreakingBytesRefBuilder(blockFactory.breaker(), "ords")) {
                for (int o = 0; o < ords.length; o++) {
                    builder.append(docValues.lookupOrd(ords[o]));

                }

                // It'd be better if we could load the strings one time and break them out
                try (BytesRefBlockBuilder builder = blockFactory.newBytesRefBlockBuilder(ords.length) {
                    for (int i = 0; i < ords.length; i++) {
                        if (ords[i] == -1) {
                            builder.appendNull();
                        } else {
                            builder.appendBytesRef();
                        }
                    }
                }
            }
            ords = null;
        } finally {
            blockFactory.adjustBreaker(-ordsSize(), false);
        }

        return null;
    }

    @Override
    public void close() {
        blockFactory.adjustBreaker(-ordsSize(), false);
    }

    private long ordsSize() {
        return RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + ords.length * Integer.BYTES;
    }

    static int compactToUnique(int[] sortedOrds) {
        Arrays.sort(sortedOrds);
        int uniqueSize = 0;
        int prev = -1;
        for (int i = 0; i < sortedOrds.length; i++) {
            if (sortedOrds[i] != prev) {
                sortedOrds[uniqueSize++] = sortedOrds[i];
            }
        }
        return uniqueSize;
    }
}
