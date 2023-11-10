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
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

public class SingletonOrdinalsBuilder implements BlockLoader.SingletonOrdinalsBuilder, Releasable, Block.Builder {
    private final BlockFactory blockFactory;
    private final SortedDocValues docValues;
    private int[] ords;
    private int count;

    public SingletonOrdinalsBuilder(BlockFactory blockFactory, SortedDocValues docValues, int count) {
        this.blockFactory = blockFactory;
        this.docValues = docValues;
        blockFactory.adjustBreaker(ordsSize(count), false);
        this.ords = new int[count];
    }

    @Override
    public SingletonOrdinalsBuilder appendNull() {
        ords[count++] = -1; // real ords can't be < 0, so we use -1 as null
        return this;
    }

    @Override
    public SingletonOrdinalsBuilder appendOrd(int value) {
        ords[count++] = value;
        return this;
    }

    int[] ords() {
        return ords;
    }

    @Override
    public SingletonOrdinalsBuilder beginPositionEntry() {
        throw new UnsupportedOperationException("should only have one value per doc");
    }

    @Override
    public SingletonOrdinalsBuilder endPositionEntry() {
        throw new UnsupportedOperationException("should only have one value per doc");
    }

    @Override
    public BytesRefBlock build() {
        try {
            long breakerSize = ordsSize(ords.length);
            // Increment breaker for sorted ords.
            blockFactory.adjustBreaker(breakerSize, false);
            try {
                int[] sortedOrds = ords.clone();
                Arrays.sort(sortedOrds);
                int uniqueCount = compactToUnique(sortedOrds);

                try (BreakingBytesRefBuilder copies = new BreakingBytesRefBuilder(blockFactory.breaker(), "ords")) {
                    long offsetsAndLength = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (uniqueCount + 1) * Integer.BYTES;
                    blockFactory.adjustBreaker(offsetsAndLength, false);
                    breakerSize += offsetsAndLength;
                    int[] offsets = new int[uniqueCount + 1];
                    for (int o = 0; o < uniqueCount; o++) {
                        BytesRef v = docValues.lookupOrd(sortedOrds[o]);
                        offsets[o] = copies.length();
                        copies.append(v);
                    }
                    offsets[uniqueCount] = copies.length();

                    /*
                     * It'd be better if BytesRefBlock could run off of a deduplicated list of
                     * blocks. It can't at the moment. So we copy many times.
                     */
                    BytesRef scratch = new BytesRef();
                    scratch.bytes = copies.bytes();
                    try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(ords.length)) {
                        for (int i = 0; i < ords.length; i++) {
                            if (ords[i] == -1) {
                                builder.appendNull();
                                continue;
                            }
                            int o = Arrays.binarySearch(sortedOrds, 0, uniqueCount, ords[i]);
                            assert 0 <= o && o < uniqueCount;
                            scratch.offset = offsets[o];
                            scratch.length = offsets[o + 1] - scratch.offset;
                            builder.appendBytesRef(scratch);
                        }
                        return builder.build();
                    }
                }
            } finally {
                blockFactory.adjustBreaker(-breakerSize, false);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("error resolving ordinals", e);
        }
    }

    @Override
    public void close() {
        blockFactory.adjustBreaker(-ordsSize(ords.length), false);
    }

    @Override
    public Block.Builder appendAllValuesToCurrentPosition(Block block) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block.Builder copyFrom(Block block, int beginInclusive, int endExclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block.Builder mvOrdering(Block.MvOrdering mvOrdering) {
        throw new UnsupportedOperationException();
    }

    private static long ordsSize(int ordsCount) {
        return RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + ordsCount * Integer.BYTES;
    }

    static int compactToUnique(int[] sortedOrds) {
        Arrays.sort(sortedOrds);
        int uniqueSize = 0;
        int prev = -1;
        for (int i = 0; i < sortedOrds.length; i++) {
            if (sortedOrds[i] != prev) {
                sortedOrds[uniqueSize++] = prev = sortedOrds[i];
            }
        }
        return uniqueSize;
    }
}
