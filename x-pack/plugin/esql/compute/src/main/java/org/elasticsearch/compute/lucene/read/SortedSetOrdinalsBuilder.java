/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

public final class SortedSetOrdinalsBuilder implements BlockLoader.SortedSetOrdinalsBuilder, Releasable, Block.Builder {
    private final BlockFactory blockFactory;
    private final SortedSetDocValues docValues;
    private int minOrd = Integer.MAX_VALUE;
    private int maxOrd = Integer.MIN_VALUE;
    private int totalValueCount;
    private final IntBlock.Builder ordsBuilder;

    public SortedSetOrdinalsBuilder(BlockFactory blockFactory, SortedSetDocValues docValues, int count) {
        this.blockFactory = blockFactory;
        this.docValues = docValues;
        this.ordsBuilder = blockFactory.newIntBlockBuilder(count).mvOrdering(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);
    }

    @Override
    public SortedSetOrdinalsBuilder appendNull() {
        ordsBuilder.appendNull();
        return this;
    }

    @Override
    public SortedSetOrdinalsBuilder appendOrd(int ord) {
        minOrd = Math.min(minOrd, ord);
        maxOrd = Math.max(maxOrd, ord);
        ordsBuilder.appendInt(ord);
        totalValueCount++;
        return this;
    }

    @Override
    public SortedSetOrdinalsBuilder beginPositionEntry() {
        ordsBuilder.beginPositionEntry();
        return this;
    }

    @Override
    public SortedSetOrdinalsBuilder endPositionEntry() {
        ordsBuilder.endPositionEntry();
        return this;
    }

    private BytesRefBlock buildBlock(IntBlock ordinals) {
        final int numOrds = maxOrd - minOrd + 1;
        final long breakerSize = arraySize(numOrds);
        blockFactory.adjustBreaker(breakerSize);
        BytesRefVector dict = null;
        IntBlock mappedOrds = null;
        try {
            final int[] newOrds = new int[numOrds];
            Arrays.fill(newOrds, -1);
            for (int p = 0; p < ordinals.getPositionCount(); p++) {
                int count = ordinals.getValueCount(p);
                if (count > 0) {
                    int first = ordinals.getFirstValueIndex(p);
                    for (int i = 0; i < count; i++) {
                        int oldOrd = ordinals.getInt(first + i);
                        newOrds[oldOrd - minOrd] = 0;
                    }
                }
            }
            int nextOrd = -1;
            try (BytesRefVector.Builder dictBuilder = blockFactory.newBytesRefVectorBuilder(Math.min(newOrds.length, totalValueCount))) {
                for (int i = 0; i < newOrds.length; i++) {
                    if (newOrds[i] != -1) {
                        newOrds[i] = ++nextOrd;
                        dictBuilder.appendBytesRef(docValues.lookupOrd(i + minOrd));
                    }
                }
                dict = dictBuilder.build();
            } catch (IOException e) {
                throw new UncheckedIOException("error resolving ordinals", e);
            }
            mappedOrds = remapOrdinals(ordinals, newOrds, minOrd);
            final OrdinalBytesRefBlock result = new OrdinalBytesRefBlock(mappedOrds, dict);
            dict = null;
            mappedOrds = null;
            return result;
        } finally {
            Releasables.close(() -> blockFactory.adjustBreaker(-breakerSize), mappedOrds, dict);
        }
    }

    private IntBlock remapOrdinals(IntBlock ordinals, int[] newOrds, int shiftOrd) {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(totalValueCount)) {
            for (int p = 0; p < ordinals.getPositionCount(); p++) {
                int valueCount = ordinals.getValueCount(p);
                switch (valueCount) {
                    case 0 -> builder.appendNull();
                    case 1 -> {
                        int ord = ordinals.getInt(ordinals.getFirstValueIndex(p));
                        builder.appendInt(newOrds[ord - shiftOrd]);
                    }
                    default -> {
                        int first = ordinals.getFirstValueIndex(p);
                        builder.beginPositionEntry();
                        int last = first + valueCount;
                        for (int i = first; i < last; i++) {
                            int ord = ordinals.getInt(i);
                            builder.appendInt(newOrds[ord - shiftOrd]);
                        }
                        builder.endPositionEntry();
                    }
                }
            }
            builder.mvOrdering(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);
            return builder.build();
        }
    }

    @Override
    public long estimatedBytes() {
        /*
         * This is a *terrible* estimate because we have no idea how big the
         * values in the ordinals are.
         */
        final int numOrds = minOrd <= maxOrd ? maxOrd - minOrd + 1 : 0;
        return totalValueCount * 4L + Math.min(numOrds, totalValueCount) * 20L;
    }

    @Override
    public BytesRefBlock build() {
        try (IntBlock ordinals = ordsBuilder.build()) {
            if (ordinals.areAllValuesNull()) {
                return (BytesRefBlock) blockFactory.newConstantNullBlock(ordinals.getPositionCount());
            }
            return buildBlock(ordinals);
        }
    }

    @Override
    public void close() {
        ordsBuilder.close();
    }

    @Override
    public Block.Builder copyFrom(Block block, int beginInclusive, int endExclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block.Builder mvOrdering(Block.MvOrdering mvOrdering) {
        throw new UnsupportedOperationException();
    }

    private static long arraySize(int ordsCount) {
        return RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) ordsCount * Integer.BYTES;
    }
}
