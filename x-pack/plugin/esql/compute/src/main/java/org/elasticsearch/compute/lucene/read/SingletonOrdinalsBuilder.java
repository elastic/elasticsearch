/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

public class SingletonOrdinalsBuilder implements BlockLoader.SingletonOrdinalsBuilder, Releasable, Block.Builder {
    private final BlockFactory blockFactory;
    private final SortedDocValues docValues;
    private int minOrd = Integer.MAX_VALUE;
    private int maxOrd = Integer.MIN_VALUE;
    private final int[] ords;
    private int count;
    private final boolean isDense;

    public SingletonOrdinalsBuilder(BlockFactory blockFactory, SortedDocValues docValues, int count, boolean isDense) {
        this.blockFactory = blockFactory;
        this.docValues = docValues;
        blockFactory.adjustBreaker(ordsSize(count));
        this.ords = new int[count];
        this.isDense = isDense;
    }

    @Override
    public SingletonOrdinalsBuilder appendNull() {
        assert isDense == false;
        ords[count++] = -1; // real ords can't be < 0, so we use -1 as null
        return this;
    }

    @Override
    public SingletonOrdinalsBuilder appendOrd(int ord) {
        ords[count++] = ord;
        minOrd = Math.min(minOrd, ord);
        maxOrd = Math.max(maxOrd, ord);
        return this;
    }

    @Override
    public BlockLoader.SingletonOrdinalsBuilder appendOrds(int[] values, int from, int length, int minOrd, int maxOrd) {
        System.arraycopy(values, from, ords, count, length);
        this.count += length;
        this.minOrd = Math.min(this.minOrd, minOrd);
        this.maxOrd = Math.max(this.maxOrd, maxOrd);
        return this;
    }

    @Override
    public BlockLoader.SingletonOrdinalsBuilder appendOrds(int ord, int length) {
        Arrays.fill(ords, count, count + length, ord);
        this.minOrd = Math.min(this.minOrd, ord);
        this.maxOrd = Math.max(this.maxOrd, ord);
        this.count += length;
        return this;
    }

    @Override
    public SingletonOrdinalsBuilder beginPositionEntry() {
        throw new UnsupportedOperationException("should only have one value per doc");
    }

    @Override
    public SingletonOrdinalsBuilder endPositionEntry() {
        throw new UnsupportedOperationException("should only have one value per doc");
    }

    private BytesRefBlock tryBuildConstantBlock() {
        if (minOrd != maxOrd) {
            return null;
        }
        if (isDense == false) {
            for (int ord : ords) {
                if (ord == -1) {
                    return null;
                }
            }
        }
        final BytesRef v;
        try {
            v = BytesRef.deepCopyOf(docValues.lookupOrd(minOrd));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to lookup ordinals", e);
        }
        BytesRefVector bytes = null;
        IntVector ordinals = null;
        boolean success = false;
        try {
            bytes = blockFactory.newConstantBytesRefVector(v, 1);
            ordinals = blockFactory.newConstantIntVector(0, ords.length);
            // Ideally, we would return a ConstantBytesRefVector, but we return an ordinal constant block instead
            // to ensure ordinal optimizations are applied when constant optimization is not available.
            final var result = new OrdinalBytesRefBlock(ordinals.asBlock(), bytes);
            success = true;
            return result;
        } finally {
            if (success == false) {
                Releasables.close(bytes, ordinals);
            }
        }
    }

    BytesRefBlock buildOrdinal() {
        int valueCount = maxOrd - minOrd + 1;
        long breakerSize = ordsSize(valueCount);
        blockFactory.adjustBreaker(breakerSize);
        BytesRefVector bytesVector = null;
        IntBlock ordinalBlock = null;
        try {
            int[] newOrds = new int[valueCount];
            Arrays.fill(newOrds, -1);
            // Re-mapping ordinals to be more space-efficient:
            if (isDense) {
                for (int ord : ords) {
                    newOrds[ord - minOrd] = 0;
                }
            } else {
                for (int ord : ords) {
                    if (ord != -1) {
                        newOrds[ord - minOrd] = 0;
                    }
                }
            }
            // resolve the ordinals and remaps the ordinals
            try {
                int nextOrd = -1;
                BytesRef firstTerm = minOrd != Integer.MAX_VALUE ? docValues.lookupOrd(minOrd) : null;
                int estimatedSize;
                if (firstTerm != null) {
                    estimatedSize = Math.min(valueCount, ords.length) * firstTerm.length;
                } else {
                    estimatedSize = Math.min(valueCount, ords.length);
                }
                try (BytesRefVector.Builder bytesBuilder = blockFactory.newBytesRefVectorBuilder(estimatedSize)) {
                    if (firstTerm != null) {
                        newOrds[0] = ++nextOrd;
                        bytesBuilder.appendBytesRef(firstTerm);
                    }
                    for (int i = firstTerm != null ? 1 : 0; i < newOrds.length; i++) {
                        if (newOrds[i] != -1) {
                            newOrds[i] = ++nextOrd;
                            bytesBuilder.appendBytesRef(docValues.lookupOrd(i + minOrd));
                        }
                    }
                    bytesVector = bytesBuilder.build();
                }
            } catch (IOException e) {
                throw new UncheckedIOException("error resolving ordinals", e);
            }
            if (isDense) {
                // Reusing ords array and overwrite all slots with re-mapped ordinals
                for (int i = 0; i < ords.length; i++) {
                    ords[i] = newOrds[ords[i] - minOrd];
                }
                ordinalBlock = blockFactory.newIntArrayVector(ords, ords.length).asBlock();
            } else {
                try (IntBlock.Builder ordinalsBuilder = blockFactory.newIntBlockBuilder(ords.length)) {
                    for (int ord : ords) {
                        if (ord == -1) {
                            ordinalsBuilder.appendNull();
                        } else {
                            ordinalsBuilder.appendInt(newOrds[ord - minOrd]);
                        }
                    }
                    ordinalBlock = ordinalsBuilder.build();
                }
            }
            final OrdinalBytesRefBlock result = new OrdinalBytesRefBlock(ordinalBlock, bytesVector);
            bytesVector = null;
            ordinalBlock = null;
            return result;
        } finally {
            Releasables.close(() -> blockFactory.adjustBreaker(-breakerSize), ordinalBlock, bytesVector);
        }
    }

    BytesRefBlock buildRegularBlock() {
        try {
            long breakerSize = ordsSize(ords.length);
            // Increment breaker for sorted ords.
            blockFactory.adjustBreaker(breakerSize);
            try {
                int[] sortedOrds = ords.clone();
                int uniqueCount = compactToUnique(sortedOrds);

                try (BreakingBytesRefBuilder copies = new BreakingBytesRefBuilder(blockFactory.breaker(), "ords")) {
                    long offsetsAndLength = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (uniqueCount + 1) * Integer.BYTES;
                    blockFactory.adjustBreaker(offsetsAndLength);
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
                blockFactory.adjustBreaker(-breakerSize);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("error resolving ordinals", e);
        }
    }

    @Override
    public long estimatedBytes() {
        /*
         * This is a *terrible* estimate because we have no idea how big the
         * values in the ordinals are.
         */
        long overhead = shouldBuildOrdinalsBlock() ? 5 : 20;
        return ords.length * overhead;
    }

    @Override
    public BytesRefBlock build() {
        if (count != ords.length) {
            assert false : "expected " + ords.length + " values but got " + count;
            throw new IllegalStateException("expected " + ords.length + " values but got " + count);
        }
        var constantBlock = tryBuildConstantBlock();
        if (constantBlock != null) {
            return constantBlock;
        }
        return shouldBuildOrdinalsBlock() ? buildOrdinal() : buildRegularBlock();
    }

    boolean shouldBuildOrdinalsBlock() {
        if (minOrd <= maxOrd) {
            int numOrds = maxOrd - minOrd + 1;
            return OrdinalBytesRefBlock.isDense(ords.length, numOrds);
        } else {
            return false;
        }
    }

    @Override
    public void close() {
        blockFactory.adjustBreaker(-ordsSize(ords.length));
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
