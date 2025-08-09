/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Fork of {@link SingletonOrdinalsBuilder} but specialized for mainly _tsid field, but also time series dimension fields.
 * The _tsid field is dense and used as primary sort field, and therefore we optimize a little more.
 * Additionally, this implementation can collect complete value blocks.
 */
public final class TSSingletonOrdinalsBuilder implements BlockLoader.TSSingletonOrdinalsBuilder, Releasable, Block.Builder {

    private static final int TSID_SIZE_GUESS = 2 + 16 + 16 + 4 * 8;

    private final boolean isPrimaryIndexSortField;
    private final BlockFactory blockFactory;
    private final SortedDocValues docValues;
    private final long[] ords;
    private int count;

    public TSSingletonOrdinalsBuilder(
        boolean isPrimaryIndexSortField,
        BlockFactory blockFactory,
        SortedDocValues docValues,
        int initialSize
    ) {
        this.isPrimaryIndexSortField = isPrimaryIndexSortField;
        this.blockFactory = blockFactory;
        this.docValues = docValues;
        blockFactory.adjustBreaker(nativeOrdsSize(initialSize));
        // tsdb codec uses long array natively. Use this array the capture native encoded ordinals and then later in build() convert:
        this.ords = new long[initialSize];
    }

    @Override
    public TSSingletonOrdinalsBuilder appendOrd(long ord) {
        ords[count++] = ord;
        return this;
    }

    @Override
    public BlockLoader.TSSingletonOrdinalsBuilder appendOrds(long[] values, int from, int length) {
        try {
            System.arraycopy(values, from, ords, count, length);
        } catch (ArrayIndexOutOfBoundsException e) {
            throw e;
        }
        count += length;
        return this;
    }

    @Override
    public long estimatedBytes() {
        return (long) ords.length * Long.BYTES;
    }

    @Override
    public BytesRefBlock build() {
        assert ords.length == count;

        int minOrd;
        int maxOrd;
        boolean isDense = true;
        if (isPrimaryIndexSortField) {
            minOrd = Math.toIntExact(ords[0]);
            maxOrd = Math.toIntExact(ords[count - 1]);
            // I think we're always ordinals dense in this case?
        } else {
            long tmpMinOrd = Long.MAX_VALUE;
            long tmpMaxOrd = Long.MIN_VALUE;
            long prevOrd = ords[0] - 1;
            for (int i = 0; i < count; i++) {
                long ord = ords[i];
                tmpMinOrd = Math.min(tmpMinOrd, ord);
                tmpMaxOrd = Math.max(tmpMaxOrd, ord);
                if (ord - prevOrd != 1) {
                    isDense = false;
                }
            }
            minOrd = Math.toIntExact(tmpMinOrd);
            maxOrd = Math.toIntExact(tmpMaxOrd);
        }

        var constantBlock = tryBuildConstantBlock(minOrd, maxOrd);
        if (constantBlock != null) {
            return constantBlock;
        }
        int valueCount = maxOrd - minOrd + 1;
        return buildOrdinal(minOrd, maxOrd, valueCount, isDense);
    }

    BytesRefBlock buildOrdinal(int minOrd, int maxOrd, int valueCount, boolean isDense) {
        long breakerSize = ordsSize(count);
        blockFactory.adjustBreaker(breakerSize);

        BytesRefVector bytesVector = null;
        IntBlock ordinalBlock = null;
        try {
            // Convert back to int[] and remap ordinals
            int[] newOrds = new int[count];
            for (int i = 0; i < count; i++) {
                newOrds[i] = Math.toIntExact(ords[i]) - minOrd;
            }
            try (BytesRefVector.Builder bytesBuilder = blockFactory.newBytesRefVectorBuilder(valueCount * TSID_SIZE_GUESS)) {
                if (isDense) {
                    TermsEnum tenum = docValues.termsEnum();
                    tenum.seekExact(minOrd);
                    for (BytesRef term = tenum.term(); term != null && tenum.ord() <= maxOrd; term = tenum.next()) {
                        bytesBuilder.appendBytesRef(term);
                    }
                } else {
                    for (int ord = minOrd; ord <= maxOrd; ord++) {
                        bytesBuilder.appendBytesRef(docValues.lookupOrd(ord));
                    }
                }
                bytesVector = bytesBuilder.build();
            } catch (IOException e) {
                throw new UncheckedIOException("error resolving tsid ordinals", e);
            }
            ordinalBlock = blockFactory.newIntArrayVector(newOrds, newOrds.length).asBlock();
            final OrdinalBytesRefBlock result = new OrdinalBytesRefBlock(ordinalBlock, bytesVector);
            assert ords.length == result.getPositionCount();

            bytesVector = null;
            ordinalBlock = null;
            return result;
        } finally {
            Releasables.close(() -> blockFactory.adjustBreaker(-breakerSize), ordinalBlock, bytesVector);
        }
    }

    private BytesRefBlock tryBuildConstantBlock(int minOrd, int maxOrd) {
        if (minOrd != maxOrd) {
            return null;
        }

        final BytesRef v;
        try {
            v = BytesRef.deepCopyOf(docValues.lookupOrd(minOrd));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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

    @Override
    public void close() {
        blockFactory.adjustBreaker(-nativeOrdsSize(ords.length));
    }

    @Override
    public TSSingletonOrdinalsBuilder appendNull() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TSSingletonOrdinalsBuilder beginPositionEntry() {
        throw new UnsupportedOperationException("should only have one value per doc");
    }

    @Override
    public TSSingletonOrdinalsBuilder endPositionEntry() {
        throw new UnsupportedOperationException("should only have one value per doc");
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
        return RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) ordsCount * Integer.BYTES;
    }

    private static long nativeOrdsSize(int ordsCount) {
        return RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) ordsCount * Long.BYTES;
    }

}
