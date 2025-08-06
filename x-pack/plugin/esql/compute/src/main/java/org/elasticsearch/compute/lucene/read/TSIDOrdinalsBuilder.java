/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.index.SortedDocValues;
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

/**
 * Fork of {@link SingletonOrdinalsBuilder} but specialized for _tsid field.
 * The _tsid field is dense and used as primary sort field, and therefore we optimize a little more.
 * Additionally, this implementation can collect complete value blocks.
 */
public class TSIDOrdinalsBuilder implements BlockLoader.TSIDOrdinalsBuilder, Releasable, Block.Builder {

    private static final int TSID_SIZE_GUESS =  2 + 16 + 16 + 4 * 8;

    private final BlockFactory blockFactory;
    private final SortedDocValues docValues;
    private final long[] ords;
    private int count;

    public TSIDOrdinalsBuilder(BlockFactory blockFactory, SortedDocValues docValues, int initialSize) {
        this.blockFactory = blockFactory;
        this.docValues = docValues;
        blockFactory.adjustBreaker(nativeOrdsSize(initialSize));
        // tsdb codec uses long array natively. Use this array the capture native encoded ordinals and then later in build() convert:
        this.ords = new long[initialSize];
    }

    @Override
    public TSIDOrdinalsBuilder appendOrd(long ord) {
        ords[count++] = ord;
        return this;
    }

    @Override
    public BlockLoader.TSIDOrdinalsBuilder appendOrds(long[] values, int from, int length) {
        System.arraycopy(values, from, ords, count, length);
        count += length;
        return this;
    }

    @Override
    public long estimatedBytes() {
        return (long) ords.length * Long.BYTES;
    }

    @Override
    public BytesRefBlock build() {
        // TODO: detect when constant block can be used like is done in: https://github.com/elastic/elasticsearch/pull/132456
        return buildOrdinal();
    }

    BytesRefBlock buildOrdinal() {
        assert ords.length == count;

        int minOrd = Math.toIntExact(ords[0]);
        int maxOrd = Math.toIntExact(ords[count - 1]);
        int valueCount = maxOrd - minOrd + 1;

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
                for (int ord = minOrd; ord <= maxOrd; ord++) {
                    bytesBuilder.appendBytesRef(docValues.lookupOrd(ord));
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

    @Override
    public void close() {
        blockFactory.adjustBreaker(-nativeOrdsSize(ords.length));
    }

    @Override
    public TSIDOrdinalsBuilder appendNull() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TSIDOrdinalsBuilder beginPositionEntry() {
        throw new UnsupportedOperationException("should only have one value per doc");
    }

    @Override
    public TSIDOrdinalsBuilder endPositionEntry() {
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
