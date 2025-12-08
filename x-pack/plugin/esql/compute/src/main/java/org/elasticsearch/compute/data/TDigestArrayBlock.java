/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.List;

public final class TDigestArrayBlock extends AbstractNonThreadSafeRefCounted implements TDigestBlock {
    private final DoubleBlock minima;
    private final DoubleBlock maxima;
    private final DoubleBlock sums;
    private final LongBlock valueCounts;
    private final BytesRefBlock encodedDigests;

    public TDigestArrayBlock(
        BytesRefBlock encodedDigests,
        DoubleBlock minima,
        DoubleBlock maxima,
        DoubleBlock sums,
        LongBlock valueCounts
    ) {
        this.encodedDigests = encodedDigests;
        this.minima = minima;
        this.maxima = maxima;
        this.sums = sums;
        this.valueCounts = valueCounts;
    }

    private List<Block> getSubBlocks() {
        return List.of(minima, maxima, sums, valueCounts, encodedDigests);
    }

    @Override
    protected void closeInternal() {
        Releasables.close(getSubBlocks());
    }

    @Override
    public Vector asVector() {
        return null;
    }

    @Override
    public int getTotalValueCount() {
        return encodedDigests.getTotalValueCount();
    }

    @Override
    public int getPositionCount() {
        return encodedDigests.getPositionCount();
    }

    @Override
    public int getFirstValueIndex(int position) {
        return position;
    }

    @Override
    public int getValueCount(int position) {
        return isNull(position) ? 0 : 1;
    }

    @Override
    public ElementType elementType() {
        throw new UnsupportedOperationException("Need to implement this later");
    }

    @Override
    public BlockFactory blockFactory() {
        return encodedDigests.blockFactory();
    }

    @Override
    public void allowPassingToDifferentDriver() {
        getSubBlocks().forEach(Block::allowPassingToDifferentDriver);
    }

    @Override
    public boolean isNull(int position) {
        return encodedDigests.isNull(position);
    }

    @Override
    public boolean mayHaveNulls() {
        return encodedDigests.mayHaveNulls();
    }

    @Override
    public boolean areAllValuesNull() {
        return encodedDigests.areAllValuesNull();
    }

    @Override
    public boolean mayHaveMultivaluedFields() {
        return false;
    }

    @Override
    public boolean doesHaveMultivaluedFields() {
        return false;
    }

    @Override
    public Block filter(int... positions) {
        /*
         TODO: Refactor this degelation pattern.  In fact, both AggregateMetricDoubleArrayBlock and ExponentialHistogramArrayBlock
               use the same pattern.  We should extract a composite block abstract class, I think.
        */
        DoubleBlock filteredMinima = null;
        DoubleBlock filteredMaxima = null;
        DoubleBlock filteredSums = null;
        LongBlock filteredValueCounts = null;
        BytesRefBlock filteredEncodedDigests = null;
        boolean success = false;
        try {
            filteredEncodedDigests = encodedDigests.filter(positions);
            filteredMinima = minima.filter(positions);
            filteredMaxima = maxima.filter(positions);
            filteredSums = sums.filter(positions);
            filteredValueCounts = valueCounts.filter(positions);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(filteredMinima, filteredMaxima, filteredSums, filteredValueCounts, filteredEncodedDigests);
            }
        }
        return new TDigestArrayBlock(filteredEncodedDigests, filteredMinima, filteredMaxima, filteredSums, filteredValueCounts);
    }

    @Override
    public Block keepMask(BooleanVector mask) {
        DoubleBlock filteredMinima = null;
        DoubleBlock filteredMaxima = null;
        DoubleBlock filteredSums = null;
        LongBlock filteredValueCounts = null;
        BytesRefBlock filteredEncodedDigests = null;
        boolean success = false;
        try {
            filteredEncodedDigests = encodedDigests.keepMask(mask);
            filteredMinima = minima.keepMask(mask);
            filteredMaxima = maxima.keepMask(mask);
            filteredSums = sums.keepMask(mask);
            filteredValueCounts = valueCounts.keepMask(mask);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(filteredMinima, filteredMaxima, filteredSums, filteredValueCounts, filteredEncodedDigests);
            }
        }
        return new TDigestArrayBlock(filteredEncodedDigests, filteredMinima, filteredMaxima, filteredSums, filteredValueCounts);
    }

    @Override
    public ReleasableIterator<? extends Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("Lookup is not supported on TDigestArrayBlock");
    }

    @Override
    public MvOrdering mvOrdering() {
        return MvOrdering.UNORDERED;
    }

    @Override
    public Block expand() {
        // we don't support multivalues so expanding is a no-op
        this.incRef();
        return this;
    }

    @Override
    public Block deepCopy(BlockFactory blockFactory) {
        DoubleBlock copiedMinima = null;
        DoubleBlock copiedMaxima = null;
        DoubleBlock copiedSums = null;
        LongBlock copiedValueCounts = null;
        BytesRefBlock copiedEncodedDigests = null;
        boolean success = false;
        try {
            copiedEncodedDigests = encodedDigests.deepCopy(blockFactory);
            copiedMinima = minima.deepCopy(blockFactory);
            copiedMaxima = maxima.deepCopy(blockFactory);
            copiedSums = sums.deepCopy(blockFactory);
            copiedValueCounts = valueCounts.deepCopy(blockFactory);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(copiedMinima, copiedMaxima, copiedSums, copiedValueCounts, copiedEncodedDigests);
            }
        }
        return new TDigestArrayBlock(copiedEncodedDigests, copiedMinima, copiedMaxima, copiedSums, copiedValueCounts);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Block.writeTypedBlock(encodedDigests, out);
        Block.writeTypedBlock(minima, out);
        Block.writeTypedBlock(maxima, out);
        Block.writeTypedBlock(sums, out);
        Block.writeTypedBlock(valueCounts, out);
    }

    @Override
    public long ramBytesUsed() {
        long bytes = 0;
        for (Block b : getSubBlocks()) {
            bytes += b.ramBytesUsed();
        }
        return bytes;
    }
}
