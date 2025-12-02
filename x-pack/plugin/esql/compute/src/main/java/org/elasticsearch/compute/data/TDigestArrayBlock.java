/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
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
        assertInvariants();
    }

    private void assertInvariants() {
        for (Block b : getSubBlocks()) {
            assert b.isReleased() == false;
            assert b.doesHaveMultivaluedFields() == false : "TDigestArrayBlock sub-blocks can't have multi-values but [" + b + "] does";
            assert b.getPositionCount() == getPositionCount()
                : "TDigestArrayBlock sub-blocks must have the same position count but ["
                    + b
                    + "] has "
                    + b.getPositionCount()
                    + " instead of "
                    + getPositionCount();
            for (int i = 0; i < b.getPositionCount(); i++) {
                if (isNull(i)) {
                    assert b.isNull(i) : "TDigestArrayBlock sub-block [" + b + "] should be null at position " + i + ", but was not";
                } else {
                    if (b == sums || b == minima || b == maxima) {
                        // sums / minima / maxima should be null exactly when value count is 0 or the histogram is null
                        assert b.isNull(i) == (valueCounts.getLong(valueCounts.getFirstValueIndex(i)) == 0)
                            : "TDigestArrayBlock sums/minima/maxima sub-block [" + b + "] has wrong nullity at position " + i;
                    } else {
                        assert b.isNull(i) == false
                            : "TDigestArrayBlock sub-block [" + b + "] should be non-null at position " + i + ", but was not";
                    }
                }
            }
        }
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
        return ElementType.TDIGEST;
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

    void copyInto(
        BytesRefBlock.Builder encodedDigestsBuilder,
        DoubleBlock.Builder minimaBuilder,
        DoubleBlock.Builder maximaBuilder,
        DoubleBlock.Builder sumsBuilder,
        LongBlock.Builder valueCountsBuilder,
        int beginInclusive,
        int endExclusive
    ) {
        encodedDigestsBuilder.copyFrom(encodedDigests, beginInclusive, endExclusive);
        minimaBuilder.copyFrom(minima, beginInclusive, endExclusive);
        maximaBuilder.copyFrom(maxima, beginInclusive, endExclusive);
        sumsBuilder.copyFrom(sums, beginInclusive, endExclusive);
        valueCountsBuilder.copyFrom(valueCounts, beginInclusive, endExclusive);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Block.writeTypedBlock(encodedDigests, out);
        Block.writeTypedBlock(minima, out);
        Block.writeTypedBlock(maxima, out);
        Block.writeTypedBlock(sums, out);
        Block.writeTypedBlock(valueCounts, out);
    }

    public static TDigestArrayBlock readFrom(BlockStreamInput in) throws IOException {
        BytesRefBlock encodedDigests = null;
        DoubleBlock minima = null;
        DoubleBlock maxima = null;
        DoubleBlock sums = null;
        LongBlock valueCounts = null;

        boolean success = false;
        try {
            encodedDigests = (BytesRefBlock) Block.readTypedBlock(in);
            minima = (DoubleBlock) Block.readTypedBlock(in);
            maxima = (DoubleBlock) Block.readTypedBlock(in);
            sums = (DoubleBlock) Block.readTypedBlock(in);
            valueCounts = (LongBlock) Block.readTypedBlock(in);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(minima, maxima, sums, valueCounts, encodedDigests);
            }
        }
        return new TDigestArrayBlock(encodedDigests, minima, maxima, sums, valueCounts);
    }

    @Override
    public long ramBytesUsed() {
        long bytes = 0;
        for (Block b : getSubBlocks()) {
            bytes += b.ramBytesUsed();
        }
        return bytes;
    }

    @Override
    public void serializeTDigest(int valueIndex, SerializedTDigestOutput out, BytesRef scratch) {
        // not that this value count is different from getValueCount(position)!
        // this value count represents the number of individual samples the histogram was computed for
        long valueCount = valueCounts.getLong(valueCounts.getFirstValueIndex(valueIndex));
        out.appendLong(valueCount);
        if (valueCount > 0) {
            // sum / min / max are only non-null for non-empty histograms
            out.appendDouble(sums.getDouble(sums.getFirstValueIndex(valueIndex)));
            out.appendDouble(minima.getDouble(minima.getFirstValueIndex(valueIndex)));
            out.appendDouble(maxima.getDouble(maxima.getFirstValueIndex(valueIndex)));
        }
        out.appendBytesRef(encodedDigests.getBytesRef(encodedDigests.getFirstValueIndex(valueIndex), scratch));
    }

    @Override
    public TDigestHolder getTDigestHolder(int offset) {
        return new TDigestHolder(
            // TODO: Memory tracking? creating a new bytes ref here doesn't seem great
            encodedDigests.getBytesRef(offset, new BytesRef()),
            minima.getDouble(offset),
            maxima.getDouble(offset),
            sums.getDouble(offset),
            valueCounts.getLong(offset)
        );
    }

    public static TDigestBlock createConstant(TDigestHolder histogram, int positionCount, BlockFactory blockFactory) {
        DoubleBlock minBlock = null;
        DoubleBlock maxBlock = null;
        DoubleBlock sumBlock = null;
        LongBlock countBlock = null;
        BytesRefBlock encodedDigestsBlock = null;
        boolean success = false;
        try {
            countBlock = blockFactory.newConstantLongBlockWith(histogram.getValueCount(), positionCount);
            if (Double.isNaN(histogram.getMin())) {
                minBlock = (DoubleBlock) blockFactory.newConstantNullBlock(positionCount);
            } else {
                minBlock = blockFactory.newConstantDoubleBlockWith(histogram.getMin(), positionCount);
            }
            if (Double.isNaN(histogram.getMax())) {
                maxBlock = (DoubleBlock) blockFactory.newConstantNullBlock(positionCount);
            } else {
                maxBlock = blockFactory.newConstantDoubleBlockWith(histogram.getMax(), positionCount);
            }
            if (Double.isNaN(histogram.getSum())) {
                sumBlock = (DoubleBlock) blockFactory.newConstantNullBlock(positionCount);
            } else {
                sumBlock = blockFactory.newConstantDoubleBlockWith(histogram.getSum(), positionCount);
            }
            encodedDigestsBlock = blockFactory.newConstantBytesRefBlockWith(histogram.getEncodedDigest(), positionCount);
            success = true;
            return new TDigestArrayBlock(encodedDigestsBlock, minBlock, maxBlock, sumBlock, countBlock);
        } finally {
            if (success == false) {
                Releasables.close(minBlock, maxBlock, sumBlock, countBlock, encodedDigestsBlock);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TDigestBlock block) {
            return TDigestBlock.equals(this, block);
        }
        return false;
    }

    boolean equalsAfterTypeCheck(TDigestArrayBlock that) {
        return minima.equals(that.minima)
            && maxima.equals(that.maxima)
            && sums.equals(that.sums)
            && valueCounts.equals(that.valueCounts)
            && encodedDigests.equals(that.encodedDigests);
    }

    @Override
    public int hashCode() {
        /*
         for now we use just the hash of encodedDigests
         this ensures proper equality with null blocks and should be unique enough for practical purposes.
         This mirrors the behavior in Exponential Histogram
        */
        return encodedDigests.hashCode();
    }
}
