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
import org.elasticsearch.exponentialhistogram.CompressedExponentialHistogram;

import java.io.IOException;
import java.util.List;

final class ExponentialHistogramArrayBlock extends AbstractNonThreadSafeRefCounted implements ExponentialHistogramBlock {

    private final DoubleBlock minima;
    private final DoubleBlock maxima;
    private final DoubleBlock sums;
    private final LongBlock valueCounts;
    private final DoubleBlock zeroThresholds;
    private final BytesRefBlock encodedHistograms;

    ExponentialHistogramArrayBlock(
        DoubleBlock minima,
        DoubleBlock maxima,
        DoubleBlock sums,
        LongBlock valueCounts,
        DoubleBlock zeroThresholds,
        BytesRefBlock encodedHistograms
    ) {
        this.minima = minima;
        this.maxima = maxima;
        this.sums = sums;
        this.valueCounts = valueCounts;
        this.zeroThresholds = zeroThresholds;
        this.encodedHistograms = encodedHistograms;
        assert assertInvariants();
    }

    private boolean assertInvariants() {
        for (Block b : getSubBlocks()) {
            assert b.isReleased() == false;
            assert b.doesHaveMultivaluedFields() == false
                : "ExponentialHistogramArrayBlock sub-blocks can't have multi-values but [" + b + "] does";
            assert b.getPositionCount() == getPositionCount()
                : "ExponentialHistogramArrayBlock sub-blocks must have the same position count but ["
                    + b
                    + "] has "
                    + b.getPositionCount()
                    + " instead of "
                    + getPositionCount();
            for (int i = 0; i < b.getPositionCount(); i++) {
                if (isNull(i)) {
                    assert b.isNull(i)
                        : "ExponentialHistogramArrayBlock sub-block [" + b + "] should be null at position " + i + ", but was not";
                } else {
                    if (b == minima || b == maxima) {
                        // minima / maxima should be null exactly when value count is 0 or the histogram is null
                        assert b.isNull(i) == (valueCounts.getLong(valueCounts.getFirstValueIndex(i)) == 0)
                            : "ExponentialHistogramArrayBlock minima/maxima sub-block [" + b + "] has wrong nullity at position " + i;
                    } else {
                        assert b.isNull(i) == false
                            : "ExponentialHistogramArrayBlock sub-block [" + b + "] should be non-null at position " + i + ", but was not";
                    }
                }
            }
        }
        return true;
    }

    private List<Block> getSubBlocks() {
        return List.of(sums, valueCounts, zeroThresholds, encodedHistograms, minima, maxima);
    }

    void loadValue(int valueIndex, CompressedExponentialHistogram resultHistogram, BytesRef tempBytesRef) {
        BytesRef bytes = getEncodedHistogramBytes(valueIndex, tempBytesRef);
        double zeroThreshold = getHistogramZeroThreshold(valueIndex);
        long valueCount = getHistogramValueCount(valueIndex);
        double sum = getHistogramSum(valueIndex);
        double min = getHistogramMin(valueIndex);
        double max = getHistogramMax(valueIndex);
        try {
            resultHistogram.reset(zeroThreshold, valueCount, sum, min, max, bytes);
        } catch (IOException e) {
            throw new IllegalStateException("error loading histogram", e);
        }
    }

    void serializeValue(int valueIndex, SerializedOutput out, BytesRef tempBytesRef) {
        long valueCount = getHistogramValueCount(valueIndex);
        out.appendLong(valueCount);
        out.appendDouble(getHistogramSum(valueIndex));
        out.appendDouble(getHistogramZeroThreshold(valueIndex));
        if (valueCount > 0) {
            // min / max are only non-null for non-empty histograms
            out.appendDouble(getHistogramMin(valueIndex));
            out.appendDouble(getHistogramMax(valueIndex));
        }
        out.appendBytesRef(getEncodedHistogramBytes(valueIndex, tempBytesRef));
    }

    private double getHistogramMin(int valueIndex) {
        int minimaValIndex = minima.getFirstValueIndex(valueIndex);
        return minima.isNull(minimaValIndex) ? Double.NaN : minima.getDouble(minimaValIndex);
    }

    private double getHistogramMax(int valueIndex) {
        int maximaValIndex = maxima.getFirstValueIndex(valueIndex);
        return maxima.isNull(maximaValIndex) ? Double.NaN : maxima.getDouble(maximaValIndex);
    }

    private double getHistogramSum(int valueIndex) {
        return sums.getDouble(sums.getFirstValueIndex(valueIndex));
    }

    private long getHistogramValueCount(int valueIndex) {
        return valueCounts.getLong(valueCounts.getFirstValueIndex(valueIndex));
    }

    private double getHistogramZeroThreshold(int valueIndex) {
        return zeroThresholds.getDouble(zeroThresholds.getFirstValueIndex(valueIndex));
    }

    private BytesRef getEncodedHistogramBytes(int valueIndex, BytesRef tempBytesRef) {
        return encodedHistograms.getBytesRef(encodedHistograms.getFirstValueIndex(valueIndex), tempBytesRef);
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
        return encodedHistograms.getTotalValueCount();
    }

    @Override
    public int getPositionCount() {
        return encodedHistograms.getPositionCount();
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
        return ElementType.EXPONENTIAL_HISTOGRAM;
    }

    @Override
    public BlockFactory blockFactory() {
        return encodedHistograms.blockFactory();
    }

    @Override
    public void allowPassingToDifferentDriver() {
        getSubBlocks().forEach(Block::allowPassingToDifferentDriver);
    }

    @Override
    public boolean isNull(int position) {
        return encodedHistograms.isNull(position);
    }

    @Override
    public boolean mayHaveNulls() {
        return encodedHistograms.mayHaveNulls();
    }

    @Override
    public boolean areAllValuesNull() {
        return encodedHistograms.areAllValuesNull();
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
        DoubleBlock filteredMinima = null;
        DoubleBlock filteredMaxima = null;
        DoubleBlock filteredSums = null;
        LongBlock filteredValueCounts = null;
        DoubleBlock filteredZeroThresholds = null;
        BytesRefBlock filteredEncodedHistograms = null;
        boolean success = false;
        try {
            filteredMinima = minima.filter(positions);
            filteredMaxima = maxima.filter(positions);
            filteredSums = sums.filter(positions);
            filteredValueCounts = valueCounts.filter(positions);
            filteredZeroThresholds = zeroThresholds.filter(positions);
            filteredEncodedHistograms = encodedHistograms.filter(positions);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(
                    filteredMinima,
                    filteredMaxima,
                    filteredSums,
                    filteredValueCounts,
                    filteredZeroThresholds,
                    filteredEncodedHistograms
                );
            }
        }
        return new ExponentialHistogramArrayBlock(
            filteredMinima,
            filteredMaxima,
            filteredSums,
            filteredValueCounts,
            filteredZeroThresholds,
            filteredEncodedHistograms
        );
    }

    @Override
    public Block keepMask(BooleanVector mask) {
        DoubleBlock filteredMinima = null;
        DoubleBlock filteredMaxima = null;
        DoubleBlock filteredSums = null;
        LongBlock filteredValueCounts = null;
        DoubleBlock filteredZeroThresholds = null;
        BytesRefBlock filteredEncodedHistograms = null;
        boolean success = false;
        try {
            filteredMinima = minima.keepMask(mask);
            filteredMaxima = maxima.keepMask(mask);
            filteredSums = sums.keepMask(mask);
            filteredValueCounts = valueCounts.keepMask(mask);
            filteredZeroThresholds = zeroThresholds.keepMask(mask);
            filteredEncodedHistograms = encodedHistograms.keepMask(mask);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(
                    filteredMinima,
                    filteredMaxima,
                    filteredSums,
                    filteredValueCounts,
                    filteredZeroThresholds,
                    filteredEncodedHistograms
                );
            }
        }
        return new ExponentialHistogramArrayBlock(
            filteredMinima,
            filteredMaxima,
            filteredSums,
            filteredValueCounts,
            filteredZeroThresholds,
            filteredEncodedHistograms
        );
    }

    @Override
    public ReleasableIterator<? extends Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("can't lookup values from ExponentialHistogramArrayBlock");
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
    public ExponentialHistogramArrayBlock deepCopy(BlockFactory blockFactory) {
        DoubleBlock copiedMinima = null;
        DoubleBlock copiedMaxima = null;
        DoubleBlock copiedSums = null;
        LongBlock copiedValueCounts = null;
        DoubleBlock copiedZeroThresholds = null;
        BytesRefBlock copiedEncodedHistograms = null;
        boolean success = false;
        try {
            copiedMinima = minima.deepCopy(blockFactory);
            copiedMaxima = maxima.deepCopy(blockFactory);
            copiedSums = sums.deepCopy(blockFactory);
            copiedValueCounts = valueCounts.deepCopy(blockFactory);
            copiedZeroThresholds = zeroThresholds.deepCopy(blockFactory);
            copiedEncodedHistograms = encodedHistograms.deepCopy(blockFactory);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(copiedMinima, copiedMaxima, copiedSums, copiedValueCounts, copiedZeroThresholds, copiedEncodedHistograms);
            }
        }
        return new ExponentialHistogramArrayBlock(
            copiedMinima,
            copiedMaxima,
            copiedSums,
            copiedValueCounts,
            copiedZeroThresholds,
            copiedEncodedHistograms
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Block.writeTypedBlock(minima, out);
        Block.writeTypedBlock(maxima, out);
        Block.writeTypedBlock(sums, out);
        Block.writeTypedBlock(valueCounts, out);
        Block.writeTypedBlock(zeroThresholds, out);
        Block.writeTypedBlock(encodedHistograms, out);
    }

    public static ExponentialHistogramArrayBlock readFrom(BlockStreamInput in) throws IOException {
        DoubleBlock minima = null;
        DoubleBlock maxima = null;
        DoubleBlock sums = null;
        LongBlock valueCounts = null;
        DoubleBlock zeroThresholds = null;
        BytesRefBlock encodedHistograms = null;

        boolean success = false;
        try {
            minima = (DoubleBlock) Block.readTypedBlock(in);
            maxima = (DoubleBlock) Block.readTypedBlock(in);
            sums = (DoubleBlock) Block.readTypedBlock(in);
            valueCounts = (LongBlock) Block.readTypedBlock(in);
            zeroThresholds = (DoubleBlock) Block.readTypedBlock(in);
            encodedHistograms = (BytesRefBlock) Block.readTypedBlock(in);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(minima, maxima, sums, valueCounts, zeroThresholds, encodedHistograms);
            }
        }
        return new ExponentialHistogramArrayBlock(minima, maxima, sums, valueCounts, zeroThresholds, encodedHistograms);
    }

    @Override
    public long ramBytesUsed() {
        long bytes = 0;
        for (Block b : getSubBlocks()) {
            bytes += b.ramBytesUsed();
        }
        return bytes;
    }

    void copyInto(
        DoubleBlock.Builder minimaBuilder,
        DoubleBlock.Builder maximaBuilder,
        DoubleBlock.Builder sumsBuilder,
        LongBlock.Builder valueCountsBuilder,
        DoubleBlock.Builder zeroThresholdsBuilder,
        BytesRefBlock.Builder encodedHistogramsBuilder,
        int beginInclusive,
        int endExclusive
    ) {
        minimaBuilder.copyFrom(minima, beginInclusive, endExclusive);
        maximaBuilder.copyFrom(maxima, beginInclusive, endExclusive);
        sumsBuilder.copyFrom(sums, beginInclusive, endExclusive);
        valueCountsBuilder.copyFrom(valueCounts, beginInclusive, endExclusive);
        zeroThresholdsBuilder.copyFrom(zeroThresholds, beginInclusive, endExclusive);
        encodedHistogramsBuilder.copyFrom(encodedHistograms, beginInclusive, endExclusive);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ExponentialHistogramBlock block) {
            return ExponentialHistogramBlock.equals(this, block);
        }
        return false;
    }

    boolean equalsAfterTypeCheck(ExponentialHistogramArrayBlock that) {
        return minima.equals(that.minima)
            && maxima.equals(that.maxima)
            && sums.equals(that.sums)
            && valueCounts.equals(that.valueCounts)
            && zeroThresholds.equals(that.zeroThresholds)
            && encodedHistograms.equals(that.encodedHistograms);
    }

    @Override
    public int hashCode() {
        // for now we use just the hash of encodedHistograms
        // this ensures proper equality with null blocks and should be unique enough for practical purposes
        return encodedHistograms.hashCode();
    }
}
