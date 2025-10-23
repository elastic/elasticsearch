/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.AbstractExponentialHistogram;
import org.elasticsearch.exponentialhistogram.CompressedExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ZeroBucket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Objects;

public final class ExponentialHistogramArrayBlock extends AbstractNonThreadSafeRefCounted implements ExponentialHistogramBlock {

    private final DoubleBlock minima;
    private final DoubleBlock maxima;
    private final DoubleBlock sums;
    private final LongBlock valueCounts;
    private final DoubleBlock zeroThresholds;
    private final BytesRefBlock encodedHistograms;

    public ExponentialHistogramArrayBlock(
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
            assert b.doesHaveMultivaluedFields() == false : "ExponentialHistogramArrayBlock sub-blocks can't have multi-values but [" + b + "] does";
            assert b.getPositionCount() == getPositionCount() : "ExponentialHistogramArrayBlock sub-blocks must have the same position count but [" + b + "] has "
                + b.getPositionCount()
                + " instead of "
                + getPositionCount();
            for (int i=0; i<b.getPositionCount(); i++) {
                assert b.isNull(i) == isNull(i) : "ExponentialHistogramArrayBlock sub-blocks can't have nulls but [" + b + "] is null at position " + i;
            }
        }
        return true;
    }

    private List<Block> getSubBlocks() {
        return List.of(minima, maxima, sums, valueCounts, zeroThresholds, encodedHistograms);
    }

    @Override
    public ExponentialHistogram getExponentialHistogram(int valueIndex) {
        return accessor().getExponentialHistogram(valueIndex);
    }

    @Override
    public Accessor accessor() {
        BytesRef bytesRef = new BytesRef();
        CompressedExponentialHistogram reusedHistogram = new CompressedExponentialHistogram();
        return new Accessor() {
            @Override
            public ExponentialHistogram getExponentialHistogram(int valueIndex) {

                // we don't support multivalues (yet) so valueIndex == position
                if (isNull(valueIndex)) {
                    return null;
                }

                BytesRef bytes = encodedHistograms.getBytesRef(encodedHistograms.getFirstValueIndex(valueIndex), bytesRef);
                double zeroThreshold = zeroThresholds.getDouble(zeroThresholds.getFirstValueIndex(valueIndex));
                long valueCount = valueCounts.getLong(valueCounts.getFirstValueIndex(valueIndex));
                double sum = sums.getDouble(sums.getFirstValueIndex(valueIndex));
                double min = valueCount == 0 ? Double.NaN : minima.getDouble(minima.getFirstValueIndex(valueIndex));
                double max = valueCount == 0 ? Double.NaN : maxima.getDouble(maxima.getFirstValueIndex(valueIndex));
                try {
                    reusedHistogram.reset(zeroThreshold, valueCount, sum, min, max, bytes);
                } catch (IOException e) {
                    throw new IllegalStateException("error loading histogram", e);
                }
                return reusedHistogram;
            }
        };
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
                Releasables.close(
                    copiedMinima,
                    copiedMaxima,
                    copiedSums,
                    copiedValueCounts,
                    copiedZeroThresholds,
                    copiedEncodedHistograms
                );
            }
        }
        return new ExponentialHistogramArrayBlock(copiedMinima, copiedMaxima , copiedSums, copiedValueCounts, copiedZeroThresholds, copiedEncodedHistograms);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        minima.writeTo(out);
        maxima.writeTo(out);
        sums.writeTo(out);
        valueCounts.writeTo(out);
        zeroThresholds.writeTo(out);
        encodedHistograms.writeTo(out);
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
            minima = DoubleBlock.readFrom(in);
            maxima = DoubleBlock.readFrom(in);
            sums = DoubleBlock.readFrom(in);
            valueCounts = LongBlock.readFrom(in);
            zeroThresholds = DoubleBlock.readFrom(in);
            encodedHistograms = BytesRefBlock.readFrom(in);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(minima, maxima, sums, valueCounts, zeroThresholds, encodedHistograms);
            }
        }
        return new ExponentialHistogramArrayBlock(
            minima,
            maxima,
            sums,
            valueCounts,
            zeroThresholds,
            encodedHistograms
        );
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
