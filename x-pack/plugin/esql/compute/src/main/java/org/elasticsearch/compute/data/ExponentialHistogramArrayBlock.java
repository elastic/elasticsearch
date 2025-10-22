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
        for (Block b : getSubBlocks()) {
            if (b.getPositionCount() != encodedHistograms.getPositionCount()) {
                throw new IllegalArgumentException("expected positionCount=" + encodedHistograms.getPositionCount() + " but was " + b);
            }
            if (b.isReleased()) {
                throw new IllegalArgumentException(
                    "can't build exponential_histogram block out of released blocks but [" + b + "] was released"
                );
            }
        }
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
                BytesRef bytes = encodedHistograms.getBytesRef(valueIndex, bytesRef);
                double zeroThreshold = zeroThresholds.getDouble(valueIndex);
                long valueCount = valueCounts.getLong(valueIndex);
                double sum = sums.getDouble(valueIndex);
                double min = valueCount == 0 ? Double.NaN : minima.getDouble(valueIndex);
                double max = valueCount == 0 ? Double.NaN : maxima.getDouble(valueIndex);
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
        return encodedHistograms.getFirstValueIndex(position);
    }

    @Override
    public int getValueCount(int position) {
        return encodedHistograms.getValueCount(position);
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
        encodedHistograms.allowPassingToDifferentDriver();
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
        return encodedHistograms.mayHaveMultivaluedFields();
    }

    @Override
    public boolean doesHaveMultivaluedFields() {
        return encodedHistograms.doesHaveMultivaluedFields();
    }

    @Override
    public Block filter(int... positions) {
        return new ExponentialHistogramArrayBlock(
            minima.filter(positions),
            maxima.filter(positions),
            sums.filter(positions),
            valueCounts.filter(positions),
            zeroThresholds.filter(positions),
            encodedHistograms.filter(positions)
        );
    }

    @Override
    public Block keepMask(BooleanVector mask) {
        return new ExponentialHistogramArrayBlock(
            minima.keepMask(mask),
            maxima.keepMask(mask),
            sums.keepMask(mask),
            valueCounts.keepMask(mask),
            zeroThresholds.keepMask(mask),
            encodedHistograms.keepMask(mask)
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
        BytesRefBlock expandedHistograms = encodedHistograms.expand();
        if (expandedHistograms == encodedHistograms) {
            // No values to expand, return original block
            Releasables.close(expandedHistograms);
            this.incRef();
            return this;
        } else {
            DoubleBlock expandedMinima = minima.expand();
            DoubleBlock expandedMaxima = maxima.expand();
            DoubleBlock expandedSums = sums.expand();
            LongBlock expandedValueCounts = valueCounts.expand();
            DoubleBlock expandedZeroThresholds = zeroThresholds.expand();
            return new ExponentialHistogramArrayBlock(
                expandedMinima,
                expandedMaxima,
                expandedSums,
                expandedValueCounts,
                expandedZeroThresholds,
                expandedHistograms
            );
        }
    }

    @Override
    public ExponentialHistogramArrayBlock deepCopy(BlockFactory blockFactory) {
        return new ExponentialHistogramArrayBlock(
            minima.deepCopy(blockFactory),
            maxima.deepCopy(blockFactory),
            sums.deepCopy(blockFactory),
            valueCounts.deepCopy(blockFactory),
            zeroThresholds.deepCopy(blockFactory),
            encodedHistograms.deepCopy(blockFactory)
        );
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
