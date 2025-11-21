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
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

import java.io.IOException;
import java.util.List;

final class ExponentialHistogramArrayBlock extends AbstractNonThreadSafeRefCounted implements ExponentialHistogramBlock {

    // Exponential histograms consist of several components that we store in separate blocks
    // due to (a) better compression in the field mapper for disk storage and (b) faster computations if only one sub-component is needed
    // What are the semantics of positions, multi-value counts and nulls in the exponential histogram block and
    // how do they relate to the sub-blocks?
    // ExponentialHistogramBlock need to adhere to the contract of Blocks for the access patterns:
    //
    // for (int position = 0; position < block.getPositionCount(); position++) {
    // ...int valueCount = block.getValueCount(position);
    // ...for (int valueIndex = 0; valueIndex < valueCount; valueIndex++) {
    // ......ExponentialHistogram histo = block.getExponentialHistogram(valueIndex, scratch);
    // ...}
    // }
    //
    // That implies that given only a value-index, we need to be able to retrieve all components of the histogram.
    // Because we can't make any assumptions on how value indices are laid out in the sub-blocks for multi-values,
    // we enforce that the sub-blocks have at most one value per position (i.e., no multi-values).
    // Based on this, we can define the valueIndex for ExponentialHistogramArrayBlock to correspond to positions in the sub-blocks.
    // So basically the sub-blocks are the "flattened" components of the histograms.
    // If we later add multi-value support to ExponentialHistogramArrayBlock,
    // we can't use the multi-value support of the sub-blocks to implement that.
    // Instead, we need to maintain a firstValueIndex array ourselves in ExponentialHistogramArrayBlock.

    private final DoubleBlock minima;
    private final DoubleBlock maxima;
    private final DoubleBlock sums;
    /**
     Holds the number of values in each histogram. Note that this is a different concept from getValueCount(position)!
     */
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
                    if (b == sums || b == minima || b == maxima) {
                        // sums / minima / maxima should be null exactly when value count is 0 or the histogram is null
                        assert b.isNull(i) == (valueCounts.getLong(valueCounts.getFirstValueIndex(i)) == 0)
                            : "ExponentialHistogramArrayBlock sums/minima/maxima sub-block [" + b + "] has wrong nullity at position " + i;
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

    @Override
    public ExponentialHistogram getExponentialHistogram(int valueIndex, ExponentialHistogramScratch scratch) {
        BytesRef bytes = encodedHistograms.getBytesRef(encodedHistograms.getFirstValueIndex(valueIndex), scratch.bytesRefScratch);
        double zeroThreshold = zeroThresholds.getDouble(zeroThresholds.getFirstValueIndex(valueIndex));
        long valueCount = valueCounts.getLong(valueCounts.getFirstValueIndex(valueIndex));
        double sum = valueCount == 0 ? 0.0 : sums.getDouble(sums.getFirstValueIndex(valueIndex));
        double min = valueCount == 0 ? Double.NaN : minima.getDouble(minima.getFirstValueIndex(valueIndex));
        double max = valueCount == 0 ? Double.NaN : maxima.getDouble(maxima.getFirstValueIndex(valueIndex));
        try {
            scratch.reusedHistogram.reset(zeroThreshold, valueCount, sum, min, max, bytes);
            return scratch.reusedHistogram;
        } catch (IOException e) {
            throw new IllegalStateException("error loading histogram", e);
        }
    }

    @Override
    public Block buildExponentialHistogramComponentBlock(Component component) {
        // as soon as we support multi-values, we need to implement this differently,
        // as the sub-blocks will be flattened and the position count won't match anymore
        // we'll likely have to return a "view" on the sub-blocks that implements the multi-value logic
        Block result = switch (component) {
            case MIN -> minima;
            case MAX -> maxima;
            case SUM -> sums;
            case COUNT -> valueCounts;
        };
        result.incRef();
        return result;
    }

    @Override
    public void serializeExponentialHistogram(int valueIndex, SerializedOutput out, BytesRef scratch) {
        // not that this value count is different from getValueCount(position)!
        // this value count represents the number of individual samples the histogram was computed for
        long valueCount = valueCounts.getLong(valueCounts.getFirstValueIndex(valueIndex));
        out.appendLong(valueCounts.getLong(valueCounts.getFirstValueIndex(valueIndex)));
        out.appendDouble(zeroThresholds.getDouble(zeroThresholds.getFirstValueIndex(valueIndex)));
        if (valueCount > 0) {
            // sum / min / max are only non-null for non-empty histograms
            out.appendDouble(sums.getDouble(sums.getFirstValueIndex(valueIndex)));
            out.appendDouble(minima.getDouble(minima.getFirstValueIndex(valueIndex)));
            out.appendDouble(maxima.getDouble(maxima.getFirstValueIndex(valueIndex)));
        }
        out.appendBytesRef(encodedHistograms.getBytesRef(encodedHistograms.getFirstValueIndex(valueIndex), scratch));
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
