/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.BucketIterator;
import org.elasticsearch.exponentialhistogram.CompressedExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ZeroBucket;

import java.io.IOException;
import java.util.List;

public final class ExponentialHistogramArrayBlock extends AbstractDelegatingCompoundBlock<ExponentialHistogramBlock>
    implements
        ExponentialHistogramBlock {

    // Exponential histograms consist of several components that we store in separate blocks
    // due to (a) better compression in the field mapper for disk storage and (b) faster computations if only one sub-component is needed
    // What are the semantics of positions, multi-value counts and nulls in the exponential histogram block and
    // how do they relate to the sub-blocks?
    // ExponentialHistogramBlock need to adhere to the contract of Blocks for the access patterns:
    //
    // for (int position = 0; position < block.getPositionCount(); position++) {
    // ...int valueCount = block.getValueCount(position);
    // ...int firstValueIndex = block.getFirstValueIndex(position);
    // ...for (int i = 0; i < valueCount; i++) {
    // ......ExponentialHistogram histo = block.getExponentialHistogram(firstValueIndex + i, scratch);
    // ...}
    // }
    //
    // That implies that given only a value-index, we need to be able to retrieve all components of the histogram.
    // Because we can't make any assumptions on how value indices are laid out in the sub-blocks for multi-values,
    // we enforce that the sub-blocks have at most one value per position (i.e., no multi-values).
    // Based on this, we can define the valueIndex for ExponentialHistogramArrayBlock to correspond to positions in the sub-blocks.
    // So basically the sub-blocks are the "flattened" components of the histograms.
    // Multi-value support is implemented via a firstValueIndexes array that maps positions to ranges of sub-block positions.

    private final DoubleBlock minima;
    private final DoubleBlock maxima;
    private final DoubleBlock sums;
    /**
     Holds the number of values in each histogram. Note that this is a different concept from getValueCount(position)!
     At the time of writing, the count will always be an integer.
     However, as we are planning on eventually supporting extrapolation and rate, the counts will then become fractional.
     To avoid annoyances with breaking changes later, we store counts as doubles right away.
     */
    private final DoubleBlock valueCounts;
    private final DoubleBlock zeroThresholds;
    private final BytesRefBlock encodedHistograms;

    ExponentialHistogramArrayBlock(
        BytesRefBlock encodedHistograms,
        DoubleBlock minima,
        DoubleBlock maxima,
        DoubleBlock sums,
        DoubleBlock valueCounts,
        DoubleBlock zeroThresholds,
        int positionCount,
        @Nullable int[] firstValueIndexes
    ) {
        super(positionCount, firstValueIndexes);
        this.minima = minima;
        this.maxima = maxima;
        this.sums = sums;
        this.valueCounts = valueCounts;
        this.zeroThresholds = zeroThresholds;
        this.encodedHistograms = encodedHistograms;
        assert assertInvariants();
    }

    @Override
    protected boolean assertInvariants() {
        super.assertInvariants();
        int expectedSubBlockPositions = subBlockPositionCount();
        for (Block b : getSubBlocks()) {
            assert b.isReleased() == false;
            assert b.doesHaveMultivaluedFields() == false
                : "ExponentialHistogramArrayBlock sub-blocks can't have multi-values but [" + b + "] does";
            assert b.getPositionCount() == expectedSubBlockPositions
                : "ExponentialHistogramArrayBlock sub-blocks must have the same position count but ["
                    + b
                    + "] has "
                    + b.getPositionCount()
                    + " instead of "
                    + expectedSubBlockPositions;
        }
        for (int subPos = 0; subPos < expectedSubBlockPositions; subPos++) {
            if (encodedHistograms.isNull(subPos)) {
                for (Block b : getSubBlocks()) {
                    assert b.isNull(subPos)
                        : "ExponentialHistogramArrayBlock sub-block [" + b + "] should be null at position " + subPos + ", but was not";
                }
            } else {
                double valueCount = valueCounts.getDouble(valueCounts.getFirstValueIndex(subPos));
                for (Block b : List.of(sums, minima, maxima)) {
                    // sums / minima / maxima should be null exactly when value count is 0 or the histogram is null
                    assert b.isNull(subPos) == (valueCount == 0)
                        : "ExponentialHistogramArrayBlock sums/minima/maxima sub-block [" + b + "] has wrong nullity at position " + subPos;
                }
                for (Block b : List.of(valueCounts, zeroThresholds, encodedHistograms)) {
                    assert b.isNull(subPos) == false
                        : "ExponentialHistogramArrayBlock sub-block [" + b + "] should be non-null at position " + subPos + ", but was not";
                }
            }
        }
        return true;
    }

    @Override
    protected List<Block> getSubBlocks() {
        // encodedHistograms must be first as it's used for null detection (see AbstractDelegatingCompoundBlock)
        return List.of(encodedHistograms, minima, maxima, sums, valueCounts, zeroThresholds);
    }

    @Override
    protected ExponentialHistogramArrayBlock buildFromSubBlocks(
        List<Block> subBlocks,
        int newPositionCount,
        @Nullable int[] newFirstValueIndexes
    ) {
        return new ExponentialHistogramArrayBlock(
            (BytesRefBlock) subBlocks.get(0),
            (DoubleBlock) subBlocks.get(1),
            (DoubleBlock) subBlocks.get(2),
            (DoubleBlock) subBlocks.get(3),
            (DoubleBlock) subBlocks.get(4),
            (DoubleBlock) subBlocks.get(5),
            newPositionCount,
            newFirstValueIndexes
        );
    }

    public static EncodedHistogramData encode(ExponentialHistogram histogram) {
        assert histogram != null;
        // TODO: check and potentially improve performance and correctness before moving out of tech-preview
        // The current implementation encodes the histogram into the format we use for storage on disk
        // This format is optimized for minimal memory usage at the cost of encoding speed
        // In addition, it only support storing the zero threshold as a double value, which is lossy when merging histograms
        // In practice this currently occurs, as the zero threshold is usually 0.0 and not impacted by merges
        // And even if it occurs, the error is usually tiny
        // We should add a dedicated encoding when building a block from computed histograms which do not originate from doc values
        // That encoding should be optimized for speed and support storing the zero threshold as (scale, index) pair
        ZeroBucket zeroBucket = histogram.zeroBucket();
        BytesStreamOutput encodedBytes = new BytesStreamOutput();
        try {
            CompressedExponentialHistogram.writeHistogramBytes(encodedBytes, histogram);
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode histogram", e);
        }
        double sum;
        if (histogram.valueCount() == 0) {
            assert histogram.sum() == 0.0 : "Empty histogram should have sum 0.0 but was " + histogram.sum();
            sum = Double.NaN; // we store null/NaN for empty histograms to ensure avg is null/0.0 instead of 0.0/0.0
        } else {
            sum = histogram.sum();
        }
        return new EncodedHistogramData(
            histogram.valueCount(),
            sum,
            histogram.min(),
            histogram.max(),
            zeroBucket.zeroThreshold(),
            encodedBytes.bytes().toBytesRef()
        );
    }

    public static EncodedHistogramData encode(
        int scale,
        BucketIterator negativeBuckets,
        BucketIterator positiveBuckets,
        double zeroThreshold,
        long zeroCount,
        long count,
        double sum,
        double min,
        double max
    ) {
        if (zeroCount < 0) {
            throw new IllegalArgumentException("zeroCount must be non-negative but was [" + zeroCount + "]");
        }
        BytesStreamOutput encodedBytes = new BytesStreamOutput();
        try {
            CompressedExponentialHistogram.writeHistogramBytes(encodedBytes, scale, negativeBuckets, positiveBuckets);
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode histogram", e);
        }
        return new EncodedHistogramData(count, sum, min, max, zeroThreshold, encodedBytes.bytes().toBytesRef());
    }

    @Override
    public ExponentialHistogram getExponentialHistogram(int valueIndex, ExponentialHistogramScratch scratch) {
        assert encodedHistograms.isNull(valueIndex) == false : "tried to get histogram at null sub-block position " + valueIndex;
        // valueIndex directly corresponds to a position in the sub-blocks
        BytesRef bytes = encodedHistograms.getBytesRef(encodedHistograms.getFirstValueIndex(valueIndex), scratch.bytesRefScratch);
        double zeroThreshold = zeroThresholds.getDouble(zeroThresholds.getFirstValueIndex(valueIndex));
        double valueCount = valueCounts.getDouble(valueCounts.getFirstValueIndex(valueIndex));
        double sum = valueCount == 0 ? 0.0 : sums.getDouble(sums.getFirstValueIndex(valueIndex));
        double min = valueCount == 0 ? Double.NaN : minima.getDouble(minima.getFirstValueIndex(valueIndex));
        double max = valueCount == 0 ? Double.NaN : maxima.getDouble(maxima.getFirstValueIndex(valueIndex));
        try {
            // Compressed histograms always have an integral value count, so we can safely round here
            long roundedValueCount = Math.round(valueCount);
            scratch.reusedHistogram.reset(zeroThreshold, roundedValueCount, sum, min, max, bytes);
            return scratch.reusedHistogram;
        } catch (IOException e) {
            throw new IllegalStateException("error loading histogram", e);
        }
    }

    public static ExponentialHistogramBlock createConstant(ExponentialHistogram histogram, int positionCount, BlockFactory blockFactory) {
        EncodedHistogramData data = encode(histogram);
        DoubleBlock minBlock = null;
        DoubleBlock maxBlock = null;
        DoubleBlock sumBlock = null;
        DoubleBlock countBlock = null;
        DoubleBlock zeroThresholdBlock = null;
        BytesRefBlock encodedHistogramBlock = null;
        boolean success = false;
        try {
            countBlock = blockFactory.newConstantDoubleBlockWith(data.count, positionCount);
            if (Double.isNaN(data.min)) {
                minBlock = (DoubleBlock) blockFactory.newConstantNullBlock(positionCount);
            } else {
                minBlock = blockFactory.newConstantDoubleBlockWith(data.min, positionCount);
            }
            if (Double.isNaN(data.max)) {
                maxBlock = (DoubleBlock) blockFactory.newConstantNullBlock(positionCount);
            } else {
                maxBlock = blockFactory.newConstantDoubleBlockWith(data.max, positionCount);
            }
            if (Double.isNaN(data.sum)) {
                sumBlock = (DoubleBlock) blockFactory.newConstantNullBlock(positionCount);
            } else {
                sumBlock = blockFactory.newConstantDoubleBlockWith(data.sum, positionCount);
            }
            zeroThresholdBlock = blockFactory.newConstantDoubleBlockWith(data.zeroThreshold, positionCount);
            encodedHistogramBlock = blockFactory.newConstantBytesRefBlockWith(data.encodedHistogram, positionCount);
            success = true;
            return new ExponentialHistogramArrayBlock(
                encodedHistogramBlock,
                minBlock,
                maxBlock,
                sumBlock,
                countBlock,
                zeroThresholdBlock,
                encodedHistogramBlock.getPositionCount(),
                null
            );
        } finally {
            if (success == false) {
                Releasables.close(minBlock, maxBlock, sumBlock, countBlock, zeroThresholdBlock, encodedHistogramBlock);
            }
        }
    }

    @Override
    public DoubleBlock buildHistogramComponentBlock(Component component) {
        if (mayHaveMultivaluedFields()) {
            throw new UnsupportedOperationException("Not yet implemented for multi-valued blocks");
        }
        DoubleBlock subBlock = switch (component) {
            case MIN -> minima;
            case MAX -> maxima;
            case SUM -> sums;
            case COUNT -> valueCounts;
        };
        subBlock.incRef();
        return subBlock;
    }

    @Override
    public void serializeExponentialHistogram(int valueIndex, SerializedOutput out, BytesRef scratch) {
        // valueIndex corresponds directly to a sub-block position
        // note that this value count is different from getValueCount(position)!
        // this value count represents the number of individual samples the histogram was computed for
        double valueCount = valueCounts.getDouble(valueCounts.getFirstValueIndex(valueIndex));
        out.appendDouble(valueCount);
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
    public int valueMaxByteSize() {
        // Five sub-blocks of doubles (dense arrays, 8 bytes per slot regardless of null)
        // plus the variable-length encoded histogram bytes.
        return 5 * Double.BYTES + encodedHistograms.valueMaxByteSize();
    }

    @Override
    public Vector asVector() {
        return null;
    }

    @Override
    public ElementType elementType() {
        return ElementType.EXPONENTIAL_HISTOGRAM;
    }

    @Override
    public ReleasableIterator<? extends Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("can't lookup values from ExponentialHistogramArrayBlock");
    }

    @Override
    public Block expand() {
        if (mayHaveMultivaluedFields()) {
            throw new UnsupportedOperationException("Not yet implemented for multi-valued blocks");
        }
        incRef();
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(MULTIVALUE_SUPPORT)) {
            writeMultiValueMetadata(out);
        } else if (doesHaveMultivaluedFields()) {
            throw new IllegalStateException("Cannot serialize multi-valued exponential histogram block on old transport version");
        }
        Block.writeTypedBlock(minima, out);
        Block.writeTypedBlock(maxima, out);
        Block.writeTypedBlock(sums, out);
        Block.writeTypedBlock(valueCounts, out);
        Block.writeTypedBlock(zeroThresholds, out);
        Block.writeTypedBlock(encodedHistograms, out);
    }

    public static ExponentialHistogramArrayBlock readFrom(BlockStreamInput input) throws IOException {
        if (input.getTransportVersion().supports(MULTIVALUE_SUPPORT)) {
            return AbstractDelegatingCompoundBlock.readFrom(input, ExponentialHistogramArrayBlock::readSubBlocksFrom);
        } else {
            return readSubBlocksFrom(input, -1, null);
        }
    }

    private static ExponentialHistogramArrayBlock readSubBlocksFrom(BlockStreamInput in, int positionCount, int[] firstValueIndexes)
        throws IOException {
        DoubleBlock minima = null;
        DoubleBlock maxima = null;
        DoubleBlock sums = null;
        DoubleBlock valueCounts = null;
        DoubleBlock zeroThresholds = null;
        BytesRefBlock encodedHistograms = null;

        boolean success = false;
        try {
            minima = (DoubleBlock) Block.readTypedBlock(in);
            maxima = (DoubleBlock) Block.readTypedBlock(in);
            sums = (DoubleBlock) Block.readTypedBlock(in);
            valueCounts = (DoubleBlock) Block.readTypedBlock(in);
            zeroThresholds = (DoubleBlock) Block.readTypedBlock(in);
            encodedHistograms = (BytesRefBlock) Block.readTypedBlock(in);
            success = true;
            return new ExponentialHistogramArrayBlock(
                encodedHistograms,
                minima,
                maxima,
                sums,
                valueCounts,
                zeroThresholds,
                firstValueIndexes == null ? encodedHistograms.getPositionCount() : positionCount,
                firstValueIndexes
            );
        } finally {
            if (success == false) {
                Releasables.close(minima, maxima, sums, valueCounts, zeroThresholds, encodedHistograms);
            }
        }
    }

    /**
     * Copy sub-block positions from beginInclusive to endExclusive into the provided builders.
     * Note: beginInclusive and endExclusive refer to sub-block positions, not this block's positions.
     */
    void copySubBlockPositionsInto(
        DoubleBlock.Builder minimaBuilder,
        DoubleBlock.Builder maximaBuilder,
        DoubleBlock.Builder sumsBuilder,
        DoubleBlock.Builder valueCountsBuilder,
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
        return layoutEquals(that)
            && minima.equals(that.minima)
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

    public record EncodedHistogramData(double count, double sum, double min, double max, double zeroThreshold, BytesRef encodedHistogram) {}
}
