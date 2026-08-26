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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.List;

public final class TDigestArrayBlock extends AbstractDelegatingCompoundBlock<TDigestBlock> implements TDigestBlock {
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
        LongBlock valueCounts,
        int positionCount,
        @Nullable int[] firstValueIndexes
    ) {
        super(positionCount, firstValueIndexes);
        this.encodedDigests = encodedDigests;
        this.minima = minima;
        this.maxima = maxima;
        this.sums = sums;
        this.valueCounts = valueCounts;
        assert assertInvariants();
    }

    @Override
    protected boolean assertInvariants() {
        super.assertInvariants();
        int expectedSubBlockPositions = subBlockPositionCount();
        for (Block b : getSubBlocks()) {
            assert b.isReleased() == false;
            assert b.doesHaveMultivaluedFields() == false : "TDigestArrayBlock sub-blocks can't have multi-values but [" + b + "] does";
            assert b.getPositionCount() == expectedSubBlockPositions
                : "TDigestArrayBlock sub-blocks must have the same position count but ["
                    + b
                    + "] has "
                    + b.getPositionCount()
                    + " instead of "
                    + expectedSubBlockPositions;
        }
        for (int subPos = 0; subPos < expectedSubBlockPositions; subPos++) {
            if (encodedDigests.isNull(subPos)) {
                for (Block b : getSubBlocks()) {
                    assert b.isNull(subPos)
                        : "TDigestArrayBlock sub-block [" + b + "] should be null at position " + subPos + ", but was not";
                }
            } else {
                long valueCount = valueCounts.getLong(valueCounts.getFirstValueIndex(subPos));
                for (Block b : List.of(sums, minima, maxima)) {
                    // sums / minima / maxima should be null exactly when value count is 0 or the tdigest is null
                    assert b.isNull(subPos) == (valueCount == 0)
                        : "TDigestArrayBlock sums/minima/maxima sub-block [" + b + "] has wrong nullity at position " + subPos;
                }
                for (Block b : List.of(valueCounts, encodedDigests)) {
                    assert b.isNull(subPos) == false
                        : "TDigestArrayBlock sub-block [" + b + "] should be non-null at position " + subPos + ", but was not";
                }
            }
        }
        return true;
    }

    @Override
    public int valueMaxByteSize() {
        // Three dense-double sub-blocks (min, max, sum) plus one dense-long sub-block (valueCount)
        // plus the variable-length encoded digest bytes.
        return 3 * Double.BYTES + Long.BYTES + encodedDigests.valueMaxByteSize();
    }

    @Override
    protected List<Block> getSubBlocks() {
        return List.of(encodedDigests, minima, maxima, sums, valueCounts);
    }

    @Override
    protected TDigestArrayBlock buildFromSubBlocks(List<Block> subBlocks, int newPositionCount, @Nullable int[] newFirstValueIndexes) {
        assert subBlocks.size() == 5;
        return new TDigestArrayBlock(
            (BytesRefBlock) subBlocks.get(0),
            (DoubleBlock) subBlocks.get(1),
            (DoubleBlock) subBlocks.get(2),
            (DoubleBlock) subBlocks.get(3),
            (LongBlock) subBlocks.get(4),
            newPositionCount,
            newFirstValueIndexes
        );
    }

    @Override
    public Vector asVector() {
        return null;
    }

    @Override
    public ElementType elementType() {
        return ElementType.TDIGEST;
    }

    @Override
    public ReleasableIterator<? extends TDigestBlock> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("Lookup is not supported on TDigestArrayBlock");
    }

    @Override
    public Block expand() {
        if (mayHaveMultivaluedFields()) {
            throw new UnsupportedOperationException("Not yet implemented for multi-valued blocks");
        }
        incRef();
        return this;
    }

    /**
     * Copy sub-block positions from beginInclusive to endExclusive into the provided builders.
     * Note: beginInclusive and endExclusive refer to sub-block positions, not this block's positions.
     */
    void copySubBlockPositionsInto(
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
    public DoubleBlock buildHistogramComponentBlock(Component component) {
        if (mayHaveMultivaluedFields()) {
            throw new UnsupportedOperationException("Not yet implemented for multi-valued blocks");
        }
        return switch (component) {
            case MIN -> {
                minima.incRef();
                yield minima;
            }
            case MAX -> {
                maxima.incRef();
                yield maxima;
            }
            case SUM -> {
                sums.incRef();
                yield sums;
            }
            case COUNT -> {
                try (var doubleBuilder = blockFactory().newDoubleBlockBuilder(valueCounts.getPositionCount())) {
                    for (int i = 0; i < valueCounts.getPositionCount(); i++) {
                        if (encodedDigests.isNull(i)) {
                            doubleBuilder.appendNull();
                        } else {
                            doubleBuilder.appendDouble(valueCounts.getLong(valueCounts.getFirstValueIndex(i)));
                        }
                    }
                    yield doubleBuilder.build();
                }
            }
        };
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(AbstractDelegatingCompoundBlock.MULTIVALUE_SUPPORT)) {
            writeMultiValueMetadata(out);
        } else if (doesHaveMultivaluedFields()) {
            throw new IllegalStateException("Cannot serialize multi-valued tdigest block on old transport version");
        }
        Block.writeTypedBlock(encodedDigests, out);
        Block.writeTypedBlock(minima, out);
        Block.writeTypedBlock(maxima, out);
        Block.writeTypedBlock(sums, out);
        Block.writeTypedBlock(valueCounts, out);
    }

    public static TDigestArrayBlock readFrom(BlockStreamInput input) throws IOException {
        if (input.getTransportVersion().supports(AbstractDelegatingCompoundBlock.MULTIVALUE_SUPPORT)) {
            return AbstractDelegatingCompoundBlock.readFrom(input, TDigestArrayBlock::readSubBlocksFrom);
        } else {
            return readSubBlocksFrom(input, -1, null);
        }
    }

    private static TDigestArrayBlock readSubBlocksFrom(BlockStreamInput in, int positionCount, int[] firstValueIndexes) throws IOException {
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
            return new TDigestArrayBlock(
                encodedDigests,
                minima,
                maxima,
                sums,
                valueCounts,
                firstValueIndexes == null ? encodedDigests.getPositionCount() : positionCount,
                firstValueIndexes
            );
        } finally {
            if (success == false) {
                Releasables.close(minima, maxima, sums, valueCounts, encodedDigests);
            }
        }
    }

    @Override
    public void serializeTDigest(int valueIndex, SerializedTDigestOutput out, BytesRef scratch) {
        // valueIndex corresponds directly to a sub-block position
        // note that this value count is different from getValueCount(position)!
        // this value count represents the number of individual samples the tdigest was computed for
        long valueCount = valueCounts.getLong(valueCounts.getFirstValueIndex(valueIndex));
        out.appendLong(valueCount);
        if (valueCount > 0) {
            // sum / min / max are only non-null for non-empty tdigests
            out.appendDouble(sums.getDouble(sums.getFirstValueIndex(valueIndex)));
            out.appendDouble(minima.getDouble(minima.getFirstValueIndex(valueIndex)));
            out.appendDouble(maxima.getDouble(maxima.getFirstValueIndex(valueIndex)));
        }
        out.appendBytesRef(encodedDigests.getBytesRef(encodedDigests.getFirstValueIndex(valueIndex), scratch));
    }

    @Override
    public TDigestHolder getTDigestHolder(int valueIndex, TDigestHolder scratch) {
        // valueIndex corresponds directly to a sub-block position
        var encoded = encodedDigests.getBytesRef(encodedDigests.getFirstValueIndex(valueIndex), scratch.scratchBytesRef());
        double min = minima.isNull(valueIndex) ? Double.NaN : minima.getDouble(minima.getFirstValueIndex(valueIndex));
        double max = maxima.isNull(valueIndex) ? Double.NaN : maxima.getDouble(maxima.getFirstValueIndex(valueIndex));
        double sum = sums.isNull(valueIndex) ? Double.NaN : sums.getDouble(sums.getFirstValueIndex(valueIndex));
        long valueCount = valueCounts.getLong(valueCounts.getFirstValueIndex(valueIndex));
        scratch.reset(encoded, min, max, sum, valueCount);
        return scratch;
    }

    public static TDigestBlock createConstant(TDigestHolder histogram, int positionCount, BlockFactory blockFactory) {
        DoubleBlock minBlock = null;
        DoubleBlock maxBlock = null;
        DoubleBlock sumBlock = null;
        LongBlock countBlock = null;
        BytesRefBlock encodedDigestsBlock = null;
        boolean success = false;
        try {
            countBlock = blockFactory.newConstantLongBlockWith(histogram.size(), positionCount);
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
            return new TDigestArrayBlock(
                encodedDigestsBlock,
                minBlock,
                maxBlock,
                sumBlock,
                countBlock,
                encodedDigestsBlock.getPositionCount(),
                null
            );
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
        return layoutEquals(that)
            && minima.equals(that.minima)
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
