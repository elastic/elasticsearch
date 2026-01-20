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

    @Override
    protected List<Block> getSubBlocks() {
        return List.of(encodedDigests, minima, maxima, sums, valueCounts);
    }

    @Override
    protected TDigestArrayBlock buildFromSubBlocks(List<Block> subBlocks) {
        assert subBlocks.size() == 5;
        return new TDigestArrayBlock(
            (BytesRefBlock) subBlocks.get(0),
            (DoubleBlock) subBlocks.get(1),
            (DoubleBlock) subBlocks.get(2),
            (DoubleBlock) subBlocks.get(3),
            (LongBlock) subBlocks.get(4)
        );
    }

    @Override
    public Vector asVector() {
        return null;
    }

    @Override
    public int getFirstValueIndex(int position) {
        return position;
    }

    @Override
    public int getTotalValueCount() {
        return encodedDigests.getTotalValueCount();
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
    public DoubleBlock buildHistogramComponentBlock(Component component) {
        // as soon as we support multi-values, we need to implement this differently,
        // as the sub-blocks will be flattened and the position count won't match anymore
        // we'll likely have to return a "view" on the sub-blocks that implements the multi-value logic
        assert doesHaveMultivaluedFields() == false;
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
                        if (isNull(i)) {
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
            encodedDigests.getBytesRef(encodedDigests.getFirstValueIndex(offset), new BytesRef()),
            minima.isNull(offset) ? Double.NaN : minima.getDouble(minima.getFirstValueIndex(offset)),
            maxima.isNull(offset) ? Double.NaN : maxima.getDouble(maxima.getFirstValueIndex(offset)),
            sums.isNull(offset) ? Double.NaN : sums.getDouble(sums.getFirstValueIndex(offset)),
            valueCounts.getLong(valueCounts.getFirstValueIndex(offset))
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
