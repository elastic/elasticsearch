/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;

public final class TDigestBlockBuilder extends AbstractDelegatingCompoundBlock.AbstractCompositeBlockBuilder<TDigestBlock>
    implements
        TDigestBlock.Builder {

    private final BytesRefBlock.Builder encodedDigestsBuilder;
    private final DoubleBlock.Builder minimaBuilder;
    private final DoubleBlock.Builder maximaBuilder;
    private final DoubleBlock.Builder sumsBuilder;
    private final LongBlock.Builder valueCountsBuilder;

    private final BytesRef scratch = new BytesRef();

    public TDigestBlockBuilder(int size, BlockFactory blockFactory) {
        super(blockFactory);
        BytesRefBlock.Builder encodedDigestsBuilder = null;
        DoubleBlock.Builder minimaBuilder = null;
        DoubleBlock.Builder maximaBuilder = null;
        DoubleBlock.Builder sumsBuilder = null;
        LongBlock.Builder valueCountsBuilder = null;
        boolean success = false;
        try {
            encodedDigestsBuilder = blockFactory.newBytesRefBlockBuilder(size);
            minimaBuilder = blockFactory.newDoubleBlockBuilder(size);
            maximaBuilder = blockFactory.newDoubleBlockBuilder(size);
            sumsBuilder = blockFactory.newDoubleBlockBuilder(size);
            valueCountsBuilder = blockFactory.newLongBlockBuilder(size);
            this.encodedDigestsBuilder = encodedDigestsBuilder;
            this.minimaBuilder = minimaBuilder;
            this.maximaBuilder = maximaBuilder;
            this.sumsBuilder = sumsBuilder;
            this.valueCountsBuilder = valueCountsBuilder;
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(encodedDigestsBuilder, minimaBuilder, maximaBuilder, sumsBuilder, valueCountsBuilder);
            }
        }
    }

    @Override
    protected void copySubBlockPositions(AbstractDelegatingCompoundBlock<?> block, int startSubBlockPos, int endSubBlockPos) {
        ((TDigestArrayBlock) block).copySubBlockPositionsInto(
            encodedDigestsBuilder,
            minimaBuilder,
            maximaBuilder,
            sumsBuilder,
            valueCountsBuilder,
            startSubBlockPos,
            endSubBlockPos
        );
    }

    @Override
    public TDigestBlock.Builder copyFrom(TDigestBlock block, int position) {
        copyFrom(block, position, position + 1);
        return this;
    }

    @Override
    public TDigestBlockBuilder appendNull() {
        assert isPositionEntryOpen() == false : "Can't append null to multi-valued entries";
        encodedDigestsBuilder.appendNull();
        minimaBuilder.appendNull();
        maximaBuilder.appendNull();
        sumsBuilder.appendNull();
        valueCountsBuilder.appendNull();
        valueAppended();
        return this;
    }

    @Override
    public TDigestBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        assert mvOrdering == Block.MvOrdering.UNORDERED : "TDigests don't have a natural order, so it doesn't make sense to call this";
        return this;
    }

    @Override
    public long estimatedBytes() {
        return super.estimatedBytes() + encodedDigestsBuilder.estimatedBytes() + minimaBuilder.estimatedBytes() + maximaBuilder
            .estimatedBytes() + sumsBuilder.estimatedBytes() + valueCountsBuilder.estimatedBytes();
    }

    @Override
    public TDigestBlock doBuild(int positionCount, @Nullable int[] firstValueIndexes) {
        DoubleBlock minima = null;
        DoubleBlock maxima = null;
        DoubleBlock sums = null;
        LongBlock valueCounts = null;
        BytesRefBlock encodedDigests = null;
        boolean success = false;
        try {
            minima = minimaBuilder.build();
            maxima = maximaBuilder.build();
            sums = sumsBuilder.build();
            valueCounts = valueCountsBuilder.build();
            encodedDigests = encodedDigestsBuilder.build();
            TDigestArrayBlock block = new TDigestArrayBlock(
                encodedDigests,
                minima,
                maxima,
                sums,
                valueCounts,
                positionCount,
                firstValueIndexes
            );
            success = true;
            return block;
        } finally {
            if (success == false) {
                Releasables.close(minima, maxima, sums, valueCounts, encodedDigests);
            }
        }
    }

    @Override
    public BlockLoader.DoubleBuilder minima() {
        return minimaBuilder;
    }

    @Override
    public BlockLoader.DoubleBuilder maxima() {
        return maximaBuilder;
    }

    @Override
    public BlockLoader.DoubleBuilder sums() {
        return sumsBuilder;
    }

    @Override
    public BlockLoader.LongBuilder valueCounts() {
        return valueCountsBuilder;
    }

    @Override
    public BlockLoader.BytesRefBuilder encodedDigests() {
        return encodedDigestsBuilder;
    }

    @Override
    protected void extraClose() {
        Releasables.close(encodedDigestsBuilder, minimaBuilder, maximaBuilder, sumsBuilder, valueCountsBuilder);
    }

    @Override
    public TDigestBlockBuilder appendTDigest(TDigestHolder val) {
        encodedDigestsBuilder.appendBytesRef(val.getEncodedDigest());
        if (Double.isNaN(val.getMin())) {
            minimaBuilder.appendNull();
        } else {
            minimaBuilder.appendDouble(val.getMin());
        }
        if (Double.isNaN(val.getMax())) {
            maximaBuilder.appendNull();
        } else {
            maximaBuilder.appendDouble(val.getMax());
        }
        if (Double.isNaN(val.getSum())) {
            sumsBuilder.appendNull();
        } else {
            sumsBuilder.appendDouble(val.getSum());
        }
        valueCountsBuilder.appendLong(val.size());
        valueAppended();
        return this;
    }

    public void deserializeAndAppend(TDigestBlock.SerializedTDigestInput input) {
        long valueCount = input.readLong();
        valueCountsBuilder.appendLong(valueCount);
        if (valueCount > 0) {
            sumsBuilder.appendDouble(input.readDouble());
            minimaBuilder.appendDouble(input.readDouble());
            maximaBuilder.appendDouble(input.readDouble());
        } else {
            sumsBuilder.appendNull();
            minimaBuilder.appendNull();
            maximaBuilder.appendNull();
        }
        encodedDigestsBuilder.appendBytesRef(input.readBytesRef(scratch));
        valueAppended();
    }
}
