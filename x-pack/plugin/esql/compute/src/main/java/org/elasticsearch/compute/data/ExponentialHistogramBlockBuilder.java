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
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.index.mapper.BlockLoader;

public class ExponentialHistogramBlockBuilder implements BlockLoader.ExponentialHistogramBuilder, Block.Builder {

    private final BytesRefBlock.Builder encodedHistogramsBuilder;
    private final LongBlock.Builder valuesCountsBuilder;

    private final BytesRef tempScratch;

    private boolean closed = false;

    ExponentialHistogramBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        BytesRefBlock.Builder histos = null;
        LongBlock.Builder valueCounts = null;
        boolean success = false;
        try {
            histos = blockFactory.newBytesRefBlockBuilder(estimatedSize);
            valueCounts = blockFactory.newLongBlockBuilder(estimatedSize);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(histos, valueCounts);
            }
        }
        this.encodedHistogramsBuilder = histos;
        this.valuesCountsBuilder = valueCounts;
        this.tempScratch = new BytesRef(new byte[256], 0, 256);
    }

    @Override
    public ExponentialHistogramBlockBuilder append(@Nullable ExponentialHistogram value) {
        if (value == null) {
            appendNull();
        } else {
            encodedHistogramsBuilder.appendBytesRef(ExponentialHistogramArrayBlock.encode(value, tempScratch));
            long totalValueCount = value.zeroBucket().count() + value.negativeBuckets().valueCount() + value.positiveBuckets().valueCount();
            valuesCountsBuilder.appendLong(totalValueCount);
        }
        return this;
    }

    @Override
    public ExponentialHistogramBlock build() {
        boolean success = false;
        BytesRefBlock encodedHistos;
        LongBlock valueCounts;
        try {
            encodedHistos = encodedHistogramsBuilder.build();
            valueCounts = valuesCountsBuilder.build();
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(encodedHistogramsBuilder, valuesCountsBuilder);
            }
        }
        return new ExponentialHistogramArrayBlock(encodedHistos, valueCounts);
    }

    @Override
    public ExponentialHistogramBlockBuilder appendNull() {
        encodedHistogramsBuilder.appendNull();
        valuesCountsBuilder.appendNull();
        return this;
    }

    @Override
    public ExponentialHistogramBlockBuilder beginPositionEntry() {
        encodedHistogramsBuilder.beginPositionEntry();
        valuesCountsBuilder.beginPositionEntry();
        return this;
    }

    @Override
    public ExponentialHistogramBlockBuilder endPositionEntry() {
        encodedHistogramsBuilder.endPositionEntry();
        valuesCountsBuilder.endPositionEntry();
        return this;
    }

    @Override
    public Block.Builder copyFrom(Block block, int beginInclusive, int endExclusive) {
        if (block.areAllValuesNull()) {
            for (int i= beginInclusive; i < endExclusive; i++) {
                appendNull();
            }
        } else {
            ExponentialHistogramArrayBlock histoBlock = (ExponentialHistogramArrayBlock) block;
            histoBlock.copyInto(encodedHistogramsBuilder, valuesCountsBuilder, beginInclusive, endExclusive);
        }
        return this;
    }

    @Override
    public Block.Builder mvOrdering(Block.MvOrdering mvOrdering) {
        //TODO: does this need implementation?
        return this;
    }

    @Override
    public long estimatedBytes() {
        return encodedHistogramsBuilder.estimatedBytes() + valuesCountsBuilder.estimatedBytes();
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            Releasables.close(encodedHistogramsBuilder, valuesCountsBuilder);
        }
    }
}
