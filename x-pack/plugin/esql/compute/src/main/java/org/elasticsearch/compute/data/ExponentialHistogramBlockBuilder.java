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

public class ExponentialHistogramBlockBuilder implements Block.Builder {

    private final BytesRefBlock.Builder encodedHistogramsBuilder;

    private final BytesRef tempScratch;

    private boolean closed = false;

    ExponentialHistogramBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        this.encodedHistogramsBuilder = blockFactory.newBytesRefBlockBuilder(estimatedSize);
        this.tempScratch = new BytesRef(new byte[256], 0, 256);
    }

    public ExponentialHistogramBlockBuilder append(@Nullable ExponentialHistogram value) {
        if (value == null) {
            appendNull();
        } else {
            encodedHistogramsBuilder.appendBytesRef(ExponentialHistogramArrayBlock.encode(value, tempScratch));
        }
        return this;
    }

    @Override
    public ExponentialHistogramBlock build() {
        return new ExponentialHistogramArrayBlock(encodedHistogramsBuilder.build());
    }

    @Override
    public ExponentialHistogramBlockBuilder appendNull() {
        encodedHistogramsBuilder.appendNull();
        return this;
    }

    @Override
    public ExponentialHistogramBlockBuilder beginPositionEntry() {
        encodedHistogramsBuilder.beginPositionEntry();
        return this;
    }

    @Override
    public ExponentialHistogramBlockBuilder endPositionEntry() {
        encodedHistogramsBuilder.endPositionEntry();
        return this;
    }

    @Override
    public ExponentialHistogramBlockBuilder copyFrom(Block block, int beginInclusive, int endExclusive) {
        if (block.areAllValuesNull()) {
            for (int i = beginInclusive; i < endExclusive; i++) {
                appendNull();
            }
        } else {
            ExponentialHistogramArrayBlock histoBlock = (ExponentialHistogramArrayBlock) block;
            histoBlock.copyInto(encodedHistogramsBuilder, beginInclusive, endExclusive);
        }
        return this;
    }

    @Override
    public ExponentialHistogramBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        return this;
    }

    @Override
    public long estimatedBytes() {
        return encodedHistogramsBuilder.estimatedBytes();
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            Releasables.close(encodedHistogramsBuilder);
        }
    }

}
