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
    private final AggregateMetricDoubleBlockBuilder aggregateMetricsBuilder;

    private final BytesRef tempScratch;

    private boolean closed = false;

    ExponentialHistogramBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        BytesRefBlock.Builder histos = null;
        AggregateMetricDoubleBlockBuilder aggregateMetrics = null;
        boolean success = false;
        try {
            histos = blockFactory.newBytesRefBlockBuilder(estimatedSize);
            aggregateMetrics = blockFactory.newAggregateMetricDoubleBlockBuilder(estimatedSize);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(histos, aggregateMetrics);
            }
        }
        this.encodedHistogramsBuilder = histos;
        this.aggregateMetricsBuilder = aggregateMetrics;
        this.tempScratch = new BytesRef(new byte[256], 0, 256);
    }

    @Override
    public ExponentialHistogramBlockBuilder append(@Nullable ExponentialHistogram value) {
        if (value == null) {
            appendNull();
        } else {
            encodedHistogramsBuilder.appendBytesRef(ExponentialHistogramArrayBlock.encode(value, tempScratch));
            long totalValueCount = value.zeroBucket().count() + value.negativeBuckets().valueCount() + value.positiveBuckets().valueCount();
            //TODO: make aggregate metric double support long values for count?
            aggregateMetricsBuilder.count().appendInt((int) totalValueCount);
            //TODO: implement sum/min/max
            aggregateMetricsBuilder.sum().appendNull();
            aggregateMetricsBuilder.min().appendNull();
            aggregateMetricsBuilder.max().appendNull();
        }
        return this;
    }

    @Override
    public ExponentialHistogramBlock build() {
        boolean success = false;
        BytesRefBlock encodedHistos;
        AggregateMetricDoubleBlock aggregateMetrics;
        try {
            encodedHistos = encodedHistogramsBuilder.build();
            aggregateMetrics = aggregateMetricsBuilder.build();
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(encodedHistogramsBuilder, aggregateMetricsBuilder);
            }
        }
        return new ExponentialHistogramArrayBlock(encodedHistos, aggregateMetrics);
    }

    @Override
    public ExponentialHistogramBlockBuilder appendNull() {
        encodedHistogramsBuilder.appendNull();
        aggregateMetricsBuilder.appendNull();
        return this;
    }

    @Override
    public ExponentialHistogramBlockBuilder beginPositionEntry() {
        encodedHistogramsBuilder.beginPositionEntry();
        aggregateMetricsBuilder.beginPositionEntry();
        return this;
    }

    @Override
    public ExponentialHistogramBlockBuilder endPositionEntry() {
        encodedHistogramsBuilder.endPositionEntry();
        aggregateMetricsBuilder.endPositionEntry();
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
            histoBlock.copyInto(encodedHistogramsBuilder, aggregateMetricsBuilder, beginInclusive, endExclusive);
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
        return encodedHistogramsBuilder.estimatedBytes() + aggregateMetricsBuilder.estimatedBytes();
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            Releasables.close(encodedHistogramsBuilder, aggregateMetricsBuilder);
        }
    }
}
