/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.CompressedExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ExponentialHistogramBlockBuilder implements Block.Builder {

    private static final long SHALLOW_SIZE =  RamUsageEstimator.shallowSizeOfInstance(ExponentialHistogramBlockBuilder.class);

    private final DoubleBlock.Builder minimaBuilder;
    private final DoubleBlock.Builder maximaBuilder;
    private final DoubleBlock.Builder sumsBuilder;
    private final LongBlock.Builder valueCountsBuilder;
    private final DoubleBlock.Builder zeroThresholdsBuilder;
    private final BytesRefBlock.Builder encodedHistogramsBuilder;

    ExponentialHistogramBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        this.minimaBuilder = blockFactory.newDoubleBlockBuilder(estimatedSize);
        this.maximaBuilder = blockFactory.newDoubleBlockBuilder(estimatedSize);
        this.sumsBuilder = blockFactory.newDoubleBlockBuilder(estimatedSize);
        this.valueCountsBuilder = blockFactory.newLongBlockBuilder(estimatedSize);
        this.zeroThresholdsBuilder = blockFactory.newDoubleBlockBuilder(estimatedSize);
        this.encodedHistogramsBuilder = blockFactory.newBytesRefBlockBuilder(estimatedSize);
    }

    public static ExponentialHistogramBlock buildDirect(
        DoubleBlock minima,
        DoubleBlock maxima,
        DoubleBlock sums,
        LongBlock valueCounts,
        DoubleBlock zeroThresholds,
        BytesRefBlock encodedHistograms
    ) {
        return new ExponentialHistogramArrayBlock(
            minima,
            maxima,
            sums,
            valueCounts,
            zeroThresholds,
            encodedHistograms
        );
    }

    public ExponentialHistogramBlockBuilder append(ExponentialHistogram histogram) {
        assert histogram != null;
        //TODO: fix performance and correctness before using in production code
        // The current implementation encodes the histogram into the format we use for storage on disk
        // This format is optimized for minimal memory usage at the cost of encoding speed
        // In addition, it only support storing the zero threshold as a double value, which is lossy when merging histograms
        // We should add a dedicated encoding when building a block from computed histograms which do not originate from doc values
        // That encoding should be optimized for speed and support storing the zero threshold as (scale, index) pair
        assert histogram.zeroBucket().isIndexBased() == false : "Current encoding only supports double-based zero thresholds";

        ByteArrayOutputStream encodedBytes = new ByteArrayOutputStream();
        try {
            CompressedExponentialHistogram.writeHistogramBytes(
                encodedBytes,
                histogram.scale(),
                histogram.negativeBuckets().iterator(),
                histogram.positiveBuckets().iterator()
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode histogram", e);
        }
        minimaBuilder.appendDouble(histogram.min());
        maximaBuilder.appendDouble(histogram.max());
        sumsBuilder.appendDouble(histogram.sum());
        valueCountsBuilder.appendLong(histogram.valueCount());
        zeroThresholdsBuilder.appendDouble(histogram.zeroBucket().zeroThreshold());
        encodedHistogramsBuilder.appendBytesRef(new BytesRef(encodedBytes.toByteArray()));
        return this;
    }

    @Override
    public ExponentialHistogramBlock build() {
        return new ExponentialHistogramArrayBlock(
            minimaBuilder.build(),
            maximaBuilder.build(),
            sumsBuilder.build(),
            valueCountsBuilder.build(),
            zeroThresholdsBuilder.build(),
            encodedHistogramsBuilder.build()
        );
    }

    @Override
    public ExponentialHistogramBlockBuilder appendNull() {
        minimaBuilder.appendNull();
        maximaBuilder.appendNull();
        sumsBuilder.appendNull();
        valueCountsBuilder.appendNull();
        zeroThresholdsBuilder.appendNull();
        encodedHistogramsBuilder.appendNull();
        return this;
    }

    @Override
    public ExponentialHistogramBlockBuilder beginPositionEntry() {
        minimaBuilder.beginPositionEntry();
        maximaBuilder.beginPositionEntry();
        sumsBuilder.beginPositionEntry();
        valueCountsBuilder.beginPositionEntry();
        zeroThresholdsBuilder.beginPositionEntry();
        encodedHistogramsBuilder.beginPositionEntry();
        return this;
    }

    @Override
    public ExponentialHistogramBlockBuilder endPositionEntry() {
        minimaBuilder.endPositionEntry();
        maximaBuilder.endPositionEntry();
        sumsBuilder.endPositionEntry();
        valueCountsBuilder.endPositionEntry();
        zeroThresholdsBuilder.endPositionEntry();
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
            histoBlock.copyInto(
                minimaBuilder,
                maximaBuilder,
                sumsBuilder,
                valueCountsBuilder,
                zeroThresholdsBuilder,
                encodedHistogramsBuilder,
                beginInclusive,
                endExclusive
            );
        }
        return this;
    }

    @Override
    public ExponentialHistogramBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        assert mvOrdering == Block.MvOrdering.UNORDERED
            : "Exponential histograms don't have a natural order, so it doesn't make sense to call this";
        return this;
    }

    @Override
    public long estimatedBytes() {
        return SHALLOW_SIZE +
            minimaBuilder.estimatedBytes() +
            maximaBuilder.estimatedBytes() +
            sumsBuilder.estimatedBytes() +
            valueCountsBuilder.estimatedBytes() +
            zeroThresholdsBuilder.estimatedBytes() +
            encodedHistogramsBuilder.estimatedBytes();
    }

    @Override
    public void close() {
        Releasables.close(
            minimaBuilder,
            maximaBuilder,
            sumsBuilder,
            valueCountsBuilder,
            zeroThresholdsBuilder,
            encodedHistogramsBuilder
        );
    }

}
