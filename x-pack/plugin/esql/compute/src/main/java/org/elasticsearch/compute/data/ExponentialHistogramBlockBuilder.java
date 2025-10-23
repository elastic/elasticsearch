/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.CompressedExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ZeroBucket;

import java.io.IOException;

public class ExponentialHistogramBlockBuilder implements Block.Builder {

    private final DoubleBlock.Builder minimaBuilder;
    private final DoubleBlock.Builder maximaBuilder;
    private final DoubleBlock.Builder sumsBuilder;
    private final LongBlock.Builder valueCountsBuilder;
    private final DoubleBlock.Builder zeroThresholdsBuilder;
    private final BytesRefBlock.Builder encodedHistogramsBuilder;

    ExponentialHistogramBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        DoubleBlock.Builder minimaBuilder = null;
        DoubleBlock.Builder maximaBuilder = null;
        DoubleBlock.Builder sumsBuilder = null;
        LongBlock.Builder valueCountsBuilder = null;
        DoubleBlock.Builder zeroThresholdsBuilder = null;
        BytesRefBlock.Builder encodedHistogramsBuilder = null;
        boolean success = false;
        try {
            minimaBuilder = blockFactory.newDoubleBlockBuilder(estimatedSize);
            maximaBuilder = blockFactory.newDoubleBlockBuilder(estimatedSize);
            sumsBuilder = blockFactory.newDoubleBlockBuilder(estimatedSize);
            valueCountsBuilder = blockFactory.newLongBlockBuilder(estimatedSize);
            zeroThresholdsBuilder = blockFactory.newDoubleBlockBuilder(estimatedSize);
            encodedHistogramsBuilder = blockFactory.newBytesRefBlockBuilder(estimatedSize);
            this.minimaBuilder = minimaBuilder;
            this.maximaBuilder = maximaBuilder;
            this.sumsBuilder = sumsBuilder;
            this.valueCountsBuilder = valueCountsBuilder;
            this.zeroThresholdsBuilder = zeroThresholdsBuilder;
            this.encodedHistogramsBuilder = encodedHistogramsBuilder;
            success = true;
        } finally {
            if (success == false) {
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
    }

    public ExponentialHistogramBlockBuilder append(ExponentialHistogram histogram) {
        assert histogram != null;
        // TODO: fix performance and correctness before using in production code
        // The current implementation encodes the histogram into the format we use for storage on disk
        // This format is optimized for minimal memory usage at the cost of encoding speed
        // In addition, it only support storing the zero threshold as a double value, which is lossy when merging histograms
        // We should add a dedicated encoding when building a block from computed histograms which do not originate from doc values
        // That encoding should be optimized for speed and support storing the zero threshold as (scale, index) pair
        ZeroBucket zeroBucket = histogram.zeroBucket();
        assert zeroBucket.compareZeroThreshold(ZeroBucket.minimalEmpty()) == 0 || zeroBucket.isIndexBased() == false
            : "Current encoding only supports double-based zero thresholds";

        BytesStreamOutput encodedBytes = new BytesStreamOutput();
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
        if (Double.isNaN(histogram.min())) {
            minimaBuilder.appendNull();
        } else {
            minimaBuilder.appendDouble(histogram.min());
        }
        if (Double.isNaN(histogram.max())) {
            maximaBuilder.appendNull();
        } else {
            maximaBuilder.appendDouble(histogram.max());
        }
        sumsBuilder.appendDouble(histogram.sum());
        valueCountsBuilder.appendLong(histogram.valueCount());
        zeroThresholdsBuilder.appendDouble(zeroBucket.zeroThreshold());
        encodedHistogramsBuilder.appendBytesRef(encodedBytes.bytes().toBytesRef());
        return this;
    }

    @Override
    public ExponentialHistogramBlock build() {
        DoubleBlock minima = null;
        DoubleBlock maxima = null;
        DoubleBlock sums = null;
        LongBlock valueCounts = null;
        DoubleBlock zeroThresholds = null;
        BytesRefBlock encodedHistograms = null;
        boolean success = false;
        try {
            minima = minimaBuilder.build();
            maxima = maximaBuilder.build();
            sums = sumsBuilder.build();
            valueCounts = valueCountsBuilder.build();
            zeroThresholds = zeroThresholdsBuilder.build();
            encodedHistograms = encodedHistogramsBuilder.build();
            success = true;
            return new ExponentialHistogramArrayBlock(minima, maxima, sums, valueCounts, zeroThresholds, encodedHistograms);
        } finally {
            if (success == false) {
                Releasables.close(minima, maxima, sums, valueCounts, zeroThresholds, encodedHistograms);
            }
        }
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
        throw new UnsupportedOperationException("ExponentialHistogramBlock does not support multi-values");
    }

    @Override
    public ExponentialHistogramBlockBuilder endPositionEntry() {
        throw new UnsupportedOperationException("ExponentialHistogramBlock does not support multi-values");
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
        return minimaBuilder.estimatedBytes() + maximaBuilder.estimatedBytes() + sumsBuilder.estimatedBytes() + valueCountsBuilder
            .estimatedBytes() + zeroThresholdsBuilder.estimatedBytes() + encodedHistogramsBuilder.estimatedBytes();
    }

    @Override
    public void close() {
        Releasables.close(minimaBuilder, maximaBuilder, sumsBuilder, valueCountsBuilder, zeroThresholdsBuilder, encodedHistogramsBuilder);
    }
}
