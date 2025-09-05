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
import org.elasticsearch.exponentialhistogram.AbstractExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ZeroBucket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class ExponentialHistogramArrayBlock extends AbstractNonThreadSafeRefCounted implements ExponentialHistogramBlock {

    // TODO(b/133393): Add blocks for min/max/sum/count
    private final BytesRefBlock encodedHistograms;
    private boolean isClosed = false;

    public ExponentialHistogramArrayBlock(BytesRefBlock encodedHistograms) {
        this.encodedHistograms = encodedHistograms;
    }

    @Override
    public ExponentialHistogram getExponentialHistogram(int valueIndex) {
        BytesRef encodedHisto = encodedHistograms.getBytesRef(valueIndex, new BytesRef());
        return new BlockBackedHistogram(encodedHisto);
    }

    static BytesRef encode(ExponentialHistogram histogram, BytesRef growableScratch) {
        return BlockBackedHistogram.encode(histogram, growableScratch);
    }

    @Override
    protected void closeInternal() {
        isClosed = true;
        Releasables.close(encodedHistograms);
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
        return encodedHistograms.getFirstValueIndex(position);
    }

    @Override
    public int getValueCount(int position) {
        return encodedHistograms.getValueCount(position);
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
        encodedHistograms.allowPassingToDifferentDriver();
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
        return encodedHistograms.mayHaveMultivaluedFields();
    }

    @Override
    public boolean doesHaveMultivaluedFields() {
        return encodedHistograms.doesHaveMultivaluedFields();
    }

    @Override
    public Block filter(int... positions) {
        return new ExponentialHistogramArrayBlock(encodedHistograms.filter(positions));
    }

    @Override
    public Block keepMask(BooleanVector mask) {
        return new ExponentialHistogramArrayBlock(encodedHistograms.keepMask(mask));
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
        BytesRefBlock expandedHistograms = encodedHistograms.expand();
        if (expandedHistograms == encodedHistograms) {
            // No values to expand, return original block
            Releasables.close(expandedHistograms);
            this.incRef();
            return this;
        } else {
            return new ExponentialHistogramArrayBlock(expandedHistograms);
        }
    }

    @Override
    public ExponentialHistogramArrayBlock deepCopy(BlockFactory blockFactory) {
        return new ExponentialHistogramArrayBlock(encodedHistograms.deepCopy(blockFactory));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        encodedHistograms.writeTo(out);
    }

    public static ExponentialHistogramArrayBlock readFrom(BlockStreamInput in) throws IOException {
        BytesRefBlock encodedHistograms = BytesRefBlock.readFrom(in);
        return new ExponentialHistogramArrayBlock(encodedHistograms);
    }

    @Override
    public long ramBytesUsed() {
        return encodedHistograms.ramBytesUsed();
    }

    void copyInto(BytesRefBlock.Builder encodedHistogramsBuilder, int beginInclusive, int endExclusive) {
        encodedHistogramsBuilder.copyFrom(this.encodedHistograms, beginInclusive, endExclusive);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ExponentialHistogramBlock block) {
            return ExponentialHistogramBlock.equals(this, block);
        }
        return false;
    }

    boolean equalsAfterTypeCheck(ExponentialHistogramArrayBlock that) {
        return this.encodedHistograms.equals(that.encodedHistograms);
    }

    @Override
    public int hashCode() {
        if (encodedHistograms.areAllValuesNull()) {
            // ensure consistent hash code for all-null blocks
            try (Block nullBlock = blockFactory().newConstantNullBlock(getPositionCount())) {
                return nullBlock.hashCode();
            }
        }
        return encodedHistograms.hashCode();
    }

    private class BlockBackedHistogram extends AbstractExponentialHistogram implements ExponentialHistogram {

        // TODO(b/133393): encode all of the ExponentialHistogram data except for min/max/sum/count
        private static final int SCALE_OFFSET = 0;

        private final ByteBuffer data;

        private BlockBackedHistogram(BytesRef bytes) {
            data = ByteBuffer.wrap(bytes.bytes, bytes.offset, bytes.length).order(ByteOrder.LITTLE_ENDIAN);
        }

        static BytesRef encode(ExponentialHistogram histogram, BytesRef growableBuffer) {
            int totalSize = 1;

            resizeIfRequired(growableBuffer, totalSize);
            BytesRef result = new BytesRef();
            result.bytes = growableBuffer.bytes;
            result.offset = growableBuffer.offset;
            result.length = totalSize;

            ByteBuffer buffer = ByteBuffer.wrap(result.bytes, result.offset, result.length).order(ByteOrder.LITTLE_ENDIAN);
            buffer.put(SCALE_OFFSET, (byte) histogram.scale());

            return result;
        }

        private static void resizeIfRequired(BytesRef growableBuffer, int totalSize) {
            if (growableBuffer.length >= totalSize) {
                return;
            }
            growableBuffer.length = Math.max(32, growableBuffer.length);
            while (growableBuffer.length < totalSize) {
                growableBuffer.length *= 2;
            }
            growableBuffer.offset = 0;
            growableBuffer.bytes = new byte[growableBuffer.length];
        }

        @Override
        public int scale() {
            checkForUseAfterClose();
            return data.get(data.position() + SCALE_OFFSET);
        }

        private void checkForUseAfterClose() {
            if (ExponentialHistogramArrayBlock.this.isClosed) {
                throw new IllegalStateException("Backing block has already been closed!");
            }
        }

        @Override
        public ZeroBucket zeroBucket() {
            // TODO(b/133393): implement
            return ZeroBucket.minimalEmpty();
        }

        @Override
        public ExponentialHistogram.Buckets positiveBuckets() {
            // TODO(b/133393): implement
            return ExponentialHistogram.builder(scale(), ExponentialHistogramCircuitBreaker.noop()).build().positiveBuckets();
        }

        @Override
        public ExponentialHistogram.Buckets negativeBuckets() {
            // TODO(b/133393): implement
            return ExponentialHistogram.builder(scale(), ExponentialHistogramCircuitBreaker.noop()).build().negativeBuckets();
        }

        @Override
        public double sum() {
            // TODO(b/133393): implement
            return 0;
        }

        @Override
        public long valueCount() {
            // TODO(b/133393): implement
            return 0;
        }

        @Override
        public double min() {
            // TODO(b/133393): implement
            return Double.NaN;
        }

        @Override
        public double max() {
            // TODO(b/133393): implement
            return Double.NaN;
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

    }
}
