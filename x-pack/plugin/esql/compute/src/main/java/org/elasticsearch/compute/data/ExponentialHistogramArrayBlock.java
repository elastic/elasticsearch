/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.BucketIterator;
import org.elasticsearch.exponentialhistogram.CopyableBucketIterator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ZeroBucket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.OptionalLong;

public final class ExponentialHistogramArrayBlock extends AbstractNonThreadSafeRefCounted implements ExponentialHistogramBlock {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ExponentialHistogramArrayBlock.class);

    // Invariants for these sub-blocks:
    // - the value count for each position is the same across all sub-blocks
    // - if an encondedHistogram value is null, min/max/sum/count must be null too
    // - if an encondedHistogram value is non-null, sum and count must not be null, but min/max may be (for empty histograms)
    private final BytesRefBlock encodedHistograms;
    private final LongBlock valuesCounts;
    //TODO: add DoubleBlocks for sum/min/max

    public ExponentialHistogramArrayBlock(BytesRefBlock encodedHistograms, LongBlock valuesCounts) {
        this.encodedHistograms = encodedHistograms;
        this.valuesCounts = valuesCounts;
        int positionCount = encodedHistograms.getPositionCount();
        for (Block b : List.of(encodedHistograms, valuesCounts)) {
            if (b.getPositionCount() != positionCount) {
                assert false : "expected positionCount=" + positionCount + " but was " + b;
                throw new IllegalArgumentException("expected positionCount=" + positionCount + " but was " + b);
            }
            if (b.isReleased()) {
                assert false : "can't build exponential histogram block out of released blocks but [" + b + "] was released";
                throw new IllegalArgumentException(
                    "can't build aggregate_metric_double block out of released blocks but [" + b + "] was released"
                );
            }
        }
    }

    @Override
    public ExponentialHistogram getExponentialHistogram(int valueIndex) {
        BytesRef encodedHisto = new BytesRef();
        encodedHistograms.getBytesRef(valueIndex, encodedHisto);
        return new BlockBackedHistogram(encodedHisto);
    }

    static BytesRef encode(ExponentialHistogram histogram, BytesRef growableScratch) {
        return BlockBackedHistogram.encode(histogram, growableScratch);
    }

    private List<Block> subBlocks() {
        return List.of(encodedHistograms, valuesCounts);
    }

    @Override
    protected void closeInternal() {
        Releasables.close(subBlocks());
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
        for (Block b : subBlocks()) {
            b.allowPassingToDifferentDriver();
        }
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
        BytesRefBlock filteredHistograms = null;
        LongBlock filteredValueCounts = null;
        boolean success = false;
        try {
            filteredHistograms = encodedHistograms.filter(positions);
            filteredValueCounts = valuesCounts.filter(positions);
            success = true;
            return new ExponentialHistogramArrayBlock(filteredHistograms, filteredValueCounts);
        } finally {
            if (success == false) {
                Releasables.close(filteredHistograms, filteredValueCounts);
            }
        }
    }

    @Override
    public Block keepMask(BooleanVector mask) {
        BytesRefBlock filteredHistograms = null;
        LongBlock filteredValueCounts = null;
        boolean success = false;
        try {
            filteredHistograms = encodedHistograms.keepMask(mask);
            filteredValueCounts = valuesCounts.keepMask(mask);
            success = true;
            return new ExponentialHistogramArrayBlock(filteredHistograms, filteredValueCounts);
        } finally {
            if (success == false) {
                Releasables.close(filteredHistograms, filteredValueCounts);
            }
        }
    }

    @Override
    public ReleasableIterator<? extends Block> lookup(IntBlock positions, ByteSizeValue targetBlockSize) {
        // TODO: add support
        throw new UnsupportedOperationException("can't lookup values from ExponentialHistogramArrayBlock");
    }

    @Override
    public MvOrdering mvOrdering() {
        // TODO: is this correct? histograms don't have a natural ordering but might be ordered by e.g. their maximum value?
        return MvOrdering.UNORDERED;
    }

    @Override
    public Block expand() {
        BytesRefBlock expandedHistograms = null;
        LongBlock expandedValueCounts = null;
        boolean success = false;
        try {
            expandedHistograms = encodedHistograms.expand();
            expandedValueCounts = valuesCounts.expand();
            success = true;

            if (expandedHistograms == encodedHistograms) {
                // No values to expand, return original block
                Releasables.close(expandedHistograms, expandedValueCounts);
                this.incRef();
                return this;
            } else {
                return new ExponentialHistogramArrayBlock(expandedHistograms, expandedValueCounts);
            }
        } finally {
            if (success == false) {
                Releasables.close(expandedHistograms, expandedValueCounts);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        encodedHistograms.writeTo(out);
        valuesCounts.writeTo(out);
    }

    public static ExponentialHistogramArrayBlock readFrom(BlockStreamInput in) throws IOException {
        BytesRefBlock encodedHistograms = BytesRefBlock.readFrom(in);
        LongBlock valuesCounts = LongBlock.readFrom(in);
        return new ExponentialHistogramArrayBlock(encodedHistograms, valuesCounts);
    }

    @Override
    public long ramBytesUsed() {
        long bytes = SHALLOW_SIZE;
        for (Block subBlock : subBlocks()) {
            bytes += subBlock.ramBytesUsed();
        }
        return bytes;
    }

    void copyInto(BytesRefBlock.Builder encodedHistogramsBuilder, LongBlock.Builder valuesCountsBuilder, int beginInclusive, int endExclusive) {
        encodedHistogramsBuilder.copyFrom(this.encodedHistograms, beginInclusive, endExclusive);
        valuesCountsBuilder.copyFrom(this.valuesCounts, beginInclusive, endExclusive);
    }

    private class BlockBackedHistogram implements ExponentialHistogram {

        // A encoding for an exponential histogram into byte[]
        // In contrast to the encoding used in the mapper this one needs to be optimized for fast en/decoding
        // and may consume a little more bytes per histogram bucket.
        // nontheless, we should probably still do some kind of compression for the buckets

        private static final int SCALE_OFFSET = 0;
        private static final int ZERO_COUNT_OFFSET = SCALE_OFFSET + 1;
        private static final int ZERO_THRESHOLD_OFFSET = ZERO_COUNT_OFFSET + 8;
        private static final int NEGATIVE_BUCKET_COUNT_OFFSET = ZERO_THRESHOLD_OFFSET + 8;
        private static final int BUCKETS_ARRAY_START_OFFSET = NEGATIVE_BUCKET_COUNT_OFFSET + 4;

        private static final int BUCKET_INDEX_OFFSET = 0;
        private static final int BUCKET_COUNT_OFFSET = BUCKET_INDEX_OFFSET + 8;

        private static final int BUCKET_STRIDE = 16;

        private final int scale;

        private final ZeroBucket zeroBucket;
        private final Buckets negativeBuckets;
        private final Buckets positiveBuckets;
        private final ByteBuffer data;

        private BlockBackedHistogram(BytesRef bytes) {
            data = ByteBuffer.wrap(bytes.bytes, 0, bytes.offset+ bytes.length).order(ByteOrder.LITTLE_ENDIAN);
            scale = data.get(bytes.offset+SCALE_OFFSET);
            long zeroCount = data.getLong(bytes.offset+ZERO_COUNT_OFFSET);
            double zeroThreshold = data.getDouble(bytes.offset+ZERO_THRESHOLD_OFFSET);
            zeroBucket = new ZeroBucket(zeroThreshold, zeroCount);
            int negativeBucketCount = data.getInt(bytes.offset+NEGATIVE_BUCKET_COUNT_OFFSET);
            int negativeBucketsLength = negativeBucketCount * BUCKET_STRIDE;

            negativeBuckets = new Buckets(bytes.offset+BUCKETS_ARRAY_START_OFFSET, negativeBucketsLength);
            int positiveBucketsStart = bytes.offset+BUCKETS_ARRAY_START_OFFSET + negativeBucketsLength;
            int positiveBucketsLength = data.limit() - positiveBucketsStart;
            positiveBuckets = new Buckets(positiveBucketsStart, positiveBucketsLength);
        }

        static BytesRef encode(ExponentialHistogram histogram, BytesRef growableBuffer) {
            int negativeBucketCount = histogram.negativeBuckets().bucketCount();
            int totalBucketCount = histogram.positiveBuckets().bucketCount() + negativeBucketCount;
            int totalSize = BUCKETS_ARRAY_START_OFFSET + BUCKET_STRIDE * totalBucketCount;
            resizeIfRequired(growableBuffer, totalSize);

            BytesRef result = new BytesRef();
            result.bytes = growableBuffer.bytes;
            result.offset = growableBuffer.offset;
            result.length = totalSize;
            ByteBuffer buffer = ByteBuffer.wrap(result.bytes, result.offset, result.length).order(ByteOrder.LITTLE_ENDIAN);
            buffer.put(SCALE_OFFSET, (byte) histogram.scale());

            ZeroBucket zb = histogram.zeroBucket();
            buffer.putLong(ZERO_COUNT_OFFSET, zb.count());
            buffer.putDouble(ZERO_THRESHOLD_OFFSET, zb.zeroThreshold());
            buffer.putInt(NEGATIVE_BUCKET_COUNT_OFFSET, negativeBucketCount);

            int offset = BUCKETS_ARRAY_START_OFFSET;
            for (BucketIterator it : List.of(histogram.negativeBuckets().iterator(), histogram.positiveBuckets().iterator())) {
                while (it.hasNext()) {
                    buffer.putLong(offset + BUCKET_INDEX_OFFSET, it.peekIndex());
                    buffer.putLong(offset + BUCKET_COUNT_OFFSET, it.peekCount());
                    offset += BUCKET_STRIDE;
                    it.advance();
                }
            }
            return result;
        }

        private static void resizeIfRequired(BytesRef growableBuffer, int totalSize) {
            if (growableBuffer.length >= totalSize) {
                return;
            }
            while (growableBuffer.length < totalSize) {
                growableBuffer.length *= 2;
            }
            growableBuffer.offset = 0;
            growableBuffer.bytes = new byte[growableBuffer.length];
        }

        @Override
        public int scale() {
            return scale;
        }

        @Override
        public ZeroBucket zeroBucket() {
            return zeroBucket;
        }

        @Override
        public ExponentialHistogram.Buckets positiveBuckets() {
            return positiveBuckets;
        }

        @Override
        public ExponentialHistogram.Buckets negativeBuckets() {
            return negativeBuckets;
        }

        @Override
        public long ramBytesUsed() {
            //TODO: implement for shallow size
            return 0L;
        }

        private class Buckets implements ExponentialHistogram.Buckets {

            private final int offset;
            private final int length;
            private long cachedValueCount = -1;

            private Buckets(int offset, int length) {
                this.offset = offset;
                this.length = length;
            }

            @Override
            public int bucketCount() {
                return length / BUCKET_STRIDE;
            }

            private long getIndexForBucket(long slot) {
                return data.getLong((int) (offset + slot * BUCKET_STRIDE + BUCKET_INDEX_OFFSET));
            }

            private long getCountForBucket(long slot) {
                return data.getLong((int) (offset + slot * BUCKET_STRIDE + BUCKET_COUNT_OFFSET));
            }

            @Override
            public CopyableBucketIterator iterator() {
                return new Iterator(0);
            }

            @Override
            public OptionalLong maxBucketIndex() {
                int bucketCount = bucketCount();
                if (bucketCount == 0) {
                    return OptionalLong.empty();
                } else {
                    return OptionalLong.of(getIndexForBucket(bucketCount - 1));
                }
            }

            @Override
            public long valueCount() {
                if (cachedValueCount == -1) {
                    cachedValueCount = 0L;
                    int bucketCount = bucketCount();
                    for (int i = 0; i < bucketCount; i++) {
                        cachedValueCount += getCountForBucket(i);
                    }
                }
                return cachedValueCount;
            }

            private class Iterator implements CopyableBucketIterator {

                int position;

                Iterator(int position) {
                    this.position = position;
                }

                @Override
                public boolean hasNext() {
                    return position < bucketCount();
                }

                @Override
                public long peekCount() {
                    return getCountForBucket(position);
                }

                @Override
                public long peekIndex() {
                    return getIndexForBucket(position);
                }

                @Override
                public void advance() {
                    position++;
                }

                @Override
                public int scale() {
                    return scale;
                }

                @Override
                public CopyableBucketIterator copy() {
                    return new Iterator(position);
                }
            }
        }
    }
}
