/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.exponentialhistogram;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.exponentialhistogram.BucketIterator;
import org.elasticsearch.exponentialhistogram.CopyableBucketIterator;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ZeroBucket;

import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;

/**
 * Implementation of a {@link ExponentialHistogram} optimized for a minimal memory footprint.
 * The compression used here also corresponds to how exponential_histogram fields are stored in
 * doc values by {@link ExponentialHistogramFieldMapper}.
 * <p>
 * While this implementation is optimized for a minimal memory footprint, it is still a fully compliant {@link ExponentialHistogram}
 * and can therefore be directly consumed for merging / quantile estimation without requiring any prior copying or decoding.
 */
public class CompressedExponentialHistogram implements ExponentialHistogram {

    private static final int SCALE_OFFSET = 11;
    private static final int HAS_NEGATIVE_BUCKETS_FLAG = 1 << 6; // = 64
    private static final int SCALE_MASK = 0x3F; // = 63
    static {
        // protection against changes to MIN_SCALE and MAX_SCALE messing with our encoding
        assert MIN_SCALE + SCALE_OFFSET >= 0;
        assert MAX_SCALE + SCALE_OFFSET <= SCALE_MASK;
    }

    private int scale;
    private double zeroThreshold;
    private long valueCount;
    private ZeroBucket lazyZeroBucket;

    private final Buckets positiveBuckets = new Buckets();
    private final Buckets negativeBuckets = new Buckets();

    private byte[] compressedData;

    @Override
    public int scale() {
        return scale;
    }

    @Override
    public ZeroBucket zeroBucket() {
        if (lazyZeroBucket == null) {
            long zeroCount = valueCount - negativeBuckets.valueCount() - positiveBuckets.valueCount();
            lazyZeroBucket = new ZeroBucket(zeroThreshold, zeroCount);
        }
        return lazyZeroBucket;
    }

    @Override
    public ExponentialHistogram.Buckets positiveBuckets() {
        return positiveBuckets;
    }

    @Override
    public ExponentialHistogram.Buckets negativeBuckets() {
        return negativeBuckets;
    }

    /**
     * Resets this instance to decode the provided histogram data.
     *
     * @param zeroThreshold the zeroThreshold for the histogram, which needs to be stored externally
     * @param valueCount the total number of values the histogram contains, needs to be stored externally
     * @param histogramData the encoded histogram bytes which previously where generated via {@code #writeHistogramBytes}
     */
    public void reset(double zeroThreshold, long valueCount, BytesRef histogramData) throws IOException {
        lazyZeroBucket = null;

        this.zeroThreshold = zeroThreshold;
        this.valueCount = valueCount;
        this.compressedData = histogramData.bytes;
        ByteArrayStreamInput input = new ByteArrayStreamInput();
        input.reset(histogramData.bytes, histogramData.offset, histogramData.length);

        int scaleWithFlags = input.readByte();
        this.scale = (scaleWithFlags & SCALE_MASK) - SCALE_OFFSET;
        boolean hasNegativeBuckets = (scaleWithFlags & HAS_NEGATIVE_BUCKETS_FLAG) != 0;

        int negativeBucketsLength = 0;
        if (hasNegativeBuckets) {
            negativeBucketsLength = input.readVInt();
        }

        negativeBuckets.reset(input.getPosition(), negativeBucketsLength);
        input.skipBytes(negativeBucketsLength);
        positiveBuckets.reset(input.getPosition(), input.available());
    }

    private final class Buckets implements ExponentialHistogram.Buckets {

        private int encodedBucketsStart;
        private int encodedBucketsLength;

        private long cachedValueCount;
        private long cachedMaxIndex;

        private Buckets() {
            reset(0, 0);
        }

        private void reset(int encodedBucketsStart, int encodedBucketsLength) {
            this.encodedBucketsStart = encodedBucketsStart;
            this.encodedBucketsLength = encodedBucketsLength;
            cachedValueCount = -1;
            cachedMaxIndex = Long.MIN_VALUE;
        }

        private void computeCachedDataIfRequired() {
            if (cachedValueCount == -1) {
                cachedValueCount = 0;
                BucketIterator it = iterator();
                while (it.hasNext()) {
                    cachedMaxIndex = it.peekIndex();
                    cachedValueCount += it.peekCount();
                    it.advance();
                }
            }
        }

        @Override
        public CopyableBucketIterator iterator() {
            return new CompressedBucketsIterator(encodedBucketsStart, encodedBucketsLength);
        }

        @Override
        public OptionalLong maxBucketIndex() {
            computeCachedDataIfRequired();
            return cachedValueCount > 0 ? OptionalLong.of(cachedMaxIndex) : OptionalLong.empty();
        }

        @Override
        public long valueCount() {
            computeCachedDataIfRequired();
            return cachedValueCount;
        }

        private class CompressedBucketsIterator implements CopyableBucketIterator {

            private long currentIndex;
            /**
             * The count for the bucket this iterator is currently pointing at.
             * A value of {@code -1} is used to represent that the end has been reached.
             */
            private long currentCount;
            private final ByteArrayStreamInput bucketData;

            CompressedBucketsIterator(int encodedBucketsStartOffset, int length) {
                bucketData = new ByteArrayStreamInput();
                if (length > 0) {
                    bucketData.reset(compressedData, encodedBucketsStartOffset, length);
                    try {
                        currentIndex = bucketData.readZLong() - 1;
                    } catch (IOException e) {
                        throw new IllegalStateException("Bad histogram bytes", e);
                    }
                    currentCount = 0;
                    advance();
                } else {
                    // no data means we are iterating over an empty set of buckets
                    markEndReached();
                }
            }

            private void markEndReached() {
                currentCount = -1;
            }

            CompressedBucketsIterator(CompressedBucketsIterator toCopy) {
                bucketData = new ByteArrayStreamInput();
                bucketData.reset(compressedData, toCopy.bucketData.getPosition(), toCopy.bucketData.available());
                currentCount = toCopy.currentCount;
                currentIndex = toCopy.currentIndex;
            }

            @Override
            public CopyableBucketIterator copy() {
                return new CompressedBucketsIterator(this);
            }

            @Override
            public final boolean hasNext() {
                return currentCount > 0;
            }

            @Override
            public final long peekCount() {
                ensureEndNotReached();
                return currentCount;
            }

            @Override
            public final long peekIndex() {
                ensureEndNotReached();
                return currentIndex;
            }

            @Override
            public int scale() {
                return CompressedExponentialHistogram.this.scale();
            }

            /**
             * For details on the encoding, see {@link #writeHistogramBytes(StreamOutput, int, List, List)}.
             */
            @Override
            public final void advance() {
                ensureEndNotReached();
                try {
                    if (bucketData.available() > 0) {
                        currentIndex++;
                        long countOrNumEmptyBuckets = bucketData.readZLong();
                        if (countOrNumEmptyBuckets < 0) {
                            // we have encountered a negative value, this means we "skip"
                            // the given amount of empty buckets
                            long numEmptyBuckets = -countOrNumEmptyBuckets;
                            currentIndex += numEmptyBuckets;
                            // after we have skipped empty buckets, we know that the next value is a non-empty bucket
                            currentCount = bucketData.readZLong();
                        } else {
                            currentCount = countOrNumEmptyBuckets;
                        }
                        assert currentCount > 0;
                    } else {
                        markEndReached();
                    }
                } catch (IOException e) {
                    throw new IllegalStateException("Bad histogram bytes", e);
                }
            }

            private void ensureEndNotReached() {
                if (currentCount == -1) {
                    throw new IllegalStateException("end has already been reached");
                }
            }
        }
    }

    /**
     * Serializes the given histogram, so that exactly the same histogram can be reconstructed via {@link #reset(double, long, BytesRef)}.
     *
     * @param output the output to write the serialized bytes to
     * @param scale the scale of the histogram
     * @param negativeBuckets the negative buckets of the histogram, sorted by the bucket indices
     * @param positiveBuckets the positive buckets of the histogram, sorted by the bucket indices
     */
    public static void writeHistogramBytes(
        StreamOutput output,
        int scale,
        List<IndexWithCount> negativeBuckets,
        List<IndexWithCount> positiveBuckets
    ) throws IOException {
        assert scale >= MIN_SCALE && scale <= MAX_SCALE : "scale must be in range [" + MIN_SCALE + ", " + MAX_SCALE + "]";
        boolean hasNegativeBuckets = negativeBuckets.isEmpty() == false;
        int scaleWithFlags = (scale + SCALE_OFFSET);
        assert scale >= 0 && scale <= SCALE_MASK;
        if (hasNegativeBuckets) {
            scaleWithFlags |= HAS_NEGATIVE_BUCKETS_FLAG;
        }
        output.writeByte((byte) scaleWithFlags);
        if (hasNegativeBuckets) {
            BytesStreamOutput temp = new BytesStreamOutput();
            serializeBuckets(temp, negativeBuckets);
            BytesReference data = temp.bytes();
            output.writeVInt(data.length());
            output.writeBytes(data.array(), data.arrayOffset(), data.length());
        }
        serializeBuckets(output, positiveBuckets);
    }

    // Encodes the given bucket indices and counts as bytes into the given output.
    // The following scheme is used to maximize compression:
    // - if there are no buckets, the result is an empty array (byte[0])
    // - write the index of the first bucket as ZigZag-VLong
    // - write the count of the first bucket as ZigZag-VLong
    // - for each remaining bucket:
    //   - if the index of the bucket is exactly previousBucketIndex+1, write the count for the bucket as ZigZag-VLong
    //   - Otherwise there is at least one empty bucket between this one and the previous one.
    //     We compute that number as n=currentBucketIndex-previousIndex-1 and then write -n out as
    //     ZigZag-VLong followed by the count for the bucket as ZigZag-VLong. The negation is performed to allow to
    //     distinguish the cases when decoding.
    //
    // This encoding provides a compact storage for both dense and sparse histograms:
    // For dense histograms it effectively results in encoding the index of the first bucket, followed by just an array of counts.
    // For sparse histograms it corresponds to an interleaved encoding of the bucket indices with delta compression and the bucket counts.
    // Even partially dense histograms profit from this encoding.
    private static void serializeBuckets(StreamOutput out, List<IndexWithCount> buckets) throws IOException {
        if (buckets.isEmpty()) {
            return; // no buckets, therefore nothing to write
        }
        long minIndex = buckets.getFirst().index();
        out.writeZLong(minIndex);
        long prevIndex = minIndex - 1;
        for (IndexWithCount indexWithCount : buckets) {
            long indexDelta = indexWithCount.index() - prevIndex;
            assert indexDelta > 0; // values must be sorted and unique
            assert indexWithCount.count() > 0;

            long numEmptyBucketsInBetween = indexDelta - 1;
            if (numEmptyBucketsInBetween > 0) {
                out.writeZLong(-numEmptyBucketsInBetween);
            }
            out.writeZLong(indexWithCount.count());

            prevIndex = indexWithCount.index();
        }
    }
}
