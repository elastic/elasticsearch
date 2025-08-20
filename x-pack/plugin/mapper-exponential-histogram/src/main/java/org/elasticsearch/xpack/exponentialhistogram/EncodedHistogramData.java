/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.exponentialhistogram;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_SCALE;

/**
 * Encodes the data of an exponential histogram in a compact format as byte array.
 * Note that some data of exponential histograms is stored outside of this array as separate doc values for better compression,
 * e.g. the zero threshold. This data is therefore not part of this encoding.
 **/
final class EncodedHistogramData {

    /*
     The encoding has the following format:
        - 1 byte: scale + flags
                  scale has a range of less than 64, so we can use the two remaining bits for flags
                  Currently the only flag is HAS_NEGATIVE_BUCKETS_FLAG, which indicates that there are negative buckets
                  If this flag is not set, we can skip encoding the length of the negative buckets and therefore save a
                  bit of space for this typical case.
        - VInt: (optional) length of encoded negative buckets in bytes, if HAS_NEGATIVE_BUCKETS_FLAG is set
        - byte[]: encoded negative buckets, details on the encoding below
        - byte[]: encoded positive buckets, details on the encoding below
     There is no end marker for the encoded buckets, therefore the total length of the encoded data needs to be known when decoding

     The following scheme is used to encode the negative and positive buckets:
        - if there are no buckets, the result is an empty array (byte[0])
        - write the index of the first bucket as ZigZag-VLong
        - write the count of the first bucket as ZigZag-VLong
        - for each remaining (non-empty) bucket:
           - if there was no empty bucket right before this bucket (the index of the bucket is exactly previousBucketIndex+1),
             write the count for the bucket as ZigZag-VLong
           - Otherwise there is at least one empty bucket between this one and the previous one.
             We compute the number of empty buckets as n=currentBucketIndex-previousIndex-1 and then write -n out as
             ZigZag-VLong followed by the count for the bucket as ZigZag-VLong. The negation is performed to allow to
             distinguish whether a value represents a bucket count (positive number) or the number of empty buckets (negative number)
             when decoding.

     While this encoding is designed for sparse histograms, it compresses well for dense histograms too.
     For fully dense histograms it effectively results in encoding the index of the first bucket, followed by just an array of counts.
     For sparse histograms it corresponds to an interleaved encoding of the bucket indices with delta compression and the bucket counts.
     Even mostly sparse histograms that have some dense regions profit from this encoding.
     */

    static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(EncodedHistogramData.class);

    private static final int SCALE_OFFSET = 11;
    private static final int HAS_NEGATIVE_BUCKETS_FLAG = 1 << 6; // = 64
    private static final int SCALE_MASK = 0x3F; // = 63
    static {
        // protection against changes to MIN_SCALE and MAX_SCALE messing with our encoding
        assert MIN_SCALE + SCALE_OFFSET >= 0;
        assert MAX_SCALE + SCALE_OFFSET <= SCALE_MASK;
    }

    private int scale;

    private byte[] encodedData;
    private int negativeBucketsStart;
    private int negativeBucketsLength;
    private int positiveBucketsLength;

    void decode(BytesRef data) throws IOException {
        this.encodedData = data.bytes;
        ByteArrayStreamInput input = new ByteArrayStreamInput();
        input.reset(data.bytes, data.offset, data.length);

        int scaleWithFlags = input.readByte();
        this.scale = (scaleWithFlags & SCALE_MASK) - SCALE_OFFSET;
        boolean hasNegativeBuckets = (scaleWithFlags & HAS_NEGATIVE_BUCKETS_FLAG) != 0;

        negativeBucketsLength = 0;
        if (hasNegativeBuckets) {
            negativeBucketsLength = input.readVInt();
        }

        negativeBucketsStart = input.getPosition();
        input.skipBytes(negativeBucketsLength);
        positiveBucketsLength = input.available();
    }

    static void write(StreamOutput output, int scale, List<IndexWithCount> negativeBuckets, List<IndexWithCount> positiveBuckets)
        throws IOException {
        assert scale >= MIN_SCALE && scale <= MAX_SCALE : "scale must be in range [" + MIN_SCALE + ", " + MAX_SCALE + "]";
        boolean hasNegativeBuckets = negativeBuckets.isEmpty() == false;
        int scaleWithFlags = (scale + SCALE_OFFSET);
        assert scaleWithFlags >= 0 && scaleWithFlags <= SCALE_MASK;
        if (hasNegativeBuckets) {
            scaleWithFlags |= HAS_NEGATIVE_BUCKETS_FLAG;
        }
        output.writeByte((byte) scaleWithFlags);
        if (hasNegativeBuckets) {
            BytesStreamOutput temp = new BytesStreamOutput();
            BucketsDecoder.serializeBuckets(temp, negativeBuckets);
            BytesReference data = temp.bytes();
            output.writeVInt(data.length());
            output.writeBytes(data.array(), data.arrayOffset(), data.length());
        }
        BucketsDecoder.serializeBuckets(output, positiveBuckets);
    }

    int scale() {
        return scale;
    }

    BucketsDecoder negativeBucketsDecoder() {
        return new BucketsDecoder(negativeBucketsStart, negativeBucketsLength);
    }

    BucketsDecoder positiveBucketsDecoder() {
        return new BucketsDecoder(negativeBucketsStart + negativeBucketsLength, positiveBucketsLength);
    }

    final class BucketsDecoder {

        private long currentIndex;
        /**
         * The count for the bucket this iterator is currently pointing at.
         * A value of {@code -1} is used to represent that the end has been reached.
         */
        private long currentCount;

        private final ByteArrayStreamInput bucketsStreamInput;

        private BucketsDecoder(int encodedBucketsStartOffset, int length) {
            bucketsStreamInput = new ByteArrayStreamInput();
            if (length > 0) {
                bucketsStreamInput.reset(encodedData, encodedBucketsStartOffset, length);
                try {
                    currentIndex = bucketsStreamInput.readZLong() - 1;
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

        private BucketsDecoder(BucketsDecoder toCopy) {
            bucketsStreamInput = new ByteArrayStreamInput();
            bucketsStreamInput.reset(encodedData, toCopy.bucketsStreamInput.getPosition(), toCopy.bucketsStreamInput.getPosition());
            currentCount = toCopy.currentCount;
            currentIndex = toCopy.currentIndex;
        }

        BucketsDecoder copy() {
            return new BucketsDecoder(this);
        }

        private void markEndReached() {
            currentCount = -1;
        }

        boolean hasNext() {
            return currentCount != -1;
        }

        long peekCount() {
            assert currentCount != -1 : "End has already been reached";
            return currentCount;
        }

        long peekIndex() {
            assert currentCount != -1 : "End has already been reached";
            return currentIndex;
        }

        void advance() {
            assert currentCount != -1 : "End has already been reached";
            try {
                if (bucketsStreamInput.available() > 0) {
                    currentIndex++;
                    long countOrNumEmptyBuckets = bucketsStreamInput.readZLong();
                    if (countOrNumEmptyBuckets < 0) {
                        // we have encountered a negative value, this means we "skip"
                        // the given amount of empty buckets
                        long numEmptyBuckets = -countOrNumEmptyBuckets;
                        currentIndex += numEmptyBuckets;
                        // after we have skipped empty buckets, we know that the next value is a non-empty bucket
                        currentCount = bucketsStreamInput.readZLong();
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

        private static void serializeBuckets(StreamOutput out, List<IndexWithCount> buckets) throws IOException {
            if (buckets.isEmpty()) {
                return; // no buckets, therefore nothing to write
            }

            IndexWithCount firstBucket = buckets.getFirst();
            out.writeZLong(firstBucket.index());
            out.writeZLong(firstBucket.count());
            long prevIndex = firstBucket.index();

            for (int i = 1; i < buckets.size(); i++) {
                IndexWithCount bucket = buckets.get(i);

                long indexDelta = bucket.index() - prevIndex;
                assert indexDelta > 0; // values must be sorted and unique
                assert bucket.count() > 0;

                long numEmptyBucketsInBetween = indexDelta - 1;
                if (numEmptyBucketsInBetween > 0) {
                    out.writeZLong(-numEmptyBucketsInBetween);
                }
                out.writeZLong(bucket.count());

                prevIndex = bucket.index();
            }
        }
    }
}
