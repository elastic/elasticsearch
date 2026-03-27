/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_SCALE;

/**
 * Encodes the data of an exponential histogram in a compact format as byte array.
 * Note that some data of exponential histograms is stored outside of this array as separate doc values for better compression,
 * e.g. the zero threshold. This data is therefore not part of this encoding.
 **/
final class CompressedHistogramData {

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

    static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(CompressedHistogramData.class);

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

    void decode(BytesRef data) {
        this.encodedData = data.bytes;
        AccessibleByteArrayStreamInput input = new AccessibleByteArrayStreamInput(data.bytes, data.offset, data.length);

        int scaleWithFlags = input.read();
        this.scale = (scaleWithFlags & SCALE_MASK) - SCALE_OFFSET;
        boolean hasNegativeBuckets = (scaleWithFlags & HAS_NEGATIVE_BUCKETS_FLAG) != 0;

        negativeBucketsLength = 0;
        if (hasNegativeBuckets) {
            negativeBucketsLength = (int) input.readVLong();
        }

        negativeBucketsStart = input.getPosition();
        input.skip(negativeBucketsLength);
        positiveBucketsLength = input.available();
    }

    static void write(OutputStream output, int scale, BucketIterator negativeBuckets, BucketIterator positiveBuckets) throws IOException {
        assert scale >= MIN_SCALE && scale <= MAX_SCALE : "scale must be in range [" + MIN_SCALE + ", " + MAX_SCALE + "]";
        boolean hasNegativeBuckets = negativeBuckets.hasNext();
        int scaleWithFlags = (scale + SCALE_OFFSET);
        assert scaleWithFlags >= 0 && scaleWithFlags <= SCALE_MASK;
        if (hasNegativeBuckets) {
            scaleWithFlags |= HAS_NEGATIVE_BUCKETS_FLAG;
        }
        output.write((byte) scaleWithFlags);
        if (hasNegativeBuckets) {
            AccessibleByteArrayOutputStream temp = new AccessibleByteArrayOutputStream();
            BucketsDecoder.serializeBuckets(temp, negativeBuckets);
            writeVLong(temp.size(), output);
            output.write(temp.getBufferDirect(), 0, temp.size());
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

        private final AccessibleByteArrayStreamInput bucketsStreamInput;

        private BucketsDecoder(int encodedBucketsStartOffset, int length) {
            if (length > 0) {
                bucketsStreamInput = new AccessibleByteArrayStreamInput(encodedData, encodedBucketsStartOffset, length);
                currentIndex = bucketsStreamInput.readZLong() - 1;
                currentCount = 0;
                advance();
            } else {
                bucketsStreamInput = null;
                // no data means we are iterating over an empty set of buckets
                markEndReached();
            }
        }

        private BucketsDecoder(BucketsDecoder toCopy) {
            if (toCopy.bucketsStreamInput != null) {
                int position = toCopy.bucketsStreamInput.getPosition();
                bucketsStreamInput = new AccessibleByteArrayStreamInput(
                    encodedData,
                    position,
                    toCopy.bucketsStreamInput.getLimit() - position
                );
            } else {
                bucketsStreamInput = null;
            }
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
        }

        private static void serializeBuckets(OutputStream out, BucketIterator buckets) throws IOException {
            if (buckets.hasNext() == false) {
                return; // no buckets, therefore nothing to write
            }

            long firstIndex = buckets.peekIndex();
            writeZLong(firstIndex, out);
            writeZLong(buckets.peekCount(), out);
            buckets.advance();

            long prevIndex = firstIndex;

            while (buckets.hasNext()) {
                long index = buckets.peekIndex();
                long count = buckets.peekCount();

                long indexDelta = index - prevIndex;
                assert indexDelta > 0; // values must be sorted and unique
                assert count > 0;

                long numEmptyBucketsInBetween = indexDelta - 1;
                if (numEmptyBucketsInBetween > 0) {
                    writeZLong(-numEmptyBucketsInBetween, out);
                }
                writeZLong(count, out);

                buckets.advance();
                prevIndex = index;
            }
        }
    }

    private static class AccessibleByteArrayOutputStream extends ByteArrayOutputStream {

        byte[] getBufferDirect() {
            return buf;
        }

    }

    private static class AccessibleByteArrayStreamInput extends ByteArrayInputStream {

        AccessibleByteArrayStreamInput(byte[] buf, int offset, int length) {
            super(buf, offset, length);
        }

        int getPosition() {
            return pos;
        }

        int getLimit() {
            return this.count;
        }

        long readZLong() {
            return BitUtil.zigZagDecode(readVLong());
        }

        long readVLong() {
            byte b = (byte) read();
            long i = b & 0x7FL;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = (byte) read();
            i |= (b & 0x7FL) << 7;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = (byte) read();
            i |= (b & 0x7FL) << 14;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = (byte) read();
            i |= (b & 0x7FL) << 21;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = (byte) read();
            i |= (b & 0x7FL) << 28;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = (byte) read();
            i |= (b & 0x7FL) << 35;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = (byte) read();
            i |= (b & 0x7FL) << 42;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = (byte) read();
            i |= (b & 0x7FL) << 49;
            if ((b & 0x80) == 0) {
                return i;
            }
            b = (byte) read();
            i |= ((b & 0x7FL) << 56);
            if ((b & 0x80) == 0) {
                return i;
            }
            b = (byte) read();
            if (b != 0 && b != 1) {
                throw new IllegalStateException("Broken VLong detected");
            }
            i |= ((long) b) << 63;
            return i;
        }
    }

    private static void writeZLong(long i, OutputStream output) throws IOException {
        writeVLong(BitUtil.zigZagEncode(i), output);
    }

    private static void writeVLong(long i, OutputStream output) throws IOException {
        while ((i & ~0x7F) != 0) {
            output.write((byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
        }
        output.write((byte) i);
    }
}
