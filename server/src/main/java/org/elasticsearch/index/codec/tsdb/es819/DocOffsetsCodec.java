/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.GroupVIntUtil;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.Arrays;

/**
 * Represents a codec for encoding and decoding document offsets.
 * Each codec defines custom strategies for compression and decompression
 * of document offsets for compressed binary doc values.
 */
enum DocOffsetsCodec {

    /**
     * A codec that uses delta encoding and bit-packing for storage of document offsets.
     */
    BITPACKING {
        @Override
        public Encoder getEncoder() {
            return (docOffsets, numDocsInCurrentBlock, output) -> {
                int numOffsets = numDocsInCurrentBlock + 1;
                // delta encode
                int maxDelta = 0;
                for (int i = numOffsets - 1; i > 0; i--) {
                    docOffsets[i] -= docOffsets[i - 1];
                    maxDelta = Math.max(maxDelta, docOffsets[i]);
                }
                int bitsPerValue = maxDelta == 0 ? 0 : PackedInts.bitsRequired(maxDelta);
                output.writeByte((byte) bitsPerValue);
                if (bitsPerValue > 0) {
                    long accumulator = 0;
                    int bitsInAccumulator = 0;
                    for (int i = 0; i < numOffsets; i++) {
                        accumulator = (accumulator << bitsPerValue) | docOffsets[i];
                        bitsInAccumulator += bitsPerValue;
                        while (bitsInAccumulator >= 8) {
                            bitsInAccumulator -= 8;
                            output.writeByte((byte) (accumulator >>> bitsInAccumulator));
                        }
                    }
                    if (bitsInAccumulator > 0) {
                        output.writeByte((byte) (accumulator << (8 - bitsInAccumulator)));
                    }
                }
            };
        }

        @Override
        public Decoder getDecoder() {
            return (docOffsets, numDocsInBlock, input) -> {
                int numOffsets = numDocsInBlock + 1;
                int bitsPerValue = input.readByte() & 0xFF;
                if (bitsPerValue == 0) {
                    Arrays.fill(docOffsets, 0, numOffsets, 0);
                } else {
                    int totalBits = numOffsets * bitsPerValue;
                    int totalBytes = (totalBits + 7) / 8;
                    long accumulator = 0;
                    int bitsInAccumulator = 0;
                    int offsetIndex = 0;
                    int mask = (1 << bitsPerValue) - 1;
                    for (int i = 0; i < totalBytes && offsetIndex < numOffsets; i++) {
                        accumulator = (accumulator << 8) | (input.readByte() & 0xFF);
                        bitsInAccumulator += 8;
                        while (bitsInAccumulator >= bitsPerValue && offsetIndex < numOffsets) {
                            bitsInAccumulator -= bitsPerValue;
                            docOffsets[offsetIndex++] = (int) ((accumulator >>> bitsInAccumulator) & mask);
                        }
                    }
                }
                deltaDecode(docOffsets, numOffsets);
            };
        }
    },
    /**
     * A codec that uses grouped VInts for storage of document offsets.
     */
    GROUPED_VINT {
        @Override
        public Encoder getEncoder() {
            return (docOffsets, numDocsInCurrentBlock, output) -> {
                int numOffsets = numDocsInCurrentBlock + 1;
                // delta encode
                for (int i = numOffsets - 1; i > 0; i--) {
                    docOffsets[i] -= docOffsets[i - 1];
                }
                output.writeGroupVInts(docOffsets, numOffsets);
            };
        }

        @Override
        public Decoder getDecoder() {
            return (docOffsets, numDocsInBlock, input) -> {
                int numOffsets = numDocsInBlock + 1;
                GroupVIntUtil.readGroupVInts(input, docOffsets, numOffsets);
                deltaDecode(docOffsets, numOffsets);
            };
        }
    };

    public abstract Encoder getEncoder();

    public abstract Decoder getDecoder();

    /**
     * An encoder to store doc offsets in a more space-efficient format for storage.
     */
    @FunctionalInterface
    public interface Encoder {
        /**
         * Encodes doc offsets in a more space efficient format for storage.
         *
         * @param docOffsets            an array of document offsets to encode
         * @param numDocsInCurrentBlock the number of documents in the current block to encode
         * @param output                the {@link DataOutput} to which the encoded data is written
         * @throws IOException          if an I/O error occurs during the encoding process
         */
        void encode(int[] docOffsets, int numDocsInCurrentBlock, DataOutput output) throws IOException;
    }

    /**
     * A decoder to decode the format on disk to doc offsets.
     * A decoder performs the operations that an encoder performs in reverse order.
     */
    @FunctionalInterface
    public interface Decoder {
        /**
         * Decodes the format on disk to doc offsets.
         *
         * @param docOffsets        the array to store decoded document offsets
         * @param numDocsInBlock    the number of documents in the block to be decoded
         * @param input             the input source containing encoded data to be decoded
         * @throws IOException      if an I/O error occurs during decoding
         */
        void decode(int[] docOffsets, int numDocsInBlock, DataInput input) throws IOException;
    }

    // Borrowed from to TSDBDocValuesEncoder.decodeDelta
    // The `sum` variable helps compiler optimize method, should not be removed.
    void deltaDecode(int[] arr, int length) {
        int sum = 0;
        for (int i = 0; i < length; ++i) {
            sum += arr[i];
            arr[i] = sum;
        }
    }

}
