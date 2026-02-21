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

public enum DocOffsetsCodec {

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
    GROUPED_VINT {
        @Override
        public Encoder getEncoder() {
            return new Encoder() {
                @Override
                public void encode(int[] docOffsets, int numDocsInCurrentBlock, DataOutput output) throws IOException {
                    int numOffsets = numDocsInCurrentBlock + 1;
                    // delta encode
                    for (int i = numOffsets - 1; i > 0; i--) {
                        docOffsets[i] -= docOffsets[i - 1];
                    }
                    output.writeGroupVInts(docOffsets, numOffsets);
                }
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

    @FunctionalInterface
    public interface Encoder {
        void encode(int[] docOffsets, int numDocsInCurrentBlock, DataOutput output) throws IOException;
    }

    @FunctionalInterface
    public interface Decoder {
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
