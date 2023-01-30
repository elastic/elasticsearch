/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.DirectWriter;

import java.io.IOException;

public class DocValuesForUtil {
    private static final int BITS_IN_FIVE_BYTES = 5 * Byte.SIZE;
    private static final int BITS_IN_SIX_BYTES = 6 * Byte.SIZE;
    private static final int BITS_IN_SEVEN_BYTES = 7 * Byte.SIZE;
    private final ForUtil forUtil = new ForUtil();
    private final int blockSize;

    public DocValuesForUtil() {
        this(ES87TSDBDocValuesFormat.DEFAULT_NUMERIC_BLOCK_SIZE);
    }

    public DocValuesForUtil(int blockSize) {
        this.blockSize = blockSize;
    }

    public static int roundBits(int bitsPerValue) {
        if (bitsPerValue > 32 && bitsPerValue <= BITS_IN_FIVE_BYTES) {
            return BITS_IN_FIVE_BYTES;
        } else if (bitsPerValue > BITS_IN_FIVE_BYTES && bitsPerValue <= BITS_IN_SIX_BYTES) {
            return BITS_IN_SIX_BYTES;
        } else if (bitsPerValue > BITS_IN_SIX_BYTES && bitsPerValue <= BITS_IN_SEVEN_BYTES) {
            return BITS_IN_SEVEN_BYTES;
        }
        return bitsPerValue;
    }

    void encode(long[] in, int bitsPerValue, final DataOutput out) throws IOException {
        if (bitsPerValue <= 24) { // these bpvs are handled efficiently by ForUtil
            forUtil.encode(in, bitsPerValue, out);
        } else if (bitsPerValue <= 32) {
            collapse32(in);
            for (int i = 0; i < blockSize / 2; ++i) {
                out.writeLong(in[i]);
            }
        } else if (bitsPerValue == BITS_IN_FIVE_BYTES || bitsPerValue == BITS_IN_SIX_BYTES || bitsPerValue == BITS_IN_SEVEN_BYTES) {
            encodeFiveSixOrSevenBytesPerValue(in, bitsPerValue, out);
        } else {
            for (long l : in) {
                out.writeLong(l);
            }
        }
    }

    private void encodeFiveSixOrSevenBytesPerValue(long[] in, int bitsPerValue, final DataOutput out) throws IOException {
        final DirectWriter writer = DirectWriter.getInstance(out, in.length, bitsPerValue);
        for (long l : in) {
            writer.add(l);
        }
        writer.finish();
    }

    void decode(int bitsPerValue, final IndexInput in, long[] out) throws IOException {
        if (bitsPerValue <= 24) {
            forUtil.decode(bitsPerValue, in, out);
        } else if (bitsPerValue <= 32) {
            in.readLongs(out, 0, blockSize / 2);
            expand32(out);
        } else if (bitsPerValue == BITS_IN_FIVE_BYTES || bitsPerValue == BITS_IN_SIX_BYTES || bitsPerValue == BITS_IN_SEVEN_BYTES) {
            decodeFiveSixOrSevenBytesPerValue(bitsPerValue, in, out);
        } else {
            in.readLongs(out, 0, blockSize);
        }
    }

    private void decodeFiveSixOrSevenBytesPerValue(int bitsPerValue, final IndexInput in, long[] out) throws IOException {
        // NOTE: we expect multibyte values to be written "least significant byte" first
        for (int longValueIndex = 0; longValueIndex < blockSize; longValueIndex++) {
            int bytesPerValue = bitsPerValue / Byte.SIZE;
            out[longValueIndex] = 0;
            for (int byteIndex = 0; byteIndex < bytesPerValue; byteIndex++) {
                byte b = in.readByte();
                out[longValueIndex] += ((long) b & 0xFFL) << (8 * byteIndex);
            }

        }
        // Skip padding bytes (bitsPerValue = 40 skip 3, bitsPerValue = 48 skip 2, bitsPerValue = 56 skip 1).
        if (bitsPerValue == BITS_IN_FIVE_BYTES) {
            in.readByte();
            in.readByte();
            in.readByte();
        } else if (bitsPerValue == BITS_IN_SIX_BYTES) {
            in.readByte();
            in.readByte();
        } else {
            in.readByte();
        }
    }

    private static void collapse32(long[] arr) {
        for (int i = 0; i < 64; ++i) {
            arr[i] = (arr[i] << 32) | arr[64 + i];
        }
    }

    private static void expand32(long[] arr) {
        for (int i = 0; i < 64; ++i) {
            long l = arr[i];
            arr[i] = l >>> 32;
            arr[64 + i] = l & 0xFFFFFFFFL;
        }
    }
}
