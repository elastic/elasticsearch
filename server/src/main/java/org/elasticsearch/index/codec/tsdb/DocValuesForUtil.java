/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.common.util.ByteUtils;

import java.io.IOException;

public class DocValuesForUtil {
    private static final int BITS_IN_FIVE_BYTES = 5 * Byte.SIZE;
    private static final int BITS_IN_SIX_BYTES = 6 * Byte.SIZE;
    private static final int BITS_IN_SEVEN_BYTES = 7 * Byte.SIZE;
    private final ForUtil forUtil = new ForUtil();
    private final int blockSize;

    public DocValuesForUtil() {
        this(ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE);
    }

    private DocValuesForUtil(int blockSize) {
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
            assert bitsPerValue > 56 : "bitsPerValue must be greater than 56 but was [" + bitsPerValue + "]";
            for (long l : in) {
                out.writeLong(l);
            }
        }
    }

    private void encodeFiveSixOrSevenBytesPerValue(long[] in, int bitsPerValue, final DataOutput out) throws IOException {
        int bytesPerValue = bitsPerValue / Byte.SIZE;
        int padding = Long.BYTES - bytesPerValue;
        byte[] encoded = new byte[bytesPerValue * blockSize + padding];
        for (int i = 0; i < in.length; ++i) {
            ByteUtils.writeLongLE(in[i], encoded, i * bytesPerValue);
        }
        out.writeBytes(encoded, bytesPerValue * in.length);
    }

    void decode(int bitsPerValue, final DataInput in, long[] out) throws IOException {
        if (bitsPerValue <= 24) {
            forUtil.decode(bitsPerValue, in, out);
        } else if (bitsPerValue <= 32) {
            in.readLongs(out, 0, blockSize / 2);
            expand32(out);
        } else if (bitsPerValue == BITS_IN_FIVE_BYTES || bitsPerValue == BITS_IN_SIX_BYTES || bitsPerValue == BITS_IN_SEVEN_BYTES) {
            decodeFiveSixOrSevenBytesPerValue(bitsPerValue, in, out);
        } else {
            assert bitsPerValue > 56 : "bitsPerValue must be greater than 56 but was [" + bitsPerValue + "]";
            in.readLongs(out, 0, blockSize);
        }
    }

    private void decodeFiveSixOrSevenBytesPerValue(int bitsPerValue, final DataInput in, long[] out) throws IOException {
        // NOTE: we expect multibyte values to be written "least significant byte" first
        int bytesPerValue = bitsPerValue / Byte.SIZE;
        long mask = (1L << bitsPerValue) - 1;
        byte[] buffer = new byte[bytesPerValue * blockSize + Long.BYTES - bytesPerValue];
        in.readBytes(buffer, 0, bytesPerValue * blockSize);
        for (int i = 0; i < blockSize; ++i) {
            out[i] = ByteUtils.readLongLE(buffer, i * bytesPerValue) & mask;
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
