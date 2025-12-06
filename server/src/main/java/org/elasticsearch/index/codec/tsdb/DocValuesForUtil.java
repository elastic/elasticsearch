/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.index.codec.ForUtil;

import java.io.IOException;

public final class DocValuesForUtil {
    private static final int BITS_IN_FOUR_BYTES = 4 * Byte.SIZE;
    private static final int BITS_IN_FIVE_BYTES = 5 * Byte.SIZE;
    private static final int BITS_IN_SIX_BYTES = 6 * Byte.SIZE;
    private static final int BITS_IN_SEVEN_BYTES = 7 * Byte.SIZE;

    private final int blockSize;
    private final byte[] encoded;

    public DocValuesForUtil(int numericBlockSize) {
        assert numericBlockSize >= ForUtil.BLOCK_SIZE && (numericBlockSize & (ForUtil.BLOCK_SIZE - 1)) == 0
            : "expected to get a block size that a multiple of " + ForUtil.BLOCK_SIZE + ", got " + numericBlockSize;
        this.blockSize = numericBlockSize;
        this.encoded = new byte[numericBlockSize * Long.BYTES];
    }

    public static int roundBits(int bitsPerValue) {
        if (bitsPerValue > 24 && bitsPerValue <= 32) {
            return BITS_IN_FOUR_BYTES;
        } else if (bitsPerValue > 32 && bitsPerValue <= BITS_IN_FIVE_BYTES) {
            return BITS_IN_FIVE_BYTES;
        } else if (bitsPerValue > BITS_IN_FIVE_BYTES && bitsPerValue <= BITS_IN_SIX_BYTES) {
            return BITS_IN_SIX_BYTES;
        } else if (bitsPerValue > BITS_IN_SIX_BYTES && bitsPerValue <= BITS_IN_SEVEN_BYTES) {
            return BITS_IN_SEVEN_BYTES;
        } else if (bitsPerValue > BITS_IN_SEVEN_BYTES) {
            return Long.BYTES * Byte.SIZE;
        }
        return bitsPerValue;
    }

    public void encode(long[] in, int bitsPerValue, final DataOutput out) throws IOException {
        if (bitsPerValue <= 24) { // these bpvs are handled efficiently by ForUtil
            ForUtil.encode(in, bitsPerValue, out);
        } else if (bitsPerValue <= 32) {
            for (int k = 0; k < blockSize >> ForUtil.BLOCK_SIZE_SHIFT; k++) {
                collapse32(in, k * ForUtil.BLOCK_SIZE);
                for (int i = 0; i < ForUtil.BLOCK_SIZE / 2; i++) {
                    out.writeLong(in[k * ForUtil.BLOCK_SIZE + i]);
                }
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
        for (int i = 0; i < in.length; ++i) {
            ByteUtils.writeLongLE(in[i], this.encoded, i * bytesPerValue);
        }
        out.writeBytes(this.encoded, bytesPerValue * in.length);
    }

    public void decode(int bitsPerValue, final DataInput in, long[] out) throws IOException {
        if (bitsPerValue <= 24) {
            ForUtil.decode(bitsPerValue, in, out);
        } else if (bitsPerValue <= 32) {
            for (int k = 0; k < blockSize >> ForUtil.BLOCK_SIZE_SHIFT; k++) {
                in.readLongs(out, k * ForUtil.BLOCK_SIZE, ForUtil.BLOCK_SIZE / 2);
                expand32(out, k * ForUtil.BLOCK_SIZE);
            }
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

    private static void collapse32(long[] arr, int offset) {
        for (int i = 0; i < 64; ++i) {
            arr[i + offset] = (arr[i + offset] << 32) | arr[64 + i + offset];
        }
    }

    private static void expand32(long[] arr, int offset) {
        for (int i = 0; i < 64; ++i) {
            long l = arr[i + offset];
            arr[i + offset] = l >>> 32;
            arr[64 + i + offset] = l & 0xFFFFFFFFL;
        }
    }
}
