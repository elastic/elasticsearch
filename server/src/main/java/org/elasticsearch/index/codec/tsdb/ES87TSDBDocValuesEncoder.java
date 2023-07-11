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
import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;
import java.util.Arrays;

/**
 * This class provides encoding and decoding of doc values using the following schemes:
 * <ul>
 * <li>
 *     delta encoding: encodes numeric fields in such a way to store the initial value and the difference between the initial value and
 *     all subsequent values. Delta values normally require much less bits than the original 32 or 64 bits.
 * </li>
 *
 * <li>
 *     offset encoding: encodes numeric fields in such a way to store values in range [0, max - min] instead of [min, max]. Reducing the
 *     range makes delta encoding much more effective since numbers in range [0, max - min] require less bits than values in range
 *     [min, max].
 * </li>
 *
 * <li>
 *     gcd encoding: encodes numeric fields in such a way to store values divided by their Greatest Common Divisor. Diving values by their
 *     GCD reduces values magnitude making delta encoding much more effective as a result of the fact that dividing a number by another
 *     number reduces its magnitude and, as a result, the bits required to represent it.
 * </li>
 *
 * <li>
 *     (f)or encoding: encodes numeric fields in such a way to store the initial value and then the XOR between each value and the previous
 *     one, making delta encoding much more effective. Values sharing common values for higher bits will require less bits when delta
 *     encoded. This is expected to be effective especially with floating point values sharing a common exponent and sign bit.
 * </li>
 * </ul>
 *
 * Notice that encoding and decoding are written in a nested way, for instance {@link ES87TSDBDocValuesEncoder#deltaEncode} calling
 * {@link ES87TSDBDocValuesEncoder#removeOffset} and so on. This allows us to easily introduce new encoding schemes or remove existing
 * (non-effective) encoding schemes in a backward-compatible way.
 *
 * A token is used as a bitmask to represent which encoding is applied and allows us to detect the applied encoding scheme at decoding time.
 * This encoding and decoding scheme is meant to work on blocks of 128 values. Larger block sizes incur a decoding penalty when random
 * access to doc values is required since a full block must be decoded.
 *
 * Of course, decoding follows the opposite order with respect to encoding.
 */
public class ES87TSDBDocValuesEncoder {
    private final DocValuesForUtil forUtil;

    public ES87TSDBDocValuesEncoder() {
        this.forUtil = new DocValuesForUtil();
    }

    /**
     * Delta-encode monotonic fields. This is typically helpful with near-primary sort fields or
     * SORTED_NUMERIC/SORTED_SET doc values with many values per document.
     */
    private void deltaEncode(int token, int tokenBits, long[] in, DataOutput out) throws IOException {
        int gts = 0;
        int lts = 0;
        for (int i = 1; i < ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE; ++i) {
            if (in[i] > in[i - 1]) {
                gts++;
            } else if (in[i] < in[i - 1]) {
                lts++;
            }
        }

        final boolean doDeltaCompression = (gts == 0 && lts >= 2) || (lts == 0 && gts >= 2);
        long first = 0;
        if (doDeltaCompression) {
            for (int i = ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE - 1; i > 0; --i) {
                in[i] -= in[i - 1];
            }
            // Avoid setting in[0] to 0 in case there is a minimum interval between
            // consecutive values. This might later help compress data using fewer
            // bits per value.
            first = in[0] - in[1];
            in[0] = in[1];
            token = (token << 1) | 0x01;
        } else {
            token <<= 1;
        }
        removeOffset(token, tokenBits + 1, in, out);
        if (doDeltaCompression) {
            out.writeZLong(first);
        }
    }

    private void removeOffset(int token, int tokenBits, long[] in, DataOutput out) throws IOException {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (long l : in) {
            min = Math.min(l, min);
            max = Math.max(l, max);
        }

        if (max - min < 0) {
            // overflow
            min = 0;
        } else if (min > 0 && min < (max >>> 2)) {
            // removing the offset is unlikely going to help save bits per value, yet it makes decoding
            // slower
            min = 0;
        }

        if (min != 0) {
            for (int i = 0; i < ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE; ++i) {
                in[i] -= min;
            }
            token = (token << 1) | 0x01;
        } else {
            token <<= 1;
        }

        gcdEncode(token, tokenBits + 1, in, out);
        if (min != 0) {
            out.writeZLong(min);
        }
    }

    /**
     * See if numbers have a common divisor. This is typically helpful for integer values in
     * floats/doubles or dates that don't have millisecond accuracy.
     */
    private void gcdEncode(int token, int tokenBits, long[] in, DataOutput out) throws IOException {
        long gcd = 0;
        for (long l : in) {
            gcd = MathUtil.gcd(gcd, l);
            if (gcd == 1) {
                break;
            }
        }
        final boolean doGcdCompression = Long.compareUnsigned(gcd, 1) > 0;
        if (doGcdCompression) {
            for (int i = 0; i < ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE; ++i) {
                in[i] /= gcd;
            }
            token = (token << 1) | 0x01;
        } else {
            token <<= 1;
        }

        forEncode(token, tokenBits + 1, in, out);
        if (doGcdCompression) {
            out.writeVLong(gcd - 2);
        }
    }

    private void forEncode(int token, int tokenBits, long[] in, DataOutput out) throws IOException {
        long or = 0;
        for (long l : in) {
            or |= l;
        }

        int bitsPerValue = or == 0 ? 0 : DocValuesForUtil.roundBits(PackedInts.unsignedBitsRequired(or));
        out.writeVInt((bitsPerValue << tokenBits) | token);
        if (bitsPerValue > 0) {
            forUtil.encode(in, bitsPerValue, out);
        }
    }

    /**
     * Encode the given longs using a combination of delta-coding, GCD factorization and bit packing.
     */
    void encode(long[] in, DataOutput out) throws IOException {
        assert in.length == ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE;

        deltaEncode(0, 0, in, out);
    }

    /** Decode longs that have been encoded with {@link #encode}. */
    void decode(DataInput in, long[] out) throws IOException {
        assert out.length == ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE : out.length;

        final int token = in.readVInt();
        final int bitsPerValue = token >>> 3;

        if (bitsPerValue != 0) {
            forUtil.decode(bitsPerValue, in, out);
        } else {
            Arrays.fill(out, 0L);
        }

        // simple blocks that only perform bit packing exit early here
        // this is typical for SORTED(_SET) ordinals
        if ((token & 0x07) != 0) {

            final boolean doGcdCompression = (token & 0x01) != 0;
            if (doGcdCompression) {
                final long gcd = 2 + in.readVLong();
                mul(out, gcd);
            }

            final boolean hasOffset = (token & 0x02) != 0;
            if (hasOffset) {
                final long min = in.readZLong();
                add(out, min);
            }

            final boolean doDeltaCompression = (token & 0x04) != 0;
            if (doDeltaCompression) {
                final long first = in.readZLong();
                out[0] += first;
                deltaDecode(out);
            }
        }
    }

    // this loop should auto-vectorize
    private static void mul(long[] arr, long m) {
        for (int i = 0; i < ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE; ++i) {
            arr[i] *= m;
        }
    }

    // this loop should auto-vectorize
    private static void add(long[] arr, long min) {
        for (int i = 0; i < ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE; ++i) {
            arr[i] += min;
        }
    }

    private static void deltaDecode(long[] arr) {
        for (int i = 1; i < ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE; ++i) {
            arr[i] += arr[i - 1];
        }
    }
}
