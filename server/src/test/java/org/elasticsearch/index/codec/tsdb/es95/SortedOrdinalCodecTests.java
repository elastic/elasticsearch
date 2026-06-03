/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

/**
 * Round-trip and encoding-selection tests for {@link SortedOrdinalCodec},
 * the wrapper that delegates to per-mode codec classes implementing the
 * sealed {@link BlockModeCodec} interface ({@link ConstantCodec},
 * {@link TwoRunCodec}, {@link BitPackedCodec}, {@link RleCodec},
 * {@link BitpackCodec}).
 *
 * <p>The wire format mirrors legacy {@code TSDBDocValuesEncoder}: each block
 * starts with a vlong header whose trailing one-bits count selects the
 * encoding (0 CONST, 1 TWO_RUN, 2 BIT_PACKED, 3 ADAPTIVE_EXTRA). Each test
 * crafts a 128-value block whose distribution forces a specific encoding,
 * then verifies encode/decode symmetry and peeks the leading vlong to
 * confirm the encoding selection.
 */
public class SortedOrdinalCodecTests extends ESTestCase {

    public void testBitPackedRoundTripUniformRandom() throws Exception {
        // NOTE: 128 random ords spread across the full bits range -> forces BIT_PACKED
        int bitsPerOrd = 16;
        long mask = (1L << bitsPerOrd) - 1L;
        long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = randomLongBetween(0L, mask);
        }

        SortedOrdinalCodec codec = new SortedOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, bitsPerOrd);

        long[] decoded = new long[128];
        codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), decoded, bitsPerOrd);

        assertArrayEquals(in, decoded);
    }

    public void testConstEncodingChosenForUniformBlock() throws Exception {
        long[] in = new long[128];
        Arrays.fill(in, 42L);

        SortedOrdinalCodec codec = new SortedOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, 16);

        assertEquals(ConstantCodec.ENCODING, peekEncoding(out));

        long[] decoded = new long[128];
        codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), decoded, 16);
        assertArrayEquals(in, decoded);
    }

    public void testConstBlockBytes() throws Exception {
        long[] in = new long[128];
        Arrays.fill(in, 12345L);

        SortedOrdinalCodec codec = new SortedOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, 16);

        // NOTE: (12345 << 1) = 24690 fits in a 3-byte vlong.
        assertThat(out.size(), equalTo(3L));
    }

    public void testSmallConstFitsInOneByte() throws Exception {
        long[] in = new long[128];
        Arrays.fill(in, 7L);

        SortedOrdinalCodec codec = new SortedOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, 16);

        // NOTE: (7 << 1) = 14 fits in a 1-byte vlong; ord and CONST marker share the byte.
        assertThat(out.size(), equalTo(1L));
    }

    public void testTwoRunEncodingChosen() throws Exception {
        long[] in = new long[128];
        Arrays.fill(in, 0, 80, 7L);
        Arrays.fill(in, 80, 128, 11L);

        SortedOrdinalCodec codec = new SortedOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, 16);

        assertEquals(TwoRunCodec.ENCODING, peekEncoding(out));

        long[] decoded = new long[128];
        codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), decoded, 16);
        assertArrayEquals(in, decoded);
    }

    public void testBitpackLocalChosenWhenRangeIsNarrow() throws Exception {
        // NOTE: segment bits says 16; block uses ords in [1000, 1015] so local bits = 4
        long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = 1000L + randomIntBetween(0, 15);
        }
        int segmentBitsPerOrd = 16;

        SortedOrdinalCodec codec = new SortedOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, segmentBitsPerOrd);

        ByteBuffersDataInput peek = new ByteBuffersDataInput(out.toBufferList());
        assertEquals(SortedOrdinalCodec.ADAPTIVE_EXTRA_ENCODING, Long.numberOfTrailingZeros(~peek.readVLong()));
        assertEquals(BitpackCodec.SUB_MODE, peek.readByte());

        long[] decoded = new long[128];
        codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), decoded, segmentBitsPerOrd);
        assertArrayEquals(in, decoded);
    }

    public void testRleChosenForManyShortRuns() throws Exception {
        long[] in = new long[128];
        // NOTE: 4 runs of 32 each (in budget for RLE_N)
        Arrays.fill(in, 0, 32, 7L);
        Arrays.fill(in, 32, 64, 11L);
        Arrays.fill(in, 64, 96, 13L);
        Arrays.fill(in, 96, 128, 17L);
        int segmentBitsPerOrd = 16;

        SortedOrdinalCodec codec = new SortedOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, segmentBitsPerOrd);

        ByteBuffersDataInput peek = new ByteBuffersDataInput(out.toBufferList());
        assertEquals(SortedOrdinalCodec.ADAPTIVE_EXTRA_ENCODING, Long.numberOfTrailingZeros(~peek.readVLong()));
        assertEquals(RleCodec.SUB_MODE, peek.readByte());

        long[] decoded = new long[128];
        codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), decoded, segmentBitsPerOrd);
        assertArrayEquals(in, decoded);
    }

    public void testBitPackedChosenForHighEntropyBlock() throws Exception {
        long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = randomLongBetween(0, (1L << 16) - 1);
        }
        SortedOrdinalCodec codec = new SortedOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, 16);
        // NOTE: 128 random 16-bit ords spread across the full range -> BIT_PACKED wins
        assertEquals(BitPackedCodec.ENCODING, peekEncoding(out));
    }

    public void testBitpackLocalBeatsBitPackedOnNarrowRangeWithManyDistinct() throws Exception {
        long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            // NOTE: 64 distinct values in [5000, 5063] - local bits = 6 vs segment bits = 16
            in[i] = 5000L + randomLongBetween(0, 63);
        }
        SortedOrdinalCodec codec = new SortedOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, 16);
        ByteBuffersDataInput peek = new ByteBuffersDataInput(out.toBufferList());
        assertEquals(SortedOrdinalCodec.ADAPTIVE_EXTRA_ENCODING, Long.numberOfTrailingZeros(~peek.readVLong()));
        assertEquals(BitpackCodec.SUB_MODE, peek.readByte());
    }

    public void testCorruptEncodingThrows() throws Exception {
        SortedOrdinalCodec codec = new SortedOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        // NOTE: vlong with 5 trailing one-bits (0b11111) selects a non-existent encoding
        out.writeVLong(0b11111L);
        long[] dst = new long[128];
        expectThrows(CorruptIndexException.class, () -> codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), dst, 16));
    }

    public void testCorruptAdaptiveExtraSubModeThrows() throws Exception {
        SortedOrdinalCodec codec = new SortedOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        // NOTE: encoding 3 header (0b111), then an unknown sub-mode byte
        out.writeVLong(0b111L);
        out.writeByte((byte) 99);
        long[] dst = new long[128];
        expectThrows(CorruptIndexException.class, () -> codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), dst, 16));
    }

    public void testRleRunOverflowThrows() throws Exception {
        SortedOrdinalCodec codec = new SortedOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        // NOTE: encoding 3 header (0b111), RLE_N sub-mode, then an out-of-range run length
        out.writeVLong(0b111L);
        out.writeByte(RleCodec.SUB_MODE);
        out.writeVInt(1);
        out.writeVLong(42);
        out.writeVInt(200);
        long[] dst = new long[128];
        expectThrows(CorruptIndexException.class, () -> codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), dst, 16));
    }

    private static int peekEncoding(final ByteBuffersDataOutput out) throws Exception {
        return Long.numberOfTrailingZeros(~new ByteBuffersDataInput(out.toBufferList()).readVLong());
    }
}
