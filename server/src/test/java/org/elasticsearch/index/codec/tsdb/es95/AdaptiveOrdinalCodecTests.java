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
 * Round-trip and mode-selection tests for {@link AdaptiveOrdinalCodec}, the
 * wrapper that delegates to per-mode codec classes implementing the sealed
 * {@link BlockModeCodec} interface ({@link LegacyCodec},
 * {@link ConstantCodec}, {@link RleCodec}, {@link BitpackCodec}). Each test
 * crafts a 128-value block whose distribution forces a specific mode, then
 * verifies encode/decode symmetry and (where relevant) the mode-byte
 * selection by peeking the wire format directly.
 */
public class AdaptiveOrdinalCodecTests extends ESTestCase {

    public void testLegacyRoundTripUniformRandom() throws Exception {
        // NOTE: 128 random ords spread across the full bits range -> forces LEGACY
        int bitsPerOrd = 16;
        long mask = (1L << bitsPerOrd) - 1L;
        long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = randomLongBetween(0L, mask);
        }

        AdaptiveOrdinalCodec codec = new AdaptiveOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, bitsPerOrd);

        long[] decoded = new long[128];
        codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), decoded, bitsPerOrd);

        assertArrayEquals(in, decoded);
    }

    public void testConstModeChosenForUniformBlock() throws Exception {
        long[] in = new long[128];
        Arrays.fill(in, 42L);

        AdaptiveOrdinalCodec codec = new AdaptiveOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, 16);

        ByteBuffersDataInput peek = new ByteBuffersDataInput(out.toBufferList());
        assertEquals(ConstantCodec.MODE, peek.readByte());

        long[] decoded = new long[128];
        codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), decoded, 16);
        assertArrayEquals(in, decoded);
    }

    public void testConstBlockEncodesToVeryFewBytes() throws Exception {
        long[] in = new long[128];
        Arrays.fill(in, 12345L);

        AdaptiveOrdinalCodec codec = new AdaptiveOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, 16);

        // NOTE: 1 byte mode + 2-byte vlong for 12345 = 3 bytes total
        assertThat(out.size(), equalTo(3L));
    }

    public void testBitpackLocalChosenWhenRangeIsNarrow() throws Exception {
        // NOTE: segment bits says 16; block uses ords in [1000, 1015] so local bits = 4
        long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = 1000L + randomIntBetween(0, 15);
        }
        int segmentBitsPerOrd = 16;

        AdaptiveOrdinalCodec codec = new AdaptiveOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, segmentBitsPerOrd);

        ByteBuffersDataInput peek = new ByteBuffersDataInput(out.toBufferList());
        assertEquals(BitpackCodec.MODE, peek.readByte());

        long[] decoded = new long[128];
        codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), decoded, segmentBitsPerOrd);
        assertArrayEquals(in, decoded);
    }

    public void testRleChosenForTwoRunBlock() throws Exception {
        long[] in = new long[128];
        Arrays.fill(in, 0, 80, 7L);
        Arrays.fill(in, 80, 128, 11L);
        int segmentBitsPerOrd = 16;

        AdaptiveOrdinalCodec codec = new AdaptiveOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, segmentBitsPerOrd);

        ByteBuffersDataInput peek = new ByteBuffersDataInput(out.toBufferList());
        assertEquals(RleCodec.MODE, peek.readByte());

        long[] decoded = new long[128];
        codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), decoded, segmentBitsPerOrd);
        assertArrayEquals(in, decoded);
    }

    public void testLegacyChosenForHighEntropyBlock() throws Exception {
        long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = randomLongBetween(0, (1L << 16) - 1);
        }
        AdaptiveOrdinalCodec codec = new AdaptiveOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, 16);
        ByteBuffersDataInput peek = new ByteBuffersDataInput(out.toBufferList());
        // NOTE: 128 random 16-bit ords spread across the full range -> LEGACY wins
        assertEquals(LegacyCodec.MODE, peek.readByte());
    }

    public void testBitpackLocalBeatsLegacyOnNarrowRangeWithManyDistinct() throws Exception {
        long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            // NOTE: 64 distinct values in [5000, 5063] - local bits = 6 vs segment bits = 16
            in[i] = 5000L + randomLongBetween(0, 63);
        }
        AdaptiveOrdinalCodec codec = new AdaptiveOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, 16);
        ByteBuffersDataInput peek = new ByteBuffersDataInput(out.toBufferList());
        assertEquals(BitpackCodec.MODE, peek.readByte());
    }

    public void testCorruptModeByteThrows() {
        AdaptiveOrdinalCodec codec = new AdaptiveOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        // NOTE: 99 is not any of LegacyCodec.MODE=0, ConstantCodec.MODE=1, RleCodec.MODE=2, BitpackCodec.MODE=3
        out.writeByte((byte) 99);
        long[] dst = new long[128];
        expectThrows(CorruptIndexException.class, () -> codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), dst, 16));
    }

    public void testRleRunOverflowThrows() throws Exception {
        AdaptiveOrdinalCodec codec = new AdaptiveOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        out.writeByte(RleCodec.MODE);
        out.writeVInt(1);          // NOTE: one run claimed
        out.writeVLong(42);
        out.writeVInt(200);        // NOTE: run length 200 overflows the 128-slot destination
        long[] dst = new long[128];
        expectThrows(CorruptIndexException.class, () -> codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), dst, 16));
    }
}
