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

/**
 * Round-trip and encoding-selection tests for {@link SortedSetOrdinalCodec},
 * the SORTED_SET wrapper that extends the per-mode dispatch with
 * {@link CycleCodec} on top of the SORTED candidates.
 *
 * <p>Mirrors {@link SortedOrdinalCodecTests} for the shared candidates; the
 * extra coverage focuses on the cycle path: a block constructed as a K-period
 * cycle of values scattered across the bit range must select
 * {@link CycleCodec}, beat {@link BitPackedCodec} by an order of magnitude,
 * and decode back byte-for-byte.
 */
public class SortedSetOrdinalCodecTests extends ESTestCase {

    public void testCycleEncodingChosenForMultiValuedPattern() throws Exception {
        int bitsPerOrd = 16;
        long[] cycleValues = { 17L, 4242L, 65000L };
        long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = cycleValues[i % cycleValues.length];
        }

        SortedSetOrdinalCodec codec = new SortedSetOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, bitsPerOrd);

        ByteBuffersDataInput peek = new ByteBuffersDataInput(out.toBufferList());
        assertEquals(SortedSetOrdinalCodec.ADAPTIVE_EXTRA_ENCODING, Long.numberOfTrailingZeros(~peek.readVLong()));
        assertEquals(CycleCodec.SUB_MODE, peek.readByte());

        long[] decoded = new long[128];
        codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), decoded, bitsPerOrd);
        assertArrayEquals(in, decoded);
    }

    public void testCycleBeatsBitPackedOnScatteredValues() throws Exception {
        // NOTE: cycle values are spread across the full 16-bit range to model multi-valued
        // SortedSet fields whose K distinct ords sort far apart in the term dictionary;
        // BitpackCodec local range -> full 16 bits -> ~257 bytes; CycleCodec -> ~12 bytes.
        int bitsPerOrd = 16;
        int period = 3;
        long mask = (1L << bitsPerOrd) - 1L;
        long[] cycleValues = new long[period];
        for (int i = 0; i < period; i++) {
            cycleValues[i] = (mask / period) * i;
        }
        long[] in = new long[128];
        for (int i = 0; i < in.length; i++) {
            in[i] = cycleValues[i % period];
        }

        SortedSetOrdinalCodec codec = new SortedSetOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, bitsPerOrd);

        assertTrue("cycle payload must beat the bit-packed lower bound", out.size() < 64);
    }

    public void testConstEncodingChosenForUniformBlock() throws Exception {
        long[] in = new long[128];
        Arrays.fill(in, 42L);

        SortedSetOrdinalCodec codec = new SortedSetOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, 16);

        assertEquals(ConstantCodec.ENCODING, peekEncoding(out));

        long[] decoded = new long[128];
        codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), decoded, 16);
        assertArrayEquals(in, decoded);
    }

    public void testTwoRunEncodingChosen() throws Exception {
        long[] in = new long[128];
        Arrays.fill(in, 0, 80, 7L);
        Arrays.fill(in, 80, 128, 11L);

        SortedSetOrdinalCodec codec = new SortedSetOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, 16);

        assertEquals(TwoRunCodec.ENCODING, peekEncoding(out));

        long[] decoded = new long[128];
        codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), decoded, 16);
        assertArrayEquals(in, decoded);
    }

    public void testRleChosenForManyShortRuns() throws Exception {
        long[] in = new long[128];
        Arrays.fill(in, 0, 32, 7L);
        Arrays.fill(in, 32, 64, 11L);
        Arrays.fill(in, 64, 96, 13L);
        Arrays.fill(in, 96, 128, 17L);

        SortedSetOrdinalCodec codec = new SortedSetOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), out, 16);

        ByteBuffersDataInput peek = new ByteBuffersDataInput(out.toBufferList());
        assertEquals(SortedSetOrdinalCodec.ADAPTIVE_EXTRA_ENCODING, Long.numberOfTrailingZeros(~peek.readVLong()));
        assertEquals(RleCodec.SUB_MODE, peek.readByte());

        long[] decoded = new long[128];
        codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), decoded, 16);
        assertArrayEquals(in, decoded);
    }

    public void testCorruptEncodingThrows() throws Exception {
        SortedSetOrdinalCodec codec = new SortedSetOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        out.writeVLong(0b11111L);
        long[] dst = new long[128];
        expectThrows(CorruptIndexException.class, () -> codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), dst, 16));
    }

    public void testCorruptAdaptiveExtraSubModeThrows() throws Exception {
        SortedSetOrdinalCodec codec = new SortedSetOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        out.writeVLong(0b111L);
        out.writeByte((byte) 99);
        long[] dst = new long[128];
        expectThrows(CorruptIndexException.class, () -> codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), dst, 16));
    }

    private static int peekEncoding(final ByteBuffersDataOutput out) throws Exception {
        return Long.numberOfTrailingZeros(~new ByteBuffersDataInput(out.toBufferList()).readVLong());
    }
}
