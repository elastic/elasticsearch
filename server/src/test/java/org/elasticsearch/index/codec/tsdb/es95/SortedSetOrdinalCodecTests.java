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
 * Round-trip and encoding-selection tests for {@link SortedSetOrdinalCodec}, the SORTED_SET
 * wrapper that adds {@link TupleRunCodec} as a sixth candidate on top of the shared
 * scalar dispatch. Tests build the perDocK array alongside the ord block so the encoder
 * sees realistic per-doc tuple boundaries.
 */
public class SortedSetOrdinalCodecTests extends ESTestCase {

    public void testTupleRunChosenForMultiValuedCycle() throws Exception {
        int bitsPerOrd = 16;
        long[] tuple = { 17L, 4242L, 65000L };
        int K = tuple.length;
        long[] in = new long[128];
        int[] perDocK = new int[129];
        int numDocs = 0;
        int pos = 0;
        for (int d = 0; d < 42; d++) {
            perDocK[numDocs++] = K;
            for (long o : tuple)
                in[pos++] = o;
        }
        perDocK[numDocs++] = K;
        in[pos++] = tuple[0];
        in[pos++] = tuple[1];
        int tailMissing = 1;
        assertEquals(128, pos);

        SortedSetOrdinalCodec codec = new SortedSetOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), perDocK, numDocs, 0, tailMissing, out, bitsPerOrd);

        ByteBuffersDataInput peek = new ByteBuffersDataInput(out.toBufferList());
        assertEquals(SortedSetOrdinalCodec.ADAPTIVE_EXTRA_ENCODING, Long.numberOfTrailingZeros(~peek.readVLong()));
        assertEquals(TupleRunCodec.SUB_MODE, peek.readByte());

        long[] decoded = new long[128];
        codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), decoded, bitsPerOrd);
        assertArrayEquals(in, decoded);
    }

    public void testTupleRunBeatsBitPackedOnScatteredValues() throws Exception {
        // NOTE: cycle ords spread across the full 16-bit range so BITPACK_LOCAL cannot help.
        int bitsPerOrd = 16;
        int K = 3;
        long mask = (1L << bitsPerOrd) - 1L;
        long[] tuple = new long[K];
        for (int i = 0; i < K; i++) {
            tuple[i] = (mask / K) * i;
        }
        long[] in = new long[128];
        int[] perDocK = new int[129];
        int numDocs = 0;
        int pos = 0;
        for (int d = 0; d < 42; d++) {
            perDocK[numDocs++] = K;
            for (long o : tuple)
                in[pos++] = o;
        }
        perDocK[numDocs++] = K;
        in[pos++] = tuple[0];
        in[pos++] = tuple[1];
        int tailMissing = 1;

        SortedSetOrdinalCodec codec = new SortedSetOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), perDocK, numDocs, 0, tailMissing, out, bitsPerOrd);

        assertTrue("tuple-run payload must beat the bit-packed lower bound", out.size() < 64);
    }

    public void testConstChosenForUniformBlock() throws Exception {
        long[] in = new long[128];
        Arrays.fill(in, 42L);
        int[] perDocK = new int[129];
        for (int d = 0; d < 128; d++)
            perDocK[d] = 1;

        SortedSetOrdinalCodec codec = new SortedSetOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), perDocK, 128, 0, 0, out, 16);

        assertEquals(ConstantCodec.ENCODING, peekEncoding(out));

        long[] decoded = new long[128];
        codec.decodeOrdinals(new ByteBuffersDataInput(out.toBufferList()), decoded, 16);
        assertArrayEquals(in, decoded);
    }

    public void testTwoRunChosen() throws Exception {
        long[] in = new long[128];
        Arrays.fill(in, 0, 80, 7L);
        Arrays.fill(in, 80, 128, 11L);
        int[] perDocK = new int[129];
        for (int d = 0; d < 128; d++)
            perDocK[d] = 1;

        SortedSetOrdinalCodec codec = new SortedSetOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), perDocK, 128, 0, 0, out, 16);

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
        int[] perDocK = new int[129];
        for (int d = 0; d < 128; d++)
            perDocK[d] = 1;

        SortedSetOrdinalCodec codec = new SortedSetOrdinalCodec(128);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        codec.encodeOrdinals(Arrays.copyOf(in, in.length), perDocK, 128, 0, 0, out, 16);

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
