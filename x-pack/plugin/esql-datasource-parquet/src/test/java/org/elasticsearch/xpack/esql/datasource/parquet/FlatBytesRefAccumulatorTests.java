/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DirectBytesRefVector;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for {@link FlatBytesRefAccumulator}: chunks decoded at the reserved tail and committed with tail-relative
 * ends come back in order, across growth, after a materialized constant run, and when moved to a {@link BytesRefArray}.
 */
public class FlatBytesRefAccumulatorTests extends ESTestCase {

    private final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(NoopCircuitBreaker.INSTANCE)
        .build();

    public void testChunksAcrossGrowthBuildDirectVector() {
        List<BytesRef> expected = new ArrayList<>();
        int chunks = randomIntBetween(1, 5);
        List<List<BytesRef>> perChunk = new ArrayList<>();
        int total = 0;
        for (int c = 0; c < chunks; c++) {
            List<BytesRef> chunk = randomValues(randomIntBetween(1, 20));
            perChunk.add(chunk);
            expected.addAll(chunk);
            total += chunk.size();
        }
        FlatBytesRefAccumulator flat = new FlatBytesRefAccumulator(total, Long.MAX_VALUE);
        for (List<BytesRef> chunk : perChunk) {
            decodeAndCommit(flat, chunk);
        }
        assertEquals(total, flat.count());
        assertEquals(expected.stream().mapToInt(b -> b.length).sum(), flat.length());
        try (BytesRefVector vector = flat.build(blockFactory)) {
            assertThat(vector, instanceOf(DirectBytesRefVector.class));
            assertValues(vector, expected);
        }
    }

    /** A chunk reserved and decoded but not committed leaves the accumulator as it was. */
    public void testUncommittedTailIsScratch() {
        FlatBytesRefAccumulator flat = new FlatBytesRefAccumulator(4, Long.MAX_VALUE);
        decode(flat, List.of(new BytesRef("dropped")), new int[1]);
        assertTrue(flat.isEmpty());
        assertEquals(0, flat.length());
        List<BytesRef> kept = List.of(new BytesRef("a"), new BytesRef("bc"));
        decodeAndCommit(flat, kept);
        try (BytesRefVector vector = flat.build(blockFactory)) {
            assertValues(vector, kept);
        }
    }

    public void testCommitAfterConstantRun() {
        BytesRef constant = new BytesRef(randomAlphaOfLengthBetween(0, 5));
        int times = randomIntBetween(1, 10);
        List<BytesRef> chunk = randomValues(randomIntBetween(1, 10));
        FlatBytesRefAccumulator flat = new FlatBytesRefAccumulator(times + chunk.size(), Long.MAX_VALUE);
        int[] ends = new int[chunk.size()];
        decode(flat, chunk, ends);
        flat.commitAfterConstantRun(constant, times, ends, chunk.size());

        List<BytesRef> expected = new ArrayList<>();
        for (int i = 0; i < times; i++) {
            expected.add(constant);
        }
        expected.addAll(chunk);
        try (BytesRefVector vector = flat.build(blockFactory)) {
            assertValues(vector, expected);
        }
    }

    /** The cap counts payload bytes plus the offsets array at full capacity. */
    public void testFitsCountsOffsetsCapacity() {
        int maxValues = randomIntBetween(1, 100);
        long offsets = (maxValues + 1L) * Integer.BYTES;
        long payload = randomIntBetween(2, 1000);
        FlatBytesRefAccumulator flat = new FlatBytesRefAccumulator(maxValues, offsets + payload);
        assertTrue(flat.fits(payload));
        assertFalse(flat.fits(payload + 1));
        decodeAndCommit(flat, List.of(new BytesRef("xy")));
        assertTrue(flat.fits(payload - 2));
        assertFalse(flat.fits(payload - 1));
    }

    public void testAppendToBytesRefArray() {
        List<BytesRef> values = randomValues(randomIntBetween(1, 30));
        FlatBytesRefAccumulator flat = new FlatBytesRefAccumulator(values.size(), Long.MAX_VALUE);
        decodeAndCommit(flat, values);
        try (BytesRefArray array = new BytesRefArray(values.size(), BigArrays.NON_RECYCLING_INSTANCE)) {
            flat.appendTo(array);
            assertEquals(values.size(), (int) array.size());
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < values.size(); i++) {
                assertEquals(values.get(i), array.get(i, scratch));
            }
        }
    }

    private static List<BytesRef> randomValues(int count) {
        List<BytesRef> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            values.add(new BytesRef(randomUnicodeOfLengthBetween(0, 8).getBytes(StandardCharsets.UTF_8)));
        }
        return values;
    }

    /** Writes {@code values} at the reserved tail the way {@link PlainValueDecoder#copyPlainPayloads} does. */
    private static void decode(FlatBytesRefAccumulator flat, List<BytesRef> values, int[] ends) {
        int chunkBytes = values.stream().mapToInt(b -> b.length).sum();
        int off = flat.reserve(chunkBytes);
        byte[] bytes = flat.bytes();
        int d = off;
        for (int i = 0; i < values.size(); i++) {
            BytesRef v = values.get(i);
            System.arraycopy(v.bytes, v.offset, bytes, d, v.length);
            d += v.length;
            ends[i] = d - off;
        }
    }

    private static void decodeAndCommit(FlatBytesRefAccumulator flat, List<BytesRef> values) {
        int[] ends = new int[values.size()];
        decode(flat, values, ends);
        flat.commit(ends, values.size());
    }

    private static void assertValues(BytesRefVector vector, List<BytesRef> expected) {
        assertEquals(expected.size(), vector.getPositionCount());
        BytesRef scratch = new BytesRef();
        for (int i = 0; i < expected.size(); i++) {
            assertEquals("value " + i, expected.get(i), vector.getBytesRef(i, scratch));
        }
    }
}
