/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.codec.profiler.ProfileCollector;
import org.elasticsearch.codec.profiler.TestCollectors;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class ProfiledChunkDecompressorTests extends ESTestCase {

    private static final byte SENTINEL = (byte) 0x5A;

    private record Call(IndexInput in, int storedLength, byte[] dst, int uncompressedLength) {}

    public void testTheDecoratorForwardsTheCallUnchangedAndLeavesTheBufferToTheDelegate() throws IOException {
        try (Directory dir = new ByteBuffersDirectory(); IndexInput in = emptyInput(dir)) {
            final List<Call> calls = new ArrayList<>();
            final ChunkDecompressor delegate = (input, stored, dst, inflated) -> {
                calls.add(new Call(input, stored, dst, inflated));
                for (int i = 0; i < inflated; i++) {
                    dst[i] = (byte) (stored + i);
                }
            };
            final ProfileCollector stats = TestCollectors.elapsed();
            final ChunkDecompressor profiled = new ProfiledChunkDecompressor(delegate, stats, ProfiledChunkDecompressor.CHUNKS);

            final byte[] buffer = new byte[64];
            for (int round = 0; round < 5; round++) {
                Arrays.fill(buffer, SENTINEL);
                final int stored = between(1, 40);
                final int inflated = between(1, 48);
                profiled.read(in, stored, buffer, inflated);

                final Call call = calls.get(round);
                assertThat(call.in(), sameInstance(in));
                assertThat("the buffer is the caller's, not a copy", call.dst(), sameInstance(buffer));
                assertThat(call.storedLength(), equalTo(stored));
                assertThat(call.uncompressedLength(), equalTo(inflated));
                for (int i = 0; i < buffer.length; i++) {
                    final byte expected = i < inflated ? (byte) (stored + i) : SENTINEL;
                    assertThat("byte " + i + " of round " + round, buffer[i], equalTo(expected));
                }
            }
            assertThat(calls.size(), equalTo(5));
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.chunks.decompressed"), equalTo(5L));
        }
    }

    public void testEveryChunkCountsItsStoredAndInflatedSize() throws IOException {
        try (Directory dir = new ByteBuffersDirectory(); IndexInput in = emptyInput(dir)) {
            final ProfileCollector stats = TestCollectors.countersOnly();
            final ChunkDecompressor profiled = new ProfiledChunkDecompressor(
                (input, stored, dst, inflated) -> {},
                stats,
                ProfiledChunkDecompressor.CHUNKS
            );
            long stored = 0;
            long inflated = 0;
            final int chunks = between(1, 30);
            for (int i = 0; i < chunks; i++) {
                final int s = between(1, 1000);
                final int u = between(s, 4000);
                profiled.read(in, s, new byte[u], u);
                stored += s;
                inflated += u;
            }
            assertThat(
                TestCollectors.toKeyedValues(stats.snapshot()),
                equalTo(
                    Map.of(
                        "ColumNAR.chunks.decompressed",
                        (long) chunks,
                        "ColumNAR.chunks.bytes_compressed",
                        stored,
                        "ColumNAR.chunks.bytes_inflated",
                        inflated
                    )
                )
            );
        }
    }

    public void testAFailingDelegateCountsNothingAndItsExceptionPropagatesUnchanged() throws IOException {
        try (Directory dir = new ByteBuffersDirectory(); IndexInput in = emptyInput(dir)) {
            final IOException failure = new IOException("corrupt chunk");
            final ProfileCollector stats = TestCollectors.elapsed();
            final boolean[] fail = { false };
            final ChunkDecompressor profiled = new ProfiledChunkDecompressor((input, stored, dst, inflated) -> {
                if (fail[0]) {
                    dst[0] = 1;
                    throw failure;
                }
            }, stats, ProfiledChunkDecompressor.CHUNKS);

            profiled.read(in, 10, new byte[20], 20);
            fail[0] = true;
            assertThat(expectThrows(IOException.class, () -> profiled.read(in, 10, new byte[20], 20)), sameInstance(failure));
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.chunks.decompressed"), equalTo(1L));
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.chunks.bytes_inflated"), equalTo(20L));

            fail[0] = false;
            profiled.read(in, 5, new byte[8], 8);
            assertThat(
                "the decorator still counts after a failure",
                TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.chunks.decompressed"),
                equalTo(2L)
            );
        }
    }

    public void testAMetricsSetWithoutStoredBytesRegistersNoStoredBytesCounter() {
        final ProfileCollector stats = TestCollectors.elapsed();
        final ProfiledChunkDecompressor.Metrics noStored = new ProfiledChunkDecompressor.Metrics(
            ProfiledChunkDecompressor.CHUNKS_DECOMPRESSED,
            null,
            ProfiledChunkDecompressor.CHUNK_BYTES_INFLATED,
            ProfiledChunkDecompressor.CHUNK_DECOMPRESS_TIME
        );
        new ProfiledChunkDecompressor((input, stored, dst, inflated) -> {}, stats, noStored);
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()), not(hasKey("ColumNAR.chunks.bytes_compressed")));
        assertThat(TestCollectors.toKeyedValues(stats.snapshot()), hasKey("ColumNAR.chunks.decompressed"));
    }

    public void testTheChunkTimeCoversTheDecompressorCallAndNothingAround() throws IOException {
        try (Directory dir = new ByteBuffersDirectory(); IndexInput in = emptyInput(dir)) {
            final long[] now = { 5 };
            final ProfileCollector stats = TestCollectors.withClock(() -> now[0]);
            final ChunkDecompressor profiled = new ProfiledChunkDecompressor(
                (input, stored, dst, inflated) -> now[0] += 40,
                stats,
                ProfiledChunkDecompressor.CHUNKS
            );
            now[0] += 1_000;
            profiled.read(in, 10, new byte[20], 20);
            now[0] += 2_000;
            profiled.read(in, 10, new byte[20], 20);
            assertThat(
                "only the two delegate calls",
                TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.chunks.decompress_time_nanos"),
                equalTo(80L)
            );
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.chunks.decompressed"), equalTo(2L));
        }
    }

    public void testAFailingDecompressorIsTimedForTheCallThatThrewAndCountsNoChunk() throws IOException {
        try (Directory dir = new ByteBuffersDirectory(); IndexInput in = emptyInput(dir)) {
            final long[] now = { 0 };
            final IOException failure = new IOException("corrupt chunk");
            final boolean[] fail = { true };
            final ProfileCollector stats = TestCollectors.withClock(() -> now[0]);
            final ChunkDecompressor profiled = new ProfiledChunkDecompressor((input, stored, dst, inflated) -> {
                now[0] += 25;
                if (fail[0]) {
                    throw failure;
                }
            }, stats, ProfiledChunkDecompressor.CHUNKS);

            assertThat(expectThrows(IOException.class, () -> profiled.read(in, 10, new byte[20], 20)), sameInstance(failure));
            assertThat(
                "the call that threw took 25",
                TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.chunks.decompress_time_nanos"),
                equalTo(25L)
            );
            assertThat(
                "no chunk was inflated",
                TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.chunks.decompressed"),
                equalTo(0L)
            );

            fail[0] = false;
            profiled.read(in, 10, new byte[20], 20);
            assertThat(
                "the timer was closed, so the next call is timed",
                TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.chunks.decompress_time_nanos"),
                equalTo(50L)
            );
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.chunks.decompressed"), equalTo(1L));
        }
    }

    public void testACollectorThatMeasuresNoTimeReadsNoClockAndReportsNoTimeMetric() throws IOException {
        try (Directory dir = new ByteBuffersDirectory(); IndexInput in = emptyInput(dir)) {
            final ProfileCollector stats = TestCollectors.countersOnly();
            final ChunkDecompressor profiled = new ProfiledChunkDecompressor(
                (input, stored, dst, inflated) -> {},
                stats,
                ProfiledChunkDecompressor.CHUNKS
            );
            profiled.read(in, 10, new byte[20], 20);
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.chunks.decompressed"), equalTo(1L));
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()), not(hasKey("ColumNAR.chunks.decompress_time_nanos")));
        }
    }

    public void testTwoDecoratorsOnOneCollectorShareOneTimeAccumulator() throws IOException {
        try (Directory dir = new ByteBuffersDirectory(); IndexInput in = emptyInput(dir)) {
            final long[] now = { 0 };
            final ProfileCollector stats = TestCollectors.withClock(() -> now[0]);
            final ChunkDecompressor first = new ProfiledChunkDecompressor(
                (input, stored, dst, inflated) -> now[0] += 10,
                stats,
                ProfiledChunkDecompressor.CHUNKS
            );
            final ChunkDecompressor second = new ProfiledChunkDecompressor(
                (input, stored, dst, inflated) -> now[0] += 7,
                stats,
                ProfiledChunkDecompressor.CHUNKS
            );
            first.read(in, 1, new byte[1], 1);
            second.read(in, 1, new byte[1], 1);
            first.read(in, 1, new byte[1], 1);
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.chunks.decompress_time_nanos"), equalTo(27L));
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.chunks.decompressed"), equalTo(3L));
        }
    }

    private static IndexInput emptyInput(Directory dir) throws IOException {
        try (IndexOutput out = dir.createOutput("chunk", IOContext.DEFAULT)) {
            out.writeByte((byte) 0);
        }
        return dir.openInput("chunk", IOContext.DEFAULT);
    }
}
