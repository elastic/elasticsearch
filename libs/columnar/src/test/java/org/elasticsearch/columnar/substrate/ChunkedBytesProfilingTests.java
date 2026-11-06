/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.codec.profiler.ProfileCollector;
import org.elasticsearch.codec.profiler.TestCollectors;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ChunkedBytesProfilingTests extends ESTestCase {

    private static final String CHUNKS = "chunks.bin";
    private static final String NAVIGATION = "chunks.nav";

    private record Stream(ChunkIndexMetadata index, long[] offsets, long[] starts, long[] fileOffsets) {
        int values() {
            return offsets.length - 1;
        }

        int length(int value) {
            return (int) (offsets[value + 1] - offsets[value]);
        }
    }

    private static final class Replay {
        private final long[] starts;
        private final long[] fileOffsets;
        private int cached = -1;
        private long visits;
        private long cacheHits;
        private long chunks;
        private long compressed;
        private long inflated;

        Replay(Stream stream) {
            this.starts = stream.starts;
            this.fileOffsets = stream.fileOffsets;
        }

        void read(long offset, int length) {
            if (length == 0) {
                return;
            }
            final int last = chunkOf(offset + length - 1);
            for (int chunk = chunkOf(offset); chunk <= last; chunk++) {
                visits++;
                if (chunk == cached) {
                    cacheHits++;
                } else {
                    cached = chunk;
                    chunks++;
                    compressed += fileOffsets[chunk + 1] - fileOffsets[chunk];
                    inflated += starts[chunk + 1] - starts[chunk];
                }
            }
        }

        private int chunkOf(long position) {
            int low = 0;
            int high = starts.length - 2;
            while (low < high) {
                final int mid = (low + high + 1) >>> 1;
                if (starts[mid] <= position) {
                    low = mid;
                } else {
                    high = mid - 1;
                }
            }
            return low;
        }

        Map<String, Long> expected() {
            return Map.of(
                "ColumNAR.chunks.decompressed",
                chunks,
                "ColumNAR.chunks.cache_hits",
                cacheHits,
                "ColumNAR.chunks.bytes_compressed",
                compressed,
                "ColumNAR.chunks.bytes_inflated",
                inflated
            );
        }
    }

    public void testFullScanInflatesEveryChunkOnce() throws IOException {
        final int chunkBytes = randomChunkBytes();
        // NOTE: The values fill at least three chunks, so the scan crosses chunk boundaries whatever the chunk size.
        List<byte[]> values = randomValues(between(200, 3000));
        while (values.stream().mapToInt(value -> value.length).sum() < 3 * chunkBytes) {
            values = randomValues(values.size() * 2);
        }
        try (Directory dir = newDirectory()) {
            final Stream stream = write(dir, ChunkCodec.ZSTD, chunkBytes, values);
            final ProfileCollector stats = TestCollectors.countersOnly();
            final Replay replay = new Replay(stream);
            try (IndexInput in = dir.openInput(CHUNKS, IOContext.DEFAULT); IndexInput nav = dir.openInput(NAVIGATION, IOContext.DEFAULT)) {
                final ChunkedBytesReader reader = stream.index.open(new ColumnInputs(in, null, null, nav, stats));
                byte[] scratch = new byte[0];
                for (int value = 0; value < stream.values(); value++) {
                    scratch = reader.read(stream.offsets[value], stream.length(value), scratch);
                    replay.read(stream.offsets[value], stream.length(value));
                }
            }
            final int numChunks = stream.index.numChunks();
            assertThat(numChunks, greaterThan(1));
            assertThat(
                TestCollectors.toKeyedValues(stats.snapshot()),
                equalTo(
                    Map.of(
                        "ColumNAR.chunks.cache_hits",
                        replay.cacheHits,
                        "ColumNAR.chunks.decompressed",
                        (long) numChunks,
                        "ColumNAR.chunks.bytes_compressed",
                        stream.fileOffsets[numChunks] - stream.fileOffsets[0],
                        "ColumNAR.chunks.bytes_inflated",
                        stream.offsets[stream.values()]
                    )
                )
            );
        }
    }

    public void testRandomReadsMatchTheReplayOfTheChunkTable() throws IOException {
        final List<byte[]> values = randomValues(between(200, 3000));
        try (Directory dir = newDirectory()) {
            final Stream stream = write(dir, ChunkCodec.ZSTD, randomChunkBytes(), values);
            final ProfileCollector stats = TestCollectors.countersOnly();
            final Replay replay = new Replay(stream);
            try (IndexInput in = dir.openInput(CHUNKS, IOContext.DEFAULT); IndexInput nav = dir.openInput(NAVIGATION, IOContext.DEFAULT)) {
                final ChunkedBytesReader reader = stream.index.open(new ColumnInputs(in, null, null, nav, stats));
                byte[] scratch = new byte[0];
                for (int probe = 0; probe < 2000; probe++) {
                    final int value = randomIntBetween(0, stream.values() - 1);
                    scratch = reader.read(stream.offsets[value], stream.length(value), scratch);
                    replay.read(stream.offsets[value], stream.length(value));
                }
            }
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()), equalTo(replay.expected()));
        }
    }

    public void testReadsSpanningChunksAreCountedPerChunk() throws IOException {
        final List<byte[]> values = randomValues(between(200, 1000));
        try (Directory dir = newDirectory()) {
            final Stream stream = write(dir, ChunkCodec.ZSTD, randomIntBetween(16, 128), values);
            final ProfileCollector stats = TestCollectors.countersOnly();
            final Replay replay = new Replay(stream);
            final long total = stream.offsets[stream.values()];
            try (IndexInput in = dir.openInput(CHUNKS, IOContext.DEFAULT); IndexInput nav = dir.openInput(NAVIGATION, IOContext.DEFAULT)) {
                final ChunkedBytesReader reader = stream.index.open(new ColumnInputs(in, null, null, nav, stats));
                byte[] scratch = new byte[0];
                for (int probe = 0; probe < 300; probe++) {
                    final int length = randomIntBetween(1, (int) Math.min(total, 600));
                    final long offset = randomLongBetween(0, total - length);
                    scratch = reader.read(offset, length, scratch);
                    replay.read(offset, length);
                }
            }
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()), equalTo(replay.expected()));
        }
    }

    public void testProfilingDoesNotChangeTheBytesRead() throws IOException {
        final List<byte[]> values = randomValues(between(200, 2000));
        try (Directory dir = newDirectory()) {
            final Stream stream = write(dir, ChunkCodec.ZSTD, randomChunkBytes(), values);
            try (IndexInput in = dir.openInput(CHUNKS, IOContext.DEFAULT); IndexInput nav = dir.openInput(NAVIGATION, IOContext.DEFAULT)) {
                final ChunkedBytesReader plain = stream.index.open(new ColumnInputs(in, null, null, nav, null));
                final ChunkedBytesReader profiled = stream.index.open(new ColumnInputs(in, null, null, nav, TestCollectors.countersOnly()));
                byte[] plainScratch = new byte[0];
                byte[] profiledScratch = new byte[0];
                for (int probe = 0; probe < 1000; probe++) {
                    final int value = randomIntBetween(0, stream.values() - 1);
                    final int length = stream.length(value);
                    plainScratch = plain.read(stream.offsets[value], length, plainScratch);
                    profiledScratch = profiled.read(stream.offsets[value], length, profiledScratch);
                    assertArrayEquals(Arrays.copyOf(plainScratch, length), Arrays.copyOf(profiledScratch, length));
                    assertArrayEquals(values.get(value), Arrays.copyOf(profiledScratch, length));
                }
            }
        }
    }

    public void testEveryChunkAReadNeedsIsEitherInflatedOrReused() throws IOException {
        final List<byte[]> values = randomValues(between(200, 3000));
        try (Directory dir = newDirectory()) {
            final Stream stream = write(dir, ChunkCodec.ZSTD, randomChunkBytes(), values);
            final ProfileCollector stats = TestCollectors.countersOnly();
            final Replay replay = new Replay(stream);
            try (IndexInput in = dir.openInput(CHUNKS, IOContext.DEFAULT); IndexInput nav = dir.openInput(NAVIGATION, IOContext.DEFAULT)) {
                final ChunkedBytesReader reader = stream.index.open(new ColumnInputs(in, null, null, nav, stats));
                byte[] scratch = new byte[0];
                for (int probe = 0; probe < 1500; probe++) {
                    final long offset = randomLongBetween(0, stream.offsets[stream.values()] - 1);
                    final int length = (int) Math.min(randomIntBetween(1, 4 * 1024), stream.offsets[stream.values()] - offset);
                    scratch = reader.read(offset, length, scratch);
                    replay.read(offset, length);
                }
            }
            final Map<String, Long> counted = TestCollectors.toKeyedValues(stats.snapshot());
            assertThat(
                "chunks needed by the reads, counted by the replay",
                replay.visits,
                equalTo(counted.get("ColumNAR.chunks.decompressed") + counted.get("ColumNAR.chunks.cache_hits"))
            );
            assertThat(counted.get("ColumNAR.chunks.cache_hits"), equalTo(replay.cacheHits));
        }
    }

    public void testAReadSpanningTwoChunksNeedsEachOnceAndIsNotCountedAsAReuse() throws IOException {
        final List<byte[]> values = randomValues(between(2000, 4000));
        try (Directory dir = newDirectory()) {
            final Stream stream = write(dir, ChunkCodec.ZSTD, 4096, values);
            assertThat(stream.index.numChunks(), greaterThan(2));
            final ProfileCollector stats = TestCollectors.countersOnly();
            try (IndexInput in = dir.openInput(CHUNKS, IOContext.DEFAULT); IndexInput nav = dir.openInput(NAVIGATION, IOContext.DEFAULT)) {
                final ChunkedBytesReader reader = stream.index.open(new ColumnInputs(in, null, null, nav, stats));
                final long boundary = stream.starts[1];
                byte[] scratch = reader.read(boundary - 4, 8, new byte[0]);
                assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.chunks.decompressed"), equalTo(2L));
                assertThat(
                    "the first chunk of a spanning read is not looked up twice",
                    TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.chunks.cache_hits"),
                    equalTo(0L)
                );
                scratch = reader.read(boundary + 1, 4, scratch);
                assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.chunks.decompressed"), equalTo(2L));
                assertThat(
                    "the next read is inside the chunk the reader holds",
                    TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.chunks.cache_hits"),
                    equalTo(1L)
                );
            }
        }
    }

    public void testIdentityChunksReportVerbatimReadsAndNoChunkMetrics() throws IOException {
        final List<byte[]> values = randomValues(between(50, 500));
        try (Directory dir = newDirectory()) {
            final Stream stream = write(dir, ChunkCodec.IDENTITY, randomChunkBytes(), values);
            final ProfileCollector stats = TestCollectors.countersOnly();
            long bytes = 0;
            try (IndexInput in = dir.openInput(CHUNKS, IOContext.DEFAULT); IndexInput nav = dir.openInput(NAVIGATION, IOContext.DEFAULT)) {
                final ChunkedBytesReader reader = stream.index.open(new ColumnInputs(in, null, null, nav, stats));
                byte[] scratch = new byte[0];
                for (int value = 0; value < stream.values(); value++) {
                    scratch = reader.read(stream.offsets[value], stream.length(value), scratch);
                    bytes += stream.length(value);
                }
            }
            assertThat(
                "every read is one bulk read off the file, and no chunk is ever inflated",
                TestCollectors.toKeyedValues(stats.snapshot()),
                equalTo(Map.of("ColumNAR.uncompressed.reads", (long) stream.values(), "ColumNAR.uncompressed.bytes_read", bytes))
            );
        }
    }

    public void testVerbatimReadsAreTimedWhenTheCollectorMeasuresTime() throws IOException {
        final List<byte[]> values = randomValues(between(50, 500));
        try (Directory dir = newDirectory()) {
            final Stream stream = write(dir, ChunkCodec.IDENTITY, randomChunkBytes(), values);
            final long step = between(2, 50);
            final long[] now = { 0 };
            final ProfileCollector stats = TestCollectors.withClock(() -> now[0] += step);
            try (IndexInput in = dir.openInput(CHUNKS, IOContext.DEFAULT); IndexInput nav = dir.openInput(NAVIGATION, IOContext.DEFAULT)) {
                final ChunkedBytesReader reader = stream.index.open(new ColumnInputs(in, null, null, nav, stats));
                byte[] scratch = new byte[0];
                for (int value = 0; value < stream.values(); value++) {
                    scratch = reader.read(stream.offsets[value], stream.length(value), scratch);
                }
            }
            assertThat(
                "each verbatim read is one call of one step",
                TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.uncompressed.read_time_nanos"),
                equalTo(step * stream.values())
            );
        }
    }

    public void testAZstdStreamReportsNoVerbatimMetrics() throws IOException {
        final List<byte[]> values = randomValues(between(50, 500));
        try (Directory dir = newDirectory()) {
            final Stream stream = write(dir, ChunkCodec.ZSTD, randomChunkBytes(), values);
            final ProfileCollector stats = TestCollectors.countersOnly();
            try (IndexInput in = dir.openInput(CHUNKS, IOContext.DEFAULT); IndexInput nav = dir.openInput(NAVIGATION, IOContext.DEFAULT)) {
                final ChunkedBytesReader reader = stream.index.open(new ColumnInputs(in, null, null, nav, stats));
                reader.read(stream.offsets[0], stream.length(0), new byte[0]);
                reader.span(stream.offsets[1], stream.length(1), new BytesRef());
                reader.transfer(0, stream.offsets[stream.values()], (bytes, offset, length) -> {});
            }
            for (String name : TestCollectors.toKeyedValues(stats.snapshot()).keySet()) {
                assertFalse("a compressed stream never counts a verbatim metric, but reported " + name, name.contains("verbatim"));
            }
        }
    }

    public void testASingleChunkIdentitySpanCountsOneVerbatimReadOfTheChunk() throws IOException {
        final List<byte[]> values = randomValues(between(5, 50));
        try (Directory dir = newDirectory()) {
            final Stream stream = write(dir, ChunkCodec.IDENTITY, 32 * 1024, values);
            assertThat("the stream fits one chunk", stream.index.numChunks(), equalTo(1));
            final long step = between(2, 50);
            final long[] now = { 0 };
            final ProfileCollector stats = TestCollectors.withClock(() -> now[0] += step);
            final long chunkBytes = stream.offsets[stream.values()];
            final Map<String, Long> afterOneLoad = Map.of(
                "ColumNAR.uncompressed.reads",
                1L,
                "ColumNAR.uncompressed.bytes_read",
                chunkBytes,
                "ColumNAR.uncompressed.read_time_nanos",
                step
            );
            try (IndexInput in = dir.openInput(CHUNKS, IOContext.DEFAULT); IndexInput nav = dir.openInput(NAVIGATION, IOContext.DEFAULT)) {
                final ChunkedBytesReader reader = stream.index.open(new ColumnInputs(in, null, null, nav, stats));
                final BytesRef dst = new BytesRef();
                reader.span(stream.offsets[0], stream.length(0), dst);
                assertArrayEquals(values.get(0), Arrays.copyOfRange(dst.bytes, dst.offset, dst.offset + dst.length));
                assertThat(
                    "loading the held chunk is one verbatim read of its whole length",
                    TestCollectors.toKeyedValues(stats.snapshot()),
                    equalTo(afterOneLoad)
                );
                for (int value = 0; value < stream.values(); value++) {
                    reader.span(stream.offsets[value], stream.length(value), dst);
                    assertArrayEquals(values.get(value), Arrays.copyOfRange(dst.bytes, dst.offset, dst.offset + dst.length));
                }
                assertThat(
                    "a span served from the held chunk reads nothing, and an identity stream has no chunk_reuses",
                    TestCollectors.toKeyedValues(stats.snapshot()),
                    equalTo(afterOneLoad)
                );
            }
        }
    }

    public void testEveryIdentityPathCountsThroughTheSameVerbatimMetrics() throws IOException {
        final ProfileCollector stats = TestCollectors.countersOnly();
        long reads = 0;
        long bytes = 0;
        try (Directory multiDir = newDirectory(); Directory singleDir = newDirectory()) {
            final Stream multi = write(multiDir, ChunkCodec.IDENTITY, 64, randomValues(between(200, 800)));
            assertThat(multi.index.numChunks(), greaterThan(1));
            final Stream single = write(singleDir, ChunkCodec.IDENTITY, 32 * 1024, randomValues(between(5, 50)));
            assertThat(single.index.numChunks(), equalTo(1));
            try (
                IndexInput multiIn = multiDir.openInput(CHUNKS, IOContext.DEFAULT);
                IndexInput multiNav = multiDir.openInput(NAVIGATION, IOContext.DEFAULT);
                IndexInput singleIn = singleDir.openInput(CHUNKS, IOContext.DEFAULT);
                IndexInput singleNav = singleDir.openInput(NAVIGATION, IOContext.DEFAULT)
            ) {
                final ChunkedBytesReader spread = multi.index.open(new ColumnInputs(multiIn, null, null, multiNav, stats));
                spread.read(multi.offsets[0], multi.length(0), new byte[0]);
                reads += 1;
                bytes += multi.length(0);
                spread.span(multi.offsets[1], multi.length(1), new BytesRef());
                reads += 1;
                bytes += multi.length(1);
                final long total = multi.offsets[multi.values()];
                spread.transfer(0, total, (chunk, offset, length) -> {});
                reads += (total + 32 * 1024 - 1) / (32 * 1024);
                bytes += total;
                final ChunkedBytesReader held = single.index.open(new ColumnInputs(singleIn, null, null, singleNav, stats));
                held.span(single.offsets[0], single.length(0), new BytesRef());
                reads += 1;
                bytes += single.offsets[single.values()];
            }
            assertThat(
                "a direct read, a spread span, a transfer and a held-chunk load all count into the same metrics",
                TestCollectors.toKeyedValues(stats.snapshot()),
                equalTo(Map.of("ColumNAR.uncompressed.reads", reads, "ColumNAR.uncompressed.bytes_read", bytes))
            );
        }
    }

    public void testCountersOnlyAndTimedCollectorsAgreeOnEveryVerbatimCount() throws IOException {
        final List<byte[]> values = randomValues(between(100, 500));
        try (Directory dir = newDirectory()) {
            final Stream stream = write(dir, ChunkCodec.IDENTITY, randomChunkBytes(), values);
            final ProfileCollector countersOnly = TestCollectors.countersOnly();
            final ProfileCollector timed = TestCollectors.elapsed();
            try (IndexInput in = dir.openInput(CHUNKS, IOContext.DEFAULT); IndexInput nav = dir.openInput(NAVIGATION, IOContext.DEFAULT)) {
                for (ProfileCollector stats : List.of(countersOnly, timed)) {
                    final ChunkedBytesReader reader = stream.index.open(new ColumnInputs(in, null, null, nav, stats));
                    byte[] scratch = new byte[0];
                    final BytesRef dst = new BytesRef();
                    for (int value = 0; value < stream.values(); value++) {
                        scratch = reader.read(stream.offsets[value], stream.length(value), scratch);
                        reader.span(stream.offsets[value], stream.length(value), dst);
                    }
                    reader.transfer(0, stream.offsets[stream.values()], (chunk, offset, length) -> {});
                }
            }
            for (String name : TestCollectors.toKeyedValues(countersOnly.snapshot()).keySet()) {
                assertFalse("a counters-only collector reports no time, but reported " + name, name.endsWith("_time_nanos"));
            }
            final Map<String, Long> withoutTimes = new TreeMap<>(TestCollectors.toKeyedValues(timed.snapshot()));
            withoutTimes.keySet().removeIf(name -> name.endsWith("_time_nanos"));
            assertThat(
                "measuring time does not change what is counted",
                TestCollectors.toKeyedValues(countersOnly.snapshot()),
                equalTo(withoutTimes)
            );
        }
    }

    public void testAnIdentityTransferCountsOneVerbatimReadPerPiece() throws IOException {
        final List<byte[]> values = randomValues(between(200, 800));
        try (Directory dir = newDirectory()) {
            final Stream stream = write(dir, ChunkCodec.IDENTITY, randomChunkBytes(), values);
            final ProfileCollector stats = TestCollectors.countersOnly();
            final long total = stream.offsets[stream.values()];
            final long[] appended = { 0 };
            try (IndexInput in = dir.openInput(CHUNKS, IOContext.DEFAULT); IndexInput nav = dir.openInput(NAVIGATION, IOContext.DEFAULT)) {
                final ChunkedBytesReader reader = stream.index.open(new ColumnInputs(in, null, null, nav, stats));
                reader.transfer(0, total, (bytes, offset, length) -> appended[0] += length);
            }
            assertThat(appended[0], equalTo(total));
            final long pieces = (total + 32 * 1024 - 1) / (32 * 1024);
            assertThat(
                "a transfer reads one bounded piece at a time, each one a verbatim read",
                TestCollectors.toKeyedValues(stats.snapshot()),
                equalTo(Map.of("ColumNAR.uncompressed.reads", pieces, "ColumNAR.uncompressed.bytes_read", total))
            );
        }
    }

    public void testAnUnprofiledReaderNeedsNoCollector() throws IOException {
        final List<byte[]> values = randomValues(between(50, 500));
        try (Directory dir = newDirectory()) {
            final Stream stream = write(dir, ChunkCodec.ZSTD, randomChunkBytes(), values);
            try (IndexInput in = dir.openInput(CHUNKS, IOContext.DEFAULT); IndexInput nav = dir.openInput(NAVIGATION, IOContext.DEFAULT)) {
                final ChunkedBytesReader reader = stream.index.open(new ColumnInputs(in, null, null, nav, null));
                assertArrayEquals(values.get(0), Arrays.copyOf(reader.read(0, stream.length(0), new byte[0]), stream.length(0)));
            }
        }
    }

    public void testAChunkThatFailsToInflateIsNotCounted() {
        final ProfileCollector stats = TestCollectors.countersOnly();
        final ChunkDecompressor failing = (in, storedLength, dst, uncompressedLength) -> { throw new IOException("corrupt chunk"); };
        final ProfiledChunkDecompressor profiled = new ProfiledChunkDecompressor(failing, stats, ProfiledChunkDecompressor.CHUNKS);
        expectThrows(IOException.class, () -> profiled.read(null, 10, new byte[10], 10));
        assertThat(
            TestCollectors.toKeyedValues(stats.snapshot()),
            equalTo(
                Map.of("ColumNAR.chunks.decompressed", 0L, "ColumNAR.chunks.bytes_compressed", 0L, "ColumNAR.chunks.bytes_inflated", 0L)
            )
        );
    }

    public void testConcurrentReadersEachCountTheirOwnWork() throws Exception {
        final List<byte[]> values = randomValues(between(2000, 6000));
        try (Directory dir = newDirectory()) {
            final Stream stream = write(dir, ChunkCodec.ZSTD, randomChunkBytes(), values);
            final int threads = between(3, 8);
            final long[] seeds = new long[threads];
            for (int t = 0; t < threads; t++) {
                seeds[t] = randomLong();
            }
            final List<ProfileCollector> collectors = new ArrayList<>();
            final List<Replay> replays = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                collectors.add(TestCollectors.countersOnly());
                replays.add(new Replay(stream));
            }
            try (IndexInput in = dir.openInput(CHUNKS, IOContext.DEFAULT); IndexInput nav = dir.openInput(NAVIGATION, IOContext.DEFAULT)) {
                final CountDownLatch start = new CountDownLatch(1);
                final AtomicReference<Throwable> failure = new AtomicReference<>();
                final List<Thread> workers = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    final int thread = t;
                    final Thread worker = new Thread(() -> {
                        try {
                            final ChunkedBytesReader reader = stream.index.open(
                                new ColumnInputs(in, null, null, nav, collectors.get(thread))
                            );
                            final Random random = new Random(seeds[thread]);
                            start.await();
                            byte[] scratch = new byte[0];
                            for (int probe = 0; probe < 1500; probe++) {
                                final int value = random.nextInt(stream.values());
                                scratch = reader.read(stream.offsets[value], stream.length(value), scratch);
                                replays.get(thread).read(stream.offsets[value], stream.length(value));
                            }
                        } catch (Throwable e) {
                            failure.compareAndSet(null, e);
                        }
                    });
                    workers.add(worker);
                    worker.start();
                }
                start.countDown();
                for (Thread worker : workers) {
                    worker.join();
                }
                if (failure.get() != null) {
                    throw new AssertionError("a concurrent reader failed", failure.get());
                }
            }
            for (int t = 0; t < threads; t++) {
                assertThat("thread " + t, TestCollectors.toKeyedValues(collectors.get(t).snapshot()), equalTo(replays.get(t).expected()));
            }
        }
    }

    private int randomChunkBytes() {
        return randomFrom(64, 512, 4096, 32 * 1024);
    }

    private List<byte[]> randomValues(int count) {
        final List<byte[]> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final byte[] value = new byte[between(1, 120)];
            for (int b = 0; b < value.length; b++) {
                value[b] = (byte) ('a' + (i + b) % 26);
            }
            values.add(value);
        }
        return values;
    }

    private static Stream write(Directory dir, ChunkCodec codec, int target, List<byte[]> values) throws IOException {
        final long[] offsets = new long[values.size() + 1];
        final ChunkIndexMetadata index;
        try (
            IndexOutput out = dir.createOutput(CHUNKS, IOContext.DEFAULT);
            IndexOutput nav = dir.createOutput(NAVIGATION, IOContext.DEFAULT)
        ) {
            final ChunkedBytesWriter writer = new ChunkedBytesWriter(codec, ChunkBounds.ofBytes(target), out, nav);
            for (int i = 0; i < values.size(); i++) {
                writer.boundary(1);
                offsets[i] = writer.uncompressedLength();
                writer.append(values.get(i), 0, values.get(i).length);
            }
            offsets[values.size()] = writer.uncompressedLength();
            index = ChunkIndexMetadata.of(writer.finish());
        }
        long[] starts = new long[0];
        long[] fileOffsets = new long[0];
        if (index.numChunks() > 0) {
            try (IndexInput nav = dir.openInput(NAVIGATION, IOContext.DEFAULT)) {
                starts = table(
                    MonotonicReader.open(
                        nav,
                        index.startsMeta(),
                        index.numChunks() + 1L,
                        index.startsDataOffset(),
                        index.startsDataLength()
                    ),
                    index.numChunks() + 1
                );
                fileOffsets = table(
                    MonotonicReader.open(
                        nav,
                        index.fileOffsetsMeta(),
                        index.numChunks() + 1L,
                        index.fileOffsetsDataOffset(),
                        index.fileOffsetsDataLength()
                    ),
                    index.numChunks() + 1
                );
            }
        }
        return new Stream(index, offsets, starts, fileOffsets);
    }

    private static long[] table(LongValues values, int entries) {
        final long[] copy = new long[entries];
        for (int i = 0; i < entries; i++) {
            copy[i] = values.get(i);
        }
        return copy;
    }
}
