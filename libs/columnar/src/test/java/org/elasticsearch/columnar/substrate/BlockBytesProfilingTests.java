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
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.codec.profiler.ProfileCollector;
import org.elasticsearch.codec.profiler.TestCollectors;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class BlockBytesProfilingTests extends ESTestCase {

    // NOTE: The first byte of a Zstd block that is stored compressed. It is part of the persisted format.
    private static final byte STORED_COMPRESSED = 1;

    private record Written(byte[][] blocks, long[] starts, long[] ends, boolean[] compressed) {
        int stored(int block) {
            return Math.toIntExact(ends[block] - starts[block]);
        }
    }

    public void testZstdBlocksAreCountedWithTheirStoredAndInflatedSize() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            final Written written = write(dir, BlockBytesCodec.ZSTD_ID, between(8, 40));
            final ProfileCollector stats = TestCollectors.countersOnly();
            long reads = 0;
            long stored = 0;
            long compressedBlocks = 0;
            long inflated = 0;
            try (IndexInput in = dir.openInput("blocks", IOContext.DEFAULT)) {
                final BlockBytesCodec codec = BlockBytesCodec.forReading(BlockBytesCodec.ZSTD_ID, stats);
                for (int probe = 0; probe < 200; probe++) {
                    final int block = randomIntBetween(0, written.blocks.length - 1);
                    in.seek(written.starts[block]);
                    read(codec, in, written.stored(block), written.blocks[block].length);
                    reads++;
                    stored += written.stored(block);
                    if (written.compressed[block]) {
                        compressedBlocks++;
                        inflated += written.blocks[block].length;
                    }
                }
            }
            assertThat(compressedBlocks, greaterThan(0L));
            assertThat(
                TestCollectors.toKeyedValues(stats.snapshot()),
                equalTo(
                    Map.of(
                        "ColumNAR.blocks.fetched",
                        reads,
                        "ColumNAR.blocks.bytes_fetched",
                        stored,
                        "ColumNAR.blocks.decompressed",
                        compressedBlocks,
                        "ColumNAR.blocks.bytes_inflated",
                        inflated
                    )
                )
            );
        }
    }

    public void testIdentityBlocksAreCountedButNeverInflated() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            final Written written = write(dir, BlockBytesCodec.IDENTITY_ID, between(4, 20));
            final ProfileCollector stats = TestCollectors.countersOnly();
            long stored = 0;
            try (IndexInput in = dir.openInput("blocks", IOContext.DEFAULT)) {
                final BlockBytesCodec codec = BlockBytesCodec.forReading(BlockBytesCodec.IDENTITY_ID, stats);
                for (int block = 0; block < written.blocks.length; block++) {
                    in.seek(written.starts[block]);
                    read(codec, in, written.stored(block), written.blocks[block].length);
                    stored += written.stored(block);
                }
            }
            assertThat(
                TestCollectors.toKeyedValues(stats.snapshot()),
                equalTo(Map.of("ColumNAR.blocks.fetched", (long) written.blocks.length, "ColumNAR.blocks.bytes_fetched", stored))
            );
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()), not(hasKey("ColumNAR.blocks.decompressed")));
        }
    }

    public void testProfilingDoesNotChangeTheBytesOfTheBlocks() throws IOException {
        for (byte codecId : List.of(BlockBytesCodec.IDENTITY_ID, BlockBytesCodec.ZSTD_ID)) {
            try (Directory dir = new ByteBuffersDirectory()) {
                final Written written = write(dir, codecId, between(4, 20));
                try (IndexInput in = dir.openInput("blocks", IOContext.DEFAULT)) {
                    final BlockBytesCodec profiled = BlockBytesCodec.forReading(codecId, TestCollectors.countersOnly());
                    for (int block = 0; block < written.blocks.length; block++) {
                        in.seek(written.starts[block]);
                        assertArrayEquals(written.blocks[block], read(profiled, in, written.stored(block), written.blocks[block].length));
                    }
                }
            }
        }
    }

    public void testABlockThatFailsToReadIsNotCounted() {
        final ProfileCollector stats = TestCollectors.countersOnly();
        final BlockBytesCodec failing = new BlockBytesCodec() {
            @Override
            public byte id() {
                return IDENTITY_ID;
            }

            @Override
            public void write(BlockEncoder encoder, DataOutput out) {
                throw new UnsupportedOperationException();
            }

            @Override
            public DataInput read(IndexInput in, int length) throws IOException {
                throw new IOException("corrupt block");
            }
        };
        final BlockBytesCodec profiled = new ProfiledBlockBytesCodec(failing, stats);
        expectThrows(IOException.class, () -> profiled.read(null, 10));
        assertThat(
            TestCollectors.toKeyedValues(stats.snapshot()),
            equalTo(Map.of("ColumNAR.blocks.fetched", 0L, "ColumNAR.blocks.bytes_fetched", 0L))
        );
    }

    public void testACodecForReadingWithoutACollectorIsTheOneThatShips() {
        assertSame(BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID), BlockBytesCodec.forReading(BlockBytesCodec.IDENTITY_ID, null));
        assertEquals(
            BlockBytesCodec.forId(BlockBytesCodec.ZSTD_ID).getClass(),
            BlockBytesCodec.forReading(BlockBytesCodec.ZSTD_ID, null).getClass()
        );
        assertNotSame(BlockBytesCodec.forReading(BlockBytesCodec.ZSTD_ID, null), BlockBytesCodec.forReading(BlockBytesCodec.ZSTD_ID, null));
    }

    public void testAnUnknownIdIsStillRejectedWhenProfiling() {
        expectThrows(IllegalArgumentException.class, () -> BlockBytesCodec.forReading((byte) 99, TestCollectors.countersOnly()));
    }

    private static byte[] read(BlockBytesCodec codec, IndexInput in, int stored, int length) throws IOException {
        final DataInput data = codec.read(in, stored);
        final byte[] block = new byte[length];
        data.readBytes(block, 0, length);
        return block;
    }

    private Written write(Directory dir, byte codecId, int count) throws IOException {
        final byte[][] blocks = new byte[count][];
        final long[] starts = new long[count];
        final long[] ends = new long[count];
        final boolean[] compressed = new boolean[count];
        final BlockBytesCodec writer = BlockBytesCodec.forId(codecId);
        try (IndexOutput out = dir.createOutput("blocks", IOContext.DEFAULT)) {
            for (int b = 0; b < count; b++) {
                blocks[b] = new byte[between(512, 4096)];
                if (b % 3 == 2) {
                    new Random(randomLong()).nextBytes(blocks[b]);
                } else {
                    for (int i = 0; i < blocks[b].length; i++) {
                        blocks[b][i] = (byte) (i % 7);
                    }
                }
                final byte[] block = blocks[b];
                starts[b] = out.getFilePointer();
                writer.write(o -> o.writeBytes(block, 0, block.length), out);
                ends[b] = out.getFilePointer();
            }
        }
        try (IndexInput in = dir.openInput("blocks", IOContext.DEFAULT)) {
            for (int b = 0; b < count; b++) {
                in.seek(starts[b]);
                compressed[b] = codecId == BlockBytesCodec.ZSTD_ID && in.readByte() == STORED_COMPRESSED;
            }
        }
        return new Written(blocks, starts, ends, compressed);
    }
}
