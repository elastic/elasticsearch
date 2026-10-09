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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Random;

public class BlockBytesCodecTests extends ESTestCase {

    /** Writes {@code block} through the codec and reads it back, answering what the stored bytes cost. */
    private int roundTrip(byte codecId, byte[] block) throws IOException {
        final byte[] read = new byte[block.length];
        final int stored;
        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexOutput out = dir.createOutput("block", IOContext.DEFAULT)) {
                BlockBytesCodec.forId(codecId).write(o -> o.writeBytes(block, 0, block.length), out);
            }
            stored = Math.toIntExact(dir.fileLength("block"));
            try (IndexInput in = dir.openInput("block", IOContext.DEFAULT)) {
                final DataInput data = BlockBytesCodec.forId(codecId).read(in, stored);
                data.readBytes(read, 0, read.length);
            }
        }
        assertArrayEquals("block did not survive the round trip", block, read);
        return stored;
    }

    public void testCompressibleBlockRoundTripsAndShrinks() throws IOException {
        final byte[] block = new byte[8192];
        for (int i = 0; i < block.length; i++) {
            block[i] = (byte) (i % 5);
        }
        final int stored = roundTrip(BlockBytesCodec.ZSTD_ID, block);
        assertTrue("a block that repeats should be stored smaller, got " + stored, stored < block.length);
    }

    /**
     * Random bytes do not compress, so the block is stored plain and costs only the marker over the bytes
     * themselves. The round trip still has to return them.
     */
    public void testIncompressibleBlockIsStoredPlain() throws IOException {
        final byte[] block = new byte[4096];
        new Random(randomLong()).nextBytes(block);
        final int stored = roundTrip(BlockBytesCodec.ZSTD_ID, block);
        assertEquals("expected the bytes and a marker", block.length + 1, stored);
    }

    public void testEmptyBlockRoundTrips() throws IOException {
        roundTrip(BlockBytesCodec.ZSTD_ID, new byte[0]);
    }

    public void testIdentityRoundTrips() throws IOException {
        final byte[] block = new byte[1024];
        new Random(randomLong()).nextBytes(block);
        assertEquals(block.length, roundTrip(BlockBytesCodec.IDENTITY_ID, block));
    }

    /**
     * Blocks are written one after another into a column and read back through the codec the reader holds,
     * so a block has to survive both the scratch the previous one left and starting somewhere other than
     * the front of the file.
     */
    public void testBlocksAreReadBackInSequence() throws IOException {
        final byte[][] blocks = new byte[4][];
        for (int b = 0; b < blocks.length; b++) {
            blocks[b] = new byte[1024 + b];
            if (b % 2 == 0) {
                // Repeats, so this one is stored compressed; the odd ones are random and stored plain.
                for (int i = 0; i < blocks[b].length; i++) {
                    blocks[b][i] = (byte) (i % 7);
                }
            } else {
                new Random(randomLong()).nextBytes(blocks[b]);
            }
        }
        try (Directory dir = new ByteBuffersDirectory()) {
            final long[] ends = new long[blocks.length];
            final BlockBytesCodec writer = BlockBytesCodec.forId(BlockBytesCodec.ZSTD_ID);
            try (IndexOutput out = dir.createOutput("blocks", IOContext.DEFAULT)) {
                for (int b = 0; b < blocks.length; b++) {
                    final byte[] block = blocks[b];
                    writer.write(o -> o.writeBytes(block, 0, block.length), out);
                    ends[b] = out.getFilePointer();
                }
            }
            final BlockBytesCodec reader = BlockBytesCodec.forId(BlockBytesCodec.ZSTD_ID);
            try (IndexInput in = dir.openInput("blocks", IOContext.DEFAULT)) {
                for (int b = 0; b < blocks.length; b++) {
                    final long start = b == 0 ? 0 : ends[b - 1];
                    in.seek(start);
                    final DataInput data = reader.read(in, Math.toIntExact(ends[b] - start));
                    final byte[] read = new byte[blocks[b].length];
                    data.readBytes(read, 0, read.length);
                    assertArrayEquals("block " + b + " did not read back", blocks[b], read);
                }
            }
        }
    }

    /** The zstd codec holds scratch, so two callers must not be handed the same one. */
    public void testCompressingCodecIsNotShared() {
        assertNotSame(BlockBytesCodec.forId(BlockBytesCodec.ZSTD_ID), BlockBytesCodec.forId(BlockBytesCodec.ZSTD_ID));
        assertSame(BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID), BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID));
    }

    public void testUnknownIdIsRejected() {
        expectThrows(IllegalArgumentException.class, () -> BlockBytesCodec.forId((byte) 99));
    }
}
