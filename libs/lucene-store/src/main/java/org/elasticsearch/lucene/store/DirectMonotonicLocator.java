/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.store;

import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.DirectMonotonicReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Locates the entries of a {@code DirectMonotonicWriter} table from its metadata: per block of {@code 1 << blockShift}
 * entries, a minimum, an average, an offset and the bits per value. An entry is bit-packed at
 * {@code offset + index * bits / 8}, and read with one read of 1, 2, 4 or 8 bytes depending on the bits.
 */
public final class DirectMonotonicLocator {

    /** Metadata bytes per block. */
    private static final int META_BYTES_PER_BLOCK = Long.BYTES + Integer.BYTES + Long.BYTES + Byte.BYTES;

    /** The metadata of a table and its locator. */
    public record Loaded(DirectMonotonicReader.Meta meta, DirectMonotonicLocator locator) {}

    private final int blockShift;
    private final long[] offsets;
    private final byte[] bitsPerValue;

    private DirectMonotonicLocator(int blockShift, long[] offsets, byte[] bitsPerValue) {
        this.blockShift = blockShift;
        this.offsets = offsets;
        this.bitsPerValue = bitsPerValue;
    }

    /** Reads a table's metadata as {@link DirectMonotonicReader#loadMeta} does. */
    public static Loaded load(IndexInput in, long numValues, int blockShift) throws IOException {
        byte[] bytes = new byte[Math.toIntExact(numBlocks(numValues, blockShift) * META_BYTES_PER_BLOCK)];
        in.readBytes(bytes, 0, bytes.length);
        DirectMonotonicReader.Meta meta;
        try (IndexInput metaIn = new ByteBuffersIndexInput(new ByteBuffersDataInput(List.of(ByteBuffer.wrap(bytes))), "meta")) {
            meta = DirectMonotonicReader.loadMeta(metaIn, numValues, blockShift);
        }
        return new Loaded(meta, of(bytes, numValues, blockShift));
    }

    /** Locates the entries of a table from its metadata bytes. */
    public static DirectMonotonicLocator of(byte[] metaBytes, long numValues, int blockShift) throws IOException {
        int numBlocks = Math.toIntExact(numBlocks(numValues, blockShift));
        long[] offsets = new long[numBlocks];
        byte[] bitsPerValue = new byte[numBlocks];
        DataInput in = new ByteBuffersDataInput(List.of(ByteBuffer.wrap(metaBytes)));
        for (int block = 0; block < numBlocks; block++) {
            in.readLong();
            in.readInt();
            offsets[block] = in.readLong();
            bitsPerValue[block] = in.readByte();
        }
        return new DirectMonotonicLocator(blockShift, offsets, bitsPerValue);
    }

    private static long numBlocks(long numValues, int blockShift) {
        long numBlocks = numValues >>> blockShift;
        return (numBlocks << blockShift) < numValues ? numBlocks + 1 : numBlocks;
    }

    /**
     * Asks for what reading entry {@code index} of the table at {@code tableOffset} reads; nothing for a block of zero bits.
     *
     * @return whether it asked for anything
     */
    public boolean prefetch(IndexInput file, long tableOffset, long index) throws IOException {
        int block = (int) (index >>> blockShift);
        int bits = bitsPerValue[block];
        if (bits == 0) {
            return false;
        }
        long inBlock = index & ((1L << blockShift) - 1);
        file.prefetch(tableOffset + offsets[block] + ((inBlock * bits) >>> 3), readBytes(bits));
        return true;
    }

    /** The bytes one read of an entry of {@code bits} bits takes. */
    private static int readBytes(int bits) {
        if (bits <= 8) {
            return Byte.BYTES;
        } else if (bits <= 16) {
            return Short.BYTES;
        } else if (bits <= 32) {
            return Integer.BYTES;
        }
        return Long.BYTES;
    }
}
