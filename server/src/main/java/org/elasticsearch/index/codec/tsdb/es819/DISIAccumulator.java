/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.io.Closeable;
import java.io.IOException;

/**
 * Fork of {@link org.apache.lucene.codecs.lucene90.IndexedDISI#writeBitSet(DocIdSetIterator, IndexOutput)} but that allows
 * building jump list iteratively by one docid at a time instead of relying on docidset iterator.
 */
final class DISIAccumulator implements Closeable {

    private static final int BLOCK_SIZE = 65536; // The number of docIDs that a single block represents

    private static final int DENSE_BLOCK_LONGS = BLOCK_SIZE / Long.SIZE; // 1024
    public static final byte DEFAULT_DENSE_RANK_POWER = 9; // Every 512 docIDs / 8 longs

    static final int MAX_ARRAY_LENGTH = (1 << 12) - 1;

    final Directory dir;
    final IOContext context;
    final String skipListTempFileName;
    final IndexOutput disiTempOutput;
    final byte denseRankPower;
    final long origo;

    int totalCardinality = 0;
    int blockCardinality = 0;
    final FixedBitSet buffer = new FixedBitSet(1 << 16);
    int[] jumps = new int[ArrayUtil.oversize(1, Integer.BYTES * 2)];
    int prevBlock = -1;
    int jumpBlockIndex = 0;

    DISIAccumulator(Directory dir, IOContext context, IndexOutput data, byte denseRankPower) throws IOException {
        this.dir = dir;
        this.context = context;
        this.denseRankPower = denseRankPower;
        if ((denseRankPower < 7 || denseRankPower > 15) && denseRankPower != -1) {
            throw new IllegalArgumentException(
                "Acceptable values for denseRankPower are 7-15 (every 128-32768 docIDs). "
                    + "The provided power was "
                    + denseRankPower
                    + " (every "
                    + (int) Math.pow(2, denseRankPower)
                    + " docIDs)"
            );
        }
        this.disiTempOutput = dir.createTempOutput(data.getName(), "disi", context);
        this.skipListTempFileName = disiTempOutput.getName();
        this.origo = disiTempOutput.getFilePointer(); // All jumps are relative to the origo
    }

    void addDocId(int doc) throws IOException {
        final int block = doc >>> 16;
        if (prevBlock != -1 && block != prevBlock) {
            // Track offset+index from previous block up to current
            jumps = addJumps(jumps, disiTempOutput.getFilePointer() - origo, totalCardinality, jumpBlockIndex, prevBlock + 1);
            jumpBlockIndex = prevBlock + 1;
            // Flush block
            flush(prevBlock, buffer, blockCardinality, denseRankPower, disiTempOutput);
            // Reset for next block
            buffer.clear();
            totalCardinality += blockCardinality;
            blockCardinality = 0;
        }
        buffer.set(doc & 0xFFFF);
        blockCardinality++;
        prevBlock = block;
    }

    short build(IndexOutput data) throws IOException {
        if (blockCardinality > 0) {
            jumps = addJumps(jumps, disiTempOutput.getFilePointer() - origo, totalCardinality, jumpBlockIndex, prevBlock + 1);
            totalCardinality += blockCardinality;
            flush(prevBlock, buffer, blockCardinality, denseRankPower, disiTempOutput);
            buffer.clear();
            prevBlock++;
        }
        final int lastBlock = prevBlock == -1 ? 0 : prevBlock; // There will always be at least 1 block (NO_MORE_DOCS)
        // Last entry is a SPARSE with blockIndex == 32767 and the single entry 65535, which becomes the
        // docID NO_MORE_DOCS
        // To avoid creating 65K jump-table entries, only a single entry is created pointing to the
        // offset of the
        // NO_MORE_DOCS block, with the jumpBlockIndex set to the logical EMPTY block after all real
        // blocks.
        jumps = addJumps(jumps, disiTempOutput.getFilePointer() - origo, totalCardinality, lastBlock, lastBlock + 1);
        buffer.set(DocIdSetIterator.NO_MORE_DOCS & 0xFFFF);
        flush(DocIdSetIterator.NO_MORE_DOCS >>> 16, buffer, 1, denseRankPower, disiTempOutput);
        // offset+index jump-table stored at the end
        short blockCount = flushBlockJumps(jumps, lastBlock + 1, disiTempOutput);
        disiTempOutput.close();
        try (var addressDataInput = dir.openInput(skipListTempFileName, context)) {
            data.copyBytes(addressDataInput, addressDataInput.length());
        }
        return blockCount;
    }

    // Adds entries to the offset & index jump-table for blocks
    private static int[] addJumps(int[] jumps, long offset, int index, int startBlock, int endBlock) {
        assert offset < Integer.MAX_VALUE : "Logically the offset should not exceed 2^30 but was >= Integer.MAX_VALUE";
        jumps = ArrayUtil.grow(jumps, (endBlock + 1) * 2);
        for (int b = startBlock; b < endBlock; b++) {
            jumps[b * 2] = index;
            jumps[b * 2 + 1] = (int) offset;
        }
        return jumps;
    }

    private static void flush(int block, FixedBitSet buffer, int cardinality, byte denseRankPower, IndexOutput out) throws IOException {
        assert block >= 0 && block < BLOCK_SIZE;
        out.writeShort((short) block);
        assert cardinality > 0 && cardinality <= BLOCK_SIZE;
        out.writeShort((short) (cardinality - 1));
        if (cardinality > MAX_ARRAY_LENGTH) {
            if (cardinality != BLOCK_SIZE) { // all docs are set
                if (denseRankPower != -1) {
                    final byte[] rank = createRank(buffer, denseRankPower);
                    out.writeBytes(rank, rank.length);
                }
                for (long word : buffer.getBits()) {
                    out.writeLong(word);
                }
            }
        } else {
            BitSetIterator it = new BitSetIterator(buffer, cardinality);
            for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                out.writeShort((short) doc);
            }
        }
    }

    // Flushes the offset & index jump-table for blocks. This should be the last data written to out
    // This method returns the blockCount for the blocks reachable for the jump_table or -1 for no
    // jump-table
    private static short flushBlockJumps(int[] jumps, int blockCount, IndexOutput out) throws IOException {
        if (blockCount == 2) { // Jumps with a single real entry + NO_MORE_DOCS is just wasted space so we ignore
            // that
            blockCount = 0;
        }
        for (int i = 0; i < blockCount; i++) {
            out.writeInt(jumps[i * 2]); // index
            out.writeInt(jumps[i * 2 + 1]); // offset
        }
        // As there are at most 32k blocks, the count is a short
        // The jumpTableOffset will be at lastPos - (blockCount * Long.BYTES)
        return (short) blockCount;
    }

    // Creates a DENSE rank-entry (the number of set bits up to a given point) for the buffer.
    // One rank-entry for every {@code 2^denseRankPower} bits, with each rank-entry using 2 bytes.
    // Represented as a byte[] for fast flushing and mirroring of the retrieval representation.
    private static byte[] createRank(FixedBitSet buffer, byte denseRankPower) {
        final int longsPerRank = 1 << (denseRankPower - 6);
        final int rankMark = longsPerRank - 1;
        final int rankIndexShift = denseRankPower - 7; // 6 for the long (2^6) + 1 for 2 bytes/entry
        final byte[] rank = new byte[DENSE_BLOCK_LONGS >> rankIndexShift];
        final long[] bits = buffer.getBits();
        int bitCount = 0;
        for (int word = 0; word < DENSE_BLOCK_LONGS; word++) {
            if ((word & rankMark) == 0) { // Every longsPerRank longs
                rank[word >> rankIndexShift] = (byte) (bitCount >> 8);
                rank[(word >> rankIndexShift) + 1] = (byte) (bitCount & 0xFF);
            }
            bitCount += Long.bitCount(bits[word]);
        }
        return rank;
    }

    @Override
    @SuppressForbidden(reason = "require usage of Lucene's IOUtils#deleteFilesIgnoringExceptions(...)")
    public void close() throws IOException {
        IOUtils.close(disiTempOutput);
        if (skipListTempFileName != null) {
            IOUtils.deleteFilesIgnoringExceptions(dir, skipListTempFileName);
        }
    }

}
