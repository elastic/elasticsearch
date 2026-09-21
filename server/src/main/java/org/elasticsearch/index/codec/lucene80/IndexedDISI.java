/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2026 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.lucene80;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RoaringDocIdSet;

import java.io.DataInput;
import java.io.IOException;

/**
 * Disk-based implementation of a {@link DocIdSetIterator} which can return the index of the current
 * document, i.e. the ordinal of the current document among the list of documents that this iterator
 * can return. This is useful to implement sparse doc values by only having to encode values for
 * documents that actually have a value.
 *
 * <p>Implementation-wise, this {@link DocIdSetIterator} is inspired of {@link RoaringDocIdSet
 * roaring bitmaps} and encodes ranges of {@code 65536} documents independently and picks between 3
 * encodings depending on the density of the range:
 *
 * <ul>
 *   <li>{@code ALL} if the range contains 65536 documents exactly,
 *   <li>{@code DENSE} if the range contains 4096 documents or more; in that case documents are
 *       stored in a bit set,
 *   <li>{@code SPARSE} otherwise, and the lower 16 bits of the doc IDs are stored in a {@link
 *       DataInput#readShort() short}.
 * </ul>
 *
 * <p>Only ranges that contain at least one value are encoded.
 *
 * <p>This implementation uses 6 bytes per document in the worst-case, which happens in the case
 * that all ranges contain exactly one document.
 *
 * <p>To avoid O(n) lookup time complexity, with n being the number of documents, two lookup tables
 * are used: A lookup table for block offset and index, and a rank structure for DENSE block index
 * lookups.
 *
 * <p>The lookup table is an array of {@code int}-pairs, with a pair for each block. It allows for
 * direct jumping to the block, as opposed to iteration from the current position and forward one
 * block at a time.
 *
 * <p>Each int-pair entry consists of 2 logical parts:
 *
 * <p>The first 32 bit int holds the index (number of set bits in the blocks) up to just before the
 * wanted block. The maximum number of set bits is the maximum number of documents, which is {@code
 * < 2^31}.
 *
 * <p>The next int holds the offset in bytes into the underlying slice. As there is a maximum of
 * 2^16 blocks, it follows that the maximum size of any block must not exceed 2^15 bytes to avoid
 * overflow (2^16 bytes if the int is treated as unsigned). This is currently the case, with the
 * largest block being DENSE and using 2^13 + 36 bytes.
 *
 * <p>The cache overhead is numDocs/1024 bytes.
 *
 * <p>Note: There are 4 types of blocks: ALL, DENSE, SPARSE and non-existing (0 set bits). In the
 * case of non-existing blocks, the entry in the lookup table has index equal to the previous entry
 * and offset equal to the next non-empty block.
 *
 * <p>The block lookup table is stored at the end of the total block structure.
 *
 * <p>The rank structure for DENSE blocks is an array of byte-pairs with an entry for each sub-block
 * (default 512 bits) out of the 65536 bits in the outer DENSE block.
 *
 * <p>Each rank-entry states the number of set bits within the block up to the bit before the bit
 * positioned at the start of the sub-block. Note that that the rank entry of the first sub-block is
 * always 0 and that the last entry can at most be 65536-2 = 65634 and thus will always fit into an
 * byte-pair of 16 bits.
 *
 * <p>The rank structure for a given DENSE block is stored at the beginning of the DENSE block. This
 * ensures locality and keeps logistics simple.
 *
 */
final class IndexedDISI extends DocIdSetIterator {

    // jump-table time/space trade-offs to consider:
    // The block offsets and the block indexes could be stored in more compressed form with
    // two PackedInts or two MonotonicDirectReaders.
    // The DENSE ranks (default 128 shorts = 256 bytes) could likewise be compressed. But as there is
    // at least 4096 set bits in DENSE blocks, there will be at least one rank with 2^12 bits, so it
    // is doubtful if there is much to gain here.

    private static final int BLOCK_SIZE = 65536; // The number of docIDs that a single block represents

    private static final int DENSE_BLOCK_LONGS = BLOCK_SIZE / Long.SIZE; // 1024
    public static final byte DEFAULT_DENSE_RANK_POWER = 9; // Every 512 docIDs / 8 longs

    static final int MAX_ARRAY_LENGTH = (1 << 12) - 1;

    private static void flush(int block, FixedBitSet buffer, int cardinality, byte denseRankPower, IndexOutput out) throws IOException {
        assert block >= 0 && block < 65536;
        out.writeShort((short) block);
        assert cardinality > 0 && cardinality <= 65536;
        out.writeShort((short) (cardinality - 1));
        if (cardinality > MAX_ARRAY_LENGTH) {
            if (cardinality != 65536) { // all docs are set
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

    /**
     * Writes the docIDs from it to out, in logical blocks, one for each 65536 docIDs in monotonically
     * increasing gap-less order. DENSE blocks uses {@link #DEFAULT_DENSE_RANK_POWER} of 9 (every 512
     * docIDs / 8 longs). The caller must keep track of the number of jump-table entries (returned by
     * this method) as well as the denseRankPower (9 for this method) and provide them when
     * constructing an IndexedDISI for reading.
     *
     * @param it the document IDs.
     * @param out destination for the blocks.
     * @throws IOException if there was an error writing to out.
     * @return the number of jump-table entries following the blocks, -1 for no entries. This should
     *     be stored in meta and used when creating an instance of IndexedDISI.
     */
    static short writeBitSet(DocIdSetIterator it, IndexOutput out) throws IOException {
        return writeBitSet(it, out, DEFAULT_DENSE_RANK_POWER);
    }

    /**
     * Writes the docIDs from it to out, in logical blocks, one for each 65536 docIDs in monotonically
     * increasing gap-less order. The caller must keep track of the number of jump-table entries
     * (returned by this method) as well as the denseRankPower and provide them when constructing an
     * IndexedDISI for reading.
     *
     * @param it the document IDs.
     * @param out destination for the blocks.
     * @param denseRankPower for {@link Method#DENSE} blocks, a rank will be written every {@code
     *     2^denseRankPower} docIDs. Values &lt; 7 (every 128 docIDs) or &gt; 15 (every 32768 docIDs)
     *     disables DENSE rank. Recommended values are 8-12: Every 256-4096 docIDs or 4-64 longs.
     *     {@link #DEFAULT_DENSE_RANK_POWER} is 9: Every 512 docIDs. This should be stored in meta and
     *     used when creating an instance of IndexedDISI.
     * @throws IOException if there was an error writing to out.
     * @return the number of jump-table entries following the blocks, -1 for no entries. This should
     *     be stored in meta and used when creating an instance of IndexedDISI.
     */
    static short writeBitSet(DocIdSetIterator it, IndexOutput out, byte denseRankPower) throws IOException {
        final long origo = out.getFilePointer(); // All jumps are relative to the origo
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
        int totalCardinality = 0;
        int blockCardinality = 0;
        final FixedBitSet buffer = new FixedBitSet(1 << 16);
        int[] jumps = new int[ArrayUtil.oversize(1, Integer.BYTES * 2)];
        int prevBlock = -1;
        int jumpBlockIndex = 0;

        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
            final int block = doc >>> 16;
            if (prevBlock != -1 && block != prevBlock) {
                // Track offset+index from previous block up to current
                jumps = addJumps(jumps, out.getFilePointer() - origo, totalCardinality, jumpBlockIndex, prevBlock + 1);
                jumpBlockIndex = prevBlock + 1;
                // Flush block
                flush(prevBlock, buffer, blockCardinality, denseRankPower, out);
                // Reset for next block
                buffer.clear();
                totalCardinality += blockCardinality;
                blockCardinality = 0;
            }
            buffer.set(doc & 0xFFFF);
            blockCardinality++;
            prevBlock = block;
        }
        if (blockCardinality > 0) {
            jumps = addJumps(jumps, out.getFilePointer() - origo, totalCardinality, jumpBlockIndex, prevBlock + 1);
            totalCardinality += blockCardinality;
            flush(prevBlock, buffer, blockCardinality, denseRankPower, out);
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
        jumps = addJumps(jumps, out.getFilePointer() - origo, totalCardinality, lastBlock, lastBlock + 1);
        buffer.set(DocIdSetIterator.NO_MORE_DOCS & 0xFFFF);
        flush(DocIdSetIterator.NO_MORE_DOCS >>> 16, buffer, 1, denseRankPower, out);
        // offset+index jump-table stored at the end
        return flushBlockJumps(jumps, lastBlock + 1, out);
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

    // Members are pkg-private to avoid synthetic accessors when accessed from the `Method` enum

    /** The slice that stores the {@link DocIdSetIterator}. */
    final IndexInput slice;

    final int jumpTableEntryCount;
    final byte denseRankPower;
    final RandomAccessInput jumpTable; // Skip blocks of 64K bits
    final byte[] denseRankTable;
    final long cost;

    /**
     * This constructor always creates a new blockSlice and a new jumpTable from in, to ensure that
     * operations are independent from the caller. See {@link #IndexedDISI(IndexInput,
     * RandomAccessInput, int, byte, long)} for re-use of blockSlice and jumpTable.
     *
     * @param in backing data.
     * @param offset starting offset for blocks in the backing data.
     * @param length the number of bytes holding blocks and jump-table in the backing data.
     * @param jumpTableEntryCount the number of blocks covered by the jump-table. This must match the
     *     number returned by {@link #writeBitSet(DocIdSetIterator, IndexOutput, byte)}.
     * @param denseRankPower the number of docIDs covered by each rank entry in DENSE blocks,
     *     expressed as {@code 2^denseRankPower}. This must match the power given in {@link
     *     #writeBitSet(DocIdSetIterator, IndexOutput, byte)}
     * @param cost normally the number of logical docIDs.
     */
    IndexedDISI(IndexInput in, long offset, long length, int jumpTableEntryCount, byte denseRankPower, long cost) throws IOException {
        this(
            createBlockSlice(in, "docs", offset, length, jumpTableEntryCount),
            createJumpTable(in, offset, length, jumpTableEntryCount),
            jumpTableEntryCount,
            denseRankPower,
            cost
        );
    }

    /**
     * This constructor allows to pass the slice and jumpTable directly in case it helps reuse. see
     * eg. Lucene80 norms producer's merge instance.
     *
     * @param blockSlice data blocks, normally created by {@link #createBlockSlice}.
     * @param jumpTable table holding jump-data for block-skips, normally created by {@link
     *     #createJumpTable}.
     * @param jumpTableEntryCount the number of blocks covered by the jump-table. This must match the
     *     number returned by {@link #writeBitSet(DocIdSetIterator, IndexOutput, byte)}.
     * @param denseRankPower the number of docIDs covered by each rank entry in DENSE blocks,
     *     expressed as {@code 2^denseRankPower}. This must match the power given in {@link
     *     #writeBitSet(DocIdSetIterator, IndexOutput, byte)}
     * @param cost normally the number of logical docIDs.
     */
    IndexedDISI(IndexInput blockSlice, RandomAccessInput jumpTable, int jumpTableEntryCount, byte denseRankPower, long cost)
        throws IOException {
        if ((denseRankPower < 7 || denseRankPower > 15) && denseRankPower != -1) {
            throw new IllegalArgumentException(
                "Acceptable values for denseRankPower are 7-15 (every 128-32768 docIDs). "
                    + "The provided power was "
                    + denseRankPower
                    + " (every "
                    + (int) Math.pow(2, denseRankPower)
                    + " docIDs). "
            );
        }

        this.slice = blockSlice;
        this.jumpTable = jumpTable;
        this.jumpTableEntryCount = jumpTableEntryCount;
        this.denseRankPower = denseRankPower;
        final int rankIndexShift = denseRankPower - 7;
        this.denseRankTable = denseRankPower == -1 ? null : new byte[DENSE_BLOCK_LONGS >> rankIndexShift];
        this.cost = cost;
    }

    /**
     * Helper method for using {@link #IndexedDISI(IndexInput, RandomAccessInput, int, byte, long)}.
     * Creates a disiSlice for the IndexedDISI data blocks, without the jump-table.
     *
     * @param slice backing data, holding both blocks and jump-table.
     * @param sliceDescription human readable slice designation.
     * @param offset relative to the backing data.
     * @param length full length of the IndexedDISI, including blocks and jump-table data.
     * @param jumpTableEntryCount the number of blocks covered by the jump-table.
     * @return a jumpTable containing the block jump-data or null if no such table exists.
     * @throws IOException if a RandomAccessInput could not be created from slice.
     */
    public static IndexInput createBlockSlice(IndexInput slice, String sliceDescription, long offset, long length, int jumpTableEntryCount)
        throws IOException {
        long jumpTableBytes = jumpTableEntryCount < 0 ? 0 : jumpTableEntryCount * Integer.BYTES * 2;
        return slice.slice(sliceDescription, offset, length - jumpTableBytes);
    }

    /**
     * Helper method for using {@link #IndexedDISI(IndexInput, RandomAccessInput, int, byte, long)}.
     * Creates a RandomAccessInput covering only the jump-table data or null.
     *
     * @param slice backing data, holding both blocks and jump-table.
     * @param offset relative to the backing data.
     * @param length full length of the IndexedDISI, including blocks and jump-table data.
     * @param jumpTableEntryCount the number of blocks covered by the jump-table.
     * @return a jumpTable containing the block jump-data or null if no such table exists.
     * @throws IOException if a RandomAccessInput could not be created from slice.
     */
    public static RandomAccessInput createJumpTable(IndexInput slice, long offset, long length, int jumpTableEntryCount)
        throws IOException {
        if (jumpTableEntryCount <= 0) {
            return null;
        } else {
            int jumpTableBytes = jumpTableEntryCount * Integer.BYTES * 2;
            return slice.randomAccessSlice(offset + length - jumpTableBytes, jumpTableBytes);
        }
    }

    int block = -1;
    long blockEnd;
    long denseBitmapOffset = -1; // Only used for DENSE blocks
    int nextBlockIndex = -1;
    Method method;

    int doc = -1;
    int index = -1;

    // SPARSE variables
    boolean exists;

    // DENSE variables
    long word;
    int wordIndex = -1;
    // number of one bits encountered so far, including those of `word`
    int numberOfOnes;
    // Used with rank for jumps inside of DENSE as they are absolute instead of relative
    int denseOrigoIndex;

    // ALL variables
    int gap;

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int advance(int target) throws IOException {
        final int targetBlock = target & 0xFFFF0000;
        if (block < targetBlock) {
            advanceBlock(targetBlock);
        }
        if (block == targetBlock) {
            if (method.advanceWithinBlock(this, target)) {
                return doc;
            }
            readBlockHeader();
        }
        boolean found = method.advanceWithinBlock(this, block);
        assert found;
        return doc;
    }

    public boolean advanceExact(int target) throws IOException {
        final int targetBlock = target & 0xFFFF0000;
        if (block < targetBlock) {
            advanceBlock(targetBlock);
        }
        boolean found = block == targetBlock && method.advanceExactWithinBlock(this, target);
        this.doc = target;
        return found;
    }

    private void advanceBlock(int targetBlock) throws IOException {
        final int blockIndex = targetBlock >> 16;
        // If the destination block is 2 blocks or more ahead, we use the jump-table.
        if (jumpTable != null && blockIndex >= (block >> 16) + 2) {
            // If the jumpTableEntryCount is exceeded, there are no further bits. Last entry is always
            // NO_MORE_DOCS
            final int inRangeBlockIndex = blockIndex < jumpTableEntryCount ? blockIndex : jumpTableEntryCount - 1;
            final int index = jumpTable.readInt(inRangeBlockIndex * (long) Integer.BYTES * 2);
            final int offset = jumpTable.readInt(inRangeBlockIndex * (long) Integer.BYTES * 2 + Integer.BYTES);
            this.nextBlockIndex = index - 1; // -1 to compensate for the always-added 1 in readBlockHeader
            slice.seek(offset);
            readBlockHeader();
            return;
        }

        // Fallback to iteration of blocks
        do {
            slice.seek(blockEnd);
            readBlockHeader();
        } while (block < targetBlock);
    }

    private void readBlockHeader() throws IOException {
        block = Short.toUnsignedInt(slice.readShort()) << 16;
        assert block >= 0;
        final int numValues = 1 + Short.toUnsignedInt(slice.readShort());
        index = nextBlockIndex;
        nextBlockIndex = index + numValues;
        if (numValues <= MAX_ARRAY_LENGTH) {
            method = Method.SPARSE;
            blockEnd = slice.getFilePointer() + (numValues << 1);
        } else if (numValues == 65536) {
            method = Method.ALL;
            blockEnd = slice.getFilePointer();
            gap = block - index - 1;
        } else {
            method = Method.DENSE;
            denseBitmapOffset = slice.getFilePointer() + (denseRankTable == null ? 0 : denseRankTable.length);
            blockEnd = denseBitmapOffset + (1 << 13);
            // Performance consideration: All rank (default 128 * 16 bits) are loaded up front. This
            // should be fast with the
            // reusable byte[] buffer, but it is still wasted if the DENSE block is iterated in small
            // steps.
            // If this results in too great a performance regression, a heuristic strategy might work
            // where the rank data
            // are loaded on first in-block advance, if said advance is > X docIDs. The hope being that a
            // small first
            // advance means that subsequent advances will be small too.
            // Another alternative is to maintain an extra slice for DENSE rank, but IndexedDISI is
            // already slice-heavy.
            if (denseRankPower != -1) {
                slice.readBytes(denseRankTable, 0, denseRankTable.length);
            }
            wordIndex = -1;
            numberOfOnes = index + 1;
            denseOrigoIndex = numberOfOnes;
        }
    }

    @Override
    public int nextDoc() throws IOException {
        return advance(doc + 1);
    }

    public int index() {
        return index;
    }

    @Override
    public long cost() {
        return cost;
    }

    enum Method {
        SPARSE {
            @Override
            boolean advanceWithinBlock(IndexedDISI disi, int target) throws IOException {
                final int targetInBlock = target & 0xFFFF;
                // TODO: binary search
                for (; disi.index < disi.nextBlockIndex;) {
                    int doc = Short.toUnsignedInt(disi.slice.readShort());
                    disi.index++;
                    if (doc >= targetInBlock) {
                        disi.doc = disi.block | doc;
                        disi.exists = true;
                        return true;
                    }
                }
                return false;
            }

            @Override
            boolean advanceExactWithinBlock(IndexedDISI disi, int target) throws IOException {
                final int targetInBlock = target & 0xFFFF;
                // TODO: binary search
                if (target == disi.doc) {
                    return disi.exists;
                }
                for (; disi.index < disi.nextBlockIndex;) {
                    int doc = Short.toUnsignedInt(disi.slice.readShort());
                    disi.index++;
                    if (doc >= targetInBlock) {
                        if (doc != targetInBlock) {
                            disi.index--;
                            disi.slice.seek(disi.slice.getFilePointer() - Short.BYTES);
                            break;
                        }
                        disi.exists = true;
                        return true;
                    }
                }
                disi.exists = false;
                return false;
            }
        },
        DENSE {
            @Override
            boolean advanceWithinBlock(IndexedDISI disi, int target) throws IOException {
                final int targetInBlock = target & 0xFFFF;
                final int targetWordIndex = targetInBlock >>> 6;

                // If possible, skip ahead using the rank cache
                // If the distance between the current position and the target is < rank-longs
                // there is no sense in using rank
                if (disi.denseRankPower != -1 && targetWordIndex - disi.wordIndex >= (1 << (disi.denseRankPower - 6))) {
                    rankSkip(disi, targetInBlock);
                }

                for (int i = disi.wordIndex + 1; i <= targetWordIndex; ++i) {
                    disi.word = disi.slice.readLong();
                    disi.numberOfOnes += Long.bitCount(disi.word);
                }
                disi.wordIndex = targetWordIndex;

                long leftBits = disi.word >>> target;
                if (leftBits != 0L) {
                    disi.doc = target + Long.numberOfTrailingZeros(leftBits);
                    disi.index = disi.numberOfOnes - Long.bitCount(leftBits);
                    return true;
                }

                // There were no set bits at the wanted position. Move forward until one is reached
                while (++disi.wordIndex < 1024) {
                    // This could use the rank cache to skip empty spaces >= 512 bits, but it seems
                    // unrealistic
                    // that such blocks would be DENSE
                    disi.word = disi.slice.readLong();
                    if (disi.word != 0) {
                        disi.index = disi.numberOfOnes;
                        disi.numberOfOnes += Long.bitCount(disi.word);
                        disi.doc = disi.block | (disi.wordIndex << 6) | Long.numberOfTrailingZeros(disi.word);
                        return true;
                    }
                }
                // No set bits in the block at or after the wanted position.
                return false;
            }

            @Override
            boolean advanceExactWithinBlock(IndexedDISI disi, int target) throws IOException {
                final int targetInBlock = target & 0xFFFF;
                final int targetWordIndex = targetInBlock >>> 6;

                // If possible, skip ahead using the rank cache
                // If the distance between the current position and the target is < rank-longs
                // there is no sense in using rank
                if (disi.denseRankPower != -1 && targetWordIndex - disi.wordIndex >= (1 << (disi.denseRankPower - 6))) {
                    rankSkip(disi, targetInBlock);
                }

                for (int i = disi.wordIndex + 1; i <= targetWordIndex; ++i) {
                    disi.word = disi.slice.readLong();
                    disi.numberOfOnes += Long.bitCount(disi.word);
                }
                disi.wordIndex = targetWordIndex;

                long leftBits = disi.word >>> target;
                disi.index = disi.numberOfOnes - Long.bitCount(leftBits);
                return (leftBits & 1L) != 0;
            }
        },
        ALL {
            @Override
            boolean advanceWithinBlock(IndexedDISI disi, int target) {
                disi.doc = target;
                disi.index = target - disi.gap;
                return true;
            }

            @Override
            boolean advanceExactWithinBlock(IndexedDISI disi, int target) {
                disi.index = target - disi.gap;
                return true;
            }
        };

        /**
         * Advance to the first doc from the block that is equal to or greater than {@code target}.
         * Return true if there is such a doc and false otherwise.
         */
        abstract boolean advanceWithinBlock(IndexedDISI disi, int target) throws IOException;

        /**
         * Advance the iterator exactly to the position corresponding to the given {@code target} and
         * return whether this document exists.
         */
        abstract boolean advanceExactWithinBlock(IndexedDISI disi, int target) throws IOException;
    }

    /**
     * If the distance between the current position and the target is > 8 words, the rank cache will
     * be used to guarantee a worst-case of 1 rank-lookup and 7 word-read-and-count-bits operations.
     * Note: This does not guarantee a skip up to target, only up to nearest rank boundary. It is the
     * responsibility of the caller to iterate further to reach target.
     *
     * @param disi standard DISI.
     * @param targetInBlock lower 16 bits of the target
     * @throws IOException if a DISI seek failed.
     */
    private static void rankSkip(IndexedDISI disi, int targetInBlock) throws IOException {
        assert disi.denseRankPower >= 0 : disi.denseRankPower;
        // Resolve the rank as close to targetInBlock as possible (maximum distance is 8 longs)
        // Note: rankOrigoOffset is tracked on block open, so it is absolute (e.g. don't add origo)
        final int rankIndex = targetInBlock >> disi.denseRankPower; // Default is 9 (8 longs: 2^3 * 2^6 = 512 docIDs)

        final int rank = (disi.denseRankTable[rankIndex << 1] & 0xFF) << 8 | (disi.denseRankTable[(rankIndex << 1) + 1] & 0xFF);

        // Position the counting logic just after the rank point
        final int rankAlignedWordIndex = rankIndex << disi.denseRankPower >> 6;
        disi.slice.seek(disi.denseBitmapOffset + rankAlignedWordIndex * (long) Long.BYTES);
        long rankWord = disi.slice.readLong();
        int denseNOO = rank + Long.bitCount(rankWord);

        disi.wordIndex = rankAlignedWordIndex;
        disi.word = rankWord;
        disi.numberOfOnes = disi.denseOrigoIndex + denseNOO;
    }
}
