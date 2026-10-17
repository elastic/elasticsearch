/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.store;

import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

import java.io.IOException;

/**
 * Prefetches what {@link IndexedDISI#advanceExact} reads for a batch of documents, in two steps: the jump-table entries of
 * their blocks, then what each block reads, by how the block is stored, which two consecutive entries tell:
 * <ul>
 *   <li>all documents: the header;</li>
 *   <li>a bitmap: the header, the rank table and the words from the rank point, or from the last word read;</li>
 *   <li>a list: the header and the whole list, which is scanned up to the document.</li>
 * </ul>
 * A structure within a single read unit, or without a jump table, is asked for whole by the first step.
 */
public final class IndexedDISIPrefetcher {

    private static final int BLOCK_SHIFT = 16;
    private static final int BLOCK_SIZE = 1 << BLOCK_SHIFT;
    /** The most documents a block lists rather than maps. */
    private static final int MAX_ARRAY_LENGTH = (1 << 12) - 1;
    private static final int HEADER_BYTES = 2 * Short.BYTES;
    private static final int BITMAP_WORDS = BLOCK_SIZE / Long.SIZE;
    private static final int JUMP_ENTRY_BYTES = 2 * Integer.BYTES;

    private final PrefetchRanges prefetches;
    private final IndexInput data;
    private final long offset;
    private final long length;
    private final int jumpTableEntryCount;
    private final byte denseRankPower;
    private final long cost;
    private final long jumpTableOffset;
    private final int rankTableBytes;

    private RandomAccessInput jumpTable;
    private boolean askedWhole;
    /** Where a reader that read the documents asked for so far stands: its block and bitmap word. */
    private int lastBlock = -1;
    private int lastWord = -1;
    private IndexedDISI disi;

    /** Asks through {@code prefetches}. */
    public IndexedDISIPrefetcher(
        PrefetchRanges prefetches,
        IndexInput data,
        long offset,
        long length,
        int jumpTableEntryCount,
        byte denseRankPower,
        long cost
    ) {
        this.prefetches = prefetches;
        this.data = prefetches.wrap(data.clone());
        this.offset = offset;
        this.length = length;
        this.jumpTableEntryCount = jumpTableEntryCount;
        this.denseRankPower = denseRankPower;
        this.cost = cost;
        this.jumpTableOffset = offset + length - (jumpTableEntryCount > 0 ? (long) jumpTableEntryCount * JUMP_ENTRY_BYTES : 0);
        this.rankTableBytes = denseRankPower == -1 ? 0 : BITMAP_WORDS >> (denseRankPower - 7);
    }

    /** First step: the jump-table entries of the blocks of {@code docs[from, to)}. */
    public boolean askJumpEntries(int[] docs, int from, int to) throws IOException {
        if (askedWhole) {
            return false;
        }
        if (jumpTableEntryCount <= 0 || prefetches.withinOneUnit(offset, length)) {
            askedWhole = true;
            data.prefetch(offset, length);
            return true;
        }
        int last = -1;
        for (int i = from; i < to; i++) {
            int entry = entry(docs[i] >>> BLOCK_SHIFT);
            if (entry != last) {
                last = entry;
                int entries = entry + 1 < jumpTableEntryCount ? 2 : 1;
                data.prefetch(jumpTableOffset + (long) entry * JUMP_ENTRY_BYTES, (long) entries * JUMP_ENTRY_BYTES);
            }
        }
        return true;
    }

    /** Second step: what the blocks the entries locate read. */
    public boolean askBlocks(int[] docs, int from, int to) throws IOException {
        if (askedWhole) {
            return false;
        }
        if (jumpTable == null) {
            jumpTable = data.randomAccessSlice(jumpTableOffset, (long) jumpTableEntryCount * JUMP_ENTRY_BYTES);
        }
        for (int i = from; i < to; i++) {
            int block = docs[i] >>> BLOCK_SHIFT;
            int entry = entry(block);
            long start = offset + jumpTable.readInt((long) entry * JUMP_ENTRY_BYTES + Integer.BYTES);
            if (block != lastBlock) {
                if (lastBlock == -1 && block == 1) {
                    // A new reader reaches the second block through the header of the first.
                    data.prefetch(offset, HEADER_BYTES);
                }
                lastBlock = block;
                lastWord = -1;
            }
            int documents = entry + 1 < jumpTableEntryCount && entry == block
                ? jumpTable.readInt((long) (entry + 1) * JUMP_ENTRY_BYTES) - jumpTable.readInt((long) entry * JUMP_ENTRY_BYTES)
                : 0;
            if (documents == 0) {
                // An empty block: the reader reads the header, and rank table, of the next block that has documents.
                data.prefetch(start, Math.min(HEADER_BYTES + rankTableBytes, jumpTableOffset - start));
            } else if (documents == BLOCK_SIZE) {
                data.prefetch(start, HEADER_BYTES);
            } else if (documents <= MAX_ARRAY_LENGTH) {
                data.prefetch(start, HEADER_BYTES + 2L * documents);
            } else {
                askBitmapWords(start, docs[i] & (BLOCK_SIZE - 1));
            }
        }
        return true;
    }

    /** What finding {@code inBlock} in the bitmap block at {@code start} reads. */
    private void askBitmapWords(long start, int inBlock) throws IOException {
        long bitmap = start + HEADER_BYTES + rankTableBytes;
        int word = inBlock >>> 6;
        if (lastWord == -1) {
            data.prefetch(start, HEADER_BYTES + rankTableBytes);
        }
        int first;
        if (denseRankPower != -1 && word - lastWord >= (1 << (denseRankPower - 6))) {
            first = (inBlock >> denseRankPower) << denseRankPower >> 6;
        } else {
            first = lastWord + 1;
        }
        if (first <= word) {
            data.prefetch(bitmap + (long) first * Long.BYTES, (long) (word - first + 1) * Long.BYTES);
        }
        lastWord = Math.max(lastWord, word);
    }

    /** The jump-table entry for {@code block}, the last one for a block past the end. */
    private int entry(int block) {
        return Math.min(block, jumpTableEntryCount - 1);
    }

    /** The rank of each document of {@code docs[from, to)}, or {@code -1} for one without a value. */
    public void ranks(int[] docs, int from, int to, long[] ranks) throws IOException {
        if (disi == null || disi.docID() > docs[from]) {
            disi = new IndexedDISI(data, offset, length, jumpTableEntryCount, denseRankPower, cost);
        }
        for (int i = from; i < to; i++) {
            ranks[i - from] = disi.advanceExact(docs[i]) ? disi.index() : -1;
        }
    }
}
