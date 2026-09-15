/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.columnar.numeric.NumericColumnReader;
import org.elasticsearch.columnar.substrate.MonotonicReader;

import java.io.IOException;

/**
 * Answers where a document's slots begin and how many it has, from the counts the column holds.
 *
 * <p>A block of counts is decoded and summed once, from that block's own base, into the address of every
 * rank it covers. Both questions are then an array read, and only the block a read is in is held.
 */
final class SlotAddressReader {

    private final NumericColumnReader counts;
    private final LongValues bases;
    private final int numDocsWithField;
    private final int blockSize;
    private final int blockShift;
    private final int blockMask;

    /** Addresses of the ranks in the decoded block, and one past its last, so a count is a difference. */
    private final long[] addresses;

    private long cachedBlock = -1;

    SlotAddressReader(SlotAddressing addressing, int numDocsWithField, IndexInput data) throws IOException {
        this.counts = new NumericColumnReader(addressing.counts(), data);
        this.numDocsWithField = numDocsWithField;
        this.blockSize = addressing.counts().blockSize();
        assert (blockSize & (blockSize - 1)) == 0 : "counts per block must be a power of two, got " + blockSize;
        this.blockShift = Integer.numberOfTrailingZeros(blockSize);
        this.blockMask = blockSize - 1;
        this.addresses = new long[blockSize + 1];
        this.bases = MonotonicReader.open(
            data,
            addressing.bases().meta(),
            SlotAddressing.numBlocks(numDocsWithField, blockSize),
            addressing.bases().dataOffset(),
            addressing.bases().dataLength()
        );
    }

    /** The value address of the document at {@code rank}. */
    long firstValueAddress(int rank) throws IOException {
        ensureBlock(rank >>> blockShift);
        return addresses[rank & blockMask];
    }

    /** How many slots the document at {@code rank} holds. */
    long valueCount(int rank) throws IOException {
        ensureBlock(rank >>> blockShift);
        final int at = rank & blockMask;
        return addresses[at + 1] - addresses[at];
    }

    private void ensureBlock(long block) throws IOException {
        if (block == cachedBlock) {
            return;
        }
        final long[] decoded = counts.block(block);
        // The last block holds the remainder; the addresses past it are never asked for.
        final int inBlock = (int) Math.min(blockSize, numDocsWithField - (block << blockShift));
        long address = bases.get(block);
        for (int i = 0; i < inBlock; i++) {
            addresses[i] = address;
            address += decoded[i];
        }
        addresses[inBlock] = address;
        cachedBlock = block;
    }
}
