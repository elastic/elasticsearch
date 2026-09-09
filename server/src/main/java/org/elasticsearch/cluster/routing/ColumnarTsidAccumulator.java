/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.core.Nullable;

import java.util.Arrays;

/**
 * Accumulates one tsid per document row while dimension values are visited <b>column by column</b>,
 * without buffering the values themselves.
 *
 * <p>That works because murmur3-128's streaming state is just {@code (h1, h2)} plus a tail, and each
 * dimension contributes a whole number of 16-byte blocks to the two streams a tsid needs. The full
 * hash takes 4 words per dimension — 32 bytes, exactly two blocks — so it is always block aligned.
 * Name similarity takes 1 word, half a block, so one pending word per row is held until the next
 * dimension completes the block.
 *
 * <p>Callers must present each row's dimensions in {@code (path, insertion order)} order, which a
 * column-major scan gets for free by visiting columns sorted by path.
 *
 * <p>Obtain an instance with {@link #create}; the two tsid layouts differ only in their prefix bytes.
 */
abstract class ColumnarTsidAccumulator {

    private static final int FULL_HASH_BYTES_PER_DIMENSION = 4 * Long.BYTES;
    private static final int NAME_HASH_BYTES_PER_DIMENSION = Long.BYTES;

    private final int slotCount;

    /**
     * When non-null, maps {@code batch row index → accumulator slot}; rows with value {@code -1} are
     * outside the subset and will be silently skipped in {@link #add}. When null, slot == row.
     */
    @Nullable
    private final int[] rowToSlot;

    /** Full-hash accumulator, {@link MurmurHash3#STATE_SIZE} words per slot. */
    private final long[] fullState;
    /** Name-similarity accumulator, {@link MurmurHash3#STATE_SIZE} words per slot. */
    private final long[] nameState;
    /** Half-block of the name-similarity stream awaiting its partner; valid when the count is odd. */
    private final long[] namePending;
    /** Dimensions folded per slot. Drives both stream lengths and the name stream's tail parity. */
    private final int[] entryCount;

    // Three distinct holders: a finished full hash stays live while the prefix byte is derived, and
    // deriving it overwrites `scratch`.
    private final MurmurHash3.Hash128 fullHash = new MurmurHash3.Hash128();
    private final MurmurHash3.Hash128 nameHash = new MurmurHash3.Hash128();
    final MurmurHash3.Hash128 scratch = new MurmurHash3.Hash128();

    /**
     * Creates an accumulator for all rows of a batch.
     *
     * @param docCount            total number of rows in the batch
     * @param singleBytePrefixLayout selects the tsid layout
     */
    static ColumnarTsidAccumulator create(int docCount, boolean singleBytePrefixLayout) {
        return singleBytePrefixLayout ? new SingleBytePrefix(docCount, null) : new MultiBytePrefix(docCount, null);
    }

    /**
     * Creates an accumulator for a subset of rows.
     *
     * <p>When {@code rows} is non-null the scan loops visit all rows in the source batch but only
     * the rows listed in {@code rows[]} contribute to the hash; rows outside the subset are silently
     * skipped. The returned array from {@link #build()} has length {@code rows.length}, with
     * {@code result[k]} being the tsid for {@code rows[k]}.
     *
     * <p>When {@code rows} is null the behaviour is identical to
     * {@link #create(int, boolean)}: every row contributes and the result has length {@code docCount}.
     *
     * @param docCount            total number of rows in the source batch (used to size the rowToSlot map)
     * @param rows                batch row indices in the subset, or null for all rows
     * @param singleBytePrefixLayout selects the tsid layout
     */
    static ColumnarTsidAccumulator create(int docCount, @Nullable int[] rows, boolean singleBytePrefixLayout) {
        if (rows == null) {
            return create(docCount, singleBytePrefixLayout);
        }
        // Build the inverse map: rowToSlot[batchRow] = slot in result array, or -1 when not in subset.
        int[] rowToSlot = new int[docCount];
        Arrays.fill(rowToSlot, -1);
        for (int k = 0; k < rows.length; k++) {
            rowToSlot[rows[k]] = k;
        }
        return singleBytePrefixLayout ? new SingleBytePrefix(rows.length, rowToSlot) : new MultiBytePrefix(rows.length, rowToSlot);
    }

    private ColumnarTsidAccumulator(int slotCount, @Nullable int[] rowToSlot) {
        this.slotCount = slotCount;
        this.rowToSlot = rowToSlot;
        // Seed is 0, so zero-filled arrays are already correctly initialised accumulator states.
        this.fullState = new long[slotCount * MurmurHash3.STATE_SIZE];
        this.nameState = new long[slotCount * MurmurHash3.STATE_SIZE];
        this.namePending = new long[slotCount];
        this.entryCount = new int[slotCount];
    }

    /**
     * Folds one dimension value into {@code row}. When a row-subset was specified at creation, rows
     * outside the subset are silently ignored.
     *
     * @param pathGroup  id of the value's path, equal for consecutive values sharing one. Keyed on path
     *                   equality rather than column identity, because two leaf columns can report the
     *                   same full path (dotted and nested spellings are not merged).
     * @param prefixRank {@link TsidBuilder#prefixByteRank} of the value's path
     */
    final void add(int row, long pathH1, long pathH2, long valueH1, long valueH2, int pathGroup, int prefixRank) {
        int slot = rowToSlot == null ? row : rowToSlot[row];
        if (slot < 0) {
            // row is outside the subset; skip it
            return;
        }
        int stateOffset = slot * MurmurHash3.STATE_SIZE;
        int count = entryCount[slot];

        MurmurHash3.mixTwoBlocks(fullState, stateOffset, pathH1, pathH2, valueH1, valueH2);

        // Name-similarity stream: buffer on even counts, complete the block on odd ones.
        long nameWord = pathH1 ^ pathH2;
        if ((count & 1) == 0) {
            namePending[slot] = nameWord;
        } else {
            MurmurHash3.mixBlock(nameState, stateOffset, namePending[slot], nameWord);
        }

        addPrefixInput(slot, valueH1, valueH2, pathGroup, prefixRank);

        entryCount[slot] = count + 1;
    }

    /** @throws IllegalArgumentException if any row in the set received no dimension values */
    final BytesRef[] build() {
        BytesRef[] tsids = new BytesRef[slotCount];
        for (int slot = 0; slot < slotCount; slot++) {
            int count = entryCount[slot];
            TsidBuilder.throwIfNoDimensions(count);
            int stateOffset = slot * MurmurHash3.STATE_SIZE;
            MurmurHash3.finalizeAlignedHash(fullHash, count * FULL_HASH_BYTES_PER_DIMENSION, fullState, stateOffset);
            tsids[slot] = finish(slot, count, fullHash);
        }
        return tsids;
    }

    /** Records whatever this layout derives its prefix bytes from. {@code slot} is the accumulator slot, not the batch row. */
    abstract void addPrefixInput(int slot, long valueH1, long valueH2, int pathGroup, int prefixRank);

    /** Assembles one slot's tsid from its finished full hash. */
    abstract BytesRef finish(int slot, int count, MurmurHash3.Hash128 fullHash);

    /** Completes the name-similarity stream for the given accumulator slot. The returned holder is shared until the next call. */
    final MurmurHash3.Hash128 finalizeNameHash(int slot, int count) {
        int byteLength = count * NAME_HASH_BYTES_PER_DIMENSION;
        int stateOffset = slot * MurmurHash3.STATE_SIZE;
        if ((count & 1) == 0) {
            return MurmurHash3.finalizeAlignedHash(nameHash, byteLength, nameState, stateOffset);
        }
        return MurmurHash3.finalizeHashWithLongTail(nameHash, byteLength, nameState, stateOffset, namePending[slot]);
    }

    /**
     * Takes the prefix byte from the row's lowest-ranked special dimension, or from the
     * name-similarity stream when it has none.
     */
    private static final class SingleBytePrefix extends ColumnarTsidAccumulator {

        /** Lowest rank seen per slot, and the value hash that produced it. */
        private final int[] bestRank;
        private final long[] bestValue;

        SingleBytePrefix(int slotCount, @Nullable int[] rowToSlot) {
            super(slotCount, rowToSlot);
            this.bestRank = new int[slotCount];
            Arrays.fill(this.bestRank, TsidBuilder.PREFIX_RANK_NONE);
            this.bestValue = new long[slotCount * 2];
        }

        @Override
        void addPrefixInput(int slot, long valueH1, long valueH2, int pathGroup, int prefixRank) {
            // Strict `<` keeps the first occurrence, so an array-valued special field uses its first
            // value, as the row path's sorted-order lookup does.
            if (prefixRank < bestRank[slot]) {
                bestRank[slot] = prefixRank;
                bestValue[slot * 2] = valueH1;
                bestValue[slot * 2 + 1] = valueH2;
            }
        }

        @Override
        BytesRef finish(int slot, int count, MurmurHash3.Hash128 fullHash) {
            // The name stream is only needed when no special dimension claimed the prefix byte.
            MurmurHash3.Hash128 nameSimilarityHash = bestRank[slot] == TsidBuilder.PREFIX_RANK_NONE ? finalizeNameHash(slot, count) : null;
            byte prefixByte = TsidBuilder.singleBytePrefix(
                bestRank[slot],
                bestValue[slot * 2],
                bestValue[slot * 2 + 1],
                nameSimilarityHash,
                scratch
            );
            return TsidBuilder.writeSingleBytePrefixTsid(prefixByte, fullHash);
        }
    }

    /**
     * The legacy layout: a name-similarity byte, then a value-similarity byte for the first value of
     * each distinct path, capped at {@link TsidBuilder#MAX_TSID_VALUE_SIMILARITY_FIELDS}.
     */
    private static final class MultiBytePrefix extends ColumnarTsidAccumulator {

        private final byte[] valueSimilarityBytes;
        private final int[] valueSimilarityCount;
        /** Path group that last contributed a byte, per slot; the dedup cursor. */
        private final int[] lastPathGroup;
        /** One slot's bytes, reused across slots; only {@code [0, emitted)} is ever read. */
        private final byte[] rowValueBytes = new byte[TsidBuilder.MAX_TSID_VALUE_SIMILARITY_FIELDS];

        MultiBytePrefix(int slotCount, @Nullable int[] rowToSlot) {
            super(slotCount, rowToSlot);
            this.valueSimilarityBytes = new byte[slotCount * TsidBuilder.MAX_TSID_VALUE_SIMILARITY_FIELDS];
            this.valueSimilarityCount = new int[slotCount];
            this.lastPathGroup = new int[slotCount];
            Arrays.fill(this.lastPathGroup, TsidBuilder.NO_PATH_GROUP);
        }

        @Override
        void addPrefixInput(int slot, long valueH1, long valueH2, int pathGroup, int prefixRank) {
            // Only the first value of each distinct path contributes, as the row path's sorted-order
            // skip-if-same-as-previous does.
            if (valueSimilarityCount[slot] < TsidBuilder.MAX_TSID_VALUE_SIMILARITY_FIELDS && pathGroup != lastPathGroup[slot]) {
                int idx = slot * TsidBuilder.MAX_TSID_VALUE_SIMILARITY_FIELDS + valueSimilarityCount[slot]++;
                valueSimilarityBytes[idx] = TsidBuilder.similarityByte(valueH1, valueH2, scratch);
                lastPathGroup[slot] = pathGroup;
            }
        }

        @Override
        BytesRef finish(int slot, int count, MurmurHash3.Hash128 fullHash) {
            byte nameSimilarityByte = TsidBuilder.similarityByte(finalizeNameHash(slot, count));
            int emitted = valueSimilarityCount[slot];
            System.arraycopy(valueSimilarityBytes, slot * TsidBuilder.MAX_TSID_VALUE_SIMILARITY_FIELDS, rowValueBytes, 0, emitted);
            return TsidBuilder.writeMultiBytePrefixTsid(nameSimilarityByte, rowValueBytes, emitted, fullHash);
        }
    }
}
