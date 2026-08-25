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

    private final int docCount;

    /** Full-hash accumulator, {@link MurmurHash3#STATE_SIZE} words per row. */
    private final long[] fullState;
    /** Name-similarity accumulator, {@link MurmurHash3#STATE_SIZE} words per row. */
    private final long[] nameState;
    /** Half-block of the name-similarity stream awaiting its partner; valid when the count is odd. */
    private final long[] namePending;
    /** Dimensions folded per row. Drives both stream lengths and the name stream's tail parity. */
    private final int[] entryCount;

    // Three distinct holders: a finished full hash stays live while the prefix byte is derived, and
    // deriving it overwrites `scratch`.
    private final MurmurHash3.Hash128 fullHash = new MurmurHash3.Hash128();
    private final MurmurHash3.Hash128 nameHash = new MurmurHash3.Hash128();
    final MurmurHash3.Hash128 scratch = new MurmurHash3.Hash128();

    static ColumnarTsidAccumulator create(int docCount, boolean singleBytePrefixLayout) {
        return singleBytePrefixLayout ? new SingleBytePrefix(docCount) : new MultiBytePrefix(docCount);
    }

    private ColumnarTsidAccumulator(int docCount) {
        this.docCount = docCount;
        // Seed is 0, so zero-filled arrays are already correctly initialised accumulator states.
        this.fullState = new long[docCount * MurmurHash3.STATE_SIZE];
        this.nameState = new long[docCount * MurmurHash3.STATE_SIZE];
        this.namePending = new long[docCount];
        this.entryCount = new int[docCount];
    }

    /**
     * Folds one dimension value into {@code row}.
     *
     * @param pathGroup  id of the value's path, equal for consecutive values sharing one. Keyed on path
     *                   equality rather than column identity, because two leaf columns can report the
     *                   same full path (dotted and nested spellings are not merged).
     * @param prefixRank {@link TsidBuilder#prefixByteRank} of the value's path
     */
    final void add(int row, long pathH1, long pathH2, long valueH1, long valueH2, int pathGroup, int prefixRank) {
        int stateOffset = row * MurmurHash3.STATE_SIZE;
        int count = entryCount[row];

        MurmurHash3.mixTwoBlocks(fullState, stateOffset, pathH1, pathH2, valueH1, valueH2);

        // Name-similarity stream: buffer on even counts, complete the block on odd ones.
        long nameWord = pathH1 ^ pathH2;
        if ((count & 1) == 0) {
            namePending[row] = nameWord;
        } else {
            MurmurHash3.mixBlock(nameState, stateOffset, namePending[row], nameWord);
        }

        addPrefixInput(row, valueH1, valueH2, pathGroup, prefixRank);

        entryCount[row] = count + 1;
    }

    /** @throws IllegalArgumentException if any row received no dimension values */
    final BytesRef[] build() {
        BytesRef[] tsids = new BytesRef[docCount];
        for (int row = 0; row < docCount; row++) {
            int count = entryCount[row];
            TsidBuilder.throwIfNoDimensions(count);
            int stateOffset = row * MurmurHash3.STATE_SIZE;
            MurmurHash3.finalizeAlignedHash(fullHash, count * FULL_HASH_BYTES_PER_DIMENSION, fullState, stateOffset);
            tsids[row] = finish(row, count, fullHash);
        }
        return tsids;
    }

    /** Records whatever this layout derives its prefix bytes from. */
    abstract void addPrefixInput(int row, long valueH1, long valueH2, int pathGroup, int prefixRank);

    /** Assembles one row's tsid from its finished full hash. */
    abstract BytesRef finish(int row, int count, MurmurHash3.Hash128 fullHash);

    /** Completes the name-similarity stream. The returned holder is shared until the next call. */
    final MurmurHash3.Hash128 finalizeNameHash(int row, int count) {
        int byteLength = count * NAME_HASH_BYTES_PER_DIMENSION;
        int stateOffset = row * MurmurHash3.STATE_SIZE;
        if ((count & 1) == 0) {
            return MurmurHash3.finalizeAlignedHash(nameHash, byteLength, nameState, stateOffset);
        }
        return MurmurHash3.finalizeHashWithLongTail(nameHash, byteLength, nameState, stateOffset, namePending[row]);
    }

    /**
     * Takes the prefix byte from the row's lowest-ranked special dimension, or from the
     * name-similarity stream when it has none.
     */
    private static final class SingleBytePrefix extends ColumnarTsidAccumulator {

        /** Lowest rank seen per row, and the value hash that produced it. */
        private final int[] bestRank;
        private final long[] bestValue;

        SingleBytePrefix(int docCount) {
            super(docCount);
            this.bestRank = new int[docCount];
            Arrays.fill(this.bestRank, TsidBuilder.PREFIX_RANK_NONE);
            this.bestValue = new long[docCount * 2];
        }

        @Override
        void addPrefixInput(int row, long valueH1, long valueH2, int pathGroup, int prefixRank) {
            // Strict `<` keeps the first occurrence, so an array-valued special field uses its first
            // value, as the row path's sorted-order lookup does.
            if (prefixRank < bestRank[row]) {
                bestRank[row] = prefixRank;
                bestValue[row * 2] = valueH1;
                bestValue[row * 2 + 1] = valueH2;
            }
        }

        @Override
        BytesRef finish(int row, int count, MurmurHash3.Hash128 fullHash) {
            // The name stream is only needed when no special dimension claimed the prefix byte.
            MurmurHash3.Hash128 nameSimilarityHash = bestRank[row] == TsidBuilder.PREFIX_RANK_NONE ? finalizeNameHash(row, count) : null;
            byte prefixByte = TsidBuilder.singleBytePrefix(
                bestRank[row],
                bestValue[row * 2],
                bestValue[row * 2 + 1],
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
        /** Path group that last contributed a byte, per row; the dedup cursor. */
        private final int[] lastPathGroup;
        /** One row's bytes, reused across rows; only {@code [0, emitted)} is ever read. */
        private final byte[] rowValueBytes = new byte[TsidBuilder.MAX_TSID_VALUE_SIMILARITY_FIELDS];

        MultiBytePrefix(int docCount) {
            super(docCount);
            this.valueSimilarityBytes = new byte[docCount * TsidBuilder.MAX_TSID_VALUE_SIMILARITY_FIELDS];
            this.valueSimilarityCount = new int[docCount];
            this.lastPathGroup = new int[docCount];
            Arrays.fill(this.lastPathGroup, TsidBuilder.NO_PATH_GROUP);
        }

        @Override
        void addPrefixInput(int row, long valueH1, long valueH2, int pathGroup, int prefixRank) {
            // Only the first value of each distinct path contributes, as the row path's sorted-order
            // skip-if-same-as-previous does.
            if (valueSimilarityCount[row] < TsidBuilder.MAX_TSID_VALUE_SIMILARITY_FIELDS && pathGroup != lastPathGroup[row]) {
                int slot = row * TsidBuilder.MAX_TSID_VALUE_SIMILARITY_FIELDS + valueSimilarityCount[row]++;
                valueSimilarityBytes[slot] = TsidBuilder.similarityByte(valueH1, valueH2, scratch);
                lastPathGroup[row] = pathGroup;
            }
        }

        @Override
        BytesRef finish(int row, int count, MurmurHash3.Hash128 fullHash) {
            byte nameSimilarityByte = TsidBuilder.similarityByte(finalizeNameHash(row, count));
            int emitted = valueSimilarityCount[row];
            System.arraycopy(valueSimilarityBytes, row * TsidBuilder.MAX_TSID_VALUE_SIMILARITY_FIELDS, rowValueBytes, 0, emitted);
            return TsidBuilder.writeMultiBytePrefixTsid(nameSimilarityByte, rowValueBytes, emitted, fullHash);
        }
    }
}
