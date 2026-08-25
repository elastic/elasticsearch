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
 * <p>That works because murmur3-128's streaming state is just {@code (h1, h2)} plus a tail, and
 * each dimension contributes a whole number of 16-byte blocks to the streams that need it:
 * <ul>
 *   <li><b>Full hash</b> — 4 words ({@code pathH1, pathH2, valueH1, valueH2}) = 32 bytes = exactly
 *       two blocks, so this stream is always block aligned and needs no tail storage.</li>
 *   <li><b>Name similarity</b> — 1 word ({@code pathH1 ^ pathH2}) = 8 bytes = half a block, so one
 *       pending word per row is held until the next entry completes the block.</li>
 * </ul>
 *
 * <p>Callers must present each row's dimensions in {@code (path, insertion order)} order, which a
 * column-major scan gets for free by visiting columns sorted by path. All layout decisions are
 * delegated to the shared statics on {@link TsidBuilder} so the two paths cannot drift.
 */
final class ColumnarTsidAccumulator {

    /** Bytes each dimension contributes to the full-hash stream: four 64-bit words. */
    private static final int FULL_HASH_BYTES_PER_DIMENSION = 4 * Long.BYTES;
    /** Bytes each dimension contributes to the name-similarity stream: one 64-bit word. */
    private static final int NAME_HASH_BYTES_PER_DIMENSION = Long.BYTES;

    private final int docCount;
    private final boolean singleBytePrefixLayout;

    /** Full-hash accumulator, {@link MurmurHash3#STATE_SIZE} words per row. */
    private final long[] fullState;
    /** Name-similarity accumulator, {@link MurmurHash3#STATE_SIZE} words per row. */
    private final long[] nameState;
    /** Half-block of the name-similarity stream awaiting its partner; valid when the count is odd. */
    private final long[] namePending;
    /** Dimension entries folded per row. Drives both stream lengths and the tail parity. */
    private final int[] entryCount;

    // Single-byte layout only: lowest prefix rank seen per row and the value hash that produced it.
    private final int[] bestRank;
    private final long[] bestValue;

    // Multi-byte layout only: the emitted value-similarity bytes and the dedup cursor per row.
    private final byte[] valueSimilarityBytes;
    private final int[] valueSimilarityCount;
    private final int[] lastPathGroup;

    private final MurmurHash3.Hash128 scratch = new MurmurHash3.Hash128();

    ColumnarTsidAccumulator(int docCount, boolean singleBytePrefixLayout) {
        this.docCount = docCount;
        this.singleBytePrefixLayout = singleBytePrefixLayout;

        // Seed is 0, so zero-filled arrays are already correctly initialised accumulator states.
        this.fullState = new long[docCount * MurmurHash3.STATE_SIZE];
        this.nameState = new long[docCount * MurmurHash3.STATE_SIZE];
        this.namePending = new long[docCount];
        this.entryCount = new int[docCount];

        if (singleBytePrefixLayout) {
            this.bestRank = new int[docCount];
            Arrays.fill(this.bestRank, TsidBuilder.PREFIX_RANK_NONE);
            this.bestValue = new long[docCount * 2];
            this.valueSimilarityBytes = null;
            this.valueSimilarityCount = null;
            this.lastPathGroup = null;
        } else {
            this.bestRank = null;
            this.bestValue = null;
            this.valueSimilarityBytes = new byte[docCount * TsidBuilder.MAX_TSID_VALUE_SIMILARITY_FIELDS];
            this.valueSimilarityCount = new int[docCount];
            this.lastPathGroup = new int[docCount];
            Arrays.fill(this.lastPathGroup, TsidBuilder.NO_PATH_GROUP);
        }
    }

    /**
     * Folds one dimension value into {@code row}.
     *
     * @param pathGroup  id of the value's path, equal for consecutive values sharing a path. Keyed on
     *                   path equality rather than column identity, because two leaf columns can
     *                   report the same full path (dotted and nested spellings are not merged).
     * @param prefixRank {@link TsidBuilder#prefixByteRank} of the value's path
     */
    void add(int row, long pathH1, long pathH2, long valueH1, long valueH2, int pathGroup, int prefixRank) {
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

        if (singleBytePrefixLayout) {
            // Strict `<` keeps the first occurrence, so an array-valued special field uses its first
            // value, matching the row path's "first dimension with this path in sorted order".
            if (prefixRank < bestRank[row]) {
                bestRank[row] = prefixRank;
                bestValue[row * 2] = valueH1;
                bestValue[row * 2 + 1] = valueH2;
            }
        } else if (TsidBuilder.emitsValueSimilarityByte(valueSimilarityCount[row], pathGroup, lastPathGroup[row])) {
            int slot = row * TsidBuilder.MAX_TSID_VALUE_SIMILARITY_FIELDS + valueSimilarityCount[row]++;
            valueSimilarityBytes[slot] = TsidBuilder.similarityByte(valueH1, valueH2, scratch);
            lastPathGroup[row] = pathGroup;
        }

        entryCount[row] = count + 1;
    }

    /**
     * Finalises every row.
     *
     * @throws IllegalArgumentException if any row received no dimension values
     */
    BytesRef[] build() {
        BytesRef[] tsids = new BytesRef[docCount];
        // Distinct instances: the full hash is still live while the prefix byte is derived, and the
        // prefix derivation overwrites `scratch`.
        MurmurHash3.Hash128 fullHash = new MurmurHash3.Hash128();
        MurmurHash3.Hash128 nameHash = new MurmurHash3.Hash128();
        byte[] valueBytes = singleBytePrefixLayout ? null : new byte[TsidBuilder.MAX_TSID_VALUE_SIMILARITY_FIELDS];

        for (int row = 0; row < docCount; row++) {
            int count = entryCount[row];
            TsidBuilder.throwIfNoDimensions(count);
            int stateOffset = row * MurmurHash3.STATE_SIZE;
            MurmurHash3.finalizeAlignedHash(fullHash, count * FULL_HASH_BYTES_PER_DIMENSION, fullState, stateOffset);

            if (singleBytePrefixLayout) {
                // The name stream is only needed when no special dimension claimed the prefix byte.
                MurmurHash3.Hash128 nameSimilarityHash = bestRank[row] == TsidBuilder.PREFIX_RANK_NONE
                    ? finalizeNameHash(nameHash, row, count)
                    : null;
                byte prefixByte = TsidBuilder.singleBytePrefix(
                    bestRank[row],
                    bestValue[row * 2],
                    bestValue[row * 2 + 1],
                    nameSimilarityHash,
                    scratch
                );
                tsids[row] = TsidBuilder.writeSingleBytePrefixTsid(prefixByte, fullHash);
            } else {
                byte nameSimilarityByte = TsidBuilder.similarityByte(finalizeNameHash(nameHash, row, count));
                int emitted = valueSimilarityCount[row];
                System.arraycopy(valueSimilarityBytes, row * TsidBuilder.MAX_TSID_VALUE_SIMILARITY_FIELDS, valueBytes, 0, emitted);
                tsids[row] = TsidBuilder.writeMultiBytePrefixTsid(nameSimilarityByte, valueBytes, emitted, count, fullHash);
            }
        }
        return tsids;
    }

    /** Completes the name-similarity stream, whose 8-bytes-per-dimension length is odd-length aware. */
    private MurmurHash3.Hash128 finalizeNameHash(MurmurHash3.Hash128 out, int row, int count) {
        int byteLength = count * NAME_HASH_BYTES_PER_DIMENSION;
        int stateOffset = row * MurmurHash3.STATE_SIZE;
        if ((count & 1) == 0) {
            return MurmurHash3.finalizeAlignedHash(out, byteLength, nameState, stateOffset);
        }
        return MurmurHash3.finalizeHashWithLongTail(out, byteLength, nameState, stateOffset, namePending[row]);
    }
}
