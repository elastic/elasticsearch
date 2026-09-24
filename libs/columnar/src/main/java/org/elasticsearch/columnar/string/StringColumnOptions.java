/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.substrate.ChunkBounds;
import org.elasticsearch.columnar.substrate.ChunkCodec;

/**
 * How one string column is written. Every choice here is a write-time one that the column records for
 * itself: the chunk codec is named in the chunk index and the layout in the column's metadata, so two
 * fields written differently are read by the same reader and a field may be written differently tomorrow
 * than it was today.
 *
 * @param dictionary when the column's values are named by ordinals rather than stored
 * @param summary    how much of what the column held it summarises for a later merge
 * @param chunkCodec what compresses the chunks the values are written in
 * @param sizes      the units the column's streams are written in
 */
public record StringColumnOptions(DictionaryPolicy dictionary, SummaryPolicy summary, ChunkCodec chunkCodec, Sizes sizes) {

    /**
     * The units a string column's streams are written in: what a block addresses, and what closes a chunk of
     * the streams that are compressed.
     *
     * @param valuesPerBlock              values behind one offset in a stream of byte values, which a read
     *                                    of one value walks the lengths of
     * @param plainChunks                 what closes a chunk of a plain column's values
     * @param escapeChunks                what closes a chunk of the values no term names
     * @param packedOrdinalBlockSize      ordinals a block holds when they are stored packed
     * @param compressedOrdinalBlockSize  ordinals a block holds when they are stored compressed
     * @param escapeRankBlockSize         values between entries in the escape-rank table, which bounds the
     *                                    ordinals a read counts to learn how many values escaped before one
     * @param slotCountsBlockSize         documents a block of slot counts holds, and so how many of them a
     *                                    read sums to reach a document outside the block it last read
     */
    public record Sizes(
        int valuesPerBlock,
        ChunkBounds plainChunks,
        ChunkBounds escapeChunks,
        int packedOrdinalBlockSize,
        int compressedOrdinalBlockSize,
        int escapeRankBlockSize,
        int slotCountsBlockSize
    ) {

        public Sizes {
            blockSize("valuesPerBlock", valuesPerBlock);
            blockSize("packedOrdinalBlockSize", packedOrdinalBlockSize);
            blockSize("compressedOrdinalBlockSize", compressedOrdinalBlockSize);
            blockSize("escapeRankBlockSize", escapeRankBlockSize);
            blockSize("slotCountsBlockSize", slotCountsBlockSize);
            if (plainChunks == null || escapeChunks == null) {
                throw new IllegalArgumentException("chunk bounds are required");
            }
        }

        /** Blocks are addressed by shifting, so every block size is a power of two within the format's bounds. */
        private static void blockSize(String name, int size) {
            if (size < ColumNARDocValuesFormat.MIN_BLOCK_SIZE
                || size > ColumNARDocValuesFormat.MAX_BLOCK_SIZE
                || Integer.bitCount(size) != 1) {
                throw new IllegalArgumentException(
                    name
                        + " must be a power of 2 in ["
                        + ColumNARDocValuesFormat.MIN_BLOCK_SIZE
                        + ", "
                        + ColumNARDocValuesFormat.MAX_BLOCK_SIZE
                        + "], got "
                        + size
                );
            }
        }
    }

    /**
     * The bounds a string column's dictionary is chosen under when a field names none of its own.
     *
     * <p>Half a megabyte holds the whole vocabulary of a column like host names. Beyond it the bound starts
     * admitting the tails of larger ones, where terms seen once cover almost nothing and widen the ordinal
     * every value pays for.
     */
    public static final DictionaryPolicy DEFAULT_DICTIONARY = new DictionaryPolicy(512 * 1024, 0.5, 0.2);

    /**
     * Values behind one offset in a stream of byte values. Larger trades a longer walk on random access for
     * a smaller offset table.
     */
    public static final int DEFAULT_VALUES_PER_BLOCK = 128;

    /**
     * The most values a chunk holds, whichever stream it belongs to.
     *
     * <p>A read of one value decompresses its whole chunk, so this is what such a read costs where the
     * values are short enough that the byte target alone would let a chunk hold tens of thousands of them.
     * Below this a scan starts paying for the chunks it crosses.
     */
    public static final int DEFAULT_MAX_VALUES_PER_CHUNK = 16384;

    /**
     * What closes a chunk of a plain column's values. These are written in document order and read
     * sequentially, so a large byte target gives the compressor context at no cost to a scan.
     */
    public static final ChunkBounds DEFAULT_PLAIN_CHUNKS = new ChunkBounds(512 * 1024, DEFAULT_MAX_VALUES_PER_CHUNK);

    /**
     * What closes a chunk of the values no term names. These are reached by escape rank rather than in the
     * order they were written, and a larger chunk barely compresses better, since what escapes a dictionary
     * is the part of a column that repeats least.
     */
    public static final ChunkBounds DEFAULT_ESCAPE_CHUNKS = new ChunkBounds(32 * 1024, DEFAULT_MAX_VALUES_PER_CHUNK);

    /**
     * Ordinals a block holds when they are stored packed. Each block is packed to the width it needs, so a
     * small block keeps a single wide ordinal from widening many narrow ones and keeps a read decoding few.
     */
    public static final int DEFAULT_PACKED_ORDINAL_BLOCK_SIZE = 128;

    /**
     * Ordinals a block holds when they are stored compressed.
     *
     * <p>Large enough to hold repetition a compressor can find, and no larger: past this the compressor
     * gains little, and the block is what a read decodes to answer for one ordinal.
     */
    public static final int DEFAULT_COMPRESSED_ORDINAL_BLOCK_SIZE = 2048;

    /**
     * Values between entries in the escape-rank table. A read that wants an escaped value counts the escapes
     * before it from the nearest entry, one ordinal apiece, so this bounds that count.
     */
    public static final int DEFAULT_ESCAPE_RANK_BLOCK_SIZE = 128;

    /**
     * Documents a block of slot counts holds.
     *
     * <p>A read reaching a document the last block did not cover sums the counts before it in its own
     * block, so this bounds that walk. It is also the granularity the base addresses are kept at, so a
     * smaller block trades a larger base table for a shorter walk.
     */
    public static final int DEFAULT_SLOT_COUNTS_BLOCK_SIZE = 128;

    public static final Sizes DEFAULT_SIZES = new Sizes(
        DEFAULT_VALUES_PER_BLOCK,
        DEFAULT_PLAIN_CHUNKS,
        DEFAULT_ESCAPE_CHUNKS,
        DEFAULT_PACKED_ORDINAL_BLOCK_SIZE,
        DEFAULT_COMPRESSED_ORDINAL_BLOCK_SIZE,
        DEFAULT_ESCAPE_RANK_BLOCK_SIZE,
        DEFAULT_SLOT_COUNTS_BLOCK_SIZE
    );

    /**
     * How much of what a column held it summarises for a later merge, when a field names nothing of its own.
     *
     * <p>The same half a megabyte the dictionary is capped at, since what a merge may name is bounded by that
     * cap too: a summary larger than it describes terms no merged dictionary could hold.
     */
    public static final SummaryPolicy DEFAULT_SUMMARY = new SummaryPolicy(DEFAULT_DICTIONARY.maxBytes());

    public static final StringColumnOptions DEFAULT = new StringColumnOptions(
        DEFAULT_DICTIONARY,
        DEFAULT_SUMMARY,
        ChunkCodec.ZSTD,
        DEFAULT_SIZES
    );

    public StringColumnOptions {
        if (dictionary == null) {
            throw new IllegalArgumentException("a dictionary policy is required; use DictionaryPolicy.NONE to store the values");
        }
        if (summary == null) {
            throw new IllegalArgumentException("a summary policy is required; use SummaryPolicy.NONE to summarise nothing");
        }
        if (chunkCodec == null) {
            throw new IllegalArgumentException("a chunk codec is required; use ChunkCodec.IDENTITY to store the bytes as they are");
        }
        if (sizes == null) {
            throw new IllegalArgumentException("sizes are required; use StringColumnOptions.DEFAULT_SIZES for the measured ones");
        }
    }

    /**
     * These options under different policies, for a field that should decide differently. Both are given,
     * since what a column names and what it summarises are chosen together: a field that wants neither says
     * so twice rather than setting one and inheriting the other.
     */
    public StringColumnOptions withPolicies(DictionaryPolicy dictionaryPolicy, SummaryPolicy summaryPolicy) {
        return new StringColumnOptions(dictionaryPolicy, summaryPolicy, chunkCodec, sizes);
    }

    /** These options with different sizes, for a field whose shape is not what the defaults were measured on. */
    public StringColumnOptions withSizes(Sizes other) {
        return new StringColumnOptions(dictionary, summary, chunkCodec, other);
    }
}
