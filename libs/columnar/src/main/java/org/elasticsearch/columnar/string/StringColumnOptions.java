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
import org.elasticsearch.columnar.substrate.ChunkCodec;

/**
 * How one string column is written. Every choice here is a write-time one that the column records for
 * itself: the chunk codec is named in the chunk index and the layout in the column's metadata, so two
 * fields written differently are read by the same reader and a field may be written differently tomorrow
 * than it was today.
 *
 * @param dictionary              when the column's values are named by ordinals rather than stored
 * @param chunkCodec              what compresses the chunks the values are written in
 * @param targetChunkBytes        bytes a chunk holds before it is closed on the dictionary path, which
 *                                bounds what reading one value has to decompress
 * @param plainPathTargetChunkBytes bytes a chunk holds before it is closed on the plain path; larger
 *                                than {@code targetChunkBytes} because plain-path columns are scanned
 *                                sequentially and never bisected, so a larger chunk compresses better
 *                                at no extra read cost
 * @param compressedOrdinalBlockSize ordinals a block holds when a column's ordinals are stored
 *                                compressed, which bounds what reading one ordinal has to decompress
 */
public record StringColumnOptions(
    DictionaryPolicy dictionary,
    ChunkCodec chunkCodec,
    int targetChunkBytes,
    int plainPathTargetChunkBytes,
    int compressedOrdinalBlockSize
) {

    /**
     * The bounds a string column's dictionary is chosen under when a field names none of its own.
     *
     * <p>Half a megabyte holds the whole vocabulary of a column like host names. Beyond it the bound starts
     * admitting the tails of larger ones, where terms seen once cover almost nothing and widen the ordinal
     * every value pays for.
     */
    public static final DictionaryPolicy DEFAULT_DICTIONARY = new DictionaryPolicy(512 * 1024, 0.5, 0.2);

    /**
     * How much a chunk holds before it is closed on the dictionary path, when a field names nothing of its own.
     *
     * <p>What this sizes is the values that escaped the dictionary, which are reached by escape rank rather
     * than in the order they were written. A larger chunk barely compresses better, since what escapes a
     * dictionary is the part of a column that repeats least, and costs a read that wants one value the whole
     * of it. Smaller and a scan starts paying the per-chunk work instead.
     */
    public static final int DEFAULT_TARGET_CHUNK_BYTES = 32 * 1024;

    /**
     * How much a chunk holds before it is closed on the plain path.
     *
     * <p>Plain-path columns are written in document order and read sequentially; they are never bisected.
     * A larger chunk gives the compressor more context without increasing read amplification, matching the
     * 512kb block size {@code ES819Version3TSDBDocValuesFormat} uses for binary doc values.
     */
    public static final int DEFAULT_PLAIN_PATH_TARGET_CHUNK_BYTES = 512 * 1024;

    /**
     * Ordinals a block holds when they are stored compressed.
     *
     * <p>Large enough to hold repetition a compressor can find, and no larger: past this the compressor
     * gains little, and the block is what a read decodes to answer for one ordinal.
     */
    public static final int DEFAULT_COMPRESSED_ORDINAL_BLOCK_SIZE = 2048;

    public static final StringColumnOptions DEFAULT = new StringColumnOptions(
        DEFAULT_DICTIONARY,
        ChunkCodec.ZSTD,
        DEFAULT_TARGET_CHUNK_BYTES,
        DEFAULT_PLAIN_PATH_TARGET_CHUNK_BYTES,
        DEFAULT_COMPRESSED_ORDINAL_BLOCK_SIZE
    );

    public StringColumnOptions {
        if (dictionary == null) {
            throw new IllegalArgumentException("a dictionary policy is required; use DictionaryPolicy.NONE to store the values");
        }
        if (chunkCodec == null) {
            throw new IllegalArgumentException("a chunk codec is required; use ChunkCodec.IDENTITY to store the bytes as they are");
        }
        if (targetChunkBytes <= 0) {
            throw new IllegalArgumentException("targetChunkBytes must be positive, got " + targetChunkBytes);
        }
        if (plainPathTargetChunkBytes <= 0) {
            throw new IllegalArgumentException("plainPathTargetChunkBytes must be positive, got " + plainPathTargetChunkBytes);
        }
        if (compressedOrdinalBlockSize < ColumNARDocValuesFormat.MIN_BLOCK_SIZE
            || compressedOrdinalBlockSize > ColumNARDocValuesFormat.MAX_BLOCK_SIZE
            || Integer.bitCount(compressedOrdinalBlockSize) != 1) {
            throw new IllegalArgumentException(
                "compressedOrdinalBlockSize must be a power of 2 in ["
                    + ColumNARDocValuesFormat.MIN_BLOCK_SIZE
                    + ", "
                    + ColumNARDocValuesFormat.MAX_BLOCK_SIZE
                    + "], got "
                    + compressedOrdinalBlockSize
            );
        }
    }

    /** These options with a different dictionary policy, for a field that should decide it differently. */
    public StringColumnOptions withDictionary(DictionaryPolicy policy) {
        return new StringColumnOptions(policy, chunkCodec, targetChunkBytes, plainPathTargetChunkBytes, compressedOrdinalBlockSize);
    }
}
