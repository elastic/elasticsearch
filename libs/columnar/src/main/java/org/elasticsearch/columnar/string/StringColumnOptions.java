/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.elasticsearch.columnar.substrate.ChunkCodec;

/**
 * How one string column is written. Every choice here is a write-time one that the column records for
 * itself: the chunk codec is named in the chunk index and the layout in the column's metadata, so two
 * fields written differently are read by the same reader and a field may be written differently tomorrow
 * than it was today.
 *
 * @param dictionary       when the column's values are named by ordinals rather than stored
 * @param chunkCodec       what compresses the chunks the values are written in
 * @param targetChunkBytes bytes a chunk holds before it is closed, which bounds what reading one value
 *                         has to decompress
 */
public record StringColumnOptions(DictionaryPolicy dictionary, ChunkCodec chunkCodec, int targetChunkBytes) {

    /**
     * The bounds a string column's dictionary is chosen under when a field names none of its own.
     *
     * <p>Half a megabyte holds the whole vocabulary of a column like host names. Beyond it the bound starts
     * admitting the tails of larger ones, where terms seen once cover almost nothing and widen the ordinal
     * every value pays for.
     */
    public static final DictionaryPolicy DEFAULT_DICTIONARY = new DictionaryPolicy(512 * 1024, 0.5, 0.2);

    /**
     * How much a chunk holds before it is closed, when a field names nothing of its own.
     *
     * <p>Smaller than the 512kb {@code ES819Version3TSDBDocValuesFormat} writes a binary field in, and
     * deliberately: a chunk is decoded whole, and this column is read at addresses a scan did not choose.
     * Bisecting a column in term order lands each probe in a chunk of its own, so what a probe costs is the
     * size of a chunk however few bytes of it the value needs, and there are a couple of dozen probes in a
     * term. A larger chunk compresses better and is the right trade where the values are read in order; here
     * it would be paid for by the reads this column exists to make cheap.
     */
    public static final int DEFAULT_TARGET_CHUNK_BYTES = 64 * 1024;

    public static final StringColumnOptions DEFAULT = new StringColumnOptions(
        DEFAULT_DICTIONARY,
        ChunkCodec.ZSTD,
        DEFAULT_TARGET_CHUNK_BYTES
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
    }

    /** These options with a different dictionary policy, for a field that should decide it differently. */
    public StringColumnOptions withDictionary(DictionaryPolicy policy) {
        return new StringColumnOptions(policy, chunkCodec, targetChunkBytes);
    }
}
