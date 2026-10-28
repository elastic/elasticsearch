/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

/**
 * Writes stage metadata values to a buffer during encoding. Supports method chaining.
 *
 * <p>This is a dedicated interface rather than Lucene's {@code DataOutput} or Elasticsearch's
 * {@code StreamOutput} because the block layout places stage metadata <em>after</em> the payload
 * on disk ({@code [bitmap][payload][stage metadata]}), while transform stages produce metadata
 * <em>before</em> the payload during encoding. The encode pipeline buffers metadata in memory
 * and flushes it after the payload is written (see {@link EncodingContext#writeStageMetadata}).
 *
 * <p>This design favors the decode path: the decoder reads bitmap, payload, then metadata in a
 * single forward pass with no buffering or seeking. Since decoding is far more frequent than
 * encoding, the buffering cost is pushed to the encode side (once per block).
 *
 * <p>By decoupling stages from the buffering strategy, each stage writes metadata through this
 * minimal interface without knowledge of the block layout or metadata ordering. This also
 * simplifies backwards compatibility: the block layout or metadata ordering can change without
 * modifying any stage implementation.
 *
 * @see MetadataReader
 * @see EncodingContext#writeStageMetadata(org.apache.lucene.store.DataOutput)
 */
public interface MetadataWriter {

    /**
     * Writes a single byte.
     *
     * @param value the byte to write
     * @return this writer for chaining
     */
    MetadataWriter writeByte(byte value);

    /**
     * Writes a zigzag-encoded variable-length integer.
     *
     * @param value the integer to write
     * @return this writer for chaining
     */
    MetadataWriter writeZInt(int value);

    /**
     * Writes a zigzag-encoded variable-length long.
     *
     * @param value the long to write
     * @return this writer for chaining
     */
    MetadataWriter writeZLong(long value);

    /**
     * Writes a fixed 8-byte long in Lucene format.
     *
     * @param value the long to write
     * @return this writer for chaining
     */
    MetadataWriter writeLong(long value);

    /**
     * Writes a fixed 4-byte integer in Lucene format.
     *
     * @param value the integer to write
     * @return this writer for chaining
     */
    MetadataWriter writeInt(int value);

    /**
     * Writes a variable-length integer.
     *
     * @param value the integer to write
     * @return this writer for chaining
     */
    MetadataWriter writeVInt(int value);

    /**
     * Writes a variable-length long.
     *
     * @param value the long to write
     * @return this writer for chaining
     */
    MetadataWriter writeVLong(long value);

    /**
     * Writes bytes from the given array.
     *
     * @param bytes the source array
     * @param offset the offset in the array
     * @param length the number of bytes to write
     * @return this writer for chaining
     */
    MetadataWriter writeBytes(byte[] bytes, int offset, int length);

    /**
     * Writes all bytes from the given array.
     *
     * @param bytes the source array
     * @return this writer for chaining
     */
    default MetadataWriter writeBytes(final byte[] bytes) {
        return writeBytes(bytes, 0, bytes.length);
    }

    /**
     * Marks the stage as applied without writing any metadata.
     * Use this for stages that always transform values but don't need
     * to store any parameters for decoding.
     */
    default void empty() {}
}
