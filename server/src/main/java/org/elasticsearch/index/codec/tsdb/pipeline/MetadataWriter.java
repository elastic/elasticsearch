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
    default MetadataWriter writeBytes(byte[] bytes) {
        return writeBytes(bytes, 0, bytes.length);
    }

    /**
     * Marks the stage as applied without writing any metadata.
     * Use this for stages that always transform values but don't need
     * to store any parameters for decoding.
     */
    default void empty() {}
}
