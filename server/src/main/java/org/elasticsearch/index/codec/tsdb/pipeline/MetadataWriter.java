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
 * Interface for writing variable-length encoded metadata.
 */
public interface MetadataWriter {

    /**
     * Writes a single byte.
     *
     * @param b the byte to write
     */
    void writeByte(byte b);

    /**
     * Writes a non-negative integer in variable-length format (1-5 bytes).
     *
     * @param value the non-negative integer to write
     * @throws IllegalArgumentException if the value is negative
     */
    void writeVInt(int value);

    /**
     * Writes a non-negative long in variable-length format (1-9 bytes).
     *
     * @param value the non-negative long to write
     * @throws IllegalArgumentException if the value is negative
     */
    void writeVLong(long value);

    /**
     * Writes a signed integer using zig-zag encoding (1-5 bytes).
     *
     * @param value the signed integer to write
     */
    void writeZInt(int value);

    /**
     * Writes a signed long using zig-zag encoding (1-10 bytes).
     *
     * @param value the signed long to write
     */
    void writeZLong(long value);

    /**
     * Clears the buffer for reuse, resetting size to zero.
     */
    void clear();

    /**
     * Returns the number of bytes written.
     *
     * @return the size in bytes
     */
    int size();

    /**
     * Returns a copy of the written bytes.
     *
     * @return a new byte array containing the written bytes
     */
    byte[] toByteArray();
}
