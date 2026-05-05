/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import java.io.IOException;

/**
 * Reads stage metadata values from a buffer during decoding.
 *
 * <p>This is the decode-side counterpart of {@link MetadataWriter}. See that interface for the
 * rationale behind using a dedicated interface rather than Lucene's {@code DataInput} or
 * Elasticsearch's {@code StreamInput}.
 *
 * @see MetadataWriter
 * @see DecodingContext
 */
public interface MetadataReader {

    /**
     * Reads a single byte.
     *
     * @return the byte value
     * @throws IOException if an I/O error occurs
     */
    byte readByte() throws IOException;

    /**
     * Reads a zigzag-encoded variable-length integer.
     *
     * @return the decoded integer
     * @throws IOException if an I/O error occurs
     */
    int readZInt() throws IOException;

    /**
     * Reads a zigzag-encoded variable-length long.
     *
     * @return the decoded long
     * @throws IOException if an I/O error occurs
     */
    long readZLong() throws IOException;

    /**
     * Reads a fixed 8-byte long in Lucene format.
     *
     * @return the long value
     * @throws IOException if an I/O error occurs
     */
    long readLong() throws IOException;

    /**
     * Reads a fixed 4-byte integer in Lucene format.
     *
     * @return the integer value
     * @throws IOException if an I/O error occurs
     */
    int readInt() throws IOException;

    /**
     * Reads a variable-length integer.
     *
     * @return the decoded integer
     * @throws IOException if an I/O error occurs
     */
    int readVInt() throws IOException;

    /**
     * Reads a variable-length long.
     *
     * @return the decoded long
     * @throws IOException if an I/O error occurs
     */
    long readVLong() throws IOException;

    /**
     * Reads bytes into the given array.
     *
     * @param bytes the destination array
     * @param offset the offset in the array
     * @param length the number of bytes to read
     * @throws IOException if an I/O error occurs
     */
    void readBytes(byte[] bytes, int offset, int length) throws IOException;

    /**
     * Reads bytes into the given array starting at offset 0.
     *
     * @param bytes the destination array
     * @throws IOException if an I/O error occurs
     */
    default void readBytes(final byte[] bytes) throws IOException {
        readBytes(bytes, 0, bytes.length);
    }
}
