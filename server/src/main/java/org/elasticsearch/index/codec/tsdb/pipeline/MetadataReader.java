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
 * Interface for reading variable-length encoded metadata.
 */
public interface MetadataReader {

    /**
     * Reads a single byte.
     *
     * @return the byte value
     * @throws IllegalStateException if no more bytes are available
     */
    byte readByte();

    /**
     * Reads a non-negative integer in variable-length format (1-5 bytes).
     *
     * @return the integer value
     * @throws IllegalStateException if no more bytes are available or encoding is invalid
     */
    int readVInt();

    /**
     * Reads a non-negative long in variable-length format (1-9 bytes).
     *
     * @return the long value
     * @throws IllegalStateException if no more bytes are available or encoding is invalid
     */
    long readVLong();

    /**
     * Reads a signed long that was written using zig-zag encoding (1-10 bytes).
     *
     * @return the signed long value
     * @throws IllegalStateException if no more bytes are available or encoding is invalid
     */
    long readZLong();

    /**
     * Sets the read position.
     *
     * @param position the new position
     * @throws IllegalArgumentException if position is negative or exceeds size
     */
    void setPosition(int position);

    /**
     * Returns the current read position.
     *
     * @return the current position
     */
    int position();

    /**
     * Returns the number of bytes available for reading.
     *
     * @return the size in bytes
     */
    int size();
}
