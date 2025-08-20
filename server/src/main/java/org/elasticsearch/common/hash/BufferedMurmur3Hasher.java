/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.hash;

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.util.ByteUtils;

/**
 * A buffered Murmur3 hasher that allows adding strings and longs efficiently.
 * It uses a byte array buffer to reduce allocations for converting strings and longs to bytes before passing them to the hasher.
 */
public class BufferedMurmur3Hasher extends Murmur3Hasher {

    public static final int DEFAULT_BUFFER_SIZE = 32 * 4; // 32 characters, each character may take up to 4 bytes in UTF-8
    /**
     * The buffer used for holding the UTF-8 encoded strings before passing them to the hasher.
     * Should be sized so that it can hold the longest UTF-8 encoded string that is expected to be hashed,
     * to avoid re-sizing the buffer.
     * But should also be small enough to not waste memory in case the keys are short.
     */
    private byte[] buffer;

    public BufferedMurmur3Hasher(long seed) {
        this(seed, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Constructs a BufferedMurmur3Hasher with a specified seed and buffer size.
     *
     * @param seed        the seed for the Murmur3 hash function
     * @param bufferSize  the size of the buffer in bytes, must be at least 32
     */
    public BufferedMurmur3Hasher(long seed, int bufferSize) {
        super(seed);
        if (bufferSize < 32) {
            throw new IllegalArgumentException("Buffer size must be at least 32 bytes");
        }
        this.buffer = new byte[bufferSize];
    }

    /**
     * Adds a string to the hasher.
     * The string is converted to UTF-8 and written into the buffer.
     * The buffer is resized if necessary to accommodate the UTF-8 encoded string.
     *
     * @param value the string value to add
     */
    public void addString(String value) {
        ensureCapacity(UnicodeUtil.maxUTF8Length(value.length()));
        int length = UnicodeUtil.UTF16toUTF8(value, 0, value.length(), buffer);
        update(buffer, 0, length);
    }

    /**
     * Adds a long value to the hasher.
     * The long is written in little-endian format.
     *
     * @param value the long value to add
     */
    public void addLong(long value) {
        ByteUtils.writeLongLE(value, buffer, 0);
        update(buffer, 0, 8);
    }

    /**
     * Adds two long values to the hasher.
     * Each long is written in little-endian format.
     *
     * @param v1 the first long value to add
     * @param v2 the second long value to add
     */
    public void addLongs(long v1, long v2) {
        ByteUtils.writeLongLE(v1, buffer, 0);
        ByteUtils.writeLongLE(v2, buffer, 8);
        update(buffer, 0, 16);
    }

    /**
     * Adds four long values to the hasher.
     * Each long is written in little-endian format.
     *
     * @param v1 the first long value to add
     * @param v2 the second long value to add
     * @param v3 the third long value to add
     * @param v4 the fourth long value to add
     */
    public void addLongs(long v1, long v2, long v3, long v4) {
        ByteUtils.writeLongLE(v1, buffer, 0);
        ByteUtils.writeLongLE(v2, buffer, 8);
        ByteUtils.writeLongLE(v3, buffer, 16);
        ByteUtils.writeLongLE(v4, buffer, 24);
        update(buffer, 0, 32);
    }

    private void ensureCapacity(int requiredBufferLength) {
        if (buffer.length < requiredBufferLength) {
            buffer = new byte[requiredBufferLength];
        }
    }
}
