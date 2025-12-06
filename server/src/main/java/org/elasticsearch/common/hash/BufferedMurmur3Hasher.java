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
 * A buffered Murmur3 hasher that allows hashing strings and longs efficiently.
 * It uses a byte array buffer to reduce allocations for converting strings and longs to bytes before passing them to the hasher.
 * The buffer also allows for more efficient execution by minimizing the number of times the underlying hasher is updated,
 * and by maximizing the amount of data processed in each update call.
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
    private int pos;

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

    @Override
    public MurmurHash3.Hash128 digestHash(MurmurHash3.Hash128 hash) {
        flush();
        return super.digestHash(hash);
    }

    @Override
    public void reset() {
        super.reset();
        pos = 0;
    }

    /**
     * Adds a string to the hasher.
     * The string is converted to UTF-8 and written into the buffer.
     * The buffer is resized if necessary to accommodate the UTF-8 encoded string.
     *
     * @param value the string value to add
     */
    public void addString(String value) {
        int requiredBufferLength = UnicodeUtil.maxUTF8Length(value.length());
        ensureCapacity(requiredBufferLength);
        flushIfRemainingCapacityLowerThan(requiredBufferLength);
        pos = UnicodeUtil.UTF16toUTF8(value, 0, value.length(), buffer, pos);
    }

    /**
     * Adds a long value to the hasher.
     * The long is written in little-endian format.
     *
     * @param value the long value to add
     */
    public void addLong(long value) {
        flushIfRemainingCapacityLowerThan(Long.BYTES);
        ByteUtils.writeLongLE(value, buffer, pos);
        pos += Long.BYTES;
    }

    /**
     * Adds two long values to the hasher.
     * Each long is written in little-endian format.
     *
     * @param v1 the first long value to add
     * @param v2 the second long value to add
     */
    public void addLongs(long v1, long v2) {
        flushIfRemainingCapacityLowerThan(Long.BYTES * 2);
        ByteUtils.writeLongLE(v1, buffer, pos);
        ByteUtils.writeLongLE(v2, buffer, pos + 8);
        pos += Long.BYTES * 2;
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
        flushIfRemainingCapacityLowerThan(Long.BYTES * 4);
        ByteUtils.writeLongLE(v1, buffer, pos);
        ByteUtils.writeLongLE(v2, buffer, pos + 8);
        ByteUtils.writeLongLE(v3, buffer, pos + 16);
        ByteUtils.writeLongLE(v4, buffer, pos + 24);
        pos += Long.BYTES * 4;
    }

    private void ensureCapacity(int requiredBufferLength) {
        if (buffer.length < requiredBufferLength) {
            flush();
            buffer = new byte[requiredBufferLength];
        }
    }

    private void flush() {
        if (pos > 0) {
            update(buffer, 0, pos);
            pos = 0;
        }
    }

    private void flushIfRemainingCapacityLowerThan(int requiredCapacity) {
        if (buffer.length - pos < requiredCapacity) {
            flush();
        }
    }
}
