/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ssl.explain;

import java.nio.ByteBuffer;

/**
 * Utility class for working with buffers. Originally based on code from <a href="https://github.com/elastic/tealess">Tealess</a>.
 * TODO: Maybe merge this with {@link org.elasticsearch.nio.utils.ByteBufferUtils}
 */
class BufferUtil {

    /**
     * Read a byte from the source buffer and treat is as an unsigned 8 bit integer.
     */
    static int readUInt8(ByteBuffer source) {
        return source.get() & 0xff;
    }

    /**
     * Read a byte from the source buffer and treat is as an unsigned 16 bit integer.
     */
    static int readUInt16(ByteBuffer source) {
        return (source.get() << 8) + source.get();
    }

    /**
     * Read a byte from the source buffer and treat is as an unsigned 24 bit integer.
     */
    static int readUInt24(ByteBuffer source) {
        return (readUInt8(source) << 16) + (readUInt8(source) << 8) + readUInt8(source);
    }

    /**
     * Read a byte from the source buffer and treat is as an unsigned 32 bit integer.
     */
    static long readUInt32(ByteBuffer source) {
        return (readUInt8(source) << 24) + (readUInt8(source) << 16) + (readUInt8(source) << 8) + readUInt8(source);
    }

    /**
     * Read an array of bytes (or the given length) from the stream.
     * @see ByteBuffer#get(byte[])
     */
    static byte[] readByteArray(ByteBuffer buffer, int length) {
        byte[] array = new byte[length];
        buffer.get(array);
        return array;
    }

    /**
     * Skip over {@code bytes} number of bytes in the given stream
     */
    static void skip(ByteBuffer buffer, int bytes) {
        if (buffer.remaining() < bytes) {
            throw new IllegalArgumentException("Cannot skip " + bytes + " bytes as only " + buffer.remaining() + " are remaining");
        }
        buffer.position(buffer.position() + bytes);
    }
}
