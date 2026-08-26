/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Utilities for implementing a {@link StreamOutput}, for cases where performance is not a concern.
 */
public enum StreamOutputHelper {
    ;

    private static final ThreadLocal<byte[]> scratch = ThreadLocal.withInitial(() -> new byte[1024]);

    /**
     * @return a thread-local {@code byte[]} that can be used to collect some data which is then passed to {@link StreamOutput#write}.
     *         <p>
     *         This is almost certainly more efficient than calling {@link StreamOutput#writeByte} repeatedly, but accessing a thread-local
     *         is not super-cheap so it is typically less efficient than writing the bytes directly into the buffer underneath the
     *         {@link StreamOutput} if it has such a buffer.
     */
    public static byte[] getThreadLocalScratchBuffer() {
        return scratch.get();
    }

    /**
     * Write string using the default thread-local scratch buffer, as described in {@link StreamOutput#writeString}.
     * <p>
     * This is almost certainly more efficient than calling {@link StreamOutput#writeByte} repeatedly, but accessing a thread-local is not
     * super-cheap so it is typically less efficient than writing the bytes directly into the buffer underneath the {@link StreamOutput} if
     * it has such a buffer.
     *
     * @param str string to write
     * @param outputStream the stream to which to write the data after buffering.
     * @return number of bytes written.
     * @throws IOException on failure
     */
    public static int writeString(String str, OutputStream outputStream) throws IOException {
        return writeString(str, getThreadLocalScratchBuffer(), 0, outputStream);
    }

    /**
     * Write string prefixed by some number of bytes (possibly zero) from the beginning of the given {@code buffer}. The given
     * {@code buffer} will also be used when encoding the given string. This is almost certainly more efficient than calling
     * {@link StreamOutput#writeByte} repeatedly, but less efficient than writing the bytes directly into the buffer underneath the
     * {@link StreamOutput} if it has such a buffer.
     *
     * @param str string to write
     * @param buffer buffer that may hold some bytes to write
     * @param prefixLength how many bytes in {code buffer} to write
     * @param outputStream the stream to which to write the data after buffering.
     * @return number of bytes written.
     * @throws IOException on failure
     */
    public static int writeString(String str, byte[] buffer, int prefixLength, OutputStream outputStream) throws IOException {
        final int charCount = str.length();
        int offset = putVInt(buffer, charCount, prefixLength);
        int total = 0;
        for (int i = 0; i < charCount; i++) {
            final int c = str.charAt(i);
            if (c <= 0x007F) {
                buffer[offset++] = ((byte) c);
            } else if (c > 0x07FF) {
                buffer[offset++] = ((byte) (0xE0 | c >> 12 & 0x0F));
                buffer[offset++] = ((byte) (0x80 | c >> 6 & 0x3F));
                buffer[offset++] = ((byte) (0x80 | c >> 0 & 0x3F));
            } else {
                buffer[offset++] = ((byte) (0xC0 | c >> 6 & 0x1F));
                buffer[offset++] = ((byte) (0x80 | c >> 0 & 0x3F));
            }
            // make sure any possible char can fit into the buffer in any possible iteration
            // we need at most 3 bytes so we flush the buffer once we have less than 3 bytes
            // left before we start another iteration
            if (offset > buffer.length - 3) {
                outputStream.write(buffer, 0, offset);
                total += offset;
                offset = 0;
            }
        }
        outputStream.write(buffer, 0, offset);
        return total + offset;
    }

    /**
     * Write possibly-null string using the default thread-local scratch buffer, as described in {@link StreamOutput#writeOptionalString}.
     * <p>
     * This is almost certainly more efficient than calling {@link StreamOutput#writeByte} repeatedly, but accessing a thread-local is not
     * super-cheap so it is typically less efficient than writing the bytes directly into the buffer underneath the {@link StreamOutput} if
     * it has such a buffer.
     *
     * @param str string to write
     * @param outputStream the stream to which to write the data after buffering.
     * @return number of bytes written.
     * @throws IOException on failure
     */
    public static int writeOptionalString(@Nullable String str, OutputStream outputStream) throws IOException {
        if (str == null) {
            outputStream.write((byte) 0);
            return 1;
        } else {
            byte[] buffer = getThreadLocalScratchBuffer();
            // put the true byte into the buffer instead of writing it outright to do fewer flushes
            buffer[0] = (byte) 1;
            return StreamOutputHelper.writeString(str, buffer, 1, outputStream);
        }
    }

    /**
     * Write generic string using the default thread-local scratch buffer, as described in {@link StreamOutput#writeGenericString}.
     * <p>
     * This is almost certainly more efficient than calling {@link StreamOutput#writeByte} repeatedly, but accessing a thread-local is not
     * super-cheap so it is typically less efficient than writing the bytes directly into the buffer underneath the {@link StreamOutput} if
     * it has such a buffer.
     *
     * @param value string to write
     * @param outputStream the stream to which to write the data after buffering.
     * @return number of bytes written.
     * @throws IOException on failure
     */
    public static int writeGenericString(String value, OutputStream outputStream) throws IOException {
        byte[] buffer = StreamOutputHelper.getThreadLocalScratchBuffer();
        // put the 0 type identifier byte into the buffer instead of writing it outright to do fewer flushes
        buffer[0] = 0;
        return StreamOutputHelper.writeString(value, buffer, 1, outputStream);
    }

    /**
     * Put the integer {@code i} into the given {@code buffer} starting at the given {@code offset}, formatted as per
     * {@link StreamOutput#writeVInt}. Performs no bounds checks: callers must verify that there is enough space in {@code buffer} first.
     *
     * @return updated offset (original offset plus number of bytes written)
     */
    public static int putVInt(byte[] buffer, int i, int offset) {
        if (Integer.numberOfLeadingZeros(i) >= 25) {
            buffer[offset] = (byte) i;
            return offset + 1;
        }
        return putMultiByteVInt(buffer, i, offset);
    }

    /**
     * Put the integer {@code i} into the given {@code buffer} starting at the given {@code offset}, formatted as per
     * {@link StreamOutput#writeVInt}. Performs no bounds checks: callers must verify that there is enough space in {@code buffer} first.
     *
     * @return updated offset (original offset plus number of bytes written)
     */
    // extracted from putVInt() to allow the hot single-byte path to be inlined
    public static int putMultiByteVInt(byte[] buffer, int i, int offset) {
        do {
            buffer[offset++] = ((byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
        } while ((i & ~0x7F) != 0);
        buffer[offset++] = (byte) i;
        return offset;
    }

}
