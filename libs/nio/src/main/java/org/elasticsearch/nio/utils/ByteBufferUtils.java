/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio.utils;

import java.nio.ByteBuffer;

public final class ByteBufferUtils {

    private ByteBufferUtils() {}

    /**
     * Copies bytes from the array of byte buffers into the destination buffer. The number of bytes copied is
     * limited by the bytes available to copy and the space remaining in the destination byte buffer.
     *
     * @param source byte buffers to copy from
     * @param destination byte buffer to copy to
     *
     * @return number of bytes copied
     */
    public static long copyBytes(ByteBuffer[] source, ByteBuffer destination) {
        long bytesCopied = 0;
        for (int i = 0; i < source.length && destination.hasRemaining(); i++) {
            ByteBuffer buffer = source[i];
            bytesCopied += copyBytes(buffer, destination);
        }
        return bytesCopied;
    }

    /**
     * Copies bytes from source byte buffer into the destination buffer. The number of bytes copied is
     * limited by the bytes available to copy and the space remaining in the destination byte buffer.
     *
     * @param source byte buffer to copy from
     * @param destination byte buffer to copy to
     *
     * @return number of bytes copied
     */
    public static int copyBytes(ByteBuffer source, ByteBuffer destination) {
        int nBytesToCopy = Math.min(destination.remaining(), source.remaining());
        int initialLimit = source.limit();
        source.limit(source.position() + nBytesToCopy);
        destination.put(source);
        source.limit(initialLimit);
        return nBytesToCopy;
    }
}
