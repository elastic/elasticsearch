/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
