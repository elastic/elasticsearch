/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

/* Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package org.elasticsearch.common.compress.lzf;

import org.elasticsearch.common.thread.ThreadLocals;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Encoder that handles splitting of input into chunks to encode,
 * calls {@link ChunkEncoder} to compress individual chunks and
 * combines resulting chunks into contiguous output byte array.
 *
 * @author tatu@ning.com
 */
public class LZFEncoder {
    // Static methods only, no point in instantiating

    private LZFEncoder() {
    }

    public static byte[] encode(byte[] data) throws IOException {
        return encode(data, data.length);
    }


    public static void encode(OutputStream os, byte[] data, int length) throws IOException {
        int left = length;
        ChunkEncoder enc = new ChunkEncoder(left);
        int chunkLen = Math.min(LZFChunk.MAX_CHUNK_LEN, left);
        enc.encodeChunk(os, data, 0, chunkLen);
        left -= chunkLen;
        // shortcut: if it all fit in, no need to coalesce:
        if (left < 1) {
            return;
        }
        int inputOffset = chunkLen;

        do {
            chunkLen = Math.min(left, LZFChunk.MAX_CHUNK_LEN);
            enc.encodeChunk(os, data, inputOffset, chunkLen);
            inputOffset += chunkLen;
            left -= chunkLen;
        } while (left > 0);
    }

    public static ThreadLocal<ThreadLocals.CleanableValue<ChunkEncoder>> cachedEncoder = new ThreadLocal<ThreadLocals.CleanableValue<ChunkEncoder>>() {
        @Override protected ThreadLocals.CleanableValue<ChunkEncoder> initialValue() {
            return new ThreadLocals.CleanableValue<ChunkEncoder>(new ChunkEncoder(LZFChunk.MAX_CHUNK_LEN));
        }
    };

    public static byte[] encodeWithCache(byte[] data, int length) throws IOException {
        int left = length;
        ChunkEncoder enc = cachedEncoder.get().get();
        int chunkLen = Math.min(LZFChunk.MAX_CHUNK_LEN, left);
        LZFChunk first = enc.encodeChunk(data, 0, chunkLen);
        left -= chunkLen;
        // shortcut: if it all fit in, no need to coalesce:
        if (left < 1) {
            return first.getData();
        }
        // otherwise need to get other chunks:
        int resultBytes = first.length();
        int inputOffset = chunkLen;
        LZFChunk last = first;

        do {
            chunkLen = Math.min(left, LZFChunk.MAX_CHUNK_LEN);
            LZFChunk chunk = enc.encodeChunk(data, inputOffset, chunkLen);
            inputOffset += chunkLen;
            left -= chunkLen;
            resultBytes += chunk.length();
            last.setNext(chunk);
            last = chunk;
        } while (left > 0);
        // and then coalesce returns into single contiguous byte array
        byte[] result = new byte[resultBytes];
        int ptr = 0;
        for (; first != null; first = first.next()) {
            ptr = first.copyTo(result, ptr);
        }
        return result;
    }

    /**
     * Method for compressing given input data using LZF encoding and
     * block structure (compatible with lzf command line utility).
     * Result consists of a sequence of chunks.
     */
    public static byte[] encode(byte[] data, int length) throws IOException {
        int left = length;
        ChunkEncoder enc = new ChunkEncoder(left);
        int chunkLen = Math.min(LZFChunk.MAX_CHUNK_LEN, left);
        LZFChunk first = enc.encodeChunk(data, 0, chunkLen);
        left -= chunkLen;
        // shortcut: if it all fit in, no need to coalesce:
        if (left < 1) {
            return first.getData();
        }
        // otherwise need to get other chunks:
        int resultBytes = first.length();
        int inputOffset = chunkLen;
        LZFChunk last = first;

        do {
            chunkLen = Math.min(left, LZFChunk.MAX_CHUNK_LEN);
            LZFChunk chunk = enc.encodeChunk(data, inputOffset, chunkLen);
            inputOffset += chunkLen;
            left -= chunkLen;
            resultBytes += chunk.length();
            last.setNext(chunk);
            last = chunk;
        } while (left > 0);
        // and then coalesce returns into single contiguous byte array
        byte[] result = new byte[resultBytes];
        int ptr = 0;
        for (; first != null; first = first.next()) {
            ptr = first.copyTo(result, ptr);
        }
        return result;
    }
}
