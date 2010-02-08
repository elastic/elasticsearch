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

package org.elasticsearch.util.io.compression.lzf;

/**
 * Helper class used to store LZF encoded segments (compressed and non-compressed)
 * that can be sequenced to produce LZF files/streams.
 */
public class LZFChunk {
    /**
     * Maximum length of literal run for LZF encoding.
     */
    public static final int MAX_LITERAL = 1 << 5; // 32

    // Chunk length is limited by 2-byte length indicator, to 64k
    public static final int MAX_CHUNK_LEN = 0xFFFF;

    public final static byte BYTE_Z = 'Z';
    public final static byte BYTE_V = 'V';

    public final static int BLOCK_TYPE_NON_COMPRESSED = 0;
    public final static int BLOCK_TYPE_COMPRESSED = 1;


    final byte[] _data;
    LZFChunk _next;

    private LZFChunk(byte[] data) {
        _data = data;
    }

    /**
     * Factory method for constructing compressed chunk
     */
    public static LZFChunk createCompressed(int origLen, byte[] encData, int encPtr, int encLen) {
        byte[] result = new byte[encLen + 7];
        result[0] = BYTE_Z;
        result[1] = BYTE_V;
        result[2] = BLOCK_TYPE_COMPRESSED;
        result[3] = (byte) (encLen >> 8);
        result[4] = (byte) encLen;
        result[5] = (byte) (origLen >> 8);
        result[6] = (byte) origLen;
        System.arraycopy(encData, encPtr, result, 7, encLen);
        return new LZFChunk(result);
    }

    /**
     * Factory method for constructing compressed chunk
     */
    public static LZFChunk createNonCompressed(byte[] plainData, int ptr, int len) {
        byte[] result = new byte[len + 5];
        result[0] = BYTE_Z;
        result[1] = BYTE_V;
        result[2] = BLOCK_TYPE_NON_COMPRESSED;
        result[3] = (byte) (len >> 8);
        result[4] = (byte) len;
        System.arraycopy(plainData, ptr, result, 5, len);
        return new LZFChunk(result);
    }

    public void setNext(LZFChunk next) {
        _next = next;
    }

    public LZFChunk next() {
        return _next;
    }

    public int length() {
        return _data.length;
    }

    public byte[] getData() {
        return _data;
    }

    public int copyTo(byte[] dst, int ptr) {
        int len = _data.length;
        System.arraycopy(_data, 0, dst, ptr, len);
        return ptr + len;
    }
}
