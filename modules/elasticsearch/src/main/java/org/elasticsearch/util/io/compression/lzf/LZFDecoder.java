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

import java.io.IOException;

/**
 * Decoder that handles decoding of sequence of encoded LZF chunks,
 * combining them into a single contiguous result byte array
 * <p>
 * Code adapted from H2 project (http://www.h2database.com) Java LZF implementation
 * by Thomas (which itself was inspired by original C code by Marc A Lehmann)
 */
public class LZFDecoder {
    final static byte BYTE_NULL = 0;

    // static methods, no need to instantiate

    private LZFDecoder() {
    }

    /**
     * Method for decompressing whole input data, which encoded in LZF
     * block structure (compatible with lzf command line utility),
     * and can consist of any number of blocks
     */
    public static byte[] decode(byte[] data, int length) throws IOException {
        /* First: let's calculate actual size, so we can allocate
         * exact result size. Also useful for basic sanity checking;
         * so that after call we know header structure is not corrupt
         * (to the degree that lengths etc seem valid)
         */
        byte[] result = new byte[calculateUncompressedSize(data, length)];
        int inPtr = 0;
        int outPtr = 0;

        while (inPtr < (length - 1)) { // -1 to offset possible end marker
            inPtr += 2; // skip 'ZV' marker
            int type = data[inPtr++];
            int len = uint16(data, inPtr);
            inPtr += 2;
            if (type == LZFChunk.BLOCK_TYPE_NON_COMPRESSED) { // uncompressed
                System.arraycopy(data, inPtr, result, outPtr, len);
                outPtr += len;
            } else { // compressed
                int uncompLen = uint16(data, inPtr);
                inPtr += 2;
                decompressChunk(data, inPtr, result, outPtr, outPtr + uncompLen);
                outPtr += uncompLen;
            }
            inPtr += len;
        }
        return result;
    }

    private static int calculateUncompressedSize(byte[] data, int length) throws IOException {
        int uncompressedSize = 0;
        int ptr = 0;
        int blockNr = 0;

        while (ptr < length) {
            // can use optional end marker
            if (ptr == (length + 1) && data[ptr] == BYTE_NULL) {
                ++ptr; // so that we'll be at end
                break;
            }
            // simpler to handle bounds checks by catching exception here...
            try {
                if (data[ptr] != LZFChunk.BYTE_Z || data[ptr + 1] != LZFChunk.BYTE_V) {
                    throw new IOException("Corrupt input data, block #" + blockNr + " (at offset " + ptr + "): did not start with 'ZV' signature bytes");
                }
                int type = (int) data[ptr + 2];
                int blockLen = uint16(data, ptr + 3);
                if (type == LZFChunk.BLOCK_TYPE_NON_COMPRESSED) { // uncompressed
                    ptr += 5;
                    uncompressedSize += blockLen;
                } else if (type == LZFChunk.BLOCK_TYPE_COMPRESSED) { // compressed
                    uncompressedSize += uint16(data, ptr + 5);
                    ptr += 7;
                } else { // unknown... CRC-32 would be 2, but that's not implemented by cli tool
                    throw new IOException("Corrupt input data, block #" + blockNr + " (at offset " + ptr + "): unrecognized block type " + (type & 0xFF));
                }
                ptr += blockLen;
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IOException("Corrupt input data, block #" + blockNr + " (at offset " + ptr + "): truncated block header");
            }
            ++blockNr;
        }
        // one more sanity check:
        if (ptr != length) {
            throw new IOException("Corrupt input data: block #" + blockNr + " extends " + (data.length - ptr) + " beyond end of input");
        }
        return uncompressedSize;
    }

    /**
     * Main decode method for individual chunks.
     */
    public static void decompressChunk(byte[] in, int inPos, byte[] out, int outPos, int outEnd)
            throws IOException {
        do {
            int ctrl = in[inPos++] & 255;
            if (ctrl < LZFChunk.MAX_LITERAL) { // literal run
                ctrl += inPos;
                do {
                    out[outPos++] = in[inPos];
                } while (inPos++ < ctrl);
            } else {
                // back reference
                int len = ctrl >> 5;
                ctrl = -((ctrl & 0x1f) << 8) - 1;
                if (len == 7) {
                    len += in[inPos++] & 255;
                }
                ctrl -= in[inPos++] & 255;
                len += outPos + 2;
                out[outPos] = out[outPos++ + ctrl];
                out[outPos] = out[outPos++ + ctrl];
                while (outPos < len - 8) {
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                    out[outPos] = out[outPos++ + ctrl];
                }
                while (outPos < len) {
                    out[outPos] = out[outPos++ + ctrl];
                }
            }
        } while (outPos < outEnd);

        // sanity check to guard against corrupt data:
        if (outPos != outEnd)
            throw new IOException("Corrupt data: overrun in decompress, input offset " + inPos + ", output offset " + outPos);
    }

    private static int uint16(byte[] data, int ptr) {
        return ((data[ptr] & 0xFF) << 8) + (data[ptr + 1] & 0xFF);
    }
}
