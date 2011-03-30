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

import java.io.IOException;
import java.io.InputStream;

/**
 * Decoder that handles decoding of sequence of encoded LZF chunks,
 * combining them into a single contiguous result byte array
 *
 * @author tatu@ning.com
 */
public class LZFDecoder {
    private final static byte BYTE_NULL = 0;
    private final static int HEADER_BYTES = 5;

    // static methods, no need to instantiate
    private LZFDecoder() {
    }

    public static byte[] decode(final byte[] sourceBuffer) throws IOException {
        byte[] result = new byte[calculateUncompressedSize(sourceBuffer)];
        decode(sourceBuffer, result);
        return result;
    }

    /**
     * Method for decompressing whole input data, which encoded in LZF
     * block structure (compatible with lzf command line utility),
     * and can consist of any number of blocks
     */
    public static int decode(final byte[] sourceBuffer, final byte[] targetBuffer) throws IOException {
        /* First: let's calculate actual size, so we can allocate
         * exact result size. Also useful for basic sanity checking;
         * so that after call we know header structure is not corrupt
         * (to the degree that lengths etc seem valid)
         */
        byte[] result = targetBuffer;
        int inPtr = 0;
        int outPtr = 0;

        while (inPtr < (sourceBuffer.length - 1)) { // -1 to offset possible end marker
            inPtr += 2; // skip 'ZV' marker
            int type = sourceBuffer[inPtr++];
            int len = uint16(sourceBuffer, inPtr);
            inPtr += 2;
            if (type == LZFChunk.BLOCK_TYPE_NON_COMPRESSED) { // uncompressed
                System.arraycopy(sourceBuffer, inPtr, result, outPtr, len);
                outPtr += len;
            } else { // compressed
                int uncompLen = uint16(sourceBuffer, inPtr);
                inPtr += 2;
                decompressChunk(sourceBuffer, inPtr, result, outPtr, outPtr + uncompLen);
                outPtr += uncompLen;
            }
            inPtr += len;
        }
        return outPtr;
    }

    private static int calculateUncompressedSize(byte[] data) throws IOException {
        int uncompressedSize = 0;
        int ptr = 0;
        int blockNr = 0;

        while (ptr < data.length) {
            // can use optional end marker
            if (ptr == (data.length + 1) && data[ptr] == BYTE_NULL) {
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
        if (ptr != data.length) {
            throw new IOException("Corrupt input data: block #" + blockNr + " extends " + (data.length - ptr) + " beyond end of input");
        }
        return uncompressedSize;
    }

    /**
     * Main decode from a stream.  Decompressed bytes are placed in the outputBuffer, inputBuffer is a "scratch-area".
     *
     * @param is           An input stream of LZF compressed bytes
     * @param inputBuffer  A byte array used as a scratch area.
     * @param outputBuffer A byte array in which the result is returned
     * @return The number of bytes placed in the outputBuffer.
     */
    public static int decompressChunk(final InputStream is, final byte[] inputBuffer, final byte[] outputBuffer)
            throws IOException {
        int bytesInOutput;
        int headerLength = is.read(inputBuffer, 0, HEADER_BYTES);
        if (headerLength != HEADER_BYTES) {
            return -1;
        }
        int inPtr = 0;
        if (inputBuffer[inPtr] != LZFChunk.BYTE_Z || inputBuffer[inPtr + 1] != LZFChunk.BYTE_V) {
            throw new IOException("Corrupt input data, block did not start with 'ZV' signature bytes");
        }
        inPtr += 2;
        int type = inputBuffer[inPtr++];
        int compLen = uint16(inputBuffer, inPtr);
        inPtr += 2;
        if (type == LZFChunk.BLOCK_TYPE_NON_COMPRESSED) { // uncompressed
            readFully(is, false, outputBuffer, 0, compLen);
            bytesInOutput = compLen;
        } else { // compressed
            readFully(is, true, inputBuffer, 0, 2 + compLen); // first 2 bytes are uncompressed length
            int uncompLen = uint16(inputBuffer, 0);
            decompressChunk(inputBuffer, 2, outputBuffer, 0, uncompLen);
            bytesInOutput = uncompLen;
        }
        return bytesInOutput;
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
                continue;
            }
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

            /* Odd: after extensive profiling, looks like magic number
             * for unrolling is 4: with 8 performance is worse (even
             * bit less than with no unrolling).
             */
            final int end = len - 3;
            while (outPos < end) {
                out[outPos] = out[outPos++ + ctrl];
                out[outPos] = out[outPos++ + ctrl];
                out[outPos] = out[outPos++ + ctrl];
                out[outPos] = out[outPos++ + ctrl];
            }
            // and, interestingly, unlooping works here too:
            if (outPos < len) { // max 3 bytes to copy
                out[outPos] = out[outPos++ + ctrl];
                if (outPos < len) {
                    out[outPos] = out[outPos++ + ctrl];
                    if (outPos < len) {
                        out[outPos] = out[outPos++ + ctrl];
                    }
                }
            }
        } while (outPos < outEnd);

        // sanity check to guard against corrupt data:
        if (outPos != outEnd)
            throw new IOException("Corrupt data: overrun in decompress, input offset " + inPos + ", output offset " + outPos);
    }

    private final static int uint16(byte[] data, int ptr) {
        return ((data[ptr] & 0xFF) << 8) + (data[ptr + 1] & 0xFF);
    }

    private final static void readFully(InputStream is, boolean compressed,
                                        byte[] outputBuffer, int offset, int len) throws IOException {
        int left = len;
        while (left > 0) {
            int count = is.read(outputBuffer, offset, left);
            if (count < 0) { // EOF not allowed here
                throw new IOException("EOF in " + len + " byte ("
                        + (compressed ? "" : "un") + "compressed) block: could only read "
                        + (len - left) + " bytes");
            }
            offset += count;
            left -= count;
        }
    }
}
