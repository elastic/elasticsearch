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

    /**
     * Method for decompressing a block of input data encoded in LZF
     * block structure (compatible with lzf command line utility),
     * and can consist of any number of blocks.
     * Note that input MUST consists of a sequence of one or more complete
     * chunks; partial chunks can not be handled.
     */
    public static byte[] decode(final byte[] inputBuffer) throws IOException {
        byte[] result = new byte[calculateUncompressedSize(inputBuffer, 0, inputBuffer.length)];
        decode(inputBuffer, 0, inputBuffer.length, result);
        return result;
    }

    /**
     * Method for decompressing a block of input data encoded in LZF
     * block structure (compatible with lzf command line utility),
     * and can consist of any number of blocks.
     * Note that input MUST consists of a sequence of one or more complete
     * chunks; partial chunks can not be handled.
     *
     * @since 0.8.2
     */
    public static byte[] decode(final byte[] inputBuffer, int inputPtr, int inputLen) throws IOException {
        byte[] result = new byte[calculateUncompressedSize(inputBuffer, inputPtr, inputLen)];
        decode(inputBuffer, inputPtr, inputLen, result);
        return result;
    }

    /**
     * Method for decompressing a block of input data encoded in LZF
     * block structure (compatible with lzf command line utility),
     * and can consist of any number of blocks.
     * Note that input MUST consists of a sequence of one or more complete
     * chunks; partial chunks can not be handled.
     */
    public static int decode(final byte[] inputBuffer, final byte[] targetBuffer) throws IOException {
        return decode(inputBuffer, 0, inputBuffer.length, targetBuffer);
    }

    /**
     * Method for decompressing a block of input data encoded in LZF
     * block structure (compatible with lzf command line utility),
     * and can consist of any number of blocks.
     * Note that input MUST consists of a sequence of one or more complete
     * chunks; partial chunks can not be handled.
     */
    public static int decode(final byte[] sourceBuffer, int inPtr, int inLength,
                             final byte[] targetBuffer) throws IOException {
        byte[] result = targetBuffer;
        int outPtr = 0;
        int blockNr = 0;

        final int end = inPtr + inLength - 1; // -1 to offset possible end marker

        while (inPtr < end) {
            // let's do basic sanity checks; no point in skimping with these checks
            if (sourceBuffer[inPtr] != LZFChunk.BYTE_Z || sourceBuffer[inPtr + 1] != LZFChunk.BYTE_V) {
                throw new IOException("Corrupt input data, block #" + blockNr + " (at offset " + inPtr + "): did not start with 'ZV' signature bytes");
            }
            inPtr += 2;
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
            ++blockNr;
        }
        return outPtr;
    }

    /**
     * Helper method that will calculate total uncompressed size, for sequence of
     * one or more LZF blocks stored in given byte array.
     * Will do basic sanity checking, so that this method can be called to
     * verify against some types of corruption.
     */
    public static int calculateUncompressedSize(byte[] data, int ptr, int length) throws IOException {
        int uncompressedSize = 0;
        int blockNr = 0;
        final int end = ptr + length;

        while (ptr < end) {
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
     * Main decode from a stream.  Decompressed bytes are placed in the outputBuffer, inputBuffer
     * is a "scratch-area".
     *
     * @param is           An input stream of LZF compressed bytes
     * @param inputBuffer  A byte array used as a scratch area.
     * @param outputBuffer A byte array in which the result is returned
     * @return The number of bytes placed in the outputBuffer.
     */
    public static int decompressChunk(final InputStream is, final byte[] inputBuffer, final byte[] outputBuffer)
            throws IOException {
        int bytesInOutput;
        /* note: we do NOT read more than 5 bytes because otherwise might need to shuffle bytes
           * for output buffer (could perhaps optimize in future?)
           */
        int bytesRead = readHeader(is, inputBuffer);
        if ((bytesRead < HEADER_BYTES)
                || inputBuffer[0] != LZFChunk.BYTE_Z || inputBuffer[1] != LZFChunk.BYTE_V) {
            if (bytesRead == 0) { // probably fine, clean EOF
                return -1;
            }
            throw new IOException("Corrupt input data, block did not start with 2 byte signature ('ZV') followed by type byte, 2-byte length)");
        }
        int type = inputBuffer[2];
        int compLen = uint16(inputBuffer, 3);
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
                // 11-Aug-2011, tatu: Looks silly, but is faster than simple loop or System.arraycopy
                switch (ctrl) {
                    case 31:
                        out[outPos++] = in[inPos++];
                    case 30:
                        out[outPos++] = in[inPos++];
                    case 29:
                        out[outPos++] = in[inPos++];
                    case 28:
                        out[outPos++] = in[inPos++];
                    case 27:
                        out[outPos++] = in[inPos++];
                    case 26:
                        out[outPos++] = in[inPos++];
                    case 25:
                        out[outPos++] = in[inPos++];
                    case 24:
                        out[outPos++] = in[inPos++];
                    case 23:
                        out[outPos++] = in[inPos++];
                    case 22:
                        out[outPos++] = in[inPos++];
                    case 21:
                        out[outPos++] = in[inPos++];
                    case 20:
                        out[outPos++] = in[inPos++];
                    case 19:
                        out[outPos++] = in[inPos++];
                    case 18:
                        out[outPos++] = in[inPos++];
                    case 17:
                        out[outPos++] = in[inPos++];
                    case 16:
                        out[outPos++] = in[inPos++];
                    case 15:
                        out[outPos++] = in[inPos++];
                    case 14:
                        out[outPos++] = in[inPos++];
                    case 13:
                        out[outPos++] = in[inPos++];
                    case 12:
                        out[outPos++] = in[inPos++];
                    case 11:
                        out[outPos++] = in[inPos++];
                    case 10:
                        out[outPos++] = in[inPos++];
                    case 9:
                        out[outPos++] = in[inPos++];
                    case 8:
                        out[outPos++] = in[inPos++];
                    case 7:
                        out[outPos++] = in[inPos++];
                    case 6:
                        out[outPos++] = in[inPos++];
                    case 5:
                        out[outPos++] = in[inPos++];
                    case 4:
                        out[outPos++] = in[inPos++];
                    case 3:
                        out[outPos++] = in[inPos++];
                    case 2:
                        out[outPos++] = in[inPos++];
                    case 1:
                        out[outPos++] = in[inPos++];
                    case 0:
                        out[outPos++] = in[inPos++];
                }
                continue;
            }
            // back reference
            int len = ctrl >> 5;
            ctrl = -((ctrl & 0x1f) << 8) - 1;
            if (len < 7) { // 2 bytes; length of 3 - 8 bytes
                ctrl -= in[inPos++] & 255;
                out[outPos] = out[outPos++ + ctrl];
                out[outPos] = out[outPos++ + ctrl];
                switch (len) {
                    case 6:
                        out[outPos] = out[outPos++ + ctrl];
                    case 5:
                        out[outPos] = out[outPos++ + ctrl];
                    case 4:
                        out[outPos] = out[outPos++ + ctrl];
                    case 3:
                        out[outPos] = out[outPos++ + ctrl];
                    case 2:
                        out[outPos] = out[outPos++ + ctrl];
                    case 1:
                        out[outPos] = out[outPos++ + ctrl];
                }
                continue;
            }

            // long version (3 bytes, length of up to 264 bytes)
            len = in[inPos++] & 255;
            ctrl -= in[inPos++] & 255;

            // First: if there is no overlap, can just use arraycopy:
            if ((ctrl + len) < -9) {
                len += 9;
                System.arraycopy(out, outPos + ctrl, out, outPos, len);
                outPos += len;
                continue;
            }

            // otherwise manual copy: so first just copy 9 bytes we know are needed
            out[outPos] = out[outPos++ + ctrl];
            out[outPos] = out[outPos++ + ctrl];
            out[outPos] = out[outPos++ + ctrl];
            out[outPos] = out[outPos++ + ctrl];
            out[outPos] = out[outPos++ + ctrl];
            out[outPos] = out[outPos++ + ctrl];
            out[outPos] = out[outPos++ + ctrl];
            out[outPos] = out[outPos++ + ctrl];
            out[outPos] = out[outPos++ + ctrl];

            // then loop
            // Odd: after extensive profiling, looks like magic number
            // for unrolling is 4: with 8 performance is worse (even
            // bit less than with no unrolling).
            len += outPos;
            final int end = len - 3;
            while (outPos < end) {
                out[outPos] = out[outPos++ + ctrl];
                out[outPos] = out[outPos++ + ctrl];
                out[outPos] = out[outPos++ + ctrl];
                out[outPos] = out[outPos++ + ctrl];
            }
            switch (len - outPos) {
                case 3:
                    out[outPos] = out[outPos++ + ctrl];
                case 2:
                    out[outPos] = out[outPos++ + ctrl];
                case 1:
                    out[outPos] = out[outPos++ + ctrl];
            }
        } while (outPos < outEnd);

        // sanity check to guard against corrupt data:
        if (outPos != outEnd)
            throw new IOException("Corrupt data: overrun in decompress, input offset " + inPos + ", output offset " + outPos);
    }

    private final static int uint16(byte[] data, int ptr) {
        return ((data[ptr] & 0xFF) << 8) + (data[ptr + 1] & 0xFF);
    }

    /**
     * Helper method to forcibly load header bytes that must be read before
     * chunk can be handled.
     */
    protected static int readHeader(final InputStream is, final byte[] inputBuffer)
            throws IOException {
        // Ok: simple case first, where we just get all data we need
        int needed = HEADER_BYTES;
        int count = is.read(inputBuffer, 0, needed);

        if (count == needed) {
            return count;
        }
        if (count <= 0) {
            return 0;
        }

        // if not, a source that trickles data (network etc); must loop
        int offset = count;
        needed -= count;

        do {
            count = is.read(inputBuffer, offset, needed);
            if (count <= 0) {
                break;
            }
            offset += count;
            needed -= count;
        } while (needed > 0);
        return offset;
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
