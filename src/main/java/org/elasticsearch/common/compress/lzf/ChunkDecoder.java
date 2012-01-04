package org.elasticsearch.common.compress.lzf;

import java.io.IOException;
import java.io.InputStream;

/**
 * Decoder that handles decoding of sequence of encoded LZF chunks,
 * combining them into a single contiguous result byte array.
 *
 * @author Tatu Saloranta (tatu@ning.com)
 * @since 0.9
 */
public abstract class ChunkDecoder {
    protected final static byte BYTE_NULL = 0;
    protected final static int HEADER_BYTES = 5;

    public ChunkDecoder() {
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Public API
    ///////////////////////////////////////////////////////////////////////
     */

    /**
     * Method for decompressing a block of input data encoded in LZF
     * block structure (compatible with lzf command line utility),
     * and can consist of any number of blocks.
     * Note that input MUST consists of a sequence of one or more complete
     * chunks; partial chunks can not be handled.
     */
    public final byte[] decode(final byte[] inputBuffer) throws IOException {
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
     */
    public final byte[] decode(final byte[] inputBuffer, int inputPtr, int inputLen) throws IOException {
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
    public final int decode(final byte[] inputBuffer, final byte[] targetBuffer) throws IOException {
        return decode(inputBuffer, 0, inputBuffer.length, targetBuffer);
    }

    /**
     * Method for decompressing a block of input data encoded in LZF
     * block structure (compatible with lzf command line utility),
     * and can consist of any number of blocks.
     * Note that input MUST consists of a sequence of one or more complete
     * chunks; partial chunks can not be handled.
     */
    public int decode(final byte[] sourceBuffer, int inPtr, int inLength,
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
                decodeChunk(sourceBuffer, inPtr, result, outPtr, outPtr + uncompLen);
                outPtr += uncompLen;
            }
            inPtr += len;
            ++blockNr;
        }
        return outPtr;
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
    public abstract int decodeChunk(final InputStream is, final byte[] inputBuffer, final byte[] outputBuffer)
            throws IOException;

    /**
     * Main decode method for individual chunks.
     */
    public abstract void decodeChunk(byte[] in, int inPos, byte[] out, int outPos, int outEnd)
            throws IOException;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Public static methods
    ///////////////////////////////////////////////////////////////////////
     */

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
        if (ptr != end) {
            throw new IOException("Corrupt input data: block #" + blockNr + " extends " + (data.length - ptr) + " beyond end of input");
        }
        return uncompressedSize;
    }

    /*
   ///////////////////////////////////////////////////////////////////////
   // Internal methods
   ///////////////////////////////////////////////////////////////////////
    */

    protected final static int uint16(byte[] data, int ptr) {
        return ((data[ptr] & 0xFF) << 8) + (data[ptr + 1] & 0xFF);
    }

    /**
     * Helper method to forcibly load header bytes that must be read before
     * chunk can be handled.
     */
    protected final static int readHeader(final InputStream is, final byte[] inputBuffer)
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

    protected final static void readFully(InputStream is, boolean compressed,
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
