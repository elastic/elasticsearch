package org.elasticsearch.common.compress.lzf.impl;

import org.elasticsearch.common.compress.lzf.ChunkDecoder;
import org.elasticsearch.common.compress.lzf.LZFChunk;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;

/**
 * Highly optimized {@link ChunkDecoder} implementation that uses
 * Sun JDK's Unsafe class (which may be included by other JDK's as well;
 * IBM's apparently does).
 * <p/>
 * Credits for the idea go to Dain Sundstrom, who kindly suggested this use,
 * and is all-around great source for optimization tips and tricks.
 */
@SuppressWarnings("restriction")
public class UnsafeChunkDecoder extends ChunkDecoder {
    private static final Unsafe unsafe;

    static {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            unsafe = (Unsafe) theUnsafe.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final long BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
//    private static final long SHORT_ARRAY_OFFSET = unsafe.arrayBaseOffset(short[].class);
//    private static final long SHORT_ARRAY_STRIDE = unsafe.arrayIndexScale(short[].class);

    public UnsafeChunkDecoder() {
    }

    @Override
    public final int decodeChunk(final InputStream is, final byte[] inputBuffer, final byte[] outputBuffer)
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
            decodeChunk(inputBuffer, 2, outputBuffer, 0, uncompLen);
            bytesInOutput = uncompLen;
        }
        return bytesInOutput;
    }

    @Override
    public final void decodeChunk(byte[] in, int inPos, byte[] out, int outPos, int outEnd)
            throws IOException {
        main_loop:
        do {
            int ctrl = in[inPos++] & 255;
            while (ctrl < LZFChunk.MAX_LITERAL) { // literal run(s)
                copyUpTo32(in, inPos, out, outPos, ctrl);
                ++ctrl;
                inPos += ctrl;
                outPos += ctrl;
                if (outPos >= outEnd) {
                    break main_loop;
                }
                ctrl = in[inPos++] & 255;
            }
            // back reference
            int len = ctrl >> 5;
            ctrl = -((ctrl & 0x1f) << 8) - 1;
            // short back reference? 2 bytes; run lengths of 2 - 8 bytes
            if (len < 7) {
                ctrl -= in[inPos++] & 255;
                if (ctrl < -7) { // non-overlapping? can use efficient bulk copy
                    moveLong(out, outPos, outEnd, ctrl);
                    outPos += len + 2;
                    continue;
                }
                // otherwise, byte-by-byte
                outPos = copyOverlappingShort(out, outPos, ctrl, len);
                continue;
            }
            // long back reference: 3 bytes, length of up to 264 bytes
            len = in[inPos++] & 255;
            ctrl -= in[inPos++] & 255;
            // First: ovelapping case can't use default handling, off line:
            if ((ctrl + len) >= -9) {
                outPos = copyOverlappingLong(out, outPos, ctrl, len);
                continue;
            }
            // but non-overlapping is simple
            len += 9;
            if (len <= 32) {
                copyUpTo32(out, outPos + ctrl, out, outPos, len - 1);
            } else {
                System.arraycopy(out, outPos + ctrl, out, outPos, len);
            }
            outPos += len;
        } while (outPos < outEnd);

        // sanity check to guard against corrupt data:
        if (outPos != outEnd)
            throw new IOException("Corrupt data: overrun in decompress, input offset " + inPos + ", output offset " + outPos);
    }

    /*
   ///////////////////////////////////////////////////////////////////////
   // Internal methods
   ///////////////////////////////////////////////////////////////////////
    */

    private final int copyOverlappingShort(final byte[] out, int outPos, final int offset, int len) {
        out[outPos] = out[outPos++ + offset];
        out[outPos] = out[outPos++ + offset];
        switch (len) {
            case 6:
                out[outPos] = out[outPos++ + offset];
            case 5:
                out[outPos] = out[outPos++ + offset];
            case 4:
                out[outPos] = out[outPos++ + offset];
            case 3:
                out[outPos] = out[outPos++ + offset];
            case 2:
                out[outPos] = out[outPos++ + offset];
            case 1:
                out[outPos] = out[outPos++ + offset];
        }
        return outPos;
    }

    private final static int copyOverlappingLong(final byte[] out, int outPos, final int offset, int len) {
        // otherwise manual copy: so first just copy 9 bytes we know are needed
        out[outPos] = out[outPos++ + offset];
        out[outPos] = out[outPos++ + offset];
        out[outPos] = out[outPos++ + offset];
        out[outPos] = out[outPos++ + offset];
        out[outPos] = out[outPos++ + offset];
        out[outPos] = out[outPos++ + offset];
        out[outPos] = out[outPos++ + offset];
        out[outPos] = out[outPos++ + offset];
        out[outPos] = out[outPos++ + offset];

        // then loop
        // Odd: after extensive profiling, looks like magic number
        // for unrolling is 4: with 8 performance is worse (even
        // bit less than with no unrolling).
        len += outPos;
        final int end = len - 3;
        while (outPos < end) {
            out[outPos] = out[outPos++ + offset];
            out[outPos] = out[outPos++ + offset];
            out[outPos] = out[outPos++ + offset];
            out[outPos] = out[outPos++ + offset];
        }
        switch (len - outPos) {
            case 3:
                out[outPos] = out[outPos++ + offset];
            case 2:
                out[outPos] = out[outPos++ + offset];
            case 1:
                out[outPos] = out[outPos++ + offset];
        }
        return outPos;
    }

    /* Note: 'delta' is negative (back ref); dataEnd is the first location AFTER
     * end of expected uncompressed data (i.e. end marker)
     */
    private final static void moveLong(byte[] data, int resultOffset, int dataEnd, int delta) {
        if ((resultOffset + 8) < dataEnd) {
            final long rawOffset = BYTE_ARRAY_OFFSET + resultOffset;
            long value = unsafe.getLong(data, rawOffset + delta);
            unsafe.putLong(data, rawOffset, value);
            return;
        }
        System.arraycopy(data, resultOffset + delta, data, resultOffset, data.length - resultOffset);
    }

    private final static void copyUpTo32(byte[] in, int inputIndex, byte[] out, int outputIndex, int lengthMinusOne) {
        if ((outputIndex + 32) > out.length) {
            System.arraycopy(in, inputIndex, out, outputIndex, lengthMinusOne + 1);
            return;
        }
        long inPtr = BYTE_ARRAY_OFFSET + inputIndex;
        long outPtr = BYTE_ARRAY_OFFSET + outputIndex;

        switch (lengthMinusOne >>> 3) {
            case 3: {
                long value = unsafe.getLong(in, inPtr);
                unsafe.putLong(out, outPtr, value);
                inPtr += 8;
                outPtr += 8;
                value = unsafe.getLong(in, inPtr);
                unsafe.putLong(out, outPtr, value);
                inPtr += 8;
                outPtr += 8;
                value = unsafe.getLong(in, inPtr);
                unsafe.putLong(out, outPtr, value);
                inPtr += 8;
                outPtr += 8;
                value = unsafe.getLong(in, inPtr);
                unsafe.putLong(out, outPtr, value);
            }
            break;
            case 2: {
                long value = unsafe.getLong(in, inPtr);
                unsafe.putLong(out, outPtr, value);
                inPtr += 8;
                outPtr += 8;
                value = unsafe.getLong(in, inPtr);
                unsafe.putLong(out, outPtr, value);
                inPtr += 8;
                outPtr += 8;
                value = unsafe.getLong(in, inPtr);
                unsafe.putLong(out, outPtr, value);
            }
            break;
            case 1: {
                long value = unsafe.getLong(in, inPtr);
                unsafe.putLong(out, outPtr, value);
                inPtr += 8;
                outPtr += 8;
                value = unsafe.getLong(in, inPtr);
                unsafe.putLong(out, outPtr, value);
            }
            break;
            case 0: {
                long value = unsafe.getLong(in, inPtr);
                unsafe.putLong(out, outPtr, value);
            }
        }
    }
}