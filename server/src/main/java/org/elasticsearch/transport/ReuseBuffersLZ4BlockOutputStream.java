/*
 * Copyright 2020 Adrien Grand and the lz4-java contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.transport;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4FrameOutputStream;
import net.jpountz.util.SafeUtils;

import org.apache.lucene.util.BytesRef;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * This file is forked from https://github.com/lz4/lz4-java. In particular it forks the following file
 * net.jpountz.lz4.LZ4BlockOutputStream.
 *
 * It modifies the original lz4-java code to allow the reuse of local thread local byte arrays. This prevents
 * the need to allocate two new byte arrays everytime a new stream is created. For the Elasticsearch use case,
 * a single thread should fully compress the stream in one go to avoid memory corruption.
 *
 * Additionally, it does not checksum (or write a check) for the data compressed. We do not read the checksum
 * when decompressing in Elasticsearch.
 *
 * Streaming LZ4 (not compatible with the LZ4 Frame format).
 * This class compresses data into fixed-size blocks of compressed data.
 * This class uses its own format and is not compatible with the LZ4 Frame format.
 * For interoperability with other LZ4 tools, use {@link LZ4FrameOutputStream},
 * which is compatible with the LZ4 Frame format. This class remains for backward compatibility.
 * @see LZ4BlockInputStream
 * @see LZ4FrameOutputStream
 */
public class ReuseBuffersLZ4BlockOutputStream extends FilterOutputStream {

    private static class ArrayBox {
        private byte[] uncompressed = BytesRef.EMPTY_BYTES;
        private byte[] compressed = BytesRef.EMPTY_BYTES;
        private boolean owned = false;

        private void markOwnership(int uncompressedBlockSize, int compressedMaxSize) {
            assert owned == false;
            owned = true;
            if (uncompressedBlockSize > uncompressed.length) {
                uncompressed = new byte[uncompressedBlockSize];
            }
            if (compressedMaxSize > compressed.length) {
                compressed = new byte[compressedMaxSize];
            }
        }

        private void release() {
            owned = false;
        }
    }

    private static final ThreadLocal<ArrayBox> ARRAY_BOX = ThreadLocal.withInitial(ArrayBox::new);

    static final byte[] MAGIC = new byte[] { 'L', 'Z', '4', 'B', 'l', 'o', 'c', 'k' };
    static final int MAGIC_LENGTH = MAGIC.length;

    static final int HEADER_LENGTH = MAGIC_LENGTH // magic bytes
        + 1          // token
        + 4          // compressed length
        + 4          // decompressed length
        + 4;         // checksum

    static final int COMPRESSION_LEVEL_BASE = 10;
    static final int MIN_BLOCK_SIZE = 64;
    static final int MAX_BLOCK_SIZE = 1 << (COMPRESSION_LEVEL_BASE + 0x0F);

    static final int COMPRESSION_METHOD_RAW = 0x10;
    static final int COMPRESSION_METHOD_LZ4 = 0x20;

    static final int DEFAULT_SEED = 0x9747b28c;

    private static int compressionLevel(int blockSize) {
        if (blockSize < MIN_BLOCK_SIZE) {
            throw new IllegalArgumentException("blockSize must be >= " + MIN_BLOCK_SIZE + ", got " + blockSize);
        } else if (blockSize > MAX_BLOCK_SIZE) {
            throw new IllegalArgumentException("blockSize must be <= " + MAX_BLOCK_SIZE + ", got " + blockSize);
        }
        int compressionLevel = 32 - Integer.numberOfLeadingZeros(blockSize - 1); // ceil of log2
        assert (1 << compressionLevel) >= blockSize;
        assert blockSize * 2 > (1 << compressionLevel);
        compressionLevel = Math.max(0, compressionLevel - COMPRESSION_LEVEL_BASE);
        assert compressionLevel >= 0 && compressionLevel <= 0x0F;
        return compressionLevel;
    }

    private final int blockSize;
    private final int compressionLevel;
    private final LZ4Compressor compressor;
    private final ArrayBox arrayBox;
    private final byte[] buffer;
    private final byte[] compressedBuffer;
    private final boolean syncFlush;
    private boolean finished;
    private int o;

    /**
     * Creates a new {@link OutputStream} with configurable block size. Large
     * blocks require more memory at compression and decompression time but
     * should improve the compression ratio.
     *
     * @param out         the {@link OutputStream} to feed
     * @param blockSize   the maximum number of bytes to try to compress at once,
     *                    must be &gt;= 64 and &lt;= 32 M
     * @param compressor  the {@link LZ4Compressor} instance to use to compress
     *                    data
     */
    public ReuseBuffersLZ4BlockOutputStream(OutputStream out, int blockSize, LZ4Compressor compressor) {
        super(out);
        this.blockSize = blockSize;
        this.compressor = compressor;
        this.compressionLevel = compressionLevel(blockSize);
        final int compressedBlockSize = HEADER_LENGTH + compressor.maxCompressedLength(blockSize);
        this.arrayBox = ARRAY_BOX.get();
        arrayBox.markOwnership(blockSize, compressedBlockSize);
        this.buffer = arrayBox.uncompressed;
        this.compressedBuffer = arrayBox.compressed;
        this.syncFlush = false;
        o = 0;
        finished = false;
        System.arraycopy(MAGIC, 0, compressedBuffer, 0, MAGIC_LENGTH);
    }

    private void ensureNotFinished() {
        if (finished) {
            throw new IllegalStateException("This stream is already closed");
        }
    }

    @Override
    public void write(int b) throws IOException {
        ensureNotFinished();
        if (o == blockSize) {
            flushBufferedData();
        }
        buffer[o++] = (byte) b;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        SafeUtils.checkRange(b, off, len);
        ensureNotFinished();

        while (o + len > blockSize) {
            final int l = blockSize - o;
            System.arraycopy(b, off, buffer, o, blockSize - o);
            o = blockSize;
            flushBufferedData();
            off += l;
            len -= l;
        }
        System.arraycopy(b, off, buffer, o, len);
        o += len;
    }

    @Override
    public void write(byte[] b) throws IOException {
        ensureNotFinished();
        write(b, 0, b.length);
    }

    @Override
    public void close() throws IOException {
        try {
            if (finished == false) {
                finish();
            }
            if (out != null) {
                out.close();
                out = null;
            }
        } finally {
            arrayBox.release();
        }
    }

    private void flushBufferedData() throws IOException {
        if (o == 0) {
            return;
        }
        int compressedLength = compressor.compress(buffer, 0, o, compressedBuffer, HEADER_LENGTH);
        final int compressMethod;
        if (compressedLength >= o) {
            compressMethod = COMPRESSION_METHOD_RAW;
            compressedLength = o;
            System.arraycopy(buffer, 0, compressedBuffer, HEADER_LENGTH, o);
        } else {
            compressMethod = COMPRESSION_METHOD_LZ4;
        }

        compressedBuffer[MAGIC_LENGTH] = (byte) (compressMethod | compressionLevel);
        writeIntLE(compressedLength, compressedBuffer, MAGIC_LENGTH + 1);
        writeIntLE(o, compressedBuffer, MAGIC_LENGTH + 5);
        // Write 0 for checksum. We do not read it on decompress.
        writeIntLE(0, compressedBuffer, MAGIC_LENGTH + 9);
        assert MAGIC_LENGTH + 13 == HEADER_LENGTH;
        out.write(compressedBuffer, 0, HEADER_LENGTH + compressedLength);
        o = 0;
    }

    /**
     * Flushes this compressed {@link OutputStream}.
     *
     * If the stream has been created with <code>syncFlush=true</code>, pending
     * data will be compressed and appended to the underlying {@link OutputStream}
     * before calling {@link OutputStream#flush()} on the underlying stream.
     * Otherwise, this method just flushes the underlying stream, so pending
     * data might not be available for reading until {@link #finish()} or
     * {@link #close()} is called.
     */
    @Override
    public void flush() throws IOException {
        if (out != null) {
            if (syncFlush) {
                flushBufferedData();
            }
            out.flush();
        }
    }

    /**
     * Same as {@link #close()} except that it doesn't close the underlying stream.
     * This can be useful if you want to keep on using the underlying stream.
     *
     * @throws IOException if an I/O error occurs.
     */
    public void finish() throws IOException {
        ensureNotFinished();
        flushBufferedData();
        compressedBuffer[MAGIC_LENGTH] = (byte) (COMPRESSION_METHOD_RAW | compressionLevel);
        writeIntLE(0, compressedBuffer, MAGIC_LENGTH + 1);
        writeIntLE(0, compressedBuffer, MAGIC_LENGTH + 5);
        writeIntLE(0, compressedBuffer, MAGIC_LENGTH + 9);
        assert MAGIC_LENGTH + 13 == HEADER_LENGTH;
        out.write(compressedBuffer, 0, HEADER_LENGTH);
        finished = true;
        out.flush();
    }

    private static void writeIntLE(int i, byte[] buf, int off) {
        buf[off++] = (byte) i;
        buf[off++] = (byte) (i >>> 8);
        buf[off++] = (byte) (i >>> 16);
        buf[off++] = (byte) (i >>> 24);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(out=" + out + ", blockSize=" + blockSize + ", compressor=" + compressor + ")";
    }

}
