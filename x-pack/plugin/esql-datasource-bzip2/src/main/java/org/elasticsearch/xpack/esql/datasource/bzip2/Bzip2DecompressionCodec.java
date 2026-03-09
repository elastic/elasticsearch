/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.bzip2;

import org.elasticsearch.xpack.esql.datasources.spi.SplittableDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.elasticsearch.xpack.esql.datasource.bzip2.Bzip2BlockScanner.BZIP2_HEADER_SIZE;
import static org.elasticsearch.xpack.esql.datasource.bzip2.Bzip2BlockScanner.scanBlockOffsets;

/**
 * Bzip2 decompression codec with splittable support for parallel decompression.
 *
 * <p>Uses a forked Hadoop-style {@link CBZip2InputStream} in BYBLOCK mode for split
 * decompression. The decompressor handles bit-aligned block boundaries natively via
 * {@code skipToNextMarker()}, avoiding the need for byte-aligned block offsets.
 *
 * <p>For split decompression, the stream is positioned at the byte containing the
 * block marker. The {@link CBZip2InputStream} in BYBLOCK mode finds the exact bit
 * position, decompresses one block at a time, and reports compressed byte positions
 * at block boundaries. A wrapper monitors these positions and signals EOF when the
 * decompressor passes the split boundary.
 */
public class Bzip2DecompressionCodec implements SplittableDecompressionCodec {

    private static final List<String> EXTENSIONS = List.of(".bz2", ".bz");

    @Override
    public String name() {
        return "bzip2";
    }

    @Override
    public List<String> extensions() {
        return EXTENSIONS;
    }

    @Override
    public InputStream decompress(InputStream raw) throws IOException {
        // Skip the 2-byte 'BZ' file header; CBZip2InputStream expects the stream
        // to start right after it (at the 'h' + block-size digit).
        int b1 = raw.read();
        int b2 = raw.read();
        if (b1 != 'B' || b2 != 'Z') {
            throw new IOException("Not a bzip2 stream: expected 'BZ' header, got [" + b1 + ", " + b2 + "]");
        }
        return new CBZip2InputStream(raw, CBZip2InputStream.ReadMode.CONTINUOUS);
    }

    @Override
    public long[] findBlockBoundaries(StorageObject object, long start, long end) throws IOException {
        if (start >= end) {
            return new long[0];
        }
        try (InputStream stream = object.newStream(start, end - start)) {
            long[] relativeOffsets = scanBlockOffsets(stream, end - start);
            for (int i = 0; i < relativeOffsets.length; i++) {
                relativeOffsets[i] += start;
            }
            return relativeOffsets;
        }
    }

    @Override
    public InputStream decompressRange(StorageObject object, long blockStart, long nextBlockStart) throws IOException {
        if (blockStart >= nextBlockStart) {
            throw new IllegalArgumentException("blockStart [" + blockStart + "] must be less than nextBlockStart [" + nextBlockStart + "]");
        }

        long fileLength = object.length();

        // Always use BYBLOCK mode. The CBZip2InputStream constructor in BYBLOCK mode
        // calls skipToNextBlockMarker() to find the next block magic from the current
        // stream position, then decompresses one block at a time.
        //
        // For the first block (near file start), we position the stream right after
        // the 'BZ' prefix (byte 2). The BYBLOCK constructor will skip the 'h' + digit
        // bytes looking for the first block magic.
        //
        // For mid-file blocks, we position at blockStart. The BYBLOCK constructor
        // scans forward to find the next block magic from that byte position.
        long streamStart;
        int positionOffset;
        if (blockStart <= BZIP2_HEADER_SIZE) {
            streamStart = 0;
            // The 'BZ' prefix (2 bytes) is skipped before passing to CBZip2InputStream,
            // so the decompressor's internal byte counter starts 2 bytes behind the file position
            positionOffset = 2;
        } else {
            streamStart = blockStart;
            positionOffset = (int) blockStart;
        }

        InputStream rawStream = object.newStream(streamStart, fileLength - streamStart);
        if (streamStart == 0) {
            rawStream.skipNBytes(2);
        }

        CBZip2InputStream decompressor = new CBZip2InputStream(rawStream, CBZip2InputStream.ReadMode.BYBLOCK);
        decompressor.updateReportedByteCount(positionOffset);
        return new BlockBoundedDecompressStream(decompressor, nextBlockStart);
    }

    /**
     * Wraps a {@link CBZip2InputStream} and monitors its compressed byte position.
     * Returns decompressed data normally until the compressed position passes the
     * split boundary, then signals EOF.
     *
     * <p>In BYBLOCK mode, the decompressor returns -2 (END_OF_BLOCK) at block
     * boundaries. At each block boundary, we check if the compressed position has
     * passed the limit. If so, we stop. Otherwise we let the decompressor advance
     * to the next block.
     *
     * <p>The single-byte read path is used for all reads because the Hadoop-style
     * decompressor's bulk read can return END_OF_BLOCK (-2) which is not a valid
     * return value for standard {@link InputStream#read(byte[], int, int)}.
     */
    static class BlockBoundedDecompressStream extends InputStream {
        private final CBZip2InputStream decompressor;
        private final long compressedLimit;
        private boolean done;

        BlockBoundedDecompressStream(CBZip2InputStream decompressor, long compressedLimit) {
            this.decompressor = decompressor;
            this.compressedLimit = compressedLimit;
        }

        private int readSingleByte() throws IOException {
            while (done == false) {
                int b = decompressor.read();
                if (b == BZip2Constants.END_OF_BLOCK) {
                    if (decompressor.getProcessedByteCount() >= compressedLimit) {
                        done = true;
                        return -1;
                    }
                    continue;
                }
                if (b == BZip2Constants.END_OF_STREAM || b < 0) {
                    done = true;
                    return -1;
                }
                return b;
            }
            return -1;
        }

        @Override
        public int read() throws IOException {
            return readSingleByte();
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            if (done) {
                return -1;
            }
            int count = 0;
            for (int i = 0; i < len; i++) {
                int b = readSingleByte();
                if (b == -1) {
                    break;
                }
                buf[off + i] = (byte) b;
                count++;
            }
            return count == 0 ? -1 : count;
        }

        @Override
        public byte[] readAllBytes() throws IOException {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] tmp = new byte[8192];
            int n;
            while ((n = read(tmp, 0, tmp.length)) > 0) {
                buf.write(tmp, 0, n);
            }
            return buf.toByteArray();
        }

        @Override
        public void close() throws IOException {
            decompressor.close();
        }
    }
}
