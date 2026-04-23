/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.bzip2;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.SplittableDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static org.elasticsearch.xpack.esql.datasource.bzip2.Bzip2BlockScanner.mergeSortedUnique;
import static org.elasticsearch.xpack.esql.datasource.bzip2.Bzip2BlockScanner.scanBlockOffsets;

/**
 * Bzip2 decompression codec with splittable support for parallel decompression.
 *
 * <p>Both sequential whole-file decompression and range decompression use
 * {@link CBZip2InputStream} in BYBLOCK mode. BYBLOCK bit-scans for the 48-bit block magic,
 * which naturally skips end-of-stream markers and member headers in files that consist of
 * several bzip2 streams back-to-back (common for large fixtures). So a single code path
 * handles both single-member and concatenated bzip2 files and produces the same byte
 * stream as {@code bzcat}.
 *
 * <p>For split decompression, the stream is positioned at the byte containing the
 * block marker. The {@link CBZip2InputStream} in BYBLOCK mode finds the exact bit
 * position, decompresses one block at a time, and reports compressed byte positions
 * at block boundaries. A wrapper monitors these positions and signals EOF when the
 * decompressor passes the split boundary.
 *
 * <p>Parallel block-boundary scanning calls {@link StorageObject#newStream(long, long)} from
 * multiple threads with disjoint byte ranges. Implementations used with this codec must
 * support concurrent range opens (or callers must not trigger the parallel path).
 */
public class Bzip2DecompressionCodec implements SplittableDecompressionCodec {

    private static final List<String> EXTENSIONS = List.of(".bz2", ".bz");

    private final Executor scanExecutor;

    /**
     * @param scanExecutor Elasticsearch-managed executor (e.g. {@code generic} thread pool) used for
     *                       parallel block-boundary scans; must not be {@code null}.
     */
    public Bzip2DecompressionCodec(Executor scanExecutor) {
        Check.notNull(scanExecutor, "scanExecutor must not be null");
        this.scanExecutor = scanExecutor;
    }

    /**
     * Minimum compressed span to use parallel block scanning (overlapped chunks). Smaller
     * ranges use a single sequential pass to avoid thread overhead.
     */
    static final long MIN_PARALLEL_SCAN_BYTES = ByteSizeValue.ofMb(10).getBytes();

    /** Target bytes per chunk before capping by {@link Runtime#availableProcessors()}. */
    private static final long TARGET_CHUNK_BYTES = ByteSizeValue.ofKb(512).getBytes();

    /**
     * Overlap between consecutive scan chunks so a 48-bit block magic cannot straddle a
     * chunk boundary without appearing fully in at least one chunk scan.
     */
    static final int CHUNK_OVERLAP_BYTES = 8;

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
        // Validate the bzip2 header up front: BYBLOCK mode bit-scans for block magics and
        // would silently produce zero output for non-bzip2 input, but callers expect an
        // IOException for malformed/non-bzip2 streams.
        PushbackInputStream pb = new PushbackInputStream(raw, 4);
        byte[] hdr = new byte[4];
        int read = 0;
        while (read < 4) {
            int n = pb.read(hdr, read, 4 - read);
            if (n < 0) {
                break;
            }
            read += n;
        }
        if (read < 4 || hdr[0] != 'B' || hdr[1] != 'Z' || hdr[2] != 'h' || hdr[3] < '1' || hdr[3] > '9') {
            throw new IOException("Stream is not BZip2 formatted: missing 'BZh[1-9]' header");
        }
        pb.unread(hdr, 0, read);
        // BYBLOCK mode bit-scans for block magics and transparently skips end-of-stream /
        // next-member headers, so concatenated bzip2 files decompress as one logical stream.
        // The wrapper absorbs END_OF_BLOCK markers and exposes a plain InputStream.
        return new ConcatenatedDecompressStream(new CBZip2InputStream(pb, CBZip2InputStream.ReadMode.BYBLOCK));
    }

    /**
     * {@inheritDoc}
     *
     * <p>When the compressed range is large enough, scans run in parallel on the injected executor.
     * Each task opens its own stream via {@link StorageObject#newStream(long, long)} for a disjoint
     * sub-range (plus small overlap between chunks); the {@code StorageObject} must permit that
     * concurrently.
     */
    @Override
    public long[] findBlockBoundaries(StorageObject object, long start, long end) throws IOException {
        if (start >= end) {
            return new long[0];
        }
        long rangeLen = end - start;
        int processors = Runtime.getRuntime().availableProcessors();
        if (rangeLen < MIN_PARALLEL_SCAN_BYTES || processors < 2) {
            return findBlockBoundariesSequential(object, start, rangeLen);
        }
        long estChunksLong = (rangeLen + TARGET_CHUNK_BYTES - 1) / TARGET_CHUNK_BYTES;
        int estChunks = estChunksLong > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) estChunksLong;
        int numChunks = Math.min(processors, estChunks);
        if (numChunks < 2) {
            return findBlockBoundariesSequential(object, start, rangeLen);
        }
        long chunkLen = (rangeLen + numChunks - 1) / numChunks;
        @SuppressWarnings("unchecked")
        CompletableFuture<long[]>[] futures = new CompletableFuture[numChunks];
        for (int k = 0; k < numChunks; k++) {
            long segStart = k * chunkLen;
            long readStart = Math.max(0L, segStart - CHUNK_OVERLAP_BYTES);
            long readEnd = Math.min(rangeLen, (k + 1L) * chunkLen + CHUNK_OVERLAP_BYTES);
            long readLen = readEnd - readStart;
            final long rs = readStart;
            final long rl = readLen;
            futures[k] = CompletableFuture.supplyAsync(() -> {
                try (InputStream stream = object.newStream(start + rs, rl)) {
                    long[] offsets = scanBlockOffsets(stream, rl);
                    for (int i = 0; i < offsets.length; i++) {
                        offsets[i] += start + rs;
                    }
                    return offsets;
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
            }, scanExecutor);
        }
        try {
            CompletableFuture.allOf(futures).join();
        } catch (CompletionException e) {
            Throwable c = e.getCause();
            if (c instanceof IOException ioe) {
                throw ioe;
            }
            if (c instanceof Error err) {
                throw err;
            }
            throw new IOException("Failed parallel bzip2 block scan", c);
        }
        long[][] parts = new long[numChunks][];
        for (int i = 0; i < numChunks; i++) {
            // After allOf().join() every future is done successfully; getNow avoids redundant blocking joins.
            parts[i] = futures[i].getNow(null);
        }
        return mergeSortedUnique(parts);
    }

    private static long[] findBlockBoundariesSequential(StorageObject object, long start, long rangeLen) throws IOException {
        try (InputStream stream = object.newStream(start, rangeLen)) {
            long[] relativeOffsets = scanBlockOffsets(stream, rangeLen);
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
        if (blockStart == 0 && nextBlockStart == fileLength) {
            return decompress(object.newStream());
        }
        return decompressOneMemberRange(object, blockStart, nextBlockStart, fileLength);
    }

    /**
     * Decompress a span of compressed file bytes. Uses {@link CBZip2InputStream} in BYBLOCK mode
     * with a suffix of the file so the first bzip2 block can be read in full even when a block
     * extends past {@code e}. The stream is clipped at {@code e} by {@link BlockBoundedDecompressStream}.
     *
     * <p>BYBLOCK bit-scans for the 48-bit block magic. Across concatenated bzip2 members the
     * bit-scan skips over a member's end-of-stream magic and the next member's {@code BZh#} header
     * and lands on the next block magic, so this path handles single-member and concatenated bzip2
     * files uniformly without needing to discover member starts.
     */
    private static InputStream decompressOneMemberRange(StorageObject object, long s, long e, long fileLength) throws IOException {
        if (s >= e) {
            return new ByteArrayInputStream(new byte[0]);
        }
        long streamStart;
        long initialCompressedPosition;
        if (s == 0) {
            streamStart = 0;
            initialCompressedPosition = 2;
        } else {
            streamStart = s;
            initialCompressedPosition = s;
        }
        InputStream rawStream = object.newStream(streamStart, fileLength - streamStart);
        if (streamStart == 0) {
            rawStream.skipNBytes(2);
        }
        CBZip2InputStream decompressor = new CBZip2InputStream(rawStream, CBZip2InputStream.ReadMode.BYBLOCK);
        decompressor.updateReportedByteCount(initialCompressedPosition);
        return new BlockBoundedDecompressStream(decompressor, e);
    }

    /**
     * Wraps a {@link CBZip2InputStream} and monitors its compressed byte position.
     * Emits decompressed data normally while the compressed position is under the
     * split boundary. Once the boundary is reached at a block end, enters
     * "finish-current-line" mode: continues emitting decompressed bytes from the
     * following block(s) until (and including) the next {@code '\n'}, then signals
     * EOF. This lets a single line record that straddles a macro-split boundary be
     * fully emitted by the split on the <em>left</em> of the boundary; the split on
     * the <em>right</em> then drops that same tail via {@code skipFirstLine}, so no
     * record is lost or double-counted across macro-split boundaries.
     *
     * <p>In BYBLOCK mode, the decompressor returns -2 (END_OF_BLOCK) at block
     * boundaries.
     *
     * <p>The single-byte read path is used for all reads because the Hadoop-style
     * decompressor's bulk read can return END_OF_BLOCK (-2) which is not a valid
     * return value for standard {@link InputStream#read(byte[], int, int)}.
     */
    static class BlockBoundedDecompressStream extends InputStream {
        private static final byte LF = (byte) '\n';
        private final CBZip2InputStream decompressor;
        private final long compressedLimit;
        private boolean done;
        private boolean finishingLine;

        BlockBoundedDecompressStream(CBZip2InputStream decompressor, long compressedLimit) {
            this.decompressor = decompressor;
            this.compressedLimit = compressedLimit;
        }

        private int readSingleByte() throws IOException {
            while (done == false) {
                int b = decompressor.read();
                if (b == BZip2Constants.END_OF_BLOCK) {
                    if (finishingLine == false && decompressor.getProcessedByteCount() >= compressedLimit) {
                        finishingLine = true;
                    }
                    continue;
                }
                if (b == BZip2Constants.END_OF_STREAM || b < 0) {
                    done = true;
                    return -1;
                }
                if (finishingLine && b == (LF & 0xFF)) {
                    done = true;
                    return b;
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
            // InputStream contract: a zero-length read returns 0, not -1. JDK's
            // readAllBytes() probes with len == 0 once a buffer page is full and would
            // otherwise treat -1 as EOF and stop after the first page.
            if (len == 0) {
                return 0;
            }
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

    /**
     * Adapts a {@link CBZip2InputStream} in BYBLOCK mode to a plain {@link InputStream} that
     * emits the fully-decompressed bytes of the underlying bzip2 input. BYBLOCK returns
     * {@link BZip2Constants#END_OF_BLOCK} at block boundaries (including across concatenated
     * bzip2 members) and {@link BZip2Constants#END_OF_STREAM} at true end-of-file; this
     * wrapper absorbs the per-block markers and reports EOF only at end-of-stream.
     */
    static class ConcatenatedDecompressStream extends InputStream {
        private final CBZip2InputStream decompressor;
        private boolean done;

        ConcatenatedDecompressStream(CBZip2InputStream decompressor) {
            this.decompressor = decompressor;
        }

        private int readSingleByte() throws IOException {
            while (done == false) {
                int b = decompressor.read();
                if (b == BZip2Constants.END_OF_BLOCK) {
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
            // InputStream contract: a zero-length read returns 0, not -1. JDK's
            // readAllBytes() probes with len == 0 once a buffer page is full and would
            // otherwise treat -1 as EOF and stop after the first page.
            if (len == 0) {
                return 0;
            }
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
        public void close() throws IOException {
            decompressor.close();
        }
    }
}
