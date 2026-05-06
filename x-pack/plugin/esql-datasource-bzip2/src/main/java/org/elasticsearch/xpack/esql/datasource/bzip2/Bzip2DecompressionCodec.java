/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.bzip2;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.SplittableDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.List;
import java.util.Objects;
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
     * Length in bytes of the bzip2 file header ({@code "BZh1".."BZh9"}): 2 signature bytes
     * {@code 'B','Z'} + 1 version byte {@code 'h'} + 1 block-size digit.
     */
    static final int BZIP2_HEADER_SIZE = 4;

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
        // This method takes ownership of {@code raw}: the returned stream's {@link InputStream#close()}
        // closes it via {@link CBZip2InputStream#close()}. If we throw before constructing that
        // stream, we must close {@code raw} ourselves or the caller leaks it.
        try {
            // Validate the bzip2 header up front: BYBLOCK mode bit-scans for block magics and
            // would silently produce zero output for non-bzip2 input, but callers expect an
            // IOException for malformed/non-bzip2 streams.
            PushbackInputStream pb = new PushbackInputStream(raw, BZIP2_HEADER_SIZE);
            byte[] hdr = new byte[BZIP2_HEADER_SIZE];
            int read = 0;
            while (read < BZIP2_HEADER_SIZE) {
                int n = pb.read(hdr, read, BZIP2_HEADER_SIZE - read);
                if (n < 0) {
                    break;
                }
                read += n;
            }
            if (read < BZIP2_HEADER_SIZE || hdr[0] != 'B' || hdr[1] != 'Z' || hdr[2] != 'h' || hdr[3] < '1' || hdr[3] > '9') {
                throw new IOException("Stream is not BZip2 formatted: missing 'BZh[1-9]' header");
            }
            pb.unread(hdr, 0, read);
            // BYBLOCK mode bit-scans for block magics and transparently skips end-of-stream /
            // next-member headers, so concatenated bzip2 files decompress as one logical stream.
            // The wrapper absorbs END_OF_BLOCK markers and exposes a plain InputStream.
            return new ConcatenatedDecompressStream(new CBZip2InputStream(pb, CBZip2InputStream.ReadMode.BYBLOCK));
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(raw);
            throw t;
        }
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
        // Non-zero boundaries always come from the block scan in {@link #findBlockBoundaries},
        // which matches 48-bit block magics; the earliest possible block magic lands at the byte
        // immediately following the 4-byte {@code BZh#} file header. A boundary inside the
        // header (0 < s < 4) would put the decoder off by up to 3 bytes and is a bug in whatever
        // produced {@code s}.
        assert s == 0 || s >= BZIP2_HEADER_SIZE : "split boundary must be 0 or at/past the BZh# header, got s=" + s;
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
        try {
            if (streamStart == 0) {
                rawStream.skipNBytes(2);
            }
            CBZip2InputStream decompressor = new CBZip2InputStream(rawStream, CBZip2InputStream.ReadMode.BYBLOCK);
            decompressor.updateReportedByteCount(initialCompressedPosition);
            return new BlockBoundedDecompressStream(decompressor, e);
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(rawStream);
            throw t;
        }
    }

    /**
     * Shared base for the two BYBLOCK-mode adapters over {@link CBZip2InputStream}. Centralises
     * the single-byte read loop, {@link InputStream#read(byte[], int, int)} contract handling
     * ({@code len == 0} short-circuit, argument validation, {@code done} gate, -1-on-empty), and
     * {@link #close()}. Subclasses only implement {@link #readSingleByte()} to encode the
     * per-wrapper semantics of what to do at {@link BZip2Constants#END_OF_BLOCK} /
     * {@link BZip2Constants#END_OF_STREAM}.
     *
     * <p>The Hadoop-style decompressor's bulk read can return {@code END_OF_BLOCK} (-2), which is
     * not a valid return for standard {@link InputStream#read(byte[], int, int)}; this base class
     * always goes through single-byte reads to avoid leaking the sentinel to callers.
     */
    abstract static class AbstractBzip2DecompressStream extends InputStream {
        protected final CBZip2InputStream decompressor;
        protected boolean done;

        AbstractBzip2DecompressStream(CBZip2InputStream decompressor) {
            this.decompressor = decompressor;
        }

        /**
         * Reads one decoded byte (0..255) or returns {@code -1} on end-of-stream. Subclasses
         * are responsible for handling {@link BZip2Constants#END_OF_BLOCK} and
         * {@link BZip2Constants#END_OF_STREAM} and for updating {@link #done}.
         */
        protected abstract int readSingleByte() throws IOException;

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
            // Aligns with InputStream.read(byte[], int, int): throw IOOBE on bad args.
            // Run after the len==0 short-circuit so callers passing a null buf with len==0
            // (legal per the contract) don't trip on a spurious NPE in checkFromIndexSize.
            Objects.checkFromIndexSize(off, len, buf.length);
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
     * <p>Protocol cross-references (prose because the splitter and reader live in sibling
     * plugin modules that are not javadoc-visible here):
     * <ul>
     *   <li>Splitter side — {@code FileSplitProvider.tryBlockAlignedSplits} in the {@code esql}
     *       module emits disjoint macro-splits and marks every non-first split as needing
     *       skip-first-line handling.</li>
     *   <li>Reader side — {@code NdJsonPageIterator.skipToNextLine} in the
     *       {@code esql-datasource-ndjson} module drops the leading partial line on every
     *       non-first split, balancing the finish-current-line bytes emitted here.</li>
     * </ul>
     */
    static final class BlockBoundedDecompressStream extends AbstractBzip2DecompressStream {
        private static final byte LF = (byte) '\n';
        private final long compressedLimit;
        /**
         * Once set, reads continue byte-by-byte until the next {@code '\n'}, at which point EOF
         * is signalled to the caller. If no {@code '\n'} appears before {@code END_OF_STREAM}
         * (the "unterminated last line" case), the stream falls through the {@code END_OF_STREAM}
         * branch in {@link #readSingleByte()} and returns {@code -1} cleanly.
         */
        private boolean finishingLine;

        BlockBoundedDecompressStream(CBZip2InputStream decompressor, long compressedLimit) {
            super(decompressor);
            this.compressedLimit = compressedLimit;
        }

        @Override
        protected int readSingleByte() throws IOException {
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
    }

    /**
     * Adapts a {@link CBZip2InputStream} in BYBLOCK mode to a plain {@link InputStream} that
     * emits the fully-decompressed bytes of the underlying bzip2 input. BYBLOCK returns
     * {@link BZip2Constants#END_OF_BLOCK} at block boundaries (including across concatenated
     * bzip2 members) and {@link BZip2Constants#END_OF_STREAM} at true end-of-file; this
     * wrapper absorbs the per-block markers and reports EOF only at end-of-stream.
     *
     * <p>Concatenated-member invariant: in BYBLOCK mode, {@code CBZip2InputStream.initBlock()}
     * skips magic validation and {@code skipToNextBlockMarker()} does a 48-bit bit-scan. After a
     * member's {@code END_OF_STREAM} the scan transparently skips the next member's
     * {@code BZh#} header and lands on that member's first block marker, which is why a single
     * decoder instance handles any number of concatenated members without re-initialization.
     * This also means {@code blockSize100k} is fixed at construction (always 9 in BYBLOCK) even
     * when a later member was compressed with a smaller block size; the {@code Data} buffer is
     * then over-allocated but decoding remains correct because each block carries its own
     * metadata. Exercised by {@code testDecompressConcatenatedMembersMixedBlockSizes}.
     */
    static final class ConcatenatedDecompressStream extends AbstractBzip2DecompressStream {
        ConcatenatedDecompressStream(CBZip2InputStream decompressor) {
            super(decompressor);
        }

        @Override
        protected int readSingleByte() throws IOException {
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
    }
}
