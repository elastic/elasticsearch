/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.compress;

import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.Zstd;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Streaming zstd decompression {@link InputStream} backed by Elasticsearch's Panama FFI binding
 * to libzstd's {@code ZSTD_decompressStream}. Replaces {@code com.github.luben.zstd.ZstdInputStream}
 * for the ESQL {@code .csv.zst} / {@code .ndjson.zstd} streaming codec paths so the entire ESQL
 * data-source plugin family stops calling zstd-jni's JNI on every page read — the same motivation
 * that drove Lucene off zstd-jni in 8.14.
 *
 * <p><b>Faithful port of zstd-jni's {@code ZstdInputStreamNoFinalizer}.</b> The {@code read} +
 * {@code readInternal} loop structure, the {@code needRead} / {@code frameFinished} flags, the
 * non-blocking refill condition ({@code in.available() > 0 || dstPos == offset}), and the EOF /
 * truncation handling all mirror the established implementation. Behaviors we intentionally drop:
 * {@code setContinuous}, dictionary support, the configurable {@code BufferPool}, and method-level
 * {@code synchronized} — none are needed for the streaming codec use case and an
 * {@link InputStream} is not required to be thread-safe.
 *
 * <p><b>Buffer footprint.</b> One heap {@code byte[srcBuffSize]} (≈ 128 KB, sized to
 * {@code ZSTD_DStreamInSize}) for the wrapper-side compressed-input scratch, plus the
 * {@link Zstd.DStream} which holds the libzstd {@code ZSTD_DStream*} (~256 KB internal allocation),
 * two 24-byte struct holders, and — inside {@code JdkDStream} — off-heap input and output staging
 * buffers sized to {@code ZSTD_DStreamInSize} / {@code ZSTD_DStreamOutSize}. The original plan was
 * to skip the off-heap output buffer and let libzstd write straight into the caller's heap array
 * via {@code MemorySegment.ofArray} + {@code Linker.Option.critical(true)}, but Panama explicitly
 * forbids embedding a heap segment as the {@code ptr} field of an off-heap struct passed to a
 * downcall (<a href="https://bugs.openjdk.org/browse/JDK-8318645">JDK-8318645</a>), and
 * {@code ZSTD_decompressStream}'s signature forces an off-heap struct. The two extra memcpys this
 * design carries (caller heap → {@code inBuf}, {@code outBuf} → caller heap) are dominated 100×
 * by libzstd's own decompression work. G1 region pinning — the original motivation behind this
 * port — is structurally impossible because no heap memory is ever pinned across the native call.
 * See {@code JdkZstdLibrary.JdkDStream} for the staging-buffer ownership detail.
 *
 * <p><b>Threading.</b> Not thread-safe. The format readers ({@code CsvBatchIterator},
 * {@code NdJsonPageIterator}) own a single instance per stream and consume it from a single thread.
 */
public final class PanamaZstdInputStream extends FilterInputStream {

    /**
     * Cached at class load — {@code ZSTD_DStreamInSize} is constant across libzstd's lifetime
     * (libzstd 1.5.7 returns 128 KB). Caching avoids an FFI call per stream construction. Mirrors
     * the {@code static final} cache zstd-jni's {@code ZstdInputStreamNoFinalizer} maintains.
     *
     * <p>Resolved through the {@link NativeAccess#instance() global NativeAccess} singleton rather
     * than the {@code Zstd} instance passed to the constructor: the {@code Zstd} ctor-parameter
     * exists purely so {@code ZstdDecompressionCodec} (and tests) can inject the production instance
     * without making this class call a static itself, but the per-class buffer size is a JVM-wide
     * constant that must not vary between stream instances. If a future test ever needs a
     * non-singleton {@code Zstd} (currently impossible — there is exactly one libzstd bound per
     * process), this field locks {@code srcBuffSize} to the singleton's value, which is the only
     * correct value: feeding the SPI from a {@code byte[]} sized for a different {@code Zstd} would
     * either over- or under-feed {@code JdkDStream}'s internal staging buffer.
     */
    private static final int srcBuffSize = NativeAccess.instance().getZstd().dStreamInSize();

    private final Zstd.DStream dstream;
    private final byte[] src;

    private int srcPos = 0;
    private int srcSize = 0;
    private boolean needRead = true;
    private boolean frameFinished = true;
    private boolean isClosed = false;

    /**
     * Wrap {@code in}. The caller must check {@link PanamaZstd#isAvailable()} before constructing —
     * this constructor assumes the native binding is loaded.
     */
    PanamaZstdInputStream(InputStream in, Zstd zstd) {
        super(in);
        // Acquire the native DStream FIRST, then allocate the heap byte[]. If the heap allocation
        // throws (e.g. OutOfMemoryError on a fragmented heap) we must free the libzstd context we
        // just got back from ZSTD_createDStream — otherwise we leak ~256 KB of native memory per
        // failed construction.
        Zstd.DStream s = zstd.newDStream();
        try {
            this.src = new byte[srcBuffSize];
        } catch (Throwable t) {
            s.close();
            throw t;
        }
        this.dstream = s;
    }

    @Override
    public int read(byte[] dst, int offset, int len) throws IOException {
        if (offset < 0 || len < 0 || len > dst.length - offset) {
            throw new IndexOutOfBoundsException("Requested length " + len + " from offset " + offset + " in buffer of size " + dst.length);
        }
        if (len == 0) {
            return 0;
        }
        // Outer loop matches zstd-jni: readInternal may return 0 when it only refilled the input
        // buffer and produced no output; treat that as "keep going" rather than EOF.
        int result = 0;
        while (result == 0) {
            result = readInternal(dst, offset, len);
        }
        return result;
    }

    private int readInternal(byte[] dst, int offset, int len) throws IOException {
        if (isClosed) {
            throw new IOException("Stream closed");
        }
        final int dstSize = offset + len;
        int dstPos = offset;
        int lastDstPos = -1;

        while (dstPos < dstSize && lastDstPos < dstPos) {
            // Refill condition mirrors zstd-jni verbatim: only block on the upstream when either
            // (a) the upstream advertises data via available(), or (b) we have not produced any
            // output yet (first iteration of the very first read). This preserves the non-blocking
            // semantics that callers like BufferedReader rely on for correct EOF signaling.
            if (needRead && (in.available() > 0 || dstPos == offset)) {
                int read = in.read(src, 0, srcBuffSize);
                if (read < 0) {
                    srcSize = 0;
                    srcPos = 0;
                    if (frameFinished) {
                        return -1;
                    }
                    // We dropped zstd-jni's setContinuous() escape hatch: an unexpected EOF in the
                    // middle of a frame is a hard error. The codec path reads complete blob/file
                    // objects, so truncation means a corrupt source.
                    throw new IOException("Truncated zstd input");
                } else if (read == 0) {
                    // Upstream returned 0 without blocking — try the loop again rather than
                    // entering the decoder with srcSize=0 which would just spin.
                    continue;
                }
                srcSize = read;
                srcPos = 0;
                frameFinished = false;
            }

            lastDstPos = dstPos;
            long hint;
            try {
                hint = dstream.decompress(dst, dstPos, dstSize, src, srcPos, srcSize);
            } catch (IllegalArgumentException e) {
                // libzstd raises (via Zstd.DStream.decompress) with the upstream error name when it
                // hits corruption / unknown frame / checksum mismatch. The InputStream contract
                // expects an IOException here so callers like BufferedReader and Channels can
                // handle it uniformly with read/upstream-IO errors.
                throw new IOException("zstd decompression failed: " + e.getMessage(), e);
            }
            dstPos = dstream.lastDstPos();
            srcPos = dstream.lastSrcPos();

            if (hint == 0) {
                // Frame fully decoded. We only need to refill if the input buffer is also drained;
                // otherwise the next call can start a new concatenated frame from the leftover bytes.
                frameFinished = true;
                needRead = srcPos == srcSize;
                return dstPos - offset;
            } else {
                // hint > 0: more input wanted. Refill only if we still have output capacity —
                // otherwise the next read() will start fresh with the remaining decoder state.
                needRead = dstPos < dstSize;
            }
        }
        return dstPos - offset;
    }

    @Override
    public int read() throws IOException {
        byte[] oneByte = new byte[1];
        int result = 0;
        while (result == 0) {
            result = readInternal(oneByte, 0, 1);
        }
        if (result == 1) {
            return oneByte[0] & 0xff;
        }
        return -1;
    }

    @Override
    public int available() throws IOException {
        if (isClosed) {
            throw new IOException("Stream closed");
        }
        // When needRead is false we still hold leftover input bytes the decoder hasn't drained;
        // signal availability with the conservative "1" that zstd-jni returns. Otherwise defer
        // to the upstream's availability. Important for BufferedReader EOF semantics.
        return needRead ? in.available() : 1;
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public long skip(long numBytes) throws IOException {
        if (isClosed) {
            throw new IOException("Stream closed");
        }
        if (numBytes <= 0) {
            return 0;
        }
        int bufferLen = (int) Math.min(numBytes, PanamaZstdInputStream.recommendedSkipBufferSize());
        assert bufferLen > 0 && bufferLen <= PanamaZstdInputStream.recommendedSkipBufferSize()
            : "scratch buffer " + bufferLen + " out of (0, " + PanamaZstdInputStream.recommendedSkipBufferSize() + "]";
        byte[] scratch = new byte[bufferLen];
        long toSkip = numBytes;
        while (toSkip > 0) {
            int read = read(scratch, 0, (int) Math.min(bufferLen, toSkip));
            if (read < 0) {
                break;
            }
            toSkip -= read;
        }
        return numBytes - toSkip;
    }

    /**
     * Output buffer size for {@link #skip}. Resolved lazily through a holder class so the FFI
     * call only happens when {@code skip} is actually invoked — the regular read path never
     * touches it.
     */
    private static int recommendedSkipBufferSize() {
        return SkipBufferSizeHolder.VALUE;
    }

    private static final class SkipBufferSizeHolder {
        static final int VALUE = NativeAccess.instance().getZstd().dStreamOutSize();
    }

    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        isClosed = true;
        // Close the upstream regardless of whether dstream.close() throws, otherwise a misbehaving
        // libzstd would leak the upstream file/blob descriptor.
        try {
            dstream.close();
        } finally {
            in.close();
        }
    }
}
