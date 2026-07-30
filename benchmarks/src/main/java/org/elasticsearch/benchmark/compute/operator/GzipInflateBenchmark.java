/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.compute.operator;

import org.elasticsearch.benchmark.Utils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.Inflater;

/**
 * Measures gzip-decompression throughput on the JDK paths used by the ES|QL streaming
 * datasource pipeline. Locates the point of diminishing returns for the raw read buffer
 * size handed to {@link GZIPInputStream} and quantifies the cost of the mandatory CRC32
 * validation, so that the buffer choice in {@code GzipDecompressionCodec} can be tuned
 * with measured numbers rather than guesses.
 *
 * <p>Two structural factors limit single-threaded inflate throughput:
 *
 * <ol>
 *   <li>{@link GZIPInputStream}'s default 512-byte raw read buffer triggers many small JNI
 *       trips into the JDK's bundled zlib, dwarfing arithmetic gains. The variants here
 *       sweep the constructor knob to identify where increasing the buffer stops paying.</li>
 *   <li>The ES|QL segmentator drains the decompressed stream into pool-sized buffers
 *       (256 KiB to 4 MiB). Measuring at those buffer sizes — rather than
 *       {@code read(byte[1])} or {@code readAllBytes()} — keeps the numbers comparable to
 *       the real bottleneck.</li>
 * </ol>
 *
 * <p>JMH reports the average wall time to decompress the full {@code uncompressedBytes}
 * payload in milliseconds per op. To convert to MB/s: {@code uncompressedBytes / (score_ms
 * / 1000) / (1024 * 1024)}. The score includes the {@code GZIPInputStream} construction
 * cost on every invocation, mirroring the production path that opens a fresh stream per
 * file rather than per chunk.
 *
 * <p>Variants exercised:
 * <ul>
 *   <li>{@code jdkDefault} — {@code new GZIPInputStream(raw)}; JDK's 512-byte default raw
 *       buffer. This is the historical baseline.</li>
 *   <li>{@code ctorSized} — {@code new GZIPInputStream(raw, rawBufferBytes)}; sweeps the
 *       raw-side buffer from 8 KiB to 1 MiB to find diminishing returns.</li>
 *   <li>{@code bufferedExternal} — {@code new GZIPInputStream(new BufferedInputStream(raw,
 *       rawBufferBytes))}; control variant that demonstrates an external
 *       {@link BufferedInputStream} is <em>not</em> a substitute for the constructor knob
 *       when the upstream is fast: {@code GZIPInputStream} reads its own raw buffer in one
 *       shot, so the outer buffer only helps when the underlying source is slow.</li>
 *   <li>{@code rawInflaterNoCrc} — drives {@link Inflater} directly in {@code nowrap} mode
 *       with the gzip header/trailer parsed by hand and the trailing CRC32 ignored. This
 *       is <strong>not safe for production</strong> — it produces silent corruption on bit
 *       flips — but it bounds how much of the remaining cost is mandatory integrity
 *       checking versus inflate proper.</li>
 * </ul>
 *
 * <p>The {@code drainBufferBytes} values bracket {@code NdJsonFormatReader.DEFAULT_SEGMENT_SIZE}
 * (4 MiB); the smaller values cover tuned configurations and the CSV reader's typical
 * segment shape.
 */
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Threads(1)
@Fork(1)
public class GzipInflateBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    /**
     * Uncompressed payload size. Sized to cover sub-buffer and multi-buffer regimes without
     * making the harness setup excessively slow.
     */
    @Param({ "8388608" })
    public int uncompressedBytes;

    /**
     * Drain buffer size used to pull bytes out of the decompressed stream. Mirrors the
     * pool-sized buffers used by {@code StreamingParallelParsingCoordinator} (chunk size
     * derives from {@code SegmentableFormatReader#minimumSegmentSize}, typically in the
     * 256 KiB – 4 MiB range for CSV/NDJSON). One representative value keeps the matrix
     * tractable; override with {@code -p drainBufferBytes=...} to sweep.
     */
    @Param({ "1048576" })
    public int drainBufferBytes;

    /**
     * Raw-side buffer size handed to the {@link GZIPInputStream}{@code (InputStream, int)}
     * constructor (or to {@link BufferedInputStream} for the external variant). Spans an
     * order of magnitude around the JDK default (512 B) and the segmentator's pool sizes.
     * Not consumed by {@link #jdkDefault} (which fixes the buffer at the JDK default of
     * 512 B) or by {@link #rawInflaterNoCrc} (which feeds the entire compressed payload to
     * {@link Inflater#setInput} in one call); JMH still iterates the param matrix for those
     * methods, so filter with {@code -p rawBufferBytes=...} when running them alone.
     */
    @Param({ "8192", "16384", "32768", "65536", "98304", "131072", "262144", "524288", "1048576" })
    public int rawBufferBytes;

    /**
     * Gzip compression level used to build the input payload. {@code 6} is the JDK default.
     * Inflate cost is largely level-independent, but the produced stream shape changes
     * (literal vs. back-reference ratio) which affects inflate code paths.
     */
    @Param({ "6" })
    public int compressionLevel;

    private byte[] gzipBytes;
    private byte[] drainBuf;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        byte[] plain = generateNdjsonLike(uncompressedBytes);
        gzipBytes = gzip(plain, compressionLevel);
        drainBuf = new byte[drainBufferBytes];
    }

    /**
     * {@code new GZIPInputStream(raw)} — JDK default constructor, 512-byte raw buffer.
     * Independent of {@code rawBufferBytes}; the JMH harness still iterates the param space
     * for this benchmark but the score should be flat. Filter with {@code -p
     * rawBufferBytes=8192} when running this method alone.
     */
    @Benchmark
    public void jdkDefault(Blackhole bh) throws IOException {
        ByteArrayInputStream raw = new ByteArrayInputStream(gzipBytes);
        try (GZIPInputStream in = new GZIPInputStream(raw)) {
            drain(in, bh);
        }
    }

    /** {@code new GZIPInputStream(raw, rawBufferBytes)} — JDK constructor with a tunable raw buffer. */
    @Benchmark
    public void ctorSized(Blackhole bh) throws IOException {
        ByteArrayInputStream raw = new ByteArrayInputStream(gzipBytes);
        try (GZIPInputStream in = new GZIPInputStream(raw, rawBufferBytes)) {
            drain(in, bh);
        }
    }

    /**
     * {@code BufferedInputStream(raw, rawBufferBytes)} ahead of the default-buffer
     * {@link GZIPInputStream}. Control variant: with an in-memory source, the score should
     * track {@link #jdkDefault} rather than {@link #ctorSized}, confirming the JNI-call
     * amortization is determined by {@code GZIPInputStream}'s internal buffer rather than
     * the outer one.
     */
    @Benchmark
    public void bufferedExternal(Blackhole bh) throws IOException {
        ByteArrayInputStream raw = new ByteArrayInputStream(gzipBytes);
        try (GZIPInputStream in = new GZIPInputStream(new BufferedInputStream(raw, rawBufferBytes))) {
            drain(in, bh);
        }
    }

    /**
     * Inflate driven directly via {@link Inflater} in {@code nowrap} mode, with the gzip
     * header skipped by hand and the trailing CRC32 + ISIZE ignored — <strong>no integrity
     * check is performed on the inflated bytes</strong>. The entire compressed payload
     * (minus the 10-byte header and 8-byte trailer) is fed to {@link Inflater#setInput} in a
     * single call, so this method does not consume {@link #rawBufferBytes}.
     *
     * <p>Production code <em>must not</em> do this: silent corruption from upstream bit
     * flips becomes invisible. The variant exists solely to upper-bound how much of the
     * remaining cost (versus the best {@code ctorSized} configuration) is mandatory
     * integrity validation rather than inflate proper. If the gap is small, CRC is not a
     * lever; if it is large, the only safe ways to recover it are hardware-accelerated
     * CRC32 (Java 9+ already uses CRC32 intrinsics on x86 and aarch64) or a native gzip
     * library that fuses CRC with inflate.
     */
    @Benchmark
    public void rawInflaterNoCrc(Blackhole bh) throws IOException {
        long total = inflateNoCrc(gzipBytes, drainBuf, bh);
        bh.consume(total);
    }

    private void drain(InputStream in, Blackhole bh) throws IOException {
        int n;
        long total = 0;
        while ((n = in.read(drainBuf, 0, drainBuf.length)) > 0) {
            total += n;
            bh.consume(drainBuf[0]);
        }
        bh.consume(total);
    }

    /**
     * Drive {@link Inflater#setInput} on the deflate payload between the 10-byte gzip header
     * and the 8-byte trailer (CRC32 + ISIZE), without verifying either. The trailer is
     * identified by {@link Inflater#finished()}. Fails loud if the inflater stalls without
     * finishing — silent partial inflate would skew the timings this benchmark is meant to
     * bound.
     */
    private static long inflateNoCrc(byte[] gz, byte[] out, Blackhole bh) throws IOException {
        int p = parseGzipHeader(gz);
        Inflater inflater = new Inflater(true);
        try {
            inflater.setInput(gz, p, gz.length - p - 8);
            long total = 0;
            while (inflater.finished() == false) {
                int n;
                try {
                    n = inflater.inflate(out, 0, out.length);
                } catch (java.util.zip.DataFormatException e) {
                    throw new IOException("inflate failed", e);
                }
                if (n == 0 && inflater.finished() == false) {
                    throw new IOException(
                        "inflater stalled: needsInput="
                            + inflater.needsInput()
                            + " needsDictionary="
                            + inflater.needsDictionary()
                            + " bytesRead="
                            + total
                    );
                }
                total += n;
                bh.consume(out[0]);
            }
            return total;
        } finally {
            inflater.end();
        }
    }

    /**
     * Minimal gzip header parser; returns the byte offset of the first DEFLATE byte. Only
     * supports the no-flags header shape produced by {@link GZIPOutputStream} (FLG = 0) —
     * the harness writer always emits that shape. Optional fields (FEXTRA, FNAME, FCOMMENT,
     * FHCRC) are <em>not</em> handled and trigger an explicit failure rather than silent
     * mis-parsing.
     */
    private static int parseGzipHeader(byte[] gz) throws IOException {
        if (gz.length < 18) {
            throw new IOException("gzip too short");
        }
        if ((gz[0] & 0xff) != 0x1f || (gz[1] & 0xff) != 0x8b) {
            throw new IOException("not gzip");
        }
        if ((gz[2] & 0xff) != 0x08) {
            throw new IOException("unsupported compression method");
        }
        int flg = gz[3] & 0xff;
        if (flg != 0) {
            throw new IOException("FLG=" + flg + " not supported by this microbenchmark helper");
        }
        return 10;
    }

    /**
     * Generates a deterministic, NDJSON-shaped payload with realistic entropy: repeated
     * field names (high back-reference share) and random-but-bounded numeric values
     * (literal share). Aims for a ratio in the 4–6x range typical of analytics JSON.
     */
    private static byte[] generateNdjsonLike(int targetBytes) {
        Random rnd = new Random(0xC057E81EL);
        StringBuilder sb = new StringBuilder(targetBytes + 1024);
        long row = 0;
        while (sb.length() < targetBytes) {
            sb.append("{\"id\":").append(row++);
            sb.append(",\"ts\":").append(1700000000000L + rnd.nextInt(86_400_000));
            sb.append(",\"src_ip\":\"").append(rnd.nextInt(256)).append('.').append(rnd.nextInt(256));
            sb.append('.').append(rnd.nextInt(256)).append('.').append(rnd.nextInt(256)).append('"');
            sb.append(",\"status\":").append(200 + rnd.nextInt(6) * 100);
            sb.append(",\"bytes\":").append(rnd.nextInt(50_000));
            sb.append(",\"path\":\"/api/v1/resource/").append(rnd.nextInt(10_000)).append('"');
            sb.append(",\"ua\":\"Mozilla/5.0 (compatible; bench/").append(rnd.nextInt(64)).append(")\"}\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] gzip(byte[] data, int level) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length / 4);
        try (GZIPOutputStream gz = new LeveledGZIPOutputStream(baos, level)) {
            gz.write(data);
        }
        byte[] out = baos.toByteArray();
        System.err.println(
            String.format(
                Locale.ROOT,
                "[GzipInflateBenchmark] input=%d B, gz=%d B, ratio=%.2fx, level=%d",
                data.length,
                out.length,
                (double) data.length / out.length,
                level
            )
        );
        return out;
    }

    /**
     * {@link GZIPOutputStream} subclass that exposes the inherited {@code Deflater} long
     * enough to set the compression level — the public {@link GZIPOutputStream} surface
     * omits a level argument, so this is the standard JDK workaround.
     */
    private static final class LeveledGZIPOutputStream extends GZIPOutputStream {
        LeveledGZIPOutputStream(OutputStream out, int level) throws IOException {
            super(out);
            def.setLevel(level);
        }
    }
}
