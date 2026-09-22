/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector;

import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.simdvec.ESVectorizationProvider;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorUtilSupport;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Benchmarks {@link ESVectorUtil#indexOfAny}, comparing the default scalar implementation against
 * the Panama SIMD path -- the production API backing ESQL's {@code
 * ReplaceCaptureUntilDelimiter.hasLineTerminator} (line-terminator safety scan guarding the REPLACE
 * idiom-detection fast path; see that class for the real caller).
 * <p>
 * {@link #scan} replays the <b>real</b> per-row remainder lengths and bytes extracted from
 * ClickBench's {@code hits.Referer} sample (min 7, p50 56, mean 74, p90 100, p99 868, max 920;
 * essentially every length mod the vector width occurs) rather than a fixed-length loop, since a
 * fixed length lets the JIT/branch-predictor hide tail-handling cost in a way real, unpredictably-
 * sized rows don't get. This -- and a since-removed masked-tail variant -- is how the choice of a
 * scalar (not masked-vector) tail in {@code PanamaESVectorUtilSupport#indexOfAny} was validated: on
 * hardware without cheap masked-load support (no AVX-512 on x86, or NEON on aarch64), a masked tail
 * measured slower than the current SWAR/scalar-tail baseline, let alone the vectorized main loop.
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 8, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class LineTerminatorScanBenchmark {

    static {
        BenchmarkLogging.configure();
        VectorizationInfo.printOnce();
    }

    // ~ row count of a 256KB ES|QL page for an ~80-100 byte keyword column (ClickBench's hits.Referer).
    private static final int BLOCK_LENGTH = 2048;

    private static final byte NL = '\n';
    private static final byte CR = '\r';
    private static final byte C2 = (byte) 0xC2; // U+0085 NEL lead byte
    private static final byte E2 = (byte) 0xE2; // U+2028 / U+2029 lead byte

    // NATIVE isn't included: Native22ESVectorUtilSupport doesn't override indexOfAny, so it inherits
    // PanamaESVectorUtilSupport's implementation verbatim -- identical numbers to PANAMA here.
    @Param({ "SCALAR", "PANAMA" })
    public VectorImplementation implementation;

    /** Real remainder byte[]s (see class javadoc), tiled to {@link #BLOCK_LENGTH}, in original order. */
    private byte[][] data;
    private ESVectorUtilSupport impl;

    @Setup(Level.Trial)
    public void setup() {
        impl = switch (implementation) {
            case SCALAR -> ESVectorizationProvider.lookup(false, false).getVectorUtilSupport();
            case PANAMA -> ESVectorizationProvider.lookup(true, false).getVectorUtilSupport();
            case NATIVE, LUCENE -> throw new IllegalArgumentException(implementation + " not benchmarked here -- see class javadoc");
        };
        byte[][] remainders = loadRealRemainders();
        data = new byte[BLOCK_LENGTH][];
        for (int i = 0; i < BLOCK_LENGTH; i++) {
            data[i] = remainders[i % remainders.length];
        }
    }

    @Benchmark
    @OperationsPerInvocation(BLOCK_LENGTH)
    public int scan() {
        int hits = 0; // prevents dead-code elimination; real data has ~0 hits, so this should stay 0
        for (byte[] b : data) {
            if (impl.indexOfAny(b, 0, b.length, NL, CR, C2, E2) >= 0) {
                hits++;
            }
        }
        return hits;
    }

    /**
     * Extracts the real per-row {@code hasLineTerminator} input: everything after the first
     * {@code /} following {@code http(s)://[www.]host}, i.e. the same remainder
     * {@code ReplaceCaptureUntilDelimiter.process} scans. Rows that don't match the idiom (34/904 in
     * the sample) never reach the scan and are skipped, matching production.
     */
    private static byte[][] loadRealRemainders() {
        Pattern idiom = Pattern.compile("^https?://(?:www\\.)?[^/]+/(.*)$", Pattern.DOTALL);
        List<byte[]> remainders = new ArrayList<>();
        try (
            InputStream in = LineTerminatorScanBenchmark.class.getResourceAsStream("clickbench-referer-sample.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                Matcher m = idiom.matcher(line);
                if (m.matches()) {
                    remainders.add(m.group(1).getBytes(StandardCharsets.UTF_8));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (remainders.isEmpty()) {
            throw new IllegalStateException("no rows matched the idiom -- resource missing or regex out of sync");
        }
        return remainders.toArray(new byte[0][]);
    }
}
