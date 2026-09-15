/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.xcontent;

import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.simdjson.internal.parsers.DoubleParser;
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
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Micro-benchmark for {@link DoubleParser}, isolated from structural JSON parsing/digit
 * scanning: calls the real production {@link DoubleParser#parse} directly with a batch of
 * pre-computed (negative, digits, exponent) tuples, so only the double computation itself is
 * measured.
 *
 * <p>Parameterized by {@link #fastPathPercent}: the JIT's decision to inline
 * {@code computeDouble}'s fast path into its caller depends on how "hot" that fast-path branch
 * is judged to be at this call site (roughly, HotSpot applies its generous hot-method inline
 * budget, {@code -XX:FreqInlineSize}, only when the callee dominates the call site; otherwise it
 * falls back to the much smaller default, {@code -XX:MaxInlineSize}, which the fast-path method
 * no longer fits under). A benchmark built only from fast-path-eligible inputs (as an earlier
 * version of this file was) cannot see that: it always reports the best case. This version
 * interleaves fast-path-eligible numbers (few decimal digits, small exponent - e.g.
 * prices/percentages/metrics) with Eisel-Lemire-eligible ones (exponent magnitude past the fast
 * path's cutoff) from a single shared, shuffled array, so the *same call site* sees a
 * controllable mix and the reported cost reflects whichever inlining decision the JIT actually
 * makes for that mix - not just the monomorphic best case.
 *
 * <p>The {@code buffer} passed to {@code parse} is deliberately empty and never dereferenced for
 * either kind of input: {@code DoubleParser} only reads from it when {@code digitCount} exceeds
 * the fast-path threshold (19 significant digits), which none of the generated numbers do.
 *
 * <pre>{@code
 * ./gradlew :libs:simdjson:benchmark --args "DoubleParserBenchmark"
 * }</pre>
 */
@Fork(value = 3, jvmArgsAppend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class DoubleParserBenchmark {

    private static final int COUNT = 5000;

    // Never dereferenced: DoubleParser only reads from the buffer when digitCount > 19, which
    // none of the numbers generated in setUp() (fast-path or Eisel-Lemire-eligible) do.
    private static final byte[] UNUSED_BUFFER = new byte[0];

    // What fraction of calls at this call site are fast-path-eligible; the rest are
    // Eisel-Lemire-eligible. See the class Javadoc for why this matters for the JIT's inlining
    // decision, not just for exercising both algorithms.
    @Param({ "0", "10", "25", "50", "75", "100" })
    private int fastPathPercent;

    private final DoubleParser doubleParser = new DoubleParser();
    private boolean[] negatives;
    private long[] digits;
    private long[] exponents;
    private int[] digitCounts;

    @Setup(Level.Trial)
    public void setUp() {
        BenchmarkLogging.configure();
        Random random = new Random(42);
        negatives = new boolean[COUNT];
        digits = new long[COUNT];
        exponents = new long[COUNT];
        digitCounts = new int[COUNT];

        // Exactly fastPathPercent% of slots are fast-path-eligible, shuffled so the two kinds
        // are interleaved rather than clustered - the JIT's profiling sees them one call at a
        // time, in this shuffled order, just like a real mixed-shape document stream would.
        boolean[] isFastPath = new boolean[COUNT];
        Arrays.fill(isFastPath, 0, (COUNT * fastPathPercent) / 100, true);
        shuffle(isFastPath, random);

        for (int i = 0; i < COUNT; i++) {
            negatives[i] = random.nextBoolean();
            if (isFastPath[i]) {
                // e.g. "123456.789" -> digits=123456789, exponent=-3: 9 significant digits,
                // well within the fast path's |exponent|<=22 scope.
                long whole = random.nextInt(1_000_000);
                int frac = random.nextInt(1000);
                digits[i] = whole * 1000 + frac;
                exponents[i] = -3;
                digitCounts[i] = 9;
            } else {
                // Eisel-Lemire-eligible: e.g. "1.234567890123456e+142"/"...e-142"-shaped -
                // exponent magnitude comfortably past the fast path's cutoff
                // (POWERS_OF_TEN.length == 23), but still real (non-zero, finite) doubles.
                long significand = 1_000_000_000_000_000L + random.nextInt(900_000_000);
                int exp = 30 + random.nextInt(200); // in [30, 230), comfortably >= 23
                digits[i] = significand;
                exponents[i] = random.nextBoolean() ? exp : -exp;
                digitCounts[i] = 16;
            }
        }
    }

    private static void shuffle(boolean[] values, Random random) {
        for (int i = values.length - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            boolean tmp = values[i];
            values[i] = values[j];
            values[j] = tmp;
        }
    }

    @Benchmark
    @OperationsPerInvocation(COUNT)
    public void parse(Blackhole bh) {
        for (int i = 0; i < COUNT; i++) {
            bh.consume(doubleParser.parse(UNUSED_BUFFER, 0, negatives[i], 0, digitCounts[i], digits[i], exponents[i]));
        }
    }
}
