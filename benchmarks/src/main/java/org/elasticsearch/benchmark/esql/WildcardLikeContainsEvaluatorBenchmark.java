/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.esql;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.xpack.esql.datasources.pushdown.ByteMatchers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures the inner predicate that drives {@code WildcardLikeContainsEvaluator} — the row-by-row
 * dispatch the ES|QL compute engine runs when {@code LIKE "*literal*"} cannot push down. Three
 * implementations are benchmarked head-to-head on the same corpus:
 *
 * <ul>
 *   <li><b>automaton</b> — {@code ByteRunAutomaton} from a Lucene {@code WildcardQuery} compile,
 *       the path {@code WildcardLike} used before the affix-contains fast paths landed. This is
 *       the baseline the PR is shrinking down from.</li>
 *   <li><b>scalarLoop</b> — the naive first-byte-anchor byte scan that lived in
 *       {@code WildcardLike#processContains} before its body was retargeted at the shared
 *       {@link ByteMatchers#containsLiteral} primitive. Kept inlined in the benchmark as a
 *       regression-detection witness — if the SIMD path ever regresses below this scalar number,
 *       something is wrong with the Panama Vector code path or the JIT routing.</li>
 *   <li><b>simdContains</b> — the current production path: {@code ByteMatchers.containsLiteral},
 *       which routes through {@code BinaryDocValuesContainsTermQuery#contains} into
 *       {@code ESVectorUtil#contains}'s first+last-byte vector filter (active when the value is
 *       at least 24 bytes).</li>
 * </ul>
 *
 * <p>{@code valueByteSize} crosses the 24-byte SIMD activation threshold; {@code literalByteSize}
 * sweeps short (3-byte) and long (16-byte) literals so the substring-search startup cost is
 * separately observable. The corpus is a mix of URL-shaped strings; half contain the literal so
 * both hit and miss paths are exercised. A startup self-test asserts that all three benchmarks
 * agree on every value before any timed run — a silent disagreement would render the wins
 * meaningless.
 */
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
public class WildcardLikeContainsEvaluatorBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    /** Approximate value length in bytes; the sweep straddles the 24-byte SIMD threshold. */
    @Param({ "16", "40", "120" })
    public int valueByteSize;

    /** Literal length in bytes. Short literal stresses the dispatcher's overhead; long literal
     * lets the Panama first+last-byte filter pay off. */
    @Param({ "3", "16" })
    public int literalByteSize;

    private static final int NUM_VALUES = 1024;

    private byte[][] valueArrays;
    private BytesRef[] valueRefs;
    private BytesRef literal;
    private ByteRunAutomaton automaton;
    private int index;

    @Setup
    public void setup() {
        Random random = new Random(1L);
        // Pick a literal that is guaranteed-present in half the values. The literal is plain ASCII
        // (so its length in bytes equals its length in chars).
        String literalString = randomAscii(random, literalByteSize);
        literal = new BytesRef(literalString.getBytes(StandardCharsets.UTF_8));
        valueArrays = new byte[NUM_VALUES][];
        valueRefs = new BytesRef[NUM_VALUES];
        for (int i = 0; i < NUM_VALUES; i++) {
            String value = synthesizeValue(random, valueByteSize, literalString, (i & 1) == 0);
            valueArrays[i] = value.getBytes(StandardCharsets.UTF_8);
            valueRefs[i] = new BytesRef(valueArrays[i]);
        }
        String pattern = "*" + literalString + "*";
        Automaton autom = WildcardQuery.toAutomaton(new Term("f", pattern), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        automaton = new ByteRunAutomaton(autom);
        // Self-test: every implementation must agree on every value, otherwise the throughput
        // numbers are uninterpretable.
        for (int i = 0; i < NUM_VALUES; i++) {
            byte[] bytes = valueArrays[i];
            BytesRef ref = valueRefs[i];
            boolean fromAutomaton = automaton.run(bytes, 0, bytes.length);
            boolean fromScalar = scalarContains(ref, literal);
            boolean fromSimd = ByteMatchers.containsLiteral(ref, literal);
            if (fromAutomaton != fromScalar || fromAutomaton != fromSimd) {
                throw new AssertionError(
                    "Disagreement on value ["
                        + new String(bytes, StandardCharsets.UTF_8)
                        + "] literal ["
                        + literalString
                        + "]: automaton="
                        + fromAutomaton
                        + " scalar="
                        + fromScalar
                        + " simd="
                        + fromSimd
                );
            }
        }
    }

    @Benchmark
    public boolean automaton() {
        int idx = index++ % NUM_VALUES;
        byte[] bytes = valueArrays[idx];
        return automaton.run(bytes, 0, bytes.length);
    }

    @Benchmark
    public boolean scalarLoop() {
        int idx = index++ % NUM_VALUES;
        return scalarContains(valueRefs[idx], literal);
    }

    @Benchmark
    public boolean simdContains() {
        int idx = index++ % NUM_VALUES;
        return ByteMatchers.containsLiteral(valueRefs[idx], literal);
    }

    /**
     * Inlined copy of the original {@code WildcardLike#processContains} body — a first-byte-
     * anchored naive scan. Kept here as a frozen baseline so the SIMD path's win is visible and
     * any regression below it surfaces immediately. Not shared with production code.
     */
    private static boolean scalarContains(BytesRef str, BytesRef pattern) {
        final int pl = pattern.length;
        if (pl == 0) {
            return true;
        }
        if (str.length < pl) {
            return false;
        }
        final byte[] sb = str.bytes;
        final byte[] pb = pattern.bytes;
        final int so = str.offset;
        final int po = pattern.offset;
        final int last = so + str.length - pl;
        final byte first = pb[po];
        outer: for (int i = so; i <= last; i++) {
            if (sb[i] != first) {
                continue;
            }
            for (int j = 1; j < pl; j++) {
                if (sb[i + j] != pb[po + j]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Builds a URL-shaped value of approximately {@code targetBytes} bytes that either contains or
     * excludes {@code literal}. The body around the literal is random ASCII so the substring
     * search has typical first-byte-filter hit rates rather than a degenerate "all bytes equal"
     * input.
     */
    private static String synthesizeValue(Random random, int targetBytes, String literal, boolean includeLiteral) {
        StringBuilder sb = new StringBuilder(targetBytes + 16);
        sb.append("https://www.");
        // Reserve enough space for the literal (when present) plus a tail; remaining bytes are
        // random ASCII so the first-byte filter inside the SIMD primitive sees realistic
        // selectivity.
        int jitter = Math.max(1, targetBytes / 10);
        int actualLen = targetBytes - jitter + random.nextInt(2 * jitter + 1);
        if (includeLiteral) {
            while (sb.length() + literal.length() + 4 < actualLen) {
                sb.append((char) ('a' + random.nextInt(26)));
            }
            sb.append(literal);
        }
        while (sb.length() < actualLen) {
            sb.append((char) ('a' + random.nextInt(26)));
        }
        String value = sb.toString();
        // If we were asked NOT to include the literal but the random body accidentally produced it,
        // regenerate once. The probability is negligible for literals >= 3 bytes.
        if (includeLiteral == false && value.contains(literal)) {
            return synthesizeValue(random, targetBytes, literal, false);
        }
        return value;
    }

    private static String randomAscii(Random random, int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + random.nextInt(26)));
        }
        return sb.toString();
    }
}
