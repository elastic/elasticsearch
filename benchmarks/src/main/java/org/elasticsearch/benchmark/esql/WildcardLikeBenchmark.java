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
import org.elasticsearch.xpack.esql.core.util.ByteMatchers;
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
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures the row-by-row predicate that {@code WildcardLike} dispatches for each of its three
 * affix-only shapes — {@code prefix} ({@code "foo*"}), {@code suffix} ({@code "*foo"}), and
 * {@code contains} ({@code "*foo*"}) — against the Lucene {@code ByteRunAutomaton} baseline that
 * handles all three before this PR.
 *
 * <p>The three shapes have very different scaling characteristics, which is the point of sweeping
 * them: prefix and suffix should show flat speedup-vs-value curves (constant-time evaluator
 * regardless of value length), while contains should show a curve that rises with value length
 * (linear evaluator overtaking linear automaton thanks to SIMD amortization).
 *
 * <p>Two implementations per shape:
 *
 * <ul>
 *   <li><b>automaton</b> — {@code ByteRunAutomaton} from a Lucene {@code WildcardQuery} compile.
 *       Walks the value byte-by-byte through a state machine. Cost grows linearly with
 *       {@code valueByteSize} regardless of shape.</li>
 *   <li><b>evaluator</b> — the production fast path this PR routes to. For prefix and suffix,
 *       this reduces to an {@code Arrays.equals} on the leading or trailing bytes — O({@code
 *       literalByteSize}), independent of {@code valueByteSize}. For contains, this is the SIMD
 *       substring primitive {@link ByteMatchers#containsLiteral} — O({@code valueByteSize}) with
 *       SIMD amortization above its 24-byte activation threshold.</li>
 * </ul>
 *
 * <p>Dimensions:
 * <ul>
 *   <li>{@code shape}: {@code prefix} / {@code suffix} / {@code contains} — the three affix-only
 *       patterns this PR fast-paths.</li>
 *   <li>{@code valueByteSize}: 16 = below SIMD's 24-byte activation threshold (scalar fallback
 *       inside the contains primitive); 24 = at threshold; 40 = modestly above; 85 = ClickBench
 *       {@code hits.URL} average; 120 = a few SIMD slices; 200 = asymptotic regime where setup is
 *       amortized over many slices.</li>
 *   <li>{@code literalByteSize}: 3 = typical short LIKE token ({@code "://"}, {@code ".gov"}); 6 =
 *       {@code "google"} (ClickBench Q20's literal); 16 = SIMD filter sweet spot (first+last byte
 *       pair has high precision); 32 = long phrase.</li>
 * </ul>
 *
 * <p>Corpus: per cell, 1024 URL-shaped strings with the literal placed at the position the shape
 * requires (start for prefix, end for suffix, random body position for contains) in half the
 * values, absent from the other half — so each cell measures the 50/50 hit/miss mix. A startup
 * self-test asserts automaton ≡ evaluator on every value before any timed measurement; a silent
 * disagreement would render the wins meaningless.
 */
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
public class WildcardLikeBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    /** Which affix-only shape to benchmark. Each shape routes through a different evaluator. */
    @Param({ "prefix", "suffix", "contains" })
    public String shape;

    /** Approximate value length in bytes. See class Javadoc for the rationale per point. */
    @Param({ "16", "24", "40", "85", "120", "200" })
    public int valueByteSize;

    /** Literal length in bytes. See class Javadoc for the rationale per point. */
    @Param({ "3", "6", "16", "32" })
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
        String literalString = randomAscii(random, literalByteSize);
        literal = new BytesRef(literalString.getBytes(StandardCharsets.UTF_8));
        String pattern = switch (shape) {
            case "prefix" -> literalString + "*";
            case "suffix" -> "*" + literalString;
            case "contains" -> "*" + literalString + "*";
            default -> throw new IllegalArgumentException("unknown shape: " + shape);
        };
        valueArrays = new byte[NUM_VALUES][];
        valueRefs = new BytesRef[NUM_VALUES];
        for (int i = 0; i < NUM_VALUES; i++) {
            boolean shouldMatch = (i & 1) == 0;
            String value = synthesizeValue(random, valueByteSize, literalString, shape, shouldMatch);
            valueArrays[i] = value.getBytes(StandardCharsets.UTF_8);
            valueRefs[i] = new BytesRef(valueArrays[i]);
        }
        Automaton autom = WildcardQuery.toAutomaton(new Term("f", pattern), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        automaton = new ByteRunAutomaton(autom);
        // Self-test: automaton and evaluator must agree on every value for this shape.
        for (int i = 0; i < NUM_VALUES; i++) {
            byte[] bytes = valueArrays[i];
            BytesRef ref = valueRefs[i];
            boolean fromAutomaton = automaton.run(bytes, 0, bytes.length);
            boolean fromEvaluator = evaluate(ref);
            if (fromAutomaton != fromEvaluator) {
                throw new AssertionError(
                    "Disagreement on shape ["
                        + shape
                        + "] value ["
                        + new String(bytes, StandardCharsets.UTF_8)
                        + "] literal ["
                        + literalString
                        + "]: automaton="
                        + fromAutomaton
                        + " evaluator="
                        + fromEvaluator
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
    public boolean evaluator() {
        int idx = index++ % NUM_VALUES;
        return evaluate(valueRefs[idx]);
    }

    private boolean evaluate(BytesRef value) {
        return switch (shape) {
            case "prefix" -> startsWith(value, literal);
            case "suffix" -> endsWith(value, literal);
            case "contains" -> ByteMatchers.containsLiteral(value, literal);
            default -> throw new IllegalArgumentException("unknown shape: " + shape);
        };
    }

    /** O(literal.length), independent of value.length — what {@code STARTS_WITH} reduces to. */
    private static boolean startsWith(BytesRef str, BytesRef prefix) {
        if (str.length < prefix.length) {
            return false;
        }
        return Arrays.equals(str.bytes, str.offset, str.offset + prefix.length, prefix.bytes, prefix.offset, prefix.offset + prefix.length);
    }

    /** O(suffix.length), independent of value.length — what {@code ENDS_WITH} reduces to. */
    private static boolean endsWith(BytesRef str, BytesRef suffix) {
        if (str.length < suffix.length) {
            return false;
        }
        return Arrays.equals(
            str.bytes,
            str.offset + str.length - suffix.length,
            str.offset + str.length,
            suffix.bytes,
            suffix.offset,
            suffix.offset + suffix.length
        );
    }

    /**
     * Builds a value of approximately {@code targetBytes} bytes that either contains the literal at
     * the position the shape requires (prefix → start; suffix → end; contains → random body
     * position) or excludes it entirely. The body around the literal is random lowercase ASCII so
     * the per-byte filter inside SIMD / the state-machine in the automaton sees realistic
     * selectivity.
     */
    private static String synthesizeValue(Random random, int targetBytes, String literal, String shape, boolean includeLiteral) {
        int jitter = Math.max(1, targetBytes / 10);
        int actualLen = Math.max(literal.length(), targetBytes - jitter + random.nextInt(2 * jitter + 1));
        StringBuilder sb = new StringBuilder(actualLen + literal.length());
        if (includeLiteral) {
            switch (shape) {
                case "prefix" -> {
                    sb.append(literal);
                    while (sb.length() < actualLen) {
                        sb.append((char) ('a' + random.nextInt(26)));
                    }
                }
                case "suffix" -> {
                    while (sb.length() + literal.length() < actualLen) {
                        sb.append((char) ('a' + random.nextInt(26)));
                    }
                    sb.append(literal);
                }
                case "contains" -> {
                    int pre = random.nextInt(Math.max(1, actualLen - literal.length()));
                    for (int i = 0; i < pre; i++) {
                        sb.append((char) ('a' + random.nextInt(26)));
                    }
                    sb.append(literal);
                    while (sb.length() < actualLen) {
                        sb.append((char) ('a' + random.nextInt(26)));
                    }
                }
                default -> throw new IllegalArgumentException("unknown shape: " + shape);
            }
        } else {
            while (sb.length() < actualLen) {
                sb.append((char) ('a' + random.nextInt(26)));
            }
        }
        String value = sb.toString();
        // Sanity: when we asked NOT to include the literal but the random body accidentally
        // produced it, regenerate. Probability is negligible for literals >= 3 bytes.
        if (includeLiteral == false && value.contains(literal)) {
            return synthesizeValue(random, targetBytes, literal, shape, false);
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
