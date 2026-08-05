/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.esql.datasources.pushdown;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.xpack.esql.core.util.ByteMatchers;
import org.elasticsearch.xpack.esql.datasources.pushdown.WildcardLikeShape;
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
 * Compares the {@link ByteMatchers#affixContains} dispatch against the {@link ByteRunAutomaton}
 * baseline that previously powered every {@code WildcardLike} pattern in the Parquet pushdown
 * evaluator. The benchmark mimics the dictionary scan: a single compiled matcher is applied to a
 * pre-built array of UTF-8 byte values (a stand-in for the ordinal dictionary), once per row.
 *
 * <p>{@code shape} controls the LIKE pattern's structure (prefix-only, suffix-only, contains,
 * affix-contains, ...). {@code valueByteSize} sweeps typical URL/path sizes around the SIMD
 * activation threshold inside {@code ESVectorUtil#contains}; the corpus is generated with values
 * within ±10% of the requested size so the parameter label is faithful.
 *
 * <p>The "automaton" benchmark is the before-state and "affixContains" is the after-state. The
 * ratio between them is the win — expect 3-10x for the contains-bearing shapes once values clear
 * ~24 bytes; smaller — but still positive — for the affix-only shapes thanks to the JDK's
 * vectorized {@code Arrays#equals}. The {@code COMPLEX_AUTOMATON_FALLBACK} shape is deliberately
 * outside the affix-contains family — it pins the fact that the dispatcher correctly degenerates
 * to the automaton path with no measurable overhead (expected ratio ~1).
 *
 * <p>The corpus is mixed: half of the values terminate with {@code .com} (so suffix-shaped
 * patterns hit), and half contain the literal {@code "google"} (so contains-shaped patterns hit).
 * The two predicates are independent, so the four hit/miss combinations all show up.
 *
 * <p>A self-test runs at setup time to confirm the automaton and the affix-contains dispatch
 * agree on every value for every shape; without it, a silent regression that flipped one branch
 * of the matcher would still measure "fast" and pass review. AGENTS.md mandates this discipline.
 */
@Warmup(iterations = 4, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
public class WildcardLikeMatcherBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    /**
     * Wildcard pattern shape under test. The labels match the {@link WildcardLikeShape} taxonomy
     * one-for-one; "complex" exercises the automaton fallback (the {@code ?} wildcard prevents
     * the affix-contains dispatch).
     */
    @Param(
        {
            "PREFIX_ONLY",
            "SUFFIX_ONLY",
            "CONTAINS_ONLY",
            "PREFIX_SUFFIX",
            "PREFIX_CONTAINS",
            "CONTAINS_SUFFIX",
            "PREFIX_CONTAINS_SUFFIX",
            "COMPLEX_AUTOMATON_FALLBACK" }
    )
    public String shape;

    /** Approximate value length in bytes; sweep crosses the SIMD activation threshold (~24). */
    @Param({ "16", "40", "120" })
    public int valueByteSize;

    private static final int NUM_VALUES = 1024;

    private byte[][] values;
    private BytesRef[] valueRefs;
    private ByteRunAutomaton automaton;
    private WildcardLikeShape compiledShape;
    private int index;

    @Setup
    public void setup() {
        Random random = new Random(1L);
        values = new byte[NUM_VALUES][];
        valueRefs = new BytesRef[NUM_VALUES];
        // Build a corpus that looks like URL data: every value starts with "https://", optionally
        // contains "google", and optionally ends with ".com". The two flags are independent so the
        // four combinations all appear with ~25% frequency.
        for (int i = 0; i < NUM_VALUES; i++) {
            boolean includeGoogle = (i & 1) == 0;
            boolean endsWithCom = (i & 2) == 0;
            String value = synthesizeUrl(random, valueByteSize, includeGoogle, endsWithCom);
            values[i] = value.getBytes(StandardCharsets.UTF_8);
            valueRefs[i] = new BytesRef(values[i]);
        }
        String pattern = patternForShape(shape);
        // Compile once — same as ParquetPushedExpressions#automatonFor caches per query.
        Automaton autom = WildcardQuery.toAutomaton(new Term("f", pattern), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        automaton = new ByteRunAutomaton(autom);
        compiledShape = WildcardLikeShape.of(pattern);
        // Self-test: every value must produce identical answers across both implementations.
        // Skip when the shape is intentionally null (the automaton-fallback row).
        if (compiledShape != null) {
            for (int i = 0; i < NUM_VALUES; i++) {
                boolean fromAutomaton = automaton.run(values[i], 0, values[i].length);
                boolean fromShape = ByteMatchers.affixContains(
                    valueRefs[i],
                    compiledShape.prefix(),
                    compiledShape.literal(),
                    compiledShape.suffix()
                );
                if (fromShape != fromAutomaton) {
                    throw new AssertionError(
                        "Disagreement on value [" + new String(values[i], StandardCharsets.UTF_8) + "] for shape [" + shape + "]"
                    );
                }
            }
        }
    }

    @Benchmark
    public boolean automaton() {
        int idx = index++ % NUM_VALUES;
        byte[] value = values[idx];
        return automaton.run(value, 0, value.length);
    }

    @Benchmark
    public boolean affixContains() {
        int idx = index++ % NUM_VALUES;
        if (compiledShape == null) {
            // COMPLEX_AUTOMATON_FALLBACK row: the dispatcher in ParquetPushedExpressions delegates
            // to the automaton when the shape parser rejects the pattern. Mirror that here so this
            // benchmark column shows the fallback-parity throughput (expected: matches the
            // automaton row almost exactly, no measurable overhead from the dispatch check).
            byte[] bytes = values[idx];
            return automaton.run(bytes, 0, bytes.length);
        }
        BytesRef value = valueRefs[idx];
        return ByteMatchers.affixContains(value, compiledShape.prefix(), compiledShape.literal(), compiledShape.suffix());
    }

    /**
     * Generates a pseudo-URL within roughly ±10% of {@code targetBytes} bytes long, optionally
     * containing the literal {@code google} and optionally terminating in {@code .com}. The body
     * between the affixes is random ASCII so the SIMD substring scan has typical Bloom-filter
     * hit rates.
     */
    private static String synthesizeUrl(Random random, int targetBytes, boolean includeGoogle, boolean endsWithCom) {
        StringBuilder sb = new StringBuilder(targetBytes + 16);
        sb.append("https://www.");
        if (includeGoogle) {
            sb.append("google");
        } else {
            sb.append("bing");
        }
        sb.append('/');
        // ±10% jitter so the param label is faithful (rather than the previous ±50% spread).
        int jitter = Math.max(1, targetBytes / 10);
        int actualLen = targetBytes - jitter + random.nextInt(2 * jitter + 1);
        int suffixReserve = endsWithCom ? 4 : 0;
        while (sb.length() + suffixReserve < actualLen) {
            sb.append((char) ('a' + random.nextInt(26)));
        }
        if (endsWithCom) {
            sb.append(".com");
        }
        return sb.toString();
    }

    private static String patternForShape(String shape) {
        return switch (shape) {
            case "PREFIX_ONLY" -> "https://*";
            case "SUFFIX_ONLY" -> "*.com";
            case "CONTAINS_ONLY" -> "*google*";
            case "PREFIX_SUFFIX" -> "https://*.com";
            case "PREFIX_CONTAINS" -> "https://*google*";
            case "CONTAINS_SUFFIX" -> "*google*.com";
            case "PREFIX_CONTAINS_SUFFIX" -> "https://*google*.com";
            case "COMPLEX_AUTOMATON_FALLBACK" -> "*go?gle*"; // '?' forces the automaton path
            default -> throw new IllegalArgumentException("Unknown shape: " + shape);
        };
    }
}
