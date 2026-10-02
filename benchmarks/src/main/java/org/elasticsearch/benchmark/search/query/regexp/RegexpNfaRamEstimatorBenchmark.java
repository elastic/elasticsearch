/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.query.regexp;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.lucene.search.cost.RegexpNfaRamEstimator;
import org.openjdk.jmh.annotations.AuxCounters;
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
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * Compares {@link RegexpNfaRamEstimator}'s cheap, AST-only estimate against the real heap of the NFA that
 * {@link RegExp#toAutomaton()} actually builds, so we can see how tightly the estimate tracks reality and
 * confirm it over-estimates (never under-estimates) across the patterns that matter.
 * <p>
 * The measured baseline is the real built {@link Automaton}'s own {@link Automaton#ramBytesUsed()} — the
 * retained size of a real object, not a model. The estimate is deliberately a slight over-estimate of the
 * <em>peak</em> build heap (which additionally includes the transient {@link Operations#repeat} allocations),
 * so a ratio comfortably above {@code 1.0} for the repetition-heavy patterns is the expected, healthy result.
 * <p>
 * The {@link RegexPattern} params are the important edge cases: literals and character classes (baseline),
 * unbounded/optional/union operators, and — most importantly — the bounded repetitions ({@code a{n}},
 * {@code a{n,}}, {@code a{n,m}}, nested and over character classes) and intersections whose multiplicative
 * blow-up is exactly what caused the incident OOM. Repetition counts are kept large enough to be
 * representative yet small enough that the real NFA builds without OOMing this benchmark JVM.
 * <p>
 * The {@link Metrics} aux counters are JMH {@code EVENTS}, scaled by the iteration count, so divide each by
 * {@code Cnt} to recover absolute bytes.
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@SuppressWarnings("unused") // invoked by JMH
public class RegexpNfaRamEstimatorBenchmark {

    /**
     * Real regex patterns exercising every branch of {@link RegexpNfaRamEstimator}. Each is a valid Lucene
     * {@link RegExp} whose NFA can be built in memory, so the measured side is a real {@link Automaton}.
     */
    public enum RegexPattern {
        LITERAL("elasticsearch"),
        CHAR_CLASS("[a-z0-9]"),
        STAR("[a-z0-9]*"),
        OPTIONAL("(elastic)?"),
        UNION("(cat|dog|bird|fish|horse|snake|lizard|turtle)"),
        // Bounded repetitions: the multiplicative blow-up that motivated the pre-build breaker charge.
        REPEAT_EXACT("a{10000}"),
        REPEAT_RANGE("a{100,10000}"),
        REPEAT_MIN("a{10000,}"),
        NESTED_REPEAT("(ab){5000}"),
        CLASS_REPEAT("[a-z]{10000}"),
        UNION_REPEAT("(cat|dog|bird){3000}"),
        // Intersection is a product construction; the estimator multiplies the two sides.
        INTERSECTION("[a-z]{5,10}&.{7}"),
        MIXED("(ab|cd){2000}(ef)?[0-9]{50}");

        private final String pattern;

        RegexPattern(String pattern) {
            this.pattern = pattern;
        }
    }

    @Param
    public RegexPattern regex;

    private RegExp parsed;
    private long precomputedEstimate;
    private long precomputedMeasured;
    private double precomputedRatio;

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class Metrics {
        public double estimatedBytes;
        public double measuredBytes;
        public double estimateOverMeasuredRatio;
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        parsed = new RegExp(regex.pattern, RegExp.ALL, 0);
        precomputedEstimate = RegexpNfaRamEstimator.estimateRamBytes(parsed);
        precomputedMeasured = parsed.toAutomaton().ramBytesUsed();
        precomputedRatio = precomputedMeasured == 0 ? 0.0 : (double) precomputedEstimate / (double) precomputedMeasured;
    }

    @Benchmark
    public long estimate(Metrics metrics) {
        publish(metrics);
        return RegexpNfaRamEstimator.estimateRamBytes(parsed);
    }

    @Benchmark
    public long build(Metrics metrics) {
        publish(metrics);
        // Build the real NFA and return its measured retained size so the timed work is the real construction.
        return parsed.toAutomaton().ramBytesUsed();
    }

    private void publish(Metrics metrics) {
        metrics.estimatedBytes = precomputedEstimate;
        metrics.measuredBytes = precomputedMeasured;
        metrics.estimateOverMeasuredRatio = precomputedRatio;
    }
}
