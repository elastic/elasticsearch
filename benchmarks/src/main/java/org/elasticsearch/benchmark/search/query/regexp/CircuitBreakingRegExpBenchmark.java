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
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.lucene.util.automaton.CircuitBreakingRegExp;
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
 * Compares {@link CircuitBreakingRegExp#toAutomaton} with Lucene's {@link RegExp#toAutomaton()} on the same parsed pattern:
 * the time of the charged build against the plain one, and the peak it reserves on the breaker against the retained size of
 * what it builds.
 * <p>
 * The reservations are upper bounds on each step's peak live memory, so a {@code peakOverBuiltRatio} above {@code 1.0} is
 * expected; how far above shows how conservative the bounds are for each shape of pattern. The {@link Metrics} aux counters
 * are JMH {@code EVENTS}, scaled by the iteration count, so divide each by {@code Cnt} to recover absolute bytes.
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@SuppressWarnings("unused") // invoked by JMH
public class CircuitBreakingRegExpBenchmark {

    private static final int FLAGS = RegExp.ALL | RegExp.DEPRECATED_COMPLEMENT;

    /**
     * Patterns covering the node kinds the build dispatches on and the shapes its bounds model. Sizes are kept small enough
     * that both builds fit comfortably in the benchmark JVM.
     */
    public enum RegexPattern {
        LITERAL("elasticsearch"),
        CHAR_CLASS("[a-z0-9]"),
        STAR("[a-z0-9]*"),
        OPTIONAL("(elastic)?"),
        UNION("(cat|dog|bird|fish|horse|snake|lizard|turtle)"),
        REPEAT_EXACT("a{10000}"),
        REPEAT_RANGE("a{100,2000}"),
        REPEAT_MIN("a{10000,}"),
        NESTED_REPEAT("(ab){5000}"),
        CLASS_REPEAT("[a-z]{10000}"),
        UNION_REPEAT("(cat|dog|bird){3000}"),
        // pieces that accept the empty string: transitions grow with the square of the count
        NULLABLE_REPEAT("x?{1000}"),
        STACKED_QUANTIFIERS("a++++++++++"),
        // a class of separate ranges carries one transition per range on each state
        DENSE_CLASS_REPEAT("[acegikmoqsuwy]{2000}"),
        INTERSECTION("[a-z]{5,10}&.{7}"),
        INTERSECTION_WITH_COMPLEMENT("[ab]{50}&~((a|b)*b(a|b){8})"),
        MIXED("(ab|cd){2000}(ef)?[0-9]{50}");

        private final String pattern;

        RegexPattern(String pattern) {
            this.pattern = pattern;
        }
    }

    @Param
    public RegexPattern regex;

    private RegExp lucene;
    private CircuitBreakingRegExp charged;
    private long precomputedPeak;
    private long precomputedBuilt;

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class Metrics {
        public double peakReservedBytes;
        public double builtBytes;
        public double peakOverBuiltRatio;
    }

    @Setup(Level.Trial)
    public void setupTrial() {
        lucene = new RegExp(regex.pattern, FLAGS, 0);
        charged = new CircuitBreakingRegExp(regex.pattern, FLAGS, 0);
        PeakTrackingBreaker breaker = new PeakTrackingBreaker();
        precomputedBuilt = charged.toAutomaton(breaker, "benchmark").ramBytesUsed();
        precomputedPeak = breaker.peak;
    }

    @Benchmark
    public Automaton lucene(Metrics metrics) {
        publish(metrics);
        return lucene.toAutomaton();
    }

    @Benchmark
    public Automaton charged(Metrics metrics) {
        publish(metrics);
        return charged.toAutomaton(new PeakTrackingBreaker(), "benchmark");
    }

    private void publish(Metrics metrics) {
        metrics.peakReservedBytes = precomputedPeak;
        metrics.builtBytes = precomputedBuilt;
        metrics.peakOverBuiltRatio = precomputedBuilt == 0 ? 0.0 : (double) precomputedPeak / precomputedBuilt;
    }

    /** Never trips; records the most it held at once. */
    private static final class PeakTrackingBreaker extends NoopCircuitBreaker {
        private long used;
        private long peak;

        PeakTrackingBreaker() {
            super("benchmark");
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            used += bytes;
            peak = Math.max(peak, used);
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used += bytes;
        }

        @Override
        public long getUsed() {
            return used;
        }
    }
}
