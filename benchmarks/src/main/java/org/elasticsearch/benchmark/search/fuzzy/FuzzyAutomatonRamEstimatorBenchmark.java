/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.fuzzy;

import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.elasticsearch.lucene.search.FuzzyAutomatonRamEstimator;
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

import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Compares the closed-form RAM estimate produced by {@link FuzzyAutomatonRamEstimator#estimate}
 * against the actual {@link CompiledAutomaton#ramBytesUsed()} of the automata Lucene's
 * {@link FuzzyQuery#getFuzzyAutomaton} would build, both in <em>cost</em> (time per call) and in
 * <em>value</em> (bytes returned) across a representative grid of fuzzy-query parameters.
 *
 * <p>The two {@code @Benchmark} methods are intentionally apples-to-apples on the work performed
 * at search time:
 * <ul>
 *   <li>{@link #estimate} runs the closed-form formula only — what the circuit breaker would
 *       charge before any automaton is built.</li>
 *   <li>{@link #measureBuild} actually builds the {@code maxEdits + 1} compiled automata and sums
 *       their {@code ramBytesUsed()} — what the breaker is trying to bound.</li>
 * </ul>
 *
 * <p>The {@link Metrics} aux counters report the byte values themselves alongside the timing
 * numbers so a single JMH run shows, per parameter combination, the estimate, the measurement,
 * and their ratio. The estimate must always be a ceiling on the measurement; the ratio
 * quantifies how loose that ceiling is for each input shape.
 *
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@SuppressWarnings("unused") // invoked by JMH
public class FuzzyAutomatonRamEstimatorBenchmark {

    public enum Alphabet {
        SINGLE_CHAR {
            @Override
            String generate(int n, Random r) {
                return "a".repeat(n);
            }
        },
        ASCII_LETTERS {
            @Override
            String generate(int n, Random r) {
                StringBuilder sb = new StringBuilder(n);
                for (int i = 0; i < n; i++) {
                    sb.append((char) ('a' + r.nextInt(26)));
                }
                return sb.toString();
            }
        },
        UNICODE_BMP {
            @Override
            String generate(int n, Random r) {
                StringBuilder sb = new StringBuilder(n);
                for (int i = 0; i < n; i++) {
                    int cp;
                    do {
                        cp = r.nextInt(0xD800);
                    } while (Character.isISOControl(cp) || Character.isWhitespace(cp));
                    sb.appendCodePoint(cp);
                }
                return sb.toString();
            }
        };

        abstract String generate(int n, Random r);
    }

    @Param({ "5", "20", "50", "200", "1024" })
    public int termLength;

    @Param({ "1", "2" })
    public int maxEdits;

    @Param({ "0", "3" })
    public int prefixLength;

    @Param({ "true", "false" })
    public boolean transpositions;

    @Param({ "SINGLE_CHAR", "ASCII_LETTERS", "UNICODE_BMP" })
    public Alphabet alphabet;

    private String term;
    private int termByteLength;
    private int distinctUtf8Bytes;
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
        if (prefixLength > termLength) {
            term = null;
            return;
        }
        Random rnd = new Random(0xC0FFEEL ^ termLength ^ alphabet.ordinal());
        term = alphabet.generate(termLength, rnd);
        byte[] utf8 = term.getBytes(StandardCharsets.UTF_8);
        termByteLength = utf8.length;
        distinctUtf8Bytes = countDistinctUtf8Bytes(utf8);

        precomputedEstimate = FuzzyAutomatonRamEstimator.estimate(termLength, termByteLength, distinctUtf8Bytes, maxEdits, prefixLength);
        precomputedMeasured = sumRamBytes(term, maxEdits, prefixLength, transpositions);
        precomputedRatio = precomputedMeasured == 0 ? 0.0 : (double) precomputedEstimate / (double) precomputedMeasured;
    }

    @Benchmark
    public long estimate(Metrics metrics) {
        if (term == null) {
            return 0L;
        }
        publish(metrics);
        return FuzzyAutomatonRamEstimator.estimate(termLength, termByteLength, distinctUtf8Bytes, maxEdits, prefixLength);
    }

    @Benchmark
    public long measureBuild(Metrics metrics) {
        if (term == null) {
            return 0L;
        }
        publish(metrics);
        return sumRamBytes(term, maxEdits, prefixLength, transpositions);
    }

    private void publish(Metrics metrics) {
        metrics.estimatedBytes = precomputedEstimate;
        metrics.measuredBytes = precomputedMeasured;
        metrics.estimateOverMeasuredRatio = precomputedRatio;
    }

    private static long sumRamBytes(String term, int maxEdits, int prefixLength, boolean transpositions) {
        long sum = 0L;
        for (int e = 0; e <= maxEdits; e++) {
            CompiledAutomaton ca = FuzzyQuery.getFuzzyAutomaton(term, e, prefixLength, transpositions);
            sum += ca.ramBytesUsed();
        }
        return sum;
    }

    private static int countDistinctUtf8Bytes(byte[] utf8) {
        BitSet seen = new BitSet(256);
        for (byte b : utf8) {
            seen.set(b & 0xff);
        }
        return seen.cardinality();
    }
}
