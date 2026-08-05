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
import org.elasticsearch.lucene.search.cost.FuzzyQueryCostEstimator;
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

@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@SuppressWarnings("unused") // invoked by JMH
public class FuzzyQueryCostEstimatorBenchmark {

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
        precomputedEstimate = new FuzzyQueryCostEstimator(termByteLength, distinctUtf8Bytes, maxEdits, prefixLength).estimate();
        precomputedMeasured = sumRamBytes(term, maxEdits, prefixLength, transpositions);
        precomputedRatio = precomputedMeasured == 0 ? 0.0 : (double) precomputedEstimate / (double) precomputedMeasured;
    }

    @Benchmark
    public long estimate(Metrics metrics) {
        if (term == null) {
            return 0L;
        }
        publish(metrics);
        return new FuzzyQueryCostEstimator(termByteLength, distinctUtf8Bytes, maxEdits, prefixLength).estimate();
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
