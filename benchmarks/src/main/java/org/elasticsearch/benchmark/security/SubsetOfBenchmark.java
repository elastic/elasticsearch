/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.security;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.xpack.core.security.support.Automatons;
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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for {@link Automatons#subsetOf} on realistic security wildcard
 * patterns — the kind used in role definitions and checked by the
 * {@code _has_privileges} API.
 *
 * <p>Parameters:
 * <ul>
 *   <li><b>wildcardMode</b> — pattern shape: SUFFIX, PREFIX, INFIX, or SHARED_PREFIX</li>
 *   <li><b>numRolePatterns</b> — number of patterns in the role automaton</li>
 *   <li><b>isSubset</b> — whether the candidate is a subset ({@code true}) or disjoint ({@code false})</li>
 * </ul>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class SubsetOfBenchmark {

    public enum WildcardMode {
        SUFFIX {
            @Override
            String apply(String base) {
                return base + "*";
            }

            @Override
            String narrow(String pattern) {
                return pattern.substring(0, pattern.length() - 1) + "-narrow*";
            }

            @Override
            String disjoint(String base) {
                return "disjoint-" + base + "*";
            }
        },
        PREFIX {
            @Override
            String apply(String base) {
                return "*" + base;
            }

            @Override
            String narrow(String pattern) {
                return "*narrow-" + pattern.substring(1);
            }

            @Override
            String disjoint(String base) {
                return "*disjoint-" + base;
            }
        },
        INFIX {
            @Override
            String apply(String base) {
                return "*" + base + "*";
            }

            @Override
            String narrow(String pattern) {
                return pattern.substring(0, pattern.length() - 1) + "-narrow*";
            }

            @Override
            String disjoint(String base) {
                return "*disjoint-" + base + "*";
            }
        },
        SHARED_PREFIX {
            @Override
            String apply(String base) {
                return "shared-" + base + "*";
            }

            @Override
            String narrow(String pattern) {
                return pattern.substring(0, pattern.length() - 1) + "-narrow*";
            }

            @Override
            String disjoint(String base) {
                return "other-" + base + "*";
            }
        };

        abstract String apply(String base);

        abstract String narrow(String pattern);

        abstract String disjoint(String base);
    }

    private static final int CANDIDATE_PATTERN_COUNT = 4;
    private static final int PATTERN_LENGTH = 32;

    @Param
    private WildcardMode wildcardMode;

    @Param({ "10", "250" })
    private int numRolePatterns;

    @Param({ "true", "false" })
    private boolean isSubset;

    private Automaton roleAutomaton;
    private Automaton candidateAutomaton;

    @Setup
    public void setup() {
        Random random = new Random(42);

        List<String> rolePatterns = new ArrayList<>(numRolePatterns);
        for (int i = 0; i < numRolePatterns; i++) {
            String base = "role-" + i + "-" + randomString(random, PATTERN_LENGTH);
            rolePatterns.add(wildcardMode.apply(base));
        }
        roleAutomaton = Automatons.patterns(rolePatterns);

        List<String> candidatePatterns = new ArrayList<>(CANDIDATE_PATTERN_COUNT);
        if (isSubset) {
            for (int i = 0; i < CANDIDATE_PATTERN_COUNT; i++) {
                candidatePatterns.add(wildcardMode.narrow(rolePatterns.get(i)));
            }
        } else {
            for (int i = 0; i < CANDIDATE_PATTERN_COUNT; i++) {
                candidatePatterns.add(wildcardMode.disjoint(randomString(random, PATTERN_LENGTH)));
            }
        }
        candidateAutomaton = Automatons.patterns(candidatePatterns);
    }

    @Benchmark
    public void subsetOf(final Blackhole bh) {
        bh.consume(Automatons.subsetOf(candidateAutomaton, roleAutomaton));
    }

    private static String randomString(Random random, int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + random.nextInt(26)));
        }
        return sb.toString();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder().include(SubsetOfBenchmark.class.getSimpleName()).build();
        new Runner(opt).run();
    }
}
