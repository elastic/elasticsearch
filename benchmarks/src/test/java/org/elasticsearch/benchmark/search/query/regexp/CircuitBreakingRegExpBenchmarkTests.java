/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.query.regexp;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.benchmark.search.query.regexp.CircuitBreakingRegExpBenchmark.Metrics;
import org.elasticsearch.benchmark.search.query.regexp.CircuitBreakingRegExpBenchmark.RegexPattern;
import org.elasticsearch.lucene.util.automaton.CircuitBreakingRegExp;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * Checks that {@link CircuitBreakingRegExpBenchmark} compares like with like, and that its numbers still describe the
 * {@link CircuitBreakingRegExp} build on the Lucene version in use.
 * <p>
 * The charged build mirrors {@code RegExp.toAutomaton()} operation by operation, so for every shape the two must build the
 * same automaton, not only the same language. A Lucene upgrade that changes how {@code toAutomaton} assembles a pattern
 * shows up here as a different state or transition count, before anyone reads a benchmark result from it. The reservation
 * must also still cover what the build keeps and stay within a fixed factor of it, so a Lucene change to the automaton's
 * footprint, or to what its operations trim, does not go unnoticed either.
 */
public class CircuitBreakingRegExpBenchmarkTests extends ESTestCase {

    /**
     * The benchmark's shapes reserve between 5 and 18 times what they keep on Lucene 10.5.1, and the earlier model that
     * summed the two stages of a repeat sat near 30. Past this the bounds have drifted from the build and need a look.
     */
    private static final double MAX_PEAK_OVER_BUILT = 40.0;

    /** A single leaf is built directly and handed to the caller to account, so nothing is reserved while it builds. */
    private static final Set<RegexPattern> LEAVES = EnumSet.of(RegexPattern.LITERAL, RegexPattern.CHAR_CLASS);

    private final RegexPattern regex;

    public CircuitBreakingRegExpBenchmarkTests(RegexPattern regex) {
        this.regex = regex;
    }

    @ParametersFactory(argumentFormatting = "%s")
    public static Iterable<Object[]> parameters() {
        return Arrays.stream(RegexPattern.values()).map(pattern -> new Object[] { pattern }).toList();
    }

    public void testChargedBuildsWhatLuceneBuilds() {
        CircuitBreakingRegExpBenchmark bench = newBenchmark();
        Automaton lucene = bench.lucene();
        Automaton charged = bench.charged(new Metrics());
        assertEquals("states", lucene.getNumStates(), charged.getNumStates());
        assertEquals("transitions", lucene.getNumTransitions(), charged.getNumTransitions());
        assertEquals("deterministic", lucene.isDeterministic(), charged.isDeterministic());
    }

    public void testReservationCoversTheBuildAndStaysClose() {
        CircuitBreakingRegExpBenchmark bench = newBenchmark();
        Metrics metrics = new Metrics();
        bench.charged(metrics);
        if (LEAVES.contains(regex)) {
            assertEquals("nothing is reserved for a single leaf", 0.0, metrics.peakReservedBytes, 0.0);
            return;
        }
        assertThat("reservation covers what the build keeps", metrics.peakReservedBytes, greaterThanOrEqualTo(metrics.builtBytes));
        assertThat("reservation stays within a fixed factor of the build", metrics.peakOverBuiltRatio, lessThan(MAX_PEAK_OVER_BUILT));
    }

    private CircuitBreakingRegExpBenchmark newBenchmark() {
        CircuitBreakingRegExpBenchmark bench = new CircuitBreakingRegExpBenchmark();
        bench.regex = regex;
        bench.setupTrial();
        return bench;
    }
}
