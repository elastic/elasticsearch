/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.esql;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.Re2Pattern;
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

import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

/**
 * Measures building the automaton of a PromQL label matcher regex, which happens once per matcher on the coordinator
 * and once per {@code Re2Like} on every shard: the RE2 automaton of {@link Re2Pattern} against the automaton Lucene's
 * {@link RegExp} builds from the same expression in its own syntax, which is what label matchers were built with
 * before.
 *
 * <p>The expressions are the shapes label matchers take: the alternations Grafana's multi-value variables expand to,
 * prefixes and suffixes, classes with counts, case-insensitive alternations and anchored literals. A self-test checks
 * that both automata accept the same language before anything is measured.
 */
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(1)
public class Re2AutomatonBenchmark {

    /** An expression in RE2 syntax and the same in Lucene's, with Lucene's flags. */
    private record Shape(String re2, String lucene, int luceneFlags) {}

    private static final Map<String, Shape> SHAPES = Map.ofEntries(
        Map.entry("alternation_3", new Shape("foo|bar|baz", "foo|bar|baz", 0)),
        Map.entry("alternation_50", new Shape(hosts(50), hosts(50), 0)),
        Map.entry("prefix", new Shape("prod-.*", "prod-.*", 0)),
        Map.entry("suffix", new Shape(".*-prod", ".*-prod", 0)),
        Map.entry("literal", new Shape("test:9999", "test:9999", 0)),
        Map.entry("anchored", new Shape("^test:9999$", "test:9999", 0)),
        Map.entry("anchored_pattern", new Shape("^(?:prod|staging)-[0-9]+$", "(prod|staging)-[0-9]+", 0)),
        Map.entry("metric", new Shape("[a-z_]+_total", "[a-z_]+_total", 0)),
        Map.entry("count", new Shape("node-[0-9]{1,3}", "node-[0-9]{1,3}", 0)),
        Map.entry("digits", new Shape("\\d+", "[0-9]+", 0)),
        Map.entry("case_insensitive", new Shape("(?i)error|warn(ing)?", "error|warn(ing)?", RegExp.ASCII_CASE_INSENSITIVE)),
        Map.entry("path", new Shape("api-(v1|v2)/(users|orders)/[0-9]+", "api-(v1|v2)/(users|orders)/[0-9]+", 0))
    );

    @Param(
        {
            "alternation_3",
            "alternation_50",
            "prefix",
            "suffix",
            "literal",
            "anchored",
            "anchored_pattern",
            "metric",
            "count",
            "digits",
            "case_insensitive",
            "path" }
    )
    public String shape;

    private static final CircuitBreaker BREAKER = new NoopCircuitBreaker("benchmark");

    private String re2;
    private String lucene;
    private int luceneFlags;

    static {
        BenchmarkLogging.configure();
        for (Map.Entry<String, Shape> entry : SHAPES.entrySet()) {
            Shape shape = entry.getValue();
            Automaton re2 = new Re2Pattern(shape.re2()).createAutomaton(false);
            Automaton lucene = lucene(shape.lucene(), shape.luceneFlags());
            if (subsetOf(re2, lucene) == false || subsetOf(lucene, re2) == false) {
                throw new AssertionError("[" + entry.getKey() + "] RE2 and Lucene automata accept different languages");
            }
        }
    }

    @Setup
    public void setup() {
        Shape s = SHAPES.get(shape);
        if (s == null) {
            throw new IllegalArgumentException("unknown shape: " + shape);
        }
        re2 = s.re2();
        lucene = s.lucene();
        luceneFlags = s.luceneFlags();
    }

    @Benchmark
    public Automaton re2() {
        return new Re2Pattern(re2).createAutomaton(false);
    }

    /** The automaton as a shard builds it for a pushed down {@code Re2Like}, accounted on a circuit breaker. */
    @Benchmark
    public Automaton re2Accounted() {
        return new Re2Pattern(re2).createAutomaton(false, BREAKER, "benchmark");
    }

    @Benchmark
    public Automaton lucene() {
        return lucene(lucene, luceneFlags);
    }

    private static Automaton lucene(String regex, int flags) {
        return Operations.determinize(new RegExp(regex, RegExp.ALL, flags).toAutomaton(), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    }

    private static boolean subsetOf(Automaton a, Automaton b) {
        Automaton notB = Operations.complement(b, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        return Operations.isEmpty(Operations.intersection(a, notB));
    }

    /** The alternation of {@code count} host names Grafana's multi-value variable expands to. */
    private static String hosts(int count) {
        StringJoiner hosts = new StringJoiner("|");
        for (int i = 0; i < count; i++) {
            hosts.add("host-" + (i % 7 == 0 ? "eu" : "us") + "-" + i);
        }
        return hosts.toString();
    }
}
