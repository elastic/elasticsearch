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
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.benchmark.Utils;
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

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * Compares two {@link Automatons} build paths for compiling a large, realistically-shaped set of Kibana
 * application-privilege actions (~5,000 pure literals, generated in-process by {@link #generateActions()} to mirror the
 * structure of the {@code kibana-.kibana} {@code all} privilege) into a Lucene {@link Automaton}, the (uncached)
 * role-resolution workload behind recurring OOM escalations on small-heap nodes. Both paths run through
 * {@link Automatons#buildPatternsAutomaton(java.util.Collection, boolean)}, which bypasses the pattern cache to isolate the
 * build algorithm. The {@code impl} parameter selects the path:
 * <ul>
 *   <li>{@code patterns-original} – the legacy general builder: per-pattern automaton, union, minimize.</li>
 *   <li>{@code patterns-literal-partition} – the strategy {@link Automatons#patterns(java.util.Collection)} now uses: compile
 *       the (dominant) literal block via {@link org.apache.lucene.util.automaton.Automata#makeStringUnion} and union it with
 *       the general build of the non-literal remainder; an all-literal set skips the trailing minimize entirely.</li>
 * </ul>
 *
 * <p>The {@code wildcards} fraction in {@code [0.0, 1.0]} rewrites that proportion of the literals into trailing-{@code *}
 * patterns ({@code some:action} becomes {@code some:action*}), which fall to the general builder: {@code 0.0} is all-literal
 * (maximum advantage) and {@code 1.0} leaves no literals (the partition path defers entirely). Run with {@code -prof gc} for
 * the transient allocation footprint.
 *
 * <p>{@link #selfTest()} runs before any iteration and asserts both paths accept the same language at every fraction: every
 * unconverted literal and an extension of every converted literal is accepted, and probes containing no literal are rejected.
 */
@Fork(1)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class AutomatonBuildBenchmark {

    private static final List<String> ALL_LITERALS = generateActions();

    static {
        Utils.configureBenchmarkLogging();
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            selfTest();
        }
    }

    @Param({ "patterns-original", "patterns-literal-partition" })
    public String impl;

    @Param({ "0.0", "0.01", "0.5", "1.0" })
    public double wildcards;

    private List<String> patterns;

    @Setup
    public void setup() {
        patterns = patterns(wildcards);
    }

    @Benchmark
    public Automaton build() {
        return buildAutomaton(impl, patterns);
    }

    /** Number of literals corresponding to the given fraction (rounded). */
    private static int countFor(double fraction) {
        return (int) Math.round(fraction * ALL_LITERALS.size());
    }

    /**
     * The full literal set with the first {@code wildcards} fraction of patterns rewritten into trailing-{@code *} wildcard
     * patterns ({@code literal*}). The remaining literals are left unchanged and the total pattern count stays fixed.
     */
    private static List<String> patterns(double wildcards) {
        final int n = ALL_LITERALS.size();
        final int wildcardCount = countFor(wildcards);
        List<String> all = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            String literal = ALL_LITERALS.get(i);
            all.add(i < wildcardCount ? literal + "*" : literal);
        }
        return all;
    }

    private static Automaton buildAutomaton(String impl, List<String> patterns) {
        return switch (impl) {
            case "patterns-original" -> Automatons.buildPatternsAutomaton(patterns, false);
            case "patterns-literal-partition" -> Automatons.buildPatternsAutomaton(patterns, true);
            default -> throw new IllegalArgumentException("unknown impl: " + impl);
        };
    }

    /**
     * Deterministically generates ~5,000 distinct, sorted, pure-literal actions that mirror the families and proportions of
     * the real kibana {@code all} privilege set.
     */
    private static List<String> generateActions() {
        final TreeSet<String> actions = new TreeSet<>();

        // alerting:<ruleType>/<consumer>/{alert|rule}/<op> — the dominant family in the real set (~3,000 actions).
        final List<String> ruleTypes = names("rule-type", 26);
        final List<String> consumers = List.of("alerts", "stackAlerts", "observability");
        final List<String> alertOps = List.of("find", "get", "getAlertSummary", "getAuthorizedAlertsIndices", "update");
        final List<String> ruleOps = names("rule-op", 33);
        for (String ruleType : ruleTypes) {
            for (String consumer : consumers) {
                for (String op : alertOps) {
                    actions.add("alerting:" + ruleType + "/" + consumer + "/alert/" + op);
                }
                for (String op : ruleOps) {
                    actions.add("alerting:" + ruleType + "/" + consumer + "/rule/" + op);
                }
            }
        }

        // saved_object:<type>/<op> — ~1,400 actions.
        final List<String> savedObjectTypes = names("so-type", 115);
        final List<String> savedObjectOps = List.of(
            "bulk_create",
            "bulk_delete",
            "bulk_get",
            "bulk_update",
            "close_point_in_time",
            "create",
            "delete",
            "find",
            "get",
            "open_point_in_time",
            "share_to_space",
            "update"
        );
        for (String type : savedObjectTypes) {
            for (String op : savedObjectOps) {
                actions.add("saved_object:" + type + "/" + op);
            }
        }

        // ui:<feature>/<capability> — ~450 actions.
        for (String feature : names("ui-feature", 37)) {
            for (String capability : names("ui-cap", 12)) {
                actions.add("ui:" + feature + "/" + capability);
            }
        }

        // Flat single-token families.
        for (String name : names("api", 143)) {
            actions.add("api:" + name);
        }
        for (String name : names("cases", 51)) {
            actions.add("cases:" + name);
        }
        for (String name : names("app", 47)) {
            actions.add("app:" + name);
        }
        actions.add("login:");
        actions.add("space:all");

        return List.copyOf(actions);
    }

    /** Generates {@code count} distinct deterministic tokens of the form {@code <prefix>-<index>}. */
    private static List<String> names(String prefix, int count) {
        final List<String> names = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            names.add(prefix + "-" + i);
        }
        return names;
    }

    static void selfTest() {
        // None of these equals a generated literal or has one as a prefix, so no "literal" or "literal*" pattern can accept
        // them at any wildcards fraction.
        final List<String> negatives = List.of("", "definitely-not-an-action", "ZZZ:no/such/action");
        for (String wildcardsParam : Utils.possibleValues(AutomatonBuildBenchmark.class, "wildcards")) {
            final List<String> patterns = patterns(Double.parseDouble(wildcardsParam));
            for (String implParam : Utils.possibleValues(AutomatonBuildBenchmark.class, "impl")) {
                final CharacterRunAutomaton run = new CharacterRunAutomaton(buildAutomaton(implParam, patterns));
                final String label = implParam + "/wildcards=" + wildcardsParam;
                // A converted literal ("literal*", detectable by the trailing '*' since literals contain none) must accept
                // "<literal>x"; an unconverted literal must accept itself.
                for (int i = 0; i < patterns.size(); i++) {
                    final String probe = patterns.get(i).endsWith("*") ? ALL_LITERALS.get(i) + "x" : ALL_LITERALS.get(i);
                    check(run.run(probe), label, "accept", probe);
                }
                for (String negative : negatives) {
                    check(run.run(negative) == false, label, "reject", negative);
                }
            }
        }
    }

    private static void check(boolean ok, String label, String verb, String probe) {
        if (ok == false) {
            throw new AssertionError("[" + label + "] should " + verb + " [" + probe + "]");
        }
    }
}
