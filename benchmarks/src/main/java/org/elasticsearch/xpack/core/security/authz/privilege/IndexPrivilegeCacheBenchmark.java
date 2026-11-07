/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.ThreadParams;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Measures the work avoided by caching resolved privileges, including automaton minimization and executable predicate construction.
 * Named privileges reuse their built-in automata and build combined automata when a selector partition needs a union. Raw actions reuse the
 * warmed pattern cache or disable it to include construction from action patterns on every privilege cache miss.
 * Inputs are generated deterministically, in sorted collections, so every fork resolves the same keys in the same order. Named keys
 * take two to five names from the sorted privilege names, either consecutive or seven apart, from each of thirty starting names.
 * Each invocation resolves all keys so that every iteration includes complete passes, with time and allocation reported per lookup.
 * The privilege cache limit is relative to the working set's measured
 * weight, so this measures the cost of repeated misses rather than establishing whether the default heap percentage is sufficient.
 * Each thread cycles through a disjoint portion of the working set. Setup checks result names, memory bounds, and
 * the expected hit/miss behavior before any timing; iteration diagnostics are collected outside the measured operation.
 * Setup also reports how many automaton bytes the cached privileges share with objects retained outside the cache, namely the
 * built-in privileges and the pattern cache. Entry weights still include those bytes, so the retained heap is lower than the weight.
 */
// Prepended because the Gradle run task passes its own -jvmArgsAppend, which replaces any jvmArgsAppend declared here.
@Fork(value = 5, jvmArgsPrepend = { "-Xms512m", "-Xmx512m" })
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class IndexPrivilegeCacheBenchmark {

    /** Four sizes, two strides and thirty starting names for the named workload; the raw action workloads use the same count. */
    private static final int KEY_COUNT = 240;
    private static final int[] NAMED_STRIDES = { 1, 7 };

    static {
        BenchmarkLogging.configure();
    }

    @Param({ "named", "raw_actions_cached_patterns", "raw_actions_uncached_patterns" })
    public String workload;

    @Param({ "100", "25" })
    public int capacityPercent;

    private List<Set<String>> keys;
    private Cache<Set<String>, IndexPrivilege.CachedPrivileges> cache;
    private long maximumWeight;
    private Cache.Stats iterationStart;

    /**
     * Measures the entire working set, then verifies that the selected capacity produces the intended cache behavior.
     */
    @Setup(Level.Trial)
    public void setup() {
        keys = createKeys();
        IndexPrivilege.updateConfiguration(
            Settings.builder().put(IndexPrivilege.CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(128)).build()
        );
        // Configure this after initializing the built-in privileges, which remain shared in every workload.
        Automatons.updateConfiguration(
            Settings.builder()
                .put("xpack.security.automata.cache.enabled", workload.equals("raw_actions_uncached_patterns") == false)
                .build()
        );
        for (Set<String> names : keys) {
            IndexPrivilege.resolveBySelectorAccess(names);
        }
        final Cache<Set<String>, IndexPrivilege.CachedPrivileges> fullCache = IndexPrivilege.getCache();
        if (fullCache.count() != KEY_COUNT || fullCache.stats().getEvictions() != 0) {
            throw new IllegalStateException("measurement cache must hold the full working set");
        }
        final long workingSetWeight = fullCache.weight();
        long largestEntryWeight = 0;
        for (IndexPrivilege.CachedPrivileges cached : fullCache.values()) {
            largestEntryWeight = Math.max(largestEntryWeight, cached.weight());
        }
        final long sharedAutomatonWeight = sharedAutomatonWeight(fullCache);
        maximumWeight = workingSetWeight * capacityPercent / 100;
        if (maximumWeight < largestEntryWeight) {
            throw new IllegalStateException("each individual entry must fit in the cache");
        }
        IndexPrivilege.updateConfiguration(
            Settings.builder().put(IndexPrivilege.CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(maximumWeight)).build()
        );
        cache = IndexPrivilege.getCache();
        for (int pass = 0; pass < 2; pass++) {
            for (Set<String> names : keys) {
                final Set<IndexPrivilege> resolved = IndexPrivilege.resolveBySelectorAccess(names);
                final Set<String> resolvedNames = resolved.stream().flatMap(p -> p.name().stream()).collect(Collectors.toSet());
                if (resolvedNames.equals(names) == false || cache.weight() > maximumWeight) {
                    throw new IllegalStateException("incorrect resolution or cache weight");
                }
            }
        }
        final Cache.Stats stats = cache.stats();
        if (capacityPercent == 100 && (stats.getHits() != KEY_COUNT || stats.getEvictions() != 0)) {
            throw new IllegalStateException("fitting cache must serve the second pass entirely from cache");
        }
        if (capacityPercent == 25 && (stats.getMisses() != 2L * KEY_COUNT || stats.getEvictions() == 0)) {
            throw new IllegalStateException("undersized cache must thrash during sequential passes");
        }
        System.out.printf(
            Locale.ROOT,
            "Working set: workload=%s keys=%d inputHash=%08x bytes=%d sharedAutomatonBytes=%d cacheBytes=%d largestEntryBytes=%d%n",
            workload,
            KEY_COUNT,
            keys.toString().hashCode(),
            workingSetWeight,
            sharedAutomatonWeight,
            maximumWeight,
            largestEntryWeight
        );
    }

    /**
     * Sums the estimated size of automata that cached privileges share with the built-in privileges or, when it is enabled, the pattern
     * cache. Both retain their automata regardless of this cache, so sharing lowers the retained heap without changing entry weights.
     */
    private long sharedAutomatonWeight(Cache<Set<String>, IndexPrivilege.CachedPrivileges> fullCache) {
        final Set<Automaton> retainedElsewhere = Collections.newSetFromMap(new IdentityHashMap<>());
        for (IndexPrivilege privilege : IndexPrivilege.values().values()) {
            retainedElsewhere.add(privilege.getAutomaton());
        }
        if (workload.equals("raw_actions_cached_patterns")) {
            for (Set<String> names : keys) {
                retainedElsewhere.add(Automatons.patterns(names.stream().map(Privilege::actionToPattern).toList()));
            }
        }
        long shared = 0;
        for (IndexPrivilege.CachedPrivileges cached : fullCache.values()) {
            for (IndexPrivilege privilege : cached.privileges()) {
                if (retainedElsewhere.contains(privilege.getAutomaton())) {
                    shared += privilege.getAutomaton().ramBytesUsed();
                }
            }
        }
        return shared;
    }

    private List<Set<String>> createKeys() {
        final Set<Set<String>> distinct = new LinkedHashSet<>();
        switch (workload) {
            case "named" -> {
                final List<String> names = new TreeSet<>(IndexPrivilege.names()).stream()
                    .filter(n -> Set.of("all", "none").contains(n) == false)
                    .toList();
                // Starting positions wrap around the names, so adding a privilege changes the keys but not their number.
                final int starts = KEY_COUNT / (4 * NAMED_STRIDES.length);
                for (int size = 2; size <= 5; size++) {
                    for (int stride : NAMED_STRIDES) {
                        for (int start = 0; start < starts; start++) {
                            // Unlike Set.copyOf, a sorted set iterates in the same order in every JVM fork.
                            final Set<String> key = new TreeSet<>();
                            for (int i = 0; i < size; i++) {
                                key.add(names.get((start + i * stride) % names.size()));
                            }
                            distinct.add(Collections.unmodifiableSet(key));
                        }
                    }
                }
            }
            case "raw_actions_cached_patterns", "raw_actions_uncached_patterns" -> {
                for (int i = 0; i < KEY_COUNT; i++) {
                    distinct.add(Set.of(String.format(Locale.ROOT, "indices:data/read/search/benchmark/%04d*", i)));
                }
            }
            default -> throw new IllegalArgumentException("unknown workload [" + workload + "]");
        }
        if (distinct.size() != KEY_COUNT) {
            throw new IllegalStateException("expected [" + KEY_COUNT + "] distinct input keys");
        }
        return List.copyOf(distinct);
    }

    /** Captures a baseline without adding statistics reads to each measured lookup. */
    @Setup(Level.Iteration)
    public void startIteration() {
        iterationStart = cache.stats();
    }

    /** Reports observed cache behavior, including any effects of concurrent access. */
    @TearDown(Level.Iteration)
    public void reportIteration() {
        final Cache.Stats stats = cache.stats();
        final long hits = stats.getHits() - iterationStart.getHits();
        final long misses = stats.getMisses() - iterationStart.getMisses();
        if ((hits + misses) % KEY_COUNT != 0) {
            throw new IllegalStateException("iterations must contain complete input passes");
        }
        System.out.printf(
            Locale.ROOT,
            "Cache iteration: hits=%d misses=%d missPercent=%.2f evictions=%d entries=%d bytes=%d limitBytes=%d%n",
            hits,
            misses,
            100.0 * misses / (hits + misses),
            stats.getEvictions() - iterationStart.getEvictions(),
            cache.count(),
            cache.weight(),
            maximumWeight
        );
    }

    /** Restores both default caches after this benchmark trial. */
    @TearDown(Level.Trial)
    public void teardown() {
        IndexPrivilege.updateConfiguration(Settings.EMPTY);
        Automatons.updateConfiguration(Settings.EMPTY);
    }

    /** Keeps input selection local to each thread without coordinating every measured lookup. */
    @State(Scope.Thread)
    public static class Cursor {
        private int position;
        private int step;

        /** Assigns each worker a disjoint, equally sized partition of the input keys. */
        @Setup(Level.Trial)
        public void setup(ThreadParams params) {
            step = params.getThreadCount();
            if (KEY_COUNT % step != 0) {
                throw new IllegalArgumentException("thread count must divide the number of keys");
            }
            position = params.getThreadIndex();
        }
    }

    /** Resolves complete input passes so faster iterations do not change the mix of cheap and expensive lookups. */
    @Benchmark
    @OperationsPerInvocation(KEY_COUNT)
    public void resolve(Cursor cursor, Blackhole blackhole) {
        for (int i = 0; i < KEY_COUNT; i++) {
            final Set<String> names = keys.get(cursor.position);
            cursor.position += cursor.step;
            if (cursor.position >= KEY_COUNT) {
                cursor.position -= KEY_COUNT;
            }
            blackhole.consume(IndexPrivilege.resolveBySelectorAccess(names));
        }
    }
}
