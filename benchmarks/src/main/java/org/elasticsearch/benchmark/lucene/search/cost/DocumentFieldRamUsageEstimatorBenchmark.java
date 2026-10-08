/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.lucene.search.cost;

import org.apache.lucene.tests.util.RamUsageTester;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.document.DocumentFieldRamUsageEstimator;
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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * Verifies that {@link DocumentFieldRamUsageEstimator} is a safe upper bound for
 *  RamUsageTester-measured retained heap and measures its per-call cost. Each payload asserts
 * {@code estimate >= actual} in {@code @Setup} and prints an {@code [accuracy]} line with the ratio.
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class DocumentFieldRamUsageEstimatorBenchmark {

    @Param(
        {
            "small_string",
            "big_string_4m",
            "byte_array_1m",
            "array_list_100",
            "linked_list_100",
            "array_deque_100",
            "hashmap_10k",
            "hashmap_100k",
            "linked_hashmap_500",
            "treemap_500",
            "hashset_500",
            "linked_hashset_500",
            "treeset_500",
            "nested_list_of_maps_1k",
            "object_array_16",
            "deep_map_30" }
    )
    private String payload;

    private DocumentField field;

    @Setup
    public void setup() {
        field = new DocumentField(payload, buildValues(payload));

        long estimate = DocumentFieldRamUsageEstimator.estimate(field);
        long actual = RamUsageTester.ramUsed(field);
        double ratio = actual == 0L ? Double.NaN : estimate / (double) actual;

        System.out.printf(
            Locale.ROOT,
            "[accuracy] payload=%-24s estimate=%,14d B  actual=%,14d B  ratio=%.3f  delta=%+,14d B%n",
            payload,
            estimate,
            actual,
            ratio,
            estimate - actual
        );

        if (estimate < actual) {
            throw new AssertionError("estimator under-counts for payload=" + payload + ": estimate=" + estimate + " < actual=" + actual);
        }
    }

    @Benchmark
    public void estimate(Blackhole bh) {
        bh.consume(DocumentFieldRamUsageEstimator.estimate(field));
    }

    @Benchmark
    public void groundTruth(Blackhole bh) {
        bh.consume(RamUsageTester.ramUsed(field));
    }

    private static List<Object> buildValues(String shape) {
        return switch (shape) {
            case "small_string" -> List.of("hello");
            case "big_string_4m" -> List.of("x".repeat(4 * 1024 * 1024));
            case "byte_array_1m" -> List.of(new byte[1_000_000]);
            case "array_list_100" -> {
                ArrayList<Object> l = new ArrayList<>();
                for (int i = 0; i < 100; i++)
                    l.add("val-" + i);
                yield List.of(l);
            }
            case "linked_list_100" -> {
                LinkedList<Object> l = new LinkedList<>();
                for (int i = 0; i < 100; i++)
                    l.add("val-" + i);
                yield List.of(l);
            }
            case "array_deque_100" -> {
                ArrayDeque<Object> d = new ArrayDeque<>();
                for (int i = 0; i < 100; i++)
                    d.add("val-" + i);
                yield List.of(d);
            }
            case "hashmap_10k" -> {
                HashMap<String, Object> m = new HashMap<>();
                for (int i = 0; i < 10_000; i++)
                    m.put("k" + i, "v" + i);
                yield List.of(m);
            }
            case "hashmap_100k" -> {
                HashMap<String, Object> m = new HashMap<>();
                for (int i = 0; i < 100_000; i++)
                    m.put("k" + i, i);
                yield List.of(m);
            }
            case "linked_hashmap_500" -> {
                LinkedHashMap<String, Object> m = new LinkedHashMap<>();
                for (int i = 0; i < 500; i++)
                    m.put("k" + i, "v" + i);
                yield List.of(m);
            }
            case "treemap_500" -> {
                TreeMap<String, Object> m = new TreeMap<>();
                for (int i = 0; i < 500; i++)
                    m.put("k" + i, "v" + i);
                yield List.of(m);
            }
            case "hashset_500" -> {
                HashSet<Object> s = new HashSet<>();
                for (int i = 0; i < 500; i++)
                    s.add("v" + i);
                yield List.of(s);
            }
            case "linked_hashset_500" -> {
                LinkedHashSet<Object> s = new LinkedHashSet<>();
                for (int i = 0; i < 500; i++)
                    s.add("v" + i);
                yield List.of(s);
            }
            case "treeset_500" -> {
                TreeSet<Object> s = new TreeSet<>();
                for (int i = 0; i < 500; i++)
                    s.add("v" + i);
                yield List.of(s);
            }
            case "nested_list_of_maps_1k" -> {
                ArrayList<Object> outer = new ArrayList<>();
                for (int i = 0; i < 1_000; i++) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("id", i);
                    row.put("name", "row-" + i);
                    row.put("tags", List.of("a", "b", "c"));
                    outer.add(row);
                }
                yield List.of(outer);
            }
            case "object_array_16" -> List.of(new Object[] { "a", "b", 42, 3.14d, true, new byte[128], "c", "d", "e", "f" });
            case "deep_map_30" -> {
                Map<String, Object> current = new HashMap<>();
                Map<String, Object> root = current;
                for (int d = 0; d < 30; d++) {
                    Map<String, Object> next = new HashMap<>();
                    current.put("leaf", "x".repeat(64));
                    current.put("nested", next);
                    current = next;
                }
                yield List.of(root);
            }
            default -> throw new IllegalArgumentException("Unknown payload [" + shape + "]");
        };
    }
}
