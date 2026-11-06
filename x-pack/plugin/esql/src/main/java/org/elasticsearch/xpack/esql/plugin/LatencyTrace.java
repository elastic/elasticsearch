/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TEMPORARY investigation aid: records wall-clock offsets of labelled points along the query path relative to the
 * start of the last query. Only meaningful when queries run one at a time (as in the latency IT). Not for commit.
 */
public final class LatencyTrace {
    private static volatile long start;
    private static final ConcurrentHashMap<String, List<Long>> SAMPLES = new ConcurrentHashMap<>();

    private LatencyTrace() {}

    public static void start() {
        start = System.nanoTime();
    }

    public static void mark(String label) {
        long s = start;
        if (s == 0) {
            return;
        }
        SAMPLES.computeIfAbsent(label, k -> Collections.synchronizedList(new ArrayList<>())).add(System.nanoTime() - s);
    }

    public static void reset() {
        SAMPLES.clear();
    }

    public static Map<String, List<Long>> samples() {
        Map<String, List<Long>> copy = new TreeMap<>();
        SAMPLES.forEach((k, v) -> copy.put(k, new ArrayList<>(v)));
        return copy;
    }
}
