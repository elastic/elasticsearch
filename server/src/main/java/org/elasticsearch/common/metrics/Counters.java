/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Simple usage stat counters based on longs. Internally this is a map mapping from String which is the counter's label to a CounterMetric
 * which is the value. This class also provides a helper method that converts the counters to a nested map, using the "." in the label as a
 * splitter. This allows the stats producer to not worry about how the map is actually nested.

 * IMPORTANT: if the consumer of the metrics will make use of the nested map, it is the responsibility of the producer to provide labels
 * that will not have conflicts, which means that there no counter will have a label which is a substring of the label of another counter.
 * For example, the counters `foo: 1` and `foo.bar: 3` cannot co-exist in a nested map.
 */
public class Counters implements Writeable {

    private final ConcurrentMap<String, CounterMetric> counters = new ConcurrentHashMap<>();

    public Counters(StreamInput in) throws IOException {
        int numCounters = in.readVInt();
        for (int i = 0; i < numCounters; i++) {
            inc(in.readString(), in.readVLong());
        }
    }

    public Counters(String... names) {
        for (String name : names) {
            counters.put(name, new CounterMetric());
        }
    }

    /**
     * Increment the counter by one
     * @param name Name of the counter
     */
    public void inc(String name) {
        inc(name, 1);
    }

    /**
     * Increment the counter by configured number
     * @param name The name of the counter
     * @param count Incremental value
     */
    public void inc(String name, long count) {
        counters.computeIfAbsent(name, ignored -> new CounterMetric()).inc(count);
    }

    public long get(String name) {
        if (counters.containsKey(name)) {
            return counters.get(name).count();
        }
        throw new IllegalArgumentException("Counter with name " + name + " does not exist.");
    }

    public long size() {
        return counters.size();
    }

    public boolean hasCounters() {
        return counters.isEmpty() == false;
    }

    /**
     * Convert the counters to a nested map, using the "." as a splitter to create a nested map. For example, the counters `foo.bar`: 2,
     * `foo.baz`: 1, `foobar`: 5 would become:
     * {
     *     "foo": {
     *         "bar": 2,
     *         "baz": 1
     *     },
     *     "foobar": 5
     * }
     * @return A mutable nested map with all the current configured counters. The map is mutable to allow the client to further enrich it.
     * @throws IllegalStateException if there is a conflict in a path of two counters for example `foo`: 1 and `foo.bar`: 1.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> toMutableNestedMap() {
        Map<String, Object> root = new HashMap<>();
        for (var counter : counters.entrySet()) {
            Map<String, Object> currentLevel = root;
            String[] parts = counter.getKey().split("\\.");
            for (int i = 0; i < parts.length - 1; i++) {
                if (currentLevel.get(parts[i]) == null || currentLevel.get(parts[i]) instanceof Map) {
                    currentLevel = (Map<String, Object>) currentLevel.computeIfAbsent(parts[i], k -> new HashMap<String, Object>());
                } else {
                    throw new IllegalStateException(
                        "Failed to convert counter '" + counter.getKey() + "' because '" + parts[i] + "' is already a leaf."
                    );
                }
            }
            String leaf = parts[parts.length - 1];
            if (currentLevel.containsKey(leaf)) {
                throw new IllegalStateException(
                    "Failed to convert counter '" + counter.getKey() + "' because this is the path of another metric."
                );
            } else {
                currentLevel.put(leaf, counter.getValue().count());
            }
        }
        return root;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(counters.size());
        for (var entry : counters.entrySet()) {
            out.writeString(entry.getKey());
            out.writeVLong(entry.getValue().count());
        }
    }

    public static Counters merge(List<Counters> counters) {
        Counters result = new Counters();
        for (Counters c : counters) {
            for (var entry : c.counters.entrySet()) {
                result.inc(entry.getKey(), entry.getValue().count());
            }
        }

        return result;
    }
}
