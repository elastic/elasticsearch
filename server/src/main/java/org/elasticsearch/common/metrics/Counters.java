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

/**
 * Simple usage stat counters based on longs. Internally this is a map mapping from String to a CounterMetric.
 * Calling toNestedMap() will create a nested map, where each dot of the key name will nest deeper. This class allows the
 * stats producer to not worry about how the map is actually nested.
 */
public class Counters implements Writeable {

    private final Map<String, CounterMetric> counters = new ConcurrentHashMap<>();

    public Counters(StreamInput in) throws IOException {
        int numCounters = in.readVInt();
        for (int i = 0; i < numCounters; i++) {
            inc(in.readString(), in.readVLong());
        }
    }

    public Counters(String... names) {
        for (String name : names) {
            set(name);
        }
    }

    /**
     * Sets a counter. This ensures that the counter is there, even though it is never incremented.
     * @param name Name of the counter
     */
    public void set(String name) {
        counters.put(name, new CounterMetric());
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
     * Convert the counters to a nested map, using the "." as a splitter to create deeper maps
     * @return A nested map with all the current configured counters
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> toNestedMap() {
        Map<String, Object> map = new HashMap<>();
        for (var counter : counters.entrySet()) {
            if (counter.getKey().contains(".")) {
                String[] parts = counter.getKey().split("\\.");
                Map<String, Object> curr = map;
                for (int i = 0; i < parts.length; i++) {
                    String part = parts[i];
                    boolean isLast = i == parts.length - 1;
                    if (isLast == false) {
                        if (curr.containsKey(part) == false) {
                            curr.put(part, new HashMap<String, Object>());
                            curr = (Map<String, Object>) curr.get(part);
                        } else {
                            curr = (Map<String, Object>) curr.get(part);
                        }
                    } else {
                        curr.put(part, counter.getValue());
                    }
                }
            } else {
                map.put(counter.getKey(), counter.getValue());
            }
        }
        return map;
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

    public boolean hasCounters() {
        return counters.isEmpty() == false;
    }
}
