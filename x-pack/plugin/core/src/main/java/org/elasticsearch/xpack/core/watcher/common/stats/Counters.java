/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.common.stats;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class to create simple usage stat counters based on longs
 * Internally this is a map mapping from String to a long, which is the counter
 * Calling toNestedMap() will create a nested map, where each dot of the key name will nest deeper
 * The main reason for this class is that the stats producer should not be worried about how the map is actually nested
 */
public class Counters implements Writeable {

    private ObjectLongHashMap<String> counters = new ObjectLongHashMap<>();

    public Counters(StreamInput in) throws IOException {
        int counters = in.readVInt();
        for (int i = 0; i < counters; i++) {
            inc(in.readString(), in.readVLong());
        }
    }

    public Counters(String ... names) {
        for (String name : names) {
            set(name);
        }
    }

    /**
     * Sets a counter. This ensures that the counter is there, even though it is never incremented.
     * @param name Name of the counter
     */
    public void set(String name) {
        counters.put(name, 0);
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
        counters.addTo(name, count);
    }

    public long get(String name) {
        return counters.get(name);
    }

    public long size() {
        return counters.size();
    }

    public boolean hasCounters() {
        return size() > 0;
    }

    /**
     * Convert the counters to a nested map, using the "." as a splitter to create deeper maps
     * @return A nested map with all the current configured counters
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> toNestedMap() {
        Map<String, Object> map = new HashMap<>();
        for (ObjectLongCursor<String> counter : counters) {
            if (counter.key.contains(".")) {
                String[] parts = counter.key.split("\\.");
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
                        curr.put(part, counter.value);
                    }
                }
            } else {
                map.put(counter.key, counter.value);
            }
        }

        return map;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(counters.size());
        for (ObjectLongCursor<String> cursor : counters) {
            out.writeString(cursor.key);
            out.writeVLong(cursor.value);
        }
    }

    public static Counters merge(List<Counters> counters) {
        Counters result = new Counters();
        for (Counters c : counters) {
            for (ObjectLongCursor<String> cursor : c.counters) {
                result.inc(cursor.key, cursor.value);
            }
        }

        return result;
    }
}
