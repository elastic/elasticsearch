/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * A reusable class to encode {@code field -&gt; memory size} mappings
 */
public final class FieldMemoryStats implements Writeable, Iterable<Map.Entry<String, Long>> {

    private final Map<String, Long> stats;

    /**
     * Creates a new FieldMemoryStats instance
     */
    public FieldMemoryStats(Map<String, Long> stats) {
        this.stats = Objects.requireNonNull(stats, "status must be non-null");
        assert stats.containsKey(null) == false;
    }

    /**
     * Creates a new FieldMemoryStats instance from a stream
     */
    public FieldMemoryStats(StreamInput input) throws IOException {
        stats = input.readMap(StreamInput::readString, StreamInput::readVLong);
    }

    /**
     * Adds / merges the given field memory stats into this stats instance
     */
    public void add(FieldMemoryStats fieldMemoryStats) {
        for (var entry : fieldMemoryStats.stats.entrySet()) {
            stats.merge(entry.getKey(), entry.getValue(), Long::sum);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(stats, StreamOutput::writeString, StreamOutput::writeVLong);
    }

    /**
     * Generates x-content into the given builder for each of the fields in this stats instance
     * @param builder the builder to generated on
     * @param key the top level key for this stats object
     * @param rawKey the raw byte key for each of the fields byte sizes
     * @param readableKey the readable key for each of the fields byte sizes
     */
    public void toXContent(XContentBuilder builder, String key, String rawKey, String readableKey) throws IOException {
        builder.startObject(key);
        for (var entry : stats.entrySet()) {
            builder.startObject(entry.getKey());
            builder.humanReadableField(rawKey, readableKey, new ByteSizeValue(entry.getValue()));
            builder.endObject();
        }
        builder.endObject();
    }

    /**
     * Creates a deep copy of this stats instance
     */
    public FieldMemoryStats copy() {
        // String keys and boxed Longs are both immutable, so only the map needs to be recreated in order to clone
        return new FieldMemoryStats(new HashMap<>(stats));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldMemoryStats that = (FieldMemoryStats) o;
        return Objects.equals(stats, that.stats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stats);
    }

    @Override
    public Iterator<Map.Entry<String, Long>> iterator() {
        return stats.entrySet().iterator();
    }

    /**
     * Returns the fields value in bytes or <code>0</code> if it's not present in the stats
     */
    public long get(String field) {
        return stats.getOrDefault(field, 0L);
    }

    /**
     * Returns <code>true</code> iff the given field is in the stats
     */
    public boolean containsField(String field) {
        return stats.containsKey(field);
    }
}
