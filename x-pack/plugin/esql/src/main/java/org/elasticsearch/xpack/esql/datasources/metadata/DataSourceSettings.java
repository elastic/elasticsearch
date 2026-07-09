/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.metadata;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Immutable collection of {@link DataSourceSetting} keyed by name; pre-computes {@link #hasSecrets()}. */
public final class DataSourceSettings implements Writeable, Iterable<Map.Entry<String, DataSourceSetting>> {

    public static final DataSourceSettings EMPTY = new DataSourceSettings(Map.of());

    private final Map<String, DataSourceSetting> underlying;
    private final boolean hasSecrets;

    public DataSourceSettings(Map<String, DataSourceSetting> source) {
        // Defensive copy — unmodifiableMap only wraps, so the source map could otherwise mutate through us.
        this.underlying = Collections.unmodifiableMap(new HashMap<>(Objects.requireNonNull(source, "source")));
        boolean any = false;
        for (DataSourceSetting s : this.underlying.values()) {
            if (s.secret() && s.rawValue() != null) {
                any = true;
                break;
            }
        }
        this.hasSecrets = any;
    }

    public DataSourceSettings(StreamInput in) throws IOException {
        this(in.readMap(DataSourceSetting::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(underlying, StreamOutput::writeWriteable);
    }

    public DataSourceSetting get(String key) {
        return underlying.get(key);
    }

    public Set<String> keys() {
        return underlying.keySet();
    }

    public Collection<DataSourceSetting> values() {
        return underlying.values();
    }

    public Set<Map.Entry<String, DataSourceSetting>> entries() {
        return underlying.entrySet();
    }

    public Map<String, DataSourceSetting> asMap() {
        return underlying;
    }

    public boolean isEmpty() {
        return underlying.isEmpty();
    }

    public int size() {
        return underlying.size();
    }

    /** True iff at least one setting is a secret with a non-null value. */
    public boolean hasSecrets() {
        return hasSecrets;
    }

    /**
     * Returns a fresh map keyed identically to this collection but with secret values replaced by
     * {@link DataSourceSetting#MASK_SENTINEL}. Safe for REST responses.
     */
    public Map<String, Object> toPresentationMap() {
        Map<String, Object> result = new HashMap<>(underlying.size());
        for (var entry : underlying.entrySet()) {
            result.put(entry.getKey(), entry.getValue().presentationValue());
        }
        return Collections.unmodifiableMap(result);
    }

    @Override
    public Iterator<Map.Entry<String, DataSourceSetting>> iterator() {
        return underlying.entrySet().iterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataSourceSettings that = (DataSourceSettings) o;
        return underlying.equals(that.underlying);
    }

    @Override
    public int hashCode() {
        return underlying.hashCode();
    }
}
