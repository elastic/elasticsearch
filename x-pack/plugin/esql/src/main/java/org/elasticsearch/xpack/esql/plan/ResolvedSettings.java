/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Immutable typed view of resolved query setting values, produced by {@link QuerySettings#resolve}.
 * Read via {@link QuerySettingDef#get(ResolvedSettings)} on each setting's constant.
 *
 * <p>Travels with {@link org.elasticsearch.xpack.esql.session.Configuration} across the wire to data
 * nodes; every node and driver that holds a Configuration also holds the full resolved view.
 */
public final class ResolvedSettings implements Writeable {

    public static final ResolvedSettings EMPTY = new ResolvedSettings(Map.of(), Set.of());

    private final Map<QuerySettingDef<?>, Object> values;
    private final Set<String> consumed;

    ResolvedSettings(Map<QuerySettingDef<?>, Object> values, Set<String> consumed) {
        this.values = Map.copyOf(values);
        this.consumed = Set.copyOf(consumed);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public ResolvedSettings(StreamInput in) throws IOException {
        int n = in.readVInt();
        Map<QuerySettingDef<?>, Object> v = new HashMap<>(n);
        for (int i = 0; i < n; i++) {
            String name = in.readString();
            QuerySettingDef def = QuerySettingDef.lookup(name);
            if (def == null) {
                throw new IOException("Unknown query setting on the wire: [" + name + "]");
            }
            v.put(def, def.readValue(in));
        }
        this.values = Map.copyOf(v);
        this.consumed = in.readCollectionAsImmutableSet(StreamInput::readString);
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(values.size());
        for (Map.Entry<QuerySettingDef<?>, Object> e : values.entrySet()) {
            QuerySettingDef def = e.getKey();
            out.writeString(def.name());
            def.writeValue(out, e.getValue());
        }
        out.writeCollection(consumed, StreamOutput::writeString);
    }

    @SuppressWarnings("unchecked")
    <T> T get(QuerySettingDef<T> def) {
        Object v = values.get(def);
        return v != null ? (T) v : def.defaultValue();
    }

    public Set<String> consumedSettingNames() {
        return consumed;
    }

    public Map<QuerySettingDef<?>, Object> values() {
        return values;
    }

    /**
     * Produce a copy with one setting's value overridden (or removed, if {@code value} is null).
     * Used by {@code Configuration.withZoneId} and similar copy-with-modification helpers.
     */
    public <T> ResolvedSettings withOverride(QuerySettingDef<T> def, @org.elasticsearch.core.Nullable T value) {
        Map<QuerySettingDef<?>, Object> updated = new HashMap<>(values);
        if (value == null) {
            updated.remove(def);
        } else {
            updated.put(def, value);
        }
        return new ResolvedSettings(updated, consumed);
    }
}
