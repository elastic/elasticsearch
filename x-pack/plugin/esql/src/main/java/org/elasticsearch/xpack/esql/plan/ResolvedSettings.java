/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Immutable typed view of resolved query setting values, produced by {@link QuerySettings#resolve}.
 * Read via {@link QuerySettingDef#get(ResolvedSettings)} on each setting's constant.
 *
 * <p>Travels with {@link org.elasticsearch.xpack.esql.session.Configuration} across the wire to data
 * nodes; every node and driver that holds a Configuration also holds the full resolved view.
 */
public final class ResolvedSettings implements Writeable {

    public static final ResolvedSettings EMPTY = new ResolvedSettings(Map.of());

    private final Map<QuerySettingDef<?>, Object> values;

    ResolvedSettings(Map<QuerySettingDef<?>, Object> values) {
        this.values = Map.copyOf(values);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public ResolvedSettings(StreamInput in) throws IOException {
        int n = in.readVInt();
        Map<QuerySettingDef<?>, Object> v = Maps.newHashMapWithExpectedSize(n);
        for (int i = 0; i < n; i++) {
            String name = in.readString();
            // Each value is a self-delimiting blob (see writeTo). A setting this node doesn't know — a newer peer's
            // setting during a rolling upgrade — is read past and ignored rather than failing the whole request.
            BytesReference valueBytes = in.readBytesReference();
            QuerySettingDef def = QuerySettings.lookup(name);
            if (def != null) {
                StreamInput valueIn = valueBytes.streamInput();
                valueIn.setTransportVersion(in.getTransportVersion());
                // Canonicalize on read too, so all three construction paths (resolve, withOverride, wire-read)
                // store one shape — a peer that wrote before a canonicalizer existed can't smuggle in an odd form.
                v.put(def, def.canonicalize(def.readValue(valueIn)));
            }
        }
        this.values = Map.copyOf(v);
    }

    /**
     * Writes every resolved setting as a {@code (name, length-prefixed value)} pair. Length-prefixing each value
     * makes the block <b>self-describing</b>: a reader can skip a setting it doesn't recognize (see the constructor),
     * so this format is forward-compatible.
     * <p>
     * <b>Adding a setting needs no new transport version.</b> The block as a whole is introduced once, gated in
     * {@link org.elasticsearch.xpack.esql.session.Configuration} on {@code esql_resolved_settings}; thereafter a new
     * setting is just a new entry, and older peers ignore names they don't know. (An old peer therefore falls back to
     * the setting's default for one it hasn't learned yet — fine for coordinator-resolved settings; a future setting
     * that must hard-fail on an old data node should additionally version-gate its inclusion here.)
     */
    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(values.size());
        for (Map.Entry<QuerySettingDef<?>, Object> e : values.entrySet()) {
            QuerySettingDef def = e.getKey();
            out.writeString(def.name());
            try (BytesStreamOutput valueOut = new BytesStreamOutput()) {
                valueOut.setTransportVersion(out.getTransportVersion());
                def.writeValue(valueOut, e.getValue());
                out.writeBytesReference(valueOut.bytes());
            }
        }
    }

    @SuppressWarnings("unchecked")
    <T> T get(QuerySettingDef<T> def) {
        Object v = values.get(def);
        return v != null ? (T) v : def.defaultValue();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return values.equals(((ResolvedSettings) o).values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public String toString() {
        return values.toString();
    }

    /**
     * Produce a copy with one setting's value overridden (or removed, if {@code value} is null).
     * Used by {@code Configuration.withSetting} / {@code ConfigurationBuilder.setting} and similar
     * copy-with-modification helpers.
     */
    public <T> ResolvedSettings withOverride(QuerySettingDef<T> def, @Nullable T value) {
        Map<QuerySettingDef<?>, Object> updated = new HashMap<>(values);
        if (value == null) {
            updated.remove(def);
        } else {
            // Canonicalize on write so every entry point (parse, resolve, programmatic override) stores one shape.
            updated.put(def, def.canonicalize(value));
        }
        return new ResolvedSettings(updated);
    }
}
