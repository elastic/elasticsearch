/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import java.util.Map;
import java.util.Set;

/**
 * Immutable typed view of resolved query setting values. Produced by
 * {@link QuerySettings#resolve(Map, EsqlStatement, SettingsValidationContext)} after folding the precedence-ordered
 * sources over the registry's per-setting default.
 *
 * <p>Reads via {@link QuerySettingDef#get(EffectiveSettings)} on each setting's constant — the only public
 * way to read a setting's value. Returns the setting's registry default when no source supplied a value.
 *
 * <p>{@link #consumedSettingNames()} lists the SET keys that had at least one source contribute, used for
 * telemetry.
 */
public final class EffectiveSettings {

    /** Empty envelope — every setting returns its registry default. Useful in tests. */
    public static final EffectiveSettings EMPTY = new EffectiveSettings(Map.of(), Set.of());

    private final Map<QuerySettingDef<?>, Object> values;
    private final Set<String> consumed;

    EffectiveSettings(Map<QuerySettingDef<?>, Object> values, Set<String> consumed) {
        this.values = Map.copyOf(values);
        this.consumed = Set.copyOf(consumed);
    }

    @SuppressWarnings("unchecked")
    <T> T get(QuerySettingDef<T> def) {
        Object v = values.get(def);
        return v != null ? (T) v : def.defaultValue();
    }

    public Set<String> consumedSettingNames() {
        return consumed;
    }
}
