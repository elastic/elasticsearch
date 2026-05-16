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
 * Immutable typed view of resolved query setting values, produced by {@link QuerySettings#resolve}.
 * Read via {@link QuerySettingDef#get(ResolvedSettings)} on each setting's constant.
 */
public final class ResolvedSettings {

    public static final ResolvedSettings EMPTY = new ResolvedSettings(Map.of(), Set.of());

    private final Map<QuerySettingDef<?>, Object> values;
    private final Set<String> consumed;

    ResolvedSettings(Map<QuerySettingDef<?>, Object> values, Set<String> consumed) {
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
