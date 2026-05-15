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
 * Typed, immutable view of resolved query setting values. Produced by
 * {@link QuerySettings#resolve(Map, EsqlStatement, SettingsValidationContext)} after folding the precedence-ordered
 * sources (request parameter, query SET) over the registry's per-setting default.
 *
 * <p>Reads via {@link #get(QuerySettings.QuerySettingDef)}: the return type is the setting's declared {@code T}.
 * Returns the registry default when no source supplied a value, otherwise the resolved value.
 *
 * <p>{@link #consumedSettingNames()} lists the SET key names that had at least one source contribute a value, used
 * for telemetry. A setting whose only contribution was the registry default is not "consumed".
 */
public final class EffectiveSettings {

    private final Map<QuerySettings.QuerySettingDef<?>, Object> values;
    private final Set<String> consumed;

    EffectiveSettings(Map<QuerySettings.QuerySettingDef<?>, Object> values, Set<String> consumed) {
        this.values = Map.copyOf(values);
        this.consumed = Set.copyOf(consumed);
    }

    /**
     * Returns the resolved value for {@code def}. May be {@code null} if the registry default is {@code null} and no
     * source supplied a value.
     */
    @SuppressWarnings("unchecked")
    public <T> T get(QuerySettings.QuerySettingDef<T> def) {
        return (T) values.get(def);
    }

    /**
     * SET key names where at least one source (request parameter or query SET) contributed a value. Use for telemetry
     * counting consumed settings.
     */
    public Set<String> consumedSettingNames() {
        return consumed;
    }
}
