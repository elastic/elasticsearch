/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Carrier for "configure with this map" operations: a value plus the keys it consumed. */
public record Configured<T>(T value, Set<String> consumedKeys) {

    public Configured {
        consumedKeys = Set.copyOf(Objects.requireNonNullElse(consumedKeys, Set.of()));
    }

    public static <T> Configured<T> empty(T value) {
        return new Configured<>(value, Set.of());
    }

    /**
     * Pairs {@code value} with the subset of {@code config}'s keys that match {@code recognized}.
     * Equivalent to manually intersecting {@code config.keySet()} with {@code recognized} —
     * factored out so multiple format readers don't repeat the loop body.
     */
    public static <T> Configured<T> fromKnownSubset(T value, Map<String, Object> config, Set<String> recognized) {
        if (config == null || config.isEmpty()) {
            return Configured.empty(value);
        }
        Set<String> consumed = new HashSet<>();
        for (String key : config.keySet()) {
            if (recognized.contains(key)) {
                consumed.add(key);
            }
        }
        return new Configured<>(value, consumed);
    }
}
