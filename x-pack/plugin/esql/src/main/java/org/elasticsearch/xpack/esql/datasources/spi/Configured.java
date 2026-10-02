/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
        // Stream straight into an unmodifiable set so the compact constructor's Set.copyOf is a no-op.
        Set<String> consumed = config.keySet().stream().filter(recognized::contains).collect(Collectors.toUnmodifiableSet());
        return new Configured<>(value, consumed);
    }
}
