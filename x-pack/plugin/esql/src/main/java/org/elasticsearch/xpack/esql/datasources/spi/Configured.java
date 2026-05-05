/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.Set;

/**
 * Result of a "configure with this map" operation. Carries both the configured value and the set of
 * keys the operation consumed from the input map.
 * <p>
 * Used to propagate the per-layer consumed-keys set to the coordinator so the coordinator can union
 * the consumed sets across storage and format layers and reject anything left over (typos, options
 * that belong to no layer).
 *
 * @param value         the configured object (e.g. a storage configuration, a format reader)
 * @param consumedKeys  the keys this operation consumed from the input map; never {@code null}
 */
public record Configured<T>(T value, Set<String> consumedKeys) {

    public Configured {
        consumedKeys = Set.copyOf(consumedKeys);
    }

    public static <T> Configured<T> empty(T value) {
        return new Configured<>(value, Set.of());
    }
}
