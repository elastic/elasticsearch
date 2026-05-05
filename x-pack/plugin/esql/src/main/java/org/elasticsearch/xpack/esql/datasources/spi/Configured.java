/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.Set;

/** Carrier for "configure with this map" operations: a value plus the keys it consumed. */
public record Configured<T>(T value, Set<String> consumedKeys) {

    public Configured {
        consumedKeys = Set.copyOf(consumedKeys);
    }

    public static <T> Configured<T> empty(T value) {
        return new Configured<>(value, Set.of());
    }
}
