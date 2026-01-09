/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ToXContentObject;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class DirectoryMetrics {
    private DirectoryMetrics(Map<String, PluggableMetrics<?>> data) {
        this.data = data;
    }

    public interface PluggableMetrics<T extends PluggableMetrics<T>> extends ToXContentObject {
        Supplier<T> delta();

        T snapshot();
    }

    private Map<String, PluggableMetrics<?>> data;

    public <T extends PluggableMetrics<T>> T metrics(String type) {
        Object result = data.get(type);
        // noinspection unchecked
        return (T) result;
    }

    public Supplier<DirectoryMetrics> delta() {
        Map<String, Supplier<? extends PluggableMetrics<?>>> delta = data.entrySet()
            .stream()
            .map(e -> Tuple.tuple(e.getKey(), e.getValue().delta()))
            .collect(Collectors.toUnmodifiableMap(Tuple::v1, Tuple::v2));

        return () -> new DirectoryMetrics(
            delta.entrySet()
                .stream()
                .map(e -> Tuple.tuple(e.getKey(), e.getValue().get()))
                .collect(Collectors.toUnmodifiableMap(Tuple::v1, Tuple::v2))
        );
    }

    public static class Builder {
        private final Map<String, PluggableMetrics<?>> data = new HashMap<>();

        public <T extends PluggableMetrics<T>> void add(String type, T metrics) {
            data.put(type, metrics);
        }

        public DirectoryMetrics build() {
            return new DirectoryMetrics(Map.copyOf(data));
        }
    }
}
