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
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Main entry point for access to directory level metrics. Access using #metrics(type) to get native
 * store or plugged in metrics.
 */
public class DirectoryMetrics implements ToXContentFragment {
    private DirectoryMetrics(Map<String, PluggableMetrics<?>> data) {
        this.data = data;
    }

    public interface PluggableMetrics<T extends PluggableMetrics<T>> extends ToXContentObject {
        Supplier<T> delta();

        T snapshot();

        default <R> R cast(Class<R> clazz) {
            return clazz.cast(this);
        }
    }

    private final Map<String, PluggableMetrics<?>> data;

    public PluggableMetrics<?> metrics(String type) {
        return data.get(type);
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, PluggableMetrics<?>> entry : data.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        return builder;
    }

    public static class Builder {
        private final Map<String, PluggableMetrics<?>> data = new HashMap<>();

        public void add(String type, PluggableMetrics<?> metrics) {
            data.put(type, metrics);
        }

        public DirectoryMetrics build() {
            return new DirectoryMetrics(Map.copyOf(data));
        }
    }
}
