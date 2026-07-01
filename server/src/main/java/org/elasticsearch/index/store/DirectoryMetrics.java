/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Main entry point for access to directory level metrics. Access using #metrics(type) to get native
 * store or plugged in metrics.
 */
public class DirectoryMetrics implements ToXContentFragment, Writeable {
    public static final DirectoryMetrics EMPTY = new DirectoryMetrics(Map.of());

    public interface PluggableMetrics<T extends PluggableMetrics<T>> extends ToXContentObject, NamedWriteable {
        /**
         * A delta object that measures the delta of the metric between invoking the delta method and
         * the returned supplier.
         * @return supplier of the delta.
         */
        Supplier<T> delta();

        /**
         * @return copy of the metric object
         */
        T copy();

        /**
         * Merge another metric of the same type into this one (e.g. summing counters).
         * @param other the metric to merge
         * @return a new merged metric
         */
        T merge(T other);

        /**
         * Returns key/value pairs that this metric wants to surface externally, e.g. as
         * HTTP response headers or telemetry attributes. Metrics that do not contribute
         * entries can rely on the default empty-map implementation.
         */
        default Map<String, String> entries() {
            return Map.of();
        }

        /**
         * Convenience method to cast to the class specified.
         * @param clazz class to cast to.
         * @return object as clazz
         */
        default <R> R cast(Class<R> clazz) {
            return clazz.cast(this);
        }
    }

    private final Map<String, PluggableMetrics<?>> data;

    private DirectoryMetrics(Map<String, PluggableMetrics<?>> data) {
        this.data = data;
    }

    public DirectoryMetrics(StreamInput in) throws IOException {
        int size = in.readVInt();
        Map<String, PluggableMetrics<?>> map = Maps.newHashMapWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
            PluggableMetrics<?> metric = in.readNamedWriteable(PluggableMetrics.class);
            map.put(metric.getWriteableName(), metric);
        }
        this.data = map;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(data.size());
        for (PluggableMetrics<?> metric : data.values()) {
            out.writeNamedWriteable(metric);
        }
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }

    public PluggableMetrics<?> metrics(String type) {
        return data.get(type);
    }

    /**
     * Collects entries from all registered pluggable metrics into a single map,
     * suitable for use as HTTP response headers, telemetry attributes, or similar key/value sinks.
     */
    public Map<String, String> entries() {
        Map<String, String> entries = Maps.newHashMapWithExpectedSize(data.size());
        for (PluggableMetrics<?> metric : data.values()) {
            entries.putAll(metric.entries());
        }
        return Collections.unmodifiableMap(entries);
    }

    public static void accumulate(AtomicReference<DirectoryMetrics> ref, DirectoryMetrics incoming) {
        if (incoming == null || incoming.isEmpty()) {
            return;
        }
        ref.accumulateAndGet(incoming, (current, in) -> current.isEmpty() ? in : current.merge(in));
    }

    /**
     * Merge another {@link DirectoryMetrics} into this one, producing a new instance with merged values
     * for each metric type present in either operand.
     */
    @SuppressWarnings("unchecked")
    public DirectoryMetrics merge(DirectoryMetrics other) {
        Map<String, PluggableMetrics<?>> merged = new HashMap<>(data);
        for (Map.Entry<String, PluggableMetrics<?>> entry : other.data.entrySet()) {
            merged.merge(entry.getKey(), entry.getValue(), (a, b) -> ((PluggableMetrics) a).merge(b));
        }
        return new DirectoryMetrics(Map.copyOf(merged));
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
