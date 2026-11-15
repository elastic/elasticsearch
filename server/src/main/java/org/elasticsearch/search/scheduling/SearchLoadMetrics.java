/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.scheduling;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.index.store.DirectoryMetrics;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The search load, CPU time and waiting a search was charged on the nodes it ran on, added up over its shard results.
 */
public record SearchLoadMetrics(long loadNanos, long cpuNanos, long waitingNanos)
    implements
        DirectoryMetrics.PluggableMetrics<SearchLoadMetrics>,
        VersionedNamedWriteable {

    public static final String NAME = "search_load";
    public static final String LOAD_NANOS_KEY = "search_load_nanos";
    public static final String CPU_NANOS_KEY = "search_cpu_nanos";
    public static final String WAITING_NANOS_KEY = "search_waiting_nanos";

    private static final TransportVersion SEARCH_LOAD_REPORT = TransportVersion.fromName("search_load_report");

    public SearchLoadMetrics(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readVLong());
    }

    /**
     * The metrics of {@code load}.
     */
    public static DirectoryMetrics of(QueryLoad load) {
        DirectoryMetrics.Builder builder = new DirectoryMetrics.Builder();
        builder.add(NAME, new SearchLoadMetrics(load.nanos(), load.cpuNanos(), load.waitingNanos()));
        return builder.build();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return SEARCH_LOAD_REPORT;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(loadNanos);
        out.writeVLong(cpuNanos);
        out.writeVLong(waitingNanos);
    }

    @Override
    public Supplier<SearchLoadMetrics> delta() {
        return () -> new SearchLoadMetrics(0, 0, 0);
    }

    @Override
    public SearchLoadMetrics copy() {
        return this;
    }

    @Override
    public SearchLoadMetrics merge(SearchLoadMetrics other) {
        return new SearchLoadMetrics(loadNanos + other.loadNanos, cpuNanos + other.cpuNanos, waitingNanos + other.waitingNanos);
    }

    @Override
    public Map<String, String> entries() {
        return Map.of(
            LOAD_NANOS_KEY,
            Long.toString(loadNanos),
            CPU_NANOS_KEY,
            Long.toString(cpuNanos),
            WAITING_NANOS_KEY,
            Long.toString(waitingNanos)
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(LOAD_NANOS_KEY, loadNanos);
        builder.field(CPU_NANOS_KEY, cpuNanos);
        builder.field(WAITING_NANOS_KEY, waitingNanos);
        return builder.endObject();
    }
}
