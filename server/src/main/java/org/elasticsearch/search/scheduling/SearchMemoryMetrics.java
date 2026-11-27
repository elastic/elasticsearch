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
 * The most memory a search held at once on one of the nodes it ran on. Each result carries the peak of the query on its
 * node so far, and results merge by max, so the search gets the highest peak of its nodes.
 */
public record SearchMemoryMetrics(long peakBytes)
    implements
        DirectoryMetrics.PluggableMetrics<SearchMemoryMetrics>,
        VersionedNamedWriteable {

    public static final SearchMemoryMetrics EMPTY = new SearchMemoryMetrics(0);

    public static final String NAME = "search_memory";
    public static final String PEAK_BYTES_KEY = "search_peak_memory_bytes";

    private static final TransportVersion SEARCH_PEAK_MEMORY = TransportVersion.fromName("search_peak_memory");

    public SearchMemoryMetrics(StreamInput in) throws IOException {
        this(in.readLong());
    }

    /**
     * The metrics of the query that {@code part} belongs to, on this node.
     */
    public static DirectoryMetrics directoryMetricsOf(QueryScheduler.QueryPart part) {
        DirectoryMetrics.Builder builder = new DirectoryMetrics.Builder();
        builder.add(NAME, new SearchMemoryMetrics(part.peakMemoryBytes()));
        return builder.build();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return SEARCH_PEAK_MEMORY;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // fixed width, so the size of a shard result does not depend on the query's memory
        out.writeLong(peakBytes);
    }

    @Override
    public Supplier<SearchMemoryMetrics> delta() {
        return () -> EMPTY;
    }

    @Override
    public SearchMemoryMetrics copy() {
        return new SearchMemoryMetrics(peakBytes);
    }

    @Override
    public SearchMemoryMetrics merge(SearchMemoryMetrics other) {
        return peakBytes >= other.peakBytes ? this : other;
    }

    @Override
    public Map<String, String> entries() {
        return Map.of(PEAK_BYTES_KEY, Long.toString(peakBytes));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PEAK_BYTES_KEY, peakBytes);
        return builder.endObject();
    }
}
