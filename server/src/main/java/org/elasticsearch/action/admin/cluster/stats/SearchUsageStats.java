/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.TransportVersions.RETRIEVERS_TELEMETRY_ADDED;
import static org.elasticsearch.TransportVersions.V_8_12_0;

/**
 * Holds a snapshot of the search usage statistics.
 * Used to hold the stats for a single node that's part of a {@link ClusterStatsNodeResponse}, as well as to
 * accumulate stats for the entire cluster and return them as part of the {@link ClusterStatsResponse}.
 */
public final class SearchUsageStats implements Writeable, ToXContentFragment {
    private long totalSearchCount;
    private final Map<String, Long> queries;
    private final Map<String, Long> rescorers;
    private final Map<String, Long> sections;
    private final Map<String, Long> retrievers;

    /**
     * Creates a new empty stats instance, that will get additional stats added through {@link #add(SearchUsageStats)}
     */
    public SearchUsageStats() {
        this.totalSearchCount = 0L;
        this.queries = new HashMap<>();
        this.sections = new HashMap<>();
        this.rescorers = new HashMap<>();
        this.retrievers = new HashMap<>();
    }

    /**
     * Creates a new stats instance with the provided info. The expectation is that when a new instance is created using
     * this constructor, the provided stats are final and won't be modified further.
     */
    public SearchUsageStats(
        Map<String, Long> queries,
        Map<String, Long> rescorers,
        Map<String, Long> sections,
        Map<String, Long> retrievers,
        long totalSearchCount
    ) {
        this.totalSearchCount = totalSearchCount;
        this.queries = queries;
        this.sections = sections;
        this.rescorers = rescorers;
        this.retrievers = retrievers;
    }

    public SearchUsageStats(StreamInput in) throws IOException {
        this.queries = in.readMap(StreamInput::readLong);
        this.sections = in.readMap(StreamInput::readLong);
        this.totalSearchCount = in.readVLong();
        this.rescorers = in.getTransportVersion().onOrAfter(V_8_12_0) ? in.readMap(StreamInput::readLong) : Map.of();
        this.retrievers = in.getTransportVersion().onOrAfter(RETRIEVERS_TELEMETRY_ADDED) ? in.readMap(StreamInput::readLong) : Map.of();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(queries, StreamOutput::writeLong);
        out.writeMap(sections, StreamOutput::writeLong);
        out.writeVLong(totalSearchCount);

        if (out.getTransportVersion().onOrAfter(V_8_12_0)) {
            out.writeMap(rescorers, StreamOutput::writeLong);
        }
        if (out.getTransportVersion().onOrAfter(RETRIEVERS_TELEMETRY_ADDED)) {
            out.writeMap(retrievers, StreamOutput::writeLong);
        }
    }

    /**
     * Add the provided stats to the ones held by the current instance, effectively merging the two
     */
    public void add(SearchUsageStats stats) {
        stats.queries.forEach((query, count) -> queries.merge(query, count, Long::sum));
        stats.rescorers.forEach((rescorer, count) -> rescorers.merge(rescorer, count, Long::sum));
        stats.sections.forEach((query, count) -> sections.merge(query, count, Long::sum));
        stats.retrievers.forEach((query, count) -> retrievers.merge(query, count, Long::sum));
        this.totalSearchCount += stats.totalSearchCount;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("search");
        builder.field("total", totalSearchCount);
        {
            builder.field("queries");
            builder.map(queries);
            builder.field("rescorers");
            builder.map(rescorers);
            builder.field("sections");
            builder.map(sections);
            builder.field("retrievers");
            builder.map(retrievers);
        }
        builder.endObject();
        return builder;
    }

    public Map<String, Long> getQueryUsage() {
        return Collections.unmodifiableMap(queries);
    }

    public Map<String, Long> getRescorerUsage() {
        return Collections.unmodifiableMap(rescorers);
    }

    public Map<String, Long> getSectionsUsage() {
        return Collections.unmodifiableMap(sections);
    }

    public Map<String, Long> getRetrieversUsage() {
        return Collections.unmodifiableMap(retrievers);
    }

    public long getTotalSearchCount() {
        return totalSearchCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SearchUsageStats that = (SearchUsageStats) o;
        return totalSearchCount == that.totalSearchCount
            && queries.equals(that.queries)
            && rescorers.equals(that.rescorers)
            && sections.equals(that.sections)
            && retrievers.equals(that.retrievers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalSearchCount, queries, rescorers, sections, retrievers);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
