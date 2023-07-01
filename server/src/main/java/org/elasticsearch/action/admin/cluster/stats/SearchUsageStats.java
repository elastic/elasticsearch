/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

/**
 * Holds a snapshot of the search usage statistics.
 * Used to hold the stats for a single node that's part of a {@link ClusterStatsNodeResponse}, as well as to
 * accumulate stats for the entire cluster and return them as part of the {@link ClusterStatsResponse}.
 */
public final class SearchUsageStats implements Writeable, ToXContentFragment {
    private long totalSearchCount;
    private final Map<String, Long> queries;
    private final Map<String, Long> sections;

    /**
     * Creates a new empty stats instance, that will get additional stats added through {@link #add(SearchUsageStats)}
     */
    public SearchUsageStats() {
        this.totalSearchCount = 0L;
        this.queries = new HashMap<>();
        this.sections = new HashMap<>();
    }

    /**
     * Creates a new stats instance with the provided info. The expectation is that when a new instance is created using
     * this constructor, the provided stats are final and won't be modified further.
     */
    public SearchUsageStats(Map<String, Long> queries, Map<String, Long> sections, long totalSearchCount) {
        this.totalSearchCount = totalSearchCount;
        this.queries = queries;
        this.sections = sections;
    }

    public SearchUsageStats(StreamInput in) throws IOException {
        this.queries = in.readMap(StreamInput::readLong);
        this.sections = in.readMap(StreamInput::readLong);
        this.totalSearchCount = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(queries, StreamOutput::writeString, StreamOutput::writeLong);
        out.writeMap(sections, StreamOutput::writeString, StreamOutput::writeLong);
        out.writeVLong(totalSearchCount);
    }

    /**
     * Add the provided stats to the ones held by the current instance, effectively merging the two
     */
    public void add(SearchUsageStats stats) {
        stats.queries.forEach((query, count) -> queries.merge(query, count, Long::sum));
        stats.sections.forEach((query, count) -> sections.merge(query, count, Long::sum));
        this.totalSearchCount += stats.totalSearchCount;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("search");
        builder.field("total", totalSearchCount);
        {
            builder.field("queries");
            builder.map(queries);
            builder.field("sections");
            builder.map(sections);
        }
        builder.endObject();
        return builder;
    }

    public Map<String, Long> getQueryUsage() {
        return Collections.unmodifiableMap(queries);
    }

    public Map<String, Long> getSectionsUsage() {
        return Collections.unmodifiableMap(sections);
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
        return totalSearchCount == that.totalSearchCount && queries.equals(that.queries) && sections.equals(that.sections);
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalSearchCount, queries, sections);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
