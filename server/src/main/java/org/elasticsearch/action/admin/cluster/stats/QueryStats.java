/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class QueryStats implements Writeable, ToXContentObject {
    private Map<String, Long> queriesCounts = new HashMap<>();

    public QueryStats() {}

    QueryStats(Map<String, Long> queriesCounts) {
        this.queriesCounts = queriesCounts;
    }

    public void add(QueryStats stats) {
        if (stats == null) return;
        stats.queriesCounts.forEach((query, count) -> queriesCounts.merge(query, count, Long::sum));
    }

    public QueryStats(StreamInput in) throws IOException {
        this.queriesCounts = in.readMap(StreamInput::readString, StreamInput::readLong);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(queriesCounts, StreamOutput::writeString, StreamOutput::writeLong);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("queries");
        long totalCount = 0;
        for (var entry : queriesCounts.entrySet()) {
            long count = entry.getValue();
            builder.startObject();
            builder.field("name", entry.getKey());
            builder.field("count", count);
            builder.endObject();
            totalCount += count;
        }
        if (totalCount > 0) {
            builder.startObject();
            builder.field("name", "total");
            builder.field("count", totalCount);
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    public Map<String, Long> getQueriesCounts() {
        return Collections.unmodifiableMap(queriesCounts);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryStats that = (QueryStats) o;
        return queriesCounts.equals(that.queriesCounts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queriesCounts);
    }
}
