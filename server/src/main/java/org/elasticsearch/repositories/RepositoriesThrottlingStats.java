/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RepositoriesThrottlingStats implements Writeable, ToXContentFragment {

    private final Map<String, ThrottlingStats> repositoryToThrottlingStats;

    public RepositoriesThrottlingStats(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_9_0)) {
            repositoryToThrottlingStats = in.readMap(StreamInput::readString, ThrottlingStats::new);
        } else {
            repositoryToThrottlingStats = new HashMap<>();
        }
    }

    public RepositoriesThrottlingStats(Map<String, ThrottlingStats> repositoryThrottlingStats) {
        repositoryToThrottlingStats = new HashMap<>(repositoryThrottlingStats);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(repositoryToThrottlingStats, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("repository", repositoryToThrottlingStats);
        return builder;
    }

    public Map<String, ThrottlingStats> stats() {
        return Collections.unmodifiableMap(repositoryToThrottlingStats);
    }

    public static class Builder {
        private final Map<String, ThrottlingStats> repoThrottlingStats = new HashMap<>();

        public Builder add(String repoName, long totalReadThrottledNanos, long totalWriteThrottledNanos) {
            repoThrottlingStats.put(repoName, new ThrottlingStats(totalReadThrottledNanos, totalWriteThrottledNanos));
            return this;
        }

        public RepositoriesThrottlingStats build() {
            return new RepositoriesThrottlingStats(repoThrottlingStats);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class ThrottlingStats implements ToXContentObject, Writeable {

        private final long totalReadThrottledNanos;
        private final long totalWriteThrottledNanos;

        ThrottlingStats(StreamInput in) throws IOException {
            this.totalReadThrottledNanos = in.readLong();
            this.totalWriteThrottledNanos = in.readLong();
        }

        public ThrottlingStats(long totalReadThrottledNanos, long totalWriteThrottledNanos) {
            this.totalReadThrottledNanos = totalReadThrottledNanos;
            this.totalWriteThrottledNanos = totalWriteThrottledNanos;
        }

        public long getTotalReadThrottledNanos() {
            return totalReadThrottledNanos;
        }

        public long getTotalWriteThrottledNanos() {
            return totalWriteThrottledNanos;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("total_read_throttled_nanos", totalReadThrottledNanos);
            builder.field("total_write_throttled_nanos", totalWriteThrottledNanos);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(totalReadThrottledNanos);
            out.writeLong(totalWriteThrottledNanos);
        }
    }
}
