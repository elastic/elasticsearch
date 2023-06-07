/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toMap;

public class RepositoriesThrottlingStats implements Writeable, ToXContentFragment {

    private final Map<String, ThrottlingStats> repositoryToThrottlingStats;

    public RepositoriesThrottlingStats(StreamInput in) throws IOException {
        repositoryToThrottlingStats = in.readMap(StreamInput::readString, ThrottlingStats::new);
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

    public record RepositoryThrottling(String repositoryName, long totalReadThrottledNanos, long totalWriteThrottledNanos) {}

    public static class Builder {
        private Set<RepositoryThrottling> recordAccumulator = new HashSet<>();

        public void add(RepositoryThrottling throttlingRecord) {
            recordAccumulator.add(throttlingRecord);
        }

        public RepositoriesThrottlingStats build() {
            Map<String, ThrottlingStats> repositoryNameToThrottlingStats = recordAccumulator.stream()
                .collect(toMap(r -> r.repositoryName, r -> new ThrottlingStats(r.totalReadThrottledNanos, r.totalWriteThrottledNanos)));
            return new RepositoriesThrottlingStats(repositoryNameToThrottlingStats);
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
