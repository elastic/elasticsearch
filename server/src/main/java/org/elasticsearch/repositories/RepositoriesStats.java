/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RepositoriesStats implements Writeable, ToXContentFragment {

    private final Map<String, ThrottlingStats> repositoryThrottlingStats;

    public RepositoriesStats(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            repositoryThrottlingStats = in.readMap(ThrottlingStats::new);
        } else {
            repositoryThrottlingStats = new HashMap<>();
        }
    }

    public RepositoriesStats(Map<String, ThrottlingStats> repositoryThrottlingStats) {
        this.repositoryThrottlingStats = new HashMap<>(repositoryThrottlingStats);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            out.writeMap(repositoryThrottlingStats, StreamOutput::writeWriteable);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("repositories", repositoryThrottlingStats);
        return builder;
    }

    public Map<String, ThrottlingStats> getRepositoryThrottlingStats() {
        return Collections.unmodifiableMap(repositoryThrottlingStats);
    }

    public record ThrottlingStats(long totalReadThrottledNanos, long totalWriteThrottledNanos) implements ToXContentObject, Writeable {

        ThrottlingStats(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (builder.humanReadable()) {
                builder.field("total_read_throttled_time", new TimeValue(totalReadThrottledNanos, TimeUnit.NANOSECONDS));
                builder.field("total_write_throttled_time", new TimeValue(totalWriteThrottledNanos, TimeUnit.NANOSECONDS));
            }
            builder.field("total_read_throttled_time_nanos", totalReadThrottledNanos);
            builder.field("total_write_throttled_time_nanos", totalWriteThrottledNanos);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(totalReadThrottledNanos);
            out.writeVLong(totalWriteThrottledNanos);
        }
    }
}
