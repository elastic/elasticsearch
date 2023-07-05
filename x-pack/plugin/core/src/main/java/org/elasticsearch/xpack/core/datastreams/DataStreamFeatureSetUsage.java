/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datastreams;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Objects;

public class DataStreamFeatureSetUsage extends XPackFeatureSet.Usage {
    private final DataStreamStats streamStats;
    private final LifecycleStats lifecycleStats;

    public DataStreamFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        this.streamStats = DataStreamStats.read(input);
        if (input.getTransportVersion().onOrAfter(TransportVersion.V_8_500_031)) {
            var lifecycleStats = input.readOptionalWriteable(LifecycleStats::read);
            this.lifecycleStats = DataStreamLifecycle.isEnabled() ? lifecycleStats : null;
        } else {
            this.lifecycleStats = null;
        }
    }

    public DataStreamFeatureSetUsage(DataStreamStats stats, LifecycleStats lifecycleStats) {
        super(XPackField.DATA_STREAMS, true, true);
        this.streamStats = stats;
        this.lifecycleStats = lifecycleStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        streamStats.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_500_031)) {
            if (DataStreamLifecycle.isEnabled()) {
                out.writeOptionalWriteable(lifecycleStats);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_7_9_0;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("data_streams", streamStats.totalDataStreamCount);
        builder.field("indices_count", streamStats.indicesBehindDataStream);
        if (DataStreamLifecycle.isEnabled() && lifecycleStats != null) {
            builder.startObject("lifecycle");
            builder.field("managed_data_steams", lifecycleStats.dataStreamsWithLifecyclesCount);
            builder.field("managed_backing_indices", lifecycleStats.indicesWithLifecyclesCount);
            builder.field("default_rollover_used", lifecycleStats.defaultRolloverUsed);
            {
                builder.startObject("retention");
                builder.field("minimum_millis", lifecycleStats.minRetentionMillis);
                builder.field("maximum_millis", lifecycleStats.maxRetentionMillis);
                builder.field("average_millis", lifecycleStats.averageRetentionMillis);
                builder.endObject();
            }
            builder.endObject();
        }
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public int hashCode() {
        return streamStats.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        DataStreamFeatureSetUsage other = (DataStreamFeatureSetUsage) obj;
        return Objects.equals(streamStats, other.streamStats);
    }

    public record DataStreamStats(long totalDataStreamCount, long indicesBehindDataStream) implements Writeable {

        public static DataStreamStats read(StreamInput in) throws IOException {
            return new DataStreamStats(in.readVLong(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(this.totalDataStreamCount);
            out.writeVLong(this.indicesBehindDataStream);
        }
    }

    public record LifecycleStats(
        long dataStreamsWithLifecyclesCount,
        long indicesWithLifecyclesCount,
        long minRetentionMillis,
        long maxRetentionMillis,
        double averageRetentionMillis,
        boolean defaultRolloverUsed
    ) implements Writeable {

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(dataStreamsWithLifecyclesCount);
            out.writeVLong(indicesWithLifecyclesCount);
            out.writeVLong(minRetentionMillis);
            out.writeVLong(maxRetentionMillis);
            out.writeDouble(averageRetentionMillis);
            out.writeBoolean(defaultRolloverUsed);
        }

        public static LifecycleStats read(StreamInput in) throws IOException {
            return new LifecycleStats(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readDouble(), in.readBoolean());
        }
    }
}
