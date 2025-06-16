/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datastreams;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.datastreams.DataStreamLifecycleFeatureSetUsage.LifecycleStats;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class DataStreamFeatureSetUsage extends XPackFeatureUsage {
    private final DataStreamStats streamStats;

    public DataStreamFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        this.streamStats = new DataStreamStats(input);
    }

    public DataStreamFeatureSetUsage(DataStreamStats stats) {
        super(XPackField.DATA_STREAMS, true, true);
        this.streamStats = stats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        streamStats.writeTo(out);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_7_9_0;
    }

    public DataStreamStats getStats() {
        return streamStats;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("data_streams", streamStats.totalDataStreamCount);
        builder.field("indices_count", streamStats.indicesBehindDataStream);
        builder.startObject("failure_store");
        builder.field("explicitly_enabled_count", streamStats.failureStoreExplicitlyEnabledDataStreamCount);
        builder.field("effectively_enabled_count", streamStats.failureStoreEffectivelyEnabledDataStreamCount);
        builder.field("failure_indices_count", streamStats.failureStoreIndicesCount);

        // Failures lifecycle
        builder.startObject("lifecycle");
        builder.field("explicitly_enabled_count", streamStats.failuresLifecycleExplicitlyEnabledCount);
        builder.field("effectively_enabled_count", streamStats.failuresLifecycleEffectivelyEnabledCount);

        // Retention
        DataStreamLifecycleFeatureSetUsage.RetentionStats.toXContentFragment(
            builder,
            streamStats.failuresLifecycleDataRetentionStats,
            false
        );
        DataStreamLifecycleFeatureSetUsage.RetentionStats.toXContentFragment(
            builder,
            streamStats.failuresLifecycleEffectiveRetentionStats,
            true
        );
        builder.startObject("global_retention");
        DataStreamLifecycleFeatureSetUsage.GlobalRetentionStats.toXContentFragment(
            builder,
            LifecycleStats.DEFAULT_RETENTION_FIELD_NAME,
            streamStats.globalRetentionStats.get(LifecycleStats.DEFAULT_RETENTION_FIELD_NAME)
        );
        DataStreamLifecycleFeatureSetUsage.GlobalRetentionStats.toXContentFragment(
            builder,
            LifecycleStats.MAX_RETENTION_FIELD_NAME,
            streamStats.globalRetentionStats.get(LifecycleStats.MAX_RETENTION_FIELD_NAME)
        );
        builder.endObject();
        builder.endObject();
        builder.endObject();
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

    public record DataStreamStats(
        long totalDataStreamCount,
        long indicesBehindDataStream,
        long failureStoreExplicitlyEnabledDataStreamCount,
        long failureStoreEffectivelyEnabledDataStreamCount,
        long failureStoreIndicesCount,
        long failuresLifecycleExplicitlyEnabledCount,
        long failuresLifecycleEffectivelyEnabledCount,
        DataStreamLifecycleFeatureSetUsage.RetentionStats failuresLifecycleDataRetentionStats,
        DataStreamLifecycleFeatureSetUsage.RetentionStats failuresLifecycleEffectiveRetentionStats,
        Map<String, DataStreamLifecycleFeatureSetUsage.GlobalRetentionStats> globalRetentionStats
    ) implements Writeable {

        public DataStreamStats(StreamInput in) throws IOException {
            this(
                in.readVLong(),
                in.readVLong(),
                in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0) ? in.readVLong() : 0,
                in.getTransportVersion().onOrAfter(TransportVersions.FAILURE_STORE_ENABLED_BY_CLUSTER_SETTING) ? in.readVLong() : 0,
                in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0) ? in.readVLong() : 0,
                in.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_FAILURES_LIFECYCLE_BACKPORT_8_19) ? in.readVLong() : 0,
                in.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_FAILURES_LIFECYCLE_BACKPORT_8_19) ? in.readVLong() : 0,
                in.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_FAILURES_LIFECYCLE_BACKPORT_8_19)
                    ? DataStreamLifecycleFeatureSetUsage.RetentionStats.read(in)
                    : DataStreamLifecycleFeatureSetUsage.RetentionStats.NO_DATA,
                in.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_FAILURES_LIFECYCLE_BACKPORT_8_19)
                    ? DataStreamLifecycleFeatureSetUsage.RetentionStats.read(in)
                    : DataStreamLifecycleFeatureSetUsage.RetentionStats.NO_DATA,
                in.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_FAILURES_LIFECYCLE_BACKPORT_8_19)
                    ? in.readMap(DataStreamLifecycleFeatureSetUsage.GlobalRetentionStats::new)
                    : Map.of()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(this.totalDataStreamCount);
            out.writeVLong(this.indicesBehindDataStream);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                out.writeVLong(this.failureStoreExplicitlyEnabledDataStreamCount);
                if (out.getTransportVersion().onOrAfter(TransportVersions.FAILURE_STORE_ENABLED_BY_CLUSTER_SETTING)) {
                    out.writeVLong(failureStoreEffectivelyEnabledDataStreamCount);
                }
                out.writeVLong(this.failureStoreIndicesCount);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.INTRODUCE_FAILURES_LIFECYCLE_BACKPORT_8_19)) {
                out.writeVLong(failuresLifecycleExplicitlyEnabledCount);
                out.writeVLong(failuresLifecycleEffectivelyEnabledCount);
                failuresLifecycleDataRetentionStats.writeTo(out);
                failuresLifecycleEffectiveRetentionStats.writeTo(out);
                out.writeMap(globalRetentionStats, (o, v) -> v.writeTo(o));
            }
        }
    }
}
