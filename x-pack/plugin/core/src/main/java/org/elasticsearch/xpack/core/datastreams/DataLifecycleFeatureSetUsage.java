/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datastreams;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Objects;

public class DataLifecycleFeatureSetUsage extends XPackFeatureSet.Usage {
    final LifecycleStats lifecycleStats;

    public DataLifecycleFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        this.lifecycleStats = new LifecycleStats(input);
    }

    public DataLifecycleFeatureSetUsage(LifecycleStats stats) {
        super(XPackField.DATA_LIFECYCLE, true, true);
        this.lifecycleStats = stats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        lifecycleStats.writeTo(out);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_500_006;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("count", lifecycleStats.dataStreamsWithLifecyclesCount);
        builder.field("default_rollover_used", lifecycleStats.defaultRolloverUsed);
        builder.startObject("retention");
        builder.field("minimum_millis", lifecycleStats.minRetentionMillis);
        builder.field("maximum_millis", lifecycleStats.maxRetentionMillis);
        builder.field("average_millis", lifecycleStats.averageRetentionMillis);
        builder.endObject();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public int hashCode() {
        return lifecycleStats.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        DataLifecycleFeatureSetUsage other = (DataLifecycleFeatureSetUsage) obj;
        return Objects.equals(lifecycleStats, other.lifecycleStats);
    }

    public static class LifecycleStats implements Writeable {
        final long dataStreamsWithLifecyclesCount;
        final long minRetentionMillis;
        final long maxRetentionMillis;
        final double averageRetentionMillis;
        final boolean defaultRolloverUsed;

        public LifecycleStats(
            long dataStreamsWithLifecyclesCount,
            long minRetention,
            long maxRetention,
            double averageRetention,
            boolean defaultRolloverUsed
        ) {
            this.dataStreamsWithLifecyclesCount = dataStreamsWithLifecyclesCount;
            this.minRetentionMillis = minRetention;
            this.maxRetentionMillis = maxRetention;
            this.averageRetentionMillis = averageRetention;
            this.defaultRolloverUsed = defaultRolloverUsed;
        }

        public LifecycleStats(StreamInput in) throws IOException {
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_500_006)) {
                this.dataStreamsWithLifecyclesCount = in.readVLong();
                this.minRetentionMillis = in.readVLong();
                this.maxRetentionMillis = in.readVLong();
                this.averageRetentionMillis = in.readDouble();
                this.defaultRolloverUsed = in.readBoolean();
            } else {
                this.dataStreamsWithLifecyclesCount = 0;
                this.minRetentionMillis = 0;
                this.maxRetentionMillis = 0;
                this.averageRetentionMillis = 0.0;
                this.defaultRolloverUsed = false;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_500_006)) {
                out.writeVLong(dataStreamsWithLifecyclesCount);
                out.writeVLong(minRetentionMillis);
                out.writeVLong(maxRetentionMillis);
                out.writeDouble(averageRetentionMillis);
                out.writeBoolean(defaultRolloverUsed);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                dataStreamsWithLifecyclesCount,
                minRetentionMillis,
                maxRetentionMillis,
                averageRetentionMillis,
                defaultRolloverUsed
            );
        }

        @Override
        public boolean equals(Object obj) {
            if (obj.getClass() != getClass()) {
                return false;
            }
            LifecycleStats other = (LifecycleStats) obj;
            return dataStreamsWithLifecyclesCount == other.dataStreamsWithLifecyclesCount
                && minRetentionMillis == other.minRetentionMillis
                && maxRetentionMillis == other.maxRetentionMillis
                && averageRetentionMillis == other.averageRetentionMillis
                && defaultRolloverUsed == other.defaultRolloverUsed;
        }
    }
}
