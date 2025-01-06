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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Objects;

public class DataStreamLifecycleFeatureSetUsage extends XPackFeatureUsage {

    public static final DataStreamLifecycleFeatureSetUsage DISABLED = new DataStreamLifecycleFeatureSetUsage();
    final LifecycleStats lifecycleStats;

    public DataStreamLifecycleFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        this.lifecycleStats = LifecycleStats.read(input);
    }

    private DataStreamLifecycleFeatureSetUsage() {
        super(XPackField.DATA_STREAM_LIFECYCLE, true, false);
        this.lifecycleStats = LifecycleStats.INITIAL;
    }

    public DataStreamLifecycleFeatureSetUsage(LifecycleStats stats) {
        super(XPackField.DATA_STREAM_LIFECYCLE, true, true);
        this.lifecycleStats = stats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        lifecycleStats.writeTo(out);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_9_X;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        if (enabled) {
            lifecycleStats.toXContent(builder, params);
        }
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled, lifecycleStats);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        DataStreamLifecycleFeatureSetUsage other = (DataStreamLifecycleFeatureSetUsage) obj;
        return available == other.available && enabled == other.enabled && Objects.equals(lifecycleStats, other.lifecycleStats);
    }

    public static class LifecycleStats implements Writeable, ToXContentFragment {

        public static final LifecycleStats INITIAL = new LifecycleStats(0, true, RetentionStats.NO_DATA, RetentionStats.NO_DATA, Map.of());
        public static final String DEFAULT_RETENTION_FIELD_NAME = "default";
        public static final String MAX_RETENTION_FIELD_NAME = "max";
        final long dataStreamsWithLifecyclesCount;
        final boolean defaultRolloverUsed;
        final RetentionStats dataRetentionStats;
        final RetentionStats effectiveRetentionStats;
        final Map<String, GlobalRetentionStats> globalRetentionStats;

        public LifecycleStats(
            long dataStreamsWithLifecyclesCount,
            boolean defaultRolloverUsed,
            RetentionStats dataRetentionStats,
            RetentionStats effectiveRetentionStats,
            Map<String, GlobalRetentionStats> globalRetentionStats
        ) {
            this.dataStreamsWithLifecyclesCount = dataStreamsWithLifecyclesCount;
            this.defaultRolloverUsed = defaultRolloverUsed;
            this.dataRetentionStats = dataRetentionStats;
            this.effectiveRetentionStats = effectiveRetentionStats;
            this.globalRetentionStats = globalRetentionStats;
        }

        public static LifecycleStats read(StreamInput in) throws IOException {
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                return new LifecycleStats(
                    in.readVLong(),
                    in.readBoolean(),
                    RetentionStats.read(in),
                    RetentionStats.read(in),
                    in.readMap(GlobalRetentionStats::new)
                );
            } else if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
                var dataStreamsWithLifecyclesCount = in.readVLong();
                var minDataRetention = in.readVLong();
                var maxDataRetention = in.readVLong();
                var avgDataRetention = in.readDouble();
                var defaultRolledOverUsed = in.readBoolean();
                return new LifecycleStats(
                    dataStreamsWithLifecyclesCount,
                    defaultRolledOverUsed,
                    new RetentionStats(dataStreamsWithLifecyclesCount, avgDataRetention, minDataRetention, maxDataRetention),
                    RetentionStats.NO_DATA,
                    Map.of()
                );
            } else {
                return INITIAL;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                out.writeVLong(dataStreamsWithLifecyclesCount);
                out.writeBoolean(defaultRolloverUsed);
                dataRetentionStats.writeTo(out);
                effectiveRetentionStats.writeTo(out);
                out.writeMap(globalRetentionStats, (o, v) -> v.writeTo(o));
            } else if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
                out.writeVLong(dataStreamsWithLifecyclesCount);
                out.writeVLong(dataRetentionStats.minMillis() == null ? 0 : dataRetentionStats.minMillis());
                out.writeVLong(dataRetentionStats.maxMillis() == null ? 0 : dataRetentionStats.maxMillis());
                out.writeDouble(dataRetentionStats.avgMillis() == null ? 0 : dataRetentionStats.avgMillis());
                out.writeBoolean(defaultRolloverUsed);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                dataStreamsWithLifecyclesCount,
                defaultRolloverUsed,
                dataRetentionStats,
                effectiveRetentionStats,
                globalRetentionStats
            );
        }

        @Override
        public boolean equals(Object obj) {
            if (obj.getClass() != getClass()) {
                return false;
            }
            LifecycleStats other = (LifecycleStats) obj;
            return dataStreamsWithLifecyclesCount == other.dataStreamsWithLifecyclesCount
                && defaultRolloverUsed == other.defaultRolloverUsed
                && Objects.equals(dataRetentionStats, other.dataRetentionStats)
                && Objects.equals(effectiveRetentionStats, other.effectiveRetentionStats)
                && Objects.equals(globalRetentionStats, other.globalRetentionStats);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("count", dataStreamsWithLifecyclesCount);
            builder.field("default_rollover_used", defaultRolloverUsed);

            builder.startObject("data_retention");
            builder.field("configured_data_streams", dataRetentionStats.dataStreamCount());
            if (dataRetentionStats.dataStreamCount() > 0) {
                builder.field("minimum_millis", dataRetentionStats.minMillis);
                builder.field("maximum_millis", dataRetentionStats.maxMillis);
                builder.field("average_millis", dataRetentionStats.avgMillis);
            }
            builder.endObject();

            builder.startObject("effective_retention");
            builder.field("retained_data_streams", effectiveRetentionStats.dataStreamCount());
            if (effectiveRetentionStats.dataStreamCount() > 0) {
                builder.field("minimum_millis", effectiveRetentionStats.minMillis);
                builder.field("maximum_millis", effectiveRetentionStats.maxMillis);
                builder.field("average_millis", effectiveRetentionStats.avgMillis);
            }
            builder.endObject();

            builder.startObject("global_retention");
            globalRetentionStatsToXContent(builder, params, LifecycleStats.DEFAULT_RETENTION_FIELD_NAME);
            globalRetentionStatsToXContent(builder, params, LifecycleStats.MAX_RETENTION_FIELD_NAME);
            builder.endObject();
            return builder;
        }

        private void globalRetentionStatsToXContent(XContentBuilder builder, Params params, String retentionType) throws IOException {
            builder.startObject(retentionType);
            GlobalRetentionStats stats = globalRetentionStats.get(retentionType);
            builder.field("defined", stats != null);
            if (stats != null) {
                builder.field("affected_data_streams", stats.dataStreamCount());
                builder.field("retention_millis", stats.retention());
            }
            builder.endObject();
        }
    }

    public record RetentionStats(long dataStreamCount, Double avgMillis, Long minMillis, Long maxMillis) implements Writeable {

        static final RetentionStats NO_DATA = new RetentionStats(0, null, null, null);

        public static RetentionStats create(LongSummaryStatistics statistics) {
            if (statistics.getCount() == 0) {
                return NO_DATA;
            }
            return new RetentionStats(statistics.getCount(), statistics.getAverage(), statistics.getMin(), statistics.getMax());
        }

        public static RetentionStats read(StreamInput in) throws IOException {
            long dataStreamCount = in.readVLong();
            if (dataStreamCount == 0) {
                return NO_DATA;
            }
            double avgMillis = in.readDouble();
            long minMillis = in.readVLong();
            long maxMillis = in.readVLong();
            return new RetentionStats(dataStreamCount, avgMillis, minMillis, maxMillis);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(dataStreamCount);
            if (dataStreamCount > 0) {
                out.writeDouble(avgMillis);
                out.writeVLong(minMillis);
                out.writeVLong(maxMillis);
            }
        }
    }

    public record GlobalRetentionStats(long dataStreamCount, long retention) implements Writeable {

        public GlobalRetentionStats(long dataStreamCount, TimeValue retention) {
            this(dataStreamCount, retention.getMillis());
        }

        public GlobalRetentionStats(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(dataStreamCount);
            out.writeVLong(retention);
        }
    }
}
