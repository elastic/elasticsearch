/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datastreams;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.HashMap;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Objects;

public class DataStreamLifecycleFeatureSetUsage extends XPackFeatureUsage {

    private static final TransportVersion INCLUDES_FROZEN_AFTER = TransportVersion.fromName("dlm_telemetry_frozen_after");

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
        return TransportVersion.minimumCompatible();
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

        public static final LifecycleStats INITIAL = new LifecycleStats(
            0,
            true,
            TimeThresholdStats.NO_DATA,
            TimeThresholdStats.NO_DATA,
            null,
            Map.of()
        );
        public static final String DEFAULT_RETENTION_FIELD_NAME = "default";
        public static final String MAX_RETENTION_FIELD_NAME = "max";
        final long dataStreamsWithLifecyclesCount;
        final boolean defaultRolloverUsed;
        final TimeThresholdStats dataRetentionStats;
        final TimeThresholdStats effectiveRetentionStats;
        @Nullable
        final TimeThresholdStats frozenAfterStats;
        final Map<String, GlobalRetentionStats> globalRetentionStats;

        public LifecycleStats(
            long dataStreamsWithLifecyclesCount,
            boolean defaultRolloverUsed,
            TimeThresholdStats dataRetentionStats,
            TimeThresholdStats effectiveRetentionStats,
            @Nullable TimeThresholdStats frozenAfterStats,
            Map<String, GlobalRetentionStats> globalRetentionStats
        ) {
            this.dataStreamsWithLifecyclesCount = dataStreamsWithLifecyclesCount;
            this.defaultRolloverUsed = defaultRolloverUsed;
            this.dataRetentionStats = dataRetentionStats;
            this.effectiveRetentionStats = effectiveRetentionStats;
            this.globalRetentionStats = globalRetentionStats;
            this.frozenAfterStats = frozenAfterStats;
        }

        public static LifecycleStats read(StreamInput in) throws IOException {
            return new LifecycleStats(
                in.readVLong(),
                in.readBoolean(),
                TimeThresholdStats.read(in),
                TimeThresholdStats.read(in),
                in.getTransportVersion().supports(INCLUDES_FROZEN_AFTER) ? in.readOptionalWriteable(TimeThresholdStats::read) : null,
                in.readMap(GlobalRetentionStats::new)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(dataStreamsWithLifecyclesCount);
            out.writeBoolean(defaultRolloverUsed);
            dataRetentionStats.writeTo(out);
            effectiveRetentionStats.writeTo(out);
            if (out.getTransportVersion().supports(INCLUDES_FROZEN_AFTER)) {
                out.writeOptionalWriteable(frozenAfterStats);
            }
            out.writeMap(globalRetentionStats, (o, v) -> v.writeTo(o));
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                dataStreamsWithLifecyclesCount,
                defaultRolloverUsed,
                dataRetentionStats,
                effectiveRetentionStats,
                frozenAfterStats,
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
                && Objects.equals(frozenAfterStats, other.frozenAfterStats)
                && Objects.equals(globalRetentionStats, other.globalRetentionStats);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("count", dataStreamsWithLifecyclesCount);
            builder.field("default_rollover_used", defaultRolloverUsed);

            TimeThresholdStats.toXContentFragment(builder, dataRetentionStats, TimeThresholdStats.DATA_RETENTION);
            TimeThresholdStats.toXContentFragment(builder, effectiveRetentionStats, TimeThresholdStats.EFFECTIVE_RETENTION);
            if (frozenAfterStats != null) {
                TimeThresholdStats.toXContentFragment(builder, frozenAfterStats, TimeThresholdStats.FROZEN_AFTER);
            }

            builder.startObject("global_retention");
            GlobalRetentionStats.toXContentFragment(
                builder,
                LifecycleStats.DEFAULT_RETENTION_FIELD_NAME,
                globalRetentionStats.get(LifecycleStats.DEFAULT_RETENTION_FIELD_NAME)
            );
            GlobalRetentionStats.toXContentFragment(
                builder,
                LifecycleStats.MAX_RETENTION_FIELD_NAME,
                globalRetentionStats.get(LifecycleStats.MAX_RETENTION_FIELD_NAME)
            );
            builder.endObject();
            return builder;
        }
    }

    public record TimeThresholdStats(long dataStreamCount, Double avgMillis, Long minMillis, Long maxMillis) implements Writeable {
        static final TimeThresholdStats NO_DATA = new TimeThresholdStats(0, null, null, null);
        private static final String CONFIGURED_DATA_STREAMS = "configured_data_streams";
        public static final Tuple<String, String> EFFECTIVE_RETENTION = Tuple.tuple("effective_retention", "retained_data_streams");
        public static final Tuple<String, String> DATA_RETENTION = Tuple.tuple("data_retention", CONFIGURED_DATA_STREAMS);
        public static final Tuple<String, String> FROZEN_AFTER = Tuple.tuple("frozen_after", CONFIGURED_DATA_STREAMS);

        public static TimeThresholdStats create(LongSummaryStatistics statistics) {
            if (statistics.getCount() == 0) {
                return NO_DATA;
            }
            return new TimeThresholdStats(statistics.getCount(), statistics.getAverage(), statistics.getMin(), statistics.getMax());
        }

        public static TimeThresholdStats read(StreamInput in) throws IOException {
            long dataStreamCount = in.readVLong();
            if (dataStreamCount == 0) {
                return NO_DATA;
            }
            double avgMillis = in.readDouble();
            long minMillis = in.readVLong();
            long maxMillis = in.readVLong();
            return new TimeThresholdStats(dataStreamCount, avgMillis, minMillis, maxMillis);
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

        static void toXContentFragment(XContentBuilder builder, TimeThresholdStats stats, Tuple<String, String> nameAndCountLabel)
            throws IOException {
            builder.startObject(nameAndCountLabel.v1());
            builder.field(nameAndCountLabel.v2(), stats.dataStreamCount());
            if (stats.dataStreamCount() > 0) {
                builder.field("minimum_millis", stats.minMillis);
                builder.field("maximum_millis", stats.maxMillis);
                builder.field("average_millis", stats.avgMillis);
            }
            builder.endObject();
        }
    }

    public record GlobalRetentionStats(long dataStreamCount, long retention) implements Writeable {

        public GlobalRetentionStats(long dataStreamCount, TimeValue retention) {
            this(dataStreamCount, retention.getMillis());
        }

        public GlobalRetentionStats(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong());
        }

        public static Map<String, GlobalRetentionStats> getGlobalRetentionStats(
            DataStreamGlobalRetention globalRetention,
            long dataStreamsWithDefaultRetention,
            long dataStreamsWithMaxRetention
        ) {
            if (globalRetention == null) {
                return Map.of();
            }
            Map<String, GlobalRetentionStats> globalRetentionStats = new HashMap<>();
            if (globalRetention.defaultRetention() != null) {
                globalRetentionStats.put(
                    LifecycleStats.DEFAULT_RETENTION_FIELD_NAME,
                    new GlobalRetentionStats(dataStreamsWithDefaultRetention, globalRetention.defaultRetention())
                );
            }
            if (globalRetention.maxRetention() != null) {
                globalRetentionStats.put(
                    LifecycleStats.MAX_RETENTION_FIELD_NAME,
                    new GlobalRetentionStats(dataStreamsWithMaxRetention, globalRetention.maxRetention())
                );
            }
            return globalRetentionStats;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(dataStreamCount);
            out.writeVLong(retention);
        }

        static void toXContentFragment(XContentBuilder builder, String retentionType, GlobalRetentionStats stats) throws IOException {
            builder.startObject(retentionType);
            builder.field("defined", stats != null);
            if (stats != null) {
                builder.field("affected_data_streams", stats.dataStreamCount());
                builder.field("retention_millis", stats.retention());
            }
            builder.endObject();
        }
    }
}
