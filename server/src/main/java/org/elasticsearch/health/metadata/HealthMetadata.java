/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

/**
 * A cluster state entry that contains a list of all the thresholds used to determine if a node is healthy.
 */
public final class HealthMetadata extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

    public static final String TYPE = "health";

    private static final ParseField DISK_METADATA = new ParseField(Disk.TYPE);
    private static final ParseField SHARD_LIMITS_METADATA = new ParseField(ShardLimits.TYPE);

    private final Disk diskMetadata;
    @Nullable
    private final ShardLimits shardLimitsMetadata;

    public HealthMetadata(Disk diskMetadata, ShardLimits shardLimitsMetadata) {
        this.diskMetadata = diskMetadata;
        this.shardLimitsMetadata = shardLimitsMetadata;
    }

    public HealthMetadata(StreamInput in) throws IOException {
        this.diskMetadata = Disk.readFrom(in);
        this.shardLimitsMetadata = in.getTransportVersion().onOrAfter(ShardLimits.VERSION_SUPPORTING_SHARD_LIMIT_FIELDS)
            ? in.readOptionalWriteable(ShardLimits::readFrom)
            : null;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_5_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        diskMetadata.writeTo(out);
        if (out.getTransportVersion().onOrAfter(ShardLimits.VERSION_SUPPORTING_SHARD_LIMIT_FIELDS)) {
            out.writeOptionalWriteable(shardLimitsMetadata);
        }
    }

    public static NamedDiff<ClusterState.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(ClusterState.Custom.class, TYPE, in);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.single((builder, params) -> {
            builder.startObject(DISK_METADATA.getPreferredName());
            diskMetadata.toXContent(builder, params);
            builder.endObject();
            if (shardLimitsMetadata != null) {
                builder.startObject(SHARD_LIMITS_METADATA.getPreferredName());
                shardLimitsMetadata.toXContent(builder, params);
                builder.endObject();
            }
            return builder;
        });
    }

    public static HealthMetadata getFromClusterState(ClusterState clusterState) {
        return clusterState.custom(HealthMetadata.TYPE);
    }

    public Disk getDiskMetadata() {
        return diskMetadata;
    }

    public ShardLimits getShardLimitsMetadata() {
        return shardLimitsMetadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HealthMetadata that = (HealthMetadata) o;
        return Objects.equals(diskMetadata, that.diskMetadata) && Objects.equals(shardLimitsMetadata, that.shardLimitsMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(diskMetadata, shardLimitsMetadata);
    }

    @Override
    public String toString() {
        return "HealthMetadata{diskMetadata=" + Strings.toString(diskMetadata) + ", shardLimitsMetadata=" + shardLimitsMetadata + "}";
    }

    /**
     * Contains the thresholds needed to determine the health of a cluster when it comes to the amount of room available to create new
     * shards. These values are determined by the elected master.
     */
    public record ShardLimits(int maxShardsPerNode, int maxShardsPerNodeFrozen) implements ToXContentFragment, Writeable {

        private static final String TYPE = "shard_limits";
        private static final ParseField MAX_SHARDS_PER_NODE = new ParseField("max_shards_per_node");
        private static final ParseField MAX_SHARDS_PER_NODE_FROZEN = new ParseField("max_shards_per_node_frozen");
        static final TransportVersion VERSION_SUPPORTING_SHARD_LIMIT_FIELDS = TransportVersion.V_8_8_0;

        static ShardLimits readFrom(StreamInput in) throws IOException {
            return new ShardLimits(in.readInt(), in.readInt());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(MAX_SHARDS_PER_NODE.getPreferredName(), maxShardsPerNode);
            builder.field(MAX_SHARDS_PER_NODE_FROZEN.getPreferredName(), maxShardsPerNodeFrozen);
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(maxShardsPerNode);
            out.writeInt(maxShardsPerNodeFrozen);
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public static Builder newBuilder(ShardLimits shardLimits) {
            return new Builder(shardLimits);
        }

        public static class Builder {

            private int maxShardsPerNode;
            private int maxShardsPerNodeFrozen;

            private Builder() {}

            private Builder(ShardLimits shardLimits) {
                this.maxShardsPerNode = shardLimits.maxShardsPerNode;
                this.maxShardsPerNodeFrozen = shardLimits.maxShardsPerNodeFrozen;
            }

            public Builder maxShardsPerNode(int maxShardsPerNode) {
                this.maxShardsPerNode = maxShardsPerNode;
                return this;
            }

            public Builder maxShardsPerNodeFrozen(int maxShardsPerNodeFrozen) {
                this.maxShardsPerNodeFrozen = maxShardsPerNodeFrozen;
                return this;
            }

            public ShardLimits build() {
                return new ShardLimits(maxShardsPerNode, maxShardsPerNodeFrozen);
            }
        }
    }

    /**
     * Contains the thresholds necessary to determine the health of the disk space of a node. The thresholds are determined by the elected
     * master.
     */
    public record Disk(
        RelativeByteSizeValue highWatermark,
        ByteSizeValue highMaxHeadroom,
        RelativeByteSizeValue floodStageWatermark,
        ByteSizeValue floodStageMaxHeadroom,
        RelativeByteSizeValue frozenFloodStageWatermark,
        ByteSizeValue frozenFloodStageMaxHeadroom
    ) implements ToXContentFragment, Writeable {

        public static final String TYPE = "disk";
        public static final TransportVersion VERSION_SUPPORTING_HEADROOM_FIELDS = TransportVersion.V_8_5_0;

        private static final ParseField HIGH_WATERMARK_FIELD = new ParseField("high_watermark");
        private static final ParseField HIGH_MAX_HEADROOM_FIELD = new ParseField("high_max_headroom");
        private static final ParseField FLOOD_STAGE_WATERMARK_FIELD = new ParseField("flood_stage_watermark");
        private static final ParseField FLOOD_STAGE_MAX_HEADROOM_FIELD = new ParseField("flood_stage_max_headroom");
        private static final ParseField FROZEN_FLOOD_STAGE_WATERMARK_FIELD = new ParseField("frozen_flood_stage_watermark");
        private static final ParseField FROZEN_FLOOD_STAGE_MAX_HEADROOM_FIELD = new ParseField("frozen_flood_stage_max_headroom");

        static Disk readFrom(StreamInput in) throws IOException {
            RelativeByteSizeValue highWatermark = RelativeByteSizeValue.parseRelativeByteSizeValue(
                in.readString(),
                HIGH_WATERMARK_FIELD.getPreferredName()
            );
            RelativeByteSizeValue floodStageWatermark = RelativeByteSizeValue.parseRelativeByteSizeValue(
                in.readString(),
                FLOOD_STAGE_WATERMARK_FIELD.getPreferredName()
            );
            RelativeByteSizeValue frozenFloodStageWatermark = RelativeByteSizeValue.parseRelativeByteSizeValue(
                in.readString(),
                FROZEN_FLOOD_STAGE_WATERMARK_FIELD.getPreferredName()
            );
            ByteSizeValue frozenFloodStageMaxHeadroom = ByteSizeValue.readFrom(in);
            ByteSizeValue highMaxHeadroom = in.getTransportVersion().onOrAfter(VERSION_SUPPORTING_HEADROOM_FIELDS)
                ? ByteSizeValue.readFrom(in)
                : ByteSizeValue.MINUS_ONE;
            ByteSizeValue floodStageMaxHeadroom = in.getTransportVersion().onOrAfter(VERSION_SUPPORTING_HEADROOM_FIELDS)
                ? ByteSizeValue.readFrom(in)
                : ByteSizeValue.MINUS_ONE;
            return new Disk(
                highWatermark,
                highMaxHeadroom,
                floodStageWatermark,
                floodStageMaxHeadroom,
                frozenFloodStageWatermark,
                frozenFloodStageMaxHeadroom
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(describeHighWatermark());
            out.writeString(describeFloodStageWatermark());
            out.writeString(describeFrozenFloodStageWatermark());
            frozenFloodStageMaxHeadroom.writeTo(out);
            if (out.getTransportVersion().onOrAfter(VERSION_SUPPORTING_HEADROOM_FIELDS)) {
                highMaxHeadroom.writeTo(out);
                floodStageMaxHeadroom.writeTo(out);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(HIGH_WATERMARK_FIELD.getPreferredName(), describeHighWatermark());
            builder.field(HIGH_MAX_HEADROOM_FIELD.getPreferredName(), highMaxHeadroom);
            builder.field(FLOOD_STAGE_WATERMARK_FIELD.getPreferredName(), describeFloodStageWatermark());
            builder.field(FLOOD_STAGE_MAX_HEADROOM_FIELD.getPreferredName(), floodStageMaxHeadroom);
            builder.field(FROZEN_FLOOD_STAGE_WATERMARK_FIELD.getPreferredName(), describeFrozenFloodStageWatermark());
            builder.field(FROZEN_FLOOD_STAGE_MAX_HEADROOM_FIELD.getPreferredName(), frozenFloodStageMaxHeadroom);
            return builder;
        }

        private ByteSizeValue getFreeBytes(ByteSizeValue total, RelativeByteSizeValue watermark, ByteSizeValue maxHeadroom) {
            if (watermark.isAbsolute()) {
                return watermark.getAbsolute();
            }
            return ByteSizeValue.subtract(total, watermark.calculateValue(total, maxHeadroom));
        }

        public ByteSizeValue getFreeBytesHighWatermark(ByteSizeValue total) {
            return getFreeBytes(total, highWatermark, highMaxHeadroom);
        }

        public ByteSizeValue getFreeBytesFloodStageWatermark(ByteSizeValue total) {
            return getFreeBytes(total, floodStageWatermark, floodStageMaxHeadroom);
        }

        public ByteSizeValue getFreeBytesFrozenFloodStageWatermark(ByteSizeValue total) {
            return getFreeBytes(total, frozenFloodStageWatermark, frozenFloodStageMaxHeadroom);
        }

        private String getThresholdStringRep(RelativeByteSizeValue relativeByteSizeValue) {
            if (relativeByteSizeValue.isAbsolute()) {
                return relativeByteSizeValue.getAbsolute().getStringRep();
            } else {
                return relativeByteSizeValue.getRatio().formatNoTrailingZerosPercent();
            }
        }

        public String describeHighWatermark() {
            return getThresholdStringRep(highWatermark);
        }

        public String describeFloodStageWatermark() {
            return getThresholdStringRep(floodStageWatermark);
        }

        public String describeFrozenFloodStageWatermark() {
            return getThresholdStringRep(frozenFloodStageWatermark);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Disk disk = (Disk) o;
            return Objects.equals(describeHighWatermark(), disk.describeHighWatermark())
                && Objects.equals(highMaxHeadroom, disk.highMaxHeadroom)
                && Objects.equals(describeFloodStageWatermark(), disk.describeFloodStageWatermark())
                && Objects.equals(floodStageMaxHeadroom, disk.floodStageMaxHeadroom)
                && Objects.equals(describeFrozenFloodStageWatermark(), disk.describeFrozenFloodStageWatermark())
                && Objects.equals(frozenFloodStageMaxHeadroom, disk.frozenFloodStageMaxHeadroom);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                describeHighWatermark(),
                highMaxHeadroom,
                describeFloodStageWatermark(),
                floodStageMaxHeadroom,
                describeFrozenFloodStageWatermark(),
                frozenFloodStageMaxHeadroom
            );
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public static Builder newBuilder(Disk disk) {
            return new Builder(disk);
        }

        public static class Builder {

            private RelativeByteSizeValue highWatermark;
            private ByteSizeValue highMaxHeadroom;
            private RelativeByteSizeValue floodStageWatermark;
            private ByteSizeValue floodStageMaxHeadroom;
            private RelativeByteSizeValue frozenFloodStageWatermark;
            private ByteSizeValue frozenFloodStageMaxHeadroom;

            private Builder(Disk disk) {
                this.highWatermark = disk.highWatermark;
                this.highMaxHeadroom = disk.highMaxHeadroom;
                this.floodStageWatermark = disk.floodStageWatermark;
                this.floodStageMaxHeadroom = disk.floodStageMaxHeadroom;
                this.frozenFloodStageWatermark = disk.frozenFloodStageWatermark;
                this.frozenFloodStageMaxHeadroom = disk.frozenFloodStageMaxHeadroom;
            }

            private Builder() {}

            public Disk.Builder highWatermark(RelativeByteSizeValue highWatermark) {
                this.highWatermark = highWatermark;
                return this;
            }

            public Disk.Builder highWatermark(String highWatermark, String setting) {
                return highWatermark(RelativeByteSizeValue.parseRelativeByteSizeValue(highWatermark, setting));
            }

            public Disk.Builder highMaxHeadroom(ByteSizeValue highMaxHeadroom) {
                this.highMaxHeadroom = highMaxHeadroom;
                return this;
            }

            public Disk.Builder highMaxHeadroom(String highMaxHeadroom, String setting) {
                return highMaxHeadroom(ByteSizeValue.parseBytesSizeValue(highMaxHeadroom, setting));
            }

            public Disk.Builder floodStageWatermark(RelativeByteSizeValue floodStageWatermark) {
                this.floodStageWatermark = floodStageWatermark;
                return this;
            }

            public Disk.Builder floodStageWatermark(String floodStageWatermark, String setting) {
                return floodStageWatermark(RelativeByteSizeValue.parseRelativeByteSizeValue(floodStageWatermark, setting));
            }

            public Disk.Builder floodStageMaxHeadroom(ByteSizeValue floodStageMaxHeadroom) {
                this.floodStageMaxHeadroom = floodStageMaxHeadroom;
                return this;
            }

            public Disk.Builder floodStageMaxHeadroom(String floodStageMaxHeadroom, String setting) {
                return floodStageMaxHeadroom(ByteSizeValue.parseBytesSizeValue(floodStageMaxHeadroom, setting));
            }

            public Disk.Builder frozenFloodStageWatermark(RelativeByteSizeValue frozenFloodStageWatermark) {
                this.frozenFloodStageWatermark = frozenFloodStageWatermark;
                return this;
            }

            public Disk.Builder frozenFloodStageWatermark(String frozenFloodStageWatermark, String setting) {
                return frozenFloodStageWatermark(RelativeByteSizeValue.parseRelativeByteSizeValue(frozenFloodStageWatermark, setting));
            }

            public Disk.Builder frozenFloodStageMaxHeadroom(ByteSizeValue frozenFloodStageMaxHeadroom) {
                this.frozenFloodStageMaxHeadroom = frozenFloodStageMaxHeadroom;
                return this;
            }

            public Disk.Builder frozenFloodStageMaxHeadroom(String frozenFloodStageMaxHeadroom, String setting) {
                return frozenFloodStageMaxHeadroom(ByteSizeValue.parseBytesSizeValue(frozenFloodStageMaxHeadroom, setting));
            }

            public Disk build() {
                return new Disk(
                    highWatermark,
                    highMaxHeadroom,
                    floodStageWatermark,
                    floodStageMaxHeadroom,
                    frozenFloodStageWatermark,
                    frozenFloodStageMaxHeadroom
                );
            }
        }
    }
}
