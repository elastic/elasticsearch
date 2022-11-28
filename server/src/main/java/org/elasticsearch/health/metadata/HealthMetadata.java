/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A cluster state entry that contains a list of all the thresholds used to determine if a node is healthy.
 */
public final class HealthMetadata extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

    public static final String TYPE = "health";

    private static final ParseField DISK_METADATA = new ParseField(Disk.TYPE);

    private final Disk diskMetadata;

    public HealthMetadata(Disk diskMetadata) {
        this.diskMetadata = diskMetadata;
    }

    public HealthMetadata(StreamInput in) throws IOException {
        this.diskMetadata = Disk.readFrom(in);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_5_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        diskMetadata.writeTo(out);
    }

    public static NamedDiff<ClusterState.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(ClusterState.Custom.class, TYPE, in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(DISK_METADATA.getPreferredName());
        diskMetadata.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public static HealthMetadata getFromClusterState(ClusterState clusterState) {
        return clusterState.custom(HealthMetadata.TYPE);
    }

    @Override
    public boolean isFragment() {
        return true;
    }

    public Disk getDiskMetadata() {
        return diskMetadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HealthMetadata that = (HealthMetadata) o;
        return Objects.equals(diskMetadata, that.diskMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(diskMetadata);
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
        public static Version VERSION_SUPPORTING_HEADROOM_FIELDS = Version.V_8_5_0;

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
            ByteSizeValue highMaxHeadroom = in.getVersion().onOrAfter(VERSION_SUPPORTING_HEADROOM_FIELDS)
                ? ByteSizeValue.readFrom(in)
                : ByteSizeValue.MINUS_ONE;
            ByteSizeValue floodStageMaxHeadroom = in.getVersion().onOrAfter(VERSION_SUPPORTING_HEADROOM_FIELDS)
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
            if (out.getVersion().onOrAfter(VERSION_SUPPORTING_HEADROOM_FIELDS)) {
                highMaxHeadroom.writeTo(out);
                floodStageMaxHeadroom.writeTo(out);
            }
        }

        @Override
        public boolean isFragment() {
            return true;
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
            return getFreeBytes(total, highWatermark, ByteSizeValue.MINUS_ONE);
        }

        public ByteSizeValue getFreeBytesFloodStageWatermark(ByteSizeValue total) {
            return getFreeBytes(total, floodStageWatermark, ByteSizeValue.MINUS_ONE);
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
