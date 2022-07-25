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
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Objects;

/**
 * A cluster state entry that contains a list of all the thresholds used to determine if a node is healthy.
 */
public final class HealthMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "health";

    public static final ConstructingObjectParser<HealthMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        true,
        args -> new HealthMetadata((Disk) args[0])
    );

    private static final ParseField DISK_METADATA = new ParseField(Disk.TYPE);

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> Disk.fromXContent(p), DISK_METADATA);
    }

    private final Disk diskMetadata;

    public HealthMetadata(Disk diskMetadata) {
        this.diskMetadata = diskMetadata;
    }

    public HealthMetadata(StreamInput in) throws IOException {
        this.diskMetadata = new Disk(in);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_4_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        diskMetadata.writeTo(out);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.Custom.class, TYPE, in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(DISK_METADATA.getPreferredName());
        diskMetadata.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public static HealthMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static HealthMetadata getHealthCustomMetadata(ClusterState clusterState) {
        return clusterState.getMetadata().custom(HealthMetadata.TYPE);
    }

    @Override
    public boolean isFragment() {
        return true;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.API_AND_GATEWAY;
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
        RelativeByteSizeValue floodStageWatermark,
        RelativeByteSizeValue frozenFloodStageWatermark,
        ByteSizeValue frozenFloodStageMaxHeadroom
    ) implements ToXContentFragment, Writeable {

        public static final String TYPE = "disk";

        private static final ParseField HIGH_WATERMARK_FIELD = new ParseField("high_watermark");
        private static final ParseField FLOOD_STAGE_WATERMARK_FIELD = new ParseField("flood_stage_watermark");
        private static final ParseField FROZEN_FLOOD_STAGE_WATERMARK_FIELD = new ParseField("frozen_flood_stage_watermark");
        private static final ParseField FROZEN_FLOOD_STAGE_MAX_HEADROOM_FIELD = new ParseField("frozen_flood_stage_max_headroom");

        public static final ConstructingObjectParser<Disk, Void> PARSER = new ConstructingObjectParser<>(
            TYPE,
            true,
            (args) -> new Disk(
                RelativeByteSizeValue.parseRelativeByteSizeValue((String) args[0], HIGH_WATERMARK_FIELD.getPreferredName()),
                RelativeByteSizeValue.parseRelativeByteSizeValue((String) args[1], FLOOD_STAGE_WATERMARK_FIELD.getPreferredName()),
                RelativeByteSizeValue.parseRelativeByteSizeValue((String) args[2], FROZEN_FLOOD_STAGE_WATERMARK_FIELD.getPreferredName()),
                ByteSizeValue.parseBytesSizeValue((String) args[3], FROZEN_FLOOD_STAGE_MAX_HEADROOM_FIELD.getPreferredName())
            )
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), HIGH_WATERMARK_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), FLOOD_STAGE_WATERMARK_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), FROZEN_FLOOD_STAGE_WATERMARK_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), FROZEN_FLOOD_STAGE_MAX_HEADROOM_FIELD);
        }

        Disk(StreamInput in) throws IOException {
            this(
                RelativeByteSizeValue.parseRelativeByteSizeValue(in.readString(), HIGH_WATERMARK_FIELD.getPreferredName()),
                RelativeByteSizeValue.parseRelativeByteSizeValue(in.readString(), FLOOD_STAGE_WATERMARK_FIELD.getPreferredName()),
                RelativeByteSizeValue.parseRelativeByteSizeValue(in.readString(), FROZEN_FLOOD_STAGE_WATERMARK_FIELD.getPreferredName()),
                new ByteSizeValue(in)
            );
        }

        static Disk fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(describeHighWatermark());
            out.writeString(describeFloodStageWatermark());
            out.writeString(describeFrozenFloodStageWatermark());
            frozenFloodStageMaxHeadroom.writeTo(out);
        }

        @Override
        public boolean isFragment() {
            return true;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(HIGH_WATERMARK_FIELD.getPreferredName(), describeHighWatermark());
            builder.field(FLOOD_STAGE_WATERMARK_FIELD.getPreferredName(), describeFloodStageWatermark());
            builder.field(FROZEN_FLOOD_STAGE_WATERMARK_FIELD.getPreferredName(), describeFrozenFloodStageWatermark());
            builder.field(FROZEN_FLOOD_STAGE_MAX_HEADROOM_FIELD.getPreferredName(), frozenFloodStageMaxHeadroom);
            return builder;
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
                && Objects.equals(describeFloodStageWatermark(), disk.describeFloodStageWatermark())
                && Objects.equals(describeFrozenFloodStageWatermark(), disk.describeFrozenFloodStageWatermark())
                && Objects.equals(frozenFloodStageMaxHeadroom, disk.frozenFloodStageMaxHeadroom);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                describeHighWatermark(),
                describeFloodStageWatermark(),
                describeFrozenFloodStageWatermark(),
                frozenFloodStageMaxHeadroom
            );
        }

        static Builder newBuilder() {
            return new Builder();
        }

        static Builder newBuilder(Disk disk) {
            return new Builder(disk);
        }

        public static class Builder {

            private RelativeByteSizeValue highWatermark;
            private RelativeByteSizeValue floodStageWatermark;
            private RelativeByteSizeValue frozenFloodStageWatermark;
            private ByteSizeValue frozenFloodStageMaxHeadroom;

            private Builder(Disk disk) {
                this.highWatermark = disk.highWatermark;
                this.floodStageWatermark = disk.floodStageWatermark;
                this.frozenFloodStageWatermark = disk.frozenFloodStageWatermark;
                this.frozenFloodStageMaxHeadroom = disk.frozenFloodStageMaxHeadroom;
            }

            private Builder() {}

            Disk.Builder highWatermark(RelativeByteSizeValue highWatermark) {
                this.highWatermark = highWatermark;
                return this;
            }

            Disk.Builder highWatermark(String highWatermark, String setting) {
                return highWatermark(RelativeByteSizeValue.parseRelativeByteSizeValue(highWatermark, setting));
            }

            Disk.Builder floodStageWatermark(RelativeByteSizeValue floodStageWatermark) {
                this.floodStageWatermark = floodStageWatermark;
                return this;
            }

            public Disk.Builder floodStageWatermark(String floodStageWatermark, String setting) {
                return floodStageWatermark(RelativeByteSizeValue.parseRelativeByteSizeValue(floodStageWatermark, setting));
            }

            Disk.Builder frozenFloodStageWatermark(RelativeByteSizeValue frozenFloodStageWatermark) {
                this.frozenFloodStageWatermark = frozenFloodStageWatermark;
                return this;
            }

            Disk.Builder frozenFloodStageWatermark(String frozenFloodStageWatermark, String setting) {
                return frozenFloodStageWatermark(RelativeByteSizeValue.parseRelativeByteSizeValue(frozenFloodStageWatermark, setting));
            }

            Disk.Builder frozenFloodStageMaxHeadroom(ByteSizeValue frozenFloodStageMaxHeadroom) {
                this.frozenFloodStageMaxHeadroom = frozenFloodStageMaxHeadroom;
                return this;
            }

            Disk.Builder frozenFloodStageMaxHeadroom(String frozenFloodStageMaxHeadroom, String setting) {
                return frozenFloodStageMaxHeadroom(ByteSizeValue.parseBytesSizeValue(frozenFloodStageMaxHeadroom, setting));
            }

            Disk build() {
                return new Disk(highWatermark, floodStageWatermark, frozenFloodStageWatermark, frozenFloodStageMaxHeadroom);
            }
        }
    }
}
