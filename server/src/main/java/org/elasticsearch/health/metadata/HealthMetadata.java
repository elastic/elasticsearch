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
import org.elasticsearch.common.settings.DiskThresholdSettingParser;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
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
        Threshold lowWatermark,
        Threshold highWatermark,
        Threshold floodStageWatermark,
        Threshold frozenFloodStageWatermark,
        ByteSizeValue frozenFloodStageMaxHeadroom
    ) implements ToXContentFragment, Writeable {

        public static final String TYPE = "disk";

        private static final ParseField LOW_WATERMARK_FIELD = new ParseField("low_watermark");
        private static final ParseField HIGH_WATERMARK_FIELD = new ParseField("high_watermark");
        private static final ParseField FLOOD_STAGE_WATERMARK_FIELD = new ParseField("flood_stage_watermark");
        private static final ParseField FROZEN_FLOOD_STAGE_WATERMARK_FIELD = new ParseField("frozen_flood_stage_watermark");
        private static final ParseField FROZEN_FLOOD_STAGE_MAX_HEADROOM_FIELD = new ParseField("frozen_flood_stage_max_headroom");

        public static final ConstructingObjectParser<Disk, Void> PARSER = new ConstructingObjectParser<>(
            TYPE,
            true,
            (args) -> new Disk(
                Threshold.parse((String) args[0], LOW_WATERMARK_FIELD.getPreferredName()),
                Threshold.parse((String) args[1], HIGH_WATERMARK_FIELD.getPreferredName()),
                Threshold.parse((String) args[2], FLOOD_STAGE_WATERMARK_FIELD.getPreferredName()),
                Threshold.parse((String) args[3], FROZEN_FLOOD_STAGE_WATERMARK_FIELD.getPreferredName()),
                ByteSizeValue.parseBytesSizeValue((String) args[4], FROZEN_FLOOD_STAGE_MAX_HEADROOM_FIELD.getPreferredName())
            )
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), LOW_WATERMARK_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), HIGH_WATERMARK_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), FLOOD_STAGE_WATERMARK_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), FROZEN_FLOOD_STAGE_WATERMARK_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), FROZEN_FLOOD_STAGE_MAX_HEADROOM_FIELD);
        }

        Disk(StreamInput in) throws IOException {
            this(
                Threshold.readFrom(in, LOW_WATERMARK_FIELD.getPreferredName()),
                Threshold.readFrom(in, HIGH_WATERMARK_FIELD.getPreferredName()),
                Threshold.readFrom(in, FLOOD_STAGE_WATERMARK_FIELD.getPreferredName()),
                Threshold.readFrom(in, FROZEN_FLOOD_STAGE_WATERMARK_FIELD.getPreferredName()),
                new ByteSizeValue(in)
            );
        }

        static Disk fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            lowWatermark.writeTo(out);
            highWatermark.writeTo(out);
            floodStageWatermark.writeTo(out);
            frozenFloodStageWatermark.writeTo(out);
            frozenFloodStageMaxHeadroom.writeTo(out);
        }

        @Override
        public boolean isFragment() {
            return true;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(LOW_WATERMARK_FIELD.getPreferredName(), lowWatermark.getStringRep());
            builder.field(HIGH_WATERMARK_FIELD.getPreferredName(), highWatermark.getStringRep());
            builder.field(FLOOD_STAGE_WATERMARK_FIELD.getPreferredName(), floodStageWatermark.getStringRep());
            builder.field(FROZEN_FLOOD_STAGE_WATERMARK_FIELD.getPreferredName(), frozenFloodStageWatermark.getStringRep());
            builder.field(FROZEN_FLOOD_STAGE_MAX_HEADROOM_FIELD.getPreferredName(), frozenFloodStageMaxHeadroom);
            return builder;
        }

        static Builder newBuilder() {
            return new Builder();
        }

        static Builder newBuilder(Disk disk) {
            return new Builder(disk);
        }

        public static class Builder {

            private Threshold lowWatermark;
            private Threshold highWatermark;
            private Threshold floodStageWatermark;
            private Threshold frozenFloodStageWatermark;
            private ByteSizeValue frozenFloodStageMaxHeadroom;

            private Builder(Disk disk) {
                this.lowWatermark = disk.lowWatermark;
                this.highWatermark = disk.highWatermark;
                this.floodStageWatermark = disk.floodStageWatermark;
                this.frozenFloodStageWatermark = disk.frozenFloodStageWatermark;
                this.frozenFloodStageMaxHeadroom = disk.frozenFloodStageMaxHeadroom;
            }

            private Builder() {}

            Disk.Builder lowWatermark(String lowWatermark, String setting) {
                return lowWatermark(Threshold.parse(lowWatermark, setting));
            }

            Disk.Builder lowWatermark(Threshold lowWatermark) {
                this.lowWatermark = lowWatermark;
                return this;
            }

            Disk.Builder highWatermark(Threshold highWatermark) {
                this.highWatermark = highWatermark;
                return this;
            }

            Disk.Builder highWatermark(String highWatermark, String setting) {
                return highWatermark(Threshold.parse(highWatermark, setting));
            }

            Disk.Builder floodStageWatermark(Threshold floodStageWatermark) {
                this.floodStageWatermark = floodStageWatermark;
                return this;
            }

            public Disk.Builder floodStageWatermark(String floodStageWatermark, String setting) {
                return floodStageWatermark(Threshold.parse(floodStageWatermark, setting));
            }

            Disk.Builder frozenFloodStageWatermark(Threshold frozenFloodStageWatermark) {
                this.frozenFloodStageWatermark = frozenFloodStageWatermark;
                return this;
            }

            Disk.Builder frozenFloodStageWatermark(String frozenFloodStageWatermark, String setting) {
                return frozenFloodStageWatermark(Threshold.parse(frozenFloodStageWatermark, setting));
            }

            Disk.Builder frozenFloodStageMaxHeadroom(ByteSizeValue frozenFloodStageMaxHeadroom) {
                this.frozenFloodStageMaxHeadroom = frozenFloodStageMaxHeadroom;
                return this;
            }

            Disk.Builder frozenFloodStageMaxHeadroom(String frozenFloodStageMaxHeadroom, String setting) {
                return frozenFloodStageMaxHeadroom(ByteSizeValue.parseBytesSizeValue(frozenFloodStageMaxHeadroom, setting));
            }

            Disk build() {
                return new Disk(lowWatermark, highWatermark, floodStageWatermark, frozenFloodStageWatermark, frozenFloodStageMaxHeadroom);
            }
        }

        /* Represents a disk space threshold in one of two ways:
         * - Percent of maximum disk usage allowed, for example, if disk usage is more than 90% it's over the threshold
         * - Minimum of free disk space allowed, for example free disk space is less than 10GB it's over the threshold
         */
        public record Threshold(double maxUsedPercent, ByteSizeValue minFreeBytes) implements Writeable {

            public Threshold {
                assert maxUsedPercent == 100.0 || minFreeBytes.getBytes() == 0
                    : "only one of the values in a disk threshold can be set, the other one needs to have the default value";
            }

            public Threshold(double maxUsedPercent) {
                this(maxUsedPercent, ByteSizeValue.ZERO);
            }

            public Threshold(ByteSizeValue minFreeBytes) {
                this(100.0, minFreeBytes);
            }

            public Threshold(RelativeByteSizeValue value) {
                this(
                    value.isAbsolute() ? 100.0 : value.getRatio().getAsPercent(),
                    value.isAbsolute() ? value.getAbsolute() : ByteSizeValue.ZERO
                );
            }

            public static Threshold readFrom(StreamInput in, String setting) throws IOException {
                String description = in.readString();
                return parse(description, setting);
            }

            static Threshold parse(String description, String setting) {
                if (DiskThresholdSettingParser.definitelyNotPercentage(description)) {
                    return new Threshold(DiskThresholdSettingParser.parseThresholdBytes(description, setting));
                } else {
                    return new Threshold(DiskThresholdSettingParser.parseThresholdPercentage(description));
                }
            }

            public String getStringRep() {
                return minFreeBytes.equals(ByteSizeValue.ZERO)
                    ? RatioValue.formatPercentNoTrailingZeros(maxUsedPercent)
                    : minFreeBytes.toString();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(this.getStringRep());
            }
        }
    }
}
