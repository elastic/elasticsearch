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
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.common.util.DiskThresholdSettingParser;
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

    public static final String TYPE = "health_metadata";

    public static final ConstructingObjectParser<HealthMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        true,
        args -> new HealthMetadata((DiskThresholds) args[0])
    );

    private static final ParseField DISK_THRESHOLDS = new ParseField("disk_thresholds");

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> DiskThresholds.fromXContent(p), DISK_THRESHOLDS);
    }

    private final DiskThresholds diskThresholds;

    public HealthMetadata(DiskThresholds diskThresholds) {
        this.diskThresholds = diskThresholds;
    }

    public HealthMetadata(StreamInput in) throws IOException {
        this.diskThresholds = new DiskThresholds(in);
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
        diskThresholds.writeTo(out);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.Custom.class, TYPE, in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(DISK_THRESHOLDS.getPreferredName());
        diskThresholds.toXContent(builder, params);
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

    public DiskThresholds getDiskThresholds() {
        return diskThresholds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HealthMetadata that = (HealthMetadata) o;
        return Objects.equals(diskThresholds, that.diskThresholds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(diskThresholds);
    }

    /**
     * Contains the thresholds necessary to determine the health of the disk space of a node. The thresholds are determined by the elected
     * master.
     */
    public record DiskThresholds(
        DiskThreshold lowWatermark,
        DiskThreshold highWatermark,
        DiskThreshold floodStageWatermark,
        DiskThreshold frozenFloodStageWatermark,
        ByteSizeValue frozenFloodStageMaxHeadroom,
        DiskThreshold yellowThreshold,
        DiskThreshold redThreshold
    ) implements ToXContentFragment, Writeable {

        private static final ParseField LOW_WATERMARK_FIELD = new ParseField("low_watermark");
        private static final ParseField HIGH_WATERMARK_FIELD = new ParseField("high_watermark");
        private static final ParseField FLOOD_STAGE_WATERMARK_FIELD = new ParseField("flood_stage_watermark");
        private static final ParseField FROZEN_FLOOD_STAGE_WATERMARK_FIELD = new ParseField("frozen_flood_stage_watermark");
        private static final ParseField FROZEN_FLOOD_STAGE_MAX_HEADROOM_FIELD = new ParseField("frozen_flood_stage_max_headroom");
        private static final ParseField YELLOW_THRESHOLD_FIELD = new ParseField("yellow_threshold");
        private static final ParseField RED_THRESHOLD_FIELD = new ParseField("red_threshold");

        public static final ConstructingObjectParser<DiskThresholds, Void> PARSER = new ConstructingObjectParser<>(
            "disk_thresholds",
            true,
            (args) -> new DiskThresholds(
                DiskThreshold.parse((String) args[0], LOW_WATERMARK_FIELD.getPreferredName()),
                DiskThreshold.parse((String) args[1], HIGH_WATERMARK_FIELD.getPreferredName()),
                DiskThreshold.parse((String) args[2], FLOOD_STAGE_WATERMARK_FIELD.getPreferredName()),
                DiskThreshold.parse((String) args[3], FROZEN_FLOOD_STAGE_WATERMARK_FIELD.getPreferredName()),
                ByteSizeValue.parseBytesSizeValue((String) args[4], FROZEN_FLOOD_STAGE_MAX_HEADROOM_FIELD.getPreferredName()),
                DiskThreshold.parse((String) args[5], YELLOW_THRESHOLD_FIELD.getPreferredName()),
                DiskThreshold.parse((String) args[6], RED_THRESHOLD_FIELD.getPreferredName())
            )
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), LOW_WATERMARK_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), HIGH_WATERMARK_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), FLOOD_STAGE_WATERMARK_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), FROZEN_FLOOD_STAGE_WATERMARK_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), FROZEN_FLOOD_STAGE_MAX_HEADROOM_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), YELLOW_THRESHOLD_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), RED_THRESHOLD_FIELD);
        }

        DiskThresholds(StreamInput in) throws IOException {
            this(
                DiskThreshold.readFrom(in),
                DiskThreshold.readFrom(in),
                DiskThreshold.readFrom(in),
                DiskThreshold.readFrom(in),
                new ByteSizeValue(in),
                DiskThreshold.readFrom(in),
                DiskThreshold.readFrom(in)
            );
        }

        static DiskThresholds createDiskThresholds(
            DiskThresholdSettings allocationDiskThresholdSettings,
            HealthDiskThresholdSettings healthDiskThresholdSettings
        ) {
            RelativeByteSizeValue frozenFloodStage = allocationDiskThresholdSettings.getFrozenFloodStage();
            return new DiskThresholds(
                DiskThreshold.fromWatermark(
                    allocationDiskThresholdSettings.getFreeDiskThresholdLow(),
                    allocationDiskThresholdSettings.getFreeBytesThresholdLow()
                ),
                DiskThreshold.fromWatermark(
                    allocationDiskThresholdSettings.getFreeDiskThresholdHigh(),
                    allocationDiskThresholdSettings.getFreeBytesThresholdHigh()
                ),
                DiskThreshold.fromWatermark(
                    allocationDiskThresholdSettings.getFreeDiskThresholdFloodStage(),
                    allocationDiskThresholdSettings.getFreeBytesThresholdFloodStage()
                ),
                frozenFloodStage.isAbsolute()
                    ? new DiskThreshold(frozenFloodStage.getAbsolute())
                    : new DiskThreshold(frozenFloodStage.getRatio().getAsPercent()),
                allocationDiskThresholdSettings.getFrozenFloodStageMaxHeadroom(),
                DiskThreshold.fromHealthDiskThreshold(healthDiskThresholdSettings.getYellowThreshold()),
                DiskThreshold.fromHealthDiskThreshold(healthDiskThresholdSettings.getRedThreshold())
            );
        }

        static DiskThresholds fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            lowWatermark.writeTo(out);
            highWatermark.writeTo(out);
            floodStageWatermark.writeTo(out);
            frozenFloodStageWatermark.writeTo(out);
            frozenFloodStageMaxHeadroom.writeTo(out);
            yellowThreshold.writeTo(out);
            redThreshold.writeTo(out);
        }

        @Override
        public boolean isFragment() {
            return true;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(LOW_WATERMARK_FIELD.getPreferredName(), lowWatermark.toStringRep());
            builder.field(HIGH_WATERMARK_FIELD.getPreferredName(), highWatermark.toStringRep());
            builder.field(FLOOD_STAGE_WATERMARK_FIELD.getPreferredName(), floodStageWatermark.toStringRep());
            builder.field(FROZEN_FLOOD_STAGE_WATERMARK_FIELD.getPreferredName(), frozenFloodStageWatermark.toStringRep());
            builder.field(FROZEN_FLOOD_STAGE_MAX_HEADROOM_FIELD.getPreferredName(), frozenFloodStageMaxHeadroom);
            builder.field(YELLOW_THRESHOLD_FIELD.getPreferredName(), yellowThreshold.toStringRep());
            builder.field(RED_THRESHOLD_FIELD.getPreferredName(), redThreshold.toStringRep());
            return builder;
        }

        public record DiskThreshold(double maxUsedPercent, ByteSizeValue minFreeBytes) implements Writeable {

            public DiskThreshold {
                assert maxUsedPercent == 100.0 || minFreeBytes.getBytes() == 0
                    : "only one of the values in a disk threshold can be set, the other one needs to have the default value";
            }

            public DiskThreshold(double maxUsedPercent) {
                this(maxUsedPercent, ByteSizeValue.ZERO);
            }

            public DiskThreshold(ByteSizeValue minFreeBytes) {
                this(100.0, minFreeBytes);
            }

            public static DiskThreshold readFrom(StreamInput in) throws IOException {
                String description = in.readString();
                return parse(description, "");
            }

            static DiskThreshold parse(String description, String setting) {
                ByteSizeValue minFreeBytes = DiskThresholdSettingParser.parseThresholdBytes(description, setting);
                if (minFreeBytes.getBytes() > 0) {
                    return new DiskThreshold(minFreeBytes);
                } else {
                    return new DiskThreshold(DiskThresholdSettingParser.parseThresholdPercentage(description));
                }
            }

            static DiskThreshold fromHealthDiskThreshold(RelativeByteSizeValue relativeByteSizeValue) {
                if (relativeByteSizeValue.isAbsolute()) {
                    return new DiskThreshold(relativeByteSizeValue.getAbsolute());
                } else {
                    return new DiskThreshold(relativeByteSizeValue.getRatio().getAsPercent());
                }
            }

            static DiskThreshold fromWatermark(double minFreeDiskSpacePercent, ByteSizeValue minFreeBytes) {
                if (minFreeBytes.getBytes() > 0) {
                    return new DiskThreshold(minFreeBytes);
                } else {
                    return new DiskThreshold(100.0 - minFreeDiskSpacePercent);
                }
            }

            public String toStringRep() {
                return minFreeBytes.equals(ByteSizeValue.ZERO)
                    ? RatioValue.formatPercentNoTrailingZeros(maxUsedPercent)
                    : minFreeBytes.toString();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(this.toStringRep());
            }
        }
    }
}
