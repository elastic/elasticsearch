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
 * A cluster state record that contains a list of all the thresholds used to determine if a node is healthy.
 */
public final class HealthMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "health_metadata";

    public static final ConstructingObjectParser<HealthMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        true,
        args -> new HealthMetadata((DiskHealthThresholds) args[0])
    );

    private static final ParseField DISK_THRESHOLDS = new ParseField("disk_thresholds");

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> DiskHealthThresholds.fromXContent(p), DISK_THRESHOLDS);
    }

    private final DiskHealthThresholds diskThresholds;

    public HealthMetadata(DiskHealthThresholds diskThresholds) {
        this.diskThresholds = diskThresholds;
    }

    public HealthMetadata(StreamInput in) throws IOException {
        this.diskThresholds = new DiskHealthThresholds(in);
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

    public DiskHealthThresholds getDiskThresholds() {
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
    public record DiskHealthThresholds(
        Threshold lowWatermark,
        Threshold highWatermark,
        Threshold floodStageWatermark,
        Threshold floodStageWatermarkFrozen,
        ByteSizeValue floodStageWatermarkFrozenMaxHeadroom,
        Threshold yellowThreshold,
        Threshold redThreshold
    ) implements ToXContentFragment, Writeable {

        private static final ParseField LOW_WATERMARK_FIELD = new ParseField("low_watermark");
        private static final ParseField HIGH_WATERMARK_FIELD = new ParseField("high_watermark");
        private static final ParseField FLOOD_STAGE_WATERMARK_FIELD = new ParseField("flood_stage_watermark");
        private static final ParseField FLOOD_STAGE_WATERMARK_FROZEN_FIELD = new ParseField("flood_stage_watermark_frozen");
        private static final ParseField FLOOD_STAGE_WATERMARK_FROZEN_MAX_HEADROOM_FIELD = new ParseField(
            "flood_stage_watermark_frozen_max_headroom"
        );
        private static final ParseField YELLOW_THRESHOLD_FIELD = new ParseField("yellow_threshold");
        private static final ParseField RED_THRESHOLD_FIELD = new ParseField("red_threshold");

        public static final ConstructingObjectParser<DiskHealthThresholds, Void> PARSER = new ConstructingObjectParser<>(
            "disk_thresholds",
            true,
            (args) -> new DiskHealthThresholds(
                (Threshold) args[0],
                (Threshold) args[1],
                (Threshold) args[2],
                (Threshold) args[3],
                ByteSizeValue.parseBytesSizeValue((String) args[4], FLOOD_STAGE_WATERMARK_FROZEN_MAX_HEADROOM_FIELD.getPreferredName()),
                (Threshold) args[5],
                (Threshold) args[6]
            )
        );

        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> Threshold.fromXContent(p), LOW_WATERMARK_FIELD);
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> Threshold.fromXContent(p), HIGH_WATERMARK_FIELD);
            PARSER.declareObject(
                ConstructingObjectParser.constructorArg(),
                (p, c) -> Threshold.fromXContent(p),
                FLOOD_STAGE_WATERMARK_FIELD
            );
            PARSER.declareObject(
                ConstructingObjectParser.constructorArg(),
                (p, c) -> Threshold.fromXContent(p),
                FLOOD_STAGE_WATERMARK_FROZEN_FIELD
            );
            PARSER.declareString(ConstructingObjectParser.constructorArg(), FLOOD_STAGE_WATERMARK_FROZEN_MAX_HEADROOM_FIELD);
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> Threshold.fromXContent(p), YELLOW_THRESHOLD_FIELD);
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> Threshold.fromXContent(p), RED_THRESHOLD_FIELD);
        }

        DiskHealthThresholds(StreamInput in) throws IOException {
            this(
                new Threshold(in),
                new Threshold(in),
                new Threshold(in),
                new Threshold(in),
                new ByteSizeValue(in),
                new Threshold(in),
                new Threshold(in)
            );
        }

        static DiskHealthThresholds from(DiskThresholdSettings settings) {
            RelativeByteSizeValue frozenFloodStage = settings.getFrozenFloodStage();
            return new DiskHealthThresholds(
                new Threshold(100.0 - settings.getFreeDiskThresholdLow(), settings.getFreeBytesThresholdLow()),
                new Threshold(100.0 - settings.getFreeDiskThresholdHigh(), settings.getFreeBytesThresholdHigh()),
                new Threshold(100.0 - settings.getFreeDiskThresholdFloodStage(), settings.getFreeBytesThresholdFloodStage()),
                new Threshold(
                    frozenFloodStage.getRatio() == null ? 0.0 : frozenFloodStage.getRatio().getAsPercent(),
                    frozenFloodStage.isAbsolute() ? frozenFloodStage.getAbsolute() : ByteSizeValue.ZERO
                ),
                settings.getFrozenFloodStageMaxHeadroom(),
                new Threshold(100.0, ByteSizeValue.ZERO),
                new Threshold(100.0, ByteSizeValue.ZERO)
            );
        }

        static DiskHealthThresholds fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            lowWatermark.writeTo(out);
            highWatermark.writeTo(out);
            floodStageWatermark.writeTo(out);
            floodStageWatermarkFrozen.writeTo(out);
            floodStageWatermarkFrozenMaxHeadroom.writeTo(out);
            yellowThreshold.writeTo(out);
            redThreshold.writeTo(out);
        }

        @Override
        public boolean isFragment() {
            return true;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(LOW_WATERMARK_FIELD.getPreferredName());
            lowWatermark.toXContent(builder, params);
            builder.endObject();
            builder.startObject(HIGH_WATERMARK_FIELD.getPreferredName());
            highWatermark.toXContent(builder, params);
            builder.endObject();
            builder.startObject(FLOOD_STAGE_WATERMARK_FIELD.getPreferredName());
            floodStageWatermark.toXContent(builder, params);
            builder.endObject();
            builder.startObject(FLOOD_STAGE_WATERMARK_FROZEN_FIELD.getPreferredName());
            floodStageWatermarkFrozen.toXContent(builder, params);
            builder.endObject();
            builder.field(FLOOD_STAGE_WATERMARK_FROZEN_MAX_HEADROOM_FIELD.getPreferredName(), floodStageWatermarkFrozenMaxHeadroom);
            builder.startObject(YELLOW_THRESHOLD_FIELD.getPreferredName());
            yellowThreshold.toXContent(builder, params);
            builder.endObject();
            builder.startObject(RED_THRESHOLD_FIELD.getPreferredName());
            redThreshold.toXContent(builder, params);
            builder.endObject();
            return builder;
        }

        public record Threshold(double maxPercentageUsed, ByteSizeValue minFreeBytes) implements ToXContentFragment, Writeable {

            private static final ParseField MAX_PERCENTAGE_USED_FIELD = new ParseField("max_percentage_used");
            private static final ParseField MIN_FREE_BYTES = new ParseField("min_free_bytes");

            public static final ConstructingObjectParser<Threshold, Void> PARSER = new ConstructingObjectParser<>(
                "threshold",
                true,
                (args) -> new Threshold(
                    (double) args[0],
                    ByteSizeValue.parseBytesSizeValue((String) args[1], MIN_FREE_BYTES.getPreferredName())
                )
            );

            static {
                PARSER.declareDouble(ConstructingObjectParser.constructorArg(), MAX_PERCENTAGE_USED_FIELD);
                PARSER.declareString(ConstructingObjectParser.constructorArg(), MIN_FREE_BYTES);
            }

            Threshold(StreamInput in) throws IOException {
                this(in.readDouble(), new ByteSizeValue(in));
            }

            static Threshold fromXContent(XContentParser parser) throws IOException {
                return PARSER.parse(parser, null);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.field(MAX_PERCENTAGE_USED_FIELD.getPreferredName(), maxPercentageUsed);
                builder.field(MIN_FREE_BYTES.getPreferredName(), minFreeBytes);
                return null;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeDouble(maxPercentageUsed);
                minFreeBytes.writeTo(out);
            }
        }
    }
}
