/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.health;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
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
        this.diskThresholds = in.readNamedWriteable(DiskThresholds.class);
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
        out.writeNamedWriteable(diskThresholds);
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
    record DiskThresholds(
        String lowWatermark,
        String highWatermark,
        String floodStageWatermark,
        String floodStageWatermarkFrozen,
        String yellowThreshold,
        String redThreshold
    ) implements ToXContentFragment, NamedWriteable {

        private static final ParseField LOW_WATERMARK_FIELD = new ParseField("low_watermark");
        private static final ParseField HIGH_WATERMARK_FIELD = new ParseField("high_watermark");
        private static final ParseField FLOOD_STAGE_WATERMARK_FIELD = new ParseField("flood_stage_watermark");
        private static final ParseField FLOOD_STAGE_WATERMARK_FROZEN_FIELD = new ParseField("flood_stage_watermark_frozen");
        private static final ParseField YELLOW_THRESHOLD_FIELD = new ParseField("yellow_threshold");
        private static final ParseField RED_THRESHOLD_FIELD = new ParseField("red_threshold");

        public static final ConstructingObjectParser<DiskThresholds, Void> PARSER = new ConstructingObjectParser<>(
            "disk_thresholds",
            true,
            (args) -> new DiskThresholds(
                (String) args[0],
                (String) args[1],
                (String) args[2],
                (String) args[3],
                (String) args[4],
                (String) args[5]
            )
        );
        public static final String NAME = "disk_health_thresholds";

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), LOW_WATERMARK_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), HIGH_WATERMARK_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), FLOOD_STAGE_WATERMARK_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), FLOOD_STAGE_WATERMARK_FROZEN_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), YELLOW_THRESHOLD_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), RED_THRESHOLD_FIELD);
        }

        static DiskThresholds fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        public static DiskThresholds readFrom(StreamInput in) throws IOException {
            return new DiskThresholds(in.readString(), in.readString(), in.readString(), in.readString(), in.readString(), in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(lowWatermark);
            out.writeString(highWatermark);
            out.writeString(floodStageWatermark);
            out.writeString(floodStageWatermarkFrozen);
            out.writeString(yellowThreshold);
            out.writeString(redThreshold);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(LOW_WATERMARK_FIELD.getPreferredName(), lowWatermark);
            builder.field(HIGH_WATERMARK_FIELD.getPreferredName(), highWatermark);
            builder.field(FLOOD_STAGE_WATERMARK_FIELD.getPreferredName(), floodStageWatermark);
            builder.field(FLOOD_STAGE_WATERMARK_FROZEN_FIELD.getPreferredName(), floodStageWatermarkFrozen);
            builder.field(YELLOW_THRESHOLD_FIELD.getPreferredName(), yellowThreshold);
            builder.field(RED_THRESHOLD_FIELD.getPreferredName(), redThreshold);
            return builder;
        }

        @Override
        public String getWriteableName() {
            return "disk_health_thresholds";
        }
    }
}
