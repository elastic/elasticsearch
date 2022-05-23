/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;

public class MlMetadata implements Metadata.Custom {

    public static final String TYPE = "ml";
    public static final ParseField UPGRADE_MODE = new ParseField("upgrade_mode");
    public static final ParseField RESET_MODE = new ParseField("reset_mode");
    public static final ParseField MAX_ML_NODE_SEEN = new ParseField("max_ml_node_seen");
    public static final ParseField CPU_RATIO = new ParseField("cpu_ratio");

    public static final MlMetadata EMPTY_METADATA = new MlMetadata(false, false, null, null);
    // This parser follows the pattern that metadata is parsed leniently (to allow for enhancements)
    public static final ObjectParser<Builder, Void> LENIENT_PARSER = new ObjectParser<>("ml_metadata", true, Builder::new);

    static {
        LENIENT_PARSER.declareBoolean(Builder::isUpgradeMode, UPGRADE_MODE);
        LENIENT_PARSER.declareBoolean(Builder::isResetMode, RESET_MODE);
        LENIENT_PARSER.declareLong(Builder::setMaxMlNodeSeen, MAX_ML_NODE_SEEN);
        LENIENT_PARSER.declareDouble(Builder::setCpuRatio, CPU_RATIO);
    }

    private final boolean upgradeMode;
    private final boolean resetMode;
    private final long maxMlNodeSeen;
    private final double cpuRatio;

    private MlMetadata(boolean upgradeMode, boolean resetMode, Long maxMlNodeSeen, Double cpuRatio) {
        this.upgradeMode = upgradeMode;
        this.resetMode = resetMode;
        this.maxMlNodeSeen = Optional.ofNullable(maxMlNodeSeen).orElse(-1L);
        this.cpuRatio = Optional.ofNullable(cpuRatio).orElse(0.0);
    }

    public boolean isUpgradeMode() {
        return upgradeMode;
    }

    public boolean isResetMode() {
        return resetMode;
    }

    public long getMaxMlNodeSeen() {
        return maxMlNodeSeen;
    }

    public double getCpuRatio() {
        return cpuRatio;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
        return new MlMetadataDiff((MlMetadata) previousState, this);
    }

    public MlMetadata(StreamInput in) throws IOException {
        if (in.getVersion().before(Version.V_8_0_0)) {
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                in.readString();
                new Job(in);
            }
            size = in.readVInt();
            for (int i = 0; i < size; i++) {
                in.readString();
                new DatafeedConfig(in);
            }
        }
        this.upgradeMode = in.readBoolean();
        this.resetMode = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_8_3_0)) {
            this.maxMlNodeSeen = in.readLong();
            this.cpuRatio = in.readDouble();
        } else {
            this.maxMlNodeSeen = -1;
            this.cpuRatio = 0.0;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_8_0_0)) {
            writeMap(Collections.emptySortedMap(), out);
            writeMap(Collections.emptySortedMap(), out);
        }
        out.writeBoolean(upgradeMode);
        out.writeBoolean(resetMode);
        if (out.getVersion().onOrAfter(Version.V_8_3_0)) {
            out.writeLong(maxMlNodeSeen);
            out.writeDouble(cpuRatio);
        }
    }

    private static <T extends Writeable> void writeMap(Map<String, T> map, StreamOutput out) throws IOException {
        out.writeVInt(map.size());
        for (Map.Entry<String, T> entry : map.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(UPGRADE_MODE.getPreferredName(), upgradeMode);
        builder.field(RESET_MODE.getPreferredName(), resetMode);
        if (maxMlNodeSeen > 0) {
            builder.field(MAX_ML_NODE_SEEN.getPreferredName(), maxMlNodeSeen);
            builder.field(CPU_RATIO.getPreferredName(), cpuRatio);
        }
        return builder;
    }

    public static class MlMetadataDiff implements NamedDiff<Metadata.Custom> {

        final boolean upgradeMode;
        final boolean resetMode;
        final long maxMlNodeSeen;
        final double cpuRatio;

        MlMetadataDiff(MlMetadata before, MlMetadata after) {
            this.upgradeMode = after.upgradeMode;
            this.resetMode = after.resetMode;
            this.maxMlNodeSeen = after.maxMlNodeSeen;
            this.cpuRatio = after.cpuRatio;
        }

        public MlMetadataDiff(StreamInput in) throws IOException {
            if (in.getVersion().before(Version.V_8_0_0)) {
                DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), Job::new, MlMetadataDiff::readJobDiffFrom);
                DiffableUtils.readJdkMapDiff(
                    in,
                    DiffableUtils.getStringKeySerializer(),
                    DatafeedConfig::new,
                    MlMetadataDiff::readDatafeedDiffFrom
                );
            }
            upgradeMode = in.readBoolean();
            resetMode = in.readBoolean();
            if (in.getVersion().onOrAfter(Version.V_8_3_0)) {
                this.maxMlNodeSeen = in.readLong();
                this.cpuRatio = in.readDouble();
            } else {
                this.maxMlNodeSeen = -1;
                this.cpuRatio = 0.0;
            }
        }

        /**
         * Merge the diff with the ML metadata.
         * @param part The current ML metadata.
         * @return The new ML metadata.
         */
        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new MlMetadata(upgradeMode, resetMode, maxMlNodeSeen, cpuRatio);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().before(Version.V_8_0_0)) {
                SortedMap<String, Job> jobs = Collections.emptySortedMap();
                DiffableUtils.diff(jobs, jobs, DiffableUtils.getStringKeySerializer()).writeTo(out);
                SortedMap<String, DatafeedConfig> datafeeds = Collections.emptySortedMap();
                DiffableUtils.diff(datafeeds, datafeeds, DiffableUtils.getStringKeySerializer()).writeTo(out);
            }
            out.writeBoolean(upgradeMode);
            out.writeBoolean(resetMode);
            if (out.getVersion().onOrAfter(Version.V_8_3_0)) {
                out.writeLong(maxMlNodeSeen);
                out.writeDouble(cpuRatio);
            }
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT.minimumCompatibilityVersion();
        }

        static Diff<Job> readJobDiffFrom(StreamInput in) throws IOException {
            return SimpleDiffable.readDiffFrom(Job::new, in);
        }

        static Diff<DatafeedConfig> readDatafeedDiffFrom(StreamInput in) throws IOException {
            return SimpleDiffable.readDiffFrom(DatafeedConfig::new, in);
        }
    }

    @Override
    public final String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MlMetadata that = (MlMetadata) o;
        return upgradeMode == that.upgradeMode
            && resetMode == that.resetMode
            && maxMlNodeSeen == that.maxMlNodeSeen
            && Double.compare(that.cpuRatio, cpuRatio) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(upgradeMode, resetMode, maxMlNodeSeen, cpuRatio);
    }

    public static class Builder {

        private boolean upgradeMode;
        private boolean resetMode;
        private Long maxMlNodeSeen;
        private Double cpuRatio;

        public static Builder from(@Nullable MlMetadata previous) {
            return new Builder(previous);
        }

        public Builder() {}

        public Builder(@Nullable MlMetadata previous) {
            if (previous != null) {
                upgradeMode = previous.upgradeMode;
                resetMode = previous.resetMode;
                maxMlNodeSeen = previous.maxMlNodeSeen;
                cpuRatio = previous.cpuRatio;
            }
        }

        public Builder isUpgradeMode(boolean isUpgradeMode) {
            this.upgradeMode = isUpgradeMode;
            return this;
        }

        public Builder isResetMode(boolean isResetMode) {
            this.resetMode = isResetMode;
            return this;
        }

        public Builder setMaxMlNodeSeen(Long maxMlNodeSeen) {
            this.maxMlNodeSeen = maxMlNodeSeen;
            return this;
        }

        public long getMaxMlNodeSeen() {
            return Optional.ofNullable(maxMlNodeSeen).orElse(-1L);
        }

        public Builder setCpuRatio(Double cpuRatio) {
            this.cpuRatio = cpuRatio;
            return this;
        }

        public MlMetadata build() {
            return new MlMetadata(upgradeMode, resetMode, maxMlNodeSeen, cpuRatio);
        }
    }

    public static MlMetadata getMlMetadata(ClusterState state) {
        MlMetadata mlMetadata = (state == null) ? null : state.getMetadata().custom(TYPE);
        if (mlMetadata == null) {
            return EMPTY_METADATA;
        }
        return mlMetadata;
    }
}
