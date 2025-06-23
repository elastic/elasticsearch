/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Objects;

public class TransformMetadata implements Metadata.ProjectCustom {
    public static final String TYPE = "transform";
    public static final ParseField RESET_MODE = new ParseField("reset_mode");
    public static final ParseField UPGRADE_MODE = new ParseField("upgrade_mode");

    public static final TransformMetadata EMPTY_METADATA = new TransformMetadata(false, false);
    // This parser follows the pattern that metadata is parsed leniently (to allow for enhancements)
    public static final ObjectParser<TransformMetadata.Builder, Void> LENIENT_PARSER = new ObjectParser<>(
        "" + "transform_metadata",
        true,
        TransformMetadata.Builder::new
    );

    static {
        LENIENT_PARSER.declareBoolean(TransformMetadata.Builder::resetMode, RESET_MODE);
        LENIENT_PARSER.declareBoolean(TransformMetadata.Builder::upgradeMode, UPGRADE_MODE);
    }

    private final boolean resetMode;
    private final boolean upgradeMode;

    private TransformMetadata(boolean resetMode, boolean upgradeMode) {
        this.resetMode = resetMode;
        this.upgradeMode = upgradeMode;
    }

    public boolean resetMode() {
        return resetMode;
    }

    public boolean upgradeMode() {
        return upgradeMode;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.MINIMUM_COMPATIBLE;
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
    public Diff<Metadata.ProjectCustom> diff(Metadata.ProjectCustom previousState) {
        return new TransformMetadata.TransformMetadataDiff((TransformMetadata) previousState, this);
    }

    public TransformMetadata(StreamInput in) throws IOException {
        this.resetMode = in.readBoolean();
        if (in.getTransportVersion().onOrAfter(TransportVersions.TRANSFORMS_UPGRADE_MODE)) {
            this.upgradeMode = in.readBoolean();
        } else {
            this.upgradeMode = false;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(resetMode);
        if (out.getTransportVersion().onOrAfter(TransportVersions.TRANSFORMS_UPGRADE_MODE)) {
            out.writeBoolean(upgradeMode);
        }
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.single(
            ((builder, params) -> builder.field(UPGRADE_MODE.getPreferredName(), upgradeMode)
                .field(RESET_MODE.getPreferredName(), resetMode))
        );
    }

    public static class TransformMetadataDiff implements NamedDiff<Metadata.ProjectCustom> {

        final boolean resetMode;
        final boolean upgradeMode;

        TransformMetadataDiff(TransformMetadata before, TransformMetadata after) {
            this.resetMode = after.resetMode;
            this.upgradeMode = after.upgradeMode;
        }

        public TransformMetadataDiff(StreamInput in) throws IOException {
            resetMode = in.readBoolean();
            if (in.getTransportVersion().onOrAfter(TransportVersions.TRANSFORMS_UPGRADE_MODE)) {
                this.upgradeMode = in.readBoolean();
            } else {
                this.upgradeMode = false;
            }
        }

        /**
         * Merge the diff with the transform metadata.
         * @param part The current transform metadata.
         * @return The new transform metadata.
         */
        @Override
        public Metadata.ProjectCustom apply(Metadata.ProjectCustom part) {
            return new TransformMetadata(resetMode, upgradeMode);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(resetMode);
            if (out.getTransportVersion().onOrAfter(TransportVersions.TRANSFORMS_UPGRADE_MODE)) {
                out.writeBoolean(upgradeMode);
            }
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.MINIMUM_COMPATIBLE;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransformMetadata that = (TransformMetadata) o;
        return resetMode == that.resetMode && upgradeMode == that.upgradeMode;
    }

    @Override
    public final String toString() {
        return Strings.toString(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resetMode, upgradeMode);
    }

    public Builder builder() {
        return new TransformMetadata.Builder(this);
    }

    public static class Builder {

        private boolean resetMode;
        private boolean upgradeMode;

        public static TransformMetadata.Builder from(@Nullable TransformMetadata previous) {
            return new TransformMetadata.Builder(previous);
        }

        public Builder() {}

        public Builder(@Nullable TransformMetadata previous) {
            if (previous != null) {
                resetMode = previous.resetMode;
                upgradeMode = previous.upgradeMode;
            }
        }

        public TransformMetadata.Builder resetMode(boolean isResetMode) {
            this.resetMode = isResetMode;
            return this;
        }

        public TransformMetadata.Builder upgradeMode(boolean upgradeMode) {
            this.upgradeMode = upgradeMode;
            return this;
        }

        public TransformMetadata build() {
            return new TransformMetadata(resetMode, upgradeMode);
        }
    }

    @Deprecated(forRemoval = true)
    public static TransformMetadata getTransformMetadata(ClusterState state) {
        TransformMetadata TransformMetadata = (state == null) ? null : state.metadata().getSingleProjectCustom(TYPE);
        if (TransformMetadata == null) {
            return EMPTY_METADATA;
        }
        return TransformMetadata;
    }

    public static boolean upgradeMode(ClusterState state) {
        return getTransformMetadata(state).upgradeMode();
    }
}
