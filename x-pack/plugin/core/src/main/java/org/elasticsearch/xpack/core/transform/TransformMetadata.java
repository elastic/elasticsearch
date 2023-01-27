/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Objects;

public class TransformMetadata implements Metadata.Custom {
    public static final String TYPE = "transform";
    public static final ParseField RESET_MODE = new ParseField("reset_mode");

    public static final TransformMetadata EMPTY_METADATA = new TransformMetadata(false);
    // This parser follows the pattern that metadata is parsed leniently (to allow for enhancements)
    public static final ObjectParser<TransformMetadata.Builder, Void> LENIENT_PARSER = new ObjectParser<>(
        "" + "transform_metadata",
        true,
        TransformMetadata.Builder::new
    );

    static {
        LENIENT_PARSER.declareBoolean(TransformMetadata.Builder::isResetMode, RESET_MODE);
    }

    private final boolean resetMode;

    private TransformMetadata(boolean resetMode) {
        this.resetMode = resetMode;
    }

    public boolean isResetMode() {
        return resetMode;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.CURRENT.minimumCompatibilityVersion();
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
        return new TransformMetadata.TransformMetadataDiff((TransformMetadata) previousState, this);
    }

    public TransformMetadata(StreamInput in) throws IOException {
        this.resetMode = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(resetMode);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return ChunkedToXContentHelper.field(RESET_MODE.getPreferredName(), resetMode);
    }

    public static class TransformMetadataDiff implements NamedDiff<Metadata.Custom> {

        final boolean resetMode;

        TransformMetadataDiff(TransformMetadata before, TransformMetadata after) {
            this.resetMode = after.resetMode;
        }

        public TransformMetadataDiff(StreamInput in) throws IOException {
            resetMode = in.readBoolean();
        }

        /**
         * Merge the diff with the transform metadata.
         * @param part The current transform metadata.
         * @return The new transform metadata.
         */
        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new TransformMetadata(resetMode);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(resetMode);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.CURRENT.minimumCompatibilityVersion();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransformMetadata that = (TransformMetadata) o;
        return resetMode == that.resetMode;
    }

    @Override
    public final String toString() {
        return Strings.toString(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resetMode);
    }

    public static class Builder {

        private boolean resetMode;

        public static TransformMetadata.Builder from(@Nullable TransformMetadata previous) {
            return new TransformMetadata.Builder(previous);
        }

        public Builder() {}

        public Builder(@Nullable TransformMetadata previous) {
            if (previous != null) {
                resetMode = previous.resetMode;
            }
        }

        public TransformMetadata.Builder isResetMode(boolean isResetMode) {
            this.resetMode = isResetMode;
            return this;
        }

        public TransformMetadata build() {
            return new TransformMetadata(resetMode);
        }
    }

    public static TransformMetadata getTransformMetadata(ClusterState state) {
        TransformMetadata TransformMetadata = (state == null) ? null : state.getMetadata().custom(TYPE);
        if (TransformMetadata == null) {
            return EMPTY_METADATA;
        }
        return TransformMetadata;
    }
}
