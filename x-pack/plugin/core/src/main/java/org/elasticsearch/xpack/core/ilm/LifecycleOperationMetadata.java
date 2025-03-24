/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;

/**
 * Class that encapsulates the running operation mode of Index Lifecycle
 * Management and Snapshot Lifecycle Management
 */
public class LifecycleOperationMetadata implements Metadata.ProjectCustom {
    public static final String TYPE = "lifecycle_operation";
    public static final ParseField ILM_OPERATION_MODE_FIELD = new ParseField("ilm_operation_mode");
    public static final ParseField SLM_OPERATION_MODE_FIELD = new ParseField("slm_operation_mode");
    public static final LifecycleOperationMetadata EMPTY = new LifecycleOperationMetadata(OperationMode.RUNNING, OperationMode.RUNNING);

    public static final ConstructingObjectParser<LifecycleOperationMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        a -> new LifecycleOperationMetadata(OperationMode.valueOf((String) a[0]), OperationMode.valueOf((String) a[1]))
    );
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ILM_OPERATION_MODE_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SLM_OPERATION_MODE_FIELD);
    }

    private final OperationMode ilmOperationMode;
    private final OperationMode slmOperationMode;

    public LifecycleOperationMetadata(OperationMode ilmOperationMode, OperationMode slmOperationMode) {
        this.ilmOperationMode = ilmOperationMode;
        this.slmOperationMode = slmOperationMode;
    }

    public LifecycleOperationMetadata(StreamInput in) throws IOException {
        this.ilmOperationMode = in.readEnum(OperationMode.class);
        this.slmOperationMode = in.readEnum(OperationMode.class);
    }

    /**
     * Returns the current ILM mode based on the given cluster state. It first checks the newer
     * storage mechanism ({@link LifecycleOperationMetadata#getILMOperationMode()}) before falling
     * back to {@link IndexLifecycleMetadata#getOperationMode()}. If neither exist, the default
     * value for an empty state is used.
     */
    @SuppressWarnings("deprecated")
    public static OperationMode currentILMMode(final ProjectMetadata projectMetadata) {
        IndexLifecycleMetadata oldMetadata = projectMetadata.custom(IndexLifecycleMetadata.TYPE);
        LifecycleOperationMetadata currentMetadata = projectMetadata.custom(LifecycleOperationMetadata.TYPE);
        return Optional.ofNullable(currentMetadata)
            .map(LifecycleOperationMetadata::getILMOperationMode)
            .orElse(
                Optional.ofNullable(oldMetadata)
                    .map(IndexLifecycleMetadata::getOperationMode)
                    .orElseGet(LifecycleOperationMetadata.EMPTY::getILMOperationMode)
            );
    }

    /**
     * Returns the current ILM mode based on the given cluster state. It first checks the newer
     * storage mechanism ({@link LifecycleOperationMetadata#getSLMOperationMode()}) before falling
     * back to {@link SnapshotLifecycleMetadata#getOperationMode()}. If neither exist, the default
     * value for an empty state is used.
     */
    @SuppressWarnings("deprecated")
    public static OperationMode currentSLMMode(final ClusterState state) {
        SnapshotLifecycleMetadata oldMetadata = state.metadata().getProject().custom(SnapshotLifecycleMetadata.TYPE);
        LifecycleOperationMetadata currentMetadata = state.metadata().getProject().custom(LifecycleOperationMetadata.TYPE);
        return Optional.ofNullable(currentMetadata)
            .map(LifecycleOperationMetadata::getSLMOperationMode)
            .orElse(
                Optional.ofNullable(oldMetadata)
                    .map(SnapshotLifecycleMetadata::getOperationMode)
                    .orElseGet(LifecycleOperationMetadata.EMPTY::getSLMOperationMode)
            );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(ilmOperationMode);
        out.writeEnum(slmOperationMode);
    }

    public OperationMode getILMOperationMode() {
        return ilmOperationMode;
    }

    public OperationMode getSLMOperationMode() {
        return slmOperationMode;
    }

    @Override
    public Diff<Metadata.ProjectCustom> diff(Metadata.ProjectCustom previousState) {
        return new LifecycleOperationMetadata.LifecycleOperationMetadataDiff((LifecycleOperationMetadata) previousState, this);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        ToXContent ilmModeField = ((builder, params2) -> builder.field(ILM_OPERATION_MODE_FIELD.getPreferredName(), ilmOperationMode));
        ToXContent slmModeField = ((builder, params2) -> builder.field(SLM_OPERATION_MODE_FIELD.getPreferredName(), slmOperationMode));
        return Iterators.forArray(new ToXContent[] { ilmModeField, slmModeField });
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_7_0;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        // We do not store the lifecycle operation mode for ILM and SLM into a snapshot. This is so
        // ILM and SLM can be operated independently from restoring snapshots.
        return EnumSet.of(Metadata.XContentContext.API, Metadata.XContentContext.GATEWAY);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ilmOperationMode, slmOperationMode);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        LifecycleOperationMetadata other = (LifecycleOperationMetadata) obj;
        return Objects.equals(ilmOperationMode, other.ilmOperationMode) && Objects.equals(slmOperationMode, other.slmOperationMode);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static class LifecycleOperationMetadataDiff implements NamedDiff<Metadata.ProjectCustom> {

        final OperationMode ilmOperationMode;
        final OperationMode slmOperationMode;

        LifecycleOperationMetadataDiff(LifecycleOperationMetadata before, LifecycleOperationMetadata after) {
            this.ilmOperationMode = after.ilmOperationMode;
            this.slmOperationMode = after.slmOperationMode;
        }

        public LifecycleOperationMetadataDiff(StreamInput in) throws IOException {
            this.ilmOperationMode = in.readEnum(OperationMode.class);
            this.slmOperationMode = in.readEnum(OperationMode.class);
        }

        @Override
        public Metadata.ProjectCustom apply(Metadata.ProjectCustom part) {
            return new LifecycleOperationMetadata(this.ilmOperationMode, this.slmOperationMode);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(ilmOperationMode);
            out.writeEnum(slmOperationMode);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_7_0;
        }
    }
}
