/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Non-secret metadata for cluster secrets (legacy).
 *
 * <p>This class is no longer written to cluster state. It is retained solely so that
 * upgraded nodes can still deserialize cluster state from non-upgraded masters during
 * a rolling upgrade. {@link ReservedClusterSecretsAction} actively removes it when present.
 *
 * TODO Delete this class and its {@code NamedWriteableRegistry} entries once all nodes have been upgraded (ES-13910).
 *
 * @deprecated No longer in use; retained for rolling-upgrade deserialization compatibility.
 */
@Deprecated
public class ClusterStateSecretsMetadata extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

    /**
     * The name for this data class
     *
     * <p>This name will be used to identify this {@link org.elasticsearch.common.io.stream.NamedWriteable} in cluster
     * state. See {@link #getWriteableName()}.
     */
    public static final String TYPE = "file_secure_settings_metadata";

    private final boolean success;
    private final long version;
    private final List<String> errorStackTrace;

    private ClusterStateSecretsMetadata(boolean success, long version, List<String> errorStackTrace) {
        this.success = success;
        this.version = version;
        this.errorStackTrace = errorStackTrace == null ? List.of() : errorStackTrace;
    }

    public ClusterStateSecretsMetadata(StreamInput in) throws IOException {
        this.success = in.readBoolean();
        this.version = in.readLong();
        this.errorStackTrace = in.readStringCollectionAsList();
    }

    public static ClusterStateSecretsMetadata createSuccessful(long version) {
        return new ClusterStateSecretsMetadata(true, version, List.of());
    }

    public static ClusterStateSecretsMetadata createError(long version, List<String> errorStackTrace) {
        return new ClusterStateSecretsMetadata(false, version, errorStackTrace);
    }

    public boolean isSuccess() {
        return success;
    }

    public long getVersion() {
        return version;
    }

    public List<String> getErrorStackTrace() {
        return errorStackTrace;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
        return Iterators.single((builder, params) -> {
            builder.field("success", this.success);
            builder.field("version", this.version);
            if (errorStackTrace.isEmpty() == false) {
                builder.stringListField("stack_trace", errorStackTrace);
            }
            return builder;
        });
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(success);
        out.writeLong(version);
        out.writeStringCollection(errorStackTrace);
    }

    public static NamedDiff<ClusterState.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(ClusterState.Custom.class, TYPE, in);
    }

    @Override
    public String toString() {
        return "ClusterStateSecretsMetadata{"
            + "success="
            + success
            + ", version="
            + version
            + ", errorStackTrace="
            + errorStackTrace
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterStateSecretsMetadata that = (ClusterStateSecretsMetadata) o;
        return success == that.success && version == that.version && Objects.equals(errorStackTrace, that.errorStackTrace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(success, errorStackTrace, version);
    }
}
