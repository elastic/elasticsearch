/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Secrets that are stored in cluster state
 *
 * <p>Cluster state secrets are initially loaded on each node, from a file on disk,
 * in the format defined by {@link org.elasticsearch.common.settings.LocallyMountedSecrets}.
 * Once the cluster is running, the master node watches the file for changes. This class
 * propagates changes in the file-based secure settings from the master node out to other
 * nodes.
 *
 * <p>Since the master node should always have settings on disk, we don't need to
 * persist this class to saved cluster state, either on disk or in the cloud. Therefore,
 * we have defined this {@link ClusterState.Custom} as a private custom object. Additionally,
 * we don't want to ever write this class's secrets out in a client response, so
 * {@link #toXContentChunked(ToXContent.Params)} returns an empty iterator.
 */
public class ClusterSecrets extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

    /**
     * The name for this data class
     *
     * <p>This name will be used to identify this {@link org.elasticsearch.common.io.stream.NamedWriteable} in cluster
     * state. See {@link #getWriteableName()}.
     */
    public static final String TYPE = "cluster_state_secrets";

    private final SecureClusterStateSettings settings;
    private final long version;

    public ClusterSecrets(long version, SecureClusterStateSettings settings) {
        this.version = version;
        this.settings = settings;
    }

    public ClusterSecrets(StreamInput in) throws IOException {
        this.version = in.readLong();
        this.settings = new SecureClusterStateSettings(in);
    }

    public SecureSettings getSettings() {
        return new SecureClusterStateSettings(settings);
    }

    public long getVersion() {
        return version;
    }

    @Override
    public boolean isPrivate() {
        return true;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        // never render this to the user
        return Collections.emptyIterator();
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_9_X;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
        settings.writeTo(out);
    }

    public static NamedDiff<ClusterState.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(ClusterState.Custom.class, TYPE, in);
    }

    @Override
    public String toString() {
        return "ClusterStateSecrets{[all secret]}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterSecrets that = (ClusterSecrets) o;
        return version == that.version && Objects.equals(settings, that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(settings, version);
    }
}
