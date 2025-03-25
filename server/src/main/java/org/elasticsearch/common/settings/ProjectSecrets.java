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
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Objects;

/**
 * Secrets that are stored in project state as a {@link Metadata.ProjectCustom}
 *
 * <p>Project state secrets are initially loaded on the master node, from a file on disk.
 * Once the cluster is running, the master node watches the file for changes. This class
 * propagates changes in the file-based secure settings for each project from the master
 * node out to other nodes using the transport protocol.
 *
 * <p>Since the master node should always have settings on disk, we don't need to
 * persist this class to saved cluster state, either on disk or in the cloud. Therefore,
 * we have defined this {@link Metadata.ProjectCustom} as a "private custom" object by not
 * serializing its content in {@link #toXContentChunked(ToXContent.Params)}.
 */
public class ProjectSecrets extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {

    public static final String TYPE = "project_state_secrets";

    private final SecureClusterStateSettings settings;

    public ProjectSecrets(SecureClusterStateSettings settings) {
        this.settings = settings;
    }

    public ProjectSecrets(StreamInput in) throws IOException {
        this.settings = new SecureClusterStateSettings(in);
    }

    public SecureSettings getSettings() {
        return new SecureClusterStateSettings(settings);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        // No need to persist in index or return to user, so do not serialize the secrets
        return Collections.emptyIterator();
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.MULTI_PROJECT;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        settings.writeTo(out);
    }

    public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.ProjectCustom.class, TYPE, in);
    }

    @Override
    public String toString() {
        return "ProjectSecrets{[all secret]}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProjectSecrets that = (ProjectSecrets) o;
        return Objects.equals(settings, that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(settings);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return EnumSet.noneOf(Metadata.XContentContext.class);
    }
}
