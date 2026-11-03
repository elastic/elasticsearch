/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The {@link SnapshotEncryptedData} of in-progress snapshots, keyed by snapshot UUID, so that any master can finalize them. Kept apart
 * from {@link SnapshotsInProgress} because {@link #isPrivate()} keeps this custom, and so the password, out of cluster state responses,
 * e.g. to remote clusters.
 */
public final class SnapshotsInProgressEncryptedData extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

    public static final String TYPE = "snapshots_in_progress_encrypted_data";

    public static final SnapshotsInProgressEncryptedData EMPTY = new SnapshotsInProgressEncryptedData(Map.of());

    private final Map<String, SnapshotEncryptedData> encryptedData;

    private SnapshotsInProgressEncryptedData(Map<String, SnapshotEncryptedData> encryptedData) {
        this.encryptedData = Map.copyOf(encryptedData);
    }

    public SnapshotsInProgressEncryptedData(StreamInput in) throws IOException {
        this(in.readImmutableMap(SnapshotEncryptedData::new));
    }

    public static SnapshotsInProgressEncryptedData get(ClusterState state) {
        return state.custom(TYPE, EMPTY);
    }

    @Nullable
    public SnapshotEncryptedData get(Snapshot snapshot) {
        return encryptedData.get(snapshot.getSnapshotId().getUUID());
    }

    public SnapshotsInProgressEncryptedData withAddedEntries(Map<Snapshot, SnapshotEncryptedData> added) {
        if (added.isEmpty()) {
            return this;
        }
        final Map<String, SnapshotEncryptedData> updated = new HashMap<>(encryptedData);
        // copy the password so closing the request's instance can't close the one finalization needs
        added.forEach(
            (snapshot, data) -> updated.put(
                snapshot.getSnapshotId().getUUID(),
                new SnapshotEncryptedData(data.type(), data.password() == null ? null : data.password().clone(), data.passwordId())
            )
        );
        return new SnapshotsInProgressEncryptedData(updated);
    }

    /**
     * Drops the entries of snapshots no longer in {@code snapshotsInProgress}, including those removed by paths other than finalization.
     */
    public SnapshotsInProgressEncryptedData retainRunning(SnapshotsInProgress snapshotsInProgress) {
        if (encryptedData.isEmpty()) {
            return this;
        }
        final Set<String> running = snapshotsInProgress.asStream()
            .map(entry -> entry.snapshot().getSnapshotId().getUUID())
            .collect(Collectors.toSet());
        if (running.containsAll(encryptedData.keySet())) {
            return this;
        }
        final Map<String, SnapshotEncryptedData> updated = new HashMap<>(encryptedData);
        updated.keySet().retainAll(running);
        return new SnapshotsInProgressEncryptedData(updated);
    }

    @Override
    public boolean isPrivate() {
        return true;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return SnapshotEncryptedData.TRANSPORT_VERSION;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(encryptedData, StreamOutput::writeWriteable);
    }

    public static NamedDiff<ClusterState.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(ClusterState.Custom.class, TYPE, in);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Collections.emptyIterator();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof SnapshotsInProgressEncryptedData other && encryptedData.equals(other.encryptedData);
    }

    @Override
    public int hashCode() {
        return encryptedData.hashCode();
    }

    @Override
    public String toString() {
        return "SnapshotsInProgressEncryptedData{snapshots=" + encryptedData.keySet() + "}";
    }
}
