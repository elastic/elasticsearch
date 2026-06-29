/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import java.io.IOException;

/**
 * Abstraction over the cloud provider's volume snapshot API. One implementation exists per cloud
 * provider (AWS EBS, GCP Persistent Disk, Azure Managed Disk). The active implementation is
 * resolved at node startup and injected into {@link CacheSnapshotService}.
 */
public interface CloudVolumeSnapshotProvider {

    /**
     * Initiates a snapshot of the volume that backs the cache file. Returns the provider-assigned
     * snapshot ID once the snapshot request has been accepted (the actual data copy may still be
     * in progress in the background on the cloud side).
     *
     * @return the provider-assigned snapshot ID
     * @throws IOException if the snapshot request fails
     */
    String createSnapshot() throws IOException;

    /**
     * Stub implementation for environments where cloud volume snapshots are not supported.
     * Returns a fixed token and logs a warning. Used as a placeholder until a real provider
     * is configured.
     */
    class NoOp implements CloudVolumeSnapshotProvider {
        @Override
        public String createSnapshot() throws IOException {
            throw new IOException("no CloudVolumeSnapshotProvider is configured for this node");
        }
    }
}
