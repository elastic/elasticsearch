/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.repositories.RepositoryData;

import java.util.Map;

/**
 * Extension point that keeps a snapshot protected from deletion while it is still the source of a restore, using evidence that outlives
 * the restore's {@link RestoreInProgress} entry.
 *
 * <p>An active {@link RestoreInProgress} entry is ordinarily what stops its source snapshot from being deleted. That protection ends the
 * moment the entry is removed, which happens before a recovery implementation has durably recorded the restore's outcome. Deleting the
 * source snapshot in that window would destroy the data a retry needs. An implementation closes the window by reporting the snapshot as
 * protected for as long as its own durable record says the restore has started but not yet finished.
 *
 * <p>Ordinary restores have no such durable record and rely on {@link RestoreInProgress} alone, so {@link #NOOP} leaves their behaviour
 * unchanged.
 */
public interface RestoreSourceProtection {

    /**
     * Returns the snapshots in the given repository that must not be deleted because a restore from them may still be in flight, beyond
     * those already protected by an active {@link RestoreInProgress} entry. Called while resolving a batch of snapshot deletions, so it
     * must not block or perform I/O.
     *
     * @param state          cluster state to resolve the protection against
     * @param projectId      project of the repository whose deletions are being resolved
     * @param repositoryName repository whose deletions are being resolved
     * @return the protected snapshots, each mapped to the UUID of the restore that protects it so that a rejected deletion can name it.
     *         Snapshots absent from {@link RepositoryData} are ignored, so an implementation need not filter them.
     *         Defaults to protecting nothing beyond {@link RestoreInProgress}.
     */
    default Map<SnapshotId, String> protectedSnapshots(ClusterState state, ProjectId projectId, String repositoryName) {
        return Map.of();
    }

    /** No-op implementation used as the default before any protection is registered. */
    RestoreSourceProtection NOOP = new RestoreSourceProtection() {};
}
