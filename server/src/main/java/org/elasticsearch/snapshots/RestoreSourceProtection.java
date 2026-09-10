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

import java.util.Map;

/**
 * Extension point that keeps a snapshot protected from deletion while it is still the source of a restore, using evidence that outlives
 * the restore's {@link RestoreInProgress} entry.
 */
public interface RestoreSourceProtection {

    /**
     * Returns the snapshots in the given repository that must not be deleted because a restore from them may still be in flight, beyond
     * those already protected by an active {@link RestoreInProgress} entry. Called while resolving a batch of snapshot deletions, so it
     * must not block or perform I/O. If it throws, the whole batch is rejected and nothing is deleted.
     *
     * @param state          cluster state to resolve the protection against
     * @param projectId      project of the repository whose deletions are being resolved
     * @param repositoryName repository whose deletions are being resolved
     * @return the protected snapshots, each mapped to the UUID of the restore that protects it so that a rejected deletion can name it.
     */
    default Map<SnapshotId, String> protectedSnapshots(ClusterState state, ProjectId projectId, String repositoryName) {
        return Map.of();
    }

    /** No-op implementation used as the default before any protection is registered. */
    RestoreSourceProtection NOOP = new RestoreSourceProtection() {};
}
