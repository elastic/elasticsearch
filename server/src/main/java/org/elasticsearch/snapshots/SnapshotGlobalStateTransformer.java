/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.core.Nullable;

/**
 * SPI for transforming the global state written to and restored from snapshots. The transformation is applied
 * to the cluster {@link Metadata} just before it is written to the repository, and again to the restored
 * {@link Metadata} before it is applied to the cluster. Implementations live in plugins. Any number of plugins
 * may provide one; the transformers are chained at snapshot time and applied in reverse order on restore.
 */
public interface SnapshotGlobalStateTransformer {

    /**
     * Transforms the global state before it is written to the snapshot.
     *
     * <p>Implementations must return the same instance when they leave the state untouched. A different instance
     * signals that the state was transformed; when the request also supplied {@code encrypted_data}, the snapshot
     * records how the data is protected (surfaced to users as the {@code encrypted_data} object of the snapshot info).
     *
     * @param request the request that initiated the snapshot, or {@code null} when it is not available on this
     *                node (e.g. the snapshot was cloned, or the elected master changed while it was running)
     */
    default Metadata transformForSnapshot(ProjectId projectId, Metadata metadata, @Nullable CreateSnapshotRequest request) {
        return metadata;
    }

    /**
     * Transforms the restored global state before it is applied to the cluster.
     */
    default Metadata transformForRestore(ProjectId projectId, Metadata restored, RestoreSnapshotRequest request) {
        return restored;
    }
}
