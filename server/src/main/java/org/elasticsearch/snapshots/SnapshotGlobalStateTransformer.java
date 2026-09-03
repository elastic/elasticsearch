/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.core.Nullable;

/**
 * SPI hook called just before the global-metadata blob ({@code meta-<uuid>.dat}) is written
 * during snapshot creation. Implementors can transform the {@link Metadata} to re-wrap
 * or remove project-scoped encrypted data.
 *
 * <p>Contract: return the <em>same</em> {@link Metadata} instance if no transformation is
 * needed; a different instance signals that the state was modified. Returning the same instance
 * avoids unnecessary serialisation work.
 *
 * <p>Providers are discovered via {@code pluginsService.loadServiceProviders} in
 * {@code NodeConstruction} and handed to both {@link SnapshotsService} (for use during
 * finalization) and {@link org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction}
 * (for pre-flight checks).
 *
 * <p>There is no {@code transformForRestore} counterpart — that is a follow-up implementation.
 */
public interface SnapshotGlobalStateTransformer {

    /**
     * Transforms {@code metadata} before it is written into the snapshot blob. Called on the
     * SNAPSHOT thread pool during finalization, after all shard snapshots have completed.
     *
     * @param projectId     the project being snapshotted
     * @param metadata      current project metadata
     * @param encryptedData the {@code encrypted_data} block from the original create-snapshot
     *                      request, or {@code null} after a master failover (in which case the
     *                      password is unavailable and encrypted data should be excluded)
     * @return the metadata to write — same instance if no change is needed
     */
    default Metadata transformForSnapshot(ProjectId projectId, Metadata metadata, @Nullable SnapshotEncryptedData encryptedData) {
        return metadata;
    }

    /**
     * Returns {@code true} if the project metadata contains data that this transformer would
     * re-wrap or exclude during snapshot creation. Used by
     * {@link org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction}
     * to decide whether to emit a {@code Warning} response header when no {@code encrypted_data}
     * was supplied with the request.
     *
     * @param projectId the project being snapshotted
     * @param metadata  current project metadata
     */
    default boolean containsEncryptedData(ProjectId projectId, Metadata metadata) {
        return false;
    }
}
