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
 * SPI hook that lets a plugin transform the global metadata written into a snapshot, e.g. to re-wrap or drop encrypted project data.
 * At most one implementation may be installed.
 */
public interface SnapshotGlobalStateTransformer {

    SnapshotGlobalStateTransformer NOOP = new SnapshotGlobalStateTransformer() {};

    /**
     * Called on the SNAPSHOT thread pool during finalization, before the global-metadata blob is written.
     *
     * @param encryptedData the validated {@code encrypted_data} of this snapshot, or {@code null} if none was requested, in which case
     *                      encrypted data must be excluded
     * @return the metadata to write, or the same instance if unchanged
     */
    default Metadata transformForSnapshot(ProjectId projectId, Metadata metadata, @Nullable SnapshotEncryptedData encryptedData) {
        return metadata;
    }

    /**
     * Returns {@code true} if the project contains data this transformer would re-wrap or exclude.
     */
    default boolean containsEncryptedData(ProjectId projectId, Metadata metadata) {
        return false;
    }
}
