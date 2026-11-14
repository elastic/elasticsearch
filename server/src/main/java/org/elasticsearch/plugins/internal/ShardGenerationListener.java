/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;

/**
 * Observes the immutable per-shard {@code index-<generation>} blobs that a
 * {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository} writes, so that a plugin can derive facts from the file list
 * (for example the bytes the shard retains) once per generation and record them durably, instead of re-reading the repository later.
 * <p>
 * It is loaded through SPI, and at most one implementation may be installed. A plugin provides it with a {@code provides} declaration in
 * its {@code module-info}; the implementation may take the plugin as its only constructor argument. The listener is called for every
 * repository of every project, so it must filter on the repository itself.
 * <p>
 * The callback fires after the blob is fully written, before the generation is committed to {@link RepositoryData}. A written generation
 * may therefore never be committed (the snapshot or deletion can fail afterwards); consumers must only trust the generations that
 * {@link RepositoryData#shardGenerations()} reports. Implementations must be cheap and must not block, and any exception they throw is
 * logged and ignored so that a listener can never fail or delay a snapshot or a snapshot deletion.
 */
public interface ShardGenerationListener {

    ShardGenerationListener NOOP = (projectRepo, indexId, shardId, generation, shardSnapshots) -> {};

    /**
     * @param projectRepo    the repository that wrote the blob
     * @param indexId        the repository index id (not the index name)
     * @param shardId        the shard number
     * @param generation     the generation under which {@code shardSnapshots} was written; it never changes after this call
     * @param shardSnapshots every snapshot's file list that is stored in the {@code index-<generation>} blob
     */
    void onShardGenerationWritten(
        ProjectRepo projectRepo,
        IndexId indexId,
        int shardId,
        ShardGeneration generation,
        BlobStoreIndexShardSnapshots shardSnapshots
    );
}
