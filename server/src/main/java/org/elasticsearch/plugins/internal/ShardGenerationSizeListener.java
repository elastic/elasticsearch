/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;

/**
 * Reports the bytes that a shard retains in a repository each time a
 * {@link org.elasticsearch.repositories.blobstore.BlobStoreRepository} writes the shard's {@code index-<generation>} blob, so that a
 * plugin can record them once per generation and keep them durably, instead of re-reading the repository later.
 * <p>
 * It is loaded through SPI, and at most one implementation may be installed. A plugin provides it with a {@code provides} declaration in
 * its {@code module-info}; the implementation may take the plugin as its only constructor argument. It is called for every repository of
 * every project, so it must return at once for the ones it has no use for.
 * <p>
 * The report is made right after the blob is written, before any data file of the shard snapshot is uploaded and before the generation is
 * committed to {@link RepositoryData}. A written generation may therefore never be committed (the snapshot or deletion can fail
 * afterwards); consumers must only trust the generations that {@link RepositoryData#shardGenerations()} reports. Implementations must be
 * cheap and must not block, and any exception they throw is logged and ignored so that a listener can never fail or delay a snapshot or
 * a snapshot deletion.
 * <p>
 * The listener receives a number and not the shard's snapshot history on purpose: the history can be large, and a listener that held it
 * would keep it on the heap for as long as it did.
 */
@FunctionalInterface
public interface ShardGenerationSizeListener {

    ShardGenerationSizeListener NOOP = (projectRepo, indexId, shardId, generation, retainedBytes) -> {};

    /**
     * @param projectRepo   the repository that wrote the blob
     * @param indexId       the repository index id (not the index name)
     * @param shardId       the shard number
     * @param generation    the generation that was written; its blob never changes
     * @param retainedBytes the total length of the data blobs that the snapshots in the blob reference, counting a blob that several
     *                      snapshots share once
     */
    void onShardGenerationSize(ProjectRepo projectRepo, IndexId indexId, int shardId, ShardGeneration generation, long retainedBytes);
}
