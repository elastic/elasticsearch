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
 * Told the bytes a shard retains in a repository each time a blob store repository writes the shard's {@code index-<generation>} blob.
 * Loaded through SPI, at most one implementation, and called for every repository, so it must ignore the ones it does not need.
 * <p>
 * The call is made before the generation is committed to {@link RepositoryData}, so it may never become current: only trust the
 * generations that {@link RepositoryData#shardGenerations()} reports. Implementations must be cheap and must not block, and an
 * exception is logged and ignored.
 */
@FunctionalInterface
public interface ShardGenerationSizeListener {

    ShardGenerationSizeListener NOOP = (projectRepo, indexId, shardId, generation, retainedBytes) -> {};

    /**
     * @param projectRepo   the repository that wrote the blob
     * @param indexId       the repository index id, not the index name
     * @param shardId       the shard number
     * @param generation    the generation that was written
     * @param retainedBytes the length of the data blobs the shard's snapshots reference, counting a shared blob once
     */
    void onShardGenerationSize(ProjectRepo projectRepo, IndexId indexId, int shardId, ShardGeneration generation, long retainedBytes);
}
