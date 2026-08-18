/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.util.Objects;

abstract class AbstractStatelessRecoveryListener {

    protected final ObjectStoreService objectStoreService;
    private final ProjectResolver projectResolver;

    AbstractStatelessRecoveryListener(final ObjectStoreService objectStoreService, final ProjectResolver projectResolver) {
        this.objectStoreService = Objects.requireNonNull(objectStoreService);
        this.projectResolver = Objects.requireNonNull(projectResolver);
    }

    BlobContainer initializeBlobContainer(final IndexShard indexShard, final Store store) {
        final var projectId = projectResolver.getProjectId();
        final var shardId = indexShard.shardId();
        assert objectStoreService.assertProjectIdAndShardIdConsistency(projectId, shardId);

        final BlobStore blobStore = objectStoreService.getProjectBlobStore(projectId);
        final BlobPath shardBasePath = objectStoreService.shardBasePath(projectId, shardId);
        final BlobContainer existingBlobContainer = hasNoExistingBlobContainer(indexShard.recoveryState().getRecoverySource())
            ? null
            : blobStore.blobContainer(shardBasePath);

        BlobStoreCacheDirectory.unwrapDirectory(store.directory())
            .setBlobContainer(primaryTerm -> blobStore.blobContainer(shardBasePath.add(String.valueOf(primaryTerm))));
        return existingBlobContainer;
    }

    private static boolean hasNoExistingBlobContainer(RecoverySource recoverySource) {
        return recoverySource == RecoverySource.EmptyStoreRecoverySource.INSTANCE
            || recoverySource instanceof RecoverySource.SnapshotRecoverySource
            || recoverySource == RecoverySource.LocalShardsRecoverySource.INSTANCE;
    }

    static void logBootstrappingFromObjectStore(Logger logger, IndexShard indexShard, BatchedCompoundCommit latestCommit) {
        logger.info(
            "{} with UUID [{}] bootstrapping [{}] shard on primary term [{}] with {} from object store ({})",
            indexShard.shardId(),
            indexShard.shardId().getIndex().getUUID(),
            indexShard.routingEntry().role(),
            indexShard.getOperationPrimaryTerm(),
            latestCommit != null ? latestCommit.lastCompoundCommit().toShortDescription() : "empty commit",
            describe(indexShard.recoveryState())
        );
    }

    static String describe(RecoveryState recoveryState) {
        return recoveryState.getRecoverySource() == RecoverySource.PeerRecoverySource.INSTANCE
            ? recoveryState.getRecoverySource() + " from " + recoveryState.getSourceNode().getName()
            : recoveryState.getRecoverySource().toString();
    }
}
