/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;

import java.util.List;
import java.util.Objects;

class StatelessIndexingOperationListener implements IndexingOperationListener {

    private final HollowShardsService hollowShardsService;

    StatelessIndexingOperationListener(HollowShardsService hollowShardsService) {
        this.hollowShardsService = Objects.requireNonNull(hollowShardsService);
    }

    @Override
    public Engine.Delete preDelete(ShardId shardId, Engine.Delete delete) {
        hollowShardsService.ensureHollowShard(shardId, false);
        return IndexingOperationListener.super.preDelete(shardId, delete);
    }

    @Override
    public Engine.Index preIndex(ShardId shardId, Engine.Index operation) {
        hollowShardsService.ensureHollowShard(shardId, false);
        return IndexingOperationListener.super.preIndex(shardId, operation);
    }

    @Override
    public IndexOperationBatch preIndexBatch(ShardId shardId, IndexOperationBatch batch) {
        hollowShardsService.ensureHollowShard(shardId, false);
        // Cannot call `IndexingOperationListener.super.preIndexBatch` since that will materialize the batch
        return batch;
    }

    @Override
    public void postIndexBatch(ShardId shardId, IndexOperationBatch batch, List<Engine.IndexResult> results) {
        // no post-work; overridden to avoid materializing batch
    }

    @Override
    public void postIndexBatch(ShardId shardId, IndexOperationBatch batch, Exception ex) {
        // no post-work; overridden to avoid materializing batch
    }
}
