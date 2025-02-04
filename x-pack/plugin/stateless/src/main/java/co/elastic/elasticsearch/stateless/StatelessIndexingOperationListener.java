/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.commits.HollowShardsService;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;

import java.util.Objects;
import java.util.function.Supplier;

class StatelessIndexingOperationListener implements IndexingOperationListener {

    private final HollowShardsService hollowShardsService;

    StatelessIndexingOperationListener(HollowShardsService hollowShardsService) {
        this.hollowShardsService = Objects.requireNonNull(hollowShardsService);
    }

    @Override
    public void preBulkOnPrimary(IndexShard indexShard, Supplier<ActionListener<Void>> proceedListenerSupplier) {
        hollowShardsService.onIngestion(indexShard.shardId(), proceedListenerSupplier);
    }

    @Override
    public Engine.Delete preDelete(ShardId shardId, Engine.Delete delete) {
        hollowShardsService.assertIngestionBlocked(shardId, false);
        return IndexingOperationListener.super.preDelete(shardId, delete);
    }

    @Override
    public Engine.Index preIndex(ShardId shardId, Engine.Index operation) {
        hollowShardsService.assertIngestionBlocked(shardId, false);
        return IndexingOperationListener.super.preIndex(shardId, operation);
    }
}
