/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.shard;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.aliases.IndexAliasesService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.engine.IndexSearcherWrappingService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.percolator.stats.ShardPercolateService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.termvectors.ShardTermVectorsService;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesWarmer;
import org.elasticsearch.indices.cache.query.IndicesQueryCache;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * ShadowIndexShard extends {@link IndexShard} to add file synchronization
 * from the primary when a flush happens. It also ensures that a replica being
 * promoted to a primary causes the shard to fail, kicking off a re-allocation
 * of the primary shard.
 */
public final class ShadowIndexShard extends IndexShard {

    @Inject
    public ShadowIndexShard(ShardId shardId, IndexSettingsService indexSettingsService,
                            IndicesLifecycle indicesLifecycle, Store store, StoreRecoveryService storeRecoveryService,
                            ThreadPool threadPool, MapperService mapperService,
                            IndexQueryParserService queryParserService, IndexCache indexCache,
                            IndexAliasesService indexAliasesService, IndicesQueryCache indicesQueryCache,
                            ShardPercolateService shardPercolateService, CodecService codecService,
                            ShardTermVectorsService termVectorsService, IndexFieldDataService indexFieldDataService,
                            IndexService indexService, @Nullable IndicesWarmer warmer,
                            SnapshotDeletionPolicy deletionPolicy, SimilarityService similarityService,
                            EngineFactory factory, ClusterService clusterService,
                            ShardPath path, BigArrays bigArrays, IndexSearcherWrappingService wrappingService) throws IOException {
        super(shardId, indexSettingsService, indicesLifecycle, store, storeRecoveryService,
                threadPool, mapperService, queryParserService, indexCache, indexAliasesService,
                indicesQueryCache, shardPercolateService, codecService,
                termVectorsService, indexFieldDataService, indexService,
                warmer, deletionPolicy, similarityService,
                factory, clusterService, path, bigArrays, wrappingService);
    }

    /**
     * In addition to the regular accounting done in
     * {@link IndexShard#updateRoutingEntry(org.elasticsearch.cluster.routing.ShardRouting, boolean)},
     * if this shadow replica needs to be promoted to a primary, the shard is
     * failed in order to allow a new primary to be re-allocated.
     */
    @Override
    public void updateRoutingEntry(ShardRouting newRouting, boolean persistState) {
        if (newRouting.primary() == true) {// becoming a primary
            throw new IllegalStateException("can't promote shard to primary");
        }
        super.updateRoutingEntry(newRouting, persistState);
    }

    @Override
    public MergeStats mergeStats() {
        return new MergeStats();
    }

    @Override
    public boolean canIndex() {
        return false;
    }

    @Override
    protected Engine newEngine(boolean skipInitialTranslogRecovery, EngineConfig config) {
        assert this.shardRouting.primary() == false;
        assert skipInitialTranslogRecovery : "can not recover from gateway";
        config.setCreate(false); // hardcoded - we always expect an index to be present
        return engineFactory.newReadOnlyEngine(config);
    }

    public boolean allowsPrimaryPromotion() {
        return false;
    }
}
