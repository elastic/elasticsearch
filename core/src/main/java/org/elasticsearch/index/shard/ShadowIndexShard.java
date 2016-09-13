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

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * ShadowIndexShard extends {@link IndexShard} to add file synchronization
 * from the primary when a flush happens. It also ensures that a replica being
 * promoted to a primary causes the shard to fail, kicking off a re-allocation
 * of the primary shard.
 */
public final class ShadowIndexShard extends IndexShard {

    public ShadowIndexShard(ShardRouting shardRouting, IndexSettings indexSettings, ShardPath path, Store store, IndexCache indexCache,
                            MapperService mapperService, SimilarityService similarityService, IndexFieldDataService indexFieldDataService,
                            @Nullable EngineFactory engineFactory, IndexEventListener indexEventListener, IndexSearcherWrapper wrapper,
                            ThreadPool threadPool, BigArrays bigArrays, Engine.Warmer engineWarmer,
                            List<SearchOperationListener> searchOperationListeners) throws IOException {
        super(shardRouting, indexSettings, path, store, indexCache, mapperService, similarityService, indexFieldDataService, engineFactory,
            indexEventListener, wrapper, threadPool, bigArrays, engineWarmer, searchOperationListeners, Collections.emptyList());
    }

    /**
     * In addition to the regular accounting done in
     * {@link IndexShard#updateRoutingEntry(ShardRouting)},
     * if this shadow replica needs to be promoted to a primary, the shard is
     * failed in order to allow a new primary to be re-allocated.
     */
    @Override
    public void updateRoutingEntry(ShardRouting newRouting) throws IOException {
        if (newRouting.primary() == true) {// becoming a primary
            throw new IllegalStateException("can't promote shard to primary");
        }
        super.updateRoutingEntry(newRouting);
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
    protected Engine newEngine(EngineConfig config) {
        assert this.shardRouting.primary() == false;
        assert config.getOpenMode() == EngineConfig.OpenMode.OPEN_INDEX_CREATE_TRANSLOG;
        return engineFactory.newReadOnlyEngine(config);
    }

    @Override
    protected RefreshListeners buildRefreshListeners() {
        // ShadowEngine doesn't have a translog so it shouldn't try to support RefreshListeners.
        return null;
    }

    @Override
    public boolean shouldFlush() {
        // we don't need to flush since we don't write - all dominated by the primary
        return false;
    }

    @Override
    public TranslogStats translogStats() {
        return null; // shadow engine has no translog
    }

    @Override
    public void addRefreshListener(Translog.Location location, Consumer<Boolean> listener) {
        throw new UnsupportedOperationException("Can't listen for a refresh on a shadow engine because it doesn't have a translog");
    }

    @Override
    public Store.MetadataSnapshot snapshotStoreMetadata() throws IOException {
        throw new UnsupportedOperationException("can't snapshot the directory as the primary may change it underneath us");
    }
}
