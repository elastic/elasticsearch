/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.service;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.UnmodifiableIterator;
import org.elasticsearch.common.component.CloseableIndexComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Injectors;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexShardAlreadyExistsException;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.deletionpolicy.DeletionPolicyModule;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineModule;
import org.elasticsearch.index.engine.IndexEngine;
import org.elasticsearch.index.gateway.IndexGateway;
import org.elasticsearch.index.gateway.IndexShardGatewayModule;
import org.elasticsearch.index.gateway.IndexShardGatewayService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.merge.policy.MergePolicyModule;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerModule;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.routing.OperationRouting;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.IndexShardManagement;
import org.elasticsearch.index.shard.IndexShardModule;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.recovery.RecoveryAction;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreModule;
import org.elasticsearch.index.translog.TranslogModule;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.InternalIndicesLifecycle;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.ShardsPluginsModule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.collect.Maps.*;
import static org.elasticsearch.common.collect.Sets.*;
import static org.elasticsearch.util.MapBuilder.*;

/**
 * @author kimchy (shay.banon)
 */
public class InternalIndexService extends AbstractIndexComponent implements IndexService {

    private final Injector injector;

    private final Settings indexSettings;

    private final PluginsService pluginsService;

    private final InternalIndicesLifecycle indicesLifecycle;

    private final MapperService mapperService;

    private final IndexQueryParserService queryParserService;

    private final SimilarityService similarityService;

    private final IndexCache indexCache;

    private final IndexEngine indexEngine;

    private final OperationRouting operationRouting;

    private volatile ImmutableMap<Integer, Injector> shardsInjectors = ImmutableMap.of();

    private volatile ImmutableMap<Integer, IndexShard> shards = ImmutableMap.of();

    private final CleanCacheOnIndicesLifecycleListener cleanCacheOnIndicesLifecycleListener = new CleanCacheOnIndicesLifecycleListener();

    @Inject public InternalIndexService(Injector injector, Index index, @IndexSettings Settings indexSettings,
                                        MapperService mapperService, IndexQueryParserService queryParserService, SimilarityService similarityService,
                                        IndexCache indexCache, IndexEngine indexEngine, OperationRouting operationRouting) {
        super(index, indexSettings);
        this.injector = injector;
        this.indexSettings = indexSettings;
        this.mapperService = mapperService;
        this.queryParserService = queryParserService;
        this.similarityService = similarityService;
        this.indexCache = indexCache;
        this.indexEngine = indexEngine;
        this.operationRouting = operationRouting;

        this.pluginsService = injector.getInstance(PluginsService.class);
        this.indicesLifecycle = (InternalIndicesLifecycle) injector.getInstance(IndicesLifecycle.class);

        this.indicesLifecycle.addListener(cleanCacheOnIndicesLifecycleListener);
    }

    @Override public int numberOfShards() {
        return shards.size();
    }

    @Override public UnmodifiableIterator<IndexShard> iterator() {
        return shards.values().iterator();
    }

    @Override public boolean hasShard(int shardId) {
        return shards.containsKey(shardId);
    }

    @Override public IndexShard shard(int shardId) {
        return shards.get(shardId);
    }

    @Override public IndexShard shardSafe(int shardId) throws IndexShardMissingException {
        IndexShard indexShard = shard(shardId);
        if (indexShard == null) {
            throw new IndexShardMissingException(new ShardId(index, shardId));
        }
        return indexShard;
    }

    @Override public Set<Integer> shardIds() {
        return newHashSet(shards.keySet());
    }

    @Override public Injector injector() {
        return injector;
    }

    @Override public IndexCache cache() {
        return indexCache;
    }

    @Override public OperationRouting operationRouting() {
        return operationRouting;
    }

    @Override public MapperService mapperService() {
        return mapperService;
    }

    @Override public IndexQueryParserService queryParserService() {
        return queryParserService;
    }

    @Override public SimilarityService similarityService() {
        return similarityService;
    }

    @Override public IndexEngine indexEngine() {
        return indexEngine;
    }

    @Override public synchronized void close(boolean delete) {
        try {
            for (int shardId : shardIds()) {
                deleteShard(shardId, delete, delete);
            }
        } finally {
            indicesLifecycle.removeListener(cleanCacheOnIndicesLifecycleListener);
        }
    }

    @Override public Injector shardInjector(int shardId) throws ElasticSearchException {
        return shardsInjectors.get(shardId);
    }

    @Override public Injector shardInjectorSafe(int shardId) throws IndexShardMissingException {
        Injector shardInjector = shardInjector(shardId);
        if (shardInjector == null) {
            throw new IndexShardMissingException(new ShardId(index, shardId));
        }
        return shardInjector;
    }

    @Override public synchronized IndexShard createShard(int sShardId) throws ElasticSearchException {
        ShardId shardId = new ShardId(index, sShardId);
        if (shardsInjectors.containsKey(shardId.id())) {
            throw new IndexShardAlreadyExistsException(shardId + " already exists");
        }

        indicesLifecycle.beforeIndexShardCreated(shardId);

        logger.debug("Creating shard_id[{}]", shardId.id());

        List<Module> modules = Lists.newArrayList();
        modules.add(new ShardsPluginsModule(indexSettings, pluginsService));
        modules.add(new IndexShardModule(shardId));
        modules.add(new StoreModule(indexSettings));
        modules.add(new DeletionPolicyModule(indexSettings));
        modules.add(new MergePolicyModule(indexSettings));
        modules.add(new MergeSchedulerModule(indexSettings));
        modules.add(new TranslogModule(indexSettings));
        modules.add(new EngineModule(indexSettings));
        modules.add(new IndexShardGatewayModule(injector.getInstance(IndexGateway.class)));

        pluginsService.processModules(modules);

        Injector shardInjector = injector.createChildInjector(modules);

        shardsInjectors = newMapBuilder(shardsInjectors).put(shardId.id(), shardInjector).immutableMap();

        IndexShard indexShard = shardInjector.getInstance(IndexShard.class);

        // clean the store
        Store store = shardInjector.getInstance(Store.class);
        try {
            store.deleteContent();
        } catch (IOException e) {
            logger.warn("Failed to clean store on shard creation", e);
        }

        indicesLifecycle.afterIndexShardCreated(indexShard);

        shards = newMapBuilder(shards).put(shardId.id(), indexShard).immutableMap();

        return indexShard;
    }

    @Override public synchronized void cleanShard(int shardId) throws ElasticSearchException {
        deleteShard(shardId, true, false);
    }

    private synchronized void deleteShard(int shardId, boolean delete, boolean deleteGateway) throws ElasticSearchException {
        Map<Integer, Injector> tmpShardInjectors = newHashMap(shardsInjectors);
        Injector shardInjector = tmpShardInjectors.remove(shardId);
        if (shardInjector == null) {
            if (!delete) {
                return;
            }
            throw new IndexShardMissingException(new ShardId(index, shardId));
        }
        shardsInjectors = ImmutableMap.copyOf(tmpShardInjectors);
        if (delete) {
            logger.debug("Deleting shard_id[{}]", shardId);
        }

        Map<Integer, IndexShard> tmpShardsMap = newHashMap(shards);
        IndexShard indexShard = tmpShardsMap.remove(shardId);
        shards = ImmutableMap.copyOf(tmpShardsMap);

        indicesLifecycle.beforeIndexShardClosed(indexShard, delete);

        for (Class<? extends CloseableIndexComponent> closeable : pluginsService.shardServices()) {
            shardInjector.getInstance(closeable).close(delete);
        }

        // close shard actions
        shardInjector.getInstance(IndexShardManagement.class).close();

        RecoveryAction recoveryAction = shardInjector.getInstance(RecoveryAction.class);
        if (recoveryAction != null) recoveryAction.close();

        shardInjector.getInstance(IndexShardGatewayService.class).close(deleteGateway);

        indexShard.close();

        Engine engine = shardInjector.getInstance(Engine.class);
        engine.close();

        // call this before we close the store, so we can release resources for it
        indicesLifecycle.afterIndexShardClosed(indexShard.shardId(), delete);

        Store store = shardInjector.getInstance(Store.class);
        try {
            store.fullDelete();
        } catch (IOException e) {
            logger.warn("Failed to clean store on shard deletion", e);
        }
        try {
            store.close();
        } catch (IOException e) {
            logger.warn("Failed to close store on shard deletion", e);
        }

        Injectors.close(injector);
    }

    class CleanCacheOnIndicesLifecycleListener extends IndicesLifecycle.Listener {

        @Override public void beforeIndexShardClosed(IndexShard indexShard, boolean delete) {
            indexCache.clearUnreferenced();
        }

        @Override public void afterIndexShardClosed(ShardId shardId, boolean delete) {
            indexCache.clearUnreferenced();
        }
    }
}