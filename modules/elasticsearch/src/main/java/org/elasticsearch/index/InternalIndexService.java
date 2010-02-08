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

package org.elasticsearch.index;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.UnmodifiableIterator;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.deletionpolicy.DeletionPolicyModule;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineModule;
import org.elasticsearch.index.gateway.IndexGateway;
import org.elasticsearch.index.gateway.IndexShardGatewayModule;
import org.elasticsearch.index.gateway.IndexShardGatewayService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.merge.policy.MergePolicyModule;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerModule;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.routing.OperationRouting;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardManagement;
import org.elasticsearch.index.shard.IndexShardModule;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.recovery.RecoveryAction;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreModule;
import org.elasticsearch.index.translog.TranslogModule;
import org.elasticsearch.util.guice.Injectors;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.*;
import static com.google.common.collect.Sets.*;
import static org.elasticsearch.util.MapBuilder.*;

/**
 * @author kimchy (Shay Banon)
 */
@IndexLifecycle
public class InternalIndexService extends AbstractIndexComponent implements IndexService {

    private final Injector injector;

    private final Settings indexSettings;

    private final MapperService mapperService;

    private final IndexQueryParserService queryParserService;

    private final SimilarityService similarityService;

    private final FilterCache filterCache;

    private final OperationRouting operationRouting;

    private volatile ImmutableMap<Integer, Injector> shardsInjectors = ImmutableMap.of();

    private volatile ImmutableMap<Integer, IndexShard> shards = ImmutableMap.of();

    @Inject public InternalIndexService(Injector injector, Index index, @IndexSettings Settings indexSettings,
                                        MapperService mapperService, IndexQueryParserService queryParserService, SimilarityService similarityService,
                                        FilterCache filterCache, OperationRouting operationRouting) {
        super(index, indexSettings);
        this.injector = injector;
        this.indexSettings = indexSettings;
        this.mapperService = mapperService;
        this.queryParserService = queryParserService;
        this.similarityService = similarityService;
        this.filterCache = filterCache;
        this.operationRouting = operationRouting;
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

    @Override public FilterCache filterCache() {
        return filterCache;
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

    @Override public synchronized void close() {
        for (int shardId : shardIds()) {
            deleteShard(shardId, true);
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

        logger.debug("Creating Shard Id [{}]", shardId.id());

        Injector shardInjector = injector.createChildInjector(
                new IndexShardModule(shardId),
                new StoreModule(indexSettings),
                new DeletionPolicyModule(indexSettings),
                new MergePolicyModule(indexSettings),
                new MergeSchedulerModule(indexSettings),
                new TranslogModule(indexSettings),
                new EngineModule(indexSettings),
                new IndexShardGatewayModule(injector.getInstance(IndexGateway.class)));

        shardsInjectors = newMapBuilder(shardsInjectors).put(shardId.id(), shardInjector).immutableMap();

        IndexShard indexShard = shardInjector.getInstance(IndexShard.class);

        // clean the store
        Store store = shardInjector.getInstance(Store.class);
        try {
            store.deleteContent();
        } catch (IOException e) {
            logger.warn("Failed to clean store on shard creation", e);
        }

        shards = newMapBuilder(shards).put(shardId.id(), indexShard).immutableMap();

        return indexShard;
    }

    @Override public synchronized void deleteShard(int shardId) throws ElasticSearchException {
        deleteShard(shardId, false);
    }

    private synchronized void deleteShard(int shardId, boolean close) throws ElasticSearchException {
        Map<Integer, Injector> tmpShardInjectors = newHashMap(shardsInjectors);
        Injector shardInjector = tmpShardInjectors.remove(shardId);
        if (shardInjector == null) {
            if (close) {
                return;
            }
            throw new IndexShardMissingException(new ShardId(index, shardId));
        }
        shardsInjectors = ImmutableMap.copyOf(tmpShardInjectors);
        if (!close) {
            logger.debug("Deleting Shard Id [{}]", shardId);
        }

        Map<Integer, IndexShard> tmpShardsMap = newHashMap(shards);
        IndexShard indexShard = tmpShardsMap.remove(shardId);
        shards = ImmutableMap.copyOf(tmpShardsMap);

        // close shard actions
        shardInjector.getInstance(IndexShardManagement.class).close();

        RecoveryAction recoveryAction = shardInjector.getInstance(RecoveryAction.class);
        if (recoveryAction != null) recoveryAction.close();

        shardInjector.getInstance(IndexShardGatewayService.class).close();

        indexShard.close();


        Engine engine = shardInjector.getInstance(Engine.class);
        engine.close();

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

}