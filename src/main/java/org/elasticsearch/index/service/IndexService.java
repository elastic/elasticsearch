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

package org.elasticsearch.index.service;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.index.IndexComponent;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.aliases.IndexAliasesService;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.engine.IndexEngine;
import org.elasticsearch.index.cache.fixedbitset.FixedBitSetFilterCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.gateway.IndexGateway;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.IndexStore;

/**
 *
 */
public interface IndexService extends IndexComponent, Iterable<IndexShard> {

    Injector injector();

    IndexGateway gateway();

    IndexCache cache();

    IndexFieldDataService fieldData();

    FixedBitSetFilterCache fixedBitSetFilterCache();

    IndexSettingsService settingsService();

    AnalysisService analysisService();

    MapperService mapperService();

    IndexQueryParserService queryParserService();

    SimilarityService similarityService();

    IndexAliasesService aliasesService();

    IndexEngine engine();

    IndexStore store();

    IndexShard createShard(int sShardId) throws ElasticsearchException;

    /**
     * Removes the shard, does not delete local data or the gateway.
     */
    void removeShard(int shardId, String reason) throws ElasticsearchException;

    int numberOfShards();

    ImmutableSet<Integer> shardIds();

    boolean hasShard(int shardId);

    /**
     * Return the shard with the provided id, or null if there is no such shard.
     */
    @Nullable
    IndexShard shard(int shardId);

    /**
     * Return the shard with the provided id, or throw an exception if it doesn't exist.
     */
    IndexShard shardSafe(int shardId) throws IndexShardMissingException;

    /**
     * Return the shard injector for the provided id, or null if there is no such shard.
     */
    @Nullable
    Injector shardInjector(int shardId);

    /**
     * Return the shard injector for the provided id, or throw an exception if there is no such shard.
     */
    Injector shardInjectorSafe(int shardId) throws IndexShardMissingException;

    String indexUUID();
}
