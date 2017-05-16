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

package org.elasticsearch.legacy.index.service;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.legacy.ElasticsearchException;
import org.elasticsearch.legacy.common.inject.Injector;
import org.elasticsearch.legacy.index.IndexComponent;
import org.elasticsearch.legacy.index.IndexShardMissingException;
import org.elasticsearch.legacy.index.aliases.IndexAliasesService;
import org.elasticsearch.legacy.index.analysis.AnalysisService;
import org.elasticsearch.legacy.index.cache.IndexCache;
import org.elasticsearch.legacy.index.engine.IndexEngine;
import org.elasticsearch.legacy.index.fielddata.IndexFieldDataService;
import org.elasticsearch.legacy.index.gateway.IndexGateway;
import org.elasticsearch.legacy.index.mapper.MapperService;
import org.elasticsearch.legacy.index.query.IndexQueryParserService;
import org.elasticsearch.legacy.index.settings.IndexSettingsService;
import org.elasticsearch.legacy.index.shard.service.IndexShard;
import org.elasticsearch.legacy.index.similarity.SimilarityService;
import org.elasticsearch.legacy.index.store.IndexStore;

/**
 *
 */
public interface IndexService extends IndexComponent, Iterable<IndexShard> {

    Injector injector();

    IndexGateway gateway();

    IndexCache cache();

    IndexFieldDataService fieldData();

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

    IndexShard shard(int shardId);

    IndexShard shardSafe(int shardId) throws IndexShardMissingException;

    Injector shardInjector(int shardId);

    Injector shardInjectorSafe(int shardId) throws IndexShardMissingException;

    String indexUUID();
}
