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

package org.elasticsearch.legacy.test.index.service;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.legacy.ElasticsearchException;
import org.elasticsearch.legacy.cluster.metadata.IndexMetaData;
import org.elasticsearch.legacy.common.inject.Injector;
import org.elasticsearch.legacy.index.Index;
import org.elasticsearch.legacy.index.IndexShardMissingException;
import org.elasticsearch.legacy.index.aliases.IndexAliasesService;
import org.elasticsearch.legacy.index.analysis.AnalysisService;
import org.elasticsearch.legacy.index.cache.IndexCache;
import org.elasticsearch.legacy.index.engine.IndexEngine;
import org.elasticsearch.legacy.index.fielddata.IndexFieldDataService;
import org.elasticsearch.legacy.index.gateway.IndexGateway;
import org.elasticsearch.legacy.index.mapper.MapperService;
import org.elasticsearch.legacy.index.query.IndexQueryParserService;
import org.elasticsearch.legacy.index.service.IndexService;
import org.elasticsearch.legacy.index.settings.IndexSettingsService;
import org.elasticsearch.legacy.index.shard.service.IndexShard;
import org.elasticsearch.legacy.index.similarity.SimilarityService;
import org.elasticsearch.legacy.index.store.IndexStore;

import java.util.Iterator;

/**
 */
public class StubIndexService implements IndexService {

    private final MapperService mapperService;

    public StubIndexService(MapperService mapperService) {
        this.mapperService = mapperService;
    }

    @Override
    public Injector injector() {
        return null;
    }

    @Override
    public IndexGateway gateway() {
        return null;
    }

    @Override
    public IndexCache cache() {
        return null;
    }

    @Override
    public IndexFieldDataService fieldData() {
        return null;
    }

    @Override
    public IndexSettingsService settingsService() {
        return null;
    }

    @Override
    public AnalysisService analysisService() {
        return null;
    }

    @Override
    public MapperService mapperService() {
        return mapperService;
    }

    @Override
    public IndexQueryParserService queryParserService() {
        return null;
    }

    @Override
    public SimilarityService similarityService() {
        return null;
    }

    @Override
    public IndexAliasesService aliasesService() {
        return null;
    }

    @Override
    public IndexEngine engine() {
        return null;
    }

    @Override
    public IndexStore store() {
        return null;
    }

    @Override
    public IndexShard createShard(int sShardId) throws ElasticsearchException {
        return null;
    }

    @Override
    public void removeShard(int shardId, String reason) throws ElasticsearchException {
    }

    @Override
    public int numberOfShards() {
        return 0;
    }

    @Override
    public ImmutableSet<Integer> shardIds() {
        return null;
    }

    @Override
    public boolean hasShard(int shardId) {
        return false;
    }

    @Override
    public IndexShard shard(int shardId) {
        return null;
    }

    @Override
    public IndexShard shardSafe(int shardId) throws IndexShardMissingException {
        return null;
    }

    @Override
    public Injector shardInjector(int shardId) {
        return null;
    }

    @Override
    public Injector shardInjectorSafe(int shardId) throws IndexShardMissingException {
        return null;
    }

    @Override
    public String indexUUID() {
        return IndexMetaData.INDEX_UUID_NA_VALUE;
    }

    @Override
    public Index index() {
        return null;
    }

    @Override
    public Iterator<IndexShard> iterator() {
        return null;
    }
}
