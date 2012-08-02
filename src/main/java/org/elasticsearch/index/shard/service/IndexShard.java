/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.shard.service;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.get.ShardGetService;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.search.stats.ShardSearchService;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexShardComponent;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.warmer.ShardIndexWarmerService;
import org.elasticsearch.index.warmer.WarmerStats;

/**
 *
 */
public interface IndexShard extends IndexShardComponent {

    ShardIndexingService indexingService();

    ShardGetService getService();

    ShardSearchService searchService();

    ShardIndexWarmerService warmerService();

    ShardRouting routingEntry();

    DocsStats docStats();

    StoreStats storeStats();

    IndexingStats indexingStats(String... types);

    SearchStats searchStats(String... groups);

    GetStats getStats();

    MergeStats mergeStats();

    RefreshStats refreshStats();

    FlushStats flushStats();

    WarmerStats warmerStats();

    IndexShardState state();

    Engine.Create prepareCreate(SourceToParse source) throws ElasticSearchException;

    ParsedDocument create(Engine.Create create) throws ElasticSearchException;

    Engine.Index prepareIndex(SourceToParse source) throws ElasticSearchException;

    ParsedDocument index(Engine.Index index) throws ElasticSearchException;

    Engine.Delete prepareDelete(String type, String id, long version) throws ElasticSearchException;

    void delete(Engine.Delete delete) throws ElasticSearchException;

    Engine.DeleteByQuery prepareDeleteByQuery(BytesReference querySource, @Nullable String[] filteringAliases, String... types) throws ElasticSearchException;

    void deleteByQuery(Engine.DeleteByQuery deleteByQuery) throws ElasticSearchException;

    Engine.GetResult get(Engine.Get get) throws ElasticSearchException;

    long count(float minScore, BytesReference querySource, @Nullable String[] filteringAliases, String... types) throws ElasticSearchException;

    void refresh(Engine.Refresh refresh) throws ElasticSearchException;

    void flush(Engine.Flush flush) throws ElasticSearchException;

    void optimize(Engine.Optimize optimize) throws ElasticSearchException;

    <T> T snapshot(Engine.SnapshotHandler<T> snapshotHandler) throws EngineException;

    void recover(Engine.RecoveryHandler recoveryHandler) throws EngineException;

    Engine.Searcher searcher();

    /**
     * Returns <tt>true</tt> if this shard can ignore a recovery attempt made to it (since the already doing/done it)
     */
    public boolean ignoreRecoveryAttempt();
}
