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

package org.elasticsearch.index.settings;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.settings.DynamicSettings;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.gateway.local.LocalGatewayAllocator;
import org.elasticsearch.index.engine.robin.RobinEngine;
import org.elasticsearch.index.gateway.IndexShardGatewayService;
import org.elasticsearch.index.indexing.slowlog.ShardSlowLogIndexingService;
import org.elasticsearch.index.merge.policy.LogByteSizeMergePolicyProvider;
import org.elasticsearch.index.merge.policy.LogDocMergePolicyProvider;
import org.elasticsearch.index.merge.policy.TieredMergePolicyProvider;
import org.elasticsearch.index.search.slowlog.ShardSlowLogSearchService;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.store.support.AbstractIndexStore;
import org.elasticsearch.index.translog.TranslogService;
import org.elasticsearch.index.translog.fs.FsTranslog;
import org.elasticsearch.indices.ttl.IndicesTTLService;

/**
 */
public class IndexDynamicSettingsModule extends AbstractModule {

    private final DynamicSettings indexDynamicSettings;

    public IndexDynamicSettingsModule() {
        indexDynamicSettings = new DynamicSettings();
        indexDynamicSettings.addDynamicSettings(
                AbstractIndexStore.INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC,
                AbstractIndexStore.INDEX_STORE_THROTTLE_TYPE,
                FilterAllocationDecider.INDEX_ROUTING_REQUIRE_GROUP + "*",
                FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "*",
                FilterAllocationDecider.INDEX_ROUTING_EXCLUDE_GROUP + "*",
                FsTranslog.INDEX_TRANSLOG_FS_TYPE,
                FsTranslog.INDEX_TRANSLOG_FS_BUFFER_SIZE,
                FsTranslog.INDEX_TRANSLOG_FS_TRANSIENT_BUFFER_SIZE,
                IndexMetaData.SETTING_NUMBER_OF_REPLICAS,
                IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS,
                IndexMetaData.SETTING_READ_ONLY,
                IndexMetaData.SETTING_BLOCKS_READ,
                IndexMetaData.SETTING_BLOCKS_WRITE,
                IndexMetaData.SETTING_BLOCKS_METADATA,
                IndexShardGatewayService.INDEX_GATEWAY_SNAPSHOT_INTERVAL,
                IndicesTTLService.INDEX_TTL_DISABLE_PURGE,
                InternalIndexShard.INDEX_REFRESH_INTERVAL,
                LocalGatewayAllocator.INDEX_RECOVERY_INITIAL_SHARDS,
                LogByteSizeMergePolicyProvider.INDEX_MERGE_POLICY_MIN_MERGE_SIZE,
                LogByteSizeMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_SIZE,
                LogByteSizeMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_DOCS,
                LogByteSizeMergePolicyProvider.INDEX_MERGE_POLICY_MERGE_FACTOR,
                LogByteSizeMergePolicyProvider.INDEX_COMPOUND_FORMAT,
                LogDocMergePolicyProvider.INDEX_MERGE_POLICY_MIN_MERGE_DOCS,
                LogDocMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_DOCS,
                LogDocMergePolicyProvider.INDEX_MERGE_POLICY_MERGE_FACTOR,
                LogDocMergePolicyProvider.INDEX_COMPOUND_FORMAT,
                RobinEngine.INDEX_TERM_INDEX_INTERVAL,
                RobinEngine.INDEX_TERM_INDEX_DIVISOR,
                RobinEngine.INDEX_INDEX_CONCURRENCY,
                RobinEngine.INDEX_GC_DELETES,
                RobinEngine.INDEX_CODEC,
                ShardSlowLogIndexingService.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN,
                ShardSlowLogIndexingService.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO,
                ShardSlowLogIndexingService.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG,
                ShardSlowLogIndexingService.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE,
                ShardSlowLogIndexingService.INDEX_INDEXING_SLOWLOG_REFORMAT,
                ShardSlowLogIndexingService.INDEX_INDEXING_SLOWLOG_LEVEL,
                ShardSlowLogSearchService.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN,
                ShardSlowLogSearchService.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO,
                ShardSlowLogSearchService.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG,
                ShardSlowLogSearchService.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE,
                ShardSlowLogSearchService.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN,
                ShardSlowLogSearchService.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO,
                ShardSlowLogSearchService.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG,
                ShardSlowLogSearchService.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE,
                ShardSlowLogSearchService.INDEX_SEARCH_SLOWLOG_REFORMAT,
                ShardSlowLogSearchService.INDEX_SEARCH_SLOWLOG_LEVEL,
                ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE,
                TieredMergePolicyProvider.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED,
                TieredMergePolicyProvider.INDEX_MERGE_POLICY_FLOOR_SEGMENT,
                TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE,
                TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT,
                TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT,
                TieredMergePolicyProvider.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER,
                TieredMergePolicyProvider.INDEX_MERGE_POLICY_RECLAIM_DELETES_WEIGHT,
                TieredMergePolicyProvider.INDEX_COMPOUND_FORMAT,
                TranslogService.INDEX_TRANSLOG_FLUSH_THRESHOLD_OPS,
                TranslogService.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE,
                TranslogService.INDEX_TRANSLOG_FLUSH_THRESHOLD_PERIOD,
                TranslogService.INDEX_TRANSLOG_DISABLE_FLUSH);
    }

    public void addDynamicSetting(String... settings) {
        indexDynamicSettings.addDynamicSettings(settings);
    }

    @Override
    protected void configure() {
        bind(DynamicSettings.class).annotatedWith(IndexDynamicSettings.class).toInstance(indexDynamicSettings);
    }
}
