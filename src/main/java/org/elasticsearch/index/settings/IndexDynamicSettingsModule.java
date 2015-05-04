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

package org.elasticsearch.index.settings;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.allocation.decider.DisableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.settings.DynamicSettings;
import org.elasticsearch.cluster.settings.Validator;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.indexing.slowlog.ShardSlowLogIndexingService;
import org.elasticsearch.index.merge.policy.LogByteSizeMergePolicyProvider;
import org.elasticsearch.index.merge.policy.LogDocMergePolicyProvider;
import org.elasticsearch.index.merge.policy.TieredMergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.ConcurrentMergeSchedulerProvider;
import org.elasticsearch.index.search.slowlog.ShardSlowLogSearchService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.translog.TranslogService;
import org.elasticsearch.index.translog.fs.FsTranslog;
import org.elasticsearch.indices.IndicesWarmer;
import org.elasticsearch.indices.cache.query.IndicesQueryCache;
import org.elasticsearch.indices.ttl.IndicesTTLService;

/**
 */
public class IndexDynamicSettingsModule extends AbstractModule {

    private final DynamicSettings indexDynamicSettings;

    public IndexDynamicSettingsModule() {
        indexDynamicSettings = new DynamicSettings();
        indexDynamicSettings.addDynamicSetting(IndexStore.INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC, Validator.BYTES_SIZE);
        indexDynamicSettings.addDynamicSetting(IndexStore.INDEX_STORE_THROTTLE_TYPE);
        indexDynamicSettings.addDynamicSetting(ConcurrentMergeSchedulerProvider.MAX_THREAD_COUNT);
        indexDynamicSettings.addDynamicSetting(ConcurrentMergeSchedulerProvider.MAX_MERGE_COUNT);
        indexDynamicSettings.addDynamicSetting(ConcurrentMergeSchedulerProvider.AUTO_THROTTLE);
        indexDynamicSettings.addDynamicSetting(FilterAllocationDecider.INDEX_ROUTING_REQUIRE_GROUP + "*");
        indexDynamicSettings.addDynamicSetting(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "*");
        indexDynamicSettings.addDynamicSetting(FilterAllocationDecider.INDEX_ROUTING_EXCLUDE_GROUP + "*");
        indexDynamicSettings.addDynamicSetting(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE);
        indexDynamicSettings.addDynamicSetting(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE);
        indexDynamicSettings.addDynamicSetting(DisableAllocationDecider.INDEX_ROUTING_ALLOCATION_DISABLE_ALLOCATION);
        indexDynamicSettings.addDynamicSetting(DisableAllocationDecider.INDEX_ROUTING_ALLOCATION_DISABLE_NEW_ALLOCATION);
        indexDynamicSettings.addDynamicSetting(DisableAllocationDecider.INDEX_ROUTING_ALLOCATION_DISABLE_REPLICA_ALLOCATION);
        indexDynamicSettings.addDynamicSetting(FsTranslog.INDEX_TRANSLOG_FS_TYPE);
        indexDynamicSettings.addDynamicSetting(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, Validator.NON_NEGATIVE_INTEGER);
        indexDynamicSettings.addDynamicSetting(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS);
        indexDynamicSettings.addDynamicSetting(IndexMetaData.SETTING_READ_ONLY);
        indexDynamicSettings.addDynamicSetting(IndexMetaData.SETTING_BLOCKS_READ);
        indexDynamicSettings.addDynamicSetting(IndexMetaData.SETTING_BLOCKS_WRITE);
        indexDynamicSettings.addDynamicSetting(IndexMetaData.SETTING_BLOCKS_METADATA);
        indexDynamicSettings.addDynamicSetting(IndicesTTLService.INDEX_TTL_DISABLE_PURGE);
        indexDynamicSettings.addDynamicSetting(IndexShard.INDEX_REFRESH_INTERVAL, Validator.TIME);
        indexDynamicSettings.addDynamicSetting(GatewayAllocator.INDEX_RECOVERY_INITIAL_SHARDS);
        indexDynamicSettings.addDynamicSetting(LogByteSizeMergePolicyProvider.INDEX_MERGE_POLICY_MIN_MERGE_SIZE, Validator.BYTES_SIZE);
        indexDynamicSettings.addDynamicSetting(LogByteSizeMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_SIZE, Validator.BYTES_SIZE);
        indexDynamicSettings.addDynamicSetting(LogByteSizeMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_DOCS, Validator.POSITIVE_INTEGER);
        indexDynamicSettings.addDynamicSetting(LogByteSizeMergePolicyProvider.INDEX_MERGE_POLICY_MERGE_FACTOR, Validator.INTEGER_GTE_2);
        indexDynamicSettings.addDynamicSetting(LogByteSizeMergePolicyProvider.INDEX_COMPOUND_FORMAT);
        indexDynamicSettings.addDynamicSetting(LogDocMergePolicyProvider.INDEX_MERGE_POLICY_MIN_MERGE_DOCS, Validator.POSITIVE_INTEGER);
        indexDynamicSettings.addDynamicSetting(LogDocMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_DOCS, Validator.POSITIVE_INTEGER);
        indexDynamicSettings.addDynamicSetting(LogDocMergePolicyProvider.INDEX_MERGE_POLICY_MERGE_FACTOR, Validator.INTEGER_GTE_2);
        indexDynamicSettings.addDynamicSetting(LogDocMergePolicyProvider.INDEX_COMPOUND_FORMAT);
        indexDynamicSettings.addDynamicSetting(EngineConfig.INDEX_COMPOUND_ON_FLUSH, Validator.BOOLEAN);
        indexDynamicSettings.addDynamicSetting(EngineConfig.INDEX_GC_DELETES_SETTING, Validator.TIME);
        indexDynamicSettings.addDynamicSetting(IndexShard.INDEX_FLUSH_ON_CLOSE, Validator.BOOLEAN);
        indexDynamicSettings.addDynamicSetting(EngineConfig.INDEX_VERSION_MAP_SIZE, Validator.BYTES_SIZE_OR_PERCENTAGE);
        indexDynamicSettings.addDynamicSetting(ShardSlowLogIndexingService.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN, Validator.TIME);
        indexDynamicSettings.addDynamicSetting(ShardSlowLogIndexingService.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO, Validator.TIME);
        indexDynamicSettings.addDynamicSetting(ShardSlowLogIndexingService.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG, Validator.TIME);
        indexDynamicSettings.addDynamicSetting(ShardSlowLogIndexingService.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE, Validator.TIME);
        indexDynamicSettings.addDynamicSetting(ShardSlowLogIndexingService.INDEX_INDEXING_SLOWLOG_REFORMAT);
        indexDynamicSettings.addDynamicSetting(ShardSlowLogIndexingService.INDEX_INDEXING_SLOWLOG_LEVEL);
        indexDynamicSettings.addDynamicSetting(ShardSlowLogSearchService.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN, Validator.TIME);
        indexDynamicSettings.addDynamicSetting(ShardSlowLogSearchService.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO, Validator.TIME);
        indexDynamicSettings.addDynamicSetting(ShardSlowLogSearchService.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG, Validator.TIME);
        indexDynamicSettings.addDynamicSetting(ShardSlowLogSearchService.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE, Validator.TIME);
        indexDynamicSettings.addDynamicSetting(ShardSlowLogSearchService.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN, Validator.TIME);
        indexDynamicSettings.addDynamicSetting(ShardSlowLogSearchService.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO, Validator.TIME);
        indexDynamicSettings.addDynamicSetting(ShardSlowLogSearchService.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG, Validator.TIME);
        indexDynamicSettings.addDynamicSetting(ShardSlowLogSearchService.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE, Validator.TIME);
        indexDynamicSettings.addDynamicSetting(ShardSlowLogSearchService.INDEX_SEARCH_SLOWLOG_REFORMAT);
        indexDynamicSettings.addDynamicSetting(ShardSlowLogSearchService.INDEX_SEARCH_SLOWLOG_LEVEL);
        indexDynamicSettings.addDynamicSetting(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE, Validator.INTEGER);
        indexDynamicSettings.addDynamicSetting(TieredMergePolicyProvider.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED, Validator.DOUBLE);
        indexDynamicSettings.addDynamicSetting(TieredMergePolicyProvider.INDEX_MERGE_POLICY_FLOOR_SEGMENT, Validator.BYTES_SIZE);
        indexDynamicSettings.addDynamicSetting(TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE, Validator.INTEGER_GTE_2);
        indexDynamicSettings.addDynamicSetting(TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT, Validator.INTEGER_GTE_2);
        indexDynamicSettings.addDynamicSetting(TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT, Validator.BYTES_SIZE);
        indexDynamicSettings.addDynamicSetting(TieredMergePolicyProvider.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER, Validator.DOUBLE_GTE_2);
        indexDynamicSettings.addDynamicSetting(TieredMergePolicyProvider.INDEX_MERGE_POLICY_RECLAIM_DELETES_WEIGHT, Validator.NON_NEGATIVE_DOUBLE);
        indexDynamicSettings.addDynamicSetting(TieredMergePolicyProvider.INDEX_COMPOUND_FORMAT);
        indexDynamicSettings.addDynamicSetting(TranslogService.INDEX_TRANSLOG_FLUSH_INTERVAL, Validator.TIME);
        indexDynamicSettings.addDynamicSetting(TranslogService.INDEX_TRANSLOG_FLUSH_THRESHOLD_OPS, Validator.INTEGER);
        indexDynamicSettings.addDynamicSetting(TranslogService.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE, Validator.BYTES_SIZE);
        indexDynamicSettings.addDynamicSetting(TranslogService.INDEX_TRANSLOG_FLUSH_THRESHOLD_PERIOD, Validator.TIME);
        indexDynamicSettings.addDynamicSetting(TranslogService.INDEX_TRANSLOG_DISABLE_FLUSH);
        indexDynamicSettings.addDynamicSetting(IndicesWarmer.INDEX_WARMER_ENABLED);
        indexDynamicSettings.addDynamicSetting(IndicesQueryCache.INDEX_CACHE_QUERY_ENABLED, Validator.BOOLEAN);
    }

    public void addDynamicSettings(String... settings) {
        indexDynamicSettings.addDynamicSettings(settings);
    }

    public void addDynamicSetting(String setting, Validator validator) {
        indexDynamicSettings.addDynamicSetting(setting, validator);
    }

    @Override
    protected void configure() {
        bind(DynamicSettings.class).annotatedWith(IndexDynamicSettings.class).toInstance(indexDynamicSettings);
    }

    /**
     * Returns <code>true</code> iff the given setting is in the dynamic settings map. Otherwise <code>false</code>.
     */
    public boolean containsSetting(String setting) {
        return indexDynamicSettings.hasDynamicSetting(setting);
    }
}
