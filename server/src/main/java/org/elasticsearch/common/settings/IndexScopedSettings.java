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
package org.elasticsearch.common.settings;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.IndexingSlowLog;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.MergeSchedulerConfig;
import org.elasticsearch.index.SearchSlowLog;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesRequestCache;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Encapsulates all valid index level settings.
 * @see Property#IndexScope
 */
public final class IndexScopedSettings extends AbstractScopedSettings {

    public static final Predicate<String> INDEX_SETTINGS_KEY_PREDICATE = (s) -> s.startsWith(IndexMetaData.INDEX_SETTING_PREFIX);

    public static final Set<Setting<?>> BUILT_IN_INDEX_SETTINGS = Set.of(
            MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY,
            MergeSchedulerConfig.AUTO_THROTTLE_SETTING,
            MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING,
            MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING,
            IndexMetaData.SETTING_INDEX_VERSION_CREATED,
            IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING,
            IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING,
            IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING,
            IndexMetaData.INDEX_AUTO_EXPAND_REPLICAS_SETTING,
            IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING,
            IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING,
            IndexMetaData.INDEX_ROUTING_PARTITION_SIZE_SETTING,
            IndexMetaData.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING,
            IndexMetaData.INDEX_READ_ONLY_SETTING,
            IndexMetaData.INDEX_BLOCKS_READ_SETTING,
            IndexMetaData.INDEX_BLOCKS_WRITE_SETTING,
            IndexMetaData.INDEX_BLOCKS_METADATA_SETTING,
            IndexMetaData.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING,
            IndexMetaData.INDEX_PRIORITY_SETTING,
            IndexMetaData.INDEX_DATA_PATH_SETTING,
            IndexMetaData.INDEX_FORMAT_SETTING,
            SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING,
            SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING,
            SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING,
            SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING,
            SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING,
            SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING,
            SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING,
            SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING,
            SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL,
            IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN_SETTING,
            IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG_SETTING,
            IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO_SETTING,
            IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE_SETTING,
            IndexingSlowLog.INDEX_INDEXING_SLOWLOG_LEVEL_SETTING,
            IndexingSlowLog.INDEX_INDEXING_SLOWLOG_REFORMAT_SETTING,
            IndexingSlowLog.INDEX_INDEXING_SLOWLOG_MAX_SOURCE_CHARS_TO_LOG_SETTING,
            MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING,
            MergePolicyConfig.INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING,
            MergePolicyConfig.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING,
            MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING,
            MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING,
            MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT_SETTING,
            MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING,
            MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING,
            MergePolicyConfig.INDEX_MERGE_POLICY_RECLAIM_DELETES_WEIGHT_SETTING,
            IndexSortConfig.INDEX_SORT_FIELD_SETTING,
            IndexSortConfig.INDEX_SORT_ORDER_SETTING,
            IndexSortConfig.INDEX_SORT_MISSING_SETTING,
            IndexSortConfig.INDEX_SORT_MODE_SETTING,
            IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING,
            IndexSettings.INDEX_WARMER_ENABLED_SETTING,
            IndexSettings.INDEX_REFRESH_INTERVAL_SETTING,
            IndexSettings.MAX_RESULT_WINDOW_SETTING,
            IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING,
            IndexSettings.MAX_TOKEN_COUNT_SETTING,
            IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING,
            IndexSettings.MAX_SCRIPT_FIELDS_SETTING,
            IndexSettings.MAX_NGRAM_DIFF_SETTING,
            IndexSettings.MAX_SHINGLE_DIFF_SETTING,
            IndexSettings.MAX_RESCORE_WINDOW_SETTING,
            IndexSettings.MAX_ANALYZED_OFFSET_SETTING,
            IndexSettings.MAX_TERMS_COUNT_SETTING,
            IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING,
            IndexSettings.DEFAULT_FIELD_SETTING,
            IndexSettings.QUERY_STRING_LENIENT_SETTING,
            IndexSettings.ALLOW_UNMAPPED,
            IndexSettings.INDEX_CHECK_ON_STARTUP,
            IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD,
            IndexSettings.MAX_SLICES_PER_SCROLL,
            IndexSettings.MAX_REGEX_LENGTH_SETTING,
            ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING,
            IndexSettings.INDEX_GC_DELETES_SETTING,
            IndexSettings.INDEX_SOFT_DELETES_SETTING,
            IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING,
            IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING,
            IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING,
            UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING,
            EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING,
            EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING,
            IndexSettings.INDEX_FLUSH_AFTER_MERGE_THRESHOLD_SIZE_SETTING,
            IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING,
            IndexSettings.INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING,
            IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING,
            IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING,
            IndexSettings.INDEX_SEARCH_IDLE_AFTER,
            IndexSettings.INDEX_SEARCH_THROTTLED,
            IndexFieldDataService.INDEX_FIELDDATA_CACHE_KEY,
            FieldMapper.IGNORE_MALFORMED_SETTING,
            FieldMapper.COERCE_SETTING,
            Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING,
            MapperService.INDEX_MAPPER_DYNAMIC_SETTING,
            MapperService.INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING,
            MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING,
            MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING,
            MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING,
            MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING,
            BitsetFilterCache.INDEX_LOAD_RANDOM_ACCESS_FILTERS_EAGERLY_SETTING,
            IndexModule.INDEX_STORE_TYPE_SETTING,
            IndexModule.INDEX_STORE_PRE_LOAD_SETTING,
            IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING,
            FsDirectoryFactory.INDEX_LOCK_FACTOR_SETTING,
            EngineConfig.INDEX_CODEC_SETTING,
            IndexMetaData.SETTING_WAIT_FOR_ACTIVE_SHARDS,
            IndexSettings.DEFAULT_PIPELINE,
            IndexSettings.FINAL_PIPELINE,
            MetaDataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING,

            // validate that built-in similarities don't get redefined
            Setting.groupSetting(
                    "index.similarity.",
                    (s) -> {
                        Map<String, Settings> groups = s.getAsGroups();
                        for (String key : SimilarityService.BUILT_IN.keySet()) {
                            if (groups.containsKey(key)) {
                                throw new IllegalArgumentException("illegal value for [index.similarity." + key +
                                        "] cannot redefine built-in similarity");
                            }
                        }
                    },
                    Property.IndexScope), // this allows similarity settings to be passed
            Setting.groupSetting("index.analysis.", Property.IndexScope)); // this allows analysis settings to be passed

    public static final IndexScopedSettings DEFAULT_SCOPED_SETTINGS = new IndexScopedSettings(Settings.EMPTY, BUILT_IN_INDEX_SETTINGS);

    public IndexScopedSettings(Settings settings, Set<Setting<?>> settingsSet) {
        super(settings, settingsSet, Collections.emptySet(), Property.IndexScope);
    }

    private IndexScopedSettings(Settings settings, IndexScopedSettings other, IndexMetaData metaData) {
        super(settings, metaData.getSettings(), other, Loggers.getLogger(IndexScopedSettings.class, metaData.getIndex()));
    }

    public IndexScopedSettings copy(Settings settings, IndexMetaData metaData) {
        return new IndexScopedSettings(settings, this, metaData);
    }

    @Override
    protected void validateSettingKey(Setting<?> setting) {
        if (setting.getKey().startsWith("index.") == false) {
            throw new IllegalArgumentException("illegal settings key: [" + setting.getKey() + "] must start with [index.]");
        }
        super.validateSettingKey(setting);
    }

    @Override
    public boolean isPrivateSetting(String key) {
        switch (key) {
            case IndexMetaData.SETTING_CREATION_DATE:
            case IndexMetaData.SETTING_INDEX_UUID:
            case IndexMetaData.SETTING_VERSION_UPGRADED:
            case IndexMetaData.SETTING_INDEX_PROVIDED_NAME:
            case MergePolicyConfig.INDEX_MERGE_ENABLED:
                // we keep the shrink settings for BWC - this can be removed in 8.0
                // we can't remove in 7 since this setting might be baked into an index coming in via a full cluster restart from 6.0
            case "index.shrink.source.uuid":
            case "index.shrink.source.name":
            case IndexMetaData.INDEX_RESIZE_SOURCE_UUID_KEY:
            case IndexMetaData.INDEX_RESIZE_SOURCE_NAME_KEY:
                return true;
            default:
                return IndexMetaData.INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING.getRawKey().match(key);
        }
    }
}
