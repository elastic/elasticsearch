/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.settings;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.IndexVersion;
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
import org.elasticsearch.indices.ShardLimitValidator;

import java.util.Map;
import java.util.Set;

/**
 * Encapsulates all valid index level settings.
 * @see Property#IndexScope
 */
public final class IndexScopedSettings extends AbstractScopedSettings {

    public static final Set<Setting<?>> BUILT_IN_INDEX_SETTINGS = Set.of(
        MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY,
        MergeSchedulerConfig.AUTO_THROTTLE_SETTING,
        MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING,
        MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING,
        IndexMetadata.SETTING_INDEX_VERSION_CREATED,
        IndexMetadata.SETTING_INDEX_VERSION_COMPATIBILITY,
        IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING,
        IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING,
        IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING,
        IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING,
        IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING,
        IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING,
        IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING,
        IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING,
        IndexMetadata.INDEX_READ_ONLY_SETTING,
        IndexMetadata.INDEX_BLOCKS_READ_SETTING,
        IndexMetadata.INDEX_BLOCKS_WRITE_SETTING,
        IndexMetadata.INDEX_BLOCKS_METADATA_SETTING,
        IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING,
        IndexMetadata.INDEX_PRIORITY_SETTING,
        IndexMetadata.INDEX_DATA_PATH_SETTING,
        IndexMetadata.INDEX_HIDDEN_SETTING,
        IndexMetadata.INDEX_FORMAT_SETTING,
        IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME,
        IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_UUID,
        IndexMetadata.INDEX_DOWNSAMPLE_STATUS,
        SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING,
        SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING,
        SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING,
        SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING,
        SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING,
        SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING,
        SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING,
        SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING,
        IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN_SETTING,
        IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG_SETTING,
        IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO_SETTING,
        IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE_SETTING,
        IndexingSlowLog.INDEX_INDEXING_SLOWLOG_REFORMAT_SETTING,
        IndexingSlowLog.INDEX_INDEXING_SLOWLOG_MAX_SOURCE_CHARS_TO_LOG_SETTING,
        MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING,
        MergePolicyConfig.INDEX_MERGE_POLICY_TYPE_SETTING,
        MergePolicyConfig.INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING,
        MergePolicyConfig.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING,
        MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING,
        MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING,
        MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT_SETTING,
        MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING,
        MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING,
        MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING,
        IndexSortConfig.INDEX_SORT_FIELD_SETTING,
        IndexSortConfig.INDEX_SORT_ORDER_SETTING,
        IndexSortConfig.INDEX_SORT_MISSING_SETTING,
        IndexSortConfig.INDEX_SORT_MODE_SETTING,
        IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING,
        IndexSettings.INDEX_WARMER_ENABLED_SETTING,
        IndexSettings.INDEX_REFRESH_INTERVAL_SETTING,
        IndexSettings.INDEX_FAST_REFRESH_SETTING,
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
        IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_AGE_SETTING,
        IndexSettings.INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING,
        IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING,
        IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING,
        IndexSettings.INDEX_SEARCH_IDLE_AFTER,
        IndexSettings.INDEX_SEARCH_THROTTLED,
        IndexFieldDataService.INDEX_FIELDDATA_CACHE_KEY,
        FieldMapper.IGNORE_MALFORMED_SETTING,
        FieldMapper.COERCE_SETTING,
        Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING,
        MapperService.INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING,
        MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING,
        MapperService.INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING,
        MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING,
        MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING,
        MapperService.INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING,
        MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING,
        BitsetFilterCache.INDEX_LOAD_RANDOM_ACCESS_FILTERS_EAGERLY_SETTING,
        IndexModule.INDEX_STORE_TYPE_SETTING,
        IndexModule.INDEX_STORE_PRE_LOAD_SETTING,
        IndexModule.INDEX_RECOVERY_TYPE_SETTING,
        IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING,
        FsDirectoryFactory.INDEX_LOCK_FACTOR_SETTING,
        EngineConfig.INDEX_CODEC_SETTING,
        IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS,
        IndexSettings.DEFAULT_PIPELINE,
        IndexSettings.FINAL_PIPELINE,
        MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING,
        ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING,
        DiskThresholdDecider.SETTING_IGNORE_DISK_WATERMARKS,
        ShardLimitValidator.INDEX_SETTING_SHARD_LIMIT_GROUP,
        DataTier.TIER_PREFERENCE_SETTING,
        IndexSettings.BLOOM_FILTER_ID_FIELD_ENABLED_SETTING,
        IndexSettings.LIFECYCLE_ORIGINATION_DATE_SETTING,
        IndexSettings.LIFECYCLE_PARSE_ORIGINATION_DATE_SETTING,
        IndexSettings.TIME_SERIES_ES87TSDB_CODEC_ENABLED_SETTING,
        IndexSettings.PREFER_ILM_SETTING,

        // validate that built-in similarities don't get redefined
        Setting.groupSetting("index.similarity.", (s) -> {
            Map<String, Settings> groups = s.getAsGroups();
            for (String key : SimilarityService.BUILT_IN.keySet()) {
                if (groups.containsKey(key)) {
                    throw new IllegalArgumentException(
                        "illegal value for [index.similarity." + key + "] cannot redefine built-in similarity"
                    );
                }
            }
        }, Property.IndexScope), // this allows similarity settings to be passed
        Setting.groupSetting("index.analysis.", Property.IndexScope), // this allows analysis settings to be passed

        // TSDB index settings
        IndexSettings.MODE,
        IndexMetadata.INDEX_ROUTING_PATH,
        IndexSettings.TIME_SERIES_START_TIME,
        IndexSettings.TIME_SERIES_END_TIME,

        // Legacy index settings we must keep around for BWC from 7.x
        EngineConfig.INDEX_OPTIMIZE_AUTO_GENERATED_IDS,
        IndexMetadata.INDEX_ROLLUP_SOURCE_NAME,
        IndexMetadata.INDEX_ROLLUP_SOURCE_UUID,
        IndexSettings.MAX_ADJACENCY_MATRIX_FILTERS_SETTING,
        IndexingSlowLog.INDEX_INDEXING_SLOWLOG_LEVEL_SETTING,
        SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL,
        Store.FORCE_RAM_TERM_DICT
    );

    public static final IndexScopedSettings DEFAULT_SCOPED_SETTINGS = new IndexScopedSettings(Settings.EMPTY, BUILT_IN_INDEX_SETTINGS);

    public IndexScopedSettings(Settings settings, Set<Setting<?>> settingsSet) {
        super(settings, settingsSet, Property.IndexScope);
    }

    private IndexScopedSettings(Settings settings, IndexScopedSettings other, IndexMetadata metadata) {
        super(settings, metadata.getSettings(), other, Loggers.getLogger(IndexScopedSettings.class, metadata.getIndex()));
    }

    public IndexScopedSettings copy(Settings settings, IndexMetadata metadata) {
        return new IndexScopedSettings(settings, this, metadata);
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
            case IndexMetadata.SETTING_CREATION_DATE:
            case IndexMetadata.SETTING_INDEX_UUID:
            case IndexMetadata.SETTING_HISTORY_UUID:
            case IndexMetadata.SETTING_VERSION_UPGRADED:
            case IndexMetadata.SETTING_INDEX_PROVIDED_NAME:
            case MergePolicyConfig.INDEX_MERGE_ENABLED:
                // we keep the shrink settings for BWC - this can be removed in 8.0
                // we can't remove in 7 since this setting might be baked into an index coming in via a full cluster restart from 6.0
            case "index.shrink.source.uuid":
            case "index.shrink.source.name":
            case IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY:
            case IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY:
                return true;
            default:
                return IndexMetadata.INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING.getRawKey().match(key);
        }
    }

    @Override
    protected void validateDeprecatedAndRemovedSettingV7(Settings settings, Setting<?> setting) {
        IndexVersion indexVersion = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(settings);
        // At various stages in settings verification we will perform validation without having the
        // IndexMetadata at hand, in which case the setting version will be empty. We don't want to
        // error out on those validations, we will check with the creation version present at index
        // creation time, as well as on index update settings.
        if (indexVersion.equals(IndexVersion.ZERO) == false
            && (indexVersion.before(IndexVersion.V_7_0_0) || indexVersion.onOrAfter(IndexVersion.V_8_0_0))) {
            throw new IllegalArgumentException("unknown setting [" + setting.getKey() + "]");
        }
    }
}
