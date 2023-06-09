/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.apache.lucene.index.MergePolicy;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.AbstractScopedSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.node.Node;

import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.index.mapper.MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING;
import static org.elasticsearch.index.mapper.MapperService.INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING;
import static org.elasticsearch.index.mapper.MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING;
import static org.elasticsearch.index.mapper.MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING;
import static org.elasticsearch.index.mapper.MapperService.INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING;
import static org.elasticsearch.index.mapper.MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING;

/**
 * This class encapsulates all index level settings and handles settings updates.
 * It's created per index and available to all index level classes and allows them to retrieve
 * the latest updated settings instance. Classes that need to listen to settings updates can register
 * a settings consumer at index creation via {@link IndexModule#addSettingsUpdateConsumer(Setting, Consumer)} that will
 * be called for each settings update.
 */
public final class IndexSettings {
    public static final Setting<List<String>> DEFAULT_FIELD_SETTING = Setting.stringListSetting(
        "index.query.default_field",
        Collections.singletonList("*"),
        Property.IndexScope,
        Property.Dynamic
    );
    public static final Setting<Boolean> QUERY_STRING_LENIENT_SETTING = Setting.boolSetting(
        "index.query_string.lenient",
        false,
        Property.IndexScope
    );
    public static final Setting<Boolean> QUERY_STRING_ANALYZE_WILDCARD = Setting.boolSetting(
        "indices.query.query_string.analyze_wildcard",
        false,
        Property.NodeScope
    );
    public static final Setting<Boolean> QUERY_STRING_ALLOW_LEADING_WILDCARD = Setting.boolSetting(
        "indices.query.query_string.allowLeadingWildcard",
        true,
        Property.NodeScope
    );
    public static final Setting<Boolean> ALLOW_UNMAPPED = Setting.boolSetting(
        "index.query.parse.allow_unmapped_fields",
        true,
        Property.IndexScope
    );
    public static final Setting<TimeValue> INDEX_TRANSLOG_SYNC_INTERVAL_SETTING = Setting.timeSetting(
        "index.translog.sync_interval",
        TimeValue.timeValueSeconds(5),
        TimeValue.timeValueMillis(100),
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<TimeValue> INDEX_SEARCH_IDLE_AFTER = Setting.timeSetting(
        "index.search.idle.after",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueMinutes(0),
        Property.IndexScope,
        Property.Dynamic
    );
    public static final Setting<Translog.Durability> INDEX_TRANSLOG_DURABILITY_SETTING = Setting.enumSetting(
        Translog.Durability.class,
        "index.translog.durability",
        Translog.Durability.REQUEST,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Boolean> INDEX_WARMER_ENABLED_SETTING = Setting.boolSetting(
        "index.warmer.enabled",
        true,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<String> INDEX_CHECK_ON_STARTUP = new Setting<>("index.shard.check_on_startup", "false", (s) -> {
        return switch (s) {
            case "false", "true", "checksum" -> s;
            default -> throw new IllegalArgumentException(
                "unknown value for [index.shard.check_on_startup] must be one of " + "[true, false, checksum] but was: " + s
            );
        };
    }, Property.IndexScope);

    /**
     * Index setting describing the maximum value of from + size on a query.
     * The Default maximum value of from + size on a query is 10,000. This was chosen as
     * a conservative default as it is sure to not cause trouble. Users can
     * certainly profile their cluster and decide to set it to 100,000
     * safely. 1,000,000 is probably way to high for any cluster to set
     * safely.
     */
    public static final Setting<Integer> MAX_RESULT_WINDOW_SETTING = Setting.intSetting(
        "index.max_result_window",
        10000,
        1,
        Property.Dynamic,
        Property.IndexScope
    );
    /**
     * Index setting describing the maximum value of from + size on an individual inner hit definition or
     * top hits aggregation. The default maximum of 100 is defensive for the reason that the number of inner hit responses
     * and number of top hits buckets returned is unbounded. Profile your cluster when increasing this setting.
     */
    public static final Setting<Integer> MAX_INNER_RESULT_WINDOW_SETTING = Setting.intSetting(
        "index.max_inner_result_window",
        100,
        1,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Index setting describing the maximum value of allowed `script_fields`that can be retrieved
     * per search request. The default maximum of 32 is defensive for the reason that retrieving
     * script fields is a costly operation.
     */
    public static final Setting<Integer> MAX_SCRIPT_FIELDS_SETTING = Setting.intSetting(
        "index.max_script_fields",
        32,
        0,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * A setting describing the maximum number of tokens that can be
     * produced using _analyze API. The default maximum of 10000 is defensive
     * to prevent generating too many token objects.
     */
    public static final Setting<Integer> MAX_TOKEN_COUNT_SETTING = Setting.intSetting(
        "index.analyze.max_token_count",
        10000,
        1,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * A setting describing the maximum number of characters that will be analyzed for a highlight request.
     * This setting is only applicable when highlighting is requested on a text that was indexed without
     * offsets or term vectors.
     * The default maximum of 1M characters is defensive as for highlighting larger texts,
     * indexing with offsets or term vectors is recommended.
     */
    public static final Setting<Integer> MAX_ANALYZED_OFFSET_SETTING = Setting.intSetting(
        "index.highlight.max_analyzed_offset",
        1000000,
        1,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Index setting describing the maximum number of terms that can be used in Terms Query.
     * The default maximum of 65536 terms is defensive, as extra processing and memory is involved
     * for each additional term, and a large number of terms degrade the cluster performance.
     */
    public static final Setting<Integer> MAX_TERMS_COUNT_SETTING = Setting.intSetting(
        "index.max_terms_count",
        65536,
        1,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Index setting describing for NGramTokenizer and NGramTokenFilter
     * the maximum difference between
     * max_gram (maximum length of characters in a gram) and
     * min_gram (minimum length of characters in a gram).
     * The default value is 1 as this is default difference in NGramTokenizer,
     * and is defensive as it prevents generating too many index terms.
     */
    public static final Setting<Integer> MAX_NGRAM_DIFF_SETTING = Setting.intSetting(
        "index.max_ngram_diff",
        1,
        0,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Index setting describing for ShingleTokenFilter
     * the maximum difference between
     * max_shingle_size and min_shingle_size.
     * The default value is 3 is defensive as it prevents generating too many tokens.
     */
    public static final Setting<Integer> MAX_SHINGLE_DIFF_SETTING = Setting.intSetting(
        "index.max_shingle_diff",
        3,
        0,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Index setting describing the maximum value of allowed `docvalue_fields`that can be retrieved
     * per search request. The default maximum of 100 is defensive for the reason that retrieving
     * doc values might incur a per-field per-document seek.
     */
    public static final Setting<Integer> MAX_DOCVALUE_FIELDS_SEARCH_SETTING = Setting.intSetting(
        "index.max_docvalue_fields_search",
        100,
        0,
        Property.Dynamic,
        Property.IndexScope
    );
    /**
     * Index setting describing the maximum size of the rescore window. Defaults to {@link #MAX_RESULT_WINDOW_SETTING}
     * because they both do the same thing: control the size of the heap of hits.
     */
    public static final Setting<Integer> MAX_RESCORE_WINDOW_SETTING = Setting.intSetting(
        "index.max_rescore_window",
        MAX_RESULT_WINDOW_SETTING,
        1,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final TimeValue DEFAULT_REFRESH_INTERVAL = new TimeValue(1, TimeUnit.SECONDS);
    public static final Setting<TimeValue> NODE_DEFAULT_REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "node._internal.default_refresh_interval",
        DEFAULT_REFRESH_INTERVAL,
        new TimeValue(-1, TimeUnit.MILLISECONDS),
        Property.NodeScope
    );
    public static final Setting<TimeValue> INDEX_REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "index.refresh_interval",
        NODE_DEFAULT_REFRESH_INTERVAL_SETTING,
        new TimeValue(-1, TimeUnit.MILLISECONDS),
        Property.Dynamic,
        Property.IndexScope
    );
    /**
     * Only intended for stateless.
     */
    public static final Setting<Boolean> INDEX_FAST_REFRESH_SETTING = Setting.boolSetting(
        "index.fast_refresh",
        false,
        Property.Final,
        Property.IndexScope
    );
    public static final Setting<ByteSizeValue> INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING = Setting.byteSizeSetting(
        "index.translog.flush_threshold_size",
        /*
         * Prevent the translog from growing over 10GB or 20% of the recommended shard size of 50GB. This helps bound the maximum disk usage
         * overhead of translogs.
         */
        new ByteSizeValue(10, ByteSizeUnit.GB),
        /*
         * An empty translog occupies 55 bytes on disk. If the flush threshold is below this, the flush thread
         * can get stuck in an infinite loop as the shouldPeriodicallyFlush can still be true after flushing.
         * However, small thresholds are useful for testing so we do not add a large lower bound here.
         */
        ByteSizeValue.ofBytes(Translog.DEFAULT_HEADER_SIZE_IN_BYTES + 1),
        ByteSizeValue.ofBytes(Long.MAX_VALUE),
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<TimeValue> INDEX_TRANSLOG_FLUSH_THRESHOLD_AGE_SETTING = Setting.timeSetting(
        "index.translog.flush_threshold_age",
        /*
         * Flush at least every minute by default. This gives a first order approximation of the maximum time it takes to replay translogs
         * of about one minute as well. In practice, this is not exactly true since replaying translogs is not as concurrent as indexing,
         * especially as Elasticsearch bounds the maximum number of concurrent replays of translogs, but it should still be a good enough
         * approximation.
         */
        new TimeValue(1, TimeUnit.MINUTES),
        new TimeValue(1, TimeUnit.SECONDS),
        new TimeValue(1, TimeUnit.HOURS),
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * The minimum size of a merge that triggers a flush in order to free resources
     */
    public static final Setting<ByteSizeValue> INDEX_FLUSH_AFTER_MERGE_THRESHOLD_SIZE_SETTING = Setting.byteSizeSetting(
        "index.flush_after_merge",
        new ByteSizeValue(512, ByteSizeUnit.MB),
        ByteSizeValue.ZERO, // always flush after merge
        ByteSizeValue.ofBytes(Long.MAX_VALUE), // never flush after merge
        Property.Dynamic,
        Property.IndexScope
    );
    /**
     * The maximum size of a translog generation. This is independent of the maximum size of
     * translog operations that have not been flushed.
     */
    public static final Setting<ByteSizeValue> INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING = Setting.byteSizeSetting(
        "index.translog.generation_threshold_size",
        new ByteSizeValue(64, ByteSizeUnit.MB),
        /*
         * An empty translog occupies 55 bytes on disk. If the generation threshold is
         * below this, the flush thread can get stuck in an infinite loop repeatedly
         * rolling the generation as every new generation will already exceed the
         * generation threshold. However, small thresholds are useful for testing so we
         * do not add a large lower bound here.
         */
        ByteSizeValue.ofBytes(Translog.DEFAULT_HEADER_SIZE_IN_BYTES + 1),
        ByteSizeValue.ofBytes(Long.MAX_VALUE),
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Index setting to enable / disable deletes garbage collection.
     * This setting is realtime updateable
     */
    public static final TimeValue DEFAULT_GC_DELETES = TimeValue.timeValueSeconds(60);
    public static final Setting<TimeValue> INDEX_GC_DELETES_SETTING = Setting.timeSetting(
        "index.gc_deletes",
        DEFAULT_GC_DELETES,
        new TimeValue(-1, TimeUnit.MILLISECONDS),
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Specifies if the index should use soft-delete instead of hard-delete for update/delete operations.
     * Soft-deletes is enabled by default for 7.0 indices and mandatory for 8.0 indices.
     */
    public static final Setting<Boolean> INDEX_SOFT_DELETES_SETTING = Setting.boolSetting(
        "index.soft_deletes.enabled",
        true,
        Property.IndexScope,
        Property.Final
    );

    /**
     * Controls how many soft-deleted documents will be kept around before being merged away. Keeping more deleted
     * documents increases the chance of operation-based recoveries and allows querying a longer history of documents.
     * If soft-deletes is enabled, an engine by default will retain all operations up to the global checkpoint.
     **/
    public static final Setting<Long> INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING = Setting.longSetting(
        "index.soft_deletes.retention.operations",
        0,
        0,
        Property.IndexScope,
        Property.Dynamic
    );

    /**
     * Controls how long translog files that are no longer needed for persistence reasons
     * will be kept around before being deleted. Keeping more files is useful to increase
     * the chance of ops based recoveries for indices with soft-deletes disabled.
     * TODO: Remove this setting in 9.0.
     **/
    public static final Setting<TimeValue> INDEX_TRANSLOG_RETENTION_AGE_SETTING = Setting.timeSetting(
        "index.translog.retention.age",
        TimeValue.MINUS_ONE,
        TimeValue.MINUS_ONE,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Controls how many translog files that are no longer needed for persistence reasons
     * will be kept around before being deleted. Keeping more files is useful to increase
     * the chance of ops based recoveries for indices with soft-deletes disabled.
     * TODO: Remove this setting in 9.0.
     **/
    public static final Setting<ByteSizeValue> INDEX_TRANSLOG_RETENTION_SIZE_SETTING = Setting.byteSizeSetting(
        "index.translog.retention.size",
        settings -> "-1",
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Controls the maximum length of time since a retention lease is created or renewed before it is considered expired.
     */
    public static final Setting<TimeValue> INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING = Setting.timeSetting(
        "index.soft_deletes.retention_lease.period",
        TimeValue.timeValueHours(12),
        TimeValue.ZERO,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * The maximum number of refresh listeners allows on this shard.
     */
    public static final Setting<Integer> MAX_REFRESH_LISTENERS_PER_SHARD = Setting.intSetting(
        "index.max_refresh_listeners",
        1000,
        0,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * The maximum number of slices allowed in a scroll request
     */
    public static final Setting<Integer> MAX_SLICES_PER_SCROLL = Setting.intSetting(
        "index.max_slices_per_scroll",
        1024,
        1,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * The maximum length of regex string allowed in a regexp query.
     */
    public static final Setting<Integer> MAX_REGEX_LENGTH_SETTING = Setting.intSetting(
        "index.max_regex_length",
        1000,
        1,
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<String> DEFAULT_PIPELINE = new Setting<>(
        "index.default_pipeline",
        IngestService.NOOP_PIPELINE_NAME,
        Function.identity(),
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<String> FINAL_PIPELINE = new Setting<>(
        "index.final_pipeline",
        IngestService.NOOP_PIPELINE_NAME,
        Function.identity(),
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Marks an index to be searched throttled. This means that never more than one shard of such an index will be searched concurrently
     */
    public static final Setting<Boolean> INDEX_SEARCH_THROTTLED = Setting.boolSetting(
        "index.search.throttled",
        false,
        Property.IndexScope,
        Property.PrivateIndex,
        Property.Dynamic
    );

    /**
     * Determines a balance between file-based and operations-based peer recoveries. The number of operations that will be used in an
     * operations-based peer recovery is limited to this proportion of the total number of documents in the shard (including deleted
     * documents) on the grounds that a file-based peer recovery may copy all of the documents in the shard over to the new peer, but is
     * significantly faster than replaying the missing operations on the peer, so once a peer falls far enough behind the primary it makes
     * more sense to copy all the data over again instead of replaying history.
     *
     * Defaults to retaining history for up to 10% of the documents in the shard. This can only be changed in tests, since this setting is
     * intentionally unregistered.
     */
    public static final Setting<Double> FILE_BASED_RECOVERY_THRESHOLD_SETTING = Setting.doubleSetting(
        "index.recovery.file_based_threshold",
        0.1d,
        0.0d,
        Setting.Property.IndexScope
    );

    /**
     * This index setting is intentionally undocumented and should be used as an escape hatch to disable BloomFilter of the
     * _id field of non-data-stream indices, which is enabled by default. This setting doesn't affect data-stream indices.
     */
    public static final Setting<Boolean> BLOOM_FILTER_ID_FIELD_ENABLED_SETTING = Setting.boolSetting(
        "index.bloom_filter_for_id_field.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope,
        Property.DeprecatedWarning
    );
    public static final String LIFECYCLE_ORIGINATION_DATE = "index.lifecycle.origination_date";
    public static final Setting<Long> LIFECYCLE_ORIGINATION_DATE_SETTING = Setting.longSetting(
        LIFECYCLE_ORIGINATION_DATE,
        -1,
        -1,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final String LIFECYCLE_PARSE_ORIGINATION_DATE = "index.lifecycle.parse_origination_date";
    public static final Setting<Boolean> LIFECYCLE_PARSE_ORIGINATION_DATE_SETTING = Setting.boolSetting(
        LIFECYCLE_PARSE_ORIGINATION_DATE,
        false,
        Property.Dynamic,
        Property.IndexScope
    );

    public static final String PREFER_ILM = "index.lifecycle.prefer_ilm";
    public static final Setting<Boolean> PREFER_ILM_SETTING = Setting.boolSetting(PREFER_ILM, true, Property.Dynamic, Property.IndexScope);

    /**
     * in time series mode, the start time of the index, timestamp must larger than start_time
     */
    public static final Setting<Instant> TIME_SERIES_START_TIME = Setting.dateSetting(
        "index.time_series.start_time",
        Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_MINUS_9999),
        v -> {},
        Property.IndexScope,
        Property.Final
    );

    /**
     * in time series mode, the end time of the index, timestamp must smaller than start_time
     */
    public static final Setting<Instant> TIME_SERIES_END_TIME = Setting.dateSetting(
        "index.time_series.end_time",
        Instant.ofEpochMilli(DateUtils.MAX_MILLIS_BEFORE_9999),
        new Setting.Validator<>() {
            @Override
            public void validate(Instant value) {}

            @Override
            public void validate(Instant value, Map<Setting<?>, Object> settings) {
                @SuppressWarnings("unchecked")
                Instant startTime = (Instant) settings.get(TIME_SERIES_START_TIME);
                if (startTime.toEpochMilli() > value.toEpochMilli()) {
                    throw new IllegalArgumentException("index.time_series.end_time must be larger than index.time_series.start_time");
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                List<Setting<?>> settings = List.of(TIME_SERIES_START_TIME);
                return settings.iterator();
            }
        },
        Property.IndexScope,
        Property.Dynamic
    );

    public static final Setting<Boolean> TIME_SERIES_ES87TSDB_CODEC_ENABLED_SETTING = Setting.boolSetting(
        "index.time_series.es87tsdb_codec.enabled",
        true,
        Property.IndexScope,
        Property.Final
    );

    /**
     * Returns <code>true</code> if TSDB encoding is enabled. The default is <code>true</code>
     */
    public boolean isES87TSDBCodecEnabled() {
        return es87TSDBCodecEnabled;
    }

    /**
     * The {@link IndexMode "mode"} of the index.
     */
    public static final Setting<IndexMode> MODE = Setting.enumSetting(
        IndexMode.class,
        "index.mode",
        IndexMode.STANDARD,
        new Setting.Validator<>() {
            @Override
            public void validate(IndexMode value) {}

            @Override
            public void validate(IndexMode value, Map<Setting<?>, Object> settings) {
                value.validateWithOtherSettings(settings);
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return IndexMode.VALIDATE_WITH_SETTINGS.iterator();
            }
        },
        Property.IndexScope,
        Property.Final
    );

    /**
     * Legacy index setting, kept for 7.x BWC compatibility. This setting has no effect in 8.x. Do not use.
     * TODO: Remove in 9.0
     */
    @Deprecated
    public static final Setting<Integer> MAX_ADJACENCY_MATRIX_FILTERS_SETTING = Setting.intSetting(
        "index.max_adjacency_matrix_filters",
        100,
        2,
        Property.Dynamic,
        Property.IndexScope,
        Property.IndexSettingDeprecatedInV7AndRemovedInV8
    );

    private final Index index;
    private final IndexVersion version;
    private final Logger logger;
    private final String nodeName;
    private final Settings nodeSettings;
    private final int numberOfShards;
    /**
     * The {@link IndexMode "mode"} of the index.
     */
    private final IndexMode mode;
    /**
     * The bounds for {@code @timestamp} on this index or
     * {@code null} if there are no bounds.
     */
    private volatile TimestampBounds timestampBounds;

    // volatile fields are updated via #updateIndexMetadata(IndexMetadata) under lock
    private volatile Settings settings;
    private volatile IndexMetadata indexMetadata;
    private volatile List<String> defaultFields;
    private final boolean queryStringLenient;
    private final boolean queryStringAnalyzeWildcard;
    private final boolean queryStringAllowLeadingWildcard;
    private final boolean defaultAllowUnmappedFields;
    private volatile Translog.Durability durability;
    private volatile TimeValue syncInterval;
    private volatile TimeValue refreshInterval;
    private volatile ByteSizeValue flushThresholdSize;
    private volatile TimeValue flushThresholdAge;
    private volatile ByteSizeValue generationThresholdSize;
    private volatile ByteSizeValue flushAfterMergeThresholdSize;
    private final MergeSchedulerConfig mergeSchedulerConfig;
    private final MergePolicyConfig mergePolicyConfig;
    private final IndexSortConfig indexSortConfig;
    private final IndexScopedSettings scopedSettings;
    private long gcDeletesInMillis = DEFAULT_GC_DELETES.millis();
    private final boolean softDeleteEnabled;
    private volatile long softDeleteRetentionOperations;
    private final boolean es87TSDBCodecEnabled;

    private volatile long retentionLeaseMillis;

    /**
     * The maximum age of a retention lease before it is considered expired.
     *
     * @return the maximum age
     */
    public long getRetentionLeaseMillis() {
        return retentionLeaseMillis;
    }

    private void setRetentionLeaseMillis(final TimeValue retentionLease) {
        this.retentionLeaseMillis = retentionLease.millis();
    }

    private volatile boolean warmerEnabled;
    private volatile int maxResultWindow;
    private volatile int maxInnerResultWindow;
    private volatile int maxRescoreWindow;
    private volatile int maxDocvalueFields;
    private volatile int maxScriptFields;
    private volatile int maxTokenCount;
    private volatile int maxNgramDiff;
    private volatile int maxShingleDiff;
    private volatile TimeValue searchIdleAfter;
    private volatile int maxAnalyzedOffset;
    private volatile int maxTermsCount;
    private volatile String defaultPipeline;
    private volatile String requiredPipeline;
    private volatile boolean searchThrottled;
    private volatile long mappingNestedFieldsLimit;
    private volatile long mappingNestedDocsLimit;
    private volatile long mappingTotalFieldsLimit;
    private volatile long mappingDepthLimit;
    private volatile long mappingFieldNameLengthLimit;
    private volatile long mappingDimensionFieldsLimit;

    /**
     * The maximum number of refresh listeners allows on this shard.
     */
    private volatile int maxRefreshListeners;
    /**
     * The maximum number of slices allowed in a scroll request.
     */
    private volatile int maxSlicesPerScroll;

    /**
     * The maximum length of regex string allowed in a regexp query.
     */
    private volatile int maxRegexLength;

    private final IndexRouting indexRouting;

    /**
     * Returns the default search fields for this index.
     */
    public List<String> getDefaultFields() {
        return defaultFields;
    }

    private void setDefaultFields(List<String> defaultFields) {
        this.defaultFields = defaultFields;
    }

    /**
     * Returns <code>true</code> if query string parsing should be lenient. The default is <code>false</code>
     */
    public boolean isQueryStringLenient() {
        return queryStringLenient;
    }

    /**
     * Returns <code>true</code> if the query string should analyze wildcards. The default is <code>false</code>
     */
    public boolean isQueryStringAnalyzeWildcard() {
        return queryStringAnalyzeWildcard;
    }

    /**
     * Returns <code>true</code> if the query string parser should allow leading wildcards. The default is <code>true</code>
     */
    public boolean isQueryStringAllowLeadingWildcard() {
        return queryStringAllowLeadingWildcard;
    }

    /**
     * Returns <code>true</code> if queries should be lenient about unmapped fields. The default is <code>true</code>
     */
    public boolean isDefaultAllowUnmappedFields() {
        return defaultAllowUnmappedFields;
    }

    /**
     * Creates a new {@link IndexSettings} instance. The given node settings will be merged with the settings in the metadata
     * while index level settings will overwrite node settings.
     *
     * @param indexMetadata the index metadata this settings object is associated with
     * @param nodeSettings the nodes settings this index is allocated on.
     */
    public IndexSettings(final IndexMetadata indexMetadata, final Settings nodeSettings) {
        this(indexMetadata, nodeSettings, IndexScopedSettings.DEFAULT_SCOPED_SETTINGS);
    }

    /**
     * Creates a new {@link IndexSettings} instance. The given node settings will be merged with the settings in the metadata
     * while index level settings will overwrite node settings.
     *
     * @param indexMetadata the index metadata this settings object is associated with
     * @param nodeSettings the nodes settings this index is allocated on.
     */
    public IndexSettings(final IndexMetadata indexMetadata, final Settings nodeSettings, IndexScopedSettings indexScopedSettings) {
        scopedSettings = indexScopedSettings.copy(nodeSettings, indexMetadata);
        this.nodeSettings = nodeSettings;
        this.settings = Settings.builder().put(nodeSettings).put(indexMetadata.getSettings()).build();
        this.index = indexMetadata.getIndex();
        version = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(settings);
        logger = Loggers.getLogger(getClass(), index);
        nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.indexMetadata = indexMetadata;
        numberOfShards = settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, null);
        mode = scopedSettings.get(MODE);
        this.timestampBounds = mode.getTimestampBound(indexMetadata);
        if (timestampBounds != null) {
            scopedSettings.addSettingsUpdateConsumer(IndexSettings.TIME_SERIES_END_TIME, endTime -> {
                this.timestampBounds = TimestampBounds.updateEndTime(this.timestampBounds, endTime);
            });
        }
        this.searchThrottled = INDEX_SEARCH_THROTTLED.get(settings);
        this.queryStringLenient = QUERY_STRING_LENIENT_SETTING.get(settings);
        this.queryStringAnalyzeWildcard = QUERY_STRING_ANALYZE_WILDCARD.get(nodeSettings);
        this.queryStringAllowLeadingWildcard = QUERY_STRING_ALLOW_LEADING_WILDCARD.get(nodeSettings);
        this.defaultAllowUnmappedFields = scopedSettings.get(ALLOW_UNMAPPED);
        this.durability = scopedSettings.get(INDEX_TRANSLOG_DURABILITY_SETTING);
        defaultFields = scopedSettings.get(DEFAULT_FIELD_SETTING);
        syncInterval = INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.get(settings);
        refreshInterval = scopedSettings.get(INDEX_REFRESH_INTERVAL_SETTING);
        if (scopedSettings.get(INDEX_FAST_REFRESH_SETTING) && DiscoveryNode.isStateless(nodeSettings) == false) {
            throw new IllegalArgumentException(INDEX_FAST_REFRESH_SETTING.getKey() + " is allowed only in stateless");
        }
        flushThresholdSize = scopedSettings.get(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING);
        flushThresholdAge = scopedSettings.get(INDEX_TRANSLOG_FLUSH_THRESHOLD_AGE_SETTING);
        generationThresholdSize = scopedSettings.get(INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING);
        flushAfterMergeThresholdSize = scopedSettings.get(INDEX_FLUSH_AFTER_MERGE_THRESHOLD_SIZE_SETTING);
        mergeSchedulerConfig = new MergeSchedulerConfig(this);
        gcDeletesInMillis = scopedSettings.get(INDEX_GC_DELETES_SETTING).getMillis();
        softDeleteEnabled = scopedSettings.get(INDEX_SOFT_DELETES_SETTING);
        assert softDeleteEnabled || version.before(IndexVersion.V_8_0_0) : "soft deletes must be enabled in version " + version;
        softDeleteRetentionOperations = scopedSettings.get(INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING);
        retentionLeaseMillis = scopedSettings.get(INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING).millis();
        warmerEnabled = scopedSettings.get(INDEX_WARMER_ENABLED_SETTING);
        maxResultWindow = scopedSettings.get(MAX_RESULT_WINDOW_SETTING);
        maxInnerResultWindow = scopedSettings.get(MAX_INNER_RESULT_WINDOW_SETTING);
        maxRescoreWindow = scopedSettings.get(MAX_RESCORE_WINDOW_SETTING);
        maxDocvalueFields = scopedSettings.get(MAX_DOCVALUE_FIELDS_SEARCH_SETTING);
        maxScriptFields = scopedSettings.get(MAX_SCRIPT_FIELDS_SETTING);
        maxTokenCount = scopedSettings.get(MAX_TOKEN_COUNT_SETTING);
        maxNgramDiff = scopedSettings.get(MAX_NGRAM_DIFF_SETTING);
        maxShingleDiff = scopedSettings.get(MAX_SHINGLE_DIFF_SETTING);
        maxRefreshListeners = scopedSettings.get(MAX_REFRESH_LISTENERS_PER_SHARD);
        maxSlicesPerScroll = scopedSettings.get(MAX_SLICES_PER_SCROLL);
        maxAnalyzedOffset = scopedSettings.get(MAX_ANALYZED_OFFSET_SETTING);
        maxTermsCount = scopedSettings.get(MAX_TERMS_COUNT_SETTING);
        maxRegexLength = scopedSettings.get(MAX_REGEX_LENGTH_SETTING);
        this.mergePolicyConfig = new MergePolicyConfig(logger, this);
        this.indexSortConfig = new IndexSortConfig(this);
        searchIdleAfter = scopedSettings.get(INDEX_SEARCH_IDLE_AFTER);
        defaultPipeline = scopedSettings.get(DEFAULT_PIPELINE);
        mappingNestedFieldsLimit = scopedSettings.get(INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING);
        mappingNestedDocsLimit = scopedSettings.get(INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING);
        mappingTotalFieldsLimit = scopedSettings.get(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING);
        mappingDepthLimit = scopedSettings.get(INDEX_MAPPING_DEPTH_LIMIT_SETTING);
        mappingFieldNameLengthLimit = scopedSettings.get(INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING);
        mappingDimensionFieldsLimit = scopedSettings.get(INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING);
        indexRouting = IndexRouting.fromIndexMetadata(indexMetadata);
        es87TSDBCodecEnabled = scopedSettings.get(TIME_SERIES_ES87TSDB_CODEC_ENABLED_SETTING);

        scopedSettings.addSettingsUpdateConsumer(
            MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING,
            mergePolicyConfig::setCompoundFormatThreshold
        );
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_TYPE_SETTING, mergePolicyConfig::setMergePolicyType);
        scopedSettings.addSettingsUpdateConsumer(
            MergePolicyConfig.INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING,
            mergePolicyConfig::setDeletesPctAllowed
        );
        scopedSettings.addSettingsUpdateConsumer(
            MergePolicyConfig.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING,
            mergePolicyConfig::setExpungeDeletesAllowed
        );
        scopedSettings.addSettingsUpdateConsumer(
            MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING,
            mergePolicyConfig::setFloorSegmentSetting
        );
        scopedSettings.addSettingsUpdateConsumer(
            MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING,
            mergePolicyConfig::setMaxMergesAtOnce
        );
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT_SETTING, ignored -> {});
        scopedSettings.addSettingsUpdateConsumer(
            MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING,
            mergePolicyConfig::setMaxMergedSegment
        );
        scopedSettings.addSettingsUpdateConsumer(
            MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING,
            mergePolicyConfig::setSegmentsPerTier
        );
        scopedSettings.addSettingsUpdateConsumer(
            MergePolicyConfig.INDEX_MERGE_POLICY_MERGE_FACTOR_SETTING,
            mergePolicyConfig::setMergeFactor
        );

        scopedSettings.addSettingsUpdateConsumer(
            MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING,
            MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING,
            mergeSchedulerConfig::setMaxThreadAndMergeCount
        );
        scopedSettings.addSettingsUpdateConsumer(MergeSchedulerConfig.AUTO_THROTTLE_SETTING, mergeSchedulerConfig::setAutoThrottle);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_DURABILITY_SETTING, this::setTranslogDurability);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_SYNC_INTERVAL_SETTING, this::setTranslogSyncInterval);
        scopedSettings.addSettingsUpdateConsumer(MAX_RESULT_WINDOW_SETTING, this::setMaxResultWindow);
        scopedSettings.addSettingsUpdateConsumer(MAX_INNER_RESULT_WINDOW_SETTING, this::setMaxInnerResultWindow);
        scopedSettings.addSettingsUpdateConsumer(MAX_RESCORE_WINDOW_SETTING, this::setMaxRescoreWindow);
        scopedSettings.addSettingsUpdateConsumer(MAX_DOCVALUE_FIELDS_SEARCH_SETTING, this::setMaxDocvalueFields);
        scopedSettings.addSettingsUpdateConsumer(MAX_SCRIPT_FIELDS_SETTING, this::setMaxScriptFields);
        scopedSettings.addSettingsUpdateConsumer(MAX_TOKEN_COUNT_SETTING, this::setMaxTokenCount);
        scopedSettings.addSettingsUpdateConsumer(MAX_NGRAM_DIFF_SETTING, this::setMaxNgramDiff);
        scopedSettings.addSettingsUpdateConsumer(MAX_SHINGLE_DIFF_SETTING, this::setMaxShingleDiff);
        scopedSettings.addSettingsUpdateConsumer(INDEX_WARMER_ENABLED_SETTING, this::setEnableWarmer);
        scopedSettings.addSettingsUpdateConsumer(INDEX_GC_DELETES_SETTING, this::setGCDeletes);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING, this::setTranslogFlushThresholdSize);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_FLUSH_THRESHOLD_AGE_SETTING, this::setTranslogFlushThresholdAge);
        scopedSettings.addSettingsUpdateConsumer(INDEX_FLUSH_AFTER_MERGE_THRESHOLD_SIZE_SETTING, this::setFlushAfterMergeThresholdSize);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING, this::setGenerationThresholdSize);
        scopedSettings.addSettingsUpdateConsumer(INDEX_REFRESH_INTERVAL_SETTING, this::setRefreshInterval);
        scopedSettings.addSettingsUpdateConsumer(MAX_REFRESH_LISTENERS_PER_SHARD, this::setMaxRefreshListeners);
        scopedSettings.addSettingsUpdateConsumer(MAX_ANALYZED_OFFSET_SETTING, this::setHighlightMaxAnalyzedOffset);
        scopedSettings.addSettingsUpdateConsumer(MAX_TERMS_COUNT_SETTING, this::setMaxTermsCount);
        scopedSettings.addSettingsUpdateConsumer(MAX_SLICES_PER_SCROLL, this::setMaxSlicesPerScroll);
        scopedSettings.addSettingsUpdateConsumer(DEFAULT_FIELD_SETTING, this::setDefaultFields);
        scopedSettings.addSettingsUpdateConsumer(INDEX_SEARCH_IDLE_AFTER, this::setSearchIdleAfter);
        scopedSettings.addSettingsUpdateConsumer(MAX_REGEX_LENGTH_SETTING, this::setMaxRegexLength);
        scopedSettings.addSettingsUpdateConsumer(DEFAULT_PIPELINE, this::setDefaultPipeline);
        scopedSettings.addSettingsUpdateConsumer(FINAL_PIPELINE, this::setRequiredPipeline);
        scopedSettings.addSettingsUpdateConsumer(INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING, this::setSoftDeleteRetentionOperations);
        scopedSettings.addSettingsUpdateConsumer(INDEX_SEARCH_THROTTLED, this::setSearchThrottled);
        scopedSettings.addSettingsUpdateConsumer(INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING, this::setRetentionLeaseMillis);
        scopedSettings.addSettingsUpdateConsumer(INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING, this::setMappingNestedFieldsLimit);
        scopedSettings.addSettingsUpdateConsumer(INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING, this::setMappingNestedDocsLimit);
        scopedSettings.addSettingsUpdateConsumer(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING, this::setMappingTotalFieldsLimit);
        scopedSettings.addSettingsUpdateConsumer(INDEX_MAPPING_DEPTH_LIMIT_SETTING, this::setMappingDepthLimit);
        scopedSettings.addSettingsUpdateConsumer(INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING, this::setMappingFieldNameLengthLimit);
        scopedSettings.addSettingsUpdateConsumer(INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING, this::setMappingDimensionFieldsLimit);
    }

    private void setSearchIdleAfter(TimeValue searchIdleAfter) {
        this.searchIdleAfter = searchIdleAfter;
    }

    private void setTranslogFlushThresholdSize(ByteSizeValue byteSizeValue) {
        this.flushThresholdSize = byteSizeValue;
    }

    private void setTranslogFlushThresholdAge(TimeValue timeValue) {
        this.flushThresholdAge = timeValue;
    }

    private void setFlushAfterMergeThresholdSize(ByteSizeValue byteSizeValue) {
        this.flushAfterMergeThresholdSize = byteSizeValue;
    }

    private void setGenerationThresholdSize(final ByteSizeValue generationThresholdSize) {
        this.generationThresholdSize = generationThresholdSize;
    }

    private void setGCDeletes(TimeValue timeValue) {
        this.gcDeletesInMillis = timeValue.getMillis();
    }

    private void setRefreshInterval(TimeValue timeValue) {
        this.refreshInterval = timeValue;
    }

    /**
     * Returns the settings for this index. These settings contain the node and index level settings where
     * settings that are specified on both index and node level are overwritten by the index settings.
     */
    public Settings getSettings() {
        return settings;
    }

    /**
     * Returns the index this settings object belongs to
     */
    public Index getIndex() {
        return index;
    }

    /**
     * Returns the indexes UUID
     */
    public String getUUID() {
        return getIndex().getUUID();
    }

    /**
     * Returns <code>true</code> if the index has a custom data path
     */
    public boolean hasCustomDataPath() {
        return Strings.isNotEmpty(customDataPath());
    }

    /**
     * Returns the customDataPath for this index, if configured. <code>""</code> o.w.
     */
    public String customDataPath() {
        return IndexMetadata.INDEX_DATA_PATH_SETTING.get(settings);
    }

    /**
     * Returns the version the index was created on.
     * @see IndexMetadata#SETTING_VERSION_CREATED
     */
    public Version getIndexVersionCreated() {
        return version.toVersion();
    }

    /**
     * Returns the current node name
     */
    public String getNodeName() {
        return nodeName;
    }

    /**
     * Returns the current IndexMetadata for this index
     */
    public IndexMetadata getIndexMetadata() {
        return indexMetadata;
    }

    /**
     * Returns the number of shards this index has.
     */
    public int getNumberOfShards() {
        return numberOfShards;
    }

    /**
     * Returns the number of replicas this index has.
     */
    public int getNumberOfReplicas() {
        return settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, null);
    }

    /**
     * "Mode" that controls which behaviors and settings an index supports.
     */
    public IndexMode getMode() {
        return mode;
    }

    /**
     * Returns the node settings. The settings returned from {@link #getSettings()} are a merged version of the
     * index settings and the node settings where node settings are overwritten by index settings.
     */
    public Settings getNodeSettings() {
        return nodeSettings;
    }

    /**
     * Updates the settings and index metadata and notifies all registered settings consumers with the new settings iff at least one
     * setting has changed.
     *
     * @return <code>true</code> iff any setting has been updated otherwise <code>false</code>.
     */
    public synchronized boolean updateIndexMetadata(IndexMetadata indexMetadata) {
        final Settings newSettings = indexMetadata.getSettings();
        IndexVersion newIndexVersion = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(newSettings);
        if (version.equals(newIndexVersion) == false) {
            throw new IllegalArgumentException("version mismatch on settings update expected: " + version + " but was: " + newIndexVersion);
        }
        IndexVersion newCompatibilityVersion = IndexMetadata.SETTING_INDEX_VERSION_COMPATIBILITY.get(newSettings);
        IndexVersion compatibilityVersion = IndexMetadata.SETTING_INDEX_VERSION_COMPATIBILITY.get(settings);
        if (compatibilityVersion.equals(newCompatibilityVersion) == false) {
            throw new IllegalArgumentException(
                "compatibility version mismatch on settings update expected: "
                    + compatibilityVersion
                    + " but was: "
                    + newCompatibilityVersion
            );
        }
        final String newUUID = newSettings.get(IndexMetadata.SETTING_INDEX_UUID, IndexMetadata.INDEX_UUID_NA_VALUE);
        if (newUUID.equals(getUUID()) == false) {
            throw new IllegalArgumentException("uuid mismatch on settings update expected: " + getUUID() + " but was: " + newUUID);
        }
        final String newRestoreUUID = newSettings.get(IndexMetadata.SETTING_HISTORY_UUID, IndexMetadata.INDEX_UUID_NA_VALUE);
        final String restoreUUID = this.settings.get(IndexMetadata.SETTING_HISTORY_UUID, IndexMetadata.INDEX_UUID_NA_VALUE);
        if (newRestoreUUID.equals(restoreUUID) == false) {
            throw new IllegalArgumentException("uuid mismatch on settings update expected: " + restoreUUID + " but was: " + newRestoreUUID);
        }
        this.indexMetadata = indexMetadata;
        final Settings newIndexSettings = Settings.builder().put(nodeSettings).put(newSettings).build();
        if (same(this.settings, newIndexSettings)) {
            // nothing to update, same settings
            return false;
        }
        scopedSettings.applySettings(newSettings);
        this.settings = newIndexSettings;
        return true;
    }

    /**
     * Compare the specified settings for equality.
     *
     * @param left  the left settings
     * @param right the right settings
     * @return true if the settings are the same, otherwise false
     */
    public static boolean same(final Settings left, final Settings right) {
        if (left.equals(right)) {
            return true;
        }
        return left.getByPrefix(IndexMetadata.INDEX_SETTING_PREFIX).equals(right.getByPrefix(IndexMetadata.INDEX_SETTING_PREFIX))
            && left.getByPrefix(AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX)
                .equals(right.getByPrefix(AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX));
    }

    /**
     * Returns the translog durability for this index.
     */
    public Translog.Durability getTranslogDurability() {
        return durability;
    }

    private void setTranslogDurability(Translog.Durability durability) {
        this.durability = durability;
    }

    /**
     * Returns true if index warmers are enabled, otherwise <code>false</code>
     */
    public boolean isWarmerEnabled() {
        return warmerEnabled;
    }

    private void setEnableWarmer(boolean enableWarmer) {
        this.warmerEnabled = enableWarmer;
    }

    /**
     * Returns the translog sync interval. This is the interval in which the transaction log is asynchronously fsynced unless
     * the transaction log is fsyncing on every operations
     */
    public TimeValue getTranslogSyncInterval() {
        return syncInterval;
    }

    public void setTranslogSyncInterval(TimeValue translogSyncInterval) {
        this.syncInterval = translogSyncInterval;
    }

    /**
     * Returns this interval in which the shards of this index are asynchronously refreshed. {@code -1} means async refresh is disabled.
     */
    public TimeValue getRefreshInterval() {
        return refreshInterval;
    }

    /**
     * Returns the transaction log threshold size when to forcefully flush the index and clear the transaction log.
     */
    public ByteSizeValue getFlushThresholdSize(ByteSizeValue totalDiskSpace) {
        // Never return more than 1% of the total disk space as a protection for small instances that may not have much disk space.
        long onePercentOfTotalDiskSpace = totalDiskSpace.getBytes() / 100;
        if (onePercentOfTotalDiskSpace <= ByteSizeUnit.MB.toBytes(10)) {
            // Paranoia: total disk usage should always be at least in the GBs. Make sure the translog is always allowed at least 10MB.
            onePercentOfTotalDiskSpace = ByteSizeUnit.MB.toBytes(10);
        }
        assert onePercentOfTotalDiskSpace > Translog.DEFAULT_HEADER_SIZE_IN_BYTES;
        if (onePercentOfTotalDiskSpace < flushThresholdSize.getBytes()) {
            return new ByteSizeValue(onePercentOfTotalDiskSpace, ByteSizeUnit.BYTES);
        } else {
            return flushThresholdSize;
        }
    }

    /**
     * Returns the transaction log threshold age when to forcefully flush the index and clear the transaction log.
     */
    public TimeValue getFlushThresholdAge() {
        return flushThresholdAge;
    }

    /**
     * Returns the merge threshold size when to forcefully flush the index and free resources.
     */
    public ByteSizeValue getFlushAfterMergeThresholdSize() {
        return flushAfterMergeThresholdSize;
    }

    /**
     * Returns the generation threshold size. As sequence numbers can cause multiple generations to
     * be preserved for rollback purposes, we want to keep the size of individual generations from
     * growing too large to avoid excessive disk space consumption. Therefore, the translog is
     * automatically rolled to a new generation when the current generation exceeds this generation
     * threshold size.
     *
     * @return the generation threshold size
     */
    public ByteSizeValue getGenerationThresholdSize() {
        return generationThresholdSize;
    }

    /**
     * Returns the {@link MergeSchedulerConfig}
     */
    public MergeSchedulerConfig getMergeSchedulerConfig() {
        return mergeSchedulerConfig;
    }

    /**
     * Returns the max result window for search requests, describing the maximum value of from + size on a query.
     */
    public int getMaxResultWindow() {
        return this.maxResultWindow;
    }

    private void setMaxResultWindow(int maxResultWindow) {
        this.maxResultWindow = maxResultWindow;
    }

    /**
     * Returns the max result window for an individual inner hit definition or top hits aggregation.
     */
    public int getMaxInnerResultWindow() {
        return maxInnerResultWindow;
    }

    private void setMaxInnerResultWindow(int maxInnerResultWindow) {
        this.maxInnerResultWindow = maxInnerResultWindow;
    }

    /**
     * Returns the maximum rescore window for search requests.
     */
    public int getMaxRescoreWindow() {
        return maxRescoreWindow;
    }

    private void setMaxRescoreWindow(int maxRescoreWindow) {
        this.maxRescoreWindow = maxRescoreWindow;
    }

    /**
     * Returns the maximum number of allowed docvalue_fields to retrieve in a search request
     */
    public int getMaxDocvalueFields() {
        return this.maxDocvalueFields;
    }

    private void setMaxDocvalueFields(int maxDocvalueFields) {
        this.maxDocvalueFields = maxDocvalueFields;
    }

    /**
     * Returns the maximum number of tokens that can be produced
     */
    public int getMaxTokenCount() {
        return maxTokenCount;
    }

    private void setMaxTokenCount(int maxTokenCount) {
        this.maxTokenCount = maxTokenCount;
    }

    /**
     * Returns the maximum allowed difference between max and min length of ngram
     */
    public int getMaxNgramDiff() {
        return this.maxNgramDiff;
    }

    private void setMaxNgramDiff(int maxNgramDiff) {
        this.maxNgramDiff = maxNgramDiff;
    }

    /**
     * Returns the maximum allowed difference between max and min shingle_size
     */
    public int getMaxShingleDiff() {
        return this.maxShingleDiff;
    }

    private void setMaxShingleDiff(int maxShingleDiff) {
        this.maxShingleDiff = maxShingleDiff;
    }

    /**
     *  Returns the maximum number of chars that will be analyzed in a highlight request
     */
    public int getHighlightMaxAnalyzedOffset() {
        return this.maxAnalyzedOffset;
    }

    private void setHighlightMaxAnalyzedOffset(int maxAnalyzedOffset) {
        this.maxAnalyzedOffset = maxAnalyzedOffset;
    }

    /**
     *  Returns the maximum number of terms that can be used in a Terms Query request
     */
    public int getMaxTermsCount() {
        return this.maxTermsCount;
    }

    private void setMaxTermsCount(int maxTermsCount) {
        this.maxTermsCount = maxTermsCount;
    }

    /**
     * Returns the maximum number of allowed script_fields to retrieve in a search request
     */
    public int getMaxScriptFields() {
        return this.maxScriptFields;
    }

    private void setMaxScriptFields(int maxScriptFields) {
        this.maxScriptFields = maxScriptFields;
    }

    /**
     * Returns the GC deletes cycle in milliseconds.
     */
    public long getGcDeletesInMillis() {
        return gcDeletesInMillis;
    }

    /**
     * Returns the merge policy that should be used for this index.
     */
    public MergePolicy getMergePolicy(boolean isTimeBasedIndex) {
        return mergePolicyConfig.getMergePolicy(isTimeBasedIndex);
    }

    public <T> T getValue(Setting<T> setting) {
        return scopedSettings.get(setting);
    }

    /**
     * The maximum number of refresh listeners allows on this shard.
     */
    public int getMaxRefreshListeners() {
        return maxRefreshListeners;
    }

    private void setMaxRefreshListeners(int maxRefreshListeners) {
        this.maxRefreshListeners = maxRefreshListeners;
    }

    /**
     * The maximum number of slices allowed in a scroll request.
     */
    public int getMaxSlicesPerScroll() {
        return maxSlicesPerScroll;
    }

    private void setMaxSlicesPerScroll(int value) {
        this.maxSlicesPerScroll = value;
    }

    /**
     * The maximum length of regex string allowed in a regexp query.
     */
    public int getMaxRegexLength() {
        return maxRegexLength;
    }

    private void setMaxRegexLength(int maxRegexLength) {
        this.maxRegexLength = maxRegexLength;
    }

    /**
     * Returns the index sort config that should be used for this index.
     */
    public IndexSortConfig getIndexSortConfig() {
        return indexSortConfig;
    }

    public IndexScopedSettings getScopedSettings() {
        return scopedSettings;
    }

    /**
     * Returns true iff the refresh setting exists or in other words is explicitly set.
     */
    public boolean isExplicitRefresh() {
        return INDEX_REFRESH_INTERVAL_SETTING.exists(settings);
    }

    /**
     * Returns the time that an index shard becomes search idle unless it's accessed in between
     */
    public TimeValue getSearchIdleAfter() {
        return searchIdleAfter;
    }

    public String getDefaultPipeline() {
        return defaultPipeline;
    }

    public void setDefaultPipeline(String defaultPipeline) {
        this.defaultPipeline = defaultPipeline;
    }

    public String getRequiredPipeline() {
        return requiredPipeline;
    }

    public void setRequiredPipeline(final String requiredPipeline) {
        this.requiredPipeline = requiredPipeline;
    }

    /**
     * Returns <code>true</code> if soft-delete is enabled.
     */
    public boolean isSoftDeleteEnabled() {
        return softDeleteEnabled;
    }

    private void setSoftDeleteRetentionOperations(long ops) {
        this.softDeleteRetentionOperations = ops;
    }

    /**
     * Returns the number of extra operations (i.e. soft-deleted documents) to be kept for recoveries and history purpose.
     */
    public long getSoftDeleteRetentionOperations() {
        return this.softDeleteRetentionOperations;
    }

    /**
     * Returns true if the this index should be searched throttled ie. using the
     * {@link org.elasticsearch.threadpool.ThreadPool.Names#SEARCH_THROTTLED} thread-pool
     */
    public boolean isSearchThrottled() {
        return searchThrottled;
    }

    private void setSearchThrottled(boolean searchThrottled) {
        this.searchThrottled = searchThrottled;
    }

    public long getMappingNestedFieldsLimit() {
        return mappingNestedFieldsLimit;
    }

    private void setMappingNestedFieldsLimit(long value) {
        this.mappingNestedFieldsLimit = value;
    }

    public long getMappingNestedDocsLimit() {
        return mappingNestedDocsLimit;
    }

    private void setMappingNestedDocsLimit(long value) {
        this.mappingNestedDocsLimit = value;
    }

    public long getMappingTotalFieldsLimit() {
        return mappingTotalFieldsLimit;
    }

    private void setMappingTotalFieldsLimit(long value) {
        this.mappingTotalFieldsLimit = value;
    }

    public long getMappingDepthLimit() {
        return mappingDepthLimit;
    }

    private void setMappingDepthLimit(long value) {
        this.mappingDepthLimit = value;
    }

    public long getMappingFieldNameLengthLimit() {
        return mappingFieldNameLengthLimit;
    }

    private void setMappingFieldNameLengthLimit(long value) {
        this.mappingFieldNameLengthLimit = value;
    }

    public long getMappingDimensionFieldsLimit() {
        return mappingDimensionFieldsLimit;
    }

    private void setMappingDimensionFieldsLimit(long value) {
        this.mappingDimensionFieldsLimit = value;
    }

    /**
     * The bounds for {@code @timestamp} on this index or
     * {@code null} if there are no bounds.
     */
    public TimestampBounds getTimestampBounds() {
        return timestampBounds;
    }

    /**
     * The way that documents are routed on the coordinating
     * node when being sent to shards of this index.
     */
    public IndexRouting getIndexRouting() {
        return indexRouting;
    }
}
