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
package org.elasticsearch.index;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.MergePolicy;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.mapper.AllFieldMapper;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.node.Node;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * This class encapsulates all index level settings and handles settings updates.
 * It's created per index and available to all index level classes and allows them to retrieve
 * the latest updated settings instance. Classes that need to listen to settings updates can register
 * a settings consumer at index creation via {@link IndexModule#addSettingsUpdateConsumer(Setting, Consumer)} that will
 * be called for each settings update.
 */
public final class IndexSettings {
    public static final String DEFAULT_FIELD_SETTING_KEY = "index.query.default_field";
    public static final Setting<List<String>> DEFAULT_FIELD_SETTING;
    static {
        Function<Settings, List<String>> defValue = settings -> {
            final String defaultField;
            if (settings.getAsVersion(IndexMetaData.SETTING_VERSION_CREATED, null) != null &&
                    Version.indexCreated(settings).before(Version.V_6_0_0_alpha1)) {
                defaultField = AllFieldMapper.NAME;
            } else {
                defaultField = "*";
            }
            return Collections.singletonList(defaultField);
        };
        DEFAULT_FIELD_SETTING = Setting.listSetting(DEFAULT_FIELD_SETTING_KEY, defValue, Function.identity(), Property.IndexScope, Property.Dynamic);
    }
    public static final Setting<Boolean> QUERY_STRING_LENIENT_SETTING =
        Setting.boolSetting("index.query_string.lenient", false, Property.IndexScope);
    public static final Setting<Boolean> QUERY_STRING_ANALYZE_WILDCARD =
        Setting.boolSetting("indices.query.query_string.analyze_wildcard", false, Property.NodeScope);
    public static final Setting<Boolean> QUERY_STRING_ALLOW_LEADING_WILDCARD =
        Setting.boolSetting("indices.query.query_string.allowLeadingWildcard", true, Property.NodeScope);
    public static final Setting<Boolean> ALLOW_UNMAPPED =
        Setting.boolSetting("index.query.parse.allow_unmapped_fields", true, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_TRANSLOG_SYNC_INTERVAL_SETTING =
        Setting.timeSetting("index.translog.sync_interval", TimeValue.timeValueSeconds(5), TimeValue.timeValueMillis(100),
            Property.IndexScope);
    public static final Setting<Translog.Durability> INDEX_TRANSLOG_DURABILITY_SETTING =
        new Setting<>("index.translog.durability", Translog.Durability.REQUEST.name(),
            (value) -> Translog.Durability.valueOf(value.toUpperCase(Locale.ROOT)), Property.Dynamic, Property.IndexScope);
    public static final Setting<Boolean> INDEX_WARMER_ENABLED_SETTING =
        Setting.boolSetting("index.warmer.enabled", true, Property.Dynamic, Property.IndexScope);
    public static final Setting<Boolean> INDEX_TTL_DISABLE_PURGE_SETTING =
        Setting.boolSetting("index.ttl.disable_purge", false, Property.Dynamic, Property.IndexScope);
    public static final Setting<String> INDEX_CHECK_ON_STARTUP = new Setting<>("index.shard.check_on_startup", "false", (s) -> {
        switch(s) {
            case "false":
            case "true":
            case "fix":
            case "checksum":
                return s;
            default:
                throw new IllegalArgumentException("unknown value for [index.shard.check_on_startup] must be one of [true, false, fix, checksum] but was: " + s);
        }
    }, Property.IndexScope);

    /**
     * Index setting describing the maximum value of from + size on a query.
     * The Default maximum value of from + size on a query is 10,000. This was chosen as
     * a conservative default as it is sure to not cause trouble. Users can
     * certainly profile their cluster and decide to set it to 100,000
     * safely. 1,000,000 is probably way to high for any cluster to set
     * safely.
     */
    public static final Setting<Integer> MAX_RESULT_WINDOW_SETTING =
        Setting.intSetting("index.max_result_window", 10000, 1, Property.Dynamic, Property.IndexScope);

    /**
     * Index setting describing the maximum value of allowed `script_fields`that can be retrieved
     * per search request. The default maximum of 32 is defensive for the reason that retrieving
     * script fields is a costly operation.
     */
    public static final Setting<Integer> MAX_SCRIPT_FIELDS_SETTING =
        Setting.intSetting("index.max_script_fields", 32, 0, Property.Dynamic, Property.IndexScope);

    /**
     * Index setting describing the maximum value of from + size on an individual inner hit definition or
     * top hits aggregation. The default maximum of 100 is defensive for the reason that the number of inner hit responses
     * and number of top hits buckets returned is unbounded. Profile your cluster when increasing this setting.
     */
    public static final Setting<Integer> MAX_INNER_RESULT_WINDOW_SETTING =
        Setting.intSetting("index.max_inner_result_window", 100, 1, Property.Dynamic, Property.IndexScope);

    /**
     * Index setting describing for NGramTokenizer and NGramTokenFilter
     * the maximum difference between
     * max_gram (maximum length of characters in a gram) and
     * min_gram (minimum length of characters in a gram).
     * The default value is 1 as this is default difference in NGramTokenizer,
     * and is defensive as it prevents generating too many index terms.
     */
    public static final Setting<Integer> MAX_NGRAM_DIFF_SETTING =
        Setting.intSetting("index.max_ngram_diff", 1, 0, Property.Dynamic, Property.IndexScope);

    /**
     * Index setting describing for ShingleTokenFilter
     * the maximum difference between
     * max_shingle_size and min_shingle_size.
     * The default value is 3 is defensive as it prevents generating too many tokens.
     */
    public static final Setting<Integer> MAX_SHINGLE_DIFF_SETTING =
        Setting.intSetting("index.max_shingle_diff", 3, 0, Property.Dynamic, Property.IndexScope);

    /**
     * Index setting describing the maximum value of allowed `docvalue_fields`that can be retrieved
     * per search request. The default maximum of 100 is defensive for the reason that retrieving
     * doc values might incur a per-field per-document seek.
     */
    public static final Setting<Integer> MAX_DOCVALUE_FIELDS_SEARCH_SETTING =
        Setting.intSetting("index.max_docvalue_fields_search", 100, 0, Property.Dynamic, Property.IndexScope);
    /**
     * Index setting describing the maximum size of the rescore window. Defaults to {@link #MAX_RESULT_WINDOW_SETTING}
     * because they both do the same thing: control the size of the heap of hits.
     */
    public static final Setting<Integer> MAX_RESCORE_WINDOW_SETTING =
            Setting.intSetting("index.max_rescore_window", MAX_RESULT_WINDOW_SETTING, 1, Property.Dynamic, Property.IndexScope);
    /**
     * Index setting describing the maximum number of filters clauses that can be used
     * in an adjacency_matrix aggregation. The max number of buckets produced by
     * N filters is (N*N)/2 so a limit of 100 filters is imposed by default.
     */
    public static final Setting<Integer> MAX_ADJACENCY_MATRIX_FILTERS_SETTING =
        Setting.intSetting("index.max_adjacency_matrix_filters", 100, 2, Property.Dynamic, Property.IndexScope);
    public static final TimeValue DEFAULT_REFRESH_INTERVAL = new TimeValue(1, TimeUnit.SECONDS);
    public static final Setting<TimeValue> INDEX_REFRESH_INTERVAL_SETTING =
        Setting.timeSetting("index.refresh_interval", DEFAULT_REFRESH_INTERVAL, new TimeValue(-1, TimeUnit.MILLISECONDS),
            Property.Dynamic, Property.IndexScope);
    public static final Setting<ByteSizeValue> INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING =
        Setting.byteSizeSetting("index.translog.flush_threshold_size", new ByteSizeValue(512, ByteSizeUnit.MB), Property.Dynamic,
            Property.IndexScope);

    /**
     * Controls how long translog files that are no longer needed for persistence reasons
     * will be kept around before being deleted. A longer retention policy is useful to increase
     * the chance of ops based recoveries.
     **/
    public static final Setting<TimeValue> INDEX_TRANSLOG_RETENTION_AGE_SETTING =
        Setting.timeSetting("index.translog.retention.age", TimeValue.timeValueHours(12), TimeValue.timeValueMillis(-1), Property.Dynamic,
            Property.IndexScope);

    /**
     * Controls how many translog files that are no longer needed for persistence reasons
     * will be kept around before being deleted. Keeping more files is useful to increase
     * the chance of ops based recoveries.
     **/
    public static final Setting<ByteSizeValue> INDEX_TRANSLOG_RETENTION_SIZE_SETTING =
        Setting.byteSizeSetting("index.translog.retention.size", new ByteSizeValue(512, ByteSizeUnit.MB), Property.Dynamic,
            Property.IndexScope);

    /**
     * The maximum size of a translog generation. This is independent of the maximum size of
     * translog operations that have not been flushed.
     */
    public static final Setting<ByteSizeValue> INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING =
            Setting.byteSizeSetting(
                    "index.translog.generation_threshold_size",
                    new ByteSizeValue(64, ByteSizeUnit.MB),
                    /*
                     * An empty translog occupies 43 bytes on disk. If the generation threshold is
                     * below this, the flush thread can get stuck in an infinite loop repeatedly
                     * rolling the generation as every new generation will already exceed the
                     * generation threshold. However, small thresholds are useful for testing so we
                     * do not add a large lower bound here.
                     */
                    new ByteSizeValue(64, ByteSizeUnit.BYTES),
                    new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
                    new Property[]{Property.Dynamic, Property.IndexScope});

    /**
     * Index setting to enable / disable deletes garbage collection.
     * This setting is realtime updateable
     */
    public static final TimeValue DEFAULT_GC_DELETES = TimeValue.timeValueSeconds(60);
    public static final Setting<TimeValue> INDEX_GC_DELETES_SETTING =
        Setting.timeSetting("index.gc_deletes", DEFAULT_GC_DELETES, new TimeValue(-1, TimeUnit.MILLISECONDS), Property.Dynamic,
            Property.IndexScope);
    /**
     * The maximum number of refresh listeners allows on this shard.
     */
    public static final Setting<Integer> MAX_REFRESH_LISTENERS_PER_SHARD = Setting.intSetting("index.max_refresh_listeners", 1000, 0,
            Property.Dynamic, Property.IndexScope);

    /**
     * The maximum number of slices allowed in a scroll request
     */
    public static final Setting<Integer> MAX_SLICES_PER_SCROLL = Setting.intSetting("index.max_slices_per_scroll",
        1024, 1, Property.Dynamic, Property.IndexScope);

    public static final String INDEX_MAPPING_SINGLE_TYPE_SETTING_KEY = "index.mapping.single_type";
    private static final Setting<Boolean> INDEX_MAPPING_SINGLE_TYPE_SETTING; // private - should not be registered
    static {
        Function<Settings, String> defValue = settings -> {
            boolean singleType = true;
            if (settings.getAsVersion(IndexMetaData.SETTING_VERSION_CREATED, null) != null) {
                singleType = Version.indexCreated(settings).onOrAfter(Version.V_6_0_0_alpha1);
            }
            return Boolean.valueOf(singleType).toString();
        };
        INDEX_MAPPING_SINGLE_TYPE_SETTING = Setting.boolSetting(INDEX_MAPPING_SINGLE_TYPE_SETTING_KEY, defValue, Property.IndexScope,
            Property.Final);
    }

    private final Index index;
    private final Version version;
    private final Logger logger;
    private final String nodeName;
    private final Settings nodeSettings;
    private final int numberOfShards;
    // volatile fields are updated via #updateIndexMetaData(IndexMetaData) under lock
    private volatile Settings settings;
    private volatile IndexMetaData indexMetaData;
    private volatile List<String> defaultFields;
    private final boolean queryStringLenient;
    private final boolean queryStringAnalyzeWildcard;
    private final boolean queryStringAllowLeadingWildcard;
    private final boolean defaultAllowUnmappedFields;
    private volatile Translog.Durability durability;
    private final TimeValue syncInterval;
    private volatile TimeValue refreshInterval;
    private volatile ByteSizeValue flushThresholdSize;
    private volatile TimeValue translogRetentionAge;
    private volatile ByteSizeValue translogRetentionSize;
    private volatile ByteSizeValue generationThresholdSize;
    private final MergeSchedulerConfig mergeSchedulerConfig;
    private final MergePolicyConfig mergePolicyConfig;
    private final IndexSortConfig indexSortConfig;
    private final IndexScopedSettings scopedSettings;
    private long gcDeletesInMillis = DEFAULT_GC_DELETES.millis();
    private volatile boolean warmerEnabled;
    private volatile int maxResultWindow;
    private volatile int maxInnerResultWindow;
    private volatile int maxAdjacencyMatrixFilters;
    private volatile int maxRescoreWindow;
    private volatile int maxDocvalueFields;
    private volatile int maxScriptFields;
    private volatile int maxNgramDiff;
    private volatile int maxShingleDiff;
    private volatile boolean TTLPurgeDisabled;
    /**
     * The maximum number of refresh listeners allows on this shard.
     */
    private volatile int maxRefreshListeners;
    /**
     * The maximum number of slices allowed in a scroll request.
     */
    private volatile int maxSlicesPerScroll;
    /**
     * Whether the index is required to have at most one type.
     */
    private final boolean singleType;

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
     * @param indexMetaData the index metadata this settings object is associated with
     * @param nodeSettings the nodes settings this index is allocated on.
     */
    public IndexSettings(final IndexMetaData indexMetaData, final Settings nodeSettings) {
        this(indexMetaData, nodeSettings, IndexScopedSettings.DEFAULT_SCOPED_SETTINGS);
    }

    /**
     * Creates a new {@link IndexSettings} instance. The given node settings will be merged with the settings in the metadata
     * while index level settings will overwrite node settings.
     *
     * @param indexMetaData the index metadata this settings object is associated with
     * @param nodeSettings the nodes settings this index is allocated on.
     */
    public IndexSettings(final IndexMetaData indexMetaData, final Settings nodeSettings, IndexScopedSettings indexScopedSettings) {
        scopedSettings = indexScopedSettings.copy(nodeSettings, indexMetaData);
        this.nodeSettings = nodeSettings;
        this.settings = Settings.builder().put(nodeSettings).put(indexMetaData.getSettings()).build();
        this.index = indexMetaData.getIndex();
        version = Version.indexCreated(settings);
        logger = Loggers.getLogger(getClass(), settings, index);
        nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.indexMetaData = indexMetaData;
        numberOfShards = settings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, null);

        this.queryStringLenient = QUERY_STRING_LENIENT_SETTING.get(settings);
        this.queryStringAnalyzeWildcard = QUERY_STRING_ANALYZE_WILDCARD.get(nodeSettings);
        this.queryStringAllowLeadingWildcard = QUERY_STRING_ALLOW_LEADING_WILDCARD.get(nodeSettings);
        this.defaultAllowUnmappedFields = scopedSettings.get(ALLOW_UNMAPPED);
        this.durability = scopedSettings.get(INDEX_TRANSLOG_DURABILITY_SETTING);
        defaultFields = scopedSettings.get(DEFAULT_FIELD_SETTING);
        syncInterval = INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.get(settings);
        refreshInterval = scopedSettings.get(INDEX_REFRESH_INTERVAL_SETTING);
        flushThresholdSize = scopedSettings.get(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING);
        translogRetentionAge = scopedSettings.get(INDEX_TRANSLOG_RETENTION_AGE_SETTING);
        translogRetentionSize = scopedSettings.get(INDEX_TRANSLOG_RETENTION_SIZE_SETTING);
        generationThresholdSize = scopedSettings.get(INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING);
        mergeSchedulerConfig = new MergeSchedulerConfig(this);
        gcDeletesInMillis = scopedSettings.get(INDEX_GC_DELETES_SETTING).getMillis();
        warmerEnabled = scopedSettings.get(INDEX_WARMER_ENABLED_SETTING);
        maxResultWindow = scopedSettings.get(MAX_RESULT_WINDOW_SETTING);
        maxInnerResultWindow = scopedSettings.get(MAX_INNER_RESULT_WINDOW_SETTING);
        maxAdjacencyMatrixFilters = scopedSettings.get(MAX_ADJACENCY_MATRIX_FILTERS_SETTING);
        maxRescoreWindow = scopedSettings.get(MAX_RESCORE_WINDOW_SETTING);
        maxDocvalueFields = scopedSettings.get(MAX_DOCVALUE_FIELDS_SEARCH_SETTING);
        maxScriptFields = scopedSettings.get(MAX_SCRIPT_FIELDS_SETTING);
        maxNgramDiff = scopedSettings.get(MAX_NGRAM_DIFF_SETTING);
        maxShingleDiff = scopedSettings.get(MAX_SHINGLE_DIFF_SETTING);
        TTLPurgeDisabled = scopedSettings.get(INDEX_TTL_DISABLE_PURGE_SETTING);
        maxRefreshListeners = scopedSettings.get(MAX_REFRESH_LISTENERS_PER_SHARD);
        maxSlicesPerScroll = scopedSettings.get(MAX_SLICES_PER_SCROLL);
        this.mergePolicyConfig = new MergePolicyConfig(logger, this);
        this.indexSortConfig = new IndexSortConfig(this);
        singleType = INDEX_MAPPING_SINGLE_TYPE_SETTING.get(indexMetaData.getSettings()); // get this from metadata - it's not registered
        if ((singleType || version.before(Version.V_6_0_0_alpha1)) == false) {
            throw new AssertionError(index.toString()  + "multiple types are only allowed on pre 6.x indices but version is: ["
                + version + "]");
        }

        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING, mergePolicyConfig::setNoCFSRatio);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING, mergePolicyConfig::setExpungeDeletesAllowed);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING, mergePolicyConfig::setFloorSegmentSetting);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING, mergePolicyConfig::setMaxMergesAtOnce);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT_SETTING, mergePolicyConfig::setMaxMergesAtOnceExplicit);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING, mergePolicyConfig::setMaxMergedSegment);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING, mergePolicyConfig::setSegmentsPerTier);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_RECLAIM_DELETES_WEIGHT_SETTING, mergePolicyConfig::setReclaimDeletesWeight);

        scopedSettings.addSettingsUpdateConsumer(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING, MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING,
            mergeSchedulerConfig::setMaxThreadAndMergeCount);
        scopedSettings.addSettingsUpdateConsumer(MergeSchedulerConfig.AUTO_THROTTLE_SETTING, mergeSchedulerConfig::setAutoThrottle);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_DURABILITY_SETTING, this::setTranslogDurability);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TTL_DISABLE_PURGE_SETTING, this::setTTLPurgeDisabled);
        scopedSettings.addSettingsUpdateConsumer(MAX_RESULT_WINDOW_SETTING, this::setMaxResultWindow);
        scopedSettings.addSettingsUpdateConsumer(MAX_INNER_RESULT_WINDOW_SETTING, this::setMaxInnerResultWindow);
        scopedSettings.addSettingsUpdateConsumer(MAX_ADJACENCY_MATRIX_FILTERS_SETTING, this::setMaxAdjacencyMatrixFilters);
        scopedSettings.addSettingsUpdateConsumer(MAX_RESCORE_WINDOW_SETTING, this::setMaxRescoreWindow);
        scopedSettings.addSettingsUpdateConsumer(MAX_DOCVALUE_FIELDS_SEARCH_SETTING, this::setMaxDocvalueFields);
        scopedSettings.addSettingsUpdateConsumer(MAX_SCRIPT_FIELDS_SETTING, this::setMaxScriptFields);
        scopedSettings.addSettingsUpdateConsumer(MAX_NGRAM_DIFF_SETTING, this::setMaxNgramDiff);
        scopedSettings.addSettingsUpdateConsumer(MAX_SHINGLE_DIFF_SETTING, this::setMaxShingleDiff);
        scopedSettings.addSettingsUpdateConsumer(INDEX_WARMER_ENABLED_SETTING, this::setEnableWarmer);
        scopedSettings.addSettingsUpdateConsumer(INDEX_GC_DELETES_SETTING, this::setGCDeletes);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING, this::setTranslogFlushThresholdSize);
        scopedSettings.addSettingsUpdateConsumer(
                INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING,
                this::setGenerationThresholdSize);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_RETENTION_AGE_SETTING, this::setTranslogRetentionAge);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_RETENTION_SIZE_SETTING, this::setTranslogRetentionSize);
        scopedSettings.addSettingsUpdateConsumer(INDEX_REFRESH_INTERVAL_SETTING, this::setRefreshInterval);
        scopedSettings.addSettingsUpdateConsumer(MAX_REFRESH_LISTENERS_PER_SHARD, this::setMaxRefreshListeners);
        scopedSettings.addSettingsUpdateConsumer(MAX_SLICES_PER_SCROLL, this::setMaxSlicesPerScroll);
        scopedSettings.addSettingsUpdateConsumer(DEFAULT_FIELD_SETTING, this::setDefaultFields);
    }

    private void setTranslogFlushThresholdSize(ByteSizeValue byteSizeValue) {
        this.flushThresholdSize = byteSizeValue;
    }

    private void setTranslogRetentionSize(ByteSizeValue byteSizeValue) {
        this.translogRetentionSize = byteSizeValue;
    }

    private void setTranslogRetentionAge(TimeValue age) {
        this.translogRetentionAge = age;
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
    public Settings getSettings() { return settings; }

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
        return customDataPath() != null;
    }

    /**
     * Returns the customDataPath for this index, if configured. <code>null</code> o.w.
     */
    public String customDataPath() {
        return settings.get(IndexMetaData.SETTING_DATA_PATH);
    }

    /**
     * Returns the version the index was created on.
     * @see Version#indexCreated(Settings)
     */
    public Version getIndexVersionCreated() {
        return version;
    }

    /**
     * Returns the current node name
     */
    public String getNodeName() {
        return nodeName;
    }

    /**
     * Returns the current IndexMetaData for this index
     */
    public IndexMetaData getIndexMetaData() {
        return indexMetaData;
    }

    /**
     * Returns the number of shards this index has.
     */
    public int getNumberOfShards() { return numberOfShards; }

    /**
     * Returns the number of replicas this index has.
     */
    public int getNumberOfReplicas() { return settings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, null); }

    /**
     * Returns whether the index enforces at most one type.
     */
    public boolean isSingleType() { return singleType; }

    /**
     * Returns the node settings. The settings returned from {@link #getSettings()} are a merged version of the
     * index settings and the node settings where node settings are overwritten by index settings.
     */
    public Settings getNodeSettings() {
        return nodeSettings;
    }

    /**
     * Updates the settings and index metadata and notifies all registered settings consumers with the new settings iff at least one setting has changed.
     *
     * @return <code>true</code> iff any setting has been updated otherwise <code>false</code>.
     */
    public synchronized boolean updateIndexMetaData(IndexMetaData indexMetaData) {
        final Settings newSettings = indexMetaData.getSettings();
        if (version.equals(Version.indexCreated(newSettings)) == false) {
            throw new IllegalArgumentException("version mismatch on settings update expected: " + version + " but was: " + Version.indexCreated(newSettings));
        }
        final String newUUID = newSettings.get(IndexMetaData.SETTING_INDEX_UUID, IndexMetaData.INDEX_UUID_NA_VALUE);
        if (newUUID.equals(getUUID()) == false) {
            throw new IllegalArgumentException("uuid mismatch on settings update expected: " + getUUID() + " but was: " + newUUID);
        }
        this.indexMetaData = indexMetaData;
        final Settings existingSettings = this.settings;
        if (existingSettings.filter(IndexScopedSettings.INDEX_SETTINGS_KEY_PREDICATE)
            .equals(newSettings.filter(IndexScopedSettings.INDEX_SETTINGS_KEY_PREDICATE))) {
            // nothing to update, same settings
            return false;
        }
        scopedSettings.applySettings(newSettings);
        this.settings = Settings.builder().put(nodeSettings).put(newSettings).build();
        return true;
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

    /**
     * Returns this interval in which the shards of this index are asynchronously refreshed. <tt>-1</tt> means async refresh is disabled.
     */
    public TimeValue getRefreshInterval() {
        return refreshInterval;
    }

    /**
     * Returns the transaction log threshold size when to forcefully flush the index and clear the transaction log.
     */
    public ByteSizeValue getFlushThresholdSize() { return flushThresholdSize; }

    /**
     * Returns the transaction log retention size which controls how much of the translog is kept around to allow for ops based recoveries
     */
    public ByteSizeValue getTranslogRetentionSize() { return translogRetentionSize; }

    /**
     * Returns the transaction log retention age which controls the maximum age (time from creation) that translog files will be kept around
     */
    public TimeValue getTranslogRetentionAge() { return translogRetentionAge; }

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
    public MergeSchedulerConfig getMergeSchedulerConfig() { return mergeSchedulerConfig; }

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
     * Returns the max number of filters in adjacency_matrix aggregation search requests
     */
    public int getMaxAdjacencyMatrixFilters() {
        return this.maxAdjacencyMatrixFilters;
    }

    private void setMaxAdjacencyMatrixFilters(int maxAdjacencyFilters) {
        this.maxAdjacencyMatrixFilters = maxAdjacencyFilters;
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
     * Returns the maximum allowed difference between max and min length of ngram
     */
    public int getMaxNgramDiff() { return this.maxNgramDiff; }

    private void setMaxNgramDiff(int maxNgramDiff) { this.maxNgramDiff = maxNgramDiff; }

    /**
     * Returns the maximum allowed difference between max and min shingle_size
     */
    public int getMaxShingleDiff() { return this.maxShingleDiff; }

    private void setMaxShingleDiff(int maxShingleDiff) { this.maxShingleDiff = maxShingleDiff; }

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
    public MergePolicy getMergePolicy() {
        return mergePolicyConfig.getMergePolicy();
    }

    /**
     * Returns <code>true</code> if the TTL purge is disabled for this index. Default is <code>false</code>
     */
    public boolean isTTLPurgeDisabled() {
        return TTLPurgeDisabled;
    }

    private  void setTTLPurgeDisabled(boolean ttlPurgeDisabled) {
        this.TTLPurgeDisabled = ttlPurgeDisabled;
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
     * Returns the index sort config that should be used for this index.
     */
    public IndexSortConfig getIndexSortConfig() {
        return indexSortConfig;
    }

    public IndexScopedSettings getScopedSettings() { return scopedSettings;}
}
