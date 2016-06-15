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

import org.apache.lucene.index.MergePolicy;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.translog.Translog;

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * This class encapsulates all index level settings and handles settings updates.
 * It's created per index and available to all index level classes and allows them to retrieve
 * the latest updated settings instance. Classes that need to listen to settings updates can register
 * a settings consumer at index creation via {@link IndexModule#addSettingsUpdateConsumer(Setting, Consumer)} that will
 * be called for each settings update.
 */
public final class IndexSettings {

    public static final Setting<String> DEFAULT_FIELD_SETTING =
        new Setting<>("index.query.default_field", AllFieldMapper.NAME, Function.identity(), Property.IndexScope);
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
     * Index setting describing the maximum size of the rescore window. Defaults to {@link #MAX_RESULT_WINDOW_SETTING}
     * because they both do the same thing: control the size of the heap of hits.
     */
    public static final Setting<Integer> MAX_RESCORE_WINDOW_SETTING =
            Setting.intSetting("index.max_rescore_window", MAX_RESULT_WINDOW_SETTING, 1, Property.Dynamic, Property.IndexScope);
    public static final TimeValue DEFAULT_REFRESH_INTERVAL = new TimeValue(1, TimeUnit.SECONDS);
    public static final Setting<TimeValue> INDEX_REFRESH_INTERVAL_SETTING =
        Setting.timeSetting("index.refresh_interval", DEFAULT_REFRESH_INTERVAL, new TimeValue(-1, TimeUnit.MILLISECONDS),
            Property.Dynamic, Property.IndexScope);
    public static final Setting<ByteSizeValue> INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING =
        Setting.byteSizeSetting("index.translog.flush_threshold_size", new ByteSizeValue(512, ByteSizeUnit.MB), Property.Dynamic,
            Property.IndexScope);


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

    private final Index index;
    private final Version version;
    private final ESLogger logger;
    private final String nodeName;
    private final Settings nodeSettings;
    private final int numberOfShards;
    private final boolean isShadowReplicaIndex;
    private final ParseFieldMatcher parseFieldMatcher;
    // volatile fields are updated via #updateIndexMetaData(IndexMetaData) under lock
    private volatile Settings settings;
    private volatile IndexMetaData indexMetaData;
    private final String defaultField;
    private final boolean queryStringLenient;
    private final boolean queryStringAnalyzeWildcard;
    private final boolean queryStringAllowLeadingWildcard;
    private final boolean defaultAllowUnmappedFields;
    private final Predicate<String> indexNameMatcher;
    private volatile Translog.Durability durability;
    private final TimeValue syncInterval;
    private volatile TimeValue refreshInterval;
    private volatile ByteSizeValue flushThresholdSize;
    private final MergeSchedulerConfig mergeSchedulerConfig;
    private final MergePolicyConfig mergePolicyConfig;
    private final IndexScopedSettings scopedSettings;
    private long gcDeletesInMillis = DEFAULT_GC_DELETES.millis();
    private volatile boolean warmerEnabled;
    private volatile int maxResultWindow;
    private volatile int maxRescoreWindow;
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
     * Returns the default search field for this index.
     */
    public String getDefaultField() {
        return defaultField;
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
        this(indexMetaData, nodeSettings, (index) -> Regex.simpleMatch(index, indexMetaData.getIndex().getName()), IndexScopedSettings.DEFAULT_SCOPED_SETTINGS);
    }

    /**
     * Creates a new {@link IndexSettings} instance. The given node settings will be merged with the settings in the metadata
     * while index level settings will overwrite node settings.
     *
     * @param indexMetaData the index metadata this settings object is associated with
     * @param nodeSettings the nodes settings this index is allocated on.
     * @param indexNameMatcher a matcher that can resolve an expression to the index name or index alias
     */
    public IndexSettings(final IndexMetaData indexMetaData, final Settings nodeSettings, final Predicate<String> indexNameMatcher, IndexScopedSettings indexScopedSettings) {
        scopedSettings = indexScopedSettings.copy(nodeSettings, indexMetaData);
        this.nodeSettings = nodeSettings;
        this.settings = Settings.builder().put(nodeSettings).put(indexMetaData.getSettings()).build();
        this.index = indexMetaData.getIndex();
        version = Version.indexCreated(settings);
        logger = Loggers.getLogger(getClass(), settings, index);
        nodeName = settings.get("node.name", "");
        this.indexMetaData = indexMetaData;
        numberOfShards = settings.getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, null);
        isShadowReplicaIndex = IndexMetaData.isIndexUsingShadowReplicas(settings);

        this.defaultField = DEFAULT_FIELD_SETTING.get(settings);
        this.queryStringLenient = QUERY_STRING_LENIENT_SETTING.get(settings);
        this.queryStringAnalyzeWildcard = QUERY_STRING_ANALYZE_WILDCARD.get(nodeSettings);
        this.queryStringAllowLeadingWildcard = QUERY_STRING_ALLOW_LEADING_WILDCARD.get(nodeSettings);
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
        this.defaultAllowUnmappedFields = scopedSettings.get(ALLOW_UNMAPPED);
        this.indexNameMatcher = indexNameMatcher;
        this.durability = scopedSettings.get(INDEX_TRANSLOG_DURABILITY_SETTING);
        syncInterval = INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.get(settings);
        refreshInterval = scopedSettings.get(INDEX_REFRESH_INTERVAL_SETTING);
        flushThresholdSize = scopedSettings.get(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING);
        mergeSchedulerConfig = new MergeSchedulerConfig(this);
        gcDeletesInMillis = scopedSettings.get(INDEX_GC_DELETES_SETTING).getMillis();
        warmerEnabled = scopedSettings.get(INDEX_WARMER_ENABLED_SETTING);
        maxResultWindow = scopedSettings.get(MAX_RESULT_WINDOW_SETTING);
        maxRescoreWindow = scopedSettings.get(MAX_RESCORE_WINDOW_SETTING);
        TTLPurgeDisabled = scopedSettings.get(INDEX_TTL_DISABLE_PURGE_SETTING);
        maxRefreshListeners = scopedSettings.get(MAX_REFRESH_LISTENERS_PER_SHARD);
        maxSlicesPerScroll = scopedSettings.get(MAX_SLICES_PER_SCROLL);
        this.mergePolicyConfig = new MergePolicyConfig(logger, this);
        assert indexNameMatcher.test(indexMetaData.getIndex().getName());

        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_COMPOUND_FORMAT_SETTING, mergePolicyConfig::setNoCFSRatio);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING, mergePolicyConfig::setExpungeDeletesAllowed);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING, mergePolicyConfig::setFloorSegmentSetting);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING, mergePolicyConfig::setMaxMergesAtOnce);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_EXPLICIT_SETTING, mergePolicyConfig::setMaxMergesAtOnceExplicit);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING, mergePolicyConfig::setMaxMergedSegment);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING, mergePolicyConfig::setSegmentsPerTier);
        scopedSettings.addSettingsUpdateConsumer(MergePolicyConfig.INDEX_MERGE_POLICY_RECLAIM_DELETES_WEIGHT_SETTING, mergePolicyConfig::setReclaimDeletesWeight);
        scopedSettings.addSettingsUpdateConsumer(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING, mergeSchedulerConfig::setMaxThreadCount);
        scopedSettings.addSettingsUpdateConsumer(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING, mergeSchedulerConfig::setMaxMergeCount);
        scopedSettings.addSettingsUpdateConsumer(MergeSchedulerConfig.AUTO_THROTTLE_SETTING, mergeSchedulerConfig::setAutoThrottle);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_DURABILITY_SETTING, this::setTranslogDurability);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TTL_DISABLE_PURGE_SETTING, this::setTTLPurgeDisabled);
        scopedSettings.addSettingsUpdateConsumer(MAX_RESULT_WINDOW_SETTING, this::setMaxResultWindow);
        scopedSettings.addSettingsUpdateConsumer(MAX_RESCORE_WINDOW_SETTING, this::setMaxRescoreWindow);
        scopedSettings.addSettingsUpdateConsumer(INDEX_WARMER_ENABLED_SETTING, this::setEnableWarmer);
        scopedSettings.addSettingsUpdateConsumer(INDEX_GC_DELETES_SETTING, this::setGCDeletes);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING, this::setTranslogFlushThresholdSize);
        scopedSettings.addSettingsUpdateConsumer(INDEX_REFRESH_INTERVAL_SETTING, this::setRefreshInterval);
        scopedSettings.addSettingsUpdateConsumer(MAX_REFRESH_LISTENERS_PER_SHARD, this::setMaxRefreshListeners);
        scopedSettings.addSettingsUpdateConsumer(MAX_SLICES_PER_SCROLL, this::setMaxSlicesPerScroll);
    }

    private void setTranslogFlushThresholdSize(ByteSizeValue byteSizeValue) {
        this.flushThresholdSize = byteSizeValue;
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
     * Returns <code>true</code> iff the given settings indicate that the index
     * associated with these settings allocates it's shards on a shared
     * filesystem.
     */
    public boolean isOnSharedFilesystem() {
        return IndexMetaData.isOnSharedFilesystem(getSettings());
    }

    /**
     * Returns <code>true</code> iff the given settings indicate that the index associated
     * with these settings uses shadow replicas. Otherwise <code>false</code>. The default
     * setting for this is <code>false</code>.
     */
    public boolean isIndexUsingShadowReplicas() {
        return IndexMetaData.isOnSharedFilesystem(getSettings());
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
     * Returns <code>true</code> iff this index uses shadow replicas.
     * @see IndexMetaData#isIndexUsingShadowReplicas(Settings)
     */
    public boolean isShadowReplicaIndex() { return isShadowReplicaIndex; }

    /**
     * Returns the node settings. The settings returned from {@link #getSettings()} are a merged version of the
     * index settings and the node settings where node settings are overwritten by index settings.
     */
    public Settings getNodeSettings() {
        return nodeSettings;
    }

    /**
     * Returns a {@link ParseFieldMatcher} for this index.
     */
    public ParseFieldMatcher getParseFieldMatcher() { return parseFieldMatcher; }

    /**
     * Returns <code>true</code> if the given expression matches the index name or one of it's aliases
     */
    public boolean matchesIndexName(String expression) {
        return indexNameMatcher.test(expression);
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
        if (existingSettings.filter(IndexScopedSettings.INDEX_SETTINGS_KEY_PREDICATE).getAsMap().equals(newSettings.filter(IndexScopedSettings.INDEX_SETTINGS_KEY_PREDICATE).getAsMap())) {
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
     * Returns the maximum rescore window for search requests.
     */
    public int getMaxRescoreWindow() {
        return maxRescoreWindow;
    }

    private void setMaxRescoreWindow(int maxRescoreWindow) {
        this.maxRescoreWindow = maxRescoreWindow;
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

    IndexScopedSettings getScopedSettings() { return scopedSettings;}
}
