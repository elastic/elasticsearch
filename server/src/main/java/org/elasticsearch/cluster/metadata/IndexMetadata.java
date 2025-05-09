/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNodeFilters;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.IndexMetadataUpdater;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.cluster.metadata.Metadata.CONTEXT_MODE_PARAM;
import static org.elasticsearch.cluster.metadata.Metadata.DEDUPLICATED_MAPPINGS_PARAM;
import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.AND;
import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.OR;
import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.validateIpValue;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_PARTIAL_SETTING_KEY;

public class IndexMetadata implements Diffable<IndexMetadata>, ToXContentFragment {

    private static final Logger logger = LogManager.getLogger(IndexMetadata.class);

    public static final ClusterBlock INDEX_READ_ONLY_BLOCK = new ClusterBlock(
        5,
        "index read-only (api)",
        false,
        false,
        false,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE)
    );
    public static final ClusterBlock INDEX_READ_BLOCK = new ClusterBlock(
        7,
        "index read (api)",
        false,
        false,
        false,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.READ)
    );
    public static final ClusterBlock INDEX_WRITE_BLOCK = new ClusterBlock(
        8,
        "index write (api)",
        false,
        false,
        false,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.WRITE)
    );
    public static final ClusterBlock INDEX_METADATA_BLOCK = new ClusterBlock(
        9,
        "index metadata (api)",
        false,
        false,
        false,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.METADATA_WRITE, ClusterBlockLevel.METADATA_READ)
    );
    public static final ClusterBlock INDEX_READ_ONLY_ALLOW_DELETE_BLOCK = new ClusterBlock(
        12,
        "disk usage exceeded flood-stage watermark, index has read-only-allow-delete block; for more information, see "
            + ReferenceDocs.FLOOD_STAGE_WATERMARK,
        false,
        false,
        true,
        RestStatus.TOO_MANY_REQUESTS,
        EnumSet.of(ClusterBlockLevel.WRITE)
    );
    public static final ClusterBlock INDEX_REFRESH_BLOCK = new ClusterBlock(
        14,
        "index refresh blocked, waiting for shard(s) to be started",
        true,
        false,
        false,
        RestStatus.REQUEST_TIMEOUT,
        EnumSet.of(ClusterBlockLevel.REFRESH)
    );

    // 'event.ingested' (part of Elastic Common Schema) range is tracked in cluster state, along with @timestamp
    public static final String EVENT_INGESTED_FIELD_NAME = "event.ingested";

    @Nullable
    public String getDownsamplingInterval() {
        return settings.get(IndexMetadata.INDEX_DOWNSAMPLE_INTERVAL_KEY);
    }

    public enum State implements Writeable {
        OPEN((byte) 0),
        CLOSE((byte) 1);

        private final byte id;

        State(byte id) {
            this.id = id;
        }

        public byte id() {
            return this.id;
        }

        public static State fromId(byte id) {
            if (id == 0) {
                return OPEN;
            } else if (id == 1) {
                return CLOSE;
            }
            throw new IllegalStateException("No state match for id [" + id + "]");
        }

        public static State readFrom(StreamInput in) throws IOException {
            byte id = in.readByte();
            return switch (id) {
                case 0 -> OPEN;
                case 1 -> CLOSE;
                default -> throw new IllegalStateException("No state match for id [" + id + "]");
            };
        }

        public static State fromString(String state) {
            if ("open".equals(state)) {
                return OPEN;
            } else if ("close".equals(state)) {
                return CLOSE;
            }
            throw new IllegalStateException("No state match for [" + state + "]");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }
    }

    /**
     * This is a safety limit that should only be exceeded in very rare and special cases. The assumption is that
     * 99% of the users have less than 1024 shards per index. We also make it a hard check that requires restart of nodes
     * if a cluster should allow to create more than 1024 shards per index. NOTE: this does not limit the number of shards
     * per cluster. this also prevents creating stuff like a new index with millions of shards by accident which essentially
     * kills the entire cluster with OOM on the spot.
     */
    public static final String PER_INDEX_MAX_NUMBER_OF_SHARDS = "1024";

    static Setting<Integer> buildNumberOfShardsSetting() {
        final int maxNumShards = Integer.parseInt(System.getProperty("es.index.max_number_of_shards", PER_INDEX_MAX_NUMBER_OF_SHARDS));
        if (maxNumShards < 1) {
            throw new IllegalArgumentException("es.index.max_number_of_shards must be > 0");
        }
        return Setting.intSetting(SETTING_NUMBER_OF_SHARDS, 1, 1, maxNumShards, Property.IndexScope, Property.Final);
    }

    public static final String INDEX_SETTING_PREFIX = "index.";
    public static final String SETTING_NUMBER_OF_SHARDS = "index.number_of_shards";
    public static final Setting<Integer> INDEX_NUMBER_OF_SHARDS_SETTING = buildNumberOfShardsSetting();
    public static final String SETTING_NUMBER_OF_REPLICAS = "index.number_of_replicas";
    public static final Setting<Integer> INDEX_NUMBER_OF_REPLICAS_SETTING = Setting.intSetting(
        SETTING_NUMBER_OF_REPLICAS,
        1,
        0,
        Property.Dynamic,
        Property.IndexScope
    );

    public static final String SETTING_ROUTING_PARTITION_SIZE = "index.routing_partition_size";
    public static final Setting<Integer> INDEX_ROUTING_PARTITION_SIZE_SETTING = Setting.intSetting(
        SETTING_ROUTING_PARTITION_SIZE,
        1,
        1,
        Property.Final,
        Property.IndexScope
    );

    @SuppressWarnings("Convert2Diamond") // since some IntelliJs mysteriously report an error if an <Integer> is replaced with <> here:
    public static final Setting<Integer> INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING = Setting.intSetting(
        "index.number_of_routing_shards",
        INDEX_NUMBER_OF_SHARDS_SETTING,
        1,
        new Setting.Validator<Integer>() {

            @Override
            public void validate(final Integer value) {

            }

            @Override
            public void validate(final Integer numRoutingShards, final Map<Setting<?>, Object> settings) {
                int numShards = (int) settings.get(INDEX_NUMBER_OF_SHARDS_SETTING);
                if (numRoutingShards < numShards) {
                    throw new IllegalArgumentException(
                        "index.number_of_routing_shards [" + numRoutingShards + "] must be >= index.number_of_shards [" + numShards + "]"
                    );
                }
                getRoutingFactor(numShards, numRoutingShards);
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final List<Setting<?>> settings = List.of(INDEX_NUMBER_OF_SHARDS_SETTING);
                return settings.iterator();
            }

        },
        Property.IndexScope
    );

    public static final String SETTING_AUTO_EXPAND_REPLICAS = "index.auto_expand_replicas";
    public static final Setting<AutoExpandReplicas> INDEX_AUTO_EXPAND_REPLICAS_SETTING = AutoExpandReplicas.SETTING;

    public enum APIBlock implements Writeable {
        READ_ONLY("read_only", INDEX_READ_ONLY_BLOCK, Property.ServerlessPublic),
        READ("read", INDEX_READ_BLOCK, Property.ServerlessPublic),
        WRITE("write", INDEX_WRITE_BLOCK, Property.ServerlessPublic),
        METADATA("metadata", INDEX_METADATA_BLOCK, Property.ServerlessPublic),
        READ_ONLY_ALLOW_DELETE("read_only_allow_delete", INDEX_READ_ONLY_ALLOW_DELETE_BLOCK);

        final String name;
        final String settingName;
        final Setting<Boolean> setting;
        final ClusterBlock block;

        APIBlock(String name, ClusterBlock block) {
            this.name = name;
            this.settingName = "index.blocks." + name;
            this.setting = Setting.boolSetting(settingName, false, Property.Dynamic, Property.IndexScope);
            this.block = block;
        }

        APIBlock(String name, ClusterBlock block, Property serverlessProperty) {
            this.name = name;
            this.settingName = "index.blocks." + name;
            this.setting = Setting.boolSetting(settingName, false, Property.Dynamic, Property.IndexScope, serverlessProperty);
            this.block = block;
        }

        public String settingName() {
            return settingName;
        }

        public Setting<Boolean> setting() {
            return setting;
        }

        public ClusterBlock getBlock() {
            return block;
        }

        public static APIBlock fromName(String name) {
            for (APIBlock block : APIBlock.values()) {
                if (block.name.equals(name)) {
                    return block;
                }
            }
            throw new IllegalArgumentException("No block found with name " + name);
        }

        public static APIBlock fromSetting(String settingName) {
            for (APIBlock block : APIBlock.values()) {
                if (block.settingName.equals(settingName)) {
                    return block;
                }
            }
            throw new IllegalArgumentException("No block found with setting name " + settingName);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(ordinal());
        }

        public static APIBlock readFrom(StreamInput input) throws IOException {
            return APIBlock.values()[input.readVInt()];
        }
    }

    public static final String SETTING_READ_ONLY = APIBlock.READ_ONLY.settingName();
    public static final Setting<Boolean> INDEX_READ_ONLY_SETTING = APIBlock.READ_ONLY.setting();

    public static final String SETTING_BLOCKS_READ = APIBlock.READ.settingName();
    public static final Setting<Boolean> INDEX_BLOCKS_READ_SETTING = APIBlock.READ.setting();

    public static final String SETTING_BLOCKS_WRITE = APIBlock.WRITE.settingName();
    public static final Setting<Boolean> INDEX_BLOCKS_WRITE_SETTING = APIBlock.WRITE.setting();

    public static final String SETTING_BLOCKS_METADATA = APIBlock.METADATA.settingName();
    public static final Setting<Boolean> INDEX_BLOCKS_METADATA_SETTING = APIBlock.METADATA.setting();

    public static final String SETTING_READ_ONLY_ALLOW_DELETE = APIBlock.READ_ONLY_ALLOW_DELETE.settingName();
    public static final Setting<Boolean> INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING = APIBlock.READ_ONLY_ALLOW_DELETE.setting();

    public static final String SETTING_VERSION_CREATED = "index.version.created";

    public static final Setting<IndexVersion> SETTING_INDEX_VERSION_CREATED = Setting.versionIdSetting(
        SETTING_VERSION_CREATED,
        IndexVersions.ZERO,
        IndexVersion::fromId,
        Property.IndexScope,
        Property.PrivateIndex
    );

    public static final String SETTING_VERSION_CREATED_STRING = "index.version.created_string";
    public static final String SETTING_CREATION_DATE = "index.creation_date";

    /**
     * These internal settings are no longer added to new indices. They are deprecated but still defined
     * to retain compatibility with old indexes. TODO: remove in 9.0.
     */
    @Deprecated
    public static final String SETTING_VERSION_UPGRADED = "index.version.upgraded";
    @Deprecated
    public static final String SETTING_VERSION_UPGRADED_STRING = "index.version.upgraded_string";

    public static final String SETTING_VERSION_COMPATIBILITY = "index.version.compatibility";

    /**
     * See {@link #getCompatibilityVersion()}
     */
    public static final Setting<IndexVersion> SETTING_INDEX_VERSION_COMPATIBILITY = Setting.versionIdSetting(
        SETTING_VERSION_COMPATIBILITY,
        SETTING_INDEX_VERSION_CREATED, // fall back to index.version.created
        new Setting.Validator<>() {
            @Override
            public void validate(final IndexVersion compatibilityVersion) {

            }

            @Override
            public void validate(final IndexVersion compatibilityVersion, final Map<Setting<?>, Object> settings) {
                IndexVersion createdVersion = (IndexVersion) settings.get(SETTING_INDEX_VERSION_CREATED);
                if (compatibilityVersion.before(createdVersion)) {
                    throw new IllegalArgumentException(
                        SETTING_VERSION_COMPATIBILITY
                            + " ["
                            + compatibilityVersion.toReleaseVersion()
                            + "] must be >= "
                            + SETTING_VERSION_CREATED
                            + " ["
                            + createdVersion.toReleaseVersion()
                            + "]"
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final List<Setting<?>> settings = List.of(SETTING_INDEX_VERSION_CREATED);
                return settings.iterator();
            }
        },
        Property.IndexScope,
        Property.PrivateIndex
    );

    /**
     * The user provided name for an index. This is the plain string provided by the user when the index was created.
     * It might still contain date math expressions etc. (added in 5.0)
     */
    public static final String SETTING_INDEX_PROVIDED_NAME = "index.provided_name";
    public static final String SETTING_PRIORITY = "index.priority";
    public static final Setting<Integer> INDEX_PRIORITY_SETTING = Setting.intSetting(
        "index.priority",
        1,
        0,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final String SETTING_CREATION_DATE_STRING = "index.creation_date_string";
    public static final String SETTING_INDEX_UUID = "index.uuid";
    public static final String SETTING_HISTORY_UUID = "index.history.uuid";
    public static final String SETTING_DATA_PATH = "index.data_path";
    public static final Setting<String> INDEX_DATA_PATH_SETTING = Setting.simpleString(
        SETTING_DATA_PATH,
        Property.IndexScope,
        Property.DeprecatedWarning
    );
    public static final String INDEX_UUID_NA_VALUE = "_na_";

    public static final String INDEX_ROUTING_REQUIRE_GROUP_PREFIX = "index.routing.allocation.require";
    public static final String INDEX_ROUTING_INCLUDE_GROUP_PREFIX = "index.routing.allocation.include";
    public static final String INDEX_ROUTING_EXCLUDE_GROUP_PREFIX = "index.routing.allocation.exclude";

    public static final Setting.AffixSetting<List<String>> INDEX_ROUTING_REQUIRE_GROUP_SETTING = Setting.prefixKeySetting(
        INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".",
        key -> Setting.stringListSetting(key, value -> validateIpValue(key, value), Property.Dynamic, Property.IndexScope)
    );
    public static final Setting.AffixSetting<List<String>> INDEX_ROUTING_INCLUDE_GROUP_SETTING = Setting.prefixKeySetting(
        INDEX_ROUTING_INCLUDE_GROUP_PREFIX + ".",
        key -> Setting.stringListSetting(key, value -> validateIpValue(key, value), Property.Dynamic, Property.IndexScope)
    );
    public static final Setting.AffixSetting<List<String>> INDEX_ROUTING_EXCLUDE_GROUP_SETTING = Setting.prefixKeySetting(
        INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + ".",
        key -> Setting.stringListSetting(key, value -> validateIpValue(key, value), Property.Dynamic, Property.IndexScope)
    );
    public static final Setting.AffixSetting<List<String>> INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING = Setting.prefixKeySetting(
        "index.routing.allocation.initial_recovery.",
        Setting::stringListSetting
    );

    /**
     * The number of active shard copies to check for before proceeding with a write operation.
     */
    public static final Setting<ActiveShardCount> SETTING_WAIT_FOR_ACTIVE_SHARDS = new Setting<>(
        "index.write.wait_for_active_shards",
        "1",
        ActiveShardCount::parseString,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    public static final String SETTING_INDEX_HIDDEN = "index.hidden";
    /**
     * Whether the index is considered hidden or not. A hidden index will not be resolved in
     * normal wildcard searches unless explicitly allowed
     */
    public static final Setting<Boolean> INDEX_HIDDEN_SETTING = Setting.boolSetting(
        SETTING_INDEX_HIDDEN,
        false,
        Property.Dynamic,
        Property.IndexScope,
        Property.ServerlessPublic
    );

    /**
     * an internal index format description, allowing us to find out if this index is upgraded or needs upgrading
     */
    private static final String INDEX_FORMAT = "index.format";
    public static final Setting<Integer> INDEX_FORMAT_SETTING = Setting.intSetting(
        INDEX_FORMAT,
        0,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static final Setting<List<String>> INDEX_ROUTING_PATH = Setting.stringListSetting(
        "index.routing_path",
        Setting.Property.IndexScope,
        Setting.Property.Final,
        Property.ServerlessPublic
    );

    /**
     * Legacy index setting, kept for 7.x BWC compatibility. This setting has no effect in 8.x. Do not use.
     * TODO: Remove in 9.0
     */
    @Deprecated
    public static final Setting<String> INDEX_ROLLUP_SOURCE_UUID = Setting.simpleString(
        "index.rollup.source.uuid",
        Property.IndexScope,
        Property.PrivateIndex,
        Property.IndexSettingDeprecatedInV7AndRemovedInV8
    );

    /**
     * Legacy index setting, kept for 7.x BWC compatibility. This setting has no effect in 8.x. Do not use.
     * TODO: Remove in 9.0
     */
    @Deprecated
    public static final Setting<String> INDEX_ROLLUP_SOURCE_NAME = Setting.simpleString(
        "index.rollup.source.name",
        Property.IndexScope,
        Property.PrivateIndex,
        Property.IndexSettingDeprecatedInV7AndRemovedInV8
    );

    public static final String KEY_IN_SYNC_ALLOCATIONS = "in_sync_allocations";

    public static final List<String> PARTIALLY_MOUNTED_INDEX_TIER_PREFERENCE = List.of(DataTier.DATA_FROZEN);

    static final String KEY_VERSION = "version";
    static final String KEY_MAPPING_VERSION = "mapping_version";
    static final String KEY_SETTINGS_VERSION = "settings_version";
    static final String KEY_ALIASES_VERSION = "aliases_version";
    static final String KEY_ROUTING_NUM_SHARDS = "routing_num_shards";
    static final String KEY_SETTINGS = "settings";
    static final String KEY_STATE = "state";
    static final String KEY_MAPPINGS = "mappings";
    static final String KEY_MAPPINGS_HASH = "mappings_hash";
    static final String KEY_ALIASES = "aliases";
    static final String KEY_ROLLOVER_INFOS = "rollover_info";
    static final String KEY_MAPPINGS_UPDATED_VERSION = "mappings_updated_version";
    static final String KEY_SYSTEM = "system";
    static final String KEY_TIMESTAMP_RANGE = "timestamp_range";
    static final String KEY_EVENT_INGESTED_RANGE = "event_ingested_range";
    public static final String KEY_PRIMARY_TERMS = "primary_terms";
    public static final String KEY_STATS = "stats";

    public static final String KEY_WRITE_LOAD_FORECAST = "write_load_forecast";

    public static final String KEY_SHARD_SIZE_FORECAST = "shard_size_forecast";

    public static final String KEY_INFERENCE_FIELDS = "field_inference";

    public static final String KEY_RESHARDING = "resharding";

    public static final String INDEX_STATE_FILE_PREFIX = "state-";

    static final TransportVersion STATS_AND_FORECAST_ADDED = TransportVersions.V_8_6_0;

    private final int routingNumShards;
    private final int routingFactor;
    private final int routingPartitionSize;
    private final List<String> routingPaths;

    private final int numberOfShards;
    private final int numberOfReplicas;

    private final Index index;
    private final long version;

    private final long mappingVersion;

    private final long settingsVersion;

    private final long aliasesVersion;

    private final long[] primaryTerms;

    private final State state;

    private final ImmutableOpenMap<String, AliasMetadata> aliases;

    private final Settings settings;

    @Nullable
    private final MappingMetadata mapping;

    private final ImmutableOpenMap<String, InferenceFieldMetadata> inferenceFields;

    private final ImmutableOpenMap<String, DiffableStringMap> customData;

    private final Map<Integer, Set<String>> inSyncAllocationIds;

    private final transient int totalNumberOfShards;

    private final DiscoveryNodeFilters requireFilters;
    private final DiscoveryNodeFilters includeFilters;
    private final DiscoveryNodeFilters excludeFilters;
    private final DiscoveryNodeFilters initialRecoveryFilters;

    private final IndexVersion indexCreatedVersion;
    private final IndexVersion mappingsUpdatedVersion;
    private final IndexVersion indexCompatibilityVersion;

    private final ActiveShardCount waitForActiveShards;
    private final ImmutableOpenMap<String, RolloverInfo> rolloverInfos;
    private final boolean isSystem;
    private final boolean isHidden;

    // range for the @timestamp field for the Index
    private final IndexLongFieldRange timestampRange;
    // range for the event.ingested field for the Index
    private final IndexLongFieldRange eventIngestedRange;

    private final int priority;

    private final long creationDate;

    private final boolean ignoreDiskWatermarks;

    @Nullable // since we store null if DataTier.TIER_PREFERENCE_SETTING failed validation
    private final List<String> tierPreference;

    private final int shardsPerNodeLimit;

    @Nullable // if an index isn't managed by ilm, it won't have a policy
    private final String lifecyclePolicyName;

    private final LifecycleExecutionState lifecycleExecutionState;

    private final AutoExpandReplicas autoExpandReplicas;

    private final boolean isSearchableSnapshot;

    private final boolean isPartialSearchableSnapshot;

    @Nullable
    private final IndexMode indexMode;
    @Nullable
    private final Instant timeSeriesStart;
    @Nullable
    private final Instant timeSeriesEnd;
    @Nullable
    private final IndexMetadataStats stats;
    @Nullable
    private final Double writeLoadForecast;
    @Nullable
    private final Long shardSizeInBytesForecast;
    @Nullable
    private final IndexReshardingMetadata reshardingMetadata;

    private IndexMetadata(
        final Index index,
        final long version,
        final long mappingVersion,
        final long settingsVersion,
        final long aliasesVersion,
        final long[] primaryTerms,
        final State state,
        final int numberOfShards,
        final int numberOfReplicas,
        final Settings settings,
        final MappingMetadata mapping,
        final ImmutableOpenMap<String, InferenceFieldMetadata> inferenceFields,
        final ImmutableOpenMap<String, AliasMetadata> aliases,
        final ImmutableOpenMap<String, DiffableStringMap> customData,
        final Map<Integer, Set<String>> inSyncAllocationIds,
        final DiscoveryNodeFilters requireFilters,
        final DiscoveryNodeFilters initialRecoveryFilters,
        final DiscoveryNodeFilters includeFilters,
        final DiscoveryNodeFilters excludeFilters,
        final IndexVersion indexCreatedVersion,
        final IndexVersion mappingsUpdatedVersion,
        final int routingNumShards,
        final int routingPartitionSize,
        final List<String> routingPaths,
        final ActiveShardCount waitForActiveShards,
        final ImmutableOpenMap<String, RolloverInfo> rolloverInfos,
        final boolean isSystem,
        final boolean isHidden,
        final IndexLongFieldRange timestampRange,
        final IndexLongFieldRange eventIngestedRange,
        final int priority,
        final long creationDate,
        final boolean ignoreDiskWatermarks,
        @Nullable final List<String> tierPreference,
        final int shardsPerNodeLimit,
        final String lifecyclePolicyName,
        final LifecycleExecutionState lifecycleExecutionState,
        final AutoExpandReplicas autoExpandReplicas,
        final boolean isSearchableSnapshot,
        final boolean isPartialSearchableSnapshot,
        @Nullable final IndexMode indexMode,
        @Nullable final Instant timeSeriesStart,
        @Nullable final Instant timeSeriesEnd,
        final IndexVersion indexCompatibilityVersion,
        @Nullable final IndexMetadataStats stats,
        @Nullable final Double writeLoadForecast,
        @Nullable Long shardSizeInBytesForecast,
        @Nullable IndexReshardingMetadata reshardingMetadata
    ) {
        this.index = index;
        this.version = version;
        assert mappingVersion >= 0 : mappingVersion;
        this.mappingVersion = mappingVersion;
        this.mappingsUpdatedVersion = mappingsUpdatedVersion;
        assert settingsVersion >= 0 : settingsVersion;
        this.settingsVersion = settingsVersion;
        assert aliasesVersion >= 0 : aliasesVersion;
        this.aliasesVersion = aliasesVersion;
        this.primaryTerms = primaryTerms;
        assert primaryTerms.length == numberOfShards;
        this.state = state;
        this.numberOfShards = numberOfShards;
        this.numberOfReplicas = numberOfReplicas;
        this.totalNumberOfShards = numberOfShards * (numberOfReplicas + 1);
        this.settings = settings;
        this.mapping = mapping;
        this.inferenceFields = inferenceFields;
        this.customData = customData;
        this.aliases = aliases;
        this.inSyncAllocationIds = inSyncAllocationIds;
        this.requireFilters = requireFilters;
        this.includeFilters = includeFilters;
        this.excludeFilters = excludeFilters;
        this.initialRecoveryFilters = initialRecoveryFilters;
        this.indexCreatedVersion = indexCreatedVersion;
        this.routingNumShards = routingNumShards;
        this.routingFactor = routingNumShards / numberOfShards;
        this.routingPartitionSize = routingPartitionSize;
        this.routingPaths = routingPaths;
        this.waitForActiveShards = waitForActiveShards;
        this.rolloverInfos = rolloverInfos;
        this.isSystem = isSystem;
        assert isHidden == INDEX_HIDDEN_SETTING.get(settings);
        this.isHidden = isHidden;
        this.timestampRange = timestampRange;
        this.eventIngestedRange = eventIngestedRange;
        this.priority = priority;
        this.creationDate = creationDate;
        this.ignoreDiskWatermarks = ignoreDiskWatermarks;
        // always configure the correct (one and only) value for the partially mounted indices tier preference
        this.tierPreference = isPartialSearchableSnapshot ? PARTIALLY_MOUNTED_INDEX_TIER_PREFERENCE : tierPreference;
        this.shardsPerNodeLimit = shardsPerNodeLimit;
        this.lifecyclePolicyName = lifecyclePolicyName;
        this.lifecycleExecutionState = lifecycleExecutionState;
        this.autoExpandReplicas = autoExpandReplicas;
        this.isSearchableSnapshot = isSearchableSnapshot;
        this.isPartialSearchableSnapshot = isPartialSearchableSnapshot;
        this.indexCompatibilityVersion = indexCompatibilityVersion;
        assert indexCompatibilityVersion.equals(SETTING_INDEX_VERSION_COMPATIBILITY.get(settings));
        this.indexMode = indexMode;
        this.timeSeriesStart = timeSeriesStart;
        this.timeSeriesEnd = timeSeriesEnd;
        this.stats = stats;
        this.writeLoadForecast = writeLoadForecast;
        this.shardSizeInBytesForecast = shardSizeInBytesForecast;
        assert numberOfShards * routingFactor == routingNumShards : routingNumShards + " must be a multiple of " + numberOfShards;
        this.reshardingMetadata = reshardingMetadata;
    }

    IndexMetadata withMappingMetadata(MappingMetadata mapping) {
        if (mapping() == mapping) {
            return this;
        }
        return new IndexMetadata(
            this.index,
            this.version,
            this.mappingVersion,
            this.settingsVersion,
            this.aliasesVersion,
            this.primaryTerms,
            this.state,
            this.numberOfShards,
            this.numberOfReplicas,
            this.settings,
            mapping,
            this.inferenceFields,
            this.aliases,
            this.customData,
            this.inSyncAllocationIds,
            this.requireFilters,
            this.initialRecoveryFilters,
            this.includeFilters,
            this.excludeFilters,
            this.indexCreatedVersion,
            this.mappingsUpdatedVersion,
            this.routingNumShards,
            this.routingPartitionSize,
            this.routingPaths,
            this.waitForActiveShards,
            this.rolloverInfos,
            this.isSystem,
            this.isHidden,
            this.timestampRange,
            this.eventIngestedRange,
            this.priority,
            this.creationDate,
            this.ignoreDiskWatermarks,
            this.tierPreference,
            this.shardsPerNodeLimit,
            this.lifecyclePolicyName,
            this.lifecycleExecutionState,
            this.autoExpandReplicas,
            this.isSearchableSnapshot,
            this.isPartialSearchableSnapshot,
            this.indexMode,
            this.timeSeriesStart,
            this.timeSeriesEnd,
            this.indexCompatibilityVersion,
            this.stats,
            this.writeLoadForecast,
            this.shardSizeInBytesForecast,
            this.reshardingMetadata
        );
    }

    /**
     * Copy constructor that sets the in-sync allocation ids for the specified shard.
     * @param shardId shard id to set in-sync allocation ids for
     * @param inSyncSet new in-sync allocation ids
     * @return updated instance
     */
    public IndexMetadata withInSyncAllocationIds(int shardId, Set<String> inSyncSet) {
        if (inSyncSet.equals(inSyncAllocationIds.get(shardId))) {
            return this;
        }
        return new IndexMetadata(
            this.index,
            this.version,
            this.mappingVersion,
            this.settingsVersion,
            this.aliasesVersion,
            this.primaryTerms,
            this.state,
            this.numberOfShards,
            this.numberOfReplicas,
            this.settings,
            this.mapping,
            this.inferenceFields,
            this.aliases,
            this.customData,
            Maps.copyMapWithAddedOrReplacedEntry(this.inSyncAllocationIds, shardId, Set.copyOf(inSyncSet)),
            this.requireFilters,
            this.initialRecoveryFilters,
            this.includeFilters,
            this.excludeFilters,
            this.indexCreatedVersion,
            this.mappingsUpdatedVersion,
            this.routingNumShards,
            this.routingPartitionSize,
            this.routingPaths,
            this.waitForActiveShards,
            this.rolloverInfos,
            this.isSystem,
            this.isHidden,
            this.timestampRange,
            this.eventIngestedRange,
            this.priority,
            this.creationDate,
            this.ignoreDiskWatermarks,
            this.tierPreference,
            this.shardsPerNodeLimit,
            this.lifecyclePolicyName,
            this.lifecycleExecutionState,
            this.autoExpandReplicas,
            this.isSearchableSnapshot,
            this.isPartialSearchableSnapshot,
            this.indexMode,
            this.timeSeriesStart,
            this.timeSeriesEnd,
            this.indexCompatibilityVersion,
            this.stats,
            this.writeLoadForecast,
            this.shardSizeInBytesForecast,
            this.reshardingMetadata
        );
    }

    /**
     * Creates a copy of this instance that has the primary term for the given shard id incremented.
     * @param shardId shard id to increment primary term for
     * @return updated instance with incremented primary term
     */
    public IndexMetadata withIncrementedPrimaryTerm(int shardId) {
        return withSetPrimaryTerm(shardId, this.primaryTerms[shardId] + 1);
    }

    /**
     * Creates a copy of this instance that has the primary term for the given shard id set to the value provided.
     * @param shardId shard id to set primary term for
     * @param primaryTerm primary term to set
     * @return updated instance with set primary term
     */
    public IndexMetadata withSetPrimaryTerm(int shardId, long primaryTerm) {
        final long[] newPrimaryTerms = this.primaryTerms.clone();
        newPrimaryTerms[shardId] = primaryTerm;
        return new IndexMetadata(
            this.index,
            this.version,
            this.mappingVersion,
            this.settingsVersion,
            this.aliasesVersion,
            newPrimaryTerms,
            this.state,
            this.numberOfShards,
            this.numberOfReplicas,
            this.settings,
            this.mapping,
            this.inferenceFields,
            this.aliases,
            this.customData,
            this.inSyncAllocationIds,
            this.requireFilters,
            this.initialRecoveryFilters,
            this.includeFilters,
            this.excludeFilters,
            this.indexCreatedVersion,
            this.mappingsUpdatedVersion,
            this.routingNumShards,
            this.routingPartitionSize,
            this.routingPaths,
            this.waitForActiveShards,
            this.rolloverInfos,
            this.isSystem,
            this.isHidden,
            this.timestampRange,
            this.eventIngestedRange,
            this.priority,
            this.creationDate,
            this.ignoreDiskWatermarks,
            this.tierPreference,
            this.shardsPerNodeLimit,
            this.lifecyclePolicyName,
            this.lifecycleExecutionState,
            this.autoExpandReplicas,
            this.isSearchableSnapshot,
            this.isPartialSearchableSnapshot,
            this.indexMode,
            this.timeSeriesStart,
            this.timeSeriesEnd,
            this.indexCompatibilityVersion,
            this.stats,
            this.writeLoadForecast,
            this.shardSizeInBytesForecast,
            this.reshardingMetadata
        );
    }

    /**
     * @param timestampRange new @timestamp range
     * @param eventIngestedRange new 'event.ingested' range
     * @return copy of this instance with updated timestamp range
     */
    public IndexMetadata withTimestampRanges(IndexLongFieldRange timestampRange, IndexLongFieldRange eventIngestedRange) {
        if (timestampRange.equals(this.timestampRange) && eventIngestedRange.equals(this.eventIngestedRange)) {
            return this;
        }
        return new IndexMetadata(
            this.index,
            this.version,
            this.mappingVersion,
            this.settingsVersion,
            this.aliasesVersion,
            this.primaryTerms,
            this.state,
            this.numberOfShards,
            this.numberOfReplicas,
            this.settings,
            this.mapping,
            this.inferenceFields,
            this.aliases,
            this.customData,
            this.inSyncAllocationIds,
            this.requireFilters,
            this.initialRecoveryFilters,
            this.includeFilters,
            this.excludeFilters,
            this.indexCreatedVersion,
            this.mappingsUpdatedVersion,
            this.routingNumShards,
            this.routingPartitionSize,
            this.routingPaths,
            this.waitForActiveShards,
            this.rolloverInfos,
            this.isSystem,
            this.isHidden,
            timestampRange,
            eventIngestedRange,
            this.priority,
            this.creationDate,
            this.ignoreDiskWatermarks,
            this.tierPreference,
            this.shardsPerNodeLimit,
            this.lifecyclePolicyName,
            this.lifecycleExecutionState,
            this.autoExpandReplicas,
            this.isSearchableSnapshot,
            this.isPartialSearchableSnapshot,
            this.indexMode,
            this.timeSeriesStart,
            this.timeSeriesEnd,
            this.indexCompatibilityVersion,
            this.stats,
            this.writeLoadForecast,
            this.shardSizeInBytesForecast,
            this.reshardingMetadata
        );
    }

    /**
     * @return a copy of this instance that has its version incremented by one
     */
    public IndexMetadata withIncrementedVersion() {
        return new IndexMetadata(
            this.index,
            this.version + 1,
            this.mappingVersion,
            this.settingsVersion,
            this.aliasesVersion,
            this.primaryTerms,
            this.state,
            this.numberOfShards,
            this.numberOfReplicas,
            this.settings,
            this.mapping,
            this.inferenceFields,
            this.aliases,
            this.customData,
            this.inSyncAllocationIds,
            this.requireFilters,
            this.initialRecoveryFilters,
            this.includeFilters,
            this.excludeFilters,
            this.indexCreatedVersion,
            this.mappingsUpdatedVersion,
            this.routingNumShards,
            this.routingPartitionSize,
            this.routingPaths,
            this.waitForActiveShards,
            this.rolloverInfos,
            this.isSystem,
            this.isHidden,
            this.timestampRange,
            this.eventIngestedRange,
            this.priority,
            this.creationDate,
            this.ignoreDiskWatermarks,
            this.tierPreference,
            this.shardsPerNodeLimit,
            this.lifecyclePolicyName,
            this.lifecycleExecutionState,
            this.autoExpandReplicas,
            this.isSearchableSnapshot,
            this.isPartialSearchableSnapshot,
            this.indexMode,
            this.timeSeriesStart,
            this.timeSeriesEnd,
            this.indexCompatibilityVersion,
            this.stats,
            this.writeLoadForecast,
            this.shardSizeInBytesForecast,
            this.reshardingMetadata
        );
    }

    public Index getIndex() {
        return index;
    }

    public String getIndexUUID() {
        return index.getUUID();
    }

    public long getVersion() {
        return this.version;
    }

    public long getMappingVersion() {
        return mappingVersion;
    }

    public IndexVersion getMappingsUpdatedVersion() {
        return mappingsUpdatedVersion;
    }

    public long getSettingsVersion() {
        return settingsVersion;
    }

    public long getAliasesVersion() {
        return aliasesVersion;
    }

    /**
     * The term of the current selected primary. This is a non-negative number incremented when
     * a primary shard is assigned after a full cluster restart or a replica shard is promoted to a primary.
     *
     * Note: since we increment the term every time a shard is assigned, the term for any operational shard (i.e., a shard
     * that can be indexed into) is larger than 0. See {@link IndexMetadataUpdater#applyChanges}.
     **/
    public long primaryTerm(int shardId) {
        return this.primaryTerms[shardId];
    }

    /**
     * Return the {@link IndexVersion} on which this index has been created. This
     * information is typically useful for backward compatibility.
     * To check index compatibility (e.g. N-1 checks), use {@link #getCompatibilityVersion()} instead.
     */
    public IndexVersion getCreationVersion() {
        return indexCreatedVersion;
    }

    /**
     * Return the {@link IndexVersion} that this index provides compatibility for.
     * This is typically compared to the {@link IndexVersions#MINIMUM_COMPATIBLE} or {@link IndexVersions#MINIMUM_READONLY_COMPATIBLE}
     * to figure out whether the index can be handled by the cluster.
     * By default, this is equal to the {@link #getCreationVersion()}, but can also be a newer version if the index has been created by
     * a legacy version, and imported archive, in which case its metadata has been converted to be handled by newer version nodes.
     */
    public IndexVersion getCompatibilityVersion() {
        return indexCompatibilityVersion;
    }

    public long getCreationDate() {
        return creationDate;
    }

    public State getState() {
        return this.state;
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }

    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    public int getRoutingPartitionSize() {
        return routingPartitionSize;
    }

    public boolean isRoutingPartitionedIndex() {
        return routingPartitionSize != 1;
    }

    public List<String> getRoutingPaths() {
        return routingPaths;
    }

    public int getTotalNumberOfShards() {
        return totalNumberOfShards;
    }

    /**
     * Returns the configured {@link #SETTING_WAIT_FOR_ACTIVE_SHARDS}, which defaults
     * to an active shard count of 1 if not specified.
     */
    public ActiveShardCount getWaitForActiveShards() {
        return waitForActiveShards;
    }

    public boolean ignoreDiskWatermarks() {
        return ignoreDiskWatermarks;
    }

    public Settings getSettings() {
        return settings;
    }

    public Map<String, AliasMetadata> getAliases() {
        return this.aliases;
    }

    public int getShardsPerNodeLimit() {
        return shardsPerNodeLimit;
    }

    public List<String> getTierPreference() {
        if (tierPreference == null) {
            final List<String> parsed = DataTier.parseTierList(DataTier.TIER_PREFERENCE_SETTING.get(settings));
            assert false : "the setting parsing should always throw if we didn't store a tier preference when building this instance";
            return parsed;
        }
        return tierPreference;
    }

    /**
     * Return the name of the Index Lifecycle Policy associated with this index, or null if it is not managed by ILM.
     */
    @Nullable
    public String getLifecyclePolicyName() {
        return lifecyclePolicyName;
    }

    public LifecycleExecutionState getLifecycleExecutionState() {
        return lifecycleExecutionState;
    }

    public AutoExpandReplicas getAutoExpandReplicas() {
        return autoExpandReplicas;
    }

    public boolean isSearchableSnapshot() {
        return isSearchableSnapshot;
    }

    public boolean isPartialSearchableSnapshot() {
        return isPartialSearchableSnapshot;
    }

    /**
     * @return the mode this index is in. This determines the behaviour and features it supports.
     *         If <code>null</code> is returned then this in index is in standard mode.
     */
    @Nullable
    public IndexMode getIndexMode() {
        return indexMode;
    }

    /**
     * If this index is in {@link IndexMode#TIME_SERIES} then this returns the lower boundary of the time series time range.
     * Together with {@link #getTimeSeriesEnd()} this defines the time series time range this index has and the range of
     * timestamps all documents in this index have.
     *
     * @return If this index is in {@link IndexMode#TIME_SERIES} then this returns the lower boundary of the time series time range.
     *         If this index isn't in {@link IndexMode#TIME_SERIES} then <code>null</code> is returned.
     */
    @Nullable
    public Instant getTimeSeriesStart() {
        return timeSeriesStart;
    }

    /**
     * If this index is in {@link IndexMode#TIME_SERIES} then this returns the upper boundary of the time series time range.
     * Together with {@link #getTimeSeriesStart()} this defines the time series time range this index has and the range of
     * timestamps all documents in this index have.
     *
     * @return If this index is in {@link IndexMode#TIME_SERIES} then this returns the upper boundary of the time series time range.
     *         If this index isn't in {@link IndexMode#TIME_SERIES} then <code>null</code> is returned.
     */
    @Nullable
    public Instant getTimeSeriesEnd() {
        return timeSeriesEnd;
    }

    /**
     * Return the concrete mapping for this index or {@code null} if this index has no mappings at all.
     */
    @Nullable
    public MappingMetadata mapping() {
        return mapping;
    }

    public Map<String, InferenceFieldMetadata> getInferenceFields() {
        return inferenceFields;
    }

    @Nullable
    public IndexMetadataStats getStats() {
        return stats;
    }

    public OptionalDouble getForecastedWriteLoad() {
        return writeLoadForecast == null ? OptionalDouble.empty() : OptionalDouble.of(writeLoadForecast);
    }

    public OptionalLong getForecastedShardSizeInBytes() {
        return shardSizeInBytesForecast == null ? OptionalLong.empty() : OptionalLong.of(shardSizeInBytesForecast);
    }

    public static final String INDEX_RESIZE_SOURCE_UUID_KEY = "index.resize.source.uuid";
    public static final String INDEX_RESIZE_SOURCE_NAME_KEY = "index.resize.source.name";
    public static final Setting<String> INDEX_RESIZE_SOURCE_UUID = Setting.simpleString(INDEX_RESIZE_SOURCE_UUID_KEY);
    public static final Setting<String> INDEX_RESIZE_SOURCE_NAME = Setting.simpleString(INDEX_RESIZE_SOURCE_NAME_KEY);

    /**
     * we use "i.r.a.initial_recovery" rather than "i.r.a.require|include" since we want the replica to allocate right away
     * once we are allocated.
     */
    public static final String INDEX_SHRINK_INITIAL_RECOVERY_KEY = INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING.getKey() + "_id";

    public Index getResizeSourceIndex() {
        return INDEX_RESIZE_SOURCE_UUID.exists(settings)
            ? new Index(INDEX_RESIZE_SOURCE_NAME.get(settings), INDEX_RESIZE_SOURCE_UUID.get(settings))
            : null;
    }

    public static final String INDEX_DOWNSAMPLE_SOURCE_UUID_KEY = "index.downsample.source.uuid";
    public static final String INDEX_DOWNSAMPLE_SOURCE_NAME_KEY = "index.downsample.source.name";
    public static final String INDEX_DOWNSAMPLE_ORIGIN_NAME_KEY = "index.downsample.origin.name";
    public static final String INDEX_DOWNSAMPLE_ORIGIN_UUID_KEY = "index.downsample.origin.uuid";

    public static final String INDEX_DOWNSAMPLE_STATUS_KEY = "index.downsample.status";
    public static final String INDEX_DOWNSAMPLE_INTERVAL_KEY = "index.downsample.interval";
    public static final Setting<String> INDEX_DOWNSAMPLE_SOURCE_UUID = Setting.simpleString(
        INDEX_DOWNSAMPLE_SOURCE_UUID_KEY,
        Property.IndexScope,
        Property.PrivateIndex
    );
    public static final Setting<String> INDEX_DOWNSAMPLE_SOURCE_NAME = Setting.simpleString(
        INDEX_DOWNSAMPLE_SOURCE_NAME_KEY,
        Property.IndexScope,
        Property.PrivateIndex
    );

    public static final Setting<String> INDEX_DOWNSAMPLE_ORIGIN_NAME = Setting.simpleString(
        INDEX_DOWNSAMPLE_ORIGIN_NAME_KEY,
        Property.IndexScope,
        Property.PrivateIndex
    );

    public static final Setting<String> INDEX_DOWNSAMPLE_ORIGIN_UUID = Setting.simpleString(
        INDEX_DOWNSAMPLE_ORIGIN_UUID_KEY,
        Property.IndexScope,
        Property.PrivateIndex
    );

    public enum DownsampleTaskStatus {
        UNKNOWN,
        STARTED,
        SUCCESS;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static final Setting<DownsampleTaskStatus> INDEX_DOWNSAMPLE_STATUS = Setting.enumSetting(
        DownsampleTaskStatus.class,
        INDEX_DOWNSAMPLE_STATUS_KEY,
        DownsampleTaskStatus.UNKNOWN,
        Property.IndexScope,
        Property.InternalIndex
    );

    public static final Setting<String> INDEX_DOWNSAMPLE_INTERVAL = Setting.simpleString(
        INDEX_DOWNSAMPLE_INTERVAL_KEY,
        Property.IndexScope,
        Property.InternalIndex
    );

    // LIFECYCLE_NAME is here an as optimization, see LifecycleSettings.LIFECYCLE_NAME and
    // LifecycleSettings.LIFECYCLE_NAME_SETTING for the 'real' version
    public static final String LIFECYCLE_NAME = "index.lifecycle.name";

    Map<String, DiffableStringMap> getCustomData() {
        return this.customData;
    }

    public Map<String, String> getCustomData(final String key) {
        return this.customData.get(key);
    }

    public Map<Integer, Set<String>> getInSyncAllocationIds() {
        return inSyncAllocationIds;
    }

    public Map<String, RolloverInfo> getRolloverInfos() {
        return rolloverInfos;
    }

    public Set<String> inSyncAllocationIds(int shardId) {
        assert shardId >= 0 && shardId < numberOfShards;
        return inSyncAllocationIds.get(shardId);
    }

    @Nullable
    public DiscoveryNodeFilters requireFilters() {
        return requireFilters;
    }

    @Nullable
    public DiscoveryNodeFilters getInitialRecoveryFilters() {
        return initialRecoveryFilters;
    }

    @Nullable
    public DiscoveryNodeFilters includeFilters() {
        return includeFilters;
    }

    @Nullable
    public DiscoveryNodeFilters excludeFilters() {
        return excludeFilters;
    }

    public IndexLongFieldRange getTimestampRange() {
        return timestampRange;
    }

    public IndexLongFieldRange getEventIngestedRange() {
        return eventIngestedRange;
    }

    /**
     * @return whether this index has a time series timestamp range
     */
    public boolean hasTimeSeriesTimestampRange() {
        return indexMode != null && indexMode.getTimestampBound(this) != null;
    }

    /**
     * @param dateFieldType the date field type of '@timestamp' field which is
     *                      used to convert the start and end times recorded in index metadata
     *                      to the right format that is being used by '@timestamp' field.
     *                      For example, the '@timestamp' can be configured with nanosecond precision.
     * @return the time range this index represents if this index is in time series mode.
     *         Otherwise <code>null</code> is returned.
     */
    @Nullable
    public IndexLongFieldRange getTimeSeriesTimestampRange(DateFieldMapper.DateFieldType dateFieldType) {
        var bounds = indexMode != null ? indexMode.getTimestampBound(this) : null;
        if (bounds != null) {
            long start = dateFieldType.resolution().convert(Instant.ofEpochMilli(bounds.startTime()));
            long end = dateFieldType.resolution().convert(Instant.ofEpochMilli(bounds.endTime()));
            return IndexLongFieldRange.NO_SHARDS.extendWithShardRange(0, 1, ShardLongFieldRange.of(start, end));
        } else {
            return null;
        }
    }

    @Nullable
    public IndexReshardingMetadata getReshardingMetadata() {
        return reshardingMetadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexMetadata that = (IndexMetadata) o;

        if (version != that.version) {
            return false;
        }

        if (aliases.equals(that.aliases) == false) {
            return false;
        }
        if (index.equals(that.index) == false) {
            return false;
        }
        if (Objects.equals(mapping, that.mapping) == false) {
            return false;
        }
        if (settings.equals(that.settings) == false) {
            return false;
        }
        if (state != that.state) {
            return false;
        }
        if (customData.equals(that.customData) == false) {
            return false;
        }
        if (routingNumShards != that.routingNumShards) {
            return false;
        }
        if (routingFactor != that.routingFactor) {
            return false;
        }
        if (Arrays.equals(primaryTerms, that.primaryTerms) == false) {
            return false;
        }
        if (inSyncAllocationIds.equals(that.inSyncAllocationIds) == false) {
            return false;
        }
        if (rolloverInfos.equals(that.rolloverInfos) == false) {
            return false;
        }
        if (inferenceFields.equals(that.inferenceFields) == false) {
            return false;
        }
        if (isSystem != that.isSystem) {
            return false;
        }
        if (Objects.equals(reshardingMetadata, that.reshardingMetadata) == false) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + Long.hashCode(version);
        result = 31 * result + state.hashCode();
        result = 31 * result + aliases.hashCode();
        result = 31 * result + settings.hashCode();
        result = 31 * result + Objects.hash(mapping);
        result = 31 * result + customData.hashCode();
        result = 31 * result + Long.hashCode(routingFactor);
        result = 31 * result + Long.hashCode(routingNumShards);
        result = 31 * result + Arrays.hashCode(primaryTerms);
        result = 31 * result + inSyncAllocationIds.hashCode();
        result = 31 * result + rolloverInfos.hashCode();
        result = 31 * result + inferenceFields.hashCode();
        result = 31 * result + Boolean.hashCode(isSystem);
        result = 31 * result + Objects.hashCode(reshardingMetadata);
        return result;
    }

    @Override
    public Diff<IndexMetadata> diff(IndexMetadata previousState) {
        return new IndexMetadataDiff(previousState, this);
    }

    public static Diff<IndexMetadata> readDiffFrom(StreamInput in) throws IOException {
        return new IndexMetadataDiff(in);
    }

    public static IndexMetadata fromXContent(XContentParser parser) throws IOException {
        return Builder.fromXContent(parser);
    }

    public static IndexMetadata fromXContent(XContentParser parser, Map<String, MappingMetadata> mappingsByHash) throws IOException {
        return Builder.fromXContent(parser, mappingsByHash);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Builder.toXContent(this, builder, params);
        return builder;
    }

    private static final TransportVersion SETTING_DIFF_VERSION = TransportVersions.V_8_5_0;

    private static class IndexMetadataDiff implements Diff<IndexMetadata> {

        private final String index;
        private final int routingNumShards;
        private final long version;
        private final long mappingVersion;
        private final long settingsVersion;
        private final long aliasesVersion;
        private final long[] primaryTerms;
        private final State state;

        // used for BwC when this instance was written by an older version node that does not diff settings yet
        @Nullable
        private final Settings settings;
        @Nullable
        private final Diff<Settings> settingsDiff;
        private final Diff<ImmutableOpenMap<String, MappingMetadata>> mappings;
        private final Diff<ImmutableOpenMap<String, InferenceFieldMetadata>> inferenceFields;
        private final Diff<ImmutableOpenMap<String, AliasMetadata>> aliases;
        private final Diff<ImmutableOpenMap<String, DiffableStringMap>> customData;
        private final Diff<Map<Integer, Set<String>>> inSyncAllocationIds;
        private final Diff<ImmutableOpenMap<String, RolloverInfo>> rolloverInfos;
        private final IndexVersion mappingsUpdatedVersion;
        private final boolean isSystem;

        // range for the @timestamp field for the Index
        private final IndexLongFieldRange timestampRange;
        // range for the event.ingested field for the Index
        private final IndexLongFieldRange eventIngestedRange;

        private final IndexMetadataStats stats;
        private final Double indexWriteLoadForecast;
        private final Long shardSizeInBytesForecast;
        private final IndexReshardingMetadata reshardingMetadata;

        IndexMetadataDiff(IndexMetadata before, IndexMetadata after) {
            index = after.index.getName();
            version = after.version;
            mappingVersion = after.mappingVersion;
            settingsVersion = after.settingsVersion;
            aliasesVersion = after.aliasesVersion;
            routingNumShards = after.routingNumShards;
            state = after.state;
            settings = after.settings;
            settingsDiff = after.settings.diff(before.settings);
            primaryTerms = after.primaryTerms;
            // TODO: find a nicer way to do BwC here and just work with Diff<MappingMetadata> here and in networking
            mappings = DiffableUtils.diff(
                before.mapping == null
                    ? ImmutableOpenMap.of()
                    : ImmutableOpenMap.<String, MappingMetadata>builder(1).fPut(MapperService.SINGLE_MAPPING_NAME, before.mapping).build(),
                after.mapping == null
                    ? ImmutableOpenMap.of()
                    : ImmutableOpenMap.<String, MappingMetadata>builder(1).fPut(MapperService.SINGLE_MAPPING_NAME, after.mapping).build(),
                DiffableUtils.getStringKeySerializer()
            );
            inferenceFields = DiffableUtils.diff(before.inferenceFields, after.inferenceFields, DiffableUtils.getStringKeySerializer());
            aliases = DiffableUtils.diff(before.aliases, after.aliases, DiffableUtils.getStringKeySerializer());
            customData = DiffableUtils.diff(before.customData, after.customData, DiffableUtils.getStringKeySerializer());
            inSyncAllocationIds = DiffableUtils.diff(
                before.inSyncAllocationIds,
                after.inSyncAllocationIds,
                DiffableUtils.getVIntKeySerializer(),
                DiffableUtils.StringSetValueSerializer.getInstance()
            );
            rolloverInfos = DiffableUtils.diff(before.rolloverInfos, after.rolloverInfos, DiffableUtils.getStringKeySerializer());
            mappingsUpdatedVersion = after.mappingsUpdatedVersion;
            isSystem = after.isSystem;
            timestampRange = after.timestampRange;
            eventIngestedRange = after.eventIngestedRange;
            stats = after.stats;
            indexWriteLoadForecast = after.writeLoadForecast;
            shardSizeInBytesForecast = after.shardSizeInBytesForecast;
            reshardingMetadata = after.reshardingMetadata;
        }

        private static final DiffableUtils.DiffableValueReader<String, AliasMetadata> ALIAS_METADATA_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(AliasMetadata::new, AliasMetadata::readDiffFrom);
        private static final DiffableUtils.DiffableValueReader<String, MappingMetadata> MAPPING_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(MappingMetadata::new, MappingMetadata::readDiffFrom);
        private static final DiffableUtils.DiffableValueReader<String, DiffableStringMap> CUSTOM_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(DiffableStringMap::readFrom, DiffableStringMap::readDiffFrom);
        private static final DiffableUtils.DiffableValueReader<String, RolloverInfo> ROLLOVER_INFO_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(RolloverInfo::new, RolloverInfo::readDiffFrom);
        private static final DiffableUtils.DiffableValueReader<String, InferenceFieldMetadata> INFERENCE_FIELDS_METADATA_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(InferenceFieldMetadata::new, InferenceFieldMetadata::readDiffFrom);

        IndexMetadataDiff(StreamInput in) throws IOException {
            index = in.readString();
            routingNumShards = in.readInt();
            version = in.readLong();
            mappingVersion = in.readVLong();
            settingsVersion = in.readVLong();
            aliasesVersion = in.readVLong();
            state = State.fromId(in.readByte());
            if (in.getTransportVersion().onOrAfter(SETTING_DIFF_VERSION)) {
                settings = null;
                settingsDiff = Settings.readSettingsDiffFromStream(in);
            } else {
                settings = Settings.readSettingsFromStream(in);
                settingsDiff = null;
            }
            primaryTerms = in.readVLongArray();
            mappings = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), MAPPING_DIFF_VALUE_READER);
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
                inferenceFields = DiffableUtils.readImmutableOpenMapDiff(
                    in,
                    DiffableUtils.getStringKeySerializer(),
                    INFERENCE_FIELDS_METADATA_DIFF_VALUE_READER
                );
            } else {
                inferenceFields = DiffableUtils.emptyDiff();
            }
            aliases = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), ALIAS_METADATA_DIFF_VALUE_READER);
            customData = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), CUSTOM_DIFF_VALUE_READER);
            inSyncAllocationIds = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getVIntKeySerializer(),
                DiffableUtils.StringSetValueSerializer.getInstance()
            );
            rolloverInfos = DiffableUtils.readImmutableOpenMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                ROLLOVER_INFO_DIFF_VALUE_READER
            );
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                mappingsUpdatedVersion = IndexVersion.readVersion(in);
            } else {
                mappingsUpdatedVersion = IndexVersions.ZERO;
            }
            isSystem = in.readBoolean();
            timestampRange = IndexLongFieldRange.readFrom(in);
            if (in.getTransportVersion().onOrAfter(STATS_AND_FORECAST_ADDED)) {
                stats = in.readOptionalWriteable(IndexMetadataStats::new);
                indexWriteLoadForecast = in.readOptionalDouble();
                shardSizeInBytesForecast = in.readOptionalLong();
            } else {
                stats = null;
                indexWriteLoadForecast = null;
                shardSizeInBytesForecast = null;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                eventIngestedRange = IndexLongFieldRange.readFrom(in);
            } else {
                eventIngestedRange = IndexLongFieldRange.UNKNOWN;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.INDEX_RESHARDING_METADATA)) {
                reshardingMetadata = in.readOptionalWriteable(IndexReshardingMetadata::new);
            } else {
                reshardingMetadata = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeInt(routingNumShards);
            out.writeLong(version);
            out.writeVLong(mappingVersion);
            out.writeVLong(settingsVersion);
            out.writeVLong(aliasesVersion);
            out.writeByte(state.id);
            assert settings != null
                : "settings should always be non-null since this instance is not expected to have been read from another node";
            if (out.getTransportVersion().onOrAfter(SETTING_DIFF_VERSION)) {
                settingsDiff.writeTo(out);
            } else {
                settings.writeTo(out);
            }
            out.writeVLongArray(primaryTerms);
            mappings.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
                inferenceFields.writeTo(out);
            }
            aliases.writeTo(out);
            customData.writeTo(out);
            inSyncAllocationIds.writeTo(out);
            rolloverInfos.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                IndexVersion.writeVersion(mappingsUpdatedVersion, out);
            }
            out.writeBoolean(isSystem);
            timestampRange.writeTo(out);
            if (out.getTransportVersion().onOrAfter(STATS_AND_FORECAST_ADDED)) {
                out.writeOptionalWriteable(stats);
                out.writeOptionalDouble(indexWriteLoadForecast);
                out.writeOptionalLong(shardSizeInBytesForecast);
            }
            eventIngestedRange.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.INDEX_RESHARDING_METADATA)) {
                out.writeOptionalWriteable(reshardingMetadata);
            }
        }

        @Override
        public IndexMetadata apply(IndexMetadata part) {
            Builder builder = builder(index);
            builder.version(version);
            builder.mappingVersion(mappingVersion);
            builder.settingsVersion(settingsVersion);
            builder.aliasesVersion(aliasesVersion);
            builder.setRoutingNumShards(routingNumShards);
            builder.state(state);
            if (settingsDiff == null) {
                builder.settings(settings);
            } else {
                builder.settings(settingsDiff.apply(part.settings));
            }
            builder.primaryTerms(primaryTerms);
            builder.mapping = mappings.apply(
                ImmutableOpenMap.<String, MappingMetadata>builder(1).fPut(MapperService.SINGLE_MAPPING_NAME, part.mapping).build()
            ).get(MapperService.SINGLE_MAPPING_NAME);
            builder.mappingsUpdatedVersion = mappingsUpdatedVersion;
            builder.inferenceFields.putAllFromMap(inferenceFields.apply(part.inferenceFields));
            builder.aliases.putAllFromMap(aliases.apply(part.aliases));
            builder.customMetadata.putAllFromMap(customData.apply(part.customData));
            builder.inSyncAllocationIds.putAll(inSyncAllocationIds.apply(part.inSyncAllocationIds));
            builder.rolloverInfos.putAllFromMap(rolloverInfos.apply(part.rolloverInfos));
            builder.system(isSystem);
            builder.timestampRange(timestampRange);
            builder.eventIngestedRange(eventIngestedRange);
            builder.stats(stats);
            builder.indexWriteLoadForecast(indexWriteLoadForecast);
            builder.shardSizeInBytesForecast(shardSizeInBytesForecast);
            builder.reshardingMetadata(reshardingMetadata);
            return builder.build(true);
        }
    }

    public static IndexMetadata readFrom(StreamInput in) throws IOException {
        return readFrom(in, null);
    }

    /**
     * @param mappingLookup optional lookup function that translates mapping metadata hashes into concrete instances. If specified we
     *                      assume that the stream contains only mapping metadata hashes but not fully serialized instances of mapping
     *                      metadata.
     */
    public static IndexMetadata readFrom(StreamInput in, @Nullable Function<String, MappingMetadata> mappingLookup) throws IOException {
        Builder builder = new Builder(in.readString());
        builder.version(in.readLong());
        builder.mappingVersion(in.readVLong());
        builder.settingsVersion(in.readVLong());
        builder.aliasesVersion(in.readVLong());
        builder.setRoutingNumShards(in.readInt());
        builder.state(State.fromId(in.readByte()));
        builder.settings(readSettingsFromStream(in));
        builder.primaryTerms(in.readVLongArray());
        int mappingsSize = in.readVInt();
        if (mappingsSize == 1) {
            if (mappingLookup != null) {
                final String mappingHash = in.readString();
                final MappingMetadata metadata = mappingLookup.apply(mappingHash);
                assert metadata != null : "failed to find mapping [" + mappingHash + "] for [" + builder.index + "]";
                builder.putMapping(metadata);
            } else {
                builder.putMapping(new MappingMetadata(in));
            }
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            var fields = in.readCollectionAsImmutableList(InferenceFieldMetadata::new);
            fields.stream().forEach(f -> builder.putInferenceField(f));
        }
        int aliasesSize = in.readVInt();
        for (int i = 0; i < aliasesSize; i++) {
            AliasMetadata aliasMd = new AliasMetadata(in);
            builder.putAlias(aliasMd);
        }
        int customSize = in.readVInt();
        for (int i = 0; i < customSize; i++) {
            String key = in.readString();
            DiffableStringMap custom = DiffableStringMap.readFrom(in);
            builder.putCustom(key, custom);
        }
        int inSyncAllocationIdsSize = in.readVInt();
        for (int i = 0; i < inSyncAllocationIdsSize; i++) {
            int key = in.readVInt();
            Set<String> allocationIds = DiffableUtils.StringSetValueSerializer.getInstance().read(in, key);
            builder.putInSyncAllocationIds(key, allocationIds);
        }
        int rolloverAliasesSize = in.readVInt();
        for (int i = 0; i < rolloverAliasesSize; i++) {
            builder.putRolloverInfo(new RolloverInfo(in));
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            builder.mappingsUpdatedVersion(IndexVersion.readVersion(in));
        }
        builder.system(in.readBoolean());
        builder.timestampRange(IndexLongFieldRange.readFrom(in));

        if (in.getTransportVersion().onOrAfter(STATS_AND_FORECAST_ADDED)) {
            builder.stats(in.readOptionalWriteable(IndexMetadataStats::new));
            builder.indexWriteLoadForecast(in.readOptionalDouble());
            builder.shardSizeInBytesForecast(in.readOptionalLong());
        }
        builder.eventIngestedRange(IndexLongFieldRange.readFrom(in));
        if (in.getTransportVersion().onOrAfter(TransportVersions.INDEX_RESHARDING_METADATA)) {
            builder.reshardingMetadata(in.readOptionalWriteable(IndexReshardingMetadata::new));
        }
        return builder.build(true);
    }

    /**
     * @param mappingsAsHash whether to serialize {@link MappingMetadata} in full or just its hash {@link MappingMetadata#getSha256()}
     */
    public void writeTo(StreamOutput out, boolean mappingsAsHash) throws IOException {
        out.writeString(index.getName()); // uuid will come as part of settings
        out.writeLong(version);
        out.writeVLong(mappingVersion);
        out.writeVLong(settingsVersion);
        out.writeVLong(aliasesVersion);
        out.writeInt(routingNumShards);
        out.writeByte(state.id());
        settings.writeTo(out);
        out.writeVLongArray(primaryTerms);
        // TODO: adjust serialization format to using an optional writable
        if (mapping == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(1);
            if (mappingsAsHash) {
                out.writeString(mapping.getSha256());
            } else {
                mapping.writeTo(out);
            }
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            out.writeCollection(inferenceFields.values());
        }
        out.writeCollection(aliases.values());
        out.writeMap(customData, StreamOutput::writeWriteable);
        out.writeMap(
            inSyncAllocationIds,
            StreamOutput::writeVInt,
            (o, v) -> DiffableUtils.StringSetValueSerializer.getInstance().write(v, o)
        );
        out.writeCollection(rolloverInfos.values());
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            IndexVersion.writeVersion(mappingsUpdatedVersion, out);
        }
        out.writeBoolean(isSystem);
        timestampRange.writeTo(out);
        if (out.getTransportVersion().onOrAfter(STATS_AND_FORECAST_ADDED)) {
            out.writeOptionalWriteable(stats);
            out.writeOptionalDouble(writeLoadForecast);
            out.writeOptionalLong(shardSizeInBytesForecast);
        }
        eventIngestedRange.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.INDEX_RESHARDING_METADATA)) {
            out.writeOptionalWriteable(reshardingMetadata);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeTo(out, false);
    }

    public boolean isSystem() {
        return isSystem;
    }

    public boolean isHidden() {
        return isHidden;
    }

    public int priority() {
        return priority;
    }

    public static Builder builder(String index) {
        return new Builder(index);
    }

    public static Builder builder(IndexMetadata indexMetadata) {
        return new Builder(indexMetadata);
    }

    public static class Builder {

        private String index;
        private State state = State.OPEN;
        private long version = 1;
        private long mappingVersion = 1;
        private long settingsVersion = 1;
        private long aliasesVersion = 1;
        private long[] primaryTerms = null;
        private Settings settings = Settings.EMPTY;
        private MappingMetadata mapping;
        private IndexVersion mappingsUpdatedVersion = IndexVersion.current();
        private final ImmutableOpenMap.Builder<String, InferenceFieldMetadata> inferenceFields;
        private final ImmutableOpenMap.Builder<String, AliasMetadata> aliases;
        private final ImmutableOpenMap.Builder<String, DiffableStringMap> customMetadata;
        private final Map<Integer, Set<String>> inSyncAllocationIds;
        private final ImmutableOpenMap.Builder<String, RolloverInfo> rolloverInfos;
        private Integer routingNumShards;
        private boolean isSystem;
        private IndexLongFieldRange timestampRange = IndexLongFieldRange.NO_SHARDS;
        private IndexLongFieldRange eventIngestedRange = IndexLongFieldRange.NO_SHARDS;
        private LifecycleExecutionState lifecycleExecutionState = LifecycleExecutionState.EMPTY_STATE;
        private IndexMetadataStats stats = null;
        private Double indexWriteLoadForecast = null;
        private Long shardSizeInBytesForecast = null;
        private IndexReshardingMetadata reshardingMetadata = null;

        public Builder(String index) {
            this.index = index;
            this.inferenceFields = ImmutableOpenMap.builder();
            this.aliases = ImmutableOpenMap.builder();
            this.customMetadata = ImmutableOpenMap.builder();
            this.inSyncAllocationIds = new HashMap<>();
            this.rolloverInfos = ImmutableOpenMap.builder();
            this.isSystem = false;
        }

        public Builder(IndexMetadata indexMetadata) {
            this.index = indexMetadata.getIndex().getName();
            this.state = indexMetadata.state;
            this.version = indexMetadata.version;
            this.mappingVersion = indexMetadata.mappingVersion;
            this.settingsVersion = indexMetadata.settingsVersion;
            this.aliasesVersion = indexMetadata.aliasesVersion;
            this.settings = indexMetadata.getSettings();
            this.primaryTerms = indexMetadata.primaryTerms.clone();
            this.mapping = indexMetadata.mapping;
            this.inferenceFields = ImmutableOpenMap.builder(indexMetadata.inferenceFields);
            this.aliases = ImmutableOpenMap.builder(indexMetadata.aliases);
            this.customMetadata = ImmutableOpenMap.builder(indexMetadata.customData);
            this.routingNumShards = indexMetadata.routingNumShards;
            this.inSyncAllocationIds = new HashMap<>(indexMetadata.inSyncAllocationIds);
            this.mappingsUpdatedVersion = indexMetadata.mappingsUpdatedVersion;
            this.rolloverInfos = ImmutableOpenMap.builder(indexMetadata.rolloverInfos);
            this.isSystem = indexMetadata.isSystem;
            this.timestampRange = indexMetadata.timestampRange;
            this.eventIngestedRange = indexMetadata.eventIngestedRange;
            this.lifecycleExecutionState = indexMetadata.lifecycleExecutionState;
            this.stats = indexMetadata.stats;
            this.indexWriteLoadForecast = indexMetadata.writeLoadForecast;
            this.shardSizeInBytesForecast = indexMetadata.shardSizeInBytesForecast;
            this.reshardingMetadata = indexMetadata.reshardingMetadata;
        }

        public Builder index(String index) {
            this.index = index;
            return this;
        }

        public Builder numberOfShards(int numberOfShards) {
            settings = Settings.builder().put(settings).put(SETTING_NUMBER_OF_SHARDS, numberOfShards).build();
            return this;
        }

        /**
         * Builder to create IndexMetadata that has an increased shard count (used for re-shard).
         * The new shard count must be a multiple of the original shardcount as well as a factor
         * of routingNumShards.
         * We do not support shrinking the shard count.
         * @param targetShardCount   target shard count after resharding
         */
        public Builder reshardAddShards(int targetShardCount) {
            final int sourceNumShards = numberOfShards();
            if (targetShardCount % sourceNumShards != 0) {
                throw new IllegalArgumentException(
                    "New shard count ["
                        + targetShardCount
                        + "] should be a multiple"
                        + " of current shard count ["
                        + sourceNumShards
                        + "] for ["
                        + index
                        + "]"
                );
            }
            settings = Settings.builder().put(settings).put(SETTING_NUMBER_OF_SHARDS, targetShardCount).build();
            var newPrimaryTerms = new long[targetShardCount];
            Arrays.fill(newPrimaryTerms, this.primaryTerms.length, newPrimaryTerms.length, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
            System.arraycopy(primaryTerms, 0, newPrimaryTerms, 0, this.primaryTerms.length);
            primaryTerms = newPrimaryTerms;
            routingNumShards = MetadataCreateIndexService.getIndexNumberOfRoutingShards(settings, sourceNumShards, this.routingNumShards);
            return this;
        }

        /**
         * Sets the number of shards that should be used for routing. This should only be used if the number of shards in
         * an index has changed ie if the index is shrunk.
         */
        public Builder setRoutingNumShards(int routingNumShards) {
            this.routingNumShards = routingNumShards;
            return this;
        }

        /**
         * Returns number of shards that should be used for routing. By default this method will return the number of shards
         * for this index.
         *
         * @see #setRoutingNumShards(int)
         * @see #numberOfShards()
         */
        public int getRoutingNumShards() {
            return routingNumShards == null ? numberOfShards() : routingNumShards;
        }

        /**
         * Returns the number of shards.
         *
         * @return the provided value or -1 if it has not been set.
         */
        public int numberOfShards() {
            return settings.getAsInt(SETTING_NUMBER_OF_SHARDS, -1);
        }

        public Builder numberOfReplicas(int numberOfReplicas) {
            settings = Settings.builder().put(settings).put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas).build();
            return this;
        }

        public Builder routingPartitionSize(int routingPartitionSize) {
            settings = Settings.builder().put(settings).put(SETTING_ROUTING_PARTITION_SIZE, routingPartitionSize).build();
            return this;
        }

        public Builder creationDate(long creationDate) {
            settings = Settings.builder().put(settings).put(SETTING_CREATION_DATE, creationDate).build();
            return this;
        }

        public Builder settings(Settings.Builder settings) {
            return settings(settings.build());
        }

        public Builder settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public MappingMetadata mapping() {
            return mapping;
        }

        public Builder putMapping(String source) {
            putMapping(
                new MappingMetadata(
                    MapperService.SINGLE_MAPPING_NAME,
                    XContentHelper.convertToMap(XContentFactory.xContent(source), source, true)
                )
            );
            return this;
        }

        public Builder putMapping(MappingMetadata mappingMd) {
            mapping = mappingMd;
            return this;
        }

        public Builder mappingsUpdatedVersion(IndexVersion indexVersion) {
            this.mappingsUpdatedVersion = indexVersion;
            return this;
        }

        public Builder state(State state) {
            this.state = state;
            return this;
        }

        public Builder putAlias(AliasMetadata aliasMetadata) {
            aliases.put(aliasMetadata.alias(), aliasMetadata);
            return this;
        }

        public Builder putAlias(AliasMetadata.Builder aliasMetadata) {
            aliases.put(aliasMetadata.alias(), aliasMetadata.build());
            return this;
        }

        public Builder removeAlias(String alias) {
            aliases.remove(alias);
            return this;
        }

        public Builder removeAllAliases() {
            aliases.clear();
            return this;
        }

        public Builder putCustom(String type, Map<String, String> customIndexMetadata) {
            this.customMetadata.put(type, new DiffableStringMap(customIndexMetadata));
            return this;
        }

        public Map<String, String> removeCustom(String type) {
            return this.customMetadata.remove(type);
        }

        public Set<String> getInSyncAllocationIds(int shardId) {
            return inSyncAllocationIds.get(shardId);
        }

        public Builder putInSyncAllocationIds(int shardId, Set<String> allocationIds) {
            inSyncAllocationIds.put(shardId, Set.copyOf(allocationIds));
            return this;
        }

        public Builder putRolloverInfo(RolloverInfo rolloverInfo) {
            rolloverInfos.put(rolloverInfo.getAlias(), rolloverInfo);
            return this;
        }

        public Builder putRolloverInfos(Map<String, RolloverInfo> rolloverInfos) {
            this.rolloverInfos.clear();
            this.rolloverInfos.putAllFromMap(rolloverInfos);
            return this;
        }

        public long version() {
            return this.version;
        }

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        public long mappingVersion() {
            return mappingVersion;
        }

        public Builder mappingVersion(final long mappingVersion) {
            this.mappingVersion = mappingVersion;
            return this;
        }

        public long settingsVersion() {
            return settingsVersion;
        }

        public Builder settingsVersion(final long settingsVersion) {
            this.settingsVersion = settingsVersion;
            return this;
        }

        public Builder aliasesVersion(final long aliasesVersion) {
            this.aliasesVersion = aliasesVersion;
            return this;
        }

        /**
         * returns the primary term for the given shard.
         * See {@link IndexMetadata#primaryTerm(int)} for more information.
         */
        public long primaryTerm(int shardId) {
            if (primaryTerms == null) {
                initializePrimaryTerms();
            }
            return this.primaryTerms[shardId];
        }

        /**
         * sets the primary term for the given shard.
         * See {@link IndexMetadata#primaryTerm(int)} for more information.
         */
        public Builder primaryTerm(int shardId, long primaryTerm) {
            if (primaryTerms == null) {
                initializePrimaryTerms();
            }
            this.primaryTerms[shardId] = primaryTerm;
            return this;
        }

        private void primaryTerms(long[] primaryTerms) {
            this.primaryTerms = primaryTerms.clone();
        }

        private void initializePrimaryTerms() {
            assert primaryTerms == null;
            if (numberOfShards() < 0) {
                throw new IllegalStateException("you must set the number of shards before setting/reading primary terms");
            }
            primaryTerms = new long[numberOfShards()];
            Arrays.fill(primaryTerms, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
        }

        public Builder system(boolean system) {
            this.isSystem = system;
            return this;
        }

        public boolean isSystem() {
            return isSystem;
        }

        public Builder timestampRange(IndexLongFieldRange timestampRange) {
            this.timestampRange = timestampRange;
            return this;
        }

        public Builder eventIngestedRange(IndexLongFieldRange eventIngestedRange) {
            assert eventIngestedRange != null : "eventIngestedRange cannot be null";
            this.eventIngestedRange = eventIngestedRange;
            return this;
        }

        public Builder stats(IndexMetadataStats stats) {
            this.stats = stats;
            return this;
        }

        public Builder indexWriteLoadForecast(Double indexWriteLoadForecast) {
            this.indexWriteLoadForecast = indexWriteLoadForecast;
            return this;
        }

        public Builder shardSizeInBytesForecast(Long shardSizeInBytesForecast) {
            this.shardSizeInBytesForecast = shardSizeInBytesForecast;
            return this;
        }

        public Builder putInferenceField(InferenceFieldMetadata value) {
            this.inferenceFields.put(value.getName(), value);
            return this;
        }

        public Builder putInferenceFields(Map<String, InferenceFieldMetadata> values) {
            this.inferenceFields.putAllFromMap(values);
            return this;
        }

        public Builder reshardingMetadata(IndexReshardingMetadata reshardingMetadata) {
            this.reshardingMetadata = reshardingMetadata;
            return this;
        }

        public IndexMetadata build() {
            return build(false);
        }

        // package private for testing
        IndexMetadata build(boolean repair) {
            /*
             * We expect that the metadata has been properly built to set the number of shards and the number of replicas, and do not rely
             * on the default values here. Those must have been set upstream.
             */
            if (INDEX_NUMBER_OF_SHARDS_SETTING.exists(settings) == false) {
                throw new IllegalArgumentException("must specify number of shards for index [" + index + "]");
            }
            final int numberOfShards = INDEX_NUMBER_OF_SHARDS_SETTING.get(settings);

            if (INDEX_NUMBER_OF_REPLICAS_SETTING.exists(settings) == false) {
                throw new IllegalArgumentException("must specify number of replicas for index [" + index + "]");
            }
            final int numberOfReplicas = INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings);

            int routingPartitionSize = INDEX_ROUTING_PARTITION_SIZE_SETTING.get(settings);
            if (routingPartitionSize != 1 && routingPartitionSize >= getRoutingNumShards()) {
                throw new IllegalArgumentException(
                    "routing partition size ["
                        + routingPartitionSize
                        + "] should be a positive number"
                        + " less than the number of routing shards ["
                        + getRoutingNumShards()
                        + "] for ["
                        + index
                        + "]"
                );
            }

            // fill missing slots in inSyncAllocationIds with empty set if needed and make all entries immutable
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Map.Entry<Integer, Set<String>> denseInSyncAllocationIds[] = new Map.Entry[numberOfShards];
            for (int i = 0; i < numberOfShards; i++) {
                Set<String> allocIds = inSyncAllocationIds.getOrDefault(i, Set.of());
                denseInSyncAllocationIds[i] = Map.entry(i, allocIds);
            }
            var requireMap = INDEX_ROUTING_REQUIRE_GROUP_SETTING.getAsMap(settings);
            final DiscoveryNodeFilters requireFilters;
            if (requireMap.isEmpty()) {
                requireFilters = null;
            } else {
                requireFilters = DiscoveryNodeFilters.buildFromKeyValues(AND, requireMap);
            }
            var includeMap = INDEX_ROUTING_INCLUDE_GROUP_SETTING.getAsMap(settings);
            final DiscoveryNodeFilters includeFilters;
            if (includeMap.isEmpty()) {
                includeFilters = null;
            } else {
                includeFilters = DiscoveryNodeFilters.buildFromKeyValues(OR, includeMap);
            }
            var excludeMap = INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getAsMap(settings);
            final DiscoveryNodeFilters excludeFilters;
            if (excludeMap.isEmpty()) {
                excludeFilters = null;
            } else {
                excludeFilters = DiscoveryNodeFilters.buildFromKeyValues(OR, excludeMap);
            }
            var initialRecoveryMap = INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING.getAsMap(settings);
            final DiscoveryNodeFilters initialRecoveryFilters;
            if (initialRecoveryMap.isEmpty()) {
                initialRecoveryFilters = null;
            } else {
                initialRecoveryFilters = DiscoveryNodeFilters.buildFromKeyValues(OR, initialRecoveryMap);
            }
            IndexVersion indexCreatedVersion = indexCreatedVersion(settings);

            if (primaryTerms == null) {
                initializePrimaryTerms();
            } else if (primaryTerms.length != numberOfShards) {
                throw new IllegalStateException(
                    "primaryTerms length is ["
                        + primaryTerms.length
                        + "] but should be equal to number of shards ["
                        + numberOfShards()
                        + "]"
                );
            }

            final ActiveShardCount waitForActiveShards = SETTING_WAIT_FOR_ACTIVE_SHARDS.get(settings);
            if (waitForActiveShards.validate(numberOfReplicas) == false) {
                throw new IllegalArgumentException(
                    "invalid "
                        + SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey()
                        + "["
                        + waitForActiveShards
                        + "]: cannot be greater than "
                        + "number of shard copies ["
                        + (numberOfReplicas + 1)
                        + "]"
                );
            }

            final List<String> routingPaths = INDEX_ROUTING_PATH.get(settings);

            final String uuid = settings.get(SETTING_INDEX_UUID, INDEX_UUID_NA_VALUE);

            List<String> tierPreference;
            try {
                tierPreference = DataTier.parseTierList(DataTier.TIER_PREFERENCE_SETTING.get(settings));
            } catch (Exception e) {
                assert e instanceof IllegalArgumentException : e;
                // BwC hack: the setting failed validation but it will be fixed in
                // #IndexMetadataVerifier#convertSharedCacheTierPreference(IndexMetadata)} later so we just store a null
                // to be able to build a temporary instance
                tierPreference = null;
            }

            ImmutableOpenMap<String, DiffableStringMap> newCustomMetadata = customMetadata.build();
            Map<String, String> custom = newCustomMetadata.get(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY);
            if (custom != null && custom.isEmpty() == false) {
                lifecycleExecutionState = LifecycleExecutionState.fromCustomMetadata(custom);
            } else {
                lifecycleExecutionState = LifecycleExecutionState.EMPTY_STATE;
            }

            if (stats != null && stats.writeLoad().numberOfShards() != numberOfShards) {
                assert false;
                throw new IllegalArgumentException(
                    "The number of write load shards ["
                        + stats.writeLoad().numberOfShards()
                        + "] is different than the number of index shards ["
                        + numberOfShards
                        + "]"
                );
            }

            var aliasesMap = aliases.build();
            for (AliasMetadata alias : aliasesMap.values()) {
                if (alias.alias().equals(index)) {
                    if (repair && indexCreatedVersion.equals(IndexVersions.V_8_5_0)) {
                        var updatedBuilder = ImmutableOpenMap.builder(aliasesMap);
                        final var brokenAlias = updatedBuilder.remove(index);
                        final var fixedAlias = AliasMetadata.newAliasMetadata(brokenAlias, index + "-alias-corrupted-by-8-5");
                        aliasesMap = updatedBuilder.fPut(fixedAlias.getAlias(), fixedAlias).build();
                        logger.warn("Repaired corrupted alias with the same name as its index for [{}]", index);
                        break;
                    } else {
                        throw new IllegalArgumentException("alias name [" + index + "] self-conflicts with index name");
                    }
                }
            }

            assert eventIngestedRange != null : "eventIngestedRange must be set (non-null) when building IndexMetadata";
            final boolean isSearchableSnapshot = SearchableSnapshotsSettings.isSearchableSnapshotStore(settings);
            String indexModeString = settings.get(IndexSettings.MODE.getKey());
            final IndexMode indexMode = indexModeString != null ? IndexMode.fromString(indexModeString.toLowerCase(Locale.ROOT)) : null;
            final boolean isTsdb = indexMode == IndexMode.TIME_SERIES;
            return new IndexMetadata(
                new Index(index, uuid),
                version,
                mappingVersion,
                settingsVersion,
                aliasesVersion,
                primaryTerms,
                state,
                numberOfShards,
                numberOfReplicas,
                settings,
                mapping,
                inferenceFields.build(),
                aliasesMap,
                newCustomMetadata,
                Map.ofEntries(denseInSyncAllocationIds),
                requireFilters,
                initialRecoveryFilters,
                includeFilters,
                excludeFilters,
                indexCreatedVersion,
                mappingsUpdatedVersion,
                getRoutingNumShards(),
                routingPartitionSize,
                routingPaths,
                waitForActiveShards,
                rolloverInfos.build(),
                isSystem,
                INDEX_HIDDEN_SETTING.get(settings),
                timestampRange,
                eventIngestedRange,
                IndexMetadata.INDEX_PRIORITY_SETTING.get(settings),
                settings.getAsLong(SETTING_CREATION_DATE, -1L),
                DiskThresholdDecider.SETTING_IGNORE_DISK_WATERMARKS.get(settings),
                tierPreference,
                ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.get(settings),
                settings.get(IndexMetadata.LIFECYCLE_NAME), // n.b. lookup by name to get null-if-not-present semantics
                lifecycleExecutionState,
                AutoExpandReplicas.SETTING.get(settings),
                isSearchableSnapshot,
                isSearchableSnapshot && settings.getAsBoolean(SEARCHABLE_SNAPSHOT_PARTIAL_SETTING_KEY, false),
                indexMode,
                isTsdb ? IndexSettings.TIME_SERIES_START_TIME.get(settings) : null,
                isTsdb ? IndexSettings.TIME_SERIES_END_TIME.get(settings) : null,
                SETTING_INDEX_VERSION_COMPATIBILITY.get(settings),
                stats,
                indexWriteLoadForecast,
                shardSizeInBytesForecast,
                reshardingMetadata
            );
        }

        @SuppressWarnings("unchecked")
        public static void toXContent(IndexMetadata indexMetadata, XContentBuilder builder, ToXContent.Params params) throws IOException {
            Metadata.XContentContext context = Metadata.XContentContext.valueOf(
                params.param(CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_API)
            );

            builder.startObject(indexMetadata.getIndex().getName());

            builder.field(KEY_VERSION, indexMetadata.getVersion());
            builder.field(KEY_MAPPING_VERSION, indexMetadata.getMappingVersion());
            builder.field(KEY_SETTINGS_VERSION, indexMetadata.getSettingsVersion());
            builder.field(KEY_ALIASES_VERSION, indexMetadata.getAliasesVersion());
            builder.field(KEY_ROUTING_NUM_SHARDS, indexMetadata.getRoutingNumShards());

            builder.field(KEY_STATE, indexMetadata.getState().toString().toLowerCase(Locale.ENGLISH));

            boolean binary = params.paramAsBoolean("binary", false);

            builder.startObject(KEY_SETTINGS);
            if (context != Metadata.XContentContext.API) {
                indexMetadata.getSettings().toXContent(builder, Settings.FLAT_SETTINGS_TRUE);
            } else {
                indexMetadata.getSettings().toXContent(builder, params);
            }
            builder.endObject();

            if (context == Metadata.XContentContext.GATEWAY && params.paramAsBoolean(DEDUPLICATED_MAPPINGS_PARAM, false)) {
                MappingMetadata mmd = indexMetadata.mapping();
                if (mmd != null) {
                    builder.field(KEY_MAPPINGS_HASH, mmd.source().getSha256());
                }
            } else if (context != Metadata.XContentContext.API) {
                builder.startArray(KEY_MAPPINGS);
                MappingMetadata mmd = indexMetadata.mapping();
                if (mmd != null) {
                    if (binary) {
                        builder.value(mmd.source().compressed());
                    } else {
                        mmd.source().copyTo(builder);
                    }
                }
                builder.endArray();
            } else {
                builder.startObject(KEY_MAPPINGS);
                MappingMetadata mmd = indexMetadata.mapping();
                if (mmd != null) {
                    Map<String, Object> mapping = XContentHelper.convertToMap(mmd.source().uncompressed(), false).v2();
                    if (mapping.size() == 1 && mapping.containsKey(mmd.type())) {
                        // the type name is the root value, reduce it
                        mapping = (Map<String, Object>) mapping.get(mmd.type());
                    }
                    builder.field(mmd.type());
                    builder.map(mapping);
                }
                builder.endObject();
            }

            for (Map.Entry<String, DiffableStringMap> cursor : indexMetadata.customData.entrySet()) {
                builder.stringStringMap(cursor.getKey(), cursor.getValue());
            }

            if (context != Metadata.XContentContext.API) {
                builder.startObject(KEY_ALIASES);
                for (AliasMetadata aliasMetadata : indexMetadata.getAliases().values()) {
                    AliasMetadata.Builder.toXContent(aliasMetadata, builder, params);
                }
                builder.endObject();

                builder.startArray(KEY_PRIMARY_TERMS);
                for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                    builder.value(indexMetadata.primaryTerm(i));
                }
                builder.endArray();
            } else {
                builder.startArray(KEY_ALIASES);
                for (Map.Entry<String, AliasMetadata> cursor : indexMetadata.getAliases().entrySet()) {
                    builder.value(cursor.getKey());
                }
                builder.endArray();

                builder.startObject(IndexMetadata.KEY_PRIMARY_TERMS);
                for (int shard = 0; shard < indexMetadata.getNumberOfShards(); shard++) {
                    builder.field(Integer.toString(shard), indexMetadata.primaryTerm(shard));
                }
                builder.endObject();
            }

            builder.startObject(KEY_IN_SYNC_ALLOCATIONS);
            for (Map.Entry<Integer, Set<String>> cursor : indexMetadata.inSyncAllocationIds.entrySet()) {
                builder.startArray(String.valueOf(cursor.getKey()));
                for (String allocationId : cursor.getValue()) {
                    builder.value(allocationId);
                }
                builder.endArray();
            }
            builder.endObject();

            builder.startObject(KEY_ROLLOVER_INFOS);
            for (RolloverInfo rolloverInfo : indexMetadata.getRolloverInfos().values()) {
                rolloverInfo.toXContent(builder, params);
            }
            builder.endObject();

            builder.field(KEY_MAPPINGS_UPDATED_VERSION, indexMetadata.mappingsUpdatedVersion);
            builder.field(KEY_SYSTEM, indexMetadata.isSystem);

            builder.startObject(KEY_TIMESTAMP_RANGE);
            indexMetadata.timestampRange.toXContent(builder, params);
            builder.endObject();

            builder.startObject(KEY_EVENT_INGESTED_RANGE);
            indexMetadata.eventIngestedRange.toXContent(builder, params);
            builder.endObject();

            if (indexMetadata.stats != null) {
                builder.startObject(KEY_STATS);
                indexMetadata.stats.toXContent(builder, params);
                builder.endObject();
            }

            if (indexMetadata.writeLoadForecast != null) {
                builder.field(KEY_WRITE_LOAD_FORECAST, indexMetadata.writeLoadForecast);
            }

            if (indexMetadata.shardSizeInBytesForecast != null) {
                builder.field(KEY_SHARD_SIZE_FORECAST, indexMetadata.shardSizeInBytesForecast);
            }

            if (indexMetadata.getInferenceFields().isEmpty() == false) {
                builder.startObject(KEY_INFERENCE_FIELDS);
                for (InferenceFieldMetadata field : indexMetadata.getInferenceFields().values()) {
                    field.toXContent(builder, params);
                }
                builder.endObject();
            }

            if (indexMetadata.reshardingMetadata != null) {
                builder.startObject(KEY_RESHARDING);
                indexMetadata.reshardingMetadata.toXContent(builder, params);
                builder.endObject();
            }

            builder.endObject();
        }

        public static IndexMetadata fromXContent(XContentParser parser) throws IOException {
            return fromXContent(parser, null);
        }

        public static IndexMetadata fromXContent(XContentParser parser, Map<String, MappingMetadata> mappingsByHash) throws IOException {
            if (parser.currentToken() == null) { // fresh parser? move to the first token
                parser.nextToken();
            }
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {  // on a start object move to next token
                parser.nextToken();
            }
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
            Builder builder = new Builder(parser.currentName());
            // default to UNKNOWN so that reading 'event.ingested' range content works in older versions
            builder.eventIngestedRange(IndexLongFieldRange.UNKNOWN);
            String currentFieldName;
            XContentParser.Token token = parser.nextToken();
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
            boolean mappingVersion = false;
            boolean settingsVersion = false;
            boolean aliasesVersion = false;
            while ((currentFieldName = parser.nextFieldName()) != null) {
                token = parser.nextToken();
                if (token == XContentParser.Token.START_OBJECT) {
                    switch (currentFieldName) {
                        case KEY_SETTINGS:
                            builder.settings(Settings.fromXContent(parser));
                            break;
                        case KEY_MAPPINGS:
                            while ((currentFieldName = parser.nextFieldName()) != null) {
                                token = parser.nextToken();
                                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
                                builder.putMapping(new MappingMetadata(currentFieldName, Map.of(currentFieldName, parser.mapOrdered())));
                            }
                            break;
                        case KEY_ALIASES:
                            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                                builder.putAlias(AliasMetadata.Builder.fromXContent(parser));
                            }
                            break;
                        case KEY_IN_SYNC_ALLOCATIONS:
                            while ((currentFieldName = parser.nextFieldName()) != null) {
                                token = parser.nextToken();
                                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, token, parser);
                                final int shardId = Integer.parseInt(currentFieldName);
                                Set<String> allocationIds = new HashSet<>();
                                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                    if (token == XContentParser.Token.VALUE_STRING) {
                                        allocationIds.add(parser.text());
                                    }
                                }
                                builder.putInSyncAllocationIds(shardId, allocationIds);
                            }
                            break;
                        case KEY_ROLLOVER_INFOS:
                            while ((currentFieldName = parser.nextFieldName()) != null) {
                                token = parser.nextToken();
                                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
                                builder.putRolloverInfo(RolloverInfo.parse(parser, currentFieldName));
                            }
                            break;
                        case "warmers":
                            // TODO: do this in 6.0:
                            // throw new IllegalArgumentException("Warmers are not supported anymore - are you upgrading from 1.x?");
                            // ignore: warmers have been removed in 5.0 and are
                            // simply ignored when upgrading from 2.x
                            assert Version.CURRENT.major <= 5;
                            parser.skipChildren();
                            break;
                        case KEY_TIMESTAMP_RANGE:
                            builder.timestampRange(IndexLongFieldRange.fromXContent(parser));
                            break;
                        case KEY_EVENT_INGESTED_RANGE:
                            builder.eventIngestedRange(IndexLongFieldRange.fromXContent(parser));
                            break;
                        case KEY_STATS:
                            builder.stats(IndexMetadataStats.fromXContent(parser));
                            break;
                        case KEY_INFERENCE_FIELDS:
                            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                                builder.putInferenceField(InferenceFieldMetadata.fromXContent(parser));
                            }
                            break;
                        case KEY_RESHARDING:
                            builder.reshardingMetadata(IndexReshardingMetadata.fromXContent(parser));
                            break;
                        default:
                            // assume it's custom index metadata
                            builder.putCustom(currentFieldName, parser.mapStrings());
                            break;
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    switch (currentFieldName) {
                        case KEY_MAPPINGS:
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                                    builder.putMapping(new MappingMetadata(new CompressedXContent(parser.binaryValue())));
                                } else {
                                    Map<String, Object> mapping = parser.mapOrdered();
                                    if (mapping.size() == 1) {
                                        String mappingType = mapping.keySet().iterator().next();
                                        builder.putMapping(new MappingMetadata(mappingType, mapping));
                                    }
                                }
                            }
                            break;
                        case KEY_PRIMARY_TERMS:
                            ArrayList<Long> list = new ArrayList<>();
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                                list.add(parser.longValue());
                            }
                            builder.primaryTerms(list.stream().mapToLong(i -> i).toArray());
                            break;
                        default:
                            throw new IllegalArgumentException("Unexpected field for an array " + currentFieldName);
                    }
                } else if (token.isValue()) {
                    switch (currentFieldName) {
                        case KEY_STATE -> builder.state(State.fromString(parser.text()));
                        case KEY_VERSION -> builder.version(parser.longValue());
                        case KEY_MAPPING_VERSION -> {
                            mappingVersion = true;
                            builder.mappingVersion(parser.longValue());
                        }
                        case KEY_SETTINGS_VERSION -> {
                            settingsVersion = true;
                            builder.settingsVersion(parser.longValue());
                        }
                        case KEY_ALIASES_VERSION -> {
                            aliasesVersion = true;
                            builder.aliasesVersion(parser.longValue());
                        }
                        case KEY_ROUTING_NUM_SHARDS -> builder.setRoutingNumShards(parser.intValue());
                        case KEY_SYSTEM -> builder.system(parser.booleanValue());
                        case KEY_MAPPINGS_HASH -> {
                            assert mappingsByHash != null : "no deduplicated mappings given";
                            if (mappingsByHash.containsKey(parser.text()) == false) {
                                throw new IllegalArgumentException(
                                    "mapping of index [" + builder.index + "] with hash [" + parser.text() + "] not found"
                                );
                            }
                            builder.putMapping(mappingsByHash.get(parser.text()));
                        }
                        case KEY_MAPPINGS_UPDATED_VERSION -> builder.mappingsUpdatedVersion(IndexVersion.fromId(parser.intValue()));
                        case KEY_WRITE_LOAD_FORECAST -> builder.indexWriteLoadForecast(parser.doubleValue());
                        case KEY_SHARD_SIZE_FORECAST -> builder.shardSizeInBytesForecast(parser.longValue());
                        default -> throw new IllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new IllegalArgumentException("Unexpected token " + token);
                }
            }
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
            assert mappingVersion : "mapping version should be present for indices created on or after 6.5.0";
            assert settingsVersion : "settings version should be present for indices created on or after 6.5.0";
            assert indexCreatedVersion(builder.settings).before(IndexVersions.V_7_2_0) || aliasesVersion
                : "aliases version should be present for indices created on or after 7.2.0";
            return builder.build(true);
        }

        /**
         * Used to load legacy metadata from ES versions that are no longer index-compatible.
         * Returns information on best-effort basis.
         * Throws an exception if the metadata is index-compatible with the current version (in that case,
         * {@link #fromXContent} should be used to load the content.
         */
        public static IndexMetadata legacyFromXContent(XContentParser parser) throws IOException {
            if (parser.currentToken() == null) { // fresh parser? move to the first token
                parser.nextToken();
            }
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {  // on a start object move to next token
                parser.nextToken();
            }
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
            Builder builder = new Builder(parser.currentName());

            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("settings".equals(currentFieldName)) {
                        Settings settings = Settings.fromXContent(parser);
                        if (SETTING_INDEX_VERSION_COMPATIBILITY.get(settings).isLegacyIndexVersion() == false) {
                            throw new IllegalStateException(
                                "this method should only be used to parse older incompatible index metadata versions "
                                    + "but got "
                                    + SETTING_INDEX_VERSION_COMPATIBILITY.get(settings).toReleaseVersion()
                            );
                        }
                        builder.settings(settings);
                    } else if ("mappings".equals(currentFieldName)) {
                        Map<String, Object> mappingSourceBuilder = new HashMap<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                String mappingType = currentFieldName;
                                mappingSourceBuilder.put(mappingType, parser.mapOrdered());
                            } else {
                                throw new IllegalArgumentException("Unexpected token: " + token);
                            }
                        }
                        handleLegacyMapping(builder, mappingSourceBuilder);
                    } else if ("in_sync_allocations".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token == XContentParser.Token.START_ARRAY) {
                                String shardId = currentFieldName;
                                Set<String> allocationIds = new HashSet<>();
                                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                    if (token == XContentParser.Token.VALUE_STRING) {
                                        allocationIds.add(parser.text());
                                    }
                                }
                                builder.putInSyncAllocationIds(Integer.parseInt(shardId), allocationIds);
                            } else {
                                throw new IllegalArgumentException("Unexpected token: " + token);
                            }
                        }
                    } else {
                        // assume it's custom index metadata
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("mappings".equals(currentFieldName)) {
                        Map<String, Object> mappingSourceBuilder = new HashMap<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            Map<String, Object> mapping;
                            if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                                CompressedXContent compressedXContent = new CompressedXContent(parser.binaryValue());
                                mapping = XContentHelper.convertToMap(compressedXContent.compressedReference(), true).v2();
                            } else {
                                mapping = parser.mapOrdered();
                            }
                            mappingSourceBuilder.putAll(mapping);
                        }
                        handleLegacyMapping(builder, mappingSourceBuilder);
                    } else {
                        parser.skipChildren();
                    }
                } else if (token.isValue()) {
                    if ("state".equals(currentFieldName)) {
                        builder.state(State.fromString(parser.text()));
                    } else if ("version".equals(currentFieldName)) {
                        builder.version(parser.longValue());
                    } else if ("mapping_version".equals(currentFieldName)) {
                        builder.mappingVersion(parser.longValue());
                    } else if ("settings_version".equals(currentFieldName)) {
                        builder.settingsVersion(parser.longValue());
                    } else if ("routing_num_shards".equals(currentFieldName)) {
                        builder.setRoutingNumShards(parser.intValue());
                    } else {
                        // unknown, ignore
                    }
                } else {
                    XContentParserUtils.throwUnknownToken(token, parser);
                }
            }
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);

            if (builder.mapping() == null) {
                builder.putMapping(MappingMetadata.EMPTY_MAPPINGS); // just make sure it's not empty so that _source can be read
            }

            IndexMetadata indexMetadata = builder.build(true);
            assert indexMetadata.getCreationVersion().isLegacyIndexVersion();
            assert indexMetadata.getCompatibilityVersion().isLegacyIndexVersion();
            return indexMetadata;
        }

        private static void handleLegacyMapping(Builder builder, Map<String, Object> mapping) {
            if (mapping.size() == 1) {
                String mappingType = mapping.keySet().iterator().next();
                builder.putMapping(new MappingMetadata(mappingType, mapping));
            } else if (mapping.size() > 1) {
                builder.putMapping(new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, mapping));
            }
        }
    }

    /**
     * Return the {@link IndexVersion} of Elasticsearch that has been used to create an index given its settings.
     *
     * @throws IllegalArgumentException if the given index settings doesn't contain a value for the key
     *                                  {@value IndexMetadata#SETTING_VERSION_CREATED}
     */
    private static IndexVersion indexCreatedVersion(Settings indexSettings) {
        IndexVersion indexVersion = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(indexSettings);
        if (indexVersion.equals(IndexVersions.ZERO)) {
            final String message = String.format(
                Locale.ROOT,
                "[%s] is not present in the index settings for index with UUID [%s]",
                IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(),
                indexSettings.get(IndexMetadata.SETTING_INDEX_UUID)
            );
            throw new IllegalArgumentException(message);
        }
        return indexVersion;
    }

    /**
     * Adds human readable version and creation date settings.
     * This method is used to display the settings in a human readable format in REST API
     */
    public static Settings addHumanReadableSettings(Settings settings) {
        Settings.Builder builder = Settings.builder().put(settings);
        IndexVersion version = SETTING_INDEX_VERSION_CREATED.get(settings);
        if (version.equals(IndexVersions.ZERO) == false) {
            builder.put(SETTING_VERSION_CREATED_STRING, version.toReleaseVersion());
        }
        Long creationDate = settings.getAsLong(SETTING_CREATION_DATE, null);
        if (creationDate != null) {
            ZonedDateTime creationDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(creationDate), ZoneOffset.UTC);
            builder.put(SETTING_CREATION_DATE_STRING, creationDateTime.toString());
        }
        return builder.build();
    }

    private static final ToXContent.Params FORMAT_PARAMS;
    static {
        Map<String, String> params = Maps.newMapWithExpectedSize(2);
        params.put("binary", "true");
        params.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY);
        FORMAT_PARAMS = new MapParams(params);
    }

    /**
     * State format for {@link IndexMetadata} to write to and load from disk
     */
    public static final MetadataStateFormat<IndexMetadata> FORMAT = new MetadataStateFormat<IndexMetadata>(INDEX_STATE_FILE_PREFIX) {

        @Override
        public void toXContent(XContentBuilder builder, IndexMetadata state) throws IOException {
            Builder.toXContent(state, builder, FORMAT_PARAMS);
        }

        @Override
        public IndexMetadata fromXContent(XContentParser parser) throws IOException {
            return Builder.fromXContent(parser);
        }
    };

    /**
     * Returns the number of shards that should be used for routing. This basically defines the hash space we use in
     * {@link IndexRouting#indexShard} to route documents
     * to shards based on their ID or their specific routing value. The default value is {@link #getNumberOfShards()}. This value only
     * changes if and index is shrunk.
     */
    public int getRoutingNumShards() {
        return routingNumShards;
    }

    /**
     * Returns the routing factor for this index. The default is {@code 1}.
     *
     * @see #getRoutingFactor(int, int) for details
     */
    public int getRoutingFactor() {
        return routingFactor;
    }

    /**
     * Returns the source shard ID to split the given target shard off
     * @param shardId the id of the target shard to split into
     * @param sourceIndexMetadata the source index metadata
     * @param numTargetShards the total number of shards in the target index
     * @return a the source shard ID to split off from
     */
    public static ShardId selectSplitShard(int shardId, IndexMetadata sourceIndexMetadata, int numTargetShards) {
        int numSourceShards = sourceIndexMetadata.getNumberOfShards();
        if (shardId >= numTargetShards) {
            throw new IllegalArgumentException(
                "the number of target shards (" + numTargetShards + ") must be greater than the shard id: " + shardId
            );
        }
        final int routingFactor = getRoutingFactor(numSourceShards, numTargetShards);
        assertSplitMetadata(numSourceShards, numTargetShards, sourceIndexMetadata);
        return new ShardId(sourceIndexMetadata.getIndex(), shardId / routingFactor);
    }

    /**
     * Returns the source shard ID to clone the given target shard off
     * @param shardId the id of the target shard to clone into
     * @param sourceIndexMetadata the source index metadata
     * @param numTargetShards the total number of shards in the target index
     * @return a the source shard ID to clone from
     */
    public static ShardId selectCloneShard(int shardId, IndexMetadata sourceIndexMetadata, int numTargetShards) {
        int numSourceShards = sourceIndexMetadata.getNumberOfShards();
        if (numSourceShards != numTargetShards) {
            throw new IllegalArgumentException(
                "the number of target shards ("
                    + numTargetShards
                    + ") must be the same as the number of "
                    + " source shards ( "
                    + numSourceShards
                    + ")"
            );
        }
        return new ShardId(sourceIndexMetadata.getIndex(), shardId);
    }

    public static void assertSplitMetadata(int numSourceShards, int numTargetShards, IndexMetadata sourceIndexMetadata) {
        if (numSourceShards > numTargetShards) {
            throw new IllegalArgumentException(
                "the number of source shards ["
                    + numSourceShards
                    + "] must be less that the number of target shards ["
                    + numTargetShards
                    + "]"
            );
        }
        // now we verify that the numRoutingShards is valid in the source index
        // note: if the number of shards is 1 in the source index we can just assume it's correct since from 1 we can split into anything
        // this is important to special case here since we use this to validate this in various places in the code but allow to split form
        // 1 to N but we never modify the sourceIndexMetadata to accommodate for that
        int routingNumShards = numSourceShards == 1 ? numTargetShards : sourceIndexMetadata.getRoutingNumShards();
        if (routingNumShards % numTargetShards != 0) {
            throw new IllegalStateException(
                "the number of routing shards [" + routingNumShards + "] must be a multiple of the target shards [" + numTargetShards + "]"
            );
        }
        // this is just an additional assertion that ensures we are a factor of the routing num shards.
        assert sourceIndexMetadata.getNumberOfShards() == 1 // special case - we can split into anything from 1 shard
            || getRoutingFactor(numTargetShards, routingNumShards) >= 0;
    }

    /**
     * Selects the source shards for a local shard recovery. This might either be a split or a shrink operation.
     * @param shardId the target shard ID to select the source shards for
     * @param sourceIndexMetadata the source metadata
     * @param numTargetShards the number of target shards
     */
    public static Set<ShardId> selectRecoverFromShards(int shardId, IndexMetadata sourceIndexMetadata, int numTargetShards) {
        if (sourceIndexMetadata.getNumberOfShards() > numTargetShards) {
            return selectShrinkShards(shardId, sourceIndexMetadata, numTargetShards);
        } else if (sourceIndexMetadata.getNumberOfShards() < numTargetShards) {
            return Collections.singleton(selectSplitShard(shardId, sourceIndexMetadata, numTargetShards));
        } else {
            return Collections.singleton(selectCloneShard(shardId, sourceIndexMetadata, numTargetShards));
        }
    }

    /**
     * Returns the source shard ids to shrink into the given shard id.
     * @param shardId the id of the target shard to shrink to
     * @param sourceIndexMetadata the source index metadata
     * @param numTargetShards the total number of shards in the target index
     * @return a set of shard IDs to shrink into the given shard ID.
     */
    public static Set<ShardId> selectShrinkShards(int shardId, IndexMetadata sourceIndexMetadata, int numTargetShards) {
        if (shardId >= numTargetShards) {
            throw new IllegalArgumentException(
                "the number of target shards (" + numTargetShards + ") must be greater than the shard id: " + shardId
            );
        }
        if (sourceIndexMetadata.getNumberOfShards() < numTargetShards) {
            throw new IllegalArgumentException(
                "the number of target shards ["
                    + numTargetShards
                    + "] must be less that the number of source shards ["
                    + sourceIndexMetadata.getNumberOfShards()
                    + "]"
            );
        }
        int routingFactor = getRoutingFactor(sourceIndexMetadata.getNumberOfShards(), numTargetShards);
        Set<ShardId> shards = Sets.newHashSetWithExpectedSize(routingFactor);
        for (int i = shardId * routingFactor; i < routingFactor * shardId + routingFactor; i++) {
            shards.add(new ShardId(sourceIndexMetadata.getIndex(), i));
        }
        return shards;
    }

    /**
     * Returns the routing factor for and shrunk index with the given number of target shards.
     * This factor is used in the hash function in
     * {@link IndexRouting#indexShard} to guarantee consistent
     * hashing / routing of documents even if the number of shards changed (ie. a shrunk index).
     *
     * @param sourceNumberOfShards the total number of shards in the source index
     * @param targetNumberOfShards the total number of shards in the target index
     * @return the routing factor for and shrunk index with the given number of target shards.
     * @throws IllegalArgumentException if the number of source shards is less than the number of target shards or if the source shards
     * are not divisible by the number of target shards.
     */
    public static int getRoutingFactor(int sourceNumberOfShards, int targetNumberOfShards) {
        final int factor;
        if (sourceNumberOfShards < targetNumberOfShards) { // split
            factor = targetNumberOfShards / sourceNumberOfShards;
            if (factor * sourceNumberOfShards != targetNumberOfShards || factor <= 1) {
                throw new IllegalArgumentException(
                    "the number of source shards [" + sourceNumberOfShards + "] must be a " + "factor of [" + targetNumberOfShards + "]"
                );
            }
        } else if (sourceNumberOfShards > targetNumberOfShards) { // shrink
            factor = sourceNumberOfShards / targetNumberOfShards;
            if (factor * targetNumberOfShards != sourceNumberOfShards || factor <= 1) {
                throw new IllegalArgumentException(
                    "the number of source shards [" + sourceNumberOfShards + "] must be a " + "multiple of [" + targetNumberOfShards + "]"
                );
            }
        } else {
            factor = 1;
        }
        return factor;
    }

    /**
     * Parses the number from the rolled over index name. It also supports the date-math format (ie. index name is wrapped in &lt; and &gt;)
     * E.g.
     * - For ".ds-logs-000002" it will return 2
     * - For "&lt;logs-{now/d}-3&gt;" it'll return 3
     * @throws IllegalArgumentException if the index doesn't contain a "-" separator or if the last token after the separator is not a
     * number
     */
    public static int parseIndexNameCounter(String indexName) {
        int numberIndex = indexName.lastIndexOf('-');
        if (numberIndex == -1) {
            throw new IllegalArgumentException("no - separator found in index name [" + indexName + "]");
        }
        try {
            return Integer.parseInt(
                indexName.substring(numberIndex + 1, indexName.endsWith(">") ? indexName.length() - 1 : indexName.length())
            );
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("unable to parse the index name [" + indexName + "] to extract the counter", e);
        }
    }
}
