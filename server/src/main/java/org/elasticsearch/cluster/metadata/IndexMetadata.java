/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.Assertions;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNodeFilters;
import org.elasticsearch.cluster.routing.allocation.IndexMetadataUpdater;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.cluster.metadata.Metadata.CONTEXT_MODE_PARAM;
import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.IP_VALIDATOR;
import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.AND;
import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.OR;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

public class IndexMetadata implements Diffable<IndexMetadata>, ToXContentFragment {

    public static final ClusterBlock INDEX_READ_ONLY_BLOCK =
        new ClusterBlock(5, "index read-only (api)", false, false, false,
            RestStatus.FORBIDDEN, EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE));
    public static final ClusterBlock INDEX_READ_BLOCK =
        new ClusterBlock(7, "index read (api)", false, false, false,
            RestStatus.FORBIDDEN, EnumSet.of(ClusterBlockLevel.READ));
    public static final ClusterBlock INDEX_WRITE_BLOCK =
        new ClusterBlock(8, "index write (api)", false, false, false,
            RestStatus.FORBIDDEN, EnumSet.of(ClusterBlockLevel.WRITE));
    public static final ClusterBlock INDEX_METADATA_BLOCK =
        new ClusterBlock(9, "index metadata (api)", false, false, false,
            RestStatus.FORBIDDEN, EnumSet.of(ClusterBlockLevel.METADATA_WRITE, ClusterBlockLevel.METADATA_READ));
    public static final ClusterBlock INDEX_READ_ONLY_ALLOW_DELETE_BLOCK =
        new ClusterBlock(12, "disk usage exceeded flood-stage watermark, index has read-only-allow-delete block", false, false,
            true, RestStatus.TOO_MANY_REQUESTS, EnumSet.of(ClusterBlockLevel.METADATA_WRITE, ClusterBlockLevel.WRITE));

    public enum State {
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

        public static State fromString(String state) {
            if ("open".equals(state)) {
                return OPEN;
            } else if ("close".equals(state)) {
                return CLOSE;
            }
            throw new IllegalStateException("No state match for [" + state + "]");
        }
    }

    static Setting<Integer> buildNumberOfShardsSetting() {
        /* This is a safety limit that should only be exceeded in very rare and special cases. The assumption is that
         * 99% of the users have less than 1024 shards per index. We also make it a hard check that requires restart of nodes
         * if a cluster should allow to create more than 1024 shards per index. NOTE: this does not limit the number of shards
         * per cluster. this also prevents creating stuff like a new index with millions of shards by accident which essentially
         * kills the entire cluster with OOM on the spot.*/
        final int maxNumShards = Integer.parseInt(System.getProperty("es.index.max_number_of_shards", "1024"));
        if (maxNumShards < 1) {
            throw new IllegalArgumentException("es.index.max_number_of_shards must be > 0");
        }
        return Setting.intSetting(SETTING_NUMBER_OF_SHARDS, 1, 1, maxNumShards, Property.IndexScope, Property.Final);
    }

    public static final String INDEX_SETTING_PREFIX = "index.";
    public static final String SETTING_NUMBER_OF_SHARDS = "index.number_of_shards";
    public static final Setting<Integer> INDEX_NUMBER_OF_SHARDS_SETTING = buildNumberOfShardsSetting();
    public static final String SETTING_NUMBER_OF_REPLICAS = "index.number_of_replicas";
    public static final Setting<Integer> INDEX_NUMBER_OF_REPLICAS_SETTING =
        Setting.intSetting(SETTING_NUMBER_OF_REPLICAS, 1, 0, Property.Dynamic, Property.IndexScope);

    public static final String SETTING_ROUTING_PARTITION_SIZE = "index.routing_partition_size";
    public static final Setting<Integer> INDEX_ROUTING_PARTITION_SIZE_SETTING =
            Setting.intSetting(SETTING_ROUTING_PARTITION_SIZE, 1, 1, Property.IndexScope);

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
                    throw new IllegalArgumentException("index.number_of_routing_shards [" + numRoutingShards
                        + "] must be >= index.number_of_shards [" + numShards + "]");
                }
                getRoutingFactor(numShards, numRoutingShards);
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final List<Setting<?>> settings = List.of(INDEX_NUMBER_OF_SHARDS_SETTING);
                return settings.iterator();
            }

        },
        Property.IndexScope);

    public static final String SETTING_AUTO_EXPAND_REPLICAS = "index.auto_expand_replicas";
    public static final Setting<AutoExpandReplicas> INDEX_AUTO_EXPAND_REPLICAS_SETTING = AutoExpandReplicas.SETTING;

    public enum APIBlock implements Writeable {
        READ_ONLY("read_only", INDEX_READ_ONLY_BLOCK),
        READ("read", INDEX_READ_BLOCK),
        WRITE("write", INDEX_WRITE_BLOCK),
        METADATA("metadata", INDEX_METADATA_BLOCK),
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

    public static final Setting<Version> SETTING_INDEX_VERSION_CREATED =
            Setting.versionSetting(SETTING_VERSION_CREATED, Version.V_EMPTY, Property.IndexScope, Property.PrivateIndex);

    public static final String SETTING_VERSION_CREATED_STRING = "index.version.created_string";
    public static final String SETTING_CREATION_DATE = "index.creation_date";

    /**
     * These internal settings are no longer added to new indices. They are deprecated but still defined
     * to retain compatibility with old indexes. TODO: remove in 9.0.
     */
    @Deprecated public static final String SETTING_VERSION_UPGRADED = "index.version.upgraded";
    @Deprecated public static final String SETTING_VERSION_UPGRADED_STRING = "index.version.upgraded_string";

    /**
     * The user provided name for an index. This is the plain string provided by the user when the index was created.
     * It might still contain date math expressions etc. (added in 5.0)
     */
    public static final String SETTING_INDEX_PROVIDED_NAME = "index.provided_name";
    public static final String SETTING_PRIORITY = "index.priority";
    public static final Setting<Integer> INDEX_PRIORITY_SETTING =
        Setting.intSetting("index.priority", 1, 0, Property.Dynamic, Property.IndexScope);
    public static final String SETTING_CREATION_DATE_STRING = "index.creation_date_string";
    public static final String SETTING_INDEX_UUID = "index.uuid";
    public static final String SETTING_HISTORY_UUID = "index.history.uuid";
    public static final String SETTING_DATA_PATH = "index.data_path";
    public static final Setting<String> INDEX_DATA_PATH_SETTING =
        new Setting<>(SETTING_DATA_PATH, "", Function.identity(), Property.IndexScope, Property.Deprecated);
    public static final String INDEX_UUID_NA_VALUE = "_na_";

    public static final String INDEX_ROUTING_REQUIRE_GROUP_PREFIX = "index.routing.allocation.require";
    public static final String INDEX_ROUTING_INCLUDE_GROUP_PREFIX = "index.routing.allocation.include";
    public static final String INDEX_ROUTING_EXCLUDE_GROUP_PREFIX = "index.routing.allocation.exclude";
    public static final Setting.AffixSetting<String> INDEX_ROUTING_REQUIRE_GROUP_SETTING =
        Setting.prefixKeySetting(INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".", key ->
            Setting.simpleString(key, value -> IP_VALIDATOR.accept(key, value), Property.Dynamic, Property.IndexScope));
    public static final Setting.AffixSetting<String> INDEX_ROUTING_INCLUDE_GROUP_SETTING =
        Setting.prefixKeySetting(INDEX_ROUTING_INCLUDE_GROUP_PREFIX + ".", key ->
            Setting.simpleString(key, value -> IP_VALIDATOR.accept(key, value), Property.Dynamic, Property.IndexScope));
    public static final Setting.AffixSetting<String> INDEX_ROUTING_EXCLUDE_GROUP_SETTING =
        Setting.prefixKeySetting(INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + ".", key ->
            Setting.simpleString(key, value -> IP_VALIDATOR.accept(key, value), Property.Dynamic, Property.IndexScope));
    public static final Setting.AffixSetting<String> INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING =
        Setting.prefixKeySetting("index.routing.allocation.initial_recovery.", key -> Setting.simpleString(key));

    /**
     * The number of active shard copies to check for before proceeding with a write operation.
     */
    public static final Setting<ActiveShardCount> SETTING_WAIT_FOR_ACTIVE_SHARDS =
        new Setting<>("index.write.wait_for_active_shards",
                      "1",
                      ActiveShardCount::parseString,
                      Setting.Property.Dynamic,
                      Setting.Property.IndexScope);

    public static final String SETTING_INDEX_HIDDEN = "index.hidden";
    /**
     * Whether the index is considered hidden or not. A hidden index will not be resolved in
     * normal wildcard searches unless explicitly allowed
     */
    public static final Setting<Boolean> INDEX_HIDDEN_SETTING =
        Setting.boolSetting(SETTING_INDEX_HIDDEN, false, Property.Dynamic, Property.IndexScope);

    /**
     * an internal index format description, allowing us to find out if this index is upgraded or needs upgrading
     */
    private static final String INDEX_FORMAT = "index.format";
    public static final Setting<Integer> INDEX_FORMAT_SETTING =
            Setting.intSetting(INDEX_FORMAT, 0, Setting.Property.IndexScope, Setting.Property.Final);

    public static final String KEY_IN_SYNC_ALLOCATIONS = "in_sync_allocations";
    static final String KEY_VERSION = "version";
    static final String KEY_MAPPING_VERSION = "mapping_version";
    static final String KEY_SETTINGS_VERSION = "settings_version";
    static final String KEY_ALIASES_VERSION = "aliases_version";
    static final String KEY_ROUTING_NUM_SHARDS = "routing_num_shards";
    static final String KEY_SETTINGS = "settings";
    static final String KEY_STATE = "state";
    static final String KEY_MAPPINGS = "mappings";
    static final String KEY_ALIASES = "aliases";
    static final String KEY_ROLLOVER_INFOS = "rollover_info";
    static final String KEY_SYSTEM = "system";
    static final String KEY_TIMESTAMP_RANGE = "timestamp_range";
    public static final String KEY_PRIMARY_TERMS = "primary_terms";

    public static final String INDEX_STATE_FILE_PREFIX = "state-";

    static final Version SYSTEM_INDEX_FLAG_ADDED = Version.V_7_10_0;

    private final int routingNumShards;
    private final int routingFactor;
    private final int routingPartitionSize;

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

    private final ImmutableOpenMap<String, MappingMetadata> mappings;

    private final ImmutableOpenMap<String, DiffableStringMap> customData;

    private final ImmutableOpenIntMap<Set<String>> inSyncAllocationIds;

    private final transient int totalNumberOfShards;

    private final DiscoveryNodeFilters requireFilters;
    private final DiscoveryNodeFilters includeFilters;
    private final DiscoveryNodeFilters excludeFilters;
    private final DiscoveryNodeFilters initialRecoveryFilters;

    private final Version indexCreatedVersion;

    private final ActiveShardCount waitForActiveShards;
    private final ImmutableOpenMap<String, RolloverInfo> rolloverInfos;
    private final boolean isSystem;

    private final IndexLongFieldRange timestampRange;

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
            final ImmutableOpenMap<String, MappingMetadata> mappings,
            final ImmutableOpenMap<String, AliasMetadata> aliases,
            final ImmutableOpenMap<String, DiffableStringMap> customData,
            final ImmutableOpenIntMap<Set<String>> inSyncAllocationIds,
            final DiscoveryNodeFilters requireFilters,
            final DiscoveryNodeFilters initialRecoveryFilters,
            final DiscoveryNodeFilters includeFilters,
            final DiscoveryNodeFilters excludeFilters,
            final Version indexCreatedVersion,
            final int routingNumShards,
            final int routingPartitionSize,
            final ActiveShardCount waitForActiveShards,
            final ImmutableOpenMap<String, RolloverInfo> rolloverInfos,
            final boolean isSystem,
            final IndexLongFieldRange timestampRange) {

        this.index = index;
        this.version = version;
        assert mappingVersion >= 0 : mappingVersion;
        this.mappingVersion = mappingVersion;
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
        this.mappings = mappings;
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
        this.waitForActiveShards = waitForActiveShards;
        this.rolloverInfos = rolloverInfos;
        this.isSystem = isSystem;
        this.timestampRange = timestampRange;
        assert numberOfShards * routingFactor == routingNumShards :  routingNumShards + " must be a multiple of " + numberOfShards;
    }

    public Index getIndex() {
        return index;
    }

    public String getIndexUUID() {
        return index.getUUID();
    }

    /**
     * Test whether the current index UUID is the same as the given one. Returns true if either are _na_
     */
    public boolean isSameUUID(String otherUUID) {
        assert otherUUID != null;
        assert getIndexUUID() != null;
        if (INDEX_UUID_NA_VALUE.equals(otherUUID) || INDEX_UUID_NA_VALUE.equals(getIndexUUID())) {
            return true;
        }
        return otherUUID.equals(getIndexUUID());
    }

    public long getVersion() {
        return this.version;
    }

    public long getMappingVersion() {
        return mappingVersion;
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
     * Return the {@link Version} on which this index has been created. This
     * information is typically useful for backward compatibility.
     */
    public Version getCreationVersion() {
        return indexCreatedVersion;
    }

    public long getCreationDate() {
        return settings.getAsLong(SETTING_CREATION_DATE, -1L);
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

    public Settings getSettings() {
        return settings;
    }

    public ImmutableOpenMap<String, AliasMetadata> getAliases() {
        return this.aliases;
    }

    /**
     * Return the concrete mapping for this index or {@code null} if this index has no mappings at all.
     */
    @Nullable
    public MappingMetadata mapping() {
        for (ObjectObjectCursor<String, MappingMetadata> cursor : mappings) {
            return cursor.value;
        }
        return null;
    }

    public static final String INDEX_RESIZE_SOURCE_UUID_KEY = "index.resize.source.uuid";
    public static final String INDEX_RESIZE_SOURCE_NAME_KEY = "index.resize.source.name";
    public static final Setting<String> INDEX_RESIZE_SOURCE_UUID = Setting.simpleString(INDEX_RESIZE_SOURCE_UUID_KEY);
    public static final Setting<String> INDEX_RESIZE_SOURCE_NAME = Setting.simpleString(INDEX_RESIZE_SOURCE_NAME_KEY);

    public Index getResizeSourceIndex() {
        return INDEX_RESIZE_SOURCE_UUID.exists(settings) ? new Index(INDEX_RESIZE_SOURCE_NAME.get(settings),
            INDEX_RESIZE_SOURCE_UUID.get(settings)) : null;
    }

    public static final String INDEX_ROLLUP_SOURCE_UUID_KEY = "index.rollup.source.uuid";
    public static final String INDEX_ROLLUP_SOURCE_NAME_KEY = "index.rollup.source.name";
    public static final Setting<String> INDEX_ROLLUP_SOURCE_UUID = Setting.simpleString(INDEX_ROLLUP_SOURCE_UUID_KEY,
        Property.IndexScope, Property.PrivateIndex);
    public static final Setting<String> INDEX_ROLLUP_SOURCE_NAME = Setting.simpleString(INDEX_ROLLUP_SOURCE_NAME_KEY,
        Property.IndexScope, Property.PrivateIndex);

    ImmutableOpenMap<String, DiffableStringMap> getCustomData() {
        return this.customData;
    }

    public Map<String, String> getCustomData(final String key) {
        return this.customData.get(key);
    }

    public ImmutableOpenIntMap<Set<String>> getInSyncAllocationIds() {
        return inSyncAllocationIds;
    }

    public ImmutableOpenMap<String, RolloverInfo> getRolloverInfos() {
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
        if (mappings.equals(that.mappings) == false) {
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
        if (isSystem != that.isSystem) {
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
        result = 31 * result + mappings.hashCode();
        result = 31 * result + customData.hashCode();
        result = 31 * result + Long.hashCode(routingFactor);
        result = 31 * result + Long.hashCode(routingNumShards);
        result = 31 * result + Arrays.hashCode(primaryTerms);
        result = 31 * result + inSyncAllocationIds.hashCode();
        result = 31 * result + rolloverInfos.hashCode();
        result = 31 * result + Boolean.hashCode(isSystem);
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Builder.toXContent(this, builder, params);
        return builder;
    }

    private static class IndexMetadataDiff implements Diff<IndexMetadata> {

        private final String index;
        private final int routingNumShards;
        private final long version;
        private final long mappingVersion;
        private final long settingsVersion;
        private final long aliasesVersion;
        private final long[] primaryTerms;
        private final State state;
        private final Settings settings;
        private final Diff<ImmutableOpenMap<String, MappingMetadata>> mappings;
        private final Diff<ImmutableOpenMap<String, AliasMetadata>> aliases;
        private final Diff<ImmutableOpenMap<String, DiffableStringMap>> customData;
        private final Diff<ImmutableOpenIntMap<Set<String>>> inSyncAllocationIds;
        private final Diff<ImmutableOpenMap<String, RolloverInfo>> rolloverInfos;
        private final boolean isSystem;
        private final IndexLongFieldRange timestampRange;

        IndexMetadataDiff(IndexMetadata before, IndexMetadata after) {
            index = after.index.getName();
            version = after.version;
            mappingVersion = after.mappingVersion;
            settingsVersion = after.settingsVersion;
            aliasesVersion = after.aliasesVersion;
            routingNumShards = after.routingNumShards;
            state = after.state;
            settings = after.settings;
            primaryTerms = after.primaryTerms;
            mappings = DiffableUtils.diff(before.mappings, after.mappings, DiffableUtils.getStringKeySerializer());
            aliases = DiffableUtils.diff(before.aliases, after.aliases, DiffableUtils.getStringKeySerializer());
            customData = DiffableUtils.diff(before.customData, after.customData, DiffableUtils.getStringKeySerializer());
            inSyncAllocationIds = DiffableUtils.diff(before.inSyncAllocationIds, after.inSyncAllocationIds,
                DiffableUtils.getVIntKeySerializer(), DiffableUtils.StringSetValueSerializer.getInstance());
            rolloverInfos = DiffableUtils.diff(before.rolloverInfos, after.rolloverInfos, DiffableUtils.getStringKeySerializer());
            isSystem = after.isSystem;
            timestampRange = after.timestampRange;
        }

        private static final DiffableUtils.DiffableValueReader<String, AliasMetadata> ALIAS_METADATA_DIFF_VALUE_READER =
                new DiffableUtils.DiffableValueReader<>(AliasMetadata::new, AliasMetadata::readDiffFrom);
        private static final DiffableUtils.DiffableValueReader<String, MappingMetadata> MAPPING_DIFF_VALUE_READER =
                new DiffableUtils.DiffableValueReader<>(MappingMetadata::new, MappingMetadata::readDiffFrom);
        private static final DiffableUtils.DiffableValueReader<String, DiffableStringMap> CUSTOM_DIFF_VALUE_READER =
                new DiffableUtils.DiffableValueReader<>(DiffableStringMap::readFrom, DiffableStringMap::readDiffFrom);
        private static final DiffableUtils.DiffableValueReader<String, RolloverInfo> ROLLOVER_INFO_DIFF_VALUE_READER =
                new DiffableUtils.DiffableValueReader<>(RolloverInfo::new, RolloverInfo::readDiffFrom);

        IndexMetadataDiff(StreamInput in) throws IOException {
            index = in.readString();
            routingNumShards = in.readInt();
            version = in.readLong();
            mappingVersion = in.readVLong();
            settingsVersion = in.readVLong();
            if (in.getVersion().onOrAfter(Version.V_7_2_0)) {
                aliasesVersion = in.readVLong();
            } else {
                aliasesVersion = 1;
            }
            state = State.fromId(in.readByte());
            settings = Settings.readSettingsFromStream(in);
            primaryTerms = in.readVLongArray();
            mappings = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), MAPPING_DIFF_VALUE_READER);
            aliases = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), ALIAS_METADATA_DIFF_VALUE_READER);
            customData = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), CUSTOM_DIFF_VALUE_READER);
            inSyncAllocationIds = DiffableUtils.readImmutableOpenIntMapDiff(in, DiffableUtils.getVIntKeySerializer(),
                    DiffableUtils.StringSetValueSerializer.getInstance());
            rolloverInfos =
                    DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), ROLLOVER_INFO_DIFF_VALUE_READER);
            if (in.getVersion().onOrAfter(SYSTEM_INDEX_FLAG_ADDED)) {
                isSystem = in.readBoolean();
            } else {
                isSystem = false;
            }
            timestampRange = IndexLongFieldRange.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeInt(routingNumShards);
            out.writeLong(version);
            out.writeVLong(mappingVersion);
            out.writeVLong(settingsVersion);
            if (out.getVersion().onOrAfter(Version.V_7_2_0)) {
                out.writeVLong(aliasesVersion);
            }
            out.writeByte(state.id);
            Settings.writeSettingsToStream(settings, out);
            out.writeVLongArray(primaryTerms);
            mappings.writeTo(out);
            aliases.writeTo(out);
            customData.writeTo(out);
            inSyncAllocationIds.writeTo(out);
            rolloverInfos.writeTo(out);
            if (out.getVersion().onOrAfter(SYSTEM_INDEX_FLAG_ADDED)) {
                out.writeBoolean(isSystem);
            }
            timestampRange.writeTo(out);
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
            builder.settings(settings);
            builder.primaryTerms(primaryTerms);
            builder.mappings.putAll(mappings.apply(part.mappings));
            builder.aliases.putAll(aliases.apply(part.aliases));
            builder.customMetadata.putAll(customData.apply(part.customData));
            builder.inSyncAllocationIds.putAll(inSyncAllocationIds.apply(part.inSyncAllocationIds));
            builder.rolloverInfos.putAll(rolloverInfos.apply(part.rolloverInfos));
            builder.system(isSystem);
            builder.timestampRange(timestampRange);
            return builder.build();
        }
    }

    public static IndexMetadata readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder(in.readString());
        builder.version(in.readLong());
        builder.mappingVersion(in.readVLong());
        builder.settingsVersion(in.readVLong());
        if (in.getVersion().onOrAfter(Version.V_7_2_0)) {
            builder.aliasesVersion(in.readVLong());
        }
        builder.setRoutingNumShards(in.readInt());
        builder.state(State.fromId(in.readByte()));
        builder.settings(readSettingsFromStream(in));
        builder.primaryTerms(in.readVLongArray());
        int mappingsSize = in.readVInt();
        for (int i = 0; i < mappingsSize; i++) {
            MappingMetadata mappingMd = new MappingMetadata(in);
            builder.putMapping(mappingMd);
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
        if (in.getVersion().onOrAfter(SYSTEM_INDEX_FLAG_ADDED)) {
            builder.system(in.readBoolean());
        }
        builder.timestampRange(IndexLongFieldRange.readFrom(in));
        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index.getName()); // uuid will come as part of settings
        out.writeLong(version);
        out.writeVLong(mappingVersion);
        out.writeVLong(settingsVersion);
        if (out.getVersion().onOrAfter(Version.V_7_2_0)) {
            out.writeVLong(aliasesVersion);
        }
        out.writeInt(routingNumShards);
        out.writeByte(state.id());
        writeSettingsToStream(settings, out);
        out.writeVLongArray(primaryTerms);
        out.writeVInt(mappings.size());
        for (ObjectCursor<MappingMetadata> cursor : mappings.values()) {
            cursor.value.writeTo(out);
        }
        out.writeVInt(aliases.size());
        for (ObjectCursor<AliasMetadata> cursor : aliases.values()) {
            cursor.value.writeTo(out);
        }
        out.writeVInt(customData.size());
        for (final ObjectObjectCursor<String, DiffableStringMap> cursor : customData) {
            out.writeString(cursor.key);
            cursor.value.writeTo(out);
        }
        out.writeVInt(inSyncAllocationIds.size());
        for (IntObjectCursor<Set<String>> cursor : inSyncAllocationIds) {
            out.writeVInt(cursor.key);
            DiffableUtils.StringSetValueSerializer.getInstance().write(cursor.value, out);
        }
        out.writeVInt(rolloverInfos.size());
        for (ObjectCursor<RolloverInfo> cursor : rolloverInfos.values()) {
            cursor.value.writeTo(out);
        }
        if (out.getVersion().onOrAfter(SYSTEM_INDEX_FLAG_ADDED)) {
            out.writeBoolean(isSystem);
        }
        timestampRange.writeTo(out);
    }

    public boolean isSystem() {
        return isSystem;
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
        private Settings settings = Settings.Builder.EMPTY_SETTINGS;
        private final ImmutableOpenMap.Builder<String, MappingMetadata> mappings;
        private final ImmutableOpenMap.Builder<String, AliasMetadata> aliases;
        private final ImmutableOpenMap.Builder<String, DiffableStringMap> customMetadata;
        private final ImmutableOpenIntMap.Builder<Set<String>> inSyncAllocationIds;
        private final ImmutableOpenMap.Builder<String, RolloverInfo> rolloverInfos;
        private Integer routingNumShards;
        private boolean isSystem;
        private IndexLongFieldRange timestampRange = IndexLongFieldRange.NO_SHARDS;

        public Builder(String index) {
            this.index = index;
            this.mappings = ImmutableOpenMap.builder();
            this.aliases = ImmutableOpenMap.builder();
            this.customMetadata = ImmutableOpenMap.builder();
            this.inSyncAllocationIds = ImmutableOpenIntMap.builder();
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
            this.mappings = ImmutableOpenMap.builder(indexMetadata.mappings);
            this.aliases = ImmutableOpenMap.builder(indexMetadata.aliases);
            this.customMetadata = ImmutableOpenMap.builder(indexMetadata.customData);
            this.routingNumShards = indexMetadata.routingNumShards;
            this.inSyncAllocationIds = ImmutableOpenIntMap.builder(indexMetadata.inSyncAllocationIds);
            this.rolloverInfos = ImmutableOpenMap.builder(indexMetadata.rolloverInfos);
            this.isSystem = indexMetadata.isSystem;
            this.timestampRange = indexMetadata.timestampRange;
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
            return mappings.get(MapperService.SINGLE_MAPPING_NAME);
        }

        public Builder putMapping(String source) {
            putMapping(new MappingMetadata(MapperService.SINGLE_MAPPING_NAME,
                XContentHelper.convertToMap(XContentFactory.xContent(source), source, true)));
            return this;
        }

        public Builder putMapping(MappingMetadata mappingMd) {
            mappings.clear();
            if (mappingMd != null) {
                mappings.put(MapperService.SINGLE_MAPPING_NAME, mappingMd);
            }
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
            inSyncAllocationIds.put(shardId, new HashSet<>(allocationIds));
            return this;
        }

        public Builder putRolloverInfo(RolloverInfo rolloverInfo) {
            rolloverInfos.put(rolloverInfo.getAlias(), rolloverInfo);
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

        public long aliasesVersion() {
            return aliasesVersion;
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

        public IndexLongFieldRange getTimestampRange() {
            return timestampRange;
        }

        public IndexMetadata build() {
            ImmutableOpenMap.Builder<String, AliasMetadata> tmpAliases = aliases;
            Settings tmpSettings = settings;

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
                throw new IllegalArgumentException("routing partition size [" + routingPartitionSize + "] should be a positive number"
                        + " less than the number of shards [" + getRoutingNumShards() + "] for [" + index + "]");
            }

            // fill missing slots in inSyncAllocationIds with empty set if needed and make all entries immutable
            ImmutableOpenIntMap.Builder<Set<String>> filledInSyncAllocationIds = ImmutableOpenIntMap.builder();
            for (int i = 0; i < numberOfShards; i++) {
                if (inSyncAllocationIds.containsKey(i)) {
                    filledInSyncAllocationIds.put(i, Set.copyOf(inSyncAllocationIds.get(i)));
                } else {
                    filledInSyncAllocationIds.put(i, Collections.emptySet());
                }
            }
            final Map<String, String> requireMap = INDEX_ROUTING_REQUIRE_GROUP_SETTING.getAsMap(settings);
            final DiscoveryNodeFilters requireFilters;
            if (requireMap.isEmpty()) {
                requireFilters = null;
            } else {
                requireFilters = DiscoveryNodeFilters.buildFromKeyValue(AND, requireMap);
            }
            Map<String, String> includeMap = INDEX_ROUTING_INCLUDE_GROUP_SETTING.getAsMap(settings);
            final DiscoveryNodeFilters includeFilters;
            if (includeMap.isEmpty()) {
                includeFilters = null;
            } else {
                includeFilters = DiscoveryNodeFilters.buildFromKeyValue(OR, includeMap);
            }
            Map<String, String> excludeMap = INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getAsMap(settings);
            final DiscoveryNodeFilters excludeFilters;
            if (excludeMap.isEmpty()) {
                excludeFilters = null;
            } else {
                excludeFilters = DiscoveryNodeFilters.buildFromKeyValue(OR, excludeMap);
            }
            Map<String, String> initialRecoveryMap = INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING.getAsMap(settings);
            final DiscoveryNodeFilters initialRecoveryFilters;
            if (initialRecoveryMap.isEmpty()) {
                initialRecoveryFilters = null;
            } else {
                initialRecoveryFilters = DiscoveryNodeFilters.buildFromKeyValue(OR, initialRecoveryMap);
            }
            Version indexCreatedVersion = indexCreatedVersion(settings);

            if (primaryTerms == null) {
                initializePrimaryTerms();
            } else if (primaryTerms.length != numberOfShards) {
                throw new IllegalStateException("primaryTerms length is [" + primaryTerms.length
                    + "] but should be equal to number of shards [" + numberOfShards() + "]");
            }

            final ActiveShardCount waitForActiveShards = SETTING_WAIT_FOR_ACTIVE_SHARDS.get(settings);
            if (waitForActiveShards.validate(numberOfReplicas) == false) {
                throw new IllegalArgumentException("invalid " + SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey() +
                                                   "[" + waitForActiveShards + "]: cannot be greater than " +
                                                   "number of shard copies [" + (numberOfReplicas + 1) + "]");
            }

            final String uuid = settings.get(SETTING_INDEX_UUID, INDEX_UUID_NA_VALUE);

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
                    tmpSettings,
                    mappings.build(),
                    tmpAliases.build(),
                    customMetadata.build(),
                    filledInSyncAllocationIds.build(),
                    requireFilters,
                    initialRecoveryFilters,
                    includeFilters,
                    excludeFilters,
                    indexCreatedVersion,
                    getRoutingNumShards(),
                    routingPartitionSize,
                    waitForActiveShards,
                    rolloverInfos.build(),
                    isSystem,
                timestampRange);
        }

        public static void toXContent(IndexMetadata indexMetadata, XContentBuilder builder, ToXContent.Params params) throws IOException {
            Metadata.XContentContext context = Metadata.XContentContext.valueOf(
                params.param(CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_API));

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
                indexMetadata.getSettings().toXContent(builder, new MapParams(Collections.singletonMap("flat_settings", "true")));
            } else {
                indexMetadata.getSettings().toXContent(builder, params);
            }
            builder.endObject();

            if (context != Metadata.XContentContext.API) {
                builder.startArray(KEY_MAPPINGS);
                MappingMetadata mmd = indexMetadata.mapping();
                if (mmd != null) {
                    if (binary) {
                        builder.value(mmd.source().compressed());
                    } else {
                        builder.map(XContentHelper.convertToMap(mmd.source().uncompressed(), true).v2());
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

            for (ObjectObjectCursor<String, DiffableStringMap> cursor : indexMetadata.customData) {
                builder.field(cursor.key);
                builder.map(cursor.value);
            }

            if (context != Metadata.XContentContext.API) {
                builder.startObject(KEY_ALIASES);
                for (ObjectCursor<AliasMetadata> cursor : indexMetadata.getAliases().values()) {
                    AliasMetadata.Builder.toXContent(cursor.value, builder, params);
                }
                builder.endObject();

                builder.startArray(KEY_PRIMARY_TERMS);
                for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                    builder.value(indexMetadata.primaryTerm(i));
                }
                builder.endArray();
            } else {
                builder.startArray(KEY_ALIASES);
                for (ObjectCursor<String> cursor : indexMetadata.getAliases().keys()) {
                    builder.value(cursor.value);
                }
                builder.endArray();

                builder.startObject(IndexMetadata.KEY_PRIMARY_TERMS);
                for (int shard = 0; shard < indexMetadata.getNumberOfShards(); shard++) {
                    builder.field(Integer.toString(shard), indexMetadata.primaryTerm(shard));
                }
                builder.endObject();
            }


            builder.startObject(KEY_IN_SYNC_ALLOCATIONS);
            for (IntObjectCursor<Set<String>> cursor : indexMetadata.inSyncAllocationIds) {
                builder.startArray(String.valueOf(cursor.key));
                for (String allocationId : cursor.value) {
                    builder.value(allocationId);
                }
                builder.endArray();
            }
            builder.endObject();

            builder.startObject(KEY_ROLLOVER_INFOS);
            for (ObjectCursor<RolloverInfo> cursor : indexMetadata.getRolloverInfos().values()) {
                cursor.value.toXContent(builder, params);
            }
            builder.endObject();
            builder.field(KEY_SYSTEM, indexMetadata.isSystem);

            builder.startObject(KEY_TIMESTAMP_RANGE);
            indexMetadata.timestampRange.toXContent(builder, params);
            builder.endObject();

            builder.endObject();
        }

        public static IndexMetadata fromXContent(XContentParser parser) throws IOException {
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
            boolean mappingVersion = false;
            boolean settingsVersion = false;
            boolean aliasesVersion = false;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (KEY_SETTINGS.equals(currentFieldName)) {
                        builder.settings(Settings.fromXContent(parser));
                    } else if (KEY_MAPPINGS.equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                String mappingType = currentFieldName;
                                Map<String, Object> mappingSource =
                                    MapBuilder.<String, Object>newMapBuilder().put(mappingType, parser.mapOrdered()).map();
                                builder.putMapping(new MappingMetadata(mappingType, mappingSource));
                            } else {
                                throw new IllegalArgumentException("Unexpected token: " + token);
                            }
                        }
                    } else if (KEY_ALIASES.equals(currentFieldName)) {
                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            builder.putAlias(AliasMetadata.Builder.fromXContent(parser));
                        }
                    } else if (KEY_IN_SYNC_ALLOCATIONS.equals(currentFieldName)) {
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
                                builder.putInSyncAllocationIds(Integer.valueOf(shardId), allocationIds);
                            } else {
                                throw new IllegalArgumentException("Unexpected token: " + token);
                            }
                        }
                    } else if (KEY_ROLLOVER_INFOS.equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                builder.putRolloverInfo(RolloverInfo.parse(parser, currentFieldName));
                            } else {
                                throw new IllegalArgumentException("Unexpected token: " + token);
                            }
                        }
                    } else if ("warmers".equals(currentFieldName)) {
                        // TODO: do this in 6.0:
                        // throw new IllegalArgumentException("Warmers are not supported anymore - are you upgrading from 1.x?");
                        // ignore: warmers have been removed in 5.0 and are
                        // simply ignored when upgrading from 2.x
                        assert Version.CURRENT.major <= 5;
                        parser.skipChildren();
                    } else if (KEY_TIMESTAMP_RANGE.equals(currentFieldName)) {
                        builder.timestampRange(IndexLongFieldRange.fromXContent(parser));
                    } else {
                        // assume it's custom index metadata
                        builder.putCustom(currentFieldName, parser.mapStrings());
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if (KEY_MAPPINGS.equals(currentFieldName)) {
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
                    } else if (KEY_PRIMARY_TERMS.equals(currentFieldName)) {
                        LongArrayList list = new LongArrayList();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                            list.add(parser.longValue());
                        }
                        builder.primaryTerms(list.toArray());
                    } else {
                        throw new IllegalArgumentException("Unexpected field for an array " + currentFieldName);
                    }
                } else if (token.isValue()) {
                    if (KEY_STATE.equals(currentFieldName)) {
                        builder.state(State.fromString(parser.text()));
                    } else if (KEY_VERSION.equals(currentFieldName)) {
                        builder.version(parser.longValue());
                    } else if (KEY_MAPPING_VERSION.equals(currentFieldName)) {
                        mappingVersion = true;
                        builder.mappingVersion(parser.longValue());
                    } else if (KEY_SETTINGS_VERSION.equals(currentFieldName)) {
                        settingsVersion = true;
                        builder.settingsVersion(parser.longValue());
                    } else if (KEY_ALIASES_VERSION.equals(currentFieldName)) {
                        aliasesVersion = true;
                        builder.aliasesVersion(parser.longValue());
                    } else if (KEY_ROUTING_NUM_SHARDS.equals(currentFieldName)) {
                        builder.setRoutingNumShards(parser.intValue());
                    } else if (KEY_SYSTEM.equals(currentFieldName)) {
                        builder.system(parser.booleanValue());
                    } else {
                        throw new IllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new IllegalArgumentException("Unexpected token " + token);
                }
            }
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
            if (Assertions.ENABLED) {
                assert mappingVersion : "mapping version should be present for indices created on or after 6.5.0";
            }
            if (Assertions.ENABLED) {
                assert settingsVersion : "settings version should be present for indices created on or after 6.5.0";
            }

            Version indexVersion = indexCreatedVersion(builder.settings);
            if (Assertions.ENABLED && indexVersion.onOrAfter(Version.V_7_2_0)) {
                assert aliasesVersion : "aliases version should be present for indices created on or after 7.2.0";
            }
            return builder.build();
        }
    }

    /**
     * Return the {@link Version} of Elasticsearch that has been used to create an index given its settings.
     *
     * @throws IllegalArgumentException if the given index settings doesn't contain a value for the key
     *                                  {@value IndexMetadata#SETTING_VERSION_CREATED}
     */
    private static Version indexCreatedVersion(Settings indexSettings) {
        final Version indexVersion = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(indexSettings);
        if (indexVersion == Version.V_EMPTY) {
            final String message = String.format(
                Locale.ROOT,
                "[%s] is not present in the index settings for index with UUID [%s]",
                IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(),
                indexSettings.get(IndexMetadata.SETTING_INDEX_UUID));
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
        Version version = SETTING_INDEX_VERSION_CREATED.get(settings);
        if (version != Version.V_EMPTY) {
            builder.put(SETTING_VERSION_CREATED_STRING, version.toString());
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
        Map<String, String> params = new HashMap<>(2);
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
     * {@link org.elasticsearch.cluster.routing.OperationRouting#generateShardId(IndexMetadata, String, String)} to route documents
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
            throw new IllegalArgumentException("the number of target shards (" + numTargetShards + ") must be greater than the shard id: "
                + shardId);
        }
        final int routingFactor = getRoutingFactor(numSourceShards, numTargetShards);
        assertSplitMetadata(numSourceShards, numTargetShards, sourceIndexMetadata);
        return new ShardId(sourceIndexMetadata.getIndex(), shardId/routingFactor);
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
            throw new IllegalArgumentException("the number of target shards (" + numTargetShards + ") must be the same as the number of "
                + " source shards ( " + numSourceShards + ")");
        }
        return new ShardId(sourceIndexMetadata.getIndex(), shardId);
    }

    private static void assertSplitMetadata(int numSourceShards, int numTargetShards, IndexMetadata sourceIndexMetadata) {
        if (numSourceShards > numTargetShards) {
            throw new IllegalArgumentException("the number of source shards [" + numSourceShards
                + "] must be less that the number of target shards [" + numTargetShards + "]");
        }
        // now we verify that the numRoutingShards is valid in the source index
        // note: if the number of shards is 1 in the source index we can just assume it's correct since from 1 we can split into anything
        // this is important to special case here since we use this to validate this in various places in the code but allow to split form
        // 1 to N but we never modify the sourceIndexMetadata to accommodate for that
        int routingNumShards = numSourceShards == 1 ? numTargetShards : sourceIndexMetadata.getRoutingNumShards();
        if (routingNumShards % numTargetShards != 0) {
            throw new IllegalStateException("the number of routing shards ["
                + routingNumShards + "] must be a multiple of the target shards [" + numTargetShards + "]");
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
            throw new IllegalArgumentException("the number of target shards (" + numTargetShards + ") must be greater than the shard id: "
                + shardId);
        }
        if (sourceIndexMetadata.getNumberOfShards() < numTargetShards) {
            throw new IllegalArgumentException("the number of target shards [" + numTargetShards
                +"] must be less that the number of source shards [" + sourceIndexMetadata.getNumberOfShards() + "]");
        }
        int routingFactor = getRoutingFactor(sourceIndexMetadata.getNumberOfShards(), numTargetShards);
        Set<ShardId> shards = new HashSet<>(routingFactor);
        for (int i = shardId * routingFactor; i < routingFactor*shardId + routingFactor; i++) {
            shards.add(new ShardId(sourceIndexMetadata.getIndex(), i));
        }
        return shards;
    }

    /**
     * Returns the routing factor for and shrunk index with the given number of target shards.
     * This factor is used in the hash function in
     * {@link org.elasticsearch.cluster.routing.OperationRouting#generateShardId(IndexMetadata, String, String)} to guarantee consistent
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
                throw new IllegalArgumentException("the number of source shards [" + sourceNumberOfShards + "] must be a " +
                    "factor of ["
                    + targetNumberOfShards + "]");
            }
        } else if (sourceNumberOfShards > targetNumberOfShards) { // shrink
            factor = sourceNumberOfShards / targetNumberOfShards;
            if (factor * targetNumberOfShards != sourceNumberOfShards || factor <= 1) {
                throw new IllegalArgumentException("the number of source shards [" + sourceNumberOfShards + "] must be a " +
                    "multiple of ["
                    + targetNumberOfShards + "]");
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
        int numberIndex = indexName.lastIndexOf("-");
        if (numberIndex == -1) {
            throw new IllegalArgumentException("no - separator found in index name [" + indexName + "]");
        }
        try {
            return Integer.parseInt(indexName.substring(numberIndex + 1, indexName.endsWith(">") ? indexName.length() - 1 :
                indexName.length()));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("unable to parse the index name [" + indexName + "] to extract the counter", e);
        }
    }
}
