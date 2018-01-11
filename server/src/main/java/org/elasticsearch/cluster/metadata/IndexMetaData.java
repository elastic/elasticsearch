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

package org.elasticsearch.cluster.metadata;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNodeFilters;
import org.elasticsearch.cluster.routing.allocation.IndexMetaDataUpdater;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.MetaDataStateFormat;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.IP_VALIDATOR;
import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.AND;
import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.OR;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

public class IndexMetaData implements Diffable<IndexMetaData>, ToXContentFragment {

    /**
     * This class will be removed in v7.0
     */
    @Deprecated
    public interface Custom extends Diffable<Custom>, ToXContent {

        String type();

        Custom fromMap(Map<String, Object> map) throws IOException;

        Custom fromXContent(XContentParser parser) throws IOException;

        /**
         * Reads the {@link org.elasticsearch.cluster.Diff} from StreamInput
         */
        Diff<Custom> readDiffFrom(StreamInput in) throws IOException;

        /**
         * Reads an object of this type from the provided {@linkplain StreamInput}. The receiving instance remains unchanged.
         */
        Custom readFrom(StreamInput in) throws IOException;

        /**
         * Merges from this to another, with this being more important, i.e., if something exists in this and another,
         * this will prevail.
         */
        Custom mergeWith(Custom another);
    }

    public static Map<String, Custom> customPrototypes = new HashMap<>();

    /**
     * Register a custom index meta data factory. Make sure to call it from a static block.
     */
    public static void registerPrototype(String type, Custom proto) {
        customPrototypes.put(type, proto);
    }

    @Nullable
    public static <T extends Custom> T lookupPrototype(String type) {
        //noinspection unchecked
        return (T) customPrototypes.get(type);
    }

    public static <T extends Custom> T lookupPrototypeSafe(String type) {
        //noinspection unchecked
        T proto = (T) customPrototypes.get(type);
        if (proto == null) {
            throw new IllegalArgumentException("No custom metadata prototype registered for type [" + type + "]");
        }
        return proto;
    }

    public static final ClusterBlock INDEX_READ_ONLY_BLOCK = new ClusterBlock(5, "index read-only (api)", false, false, false, RestStatus.FORBIDDEN, EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE));
    public static final ClusterBlock INDEX_READ_BLOCK = new ClusterBlock(7, "index read (api)", false, false, false, RestStatus.FORBIDDEN, EnumSet.of(ClusterBlockLevel.READ));
    public static final ClusterBlock INDEX_WRITE_BLOCK = new ClusterBlock(8, "index write (api)", false, false, false, RestStatus.FORBIDDEN, EnumSet.of(ClusterBlockLevel.WRITE));
    public static final ClusterBlock INDEX_METADATA_BLOCK = new ClusterBlock(9, "index metadata (api)", false, false, false, RestStatus.FORBIDDEN, EnumSet.of(ClusterBlockLevel.METADATA_WRITE, ClusterBlockLevel.METADATA_READ));
    public static final ClusterBlock INDEX_READ_ONLY_ALLOW_DELETE_BLOCK = new ClusterBlock(12, "index read-only / allow delete (api)", false, false, true, RestStatus.FORBIDDEN, EnumSet.of(ClusterBlockLevel.METADATA_WRITE, ClusterBlockLevel.WRITE));

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
         * if a cluster should allow to create more than 1024 shards per index. NOTE: this does not limit the number of shards per cluster.
         * this also prevents creating stuff like a new index with millions of shards by accident which essentially kills the entire cluster
         * with OOM on the spot.*/
        final int maxNumShards = Integer.parseInt(System.getProperty("es.index.max_number_of_shards", "1024"));
        if (maxNumShards < 1) {
            throw new IllegalArgumentException("es.index.max_number_of_shards must be > 0");
        }
        return Setting.intSetting(SETTING_NUMBER_OF_SHARDS, Math.min(5, maxNumShards), 1, maxNumShards,
            Property.IndexScope, Property.Final);
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

    public static final Setting<Integer> INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING =
        Setting.intSetting("index.number_of_routing_shards", INDEX_NUMBER_OF_SHARDS_SETTING, 1, new Setting.Validator<Integer>() {
            @Override
            public void validate(Integer numRoutingShards, Map<Setting<Integer>, Integer> settings) {
                Integer numShards = settings.get(INDEX_NUMBER_OF_SHARDS_SETTING);
                if (numRoutingShards < numShards) {
                    throw new IllegalArgumentException("index.number_of_routing_shards [" + numRoutingShards
                        + "] must be >= index.number_of_shards [" + numShards + "]");
                }
                getRoutingFactor(numShards, numRoutingShards);
            }

            @Override
            public Iterator<Setting<Integer>> settings() {
                return Collections.singleton(INDEX_NUMBER_OF_SHARDS_SETTING).iterator();
            }
        }, Property.IndexScope);

    public static final String SETTING_AUTO_EXPAND_REPLICAS = "index.auto_expand_replicas";
    public static final Setting<AutoExpandReplicas> INDEX_AUTO_EXPAND_REPLICAS_SETTING = AutoExpandReplicas.SETTING;
    public static final String SETTING_READ_ONLY = "index.blocks.read_only";
    public static final Setting<Boolean> INDEX_READ_ONLY_SETTING =
        Setting.boolSetting(SETTING_READ_ONLY, false, Property.Dynamic, Property.IndexScope);

    public static final String SETTING_BLOCKS_READ = "index.blocks.read";
    public static final Setting<Boolean> INDEX_BLOCKS_READ_SETTING =
        Setting.boolSetting(SETTING_BLOCKS_READ, false, Property.Dynamic, Property.IndexScope);

    public static final String SETTING_BLOCKS_WRITE = "index.blocks.write";
    public static final Setting<Boolean> INDEX_BLOCKS_WRITE_SETTING =
        Setting.boolSetting(SETTING_BLOCKS_WRITE, false, Property.Dynamic, Property.IndexScope);

    public static final String SETTING_BLOCKS_METADATA = "index.blocks.metadata";
    public static final Setting<Boolean> INDEX_BLOCKS_METADATA_SETTING =
        Setting.boolSetting(SETTING_BLOCKS_METADATA, false, Property.Dynamic, Property.IndexScope);

    public static final String SETTING_READ_ONLY_ALLOW_DELETE = "index.blocks.read_only_allow_delete";
    public static final Setting<Boolean> INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING =
        Setting.boolSetting(SETTING_READ_ONLY_ALLOW_DELETE, false, Property.Dynamic, Property.IndexScope);

    public static final String SETTING_VERSION_CREATED = "index.version.created";
    public static final String SETTING_VERSION_CREATED_STRING = "index.version.created_string";
    public static final String SETTING_VERSION_UPGRADED = "index.version.upgraded";
    public static final String SETTING_VERSION_UPGRADED_STRING = "index.version.upgraded_string";
    public static final String SETTING_CREATION_DATE = "index.creation_date";
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
    public static final String SETTING_DATA_PATH = "index.data_path";
    public static final Setting<String> INDEX_DATA_PATH_SETTING =
        new Setting<>(SETTING_DATA_PATH, "", Function.identity(), Property.IndexScope);
    public static final String INDEX_UUID_NA_VALUE = "_na_";

    public static final String INDEX_ROUTING_REQUIRE_GROUP_PREFIX = "index.routing.allocation.require";
    public static final String INDEX_ROUTING_INCLUDE_GROUP_PREFIX = "index.routing.allocation.include";
    public static final String INDEX_ROUTING_EXCLUDE_GROUP_PREFIX = "index.routing.allocation.exclude";
    public static final Setting.AffixSetting<String> INDEX_ROUTING_REQUIRE_GROUP_SETTING =
        Setting.prefixKeySetting(INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".", (key) ->
            Setting.simpleString(key, (value, map) -> IP_VALIDATOR.accept(key, value), Property.Dynamic, Property.IndexScope));
    public static final Setting.AffixSetting<String> INDEX_ROUTING_INCLUDE_GROUP_SETTING =
        Setting.prefixKeySetting(INDEX_ROUTING_INCLUDE_GROUP_PREFIX + ".", (key) ->
            Setting.simpleString(key, (value, map) -> IP_VALIDATOR.accept(key, value), Property.Dynamic, Property.IndexScope));
    public static final Setting.AffixSetting<String> INDEX_ROUTING_EXCLUDE_GROUP_SETTING =
        Setting.prefixKeySetting(INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + ".", (key) ->
            Setting.simpleString(key, (value, map) -> IP_VALIDATOR.accept(key, value), Property.Dynamic, Property.IndexScope));
    public static final Setting.AffixSetting<String> INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING =
        Setting.prefixKeySetting("index.routing.allocation.initial_recovery.", key -> Setting.simpleString(key));
        // this is only setable internally not a registered setting!!

    /**
     * The number of active shard copies to check for before proceeding with a write operation.
     */
    public static final Setting<ActiveShardCount> SETTING_WAIT_FOR_ACTIVE_SHARDS =
        new Setting<>("index.write.wait_for_active_shards",
                      "1",
                      ActiveShardCount::parseString,
                      Setting.Property.Dynamic,
                      Setting.Property.IndexScope);

    /**
     * an internal index format description, allowing us to find out if this index is upgraded or needs upgrading
     */
    private static final String INDEX_FORMAT = "index.format";
    public static final Setting<Integer> INDEX_FORMAT_SETTING =
            Setting.intSetting(INDEX_FORMAT, 0, Setting.Property.IndexScope, Setting.Property.Final);

    public static final String KEY_IN_SYNC_ALLOCATIONS = "in_sync_allocations";
    static final String KEY_VERSION = "version";
    static final String KEY_ROUTING_NUM_SHARDS = "routing_num_shards";
    static final String KEY_SETTINGS = "settings";
    static final String KEY_STATE = "state";
    static final String KEY_MAPPINGS = "mappings";
    static final String KEY_ALIASES = "aliases";
    public static final String KEY_PRIMARY_TERMS = "primary_terms";

    public static final String INDEX_STATE_FILE_PREFIX = "state-";
    private final int routingNumShards;
    private final int routingFactor;
    private final int routingPartitionSize;

    private final int numberOfShards;
    private final int numberOfReplicas;

    private final Index index;
    private final long version;
    private final long[] primaryTerms;

    private final State state;

    private final ImmutableOpenMap<String, AliasMetaData> aliases;

    private final Settings settings;

    private final ImmutableOpenMap<String, MappingMetaData> mappings;

    private final ImmutableOpenMap<String, Custom> customs;

    private final ImmutableOpenIntMap<Set<String>> inSyncAllocationIds;

    private final transient int totalNumberOfShards;

    private final DiscoveryNodeFilters requireFilters;
    private final DiscoveryNodeFilters includeFilters;
    private final DiscoveryNodeFilters excludeFilters;
    private final DiscoveryNodeFilters initialRecoveryFilters;

    private final Version indexCreatedVersion;
    private final Version indexUpgradedVersion;

    private final ActiveShardCount waitForActiveShards;

    private IndexMetaData(Index index, long version, long[] primaryTerms, State state, int numberOfShards, int numberOfReplicas, Settings settings,
                          ImmutableOpenMap<String, MappingMetaData> mappings, ImmutableOpenMap<String, AliasMetaData> aliases,
                          ImmutableOpenMap<String, Custom> customs, ImmutableOpenIntMap<Set<String>> inSyncAllocationIds,
                          DiscoveryNodeFilters requireFilters, DiscoveryNodeFilters initialRecoveryFilters, DiscoveryNodeFilters includeFilters, DiscoveryNodeFilters excludeFilters,
                          Version indexCreatedVersion, Version indexUpgradedVersion,
                          int routingNumShards, int routingPartitionSize, ActiveShardCount waitForActiveShards) {

        this.index = index;
        this.version = version;
        this.primaryTerms = primaryTerms;
        assert primaryTerms.length == numberOfShards;
        this.state = state;
        this.numberOfShards = numberOfShards;
        this.numberOfReplicas = numberOfReplicas;
        this.totalNumberOfShards = numberOfShards * (numberOfReplicas + 1);
        this.settings = settings;
        this.mappings = mappings;
        this.customs = customs;
        this.aliases = aliases;
        this.inSyncAllocationIds = inSyncAllocationIds;
        this.requireFilters = requireFilters;
        this.includeFilters = includeFilters;
        this.excludeFilters = excludeFilters;
        this.initialRecoveryFilters = initialRecoveryFilters;
        this.indexCreatedVersion = indexCreatedVersion;
        this.indexUpgradedVersion = indexUpgradedVersion;
        this.routingNumShards = routingNumShards;
        this.routingFactor = routingNumShards / numberOfShards;
        this.routingPartitionSize = routingPartitionSize;
        this.waitForActiveShards = waitForActiveShards;
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


    /**
     * The term of the current selected primary. This is a non-negative number incremented when
     * a primary shard is assigned after a full cluster restart or a replica shard is promoted to a primary.
     *
     * Note: since we increment the term every time a shard is assigned, the term for any operational shard (i.e., a shard
     * that can be indexed into) is larger than 0. See {@link IndexMetaDataUpdater#applyChanges}.
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

    /**
     * Return the {@link Version} on which this index has been upgraded. This
     * information is typically useful for backward compatibility.
     */
    public Version getUpgradedVersion() {
        return indexUpgradedVersion;
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

    public ImmutableOpenMap<String, AliasMetaData> getAliases() {
        return this.aliases;
    }

    public ImmutableOpenMap<String, MappingMetaData> getMappings() {
        return mappings;
    }

    @Nullable
    public MappingMetaData mapping(String mappingType) {
        return mappings.get(mappingType);
    }

    // we keep the shrink settings for BWC - this can be removed in 8.0
    // we can't remove in 7 since this setting might be baked into an index coming in via a full cluster restart from 6.0
    public static final String INDEX_SHRINK_SOURCE_UUID_KEY = "index.shrink.source.uuid";
    public static final String INDEX_SHRINK_SOURCE_NAME_KEY = "index.shrink.source.name";
    public static final String INDEX_RESIZE_SOURCE_UUID_KEY = "index.resize.source.uuid";
    public static final String INDEX_RESIZE_SOURCE_NAME_KEY = "index.resize.source.name";
    public static final Setting<String> INDEX_SHRINK_SOURCE_UUID = Setting.simpleString(INDEX_SHRINK_SOURCE_UUID_KEY);
    public static final Setting<String> INDEX_SHRINK_SOURCE_NAME = Setting.simpleString(INDEX_SHRINK_SOURCE_NAME_KEY);
    public static final Setting<String> INDEX_RESIZE_SOURCE_UUID = Setting.simpleString(INDEX_RESIZE_SOURCE_UUID_KEY,
        INDEX_SHRINK_SOURCE_UUID);
    public static final Setting<String> INDEX_RESIZE_SOURCE_NAME = Setting.simpleString(INDEX_RESIZE_SOURCE_NAME_KEY,
        INDEX_SHRINK_SOURCE_NAME);

    public Index getResizeSourceIndex() {
        return INDEX_RESIZE_SOURCE_UUID.exists(settings) || INDEX_SHRINK_SOURCE_UUID.exists(settings)
            ? new Index(INDEX_RESIZE_SOURCE_NAME.get(settings), INDEX_RESIZE_SOURCE_UUID.get(settings)) : null;
    }

    /**
     * Sometimes, the default mapping exists and an actual mapping is not created yet (introduced),
     * in this case, we want to return the default mapping in case it has some default mapping definitions.
     * <p>
     * Note, once the mapping type is introduced, the default mapping is applied on the actual typed MappingMetaData,
     * setting its routing, timestamp, and so on if needed.
     */
    @Nullable
    public MappingMetaData mappingOrDefault(String mappingType) {
        MappingMetaData mapping = mappings.get(mappingType);
        if (mapping != null) {
            return mapping;
        }
        return mappings.get(MapperService.DEFAULT_MAPPING);
    }

    public ImmutableOpenMap<String, Custom> getCustoms() {
        return this.customs;
    }

    @SuppressWarnings("unchecked")
    public <T extends Custom> T custom(String type) {
        return (T) customs.get(type);
    }

    public ImmutableOpenIntMap<Set<String>> getInSyncAllocationIds() {
        return inSyncAllocationIds;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexMetaData that = (IndexMetaData) o;

        if (version != that.version) {
            return false;
        }

        if (!aliases.equals(that.aliases)) {
            return false;
        }
        if (!index.equals(that.index)) {
            return false;
        }
        if (!mappings.equals(that.mappings)) {
            return false;
        }
        if (!settings.equals(that.settings)) {
            return false;
        }
        if (state != that.state) {
            return false;
        }
        if (!customs.equals(that.customs)) {
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
        if (!inSyncAllocationIds.equals(that.inSyncAllocationIds)) {
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
        result = 31 * result + customs.hashCode();
        result = 31 * result + Long.hashCode(routingFactor);
        result = 31 * result + Long.hashCode(routingNumShards);
        result = 31 * result + Arrays.hashCode(primaryTerms);
        result = 31 * result + inSyncAllocationIds.hashCode();
        return result;
    }


    @Override
    public Diff<IndexMetaData> diff(IndexMetaData previousState) {
        return new IndexMetaDataDiff(previousState, this);
    }

    public static Diff<IndexMetaData> readDiffFrom(StreamInput in) throws IOException {
        return new IndexMetaDataDiff(in);
    }

    public static IndexMetaData fromXContent(XContentParser parser) throws IOException {
        return Builder.fromXContent(parser);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Builder.toXContent(this, builder, params);
        return builder;
    }

    private static class IndexMetaDataDiff implements Diff<IndexMetaData> {

        private final String index;
        private final int routingNumShards;
        private final long version;
        private final long[] primaryTerms;
        private final State state;
        private final Settings settings;
        private final Diff<ImmutableOpenMap<String, MappingMetaData>> mappings;
        private final Diff<ImmutableOpenMap<String, AliasMetaData>> aliases;
        private final Diff<ImmutableOpenMap<String, Custom>> customs;
        private final Diff<ImmutableOpenIntMap<Set<String>>> inSyncAllocationIds;

        IndexMetaDataDiff(IndexMetaData before, IndexMetaData after) {
            index = after.index.getName();
            version = after.version;
            routingNumShards = after.routingNumShards;
            state = after.state;
            settings = after.settings;
            primaryTerms = after.primaryTerms;
            mappings = DiffableUtils.diff(before.mappings, after.mappings, DiffableUtils.getStringKeySerializer());
            aliases = DiffableUtils.diff(before.aliases, after.aliases, DiffableUtils.getStringKeySerializer());
            customs = DiffableUtils.diff(before.customs, after.customs, DiffableUtils.getStringKeySerializer());
            inSyncAllocationIds = DiffableUtils.diff(before.inSyncAllocationIds, after.inSyncAllocationIds,
                DiffableUtils.getVIntKeySerializer(), DiffableUtils.StringSetValueSerializer.getInstance());
        }

        IndexMetaDataDiff(StreamInput in) throws IOException {
            index = in.readString();
            routingNumShards = in.readInt();
            version = in.readLong();
            state = State.fromId(in.readByte());
            settings = Settings.readSettingsFromStream(in);
            primaryTerms = in.readVLongArray();
            mappings = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), MappingMetaData::new,
                MappingMetaData::readDiffFrom);
            aliases = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), AliasMetaData::new,
                AliasMetaData::readDiffFrom);
            customs = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(),
                new DiffableUtils.DiffableValueSerializer<String, Custom>() {
                    @Override
                    public Custom read(StreamInput in, String key) throws IOException {
                        return lookupPrototypeSafe(key).readFrom(in);
                    }

                    @SuppressWarnings("unchecked")
                    @Override
                    public Diff<Custom> readDiff(StreamInput in, String key) throws IOException {
                        return lookupPrototypeSafe(key).readDiffFrom(in);
                    }
                });
            inSyncAllocationIds = DiffableUtils.readImmutableOpenIntMapDiff(in, DiffableUtils.getVIntKeySerializer(),
                DiffableUtils.StringSetValueSerializer.getInstance());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeInt(routingNumShards);
            out.writeLong(version);
            out.writeByte(state.id);
            Settings.writeSettingsToStream(settings, out);
            out.writeVLongArray(primaryTerms);
            mappings.writeTo(out);
            aliases.writeTo(out);
            customs.writeTo(out);
            inSyncAllocationIds.writeTo(out);
        }

        @Override
        public IndexMetaData apply(IndexMetaData part) {
            Builder builder = builder(index);
            builder.version(version);
            builder.setRoutingNumShards(routingNumShards);
            builder.state(state);
            builder.settings(settings);
            builder.primaryTerms(primaryTerms);
            builder.mappings.putAll(mappings.apply(part.mappings));
            builder.aliases.putAll(aliases.apply(part.aliases));
            builder.customs.putAll(customs.apply(part.customs));
            builder.inSyncAllocationIds.putAll(inSyncAllocationIds.apply(part.inSyncAllocationIds));
            return builder.build();
        }
    }

    public static IndexMetaData readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder(in.readString());
        builder.version(in.readLong());
        builder.setRoutingNumShards(in.readInt());
        builder.state(State.fromId(in.readByte()));
        builder.settings(readSettingsFromStream(in));
        builder.primaryTerms(in.readVLongArray());
        int mappingsSize = in.readVInt();
        for (int i = 0; i < mappingsSize; i++) {
            MappingMetaData mappingMd = new MappingMetaData(in);
            builder.putMapping(mappingMd);
        }
        int aliasesSize = in.readVInt();
        for (int i = 0; i < aliasesSize; i++) {
            AliasMetaData aliasMd = new AliasMetaData(in);
            builder.putAlias(aliasMd);
        }
        int customSize = in.readVInt();
        for (int i = 0; i < customSize; i++) {
            String type = in.readString();
            Custom customIndexMetaData = lookupPrototypeSafe(type).readFrom(in);
            builder.putCustom(type, customIndexMetaData);
        }
        int inSyncAllocationIdsSize = in.readVInt();
        for (int i = 0; i < inSyncAllocationIdsSize; i++) {
            int key = in.readVInt();
            Set<String> allocationIds = DiffableUtils.StringSetValueSerializer.getInstance().read(in, key);
            builder.putInSyncAllocationIds(key, allocationIds);
        }
        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index.getName()); // uuid will come as part of settings
        out.writeLong(version);
        out.writeInt(routingNumShards);
        out.writeByte(state.id());
        writeSettingsToStream(settings, out);
        out.writeVLongArray(primaryTerms);
        out.writeVInt(mappings.size());
        for (ObjectCursor<MappingMetaData> cursor : mappings.values()) {
            cursor.value.writeTo(out);
        }
        out.writeVInt(aliases.size());
        for (ObjectCursor<AliasMetaData> cursor : aliases.values()) {
            cursor.value.writeTo(out);
        }
        out.writeVInt(customs.size());
        for (ObjectObjectCursor<String, Custom> cursor : customs) {
            out.writeString(cursor.key);
            cursor.value.writeTo(out);
        }
        out.writeVInt(inSyncAllocationIds.size());
        for (IntObjectCursor<Set<String>> cursor : inSyncAllocationIds) {
            out.writeVInt(cursor.key);
            DiffableUtils.StringSetValueSerializer.getInstance().write(cursor.value, out);
        }
    }

    public static Builder builder(String index) {
        return new Builder(index);
    }

    public static Builder builder(IndexMetaData indexMetaData) {
        return new Builder(indexMetaData);
    }

    public static class Builder {

        private String index;
        private State state = State.OPEN;
        private long version = 1;
        private long[] primaryTerms = null;
        private Settings settings = Settings.Builder.EMPTY_SETTINGS;
        private final ImmutableOpenMap.Builder<String, MappingMetaData> mappings;
        private final ImmutableOpenMap.Builder<String, AliasMetaData> aliases;
        private final ImmutableOpenMap.Builder<String, Custom> customs;
        private final ImmutableOpenIntMap.Builder<Set<String>> inSyncAllocationIds;
        private Integer routingNumShards;

        public Builder(String index) {
            this.index = index;
            this.mappings = ImmutableOpenMap.builder();
            this.aliases = ImmutableOpenMap.builder();
            this.customs = ImmutableOpenMap.builder();
            this.inSyncAllocationIds = ImmutableOpenIntMap.builder();
        }

        public Builder(IndexMetaData indexMetaData) {
            this.index = indexMetaData.getIndex().getName();
            this.state = indexMetaData.state;
            this.version = indexMetaData.version;
            this.settings = indexMetaData.getSettings();
            this.primaryTerms = indexMetaData.primaryTerms.clone();
            this.mappings = ImmutableOpenMap.builder(indexMetaData.mappings);
            this.aliases = ImmutableOpenMap.builder(indexMetaData.aliases);
            this.customs = ImmutableOpenMap.builder(indexMetaData.customs);
            this.routingNumShards = indexMetaData.routingNumShards;
            this.inSyncAllocationIds = ImmutableOpenIntMap.builder(indexMetaData.inSyncAllocationIds);
        }

        public String index() {
            return index;
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

        /**
         * Returns the number of replicas.
         *
         * @return the provided value or -1 if it has not been set.
         */
        public int numberOfReplicas() {
            return settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, -1);
        }

        public Builder routingPartitionSize(int routingPartitionSize) {
            settings = Settings.builder().put(settings).put(SETTING_ROUTING_PARTITION_SIZE, routingPartitionSize).build();
            return this;
        }

        /**
         * Returns the routing partition size.
         *
         * @return the provided value or -1 if it has not been set.
         */
        public int routingPartitionSize() {
            return settings.getAsInt(SETTING_ROUTING_PARTITION_SIZE, -1);
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

        public MappingMetaData mapping(String type) {
            return mappings.get(type);
        }

        public Builder putMapping(String type, String source) throws IOException {
            putMapping(new MappingMetaData(type, XContentHelper.convertToMap(XContentFactory.xContent(source), source, true)));
            return this;
        }

        public Builder putMapping(MappingMetaData mappingMd) {
            mappings.put(mappingMd.type(), mappingMd);
            return this;
        }

        public Builder state(State state) {
            this.state = state;
            return this;
        }

        public Builder putAlias(AliasMetaData aliasMetaData) {
            aliases.put(aliasMetaData.alias(), aliasMetaData);
            return this;
        }

        public Builder putAlias(AliasMetaData.Builder aliasMetaData) {
            aliases.put(aliasMetaData.alias(), aliasMetaData.build());
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

        public Builder putCustom(String type, Custom customIndexMetaData) {
            this.customs.put(type, customIndexMetaData);
            return this;
        }

        public Set<String> getInSyncAllocationIds(int shardId) {
            return inSyncAllocationIds.get(shardId);
        }

        public Builder putInSyncAllocationIds(int shardId, Set<String> allocationIds) {
            inSyncAllocationIds.put(shardId, new HashSet<>(allocationIds));
            return this;
        }

        public long version() {
            return this.version;
        }

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        /**
         * returns the primary term for the given shard.
         * See {@link IndexMetaData#primaryTerm(int)} for more information.
         */
        public long primaryTerm(int shardId) {
            if (primaryTerms == null) {
                initializePrimaryTerms();
            }
            return this.primaryTerms[shardId];
        }

        /**
         * sets the primary term for the given shard.
         * See {@link IndexMetaData#primaryTerm(int)} for more information.
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
        }


        public IndexMetaData build() {
            ImmutableOpenMap.Builder<String, AliasMetaData> tmpAliases = aliases;
            Settings tmpSettings = settings;

            // update default mapping on the MappingMetaData
            if (mappings.containsKey(MapperService.DEFAULT_MAPPING)) {
                MappingMetaData defaultMapping = mappings.get(MapperService.DEFAULT_MAPPING);
                for (ObjectCursor<MappingMetaData> cursor : mappings.values()) {
                    cursor.value.updateDefaultMapping(defaultMapping);
                }
            }

            Integer maybeNumberOfShards = settings.getAsInt(SETTING_NUMBER_OF_SHARDS, null);
            if (maybeNumberOfShards == null) {
                throw new IllegalArgumentException("must specify numberOfShards for index [" + index + "]");
            }
            int numberOfShards = maybeNumberOfShards;
            if (numberOfShards <= 0) {
                throw new IllegalArgumentException("must specify positive number of shards for index [" + index + "]");
            }

            Integer maybeNumberOfReplicas = settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, null);
            if (maybeNumberOfReplicas == null) {
                throw new IllegalArgumentException("must specify numberOfReplicas for index [" + index + "]");
            }
            int numberOfReplicas = maybeNumberOfReplicas;
            if (numberOfReplicas < 0) {
                throw new IllegalArgumentException("must specify non-negative number of shards for index [" + index + "]");
            }

            int routingPartitionSize = INDEX_ROUTING_PARTITION_SIZE_SETTING.get(settings);
            if (routingPartitionSize != 1 && routingPartitionSize >= getRoutingNumShards()) {
                throw new IllegalArgumentException("routing partition size [" + routingPartitionSize + "] should be a positive number"
                        + " less than the number of shards [" + getRoutingNumShards() + "] for [" + index + "]");
            }
            // fill missing slots in inSyncAllocationIds with empty set if needed and make all entries immutable
            ImmutableOpenIntMap.Builder<Set<String>> filledInSyncAllocationIds = ImmutableOpenIntMap.builder();
            for (int i = 0; i < numberOfShards; i++) {
                if (inSyncAllocationIds.containsKey(i)) {
                    filledInSyncAllocationIds.put(i, Collections.unmodifiableSet(new HashSet<>(inSyncAllocationIds.get(i))));
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
            Version indexCreatedVersion = Version.indexCreated(settings);
            Version indexUpgradedVersion = settings.getAsVersion(IndexMetaData.SETTING_VERSION_UPGRADED, indexCreatedVersion);

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

            return new IndexMetaData(new Index(index, uuid), version, primaryTerms, state, numberOfShards, numberOfReplicas, tmpSettings, mappings.build(),
                tmpAliases.build(), customs.build(), filledInSyncAllocationIds.build(), requireFilters, initialRecoveryFilters, includeFilters, excludeFilters,
                indexCreatedVersion, indexUpgradedVersion, getRoutingNumShards(), routingPartitionSize, waitForActiveShards);
        }

        public static void toXContent(IndexMetaData indexMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject(indexMetaData.getIndex().getName());

            builder.field(KEY_VERSION, indexMetaData.getVersion());
            builder.field(KEY_ROUTING_NUM_SHARDS, indexMetaData.getRoutingNumShards());
            builder.field(KEY_STATE, indexMetaData.getState().toString().toLowerCase(Locale.ENGLISH));

            boolean binary = params.paramAsBoolean("binary", false);

            builder.startObject(KEY_SETTINGS);
            indexMetaData.getSettings().toXContent(builder, new MapParams(Collections.singletonMap("flat_settings", "true")));
            builder.endObject();

            builder.startArray(KEY_MAPPINGS);
            for (ObjectObjectCursor<String, MappingMetaData> cursor : indexMetaData.getMappings()) {
                if (binary) {
                    builder.value(cursor.value.source().compressed());
                } else {
                    builder.map(XContentHelper.convertToMap(new BytesArray(cursor.value.source().uncompressed()), true).v2());
                }
            }
            builder.endArray();

            for (ObjectObjectCursor<String, Custom> cursor : indexMetaData.getCustoms()) {
                builder.startObject(cursor.key);
                cursor.value.toXContent(builder, params);
                builder.endObject();
            }

            builder.startObject(KEY_ALIASES);
            for (ObjectCursor<AliasMetaData> cursor : indexMetaData.getAliases().values()) {
                AliasMetaData.Builder.toXContent(cursor.value, builder, params);
            }
            builder.endObject();

            builder.startArray(KEY_PRIMARY_TERMS);
            for (int i = 0; i < indexMetaData.getNumberOfShards(); i++) {
                builder.value(indexMetaData.primaryTerm(i));
            }
            builder.endArray();

            builder.startObject(KEY_IN_SYNC_ALLOCATIONS);
            for (IntObjectCursor<Set<String>> cursor : indexMetaData.inSyncAllocationIds) {
                builder.startArray(String.valueOf(cursor.key));
                for (String allocationId : cursor.value) {
                    builder.value(allocationId);
                }
                builder.endArray();
            }
            builder.endObject();

            builder.endObject();
        }

        public static IndexMetaData fromXContent(XContentParser parser) throws IOException {
            if (parser.currentToken() == null) { // fresh parser? move to the first token
                parser.nextToken();
            }
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {  // on a start object move to next token
                parser.nextToken();
            }
            if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                throw new IllegalArgumentException("expected field name but got a " + parser.currentToken());
            }
            Builder builder = new Builder(parser.currentName());

            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("expected object but got a " + token);
            }
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
                                Map<String, Object> mappingSource = MapBuilder.<String, Object>newMapBuilder().put(mappingType, parser.mapOrdered()).map();
                                builder.putMapping(new MappingMetaData(mappingType, mappingSource));
                            } else {
                                throw new IllegalArgumentException("Unexpected token: " + token);
                            }
                        }
                    } else if (KEY_ALIASES.equals(currentFieldName)) {
                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            builder.putAlias(AliasMetaData.Builder.fromXContent(parser));
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
                    } else if ("warmers".equals(currentFieldName)) {
                        // TODO: do this in 6.0:
                        // throw new IllegalArgumentException("Warmers are not supported anymore - are you upgrading from 1.x?");
                        // ignore: warmers have been removed in 5.0 and are
                        // simply ignored when upgrading from 2.x
                        assert Version.CURRENT.major <= 5;
                        parser.skipChildren();
                    } else {
                        // check if its a custom index metadata
                        Custom proto = lookupPrototype(currentFieldName);
                        if (proto == null) {
                            //TODO warn
                            parser.skipChildren();
                        } else {
                            Custom custom = proto.fromXContent(parser);
                            builder.putCustom(custom.type(), custom);
                        }
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if (KEY_MAPPINGS.equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                                builder.putMapping(new MappingMetaData(new CompressedXContent(parser.binaryValue())));
                            } else {
                                Map<String, Object> mapping = parser.mapOrdered();
                                if (mapping.size() == 1) {
                                    String mappingType = mapping.keySet().iterator().next();
                                    builder.putMapping(new MappingMetaData(mappingType, mapping));
                                }
                            }
                        }
                    } else if (KEY_PRIMARY_TERMS.equals(currentFieldName)) {
                        LongArrayList list = new LongArrayList();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_NUMBER) {
                                list.add(parser.longValue());
                            } else {
                                throw new IllegalStateException("found a non-numeric value under [" + KEY_PRIMARY_TERMS + "]");
                            }
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
                    } else if (KEY_ROUTING_NUM_SHARDS.equals(currentFieldName)) {
                        builder.setRoutingNumShards(parser.intValue());
                    } else {
                        throw new IllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new IllegalArgumentException("Unexpected token " + token);
                }
            }
            return builder.build();
        }
    }

    /**
     * Adds human readable version and creation date settings.
     * This method is used to display the settings in a human readable format in REST API
     */
    public static Settings addHumanReadableSettings(Settings settings) {
        Settings.Builder builder = Settings.builder().put(settings);
        Version version = settings.getAsVersion(SETTING_VERSION_CREATED, null);
        if (version != null) {
            builder.put(SETTING_VERSION_CREATED_STRING, version.toString());
        }
        Version versionUpgraded = settings.getAsVersion(SETTING_VERSION_UPGRADED, null);
        if (versionUpgraded != null) {
            builder.put(SETTING_VERSION_UPGRADED_STRING, versionUpgraded.toString());
        }
        Long creationDate = settings.getAsLong(SETTING_CREATION_DATE, null);
        if (creationDate != null) {
            DateTime creationDateTime = new DateTime(creationDate, DateTimeZone.UTC);
            builder.put(SETTING_CREATION_DATE_STRING, creationDateTime.toString());
        }
        return builder.build();
    }

    private static final ToXContent.Params FORMAT_PARAMS = new MapParams(Collections.singletonMap("binary", "true"));

    /**
     * State format for {@link IndexMetaData} to write to and load from disk
     */
    public static final MetaDataStateFormat<IndexMetaData> FORMAT = new MetaDataStateFormat<IndexMetaData>(XContentType.SMILE, INDEX_STATE_FILE_PREFIX) {

        @Override
        public void toXContent(XContentBuilder builder, IndexMetaData state) throws IOException {
            Builder.toXContent(state, builder, FORMAT_PARAMS);
        }

        @Override
        public IndexMetaData fromXContent(XContentParser parser) throws IOException {
            return Builder.fromXContent(parser);
        }
    };

    /**
     * Returns the number of shards that should be used for routing. This basically defines the hash space we use in
     * {@link org.elasticsearch.cluster.routing.OperationRouting#generateShardId(IndexMetaData, String, String)} to route documents
     * to shards based on their ID or their specific routing value. The default value is {@link #getNumberOfShards()}. This value only
     * changes if and index is shrunk.
     */
    public int getRoutingNumShards() {
        return routingNumShards;
    }

    /**
     * Returns the routing factor for this index. The default is <tt>1</tt>.
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
    public static ShardId selectSplitShard(int shardId, IndexMetaData sourceIndexMetadata, int numTargetShards) {
        if (shardId >= numTargetShards) {
            throw new IllegalArgumentException("the number of target shards (" + numTargetShards + ") must be greater than the shard id: "
                + shardId);
        }
        int numSourceShards = sourceIndexMetadata.getNumberOfShards();
        if (numSourceShards > numTargetShards) {
            throw new IllegalArgumentException("the number of source shards [" + numSourceShards
                 + "] must be less that the number of target shards [" + numTargetShards + "]");
        }
        int routingFactor = getRoutingFactor(numSourceShards, numTargetShards);
        // now we verify that the numRoutingShards is valid in the source index
        int routingNumShards = sourceIndexMetadata.getRoutingNumShards();
        if (routingNumShards % numTargetShards != 0) {
            throw new IllegalStateException("the number of routing shards ["
                + routingNumShards + "] must be a multiple of the target shards [" + numTargetShards + "]");
        }
        // this is just an additional assertion that ensures we are a factor of the routing num shards.
        assert getRoutingFactor(numTargetShards, sourceIndexMetadata.getRoutingNumShards()) >= 0;
        return new ShardId(sourceIndexMetadata.getIndex(), shardId/routingFactor);
    }

    /**
     * Selects the source shards for a local shard recovery. This might either be a split or a shrink operation.
     * @param shardId the target shard ID to select the source shards for
     * @param sourceIndexMetadata the source metadata
     * @param numTargetShards the number of target shards
     */
    public static Set<ShardId> selectRecoverFromShards(int shardId, IndexMetaData sourceIndexMetadata, int numTargetShards) {
        if (sourceIndexMetadata.getNumberOfShards() > numTargetShards) {
            return selectShrinkShards(shardId, sourceIndexMetadata, numTargetShards);
        } else if (sourceIndexMetadata.getNumberOfShards() < numTargetShards) {
            return Collections.singleton(selectSplitShard(shardId, sourceIndexMetadata, numTargetShards));
        }
        throw new IllegalArgumentException("can't select recover from shards if both indices have the same number of shards");
    }

    /**
     * Returns the source shard ids to shrink into the given shard id.
     * @param shardId the id of the target shard to shrink to
     * @param sourceIndexMetadata the source index metadata
     * @param numTargetShards the total number of shards in the target index
     * @return a set of shard IDs to shrink into the given shard ID.
     */
    public static Set<ShardId> selectShrinkShards(int shardId, IndexMetaData sourceIndexMetadata, int numTargetShards) {
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
     * {@link org.elasticsearch.cluster.routing.OperationRouting#generateShardId(IndexMetaData, String, String)} to guarantee consistent
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
                throw new IllegalArgumentException("the number of source shards [" + sourceNumberOfShards + "] must be a must be a " +
                    "factor of ["
                    + targetNumberOfShards + "]");
            }
        } else if (sourceNumberOfShards > targetNumberOfShards) { // shrink
            factor = sourceNumberOfShards / targetNumberOfShards;
            if (factor * targetNumberOfShards != sourceNumberOfShards || factor <= 1) {
                throw new IllegalArgumentException("the number of source shards [" + sourceNumberOfShards + "] must be a must be a " +
                    "multiple of ["
                    + targetNumberOfShards + "]");
            }
        } else {
            factor = 1;
        }
        return factor;
    }
}
