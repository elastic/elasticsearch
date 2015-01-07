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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNodeFilters;
import org.elasticsearch.cluster.routing.HashFunction;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.SettingsLoader;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder.FieldCaseConversion;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.AND;
import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.OR;
import static org.elasticsearch.common.settings.ImmutableSettings.*;

/**
 *
 */
public class IndexMetaData extends NamedCompositeClusterStatePart<IndexClusterStatePart> implements NamedClusterStatePart {

    public static String TYPE = "index_metadata";

    public static Factory FACTORY = new Factory();

    public static final String SETTINGS_TYPE = "settings";

    public static final ClusterStateSettingsPart.Factory SETTINGS_FACTORY = new ClusterStateSettingsPart.Factory(SETTINGS_TYPE);

    public static final String MAPPINGS_TYPE = "mappings";

    public static final MapClusterStatePart.Factory<MappingMetaData> MAPPINGS_FACTORY = new MapClusterStatePart.Factory<>(MAPPINGS_TYPE, MappingMetaData.FACTORY, API_GATEWAY_SNAPSHOT);

    public static final String ALIASES_TYPE = "aliases";

    public static final MapClusterStatePart.Factory<AliasMetaData> ALIASES_FACTORY = new MapClusterStatePart.Factory<>(ALIASES_TYPE, AliasMetaData.FACTORY, API_GATEWAY_SNAPSHOT);

    static {
        FACTORY.registerFactory(SETTINGS_TYPE, SETTINGS_FACTORY);
        FACTORY.registerFactory(MAPPINGS_TYPE, MAPPINGS_FACTORY);
        FACTORY.registerFactory(ALIASES_TYPE, ALIASES_FACTORY);
        // register non plugin custom metadata
        FACTORY.registerFactory(IndexWarmersMetaData.TYPE, IndexWarmersMetaData.FACTORY);
    }

    public static class Factory extends NamedCompositeClusterStatePart.AbstractFactory<IndexClusterStatePart, IndexMetaData> {

        @Override
        public NamedCompositeClusterStatePart.Builder<IndexClusterStatePart, IndexMetaData> builder(String key) {
            return new Builder(key);
        }

        @Override
        public NamedCompositeClusterStatePart.Builder<IndexClusterStatePart, IndexMetaData> builder(IndexMetaData part) {
            return new Builder(part);
        }

        @Override
        public IndexMetaData fromXContent(XContentParser parser, LocalContext context) throws IOException {
            return Builder.fromXContent(parser);
        }

        @Override
        protected void valuesPartWriteTo(IndexMetaData indexMetaData, StreamOutput out) throws IOException {
            out.writeVLong(indexMetaData.version);
            out.writeByte(indexMetaData.state.id());
        }

        @Override
        protected void valuesPartToXContent(IndexMetaData indexMetaData, XContentBuilder builder, Params params) throws IOException {
            builder.field("version", indexMetaData.version);
            builder.field("state", indexMetaData.state.toString().toLowerCase(Locale.ENGLISH));
        }

        @Override
        public void toXContent(IndexMetaData indexMetaData, XContentBuilder builder, Params params) throws IOException {
            // TODO: switch to generic toXContent
            Builder.toXContent(indexMetaData, builder, params);
        }

        @Override
        public String partType() {
            return TYPE;
        }
    }

    public static final ClusterBlock INDEX_READ_ONLY_BLOCK = new ClusterBlock(5, "index read-only (api)", false, false, RestStatus.FORBIDDEN, EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA));
    public static final ClusterBlock INDEX_READ_BLOCK = new ClusterBlock(7, "index read (api)", false, false, RestStatus.FORBIDDEN, EnumSet.of(ClusterBlockLevel.READ));
    public static final ClusterBlock INDEX_WRITE_BLOCK = new ClusterBlock(8, "index write (api)", false, false, RestStatus.FORBIDDEN, EnumSet.of(ClusterBlockLevel.WRITE));
    public static final ClusterBlock INDEX_METADATA_BLOCK = new ClusterBlock(9, "index metadata (api)", false, false, RestStatus.FORBIDDEN, EnumSet.of(ClusterBlockLevel.METADATA));

    public static enum State {
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
            throw new ElasticsearchIllegalStateException("No state match for id [" + id + "]");
        }

        public static State fromString(String state) {
            if ("open".equals(state)) {
                return OPEN;
            } else if ("close".equals(state)) {
                return CLOSE;
            }
            throw new ElasticsearchIllegalStateException("No state match for [" + state + "]");
        }
    }

    public static final String SETTING_NUMBER_OF_SHARDS = "index.number_of_shards";
    public static final String SETTING_NUMBER_OF_REPLICAS = "index.number_of_replicas";
    public static final String SETTING_AUTO_EXPAND_REPLICAS = "index.auto_expand_replicas";
    public static final String SETTING_READ_ONLY = "index.blocks.read_only";
    public static final String SETTING_BLOCKS_READ = "index.blocks.read";
    public static final String SETTING_BLOCKS_WRITE = "index.blocks.write";
    public static final String SETTING_BLOCKS_METADATA = "index.blocks.metadata";
    public static final String SETTING_VERSION_CREATED = "index.version.created";
    public static final String SETTING_CREATION_DATE = "index.creation_date";
    public static final String SETTING_UUID = "index.uuid";
    public static final String SETTING_LEGACY_ROUTING_HASH_FUNCTION = "index.legacy.routing.hash.type";
    public static final String SETTING_LEGACY_ROUTING_USE_TYPE = "index.legacy.routing.use_type";
    public static final String SETTING_DATA_PATH = "index.data_path";
    public static final String INDEX_UUID_NA_VALUE = "_na_";

    // hard-coded hash function as of 2.0
    // older indices will read which hash function to use in their index settings
    private static final HashFunction MURMUR3_HASH_FUNCTION = new Murmur3HashFunction();

    private final String index;
    private final long version;

    private final State state;

    private final ImmutableOpenMap<String, AliasMetaData> aliases;

    private final Settings settings;
    private final ImmutableOpenMap<String, MappingMetaData> mappings;

    private transient final int totalNumberOfShards;

    private final DiscoveryNodeFilters requireFilters;
    private final DiscoveryNodeFilters includeFilters;
    private final DiscoveryNodeFilters excludeFilters;

    private final Version indexCreatedVersion;
    private final HashFunction routingHashFunction;
    private final boolean useTypeForRouting;

    private IndexMetaData(String index, long version, State state, ImmutableOpenMap<String, IndexClusterStatePart> parts) {
        super(parts);
        this.index = index;
        this.version = version;
        this.state = state;
        this.settings = parts.containsKey(SETTINGS_TYPE) ? ((ClusterStateSettingsPart)get(SETTINGS_TYPE)).getSettings() : ImmutableSettings.EMPTY;
        this.totalNumberOfShards = numberOfShards() * (numberOfReplicas() + 1);
        Preconditions.checkArgument(settings.getAsInt(SETTING_NUMBER_OF_SHARDS, null) != null, "must specify numberOfShards for index [" + index + "]");
        Preconditions.checkArgument(settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, null) != null, "must specify numberOfReplicas for index [" + index + "]");
        this.mappings = parts.containsKey(MAPPINGS_TYPE) ? ((MapClusterStatePart<MappingMetaData>)get(MAPPINGS_TYPE)).parts() : ImmutableOpenMap.<String, MappingMetaData>of();
        this.aliases = parts.containsKey(ALIASES_TYPE) ? ((MapClusterStatePart<AliasMetaData>)get(ALIASES_TYPE)).parts() : ImmutableOpenMap.<String, AliasMetaData>of();

        ImmutableMap<String, String> requireMap = settings.getByPrefix("index.routing.allocation.require.").getAsMap();
        if (requireMap.isEmpty()) {
            requireFilters = null;
        } else {
            requireFilters = DiscoveryNodeFilters.buildFromKeyValue(AND, requireMap);
        }
        ImmutableMap<String, String> includeMap = settings.getByPrefix("index.routing.allocation.include.").getAsMap();
        if (includeMap.isEmpty()) {
            includeFilters = null;
        } else {
            includeFilters = DiscoveryNodeFilters.buildFromKeyValue(OR, includeMap);
        }
        ImmutableMap<String, String> excludeMap = settings.getByPrefix("index.routing.allocation.exclude.").getAsMap();
        if (excludeMap.isEmpty()) {
            excludeFilters = null;
        } else {
            excludeFilters = DiscoveryNodeFilters.buildFromKeyValue(OR, excludeMap);
        }
        indexCreatedVersion = Version.indexCreated(settings);
        final Class<? extends HashFunction> hashFunctionClass = settings.getAsClass(SETTING_LEGACY_ROUTING_HASH_FUNCTION, null);
        if (hashFunctionClass == null) {
            routingHashFunction = MURMUR3_HASH_FUNCTION;
        } else {
            try {
                routingHashFunction = hashFunctionClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new ElasticsearchIllegalStateException("Cannot instantiate hash function", e);
            }
        }
        useTypeForRouting = settings.getAsBoolean(SETTING_LEGACY_ROUTING_USE_TYPE, false);
    }

    public String index() {
        return index;
    }

    public String getIndex() {
        return index();
    }

    public String uuid() {
        return settings.get(SETTING_UUID, INDEX_UUID_NA_VALUE);
    }

    public String getUUID() {
        return uuid();
    }

    /**
     * Test whether the current index UUID is the same as the given one. Returns true if either are _na_
     */
    public boolean isSameUUID(String otherUUID) {
        assert otherUUID != null;
        assert uuid() != null;
        if (INDEX_UUID_NA_VALUE.equals(otherUUID) || INDEX_UUID_NA_VALUE.equals(uuid())) {
            return true;
        }
        return otherUUID.equals(getUUID());
    }

    public long version() {
        return this.version;
    }

    public long getVersion() {
        return this.version;
    }

    /**
     * Return the {@link Version} on which this index has been created. This
     * information is typically useful for backward compatibility.
     */
    public Version creationVersion() {
        return indexCreatedVersion;
    }

    public Version getCreationVersion() {
        return creationVersion();
    }

    /**
     * Return the {@link HashFunction} that should be used for routing.
     */
    public HashFunction routingHashFunction() {
        return routingHashFunction;
    }

    public HashFunction getRoutingHashFunction() {
        return routingHashFunction();
    }

    /**
     * Return whether routing should use the _type in addition to the _id in
     * order to decide which shard a document should go to.
     */
    public boolean routingUseType() {
        return useTypeForRouting;
    }

    public boolean getRoutingUseType() {
        return routingUseType();
    }

    public long creationDate() {
        return settings.getAsLong(SETTING_CREATION_DATE, -1l);
    }

    public long getCreationDate() {
        return creationDate();
    }

    public State state() {
        return this.state;
    }

    public State getState() {
        return state();
    }

    public int numberOfShards() {
        return settings.getAsInt(SETTING_NUMBER_OF_SHARDS, -1);
    }

    public int getNumberOfShards() {
        return numberOfShards();
    }

    public int numberOfReplicas() {
        return settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, -1);
    }

    public int getNumberOfReplicas() {
        return numberOfReplicas();
    }

    public int totalNumberOfShards() {
        return totalNumberOfShards;
    }

    public int getTotalNumberOfShards() {
        return totalNumberOfShards();
    }

    public Settings settings() {
        return settings;
    }

    public Settings getSettings() {
        return settings();
    }

    public ImmutableOpenMap<String, AliasMetaData> aliases() {
        return this.aliases;
    }

    public ImmutableOpenMap<String, AliasMetaData> getAliases() {
        return aliases();
    }

    public ImmutableOpenMap<String, MappingMetaData> mappings() {
        return mappings;
    }

    public ImmutableOpenMap<String, MappingMetaData> getMappings() {
        return mappings();
    }

    @Nullable
    public MappingMetaData mapping(String mappingType) {
        return mappings.get(mappingType);
    }

    /**
     * Sometimes, the default mapping exists and an actual mapping is not created yet (introduced),
     * in this case, we want to return the default mapping in case it has some default mapping definitions.
     * <p/>
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

    @Nullable
    public DiscoveryNodeFilters requireFilters() {
        return requireFilters;
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

        if (state != that.state) {
            return false;
        }

        if (!parts.equals(that.parts)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + state.hashCode();
        result = 31 * result + parts.hashCode();
        return result;
    }

    public static Builder builder(String index) {
        return new Builder(index);
    }

    public static Builder builder(IndexMetaData indexMetaData) {
        return new Builder(indexMetaData);
    }

    public static class Builder extends NamedCompositeClusterStatePart.Builder<IndexClusterStatePart, IndexMetaData> {

        private String index;
        private State state = State.OPEN;
        private long version = 1;
        private Settings settings = ImmutableSettings.Builder.EMPTY_SETTINGS;
        private final ImmutableOpenMap.Builder<String, MappingMetaData> mappings;
        private final ImmutableOpenMap.Builder<String, AliasMetaData> aliases;

        public Builder(String index) {
            this.index = index;
            this.mappings = ImmutableOpenMap.builder();
            this.aliases = ImmutableOpenMap.builder();
        }

        public Builder(IndexMetaData indexMetaData) {
            this.index = indexMetaData.index();
            this.state = indexMetaData.state;
            this.version = indexMetaData.version;
            this.settings = indexMetaData.settings();
            this.mappings = ImmutableOpenMap.builder(indexMetaData.mappings);
            this.aliases = ImmutableOpenMap.builder(indexMetaData.aliases);
            parts.putAll(indexMetaData.parts());
            parts.remove(SETTINGS_TYPE);
            parts.remove(MAPPINGS_TYPE);
            parts.remove(ALIASES_TYPE);
        }

        public String index() {
            return index;
        }

        public Builder index(String index) {
            this.index = index;
            return this;
        }

        public String getKey() {
            return index;
        }

        public Builder numberOfShards(int numberOfShards) {
            settings = settingsBuilder().put(settings).put(SETTING_NUMBER_OF_SHARDS, numberOfShards).build();
            return this;
        }

        public int numberOfShards() {
            return settings.getAsInt(SETTING_NUMBER_OF_SHARDS, -1);
        }

        public Builder numberOfReplicas(int numberOfReplicas) {
            settings = settingsBuilder().put(settings).put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas).build();
            return this;
        }

        public int numberOfReplicas() {
            return settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, -1);
        }
        
        public Builder creationDate(long creationDate) {
            settings = settingsBuilder().put(settings).put(SETTING_CREATION_DATE, creationDate).build();
            return this;
        }

        public long creationDate() {
            return settings.getAsLong(SETTING_CREATION_DATE, -1l);
        }

        public Builder settings(Settings.Builder settings) {
            this.settings = settings.build();
            return this;
        }

        public Builder settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public MappingMetaData mapping(String type) {
            return mappings.get(type);
        }

        public Builder removeMapping(String mappingType) {
            mappings.remove(mappingType);
            return this;
        }

        public Builder putMapping(String type, String source) throws IOException {
            try (XContentParser parser = XContentFactory.xContent(source).createParser(source)) {
                putMapping(new MappingMetaData(type, parser.mapOrdered()));
            }
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

        public Builder putCustom(String type, IndexClusterStatePart customIndexMetaData) {
            this.parts.put(type, customIndexMetaData);
            return this;
        }

        public Builder removeCustom(String type) {
            this.parts.remove(type);
            return this;
        }

        public ClusterStatePart getCustom(String type) {
            return this.parts.get(type);
        }

        public long version() {
            return this.version;
        }

        public Builder version(long version) {
            this.version = version;
            return this;
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

            return new IndexMetaData(index, version, state, buildParts(tmpSettings, mappings.build(), tmpAliases.build(), parts.build()));
        }

        @Override
        public void parseValuePart(XContentParser parser, String currentFieldName, LocalContext context) throws IOException {
            if ("state".equals(currentFieldName)) {
                state = State.fromString(parser.text());
            } else if ("version".equals(currentFieldName)) {
                version = parser.longValue();
            }
        }

        @Override
        public void readValuePartsFrom(StreamInput in, LocalContext context) throws IOException {
            version = in.readVLong();
            state = State.fromId(in.readByte());
        }

        @Override
        public void writeValuePartsTo(StreamOutput out) throws IOException {
            out.writeVLong(version);
            out.writeByte(state.id());
        }

        private static ImmutableOpenMap<String, IndexClusterStatePart> buildParts(Settings settings,
                                                                             ImmutableOpenMap<String, MappingMetaData> mappings,
                                                                             ImmutableOpenMap<String, AliasMetaData> aliases,
                                                                             ImmutableOpenMap<String, IndexClusterStatePart> parts) {
            ImmutableOpenMap.Builder<String, IndexClusterStatePart> builder = ImmutableOpenMap.builder();
            builder.put(SETTINGS_TYPE, SETTINGS_FACTORY.fromSettings(settings));
            builder.put(MAPPINGS_TYPE, MAPPINGS_FACTORY.fromOpenMap(mappings));
            builder.put(ALIASES_TYPE, ALIASES_FACTORY.fromOpenMap(aliases));
            builder.putAll(parts);
            return builder.build();
        }

        public static void toXContent(IndexMetaData indexMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject(indexMetaData.index(), XContentBuilder.FieldCaseConversion.NONE);
            FACTORY.valuesPartToXContent(indexMetaData, builder, params);

            // TODO: we can make this generic but we need to move startObject inside Factory.toXContent
            if (params.paramAsBoolean("reduce_mappings", false)) {
                builder.startObject("mappings");
                for (ObjectObjectCursor<String, MappingMetaData> cursor : indexMetaData.mappings()) {
                    MappingMetaData.FACTORY.toXContent(cursor.value, builder, params);
                }
                builder.endObject();
            } else {
                builder.startArray("mappings");
                for (ObjectObjectCursor<String, MappingMetaData> cursor : indexMetaData.mappings()) {
                    MappingMetaData.FACTORY.toXContent(cursor.value, builder, params);
                }
                builder.endArray();
            }

            for (ObjectObjectCursor<String, IndexClusterStatePart> cursor : indexMetaData.parts()) {
                if (!cursor.key.equals(MAPPINGS_TYPE)) {
                    builder.field(cursor.key, FieldCaseConversion.NONE);
                    builder.startObject();
                    FACTORY.lookupFactorySafe(cursor.key).toXContent(cursor.value, builder, params);
                    builder.endObject();
                }
            }

            builder.endObject();
        }

        public static IndexMetaData fromXContent(XContentParser parser) throws IOException {
            // TODO : switch to NamedCompositeClusterStatePart.AbstractFactory parser
            if (parser.currentToken() == null) { // fresh parser? move to the first token
                parser.nextToken();
            }
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {  // on a start object move to next token
                parser.nextToken();
            }
            Builder builder = new Builder(parser.currentName());

            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("mappings".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                String mappingType = currentFieldName;
                                Map<String, Object> mappingSource = MapBuilder.<String, Object>newMapBuilder().put(mappingType, parser.mapOrdered()).map();
                                builder.putMapping(new MappingMetaData(mappingType, mappingSource));
                            }
                        }
                    } else {
                        // check if its a custom index metadata
                        ClusterStatePart.Factory<IndexClusterStatePart> factory = FACTORY.lookupFactory(currentFieldName);
                        if (factory == null) {
                            //TODO warn
                            parser.skipChildren();
                        } else {
                            builder.putCustom(currentFieldName, factory.fromXContent(parser, null));
                        }
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("mappings".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                                builder.putMapping(new MappingMetaData(new CompressedString(parser.binaryValue())));
                            } else {
                                Map<String, Object> mapping = parser.mapOrdered();
                                if (mapping.size() == 1) {
                                    String mappingType = mapping.keySet().iterator().next();
                                    builder.putMapping(new MappingMetaData(mappingType, mapping));
                                }
                            }
                        }
                    }
                } else if (token.isValue()) {
                    builder.parseValuePart(parser, currentFieldName, null);
                }
            }
            return builder.build();
        }

        public static IndexMetaData readFrom(StreamInput in) throws IOException {
            return FACTORY.readFrom(in, null);
        }

        public static void writeTo(IndexMetaData indexMetaData, StreamOutput out) throws IOException {
            FACTORY.writeTo(indexMetaData, out);
        }
    }

    @Override
    public String key() {
        return index;
    }

}
