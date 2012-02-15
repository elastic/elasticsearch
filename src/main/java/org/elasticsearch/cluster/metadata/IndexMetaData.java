/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this 
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNodeFilters;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.*;

/**
 *
 */
public class IndexMetaData {

    private static ImmutableSet<String> dynamicSettings = ImmutableSet.<String>builder()
            .add(IndexMetaData.SETTING_NUMBER_OF_REPLICAS)
            .add(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS)
            .add(IndexMetaData.SETTING_READ_ONLY)
            .build();

    public static final ClusterBlock INDEX_READ_ONLY_BLOCK = new ClusterBlock(5, "index read-only (api)", false, false, RestStatus.FORBIDDEN, ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA);

    public static ImmutableSet<String> dynamicSettings() {
        return dynamicSettings;
    }

    public static boolean hasDynamicSetting(String key) {
        for (String dynamicSetting : dynamicSettings) {
            if (Regex.simpleMatch(dynamicSetting, key)) {
                return true;
            }
        }
        return false;
    }

    public static synchronized void addDynamicSettings(String... settings) {
        HashSet<String> updatedSettings = new HashSet<String>(dynamicSettings);
        updatedSettings.addAll(Arrays.asList(settings));
        dynamicSettings = ImmutableSet.copyOf(updatedSettings);
    }

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
            throw new ElasticSearchIllegalStateException("No state match for id [" + id + "]");
        }

        public static State fromString(String state) {
            if ("open".equals(state)) {
                return OPEN;
            } else if ("close".equals(state)) {
                return CLOSE;
            }
            throw new ElasticSearchIllegalStateException("No state match for [" + state + "]");
        }
    }

    public static final String SETTING_NUMBER_OF_SHARDS = "index.number_of_shards";
    public static final String SETTING_NUMBER_OF_REPLICAS = "index.number_of_replicas";
    public static final String SETTING_AUTO_EXPAND_REPLICAS = "index.auto_expand_replicas";
    public static final String SETTING_READ_ONLY = "index.blocks.read_only";
    public static final String SETTING_VERSION_CREATED = "index.version.created";

    private final String index;
    private final long version;

    private final State state;

    private final ImmutableMap<String, AliasMetaData> aliases;

    private final Settings settings;

    private final ImmutableMap<String, MappingMetaData> mappings;

    private transient final int totalNumberOfShards;

    private final DiscoveryNodeFilters includeFilters;
    private final DiscoveryNodeFilters excludeFilters;

    private IndexMetaData(String index, long version, State state, Settings settings, ImmutableMap<String, MappingMetaData> mappings, ImmutableMap<String, AliasMetaData> aliases) {
        Preconditions.checkArgument(settings.getAsInt(SETTING_NUMBER_OF_SHARDS, -1) != -1, "must specify numberOfShards for index [" + index + "]");
        Preconditions.checkArgument(settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, -1) != -1, "must specify numberOfReplicas for index [" + index + "]");
        this.index = index;
        this.version = version;
        this.state = state;
        this.settings = settings;
        this.mappings = mappings;
        this.totalNumberOfShards = numberOfShards() * (numberOfReplicas() + 1);

        this.aliases = aliases;

        ImmutableMap<String, String> includeMap = settings.getByPrefix("index.routing.allocation.include.").getAsMap();
        if (includeMap.isEmpty()) {
            includeFilters = null;
        } else {
            includeFilters = DiscoveryNodeFilters.buildFromKeyValue(includeMap);
        }
        ImmutableMap<String, String> excludeMap = settings.getByPrefix("index.routing.allocation.exclude.").getAsMap();
        if (excludeMap.isEmpty()) {
            excludeFilters = null;
        } else {
            excludeFilters = DiscoveryNodeFilters.buildFromKeyValue(excludeMap);
        }
    }

    public String index() {
        return index;
    }

    public String getIndex() {
        return index();
    }

    public long version() {
        return this.version;
    }

    public long getVersion() {
        return this.version;
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

    public ImmutableMap<String, AliasMetaData> aliases() {
        return this.aliases;
    }

    public ImmutableMap<String, AliasMetaData> getAliases() {
        return aliases();
    }

    public ImmutableMap<String, MappingMetaData> mappings() {
        return mappings;
    }

    public ImmutableMap<String, MappingMetaData> getMappings() {
        return mappings();
    }

    public MappingMetaData mapping(String mappingType) {
        return mappings.get(mappingType);
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexMetaData that = (IndexMetaData) o;

        if (!aliases.equals(that.aliases)) return false;
        if (!index.equals(that.index)) return false;
        if (!mappings.equals(that.mappings)) return false;
        if (!settings.equals(that.settings)) return false;
        if (state != that.state) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + state.hashCode();
        result = 31 * result + aliases.hashCode();
        result = 31 * result + settings.hashCode();
        result = 31 * result + mappings.hashCode();
        return result;
    }

    public static Builder newIndexMetaDataBuilder(String index) {
        return new Builder(index);
    }

    public static Builder newIndexMetaDataBuilder(IndexMetaData indexMetaData) {
        return new Builder(indexMetaData);
    }

    public static class Builder {

        private String index;

        private State state = State.OPEN;

        private long version = 1;

        private Settings settings = ImmutableSettings.Builder.EMPTY_SETTINGS;

        private MapBuilder<String, MappingMetaData> mappings = MapBuilder.newMapBuilder();

        private MapBuilder<String, AliasMetaData> aliases = MapBuilder.newMapBuilder();

        public Builder(String index) {
            this.index = index;
        }

        public Builder(IndexMetaData indexMetaData) {
            this(indexMetaData.index());
            settings(indexMetaData.settings());
            mappings.putAll(indexMetaData.mappings);
            aliases.putAll(indexMetaData.aliases);
            this.state = indexMetaData.state;
            this.version = indexMetaData.version;
        }

        public String index() {
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

        public Builder settings(Settings.Builder settings) {
            this.settings = settings.build();
            return this;
        }

        public Builder settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public Builder removeMapping(String mappingType) {
            mappings.remove(mappingType);
            return this;
        }

        public Builder putMapping(String type, String source) throws IOException {
            XContentParser parser = XContentFactory.xContent(source).createParser(source);
            try {
                putMapping(new MappingMetaData(type, parser.mapOrdered()));
            } finally {
                parser.close();
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

        public Builder removerAlias(String alias) {
            aliases.remove(alias);
            return this;
        }

        public long version() {
            return this.version;
        }

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        public IndexMetaData build() {
            MapBuilder<String, AliasMetaData> tmpAliases = aliases;
            Settings tmpSettings = settings;

            // For backward compatibility
            String[] legacyAliases = settings.getAsArray("index.aliases");
            if (legacyAliases.length > 0) {
                tmpAliases = MapBuilder.newMapBuilder();
                for (String alias : legacyAliases) {
                    AliasMetaData aliasMd = AliasMetaData.newAliasMetaDataBuilder(alias).build();
                    tmpAliases.put(alias, aliasMd);
                }
                tmpAliases.putAll(aliases.immutableMap());
                // Remove index.aliases from settings once they are migrated to the new data structure
                tmpSettings = ImmutableSettings.settingsBuilder().put(settings).putArray("index.aliases").build();
            }

            return new IndexMetaData(index, version, state, tmpSettings, mappings.immutableMap(), tmpAliases.immutableMap());
        }

        public static void toXContent(IndexMetaData indexMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject(indexMetaData.index(), XContentBuilder.FieldCaseConversion.NONE);

            builder.field("version", indexMetaData.version());
            builder.field("state", indexMetaData.state().toString().toLowerCase());

            boolean binary = params.paramAsBoolean("binary", false);

            builder.startObject("settings");
            for (Map.Entry<String, String> entry : indexMetaData.settings().getAsMap().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();

            builder.startArray("mappings");
            for (Map.Entry<String, MappingMetaData> entry : indexMetaData.mappings().entrySet()) {
                if (binary) {
                    builder.value(entry.getValue().source().compressed());
                } else {
                    byte[] data = entry.getValue().source().uncompressed();
                    XContentParser parser = XContentFactory.xContent(data).createParser(data);
                    Map<String, Object> mapping = parser.mapOrdered();
                    parser.close();
                    builder.map(mapping);
                }
            }
            builder.endArray();

            builder.startObject("aliases");
            for (AliasMetaData alias : indexMetaData.aliases().values()) {
                AliasMetaData.Builder.toXContent(alias, builder, params);
            }
            builder.endObject();


            builder.endObject();
        }

        public static IndexMetaData fromXContent(XContentParser parser) throws IOException {
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                parser.nextToken();
            }
            Builder builder = new Builder(parser.currentName());

            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("settings".equals(currentFieldName)) {
                        ImmutableSettings.Builder settingsBuilder = settingsBuilder();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            String key = parser.currentName();
                            token = parser.nextToken();
                            String value = parser.text();
                            settingsBuilder.put(key, value);
                        }
                        builder.settings(settingsBuilder.build());
                    } else if ("mappings".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                String mappingType = currentFieldName;
                                Map<String, Object> mappingSource = MapBuilder.<String, Object>newMapBuilder().put(mappingType, parser.mapOrdered()).map();
                                builder.putMapping(new MappingMetaData(mappingType, mappingSource));
                            }
                        }
                    } else if ("aliases".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.putAlias(AliasMetaData.Builder.fromXContent(parser));
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
                    if ("state".equals(currentFieldName)) {
                        builder.state(State.fromString(parser.text()));
                    } else if ("version".equals(currentFieldName)) {
                        builder.version(parser.longValue());
                    }
                }
            }
            return builder.build();
        }

        public static IndexMetaData readFrom(StreamInput in) throws IOException {
            Builder builder = new Builder(in.readUTF());
            builder.version(in.readLong());
            builder.state(State.fromId(in.readByte()));
            builder.settings(readSettingsFromStream(in));
            int mappingsSize = in.readVInt();
            for (int i = 0; i < mappingsSize; i++) {
                MappingMetaData mappingMd = MappingMetaData.readFrom(in);
                builder.putMapping(mappingMd);
            }
            int aliasesSize = in.readVInt();
            for (int i = 0; i < aliasesSize; i++) {
                AliasMetaData aliasMd = AliasMetaData.Builder.readFrom(in);
                builder.putAlias(aliasMd);
            }
            return builder.build();
        }

        public static void writeTo(IndexMetaData indexMetaData, StreamOutput out) throws IOException {
            out.writeUTF(indexMetaData.index());
            out.writeLong(indexMetaData.version());
            out.writeByte(indexMetaData.state().id());
            writeSettingsToStream(indexMetaData.settings(), out);
            out.writeVInt(indexMetaData.mappings().size());
            for (MappingMetaData mappingMd : indexMetaData.mappings().values()) {
                MappingMetaData.writeTo(mappingMd, out);
            }
            out.writeVInt(indexMetaData.aliases().size());
            for (AliasMetaData aliasMd : indexMetaData.aliases().values()) {
                AliasMetaData.Builder.writeTo(aliasMd, out);
            }
        }
    }
}
