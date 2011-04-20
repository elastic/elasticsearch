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

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.Immutable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.*;

/**
 * @author kimchy (shay.banon)
 */
@Immutable
public class IndexMetaData {

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

    private final String index;

    private final State state;

    private final ImmutableSet<String> aliases;

    private final Settings settings;

    private final ImmutableMap<String, MappingMetaData> mappings;

    private transient final int totalNumberOfShards;

    private IndexMetaData(String index, State state, Settings settings, ImmutableMap<String, MappingMetaData> mappings) {
        Preconditions.checkArgument(settings.getAsInt(SETTING_NUMBER_OF_SHARDS, -1) != -1, "must specify numberOfShards for index [" + index + "]");
        Preconditions.checkArgument(settings.getAsInt(SETTING_NUMBER_OF_REPLICAS, -1) != -1, "must specify numberOfReplicas for index [" + index + "]");
        this.index = index;
        this.state = state;
        this.settings = settings;
        this.mappings = mappings;
        this.totalNumberOfShards = numberOfShards() * (numberOfReplicas() + 1);

        this.aliases = ImmutableSet.copyOf(settings.getAsArray("index.aliases"));
    }

    public String index() {
        return index;
    }

    public String getIndex() {
        return index();
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

    public ImmutableSet<String> aliases() {
        return this.aliases;
    }

    public ImmutableSet<String> getAliases() {
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

    public static Builder newIndexMetaDataBuilder(String index) {
        return new Builder(index);
    }

    public static Builder newIndexMetaDataBuilder(IndexMetaData indexMetaData) {
        return new Builder(indexMetaData);
    }

    public static class Builder {

        private String index;

        private State state = State.OPEN;

        private Settings settings = ImmutableSettings.Builder.EMPTY_SETTINGS;

        private MapBuilder<String, MappingMetaData> mappings = MapBuilder.newMapBuilder();

        public Builder(String index) {
            this.index = index;
        }

        public Builder(IndexMetaData indexMetaData) {
            this(indexMetaData.index());
            settings(indexMetaData.settings());
            mappings.putAll(indexMetaData.mappings);
            this.state = indexMetaData.state;
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
                putMapping(new MappingMetaData(type, parser.map()));
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

        public IndexMetaData build() {
            return new IndexMetaData(index, state, settings, mappings.immutableMap());
        }

        public static void toXContent(IndexMetaData indexMetaData, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject(indexMetaData.index(), XContentBuilder.FieldCaseConversion.NONE);

            builder.field("state", indexMetaData.state().toString().toLowerCase());

            builder.startObject("settings");
            for (Map.Entry<String, String> entry : indexMetaData.settings().getAsMap().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();

            builder.startArray("mappings");
            for (Map.Entry<String, MappingMetaData> entry : indexMetaData.mappings().entrySet()) {
                byte[] data = entry.getValue().source().uncompressed();
                XContentParser parser = XContentFactory.xContent(data).createParser(data);
                Map<String, Object> mapping = parser.mapOrdered();
                parser.close();
                builder.map(mapping);
            }
            builder.endArray();

            builder.endObject();
        }

        public static IndexMetaData fromXContent(XContentParser parser) throws IOException {
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
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            Map<String, Object> mapping = parser.mapOrdered();
                            if (mapping.size() == 1) {
                                String mappingType = mapping.keySet().iterator().next();
                                builder.putMapping(new MappingMetaData(mappingType, mapping));
                            }
                        }
                    }
                } else if (token.isValue()) {
                    if ("state".equals(currentFieldName)) {
                        builder.state(State.fromString(parser.text()));
                    }
                }
            }
            return builder.build();
        }

        public static IndexMetaData readFrom(StreamInput in) throws IOException {
            Builder builder = new Builder(in.readUTF());
            builder.state(State.fromId(in.readByte()));
            builder.settings(readSettingsFromStream(in));
            int mappingsSize = in.readVInt();
            for (int i = 0; i < mappingsSize; i++) {
                MappingMetaData mappingMd = MappingMetaData.readFrom(in);
                builder.putMapping(mappingMd);
            }
            return builder.build();
        }

        public static void writeTo(IndexMetaData indexMetaData, StreamOutput out) throws IOException {
            out.writeUTF(indexMetaData.index());
            out.writeByte(indexMetaData.state().id());
            writeSettingsToStream(indexMetaData.settings(), out);
            out.writeVInt(indexMetaData.mappings().size());
            for (MappingMetaData mappingMd : indexMetaData.mappings().values()) {
                MappingMetaData.writeTo(mappingMd, out);
            }
        }
    }
}
