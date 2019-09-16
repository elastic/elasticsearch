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

package org.elasticsearch.action.admin.indices.get;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.rest.BaseRestHandler.DEFAULT_INCLUDE_TYPE_NAME_POLICY;
import static org.elasticsearch.rest.BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER;

/**
 * A response for a get index action.
 */
public class GetIndexResponse extends ActionResponse implements ToXContentObject {

    private ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = ImmutableOpenMap.of();
    private ImmutableOpenMap<String, List<AliasMetaData>> aliases = ImmutableOpenMap.of();
    private ImmutableOpenMap<String, Settings> settings = ImmutableOpenMap.of();
    private ImmutableOpenMap<String, Settings> defaultSettings = ImmutableOpenMap.of();
    private String[] indices;

    public GetIndexResponse(String[] indices,
                     ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings,
                     ImmutableOpenMap<String, List<AliasMetaData>> aliases,
                     ImmutableOpenMap<String, Settings> settings,
                     ImmutableOpenMap<String, Settings> defaultSettings) {
        this.indices = indices;
        // to have deterministic order
        Arrays.sort(indices);
        if (mappings != null) {
            this.mappings = mappings;
        }
        if (aliases != null) {
            this.aliases = aliases;
        }
        if (settings != null) {
            this.settings = settings;
        }
        if (defaultSettings != null) {
            this.defaultSettings = defaultSettings;
        }
    }

    GetIndexResponse(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readStringArray();

        int mappingsSize = in.readVInt();
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> mappingsMapBuilder = ImmutableOpenMap.builder();
        for (int i = 0; i < mappingsSize; i++) {
            String key = in.readString();
            int valueSize = in.readVInt();
            ImmutableOpenMap.Builder<String, MappingMetaData> mappingEntryBuilder = ImmutableOpenMap.builder();
            for (int j = 0; j < valueSize; j++) {
                mappingEntryBuilder.put(in.readString(), new MappingMetaData(in));
            }
            mappingsMapBuilder.put(key, mappingEntryBuilder.build());
        }
        mappings = mappingsMapBuilder.build();

        int aliasesSize = in.readVInt();
        ImmutableOpenMap.Builder<String, List<AliasMetaData>> aliasesMapBuilder = ImmutableOpenMap.builder();
        for (int i = 0; i < aliasesSize; i++) {
            String key = in.readString();
            int valueSize = in.readVInt();
            List<AliasMetaData> aliasEntryBuilder = new ArrayList<>(valueSize);
            for (int j = 0; j < valueSize; j++) {
                aliasEntryBuilder.add(new AliasMetaData(in));
            }
            aliasesMapBuilder.put(key, Collections.unmodifiableList(aliasEntryBuilder));
        }
        aliases = aliasesMapBuilder.build();

        int settingsSize = in.readVInt();
        ImmutableOpenMap.Builder<String, Settings> settingsMapBuilder = ImmutableOpenMap.builder();
        for (int i = 0; i < settingsSize; i++) {
            String key = in.readString();
            settingsMapBuilder.put(key, Settings.readSettingsFromStream(in));
        }
        settings = settingsMapBuilder.build();

        ImmutableOpenMap.Builder<String, Settings> defaultSettingsMapBuilder = ImmutableOpenMap.builder();
        int defaultSettingsSize = in.readVInt();
        for (int i = 0; i < defaultSettingsSize; i++) {
            defaultSettingsMapBuilder.put(in.readString(), Settings.readSettingsFromStream(in));
        }
        defaultSettings = defaultSettingsMapBuilder.build();
    }

    public String[] indices() {
        return indices;
    }

    public String[] getIndices() {
        return indices();
    }

    public ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings() {
        return mappings;
    }

    public ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> getMappings() {
        return mappings();
    }

    public ImmutableOpenMap<String, List<AliasMetaData>> aliases() {
        return aliases;
    }

    public ImmutableOpenMap<String, List<AliasMetaData>> getAliases() {
        return aliases();
    }

    public ImmutableOpenMap<String, Settings> settings() {
        return settings;
    }

    /**
     * If the originating {@link GetIndexRequest} object was configured to include
     * defaults, this will contain a mapping of index name to {@link Settings} objects.
     * The returned {@link Settings} objects will contain only those settings taking
     * effect as defaults.  Any settings explicitly set on the index will be available
     * via {@link #settings()}.
     * See also {@link GetIndexRequest#includeDefaults(boolean)}
     */
    public ImmutableOpenMap<String, Settings> defaultSettings() {
        return defaultSettings;
    }

    public ImmutableOpenMap<String, Settings> getSettings() {
        return settings();
    }

    /**
     * Returns the string value for the specified index and setting.  If the includeDefaults flag was not set or set to
     * false on the {@link GetIndexRequest}, this method will only return a value where the setting was explicitly set
     * on the index.  If the includeDefaults flag was set to true on the {@link GetIndexRequest}, this method will fall
     * back to return the default value if the setting was not explicitly set.
     */
    public String getSetting(String index, String setting) {
        Settings indexSettings = settings.get(index);
        if (setting != null) {
            if (indexSettings != null && indexSettings.hasValue(setting)) {
                return indexSettings.get(setting);
            } else {
                Settings defaultIndexSettings = defaultSettings.get(index);
                if (defaultIndexSettings != null) {
                    return defaultIndexSettings.get(setting);
                } else {
                    return null;
                }
            }
        } else {
            return null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(indices);
        out.writeVInt(mappings.size());
        for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> indexEntry : mappings) {
            out.writeString(indexEntry.key);
            out.writeVInt(indexEntry.value.size());
            for (ObjectObjectCursor<String, MappingMetaData> mappingEntry : indexEntry.value) {
                out.writeString(mappingEntry.key);
                mappingEntry.value.writeTo(out);
            }
        }
        out.writeVInt(aliases.size());
        for (ObjectObjectCursor<String, List<AliasMetaData>> indexEntry : aliases) {
            out.writeString(indexEntry.key);
            out.writeVInt(indexEntry.value.size());
            for (AliasMetaData aliasEntry : indexEntry.value) {
                aliasEntry.writeTo(out);
            }
        }
        out.writeVInt(settings.size());
        for (ObjectObjectCursor<String, Settings> indexEntry : settings) {
            out.writeString(indexEntry.key);
            Settings.writeSettingsToStream(indexEntry.value, out);
        }
        out.writeVInt(defaultSettings.size());
        for (ObjectObjectCursor<String, Settings> indexEntry : defaultSettings) {
            out.writeString(indexEntry.key);
            Settings.writeSettingsToStream(indexEntry.value, out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            for (final String index : indices) {
                builder.startObject(index);
                {
                    builder.startObject("aliases");
                    List<AliasMetaData> indexAliases = aliases.get(index);
                    if (indexAliases != null) {
                        for (final AliasMetaData alias : indexAliases) {
                            AliasMetaData.Builder.toXContent(alias, builder, params);
                        }
                    }
                    builder.endObject();

                    ImmutableOpenMap<String, MappingMetaData> indexMappings = mappings.get(index);
                    boolean includeTypeName = params.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER,
                        DEFAULT_INCLUDE_TYPE_NAME_POLICY);
                    if (includeTypeName) {
                        builder.startObject("mappings");
                        if (indexMappings != null) {
                            for (final ObjectObjectCursor<String, MappingMetaData> typeEntry : indexMappings) {
                                builder.field(typeEntry.key);
                                builder.map(typeEntry.value.sourceAsMap());
                            }
                        }
                        builder.endObject();
                    } else {
                        MappingMetaData mappings = null;
                        for (final ObjectObjectCursor<String, MappingMetaData> typeEntry : indexMappings) {
                            if (typeEntry.key.equals(MapperService.DEFAULT_MAPPING) == false) {
                                assert mappings == null;
                                mappings = typeEntry.value;
                            }
                        }
                        if (mappings == null) {
                            // no mappings yet
                            builder.startObject("mappings").endObject();
                        } else {
                            builder.field("mappings", mappings.sourceAsMap());
                        }
                    }

                    builder.startObject("settings");
                    Settings indexSettings = settings.get(index);
                    if (indexSettings != null) {
                        indexSettings.toXContent(builder, params);
                    }
                    builder.endObject();

                    Settings defaultIndexSettings = defaultSettings.get(index);
                    if (defaultIndexSettings != null && defaultIndexSettings.isEmpty() == false) {
                        builder.startObject("defaults");
                        defaultIndexSettings.toXContent(builder, params);
                        builder.endObject();
                    }
                }
                builder.endObject();
            }
        }
        builder.endObject();
        return builder;
    }

    private static List<AliasMetaData> parseAliases(XContentParser parser) throws IOException {
        List<AliasMetaData> indexAliases = new ArrayList<>();
        // We start at START_OBJECT since parseIndexEntry ensures that
        while (parser.nextToken() != Token.END_OBJECT) {
            ensureExpectedToken(Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
            indexAliases.add(AliasMetaData.Builder.fromXContent(parser));
        }
        return indexAliases;
    }

    private static ImmutableOpenMap<String, MappingMetaData> parseMappings(XContentParser parser) throws IOException {
        ImmutableOpenMap.Builder<String, MappingMetaData> indexMappings = ImmutableOpenMap.builder();
        // We start at START_OBJECT since parseIndexEntry ensures that
        while (parser.nextToken() != Token.END_OBJECT) {
            ensureExpectedToken(Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
            parser.nextToken();
            if (parser.currentToken() == Token.START_OBJECT) {
                String mappingType = parser.currentName();
                indexMappings.put(mappingType, new MappingMetaData(mappingType, parser.map()));
            } else if (parser.currentToken() == Token.START_ARRAY) {
                parser.skipChildren();
            }
        }
        return indexMappings.build();
    }

    private static IndexEntry parseIndexEntry(XContentParser parser) throws IOException {
        List<AliasMetaData> indexAliases = null;
        ImmutableOpenMap<String, MappingMetaData> indexMappings = null;
        Settings indexSettings = null;
        Settings indexDefaultSettings = null;
        // We start at START_OBJECT since fromXContent ensures that
        while (parser.nextToken() != Token.END_OBJECT) {
            ensureExpectedToken(Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
            parser.nextToken();
            if (parser.currentToken() == Token.START_OBJECT) {
                switch (parser.currentName()) {
                    case "aliases":
                        indexAliases = parseAliases(parser);
                        break;
                    case "mappings":
                        indexMappings = parseMappings(parser);
                        break;
                    case "settings":
                        indexSettings = Settings.fromXContent(parser);
                        break;
                    case "defaults":
                        indexDefaultSettings = Settings.fromXContent(parser);
                        break;
                    default:
                        parser.skipChildren();
                }
            } else if (parser.currentToken() == Token.START_ARRAY) {
                parser.skipChildren();
            }
        }
        return new IndexEntry(indexAliases, indexMappings, indexSettings, indexDefaultSettings);
    }

    // This is just an internal container to make stuff easier for returning
    private static class IndexEntry {
        List<AliasMetaData> indexAliases = new ArrayList<>();
        ImmutableOpenMap<String, MappingMetaData> indexMappings = ImmutableOpenMap.of();
        Settings indexSettings = Settings.EMPTY;
        Settings indexDefaultSettings = Settings.EMPTY;
        IndexEntry(List<AliasMetaData> indexAliases, ImmutableOpenMap<String, MappingMetaData> indexMappings,
                   Settings indexSettings, Settings indexDefaultSettings) {
            if (indexAliases != null) this.indexAliases = indexAliases;
            if (indexMappings != null) this.indexMappings = indexMappings;
            if (indexSettings != null) this.indexSettings = indexSettings;
            if (indexDefaultSettings != null) this.indexDefaultSettings = indexDefaultSettings;
        }
    }

    public static GetIndexResponse fromXContent(XContentParser parser) throws IOException {
        ImmutableOpenMap.Builder<String, List<AliasMetaData>> aliases = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> mappings = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, Settings> settings = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, Settings> defaultSettings = ImmutableOpenMap.builder();
        List<String> indices = new ArrayList<>();

        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        ensureExpectedToken(Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        parser.nextToken();

        while (!parser.isClosed()) {
            if (parser.currentToken() == Token.START_OBJECT) {
                // we assume this is an index entry
                String indexName = parser.currentName();
                indices.add(indexName);
                IndexEntry indexEntry = parseIndexEntry(parser);
                // make the order deterministic
                CollectionUtil.timSort(indexEntry.indexAliases, Comparator.comparing(AliasMetaData::alias));
                aliases.put(indexName, Collections.unmodifiableList(indexEntry.indexAliases));
                mappings.put(indexName, indexEntry.indexMappings);
                settings.put(indexName, indexEntry.indexSettings);
                if (indexEntry.indexDefaultSettings.isEmpty() == false) {
                    defaultSettings.put(indexName, indexEntry.indexDefaultSettings);
                }
            } else if (parser.currentToken() == Token.START_ARRAY) {
                parser.skipChildren();
            } else {
                parser.nextToken();
            }
        }
        return
            new GetIndexResponse(
                indices.toArray(new String[0]), mappings.build(), aliases.build(),
                settings.build(), defaultSettings.build()
            );
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o== null || getClass() != o.getClass()) return false;
        GetIndexResponse that = (GetIndexResponse) o;
        return Arrays.equals(indices, that.indices) &&
            Objects.equals(aliases, that.aliases) &&
            Objects.equals(mappings, that.mappings) &&
            Objects.equals(settings, that.settings) &&
            Objects.equals(defaultSettings, that.defaultSettings);
    }

    @Override
    public int hashCode() {
        return
            Objects.hash(
                Arrays.hashCode(indices),
                aliases,
                mappings,
                settings,
                defaultSettings
            );
    }
}
