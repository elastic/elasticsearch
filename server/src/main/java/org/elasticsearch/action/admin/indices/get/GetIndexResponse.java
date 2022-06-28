/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.get;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

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

    private ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetadata>> mappings = ImmutableOpenMap.of();
    private ImmutableOpenMap<String, List<AliasMetadata>> aliases = ImmutableOpenMap.of();
    private ImmutableOpenMap<String, Settings> settings = ImmutableOpenMap.of();
    private ImmutableOpenMap<String, Settings> defaultSettings = ImmutableOpenMap.of();
    private ImmutableOpenMap<String, String> dataStreams = ImmutableOpenMap.of();
    private final String[] indices;

    public GetIndexResponse(
        String[] indices,
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetadata>> mappings,
        ImmutableOpenMap<String, List<AliasMetadata>> aliases,
        ImmutableOpenMap<String, Settings> settings,
        ImmutableOpenMap<String, Settings> defaultSettings,
        ImmutableOpenMap<String, String> dataStreams
    ) {
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
        if (dataStreams != null) {
            this.dataStreams = dataStreams;
        }
    }

    GetIndexResponse(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readStringArray();

        int mappingsSize = in.readVInt();
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetadata>> mappingsMapBuilder = ImmutableOpenMap.builder();
        for (int i = 0; i < mappingsSize; i++) {
            String key = in.readString();
            int valueSize = in.readVInt();
            ImmutableOpenMap.Builder<String, MappingMetadata> mappingEntryBuilder = ImmutableOpenMap.builder();
            for (int j = 0; j < valueSize; j++) {
                mappingEntryBuilder.put(in.readString(), new MappingMetadata(in));
            }
            mappingsMapBuilder.put(key, mappingEntryBuilder.build());
        }
        mappings = mappingsMapBuilder.build();

        int aliasesSize = in.readVInt();
        ImmutableOpenMap.Builder<String, List<AliasMetadata>> aliasesMapBuilder = ImmutableOpenMap.builder();
        for (int i = 0; i < aliasesSize; i++) {
            String key = in.readString();
            int valueSize = in.readVInt();
            List<AliasMetadata> aliasEntryBuilder = new ArrayList<>(valueSize);
            for (int j = 0; j < valueSize; j++) {
                aliasEntryBuilder.add(new AliasMetadata(in));
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

        if (in.getVersion().onOrAfter(Version.V_7_8_0)) {
            ImmutableOpenMap.Builder<String, String> dataStreamsMapBuilder = ImmutableOpenMap.builder();
            int dataStreamsSize = in.readVInt();
            for (int i = 0; i < dataStreamsSize; i++) {
                dataStreamsMapBuilder.put(in.readString(), in.readOptionalString());
            }
            dataStreams = dataStreamsMapBuilder.build();
        }
    }

    public String[] indices() {
        return indices;
    }

    public String[] getIndices() {
        return indices();
    }

    public ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetadata>> mappings() {
        return mappings;
    }

    public ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetadata>> getMappings() {
        return mappings();
    }

    public ImmutableOpenMap<String, List<AliasMetadata>> aliases() {
        return aliases;
    }

    public ImmutableOpenMap<String, List<AliasMetadata>> getAliases() {
        return aliases();
    }

    public ImmutableOpenMap<String, Settings> settings() {
        return settings;
    }

    public ImmutableOpenMap<String, String> dataStreams() {
        return dataStreams;
    }

    public ImmutableOpenMap<String, String> getDataStreams() {
        return dataStreams();
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
        for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetadata>> indexEntry : mappings) {
            out.writeString(indexEntry.key);
            out.writeVInt(indexEntry.value.size());
            for (ObjectObjectCursor<String, MappingMetadata> mappingEntry : indexEntry.value) {
                out.writeString(mappingEntry.key);
                mappingEntry.value.writeTo(out);
            }
        }
        out.writeVInt(aliases.size());
        for (ObjectObjectCursor<String, List<AliasMetadata>> indexEntry : aliases) {
            out.writeString(indexEntry.key);
            out.writeVInt(indexEntry.value.size());
            for (AliasMetadata aliasEntry : indexEntry.value) {
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
        if (out.getVersion().onOrAfter(Version.V_7_8_0)) {
            out.writeVInt(dataStreams.size());
            for (ObjectObjectCursor<String, String> indexEntry : dataStreams) {
                out.writeString(indexEntry.key);
                out.writeOptionalString(indexEntry.value);
            }
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
                    List<AliasMetadata> indexAliases = aliases.get(index);
                    if (indexAliases != null) {
                        for (final AliasMetadata alias : indexAliases) {
                            AliasMetadata.Builder.toXContent(alias, builder, params);
                        }
                    }
                    builder.endObject();

                    ImmutableOpenMap<String, MappingMetadata> indexMappings = mappings.get(index);
                    boolean includeTypeName = params.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER, DEFAULT_INCLUDE_TYPE_NAME_POLICY);
                    if (includeTypeName) {
                        builder.startObject("mappings");
                        if (indexMappings != null) {
                            for (final ObjectObjectCursor<String, MappingMetadata> typeEntry : indexMappings) {
                                builder.field(typeEntry.key);
                                builder.map(typeEntry.value.sourceAsMap());
                            }
                        }
                        builder.endObject();
                    } else {
                        MappingMetadata mappings = null;
                        for (final ObjectObjectCursor<String, MappingMetadata> typeEntry : indexMappings) {
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

                    String dataStream = dataStreams.get(index);
                    if (dataStream != null) {
                        builder.field("data_stream", dataStream);
                    }
                }
                builder.endObject();
            }
        }
        builder.endObject();
        return builder;
    }

    private static List<AliasMetadata> parseAliases(XContentParser parser) throws IOException {
        List<AliasMetadata> indexAliases = new ArrayList<>();
        // We start at START_OBJECT since parseIndexEntry ensures that
        while (parser.nextToken() != Token.END_OBJECT) {
            ensureExpectedToken(Token.FIELD_NAME, parser.currentToken(), parser);
            indexAliases.add(AliasMetadata.Builder.fromXContent(parser));
        }
        return indexAliases;
    }

    private static ImmutableOpenMap<String, MappingMetadata> parseMappings(XContentParser parser) throws IOException {
        ImmutableOpenMap.Builder<String, MappingMetadata> indexMappings = ImmutableOpenMap.builder();
        // We start at START_OBJECT since parseIndexEntry ensures that
        while (parser.nextToken() != Token.END_OBJECT) {
            ensureExpectedToken(Token.FIELD_NAME, parser.currentToken(), parser);
            parser.nextToken();
            if (parser.currentToken() == Token.START_OBJECT) {
                String mappingType = parser.currentName();
                indexMappings.put(mappingType, new MappingMetadata(mappingType, parser.map()));
            } else if (parser.currentToken() == Token.START_ARRAY) {
                parser.skipChildren();
            }
        }
        return indexMappings.build();
    }

    private static IndexEntry parseIndexEntry(XContentParser parser) throws IOException {
        List<AliasMetadata> indexAliases = null;
        ImmutableOpenMap<String, MappingMetadata> indexMappings = null;
        Settings indexSettings = null;
        Settings indexDefaultSettings = null;
        String dataStream = null;
        // We start at START_OBJECT since fromXContent ensures that
        while (parser.nextToken() != Token.END_OBJECT) {
            ensureExpectedToken(Token.FIELD_NAME, parser.currentToken(), parser);
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
            } else if (parser.currentToken() == Token.VALUE_STRING) {
                if (parser.currentName().equals("data_stream")) {
                    dataStream = parser.text();
                }
                parser.skipChildren();
            } else if (parser.currentToken() == Token.START_ARRAY) {
                parser.skipChildren();
            }
        }
        return new IndexEntry(indexAliases, indexMappings, indexSettings, indexDefaultSettings, dataStream);
    }

    // This is just an internal container to make stuff easier for returning
    private static class IndexEntry {
        List<AliasMetadata> indexAliases = new ArrayList<>();
        ImmutableOpenMap<String, MappingMetadata> indexMappings = ImmutableOpenMap.of();
        Settings indexSettings = Settings.EMPTY;
        Settings indexDefaultSettings = Settings.EMPTY;
        String dataStream;

        IndexEntry(
            List<AliasMetadata> indexAliases,
            ImmutableOpenMap<String, MappingMetadata> indexMappings,
            Settings indexSettings,
            Settings indexDefaultSettings,
            String dataStream
        ) {
            if (indexAliases != null) this.indexAliases = indexAliases;
            if (indexMappings != null) this.indexMappings = indexMappings;
            if (indexSettings != null) this.indexSettings = indexSettings;
            if (indexDefaultSettings != null) this.indexDefaultSettings = indexDefaultSettings;
            if (dataStream != null) this.dataStream = dataStream;
        }
    }

    public static GetIndexResponse fromXContent(XContentParser parser) throws IOException {
        ImmutableOpenMap.Builder<String, List<AliasMetadata>> aliases = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetadata>> mappings = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, Settings> settings = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, Settings> defaultSettings = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, String> dataStreams = ImmutableOpenMap.builder();
        List<String> indices = new ArrayList<>();

        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        ensureExpectedToken(Token.START_OBJECT, parser.currentToken(), parser);
        parser.nextToken();

        while (parser.isClosed() == false) {
            if (parser.currentToken() == Token.START_OBJECT) {
                // we assume this is an index entry
                String indexName = parser.currentName();
                indices.add(indexName);
                IndexEntry indexEntry = parseIndexEntry(parser);
                // make the order deterministic
                CollectionUtil.timSort(indexEntry.indexAliases, Comparator.comparing(AliasMetadata::alias));
                aliases.put(indexName, Collections.unmodifiableList(indexEntry.indexAliases));
                mappings.put(indexName, indexEntry.indexMappings);
                settings.put(indexName, indexEntry.indexSettings);
                if (indexEntry.indexDefaultSettings.isEmpty() == false) {
                    defaultSettings.put(indexName, indexEntry.indexDefaultSettings);
                }
                if (indexEntry.dataStream != null) {
                    dataStreams.put(indexName, indexEntry.dataStream);
                }
            } else if (parser.currentToken() == Token.START_ARRAY) {
                parser.skipChildren();
            } else {
                parser.nextToken();
            }
        }
        return new GetIndexResponse(
            indices.toArray(new String[0]),
            mappings.build(),
            aliases.build(),
            settings.build(),
            defaultSettings.build(),
            dataStreams.build()
        );
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetIndexResponse that = (GetIndexResponse) o;
        return Arrays.equals(indices, that.indices)
            && Objects.equals(aliases, that.aliases)
            && Objects.equals(mappings, that.mappings)
            && Objects.equals(settings, that.settings)
            && Objects.equals(defaultSettings, that.defaultSettings)
            && Objects.equals(dataStreams, that.dataStreams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(indices), aliases, mappings, settings, defaultSettings, dataStreams);
    }
}
