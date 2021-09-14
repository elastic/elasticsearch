/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A client side response for a get index action.
 */
public class GetIndexResponse {

    private Map<String, MappingMetadata> mappings;
    private Map<String, List<AliasMetadata>> aliases;
    private Map<String, Settings> settings;
    private Map<String, Settings> defaultSettings;
    private Map<String, String> dataStreams;
    private String[] indices;

    GetIndexResponse(String[] indices,
                     Map<String, MappingMetadata> mappings,
                     Map<String, List<AliasMetadata>> aliases,
                     Map<String, Settings> settings,
                     Map<String, Settings> defaultSettings,
                     Map<String, String> dataStreams) {
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

    public String[] getIndices() {
        return indices;
    }

    public Map<String, MappingMetadata> getMappings() {
        return mappings;
    }

    public Map<String, List<AliasMetadata>> getAliases() {
        return aliases;
    }

    /**
     * If the originating {@link GetIndexRequest} object was configured to include
     * defaults, this will contain a mapping of index name to {@link Settings} objects.
     * The returned {@link Settings} objects will contain only those settings taking
     * effect as defaults.  Any settings explicitly set on the index will be available
     * via {@link #getSettings()}.
     * See also {@link GetIndexRequest#includeDefaults(boolean)}
     */
    public Map<String, Settings> getDefaultSettings() {
        return defaultSettings;
    }

    public Map<String, Settings> getSettings() {
        return settings;
    }

    public Map<String, String> getDataStreams() {
        return dataStreams;
    }

    /**
     * Returns the string value for the specified index and setting. If the includeDefaults flag was not set or set to
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

    private static List<AliasMetadata> parseAliases(XContentParser parser) throws IOException {
        List<AliasMetadata> indexAliases = new ArrayList<>();
        // We start at START_OBJECT since parseIndexEntry ensures that
        while (parser.nextToken() != Token.END_OBJECT) {
            ensureExpectedToken(Token.FIELD_NAME, parser.currentToken(), parser);
            indexAliases.add(AliasMetadata.Builder.fromXContent(parser));
        }
        return indexAliases;
    }

    private static MappingMetadata parseMappings(XContentParser parser) throws IOException {
        return new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, parser.map());
    }

    private static IndexEntry parseIndexEntry(XContentParser parser) throws IOException {
        List<AliasMetadata> indexAliases = null;
        MappingMetadata indexMappings = null;
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
        MappingMetadata indexMappings;
        Settings indexSettings = Settings.EMPTY;
        Settings indexDefaultSettings = Settings.EMPTY;
        String dataStream;
        IndexEntry(List<AliasMetadata> indexAliases, MappingMetadata indexMappings, Settings indexSettings, Settings indexDefaultSettings,
                   String dataStream) {
            if (indexAliases != null) this.indexAliases = indexAliases;
            if (indexMappings != null) this.indexMappings = indexMappings;
            if (indexSettings != null) this.indexSettings = indexSettings;
            if (indexDefaultSettings != null) this.indexDefaultSettings = indexDefaultSettings;
            if (dataStream != null) this.dataStream = dataStream;
        }
    }

    public static GetIndexResponse fromXContent(XContentParser parser) throws IOException {
        Map<String, List<AliasMetadata>> aliases = new HashMap<>();
        Map<String, MappingMetadata> mappings = new HashMap<>();
        Map<String, Settings> settings = new HashMap<>();
        Map<String, Settings> defaultSettings = new HashMap<>();
        Map<String, String> dataStreams = new HashMap<>();
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
        return new GetIndexResponse(indices.toArray(new String[0]), mappings, aliases, settings, defaultSettings, dataStreams);
    }
}
