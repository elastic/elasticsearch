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

package org.elasticsearch.client.indices;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
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

    private Map<String, MappingMetaData> mappings;
    private Map<String, List<AliasMetaData>> aliases;
    private Map<String, Settings> settings;
    private Map<String, Settings> defaultSettings;
    private String[] indices;

    GetIndexResponse(String[] indices,
                     Map<String, MappingMetaData> mappings,
                     Map<String, List<AliasMetaData>> aliases,
                     Map<String, Settings> settings,
                     Map<String, Settings> defaultSettings) {
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

    public String[] getIndices() {
        return indices;
    }

    public Map<String, MappingMetaData> getMappings() {
        return mappings;
    }

    public Map<String, List<AliasMetaData>> getAliases() {
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

    private static List<AliasMetaData> parseAliases(XContentParser parser) throws IOException {
        List<AliasMetaData> indexAliases = new ArrayList<>();
        // We start at START_OBJECT since parseIndexEntry ensures that
        while (parser.nextToken() != Token.END_OBJECT) {
            ensureExpectedToken(Token.FIELD_NAME, parser.currentToken(), parser::getTokenLocation);
            indexAliases.add(AliasMetaData.Builder.fromXContent(parser));
        }
        return indexAliases;
    }

    private static MappingMetaData parseMappings(XContentParser parser) throws IOException {
        return new MappingMetaData(MapperService.SINGLE_MAPPING_NAME, parser.map());
    }

    private static IndexEntry parseIndexEntry(XContentParser parser) throws IOException {
        List<AliasMetaData> indexAliases = null;
        MappingMetaData indexMappings = null;
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
        MappingMetaData indexMappings;
        Settings indexSettings = Settings.EMPTY;
        Settings indexDefaultSettings = Settings.EMPTY;
        IndexEntry(List<AliasMetaData> indexAliases, MappingMetaData indexMappings, Settings indexSettings, Settings indexDefaultSettings) {
            if (indexAliases != null) this.indexAliases = indexAliases;
            if (indexMappings != null) this.indexMappings = indexMappings;
            if (indexSettings != null) this.indexSettings = indexSettings;
            if (indexDefaultSettings != null) this.indexDefaultSettings = indexDefaultSettings;
        }
    }

    public static GetIndexResponse fromXContent(XContentParser parser) throws IOException {
        Map<String, List<AliasMetaData>> aliases = new HashMap<>();
        Map<String, MappingMetaData> mappings = new HashMap<>();
        Map<String, Settings> settings = new HashMap<>();
        Map<String, Settings> defaultSettings = new HashMap<>();
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
        return new GetIndexResponse(indices.toArray(new String[0]), mappings, aliases, settings, defaultSettings);
    }
}
