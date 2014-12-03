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
import com.google.common.collect.ImmutableList;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.warmer.get.GetWarmersResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.ImmutableOpenMap.Builder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.elasticsearch.search.warmer.IndexWarmersMetaData.Entry;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A response for a delete index action.
 */
public class GetIndexResponse extends ActionResponse {

    private ImmutableOpenMap<String, ImmutableList<IndexWarmersMetaData.Entry>> warmers = ImmutableOpenMap.of();
    private ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = ImmutableOpenMap.of();
    private ImmutableOpenMap<String, ImmutableList<AliasMetaData>> aliases = ImmutableOpenMap.of();
    private ImmutableOpenMap<String, Settings> settings = ImmutableOpenMap.of();
    private String[] indices;

    GetIndexResponse(String[] indices, ImmutableOpenMap<String, ImmutableList<IndexWarmersMetaData.Entry>> warmers,
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings,
            ImmutableOpenMap<String, ImmutableList<AliasMetaData>> aliases, ImmutableOpenMap<String, Settings> settings) {
        this.indices = indices;
        if (warmers != null) {
            this.warmers = warmers;
        }
        if (mappings != null) {
            this.mappings = mappings;
        }
        if (aliases != null) {
            this.aliases = aliases;
        }
        if (settings != null) {
            this.settings = settings;
        }
    }

    GetIndexResponse() {
    }

    public String[] indices() {
        return indices;
    }

    public String[] getIndices() {
        return indices();
    }

    public ImmutableOpenMap<String, ImmutableList<IndexWarmersMetaData.Entry>> warmers() {
        return warmers;
    }

    public ImmutableOpenMap<String, ImmutableList<IndexWarmersMetaData.Entry>> getWarmers() {
        return warmers();
    }

    public ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings() {
        return mappings;
    }

    public ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> getMappings() {
        return mappings();
    }

    public ImmutableOpenMap<String, ImmutableList<AliasMetaData>> aliases() {
        return aliases;
    }

    public ImmutableOpenMap<String, ImmutableList<AliasMetaData>> getAliases() {
        return aliases();
    }

    public ImmutableOpenMap<String, Settings> settings() {
        return settings;
    }

    public ImmutableOpenMap<String, Settings> getSettings() {
        return settings();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.indices = in.readStringArray();
        int warmersSize = in.readVInt();
        ImmutableOpenMap.Builder<String, ImmutableList<IndexWarmersMetaData.Entry>> warmersMapBuilder = ImmutableOpenMap.builder();
        for (int i = 0; i < warmersSize; i++) {
            String key = in.readString();
            int valueSize = in.readVInt();
            ImmutableList.Builder<IndexWarmersMetaData.Entry> warmerEntryBuilder = ImmutableList.builder();
            for (int j = 0; j < valueSize; j++) {
                warmerEntryBuilder.add(new IndexWarmersMetaData.Entry(
                        in.readString(),
                        in.readStringArray(),
                        in.readOptionalBoolean(),
                        in.readBytesReference())
                );
            }
            warmersMapBuilder.put(key, warmerEntryBuilder.build());
        }
        warmers = warmersMapBuilder.build();
        int mappingsSize = in.readVInt();
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> mappingsMapBuilder = ImmutableOpenMap.builder();
        for (int i = 0; i < mappingsSize; i++) {
            String key = in.readString();
            int valueSize = in.readVInt();
            ImmutableOpenMap.Builder<String, MappingMetaData> mappingEntryBuilder = ImmutableOpenMap.builder();
            for (int j = 0; j < valueSize; j++) {
                mappingEntryBuilder.put(in.readString(), MappingMetaData.readFrom(in));
            }
            mappingsMapBuilder.put(key, mappingEntryBuilder.build());
        }
        mappings = mappingsMapBuilder.build();
        int aliasesSize = in.readVInt();
        ImmutableOpenMap.Builder<String, ImmutableList<AliasMetaData>> aliasesMapBuilder = ImmutableOpenMap.builder();
        for (int i = 0; i < aliasesSize; i++) {
            String key = in.readString();
            int valueSize = in.readVInt();
            ImmutableList.Builder<AliasMetaData> aliasEntryBuilder = ImmutableList.builder();
            for (int j = 0; j < valueSize; j++) {
                aliasEntryBuilder.add(AliasMetaData.Builder.readFrom(in));
            }
            aliasesMapBuilder.put(key, aliasEntryBuilder.build());
        }
        aliases = aliasesMapBuilder.build();
        int settingsSize = in.readVInt();
        ImmutableOpenMap.Builder<String, Settings> settingsMapBuilder = ImmutableOpenMap.builder();
        for (int i = 0; i < settingsSize; i++) {
            String key = in.readString();
            settingsMapBuilder.put(key, ImmutableSettings.readSettingsFromStream(in));
        }
        settings = settingsMapBuilder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        out.writeVInt(warmers.size());
        for (ObjectObjectCursor<String, ImmutableList<IndexWarmersMetaData.Entry>> indexEntry : warmers) {
            out.writeString(indexEntry.key);
            out.writeVInt(indexEntry.value.size());
            for (IndexWarmersMetaData.Entry warmerEntry : indexEntry.value) {
                out.writeString(warmerEntry.name());
                out.writeStringArray(warmerEntry.types());
                out.writeOptionalBoolean(warmerEntry.queryCache());
                out.writeBytesReference(warmerEntry.source());
            }
        }
        out.writeVInt(mappings.size());
        for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> indexEntry : mappings) {
            out.writeString(indexEntry.key);
            out.writeVInt(indexEntry.value.size());
            for (ObjectObjectCursor<String, MappingMetaData> mappingEntry : indexEntry.value) {
                out.writeString(mappingEntry.key);
                MappingMetaData.writeTo(mappingEntry.value, out);
            }
        }
        out.writeVInt(aliases.size());
        for (ObjectObjectCursor<String, ImmutableList<AliasMetaData>> indexEntry : aliases) {
            out.writeString(indexEntry.key);
            out.writeVInt(indexEntry.value.size());
            for (AliasMetaData aliasEntry : indexEntry.value) {
                AliasMetaData.Builder.writeTo(aliasEntry, out);
            }
        }
        out.writeVInt(settings.size());
        for (ObjectObjectCursor<String, Settings> indexEntry : settings) {
            out.writeString(indexEntry.key);
            ImmutableSettings.writeSettingsToStream(indexEntry.value, out);
        }
    }

    public static GetIndexResponse convertResponses(GetAliasesResponse aliasesResponse, GetMappingsResponse mappingsResponse,
            GetSettingsResponse settingsResponse, GetWarmersResponse warmersResponse) {
        Set<String> indices = new HashSet<String>();
        Builder<String, ImmutableList<AliasMetaData>> aliasesBuilder = ImmutableOpenMap.builder();
        if (aliasesResponse != null) {
            ImmutableOpenMap<String, List<AliasMetaData>> returnedAliasesMap = aliasesResponse.getAliases();
            if (returnedAliasesMap != null) {
                for (ObjectObjectCursor<String, List<AliasMetaData>> entry : returnedAliasesMap) {
                    ImmutableList.Builder<AliasMetaData> listBuilder = ImmutableList.builder();
                    listBuilder.addAll(entry.value);
                    aliasesBuilder.put(entry.key, listBuilder.build());
                    indices.add(entry.key);
                }
            }
        }
        ImmutableOpenMap<String, ImmutableList<AliasMetaData>> aliases = aliasesBuilder.build();
        ImmutableOpenMap<String, ImmutableList<Entry>> warmers = null;
        if (warmersResponse != null) {
            warmers = warmersResponse.getWarmers();
            if (warmers != null) {
                for (ObjectObjectCursor<String, ImmutableList<Entry>> warmer : warmers) {
                    indices.add(warmer.key);
                }
            }
        }
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = null;
        if (mappingsResponse != null) {
            mappings = mappingsResponse.getMappings();
            if (mappings != null) {
                for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> mapping : mappings) {
                    indices.add(mapping.key);
                }
            }
        }
        ImmutableOpenMap<String, Settings> indexToSettings = null;
        if (settingsResponse != null) {
            indexToSettings = settingsResponse.getIndexToSettings();
            if (indexToSettings != null) {
                for (ObjectObjectCursor<String, Settings> settings : indexToSettings) {
                    indices.add(settings.key);
                }
            }
        }
        GetIndexResponse response = new GetIndexResponse(indices.toArray(new String[indices.size()]), warmers, mappings,
                aliases, indexToSettings);
        return response;
    }
}
