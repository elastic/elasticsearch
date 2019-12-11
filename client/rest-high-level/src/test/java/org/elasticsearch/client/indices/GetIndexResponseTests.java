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
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.GetAliasesResponseTests;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.RandomCreateIndexGenerator;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GetIndexResponseTests extends AbstractResponseTestCase<org.elasticsearch.action.admin.indices.get.GetIndexResponse,
    GetIndexResponse> {

    @Override
    protected org.elasticsearch.action.admin.indices.get.GetIndexResponse createServerTestInstance(XContentType xContentType) {
        String[] indices = generateRandomStringArray(5, 5, false, false);
        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, List<AliasMetaData>> aliases = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, Settings> settings = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, Settings> defaultSettings = ImmutableOpenMap.builder();
        IndexScopedSettings indexScopedSettings = IndexScopedSettings.DEFAULT_SCOPED_SETTINGS;
        boolean includeDefaults = randomBoolean();
        for (String index: indices) {
            mappings.put(index, createMappingsForIndex());

            List<AliasMetaData> aliasMetaDataList = new ArrayList<>();
            int aliasesNum = randomIntBetween(0, 3);
            for (int i=0; i<aliasesNum; i++) {
                aliasMetaDataList.add(GetAliasesResponseTests.createAliasMetaData());
            }
            CollectionUtil.timSort(aliasMetaDataList, Comparator.comparing(AliasMetaData::alias));
            aliases.put(index, Collections.unmodifiableList(aliasMetaDataList));

            Settings.Builder builder = Settings.builder();
            builder.put(RandomCreateIndexGenerator.randomIndexSettings());
            settings.put(index, builder.build());

            if (includeDefaults) {
                defaultSettings.put(index, indexScopedSettings.diff(settings.get(index), Settings.EMPTY));
            }
        }
        return new org.elasticsearch.action.admin.indices.get.GetIndexResponse(indices,
            mappings.build(), aliases.build(), settings.build(), defaultSettings.build());
    }

    @Override
    protected GetIndexResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetIndexResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.action.admin.indices.get.GetIndexResponse serverTestInstance,
                                   GetIndexResponse clientInstance) {
        assertArrayEquals(serverTestInstance.getIndices(), clientInstance.getIndices());
        assertMapEquals(serverTestInstance.getMappings(), clientInstance.getMappings());
        assertMapEquals(serverTestInstance.getSettings(), clientInstance.getSettings());
        assertMapEquals(serverTestInstance.defaultSettings(), clientInstance.getDefaultSettings());
        assertMapEquals(serverTestInstance.getAliases(), clientInstance.getAliases());
    }

    private static MappingMetaData createMappingsForIndex() {
        int typeCount = rarely() ? 0 : 1;
        MappingMetaData mmd = new MappingMetaData(MapperService.SINGLE_MAPPING_NAME, Collections.emptyMap());
        for (int i = 0; i < typeCount; i++) {
            if (rarely() == false) { // rarely have no fields
                Map<String, Object> mappings = new HashMap<>();
                mappings.put("field-" + i, randomFieldMapping());
                if (randomBoolean()) {
                    mappings.put("field2-" + i, randomFieldMapping());
                }

                String typeName = MapperService.SINGLE_MAPPING_NAME;
                mmd = new MappingMetaData(typeName, mappings);
            }
        }
        return mmd;
    }

    // Not meant to be exhaustive
    private static Map<String, Object> randomFieldMapping() {
        Map<String, Object> mappings = new HashMap<>();
        if (randomBoolean()) {
            mappings.put("type", randomBoolean() ? "text" : "keyword");
            mappings.put("index", "analyzed");
            mappings.put("analyzer", "english");
        } else if (randomBoolean()) {
            mappings.put("type", randomFrom("integer", "float", "long", "double"));
            mappings.put("index", Objects.toString(randomBoolean()));
        } else if (randomBoolean()) {
            mappings.put("type", "object");
            mappings.put("dynamic", "strict");
            Map<String, Object> properties = new HashMap<>();
            Map<String, Object> props1 = new HashMap<>();
            props1.put("type", randomFrom("text", "keyword"));
            props1.put("analyzer", "keyword");
            properties.put("subtext", props1);
            Map<String, Object> props2 = new HashMap<>();
            props2.put("type", "object");
            Map<String, Object> prop2properties = new HashMap<>();
            Map<String, Object> props3 = new HashMap<>();
            props3.put("type", "integer");
            props3.put("index", "false");
            prop2properties.put("subsubfield", props3);
            props2.put("properties", prop2properties);
            mappings.put("properties", properties);
        } else {
            mappings.put("type", "keyword");
        }
        return mappings;
    }

}
