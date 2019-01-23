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
import org.elasticsearch.client.GetAliasesResponseTests;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.RandomCreateIndexGenerator;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GetIndexResponseTests extends ESTestCase {

    private static final int NUMBER_OF_RUNS = 20;

    /**
     * check that we can parse the xContent output of a client side response and get the same information back
     *
     * TODO: ideally we would create a random instance of the client side response that contains the rendering code
     * write it to xContent, parse it back and compare the results
     * this currently would mean we would need test dependencies to server:test
     */
    public void testFromXContent() throws IOException {
        org.elasticsearch.action.admin.indices.get.GetIndexResponse serverSideResponse = createTestInstance();
        for (int runs = 0; runs < NUMBER_OF_RUNS; runs++) {
            XContentType xContentType = randomFrom(XContentType.values());
            BytesReference originalBytes = toShuffledXContent(serverSideResponse, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());
            GetIndexResponse parsedResponse;
            try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
                parsedResponse = GetIndexResponse.fromXContent(parser);
                assertNull(parser.nextToken());
            }
            assertArrayEquals(serverSideResponse.getIndices(), parsedResponse.getIndices());
            for (String index : serverSideResponse.getIndices()) {
                MappingMetaData indexMapping = serverSideResponse.getMappings().get(index).get("_doc");
                if (indexMapping != null) {
                    assertEquals(indexMapping, parsedResponse.getMappings().get(index));
                } else {
                    assertEquals(new MappingMetaData(MapperService.SINGLE_MAPPING_NAME, Collections.emptyMap()),
                            parsedResponse.getMappings().get(index));
                }
            }
            assertEquals(serverSideResponse.getAliases(), parsedResponse.getAliases());
            assertEquals(serverSideResponse.getSettings(), parsedResponse.getSettings());
            assertEquals(serverSideResponse.defaultSettings(), parsedResponse.getDefaultSettings());

        }

    }

    private static org.elasticsearch.action.admin.indices.get.GetIndexResponse createTestInstance() {
        String[] indices = generateRandomStringArray(5, 5, false, false);
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> mappings = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, List<AliasMetaData>> aliases = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, Settings> settings = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, Settings> defaultSettings = ImmutableOpenMap.builder();
        IndexScopedSettings indexScopedSettings = IndexScopedSettings.DEFAULT_SCOPED_SETTINGS;
        boolean includeDefaults = randomBoolean();
        for (String index: indices) {
            // rarely have no types
            int typeCount = rarely() ? 0 : 1;
            mappings.put(index, createMappingsForIndex(typeCount));

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
        return new org.elasticsearch.action.admin.indices.get.GetIndexResponse(
            indices, mappings.build(), aliases.build(), settings.build(), defaultSettings.build()
        );
    }

    public static ImmutableOpenMap<String, MappingMetaData> createMappingsForIndex(int typeCount) {
        List<MappingMetaData> typeMappings = new ArrayList<>(typeCount);

        for (int i = 0; i < typeCount; i++) {
            if (rarely() == false) { // rarely have no fields
                Map<String, Object> mappings = new HashMap<>();
                mappings.put("field-" + i, randomFieldMapping());
                if (randomBoolean()) {
                    mappings.put("field2-" + i, randomFieldMapping());
                }

                try {
                    String typeName = MapperService.SINGLE_MAPPING_NAME;
                    MappingMetaData mmd = new MappingMetaData(typeName, mappings);
                    typeMappings.add(mmd);
                } catch (IOException e) {
                    fail("shouldn't have failed " + e);
                }
            }
        }
        ImmutableOpenMap.Builder<String, MappingMetaData> typeBuilder = ImmutableOpenMap.builder();
        typeMappings.forEach(mmd -> typeBuilder.put(mmd.type(), mmd));
        return typeBuilder.build();
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
