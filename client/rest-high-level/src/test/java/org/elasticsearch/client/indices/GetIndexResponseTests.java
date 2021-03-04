/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.GetAliasesResponseTests;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class GetIndexResponseTests extends AbstractResponseTestCase<org.elasticsearch.action.admin.indices.get.GetIndexResponse,
    GetIndexResponse> {

    @Override
    protected org.elasticsearch.action.admin.indices.get.GetIndexResponse createServerTestInstance(XContentType xContentType) {
        String[] indices = generateRandomStringArray(5, 5, false, false);
        ImmutableOpenMap.Builder<String, MappingMetadata> mappings = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, List<AliasMetadata>> aliases = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, Settings> settings = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, Settings> defaultSettings = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, String> dataStreams = ImmutableOpenMap.builder();
        IndexScopedSettings indexScopedSettings = IndexScopedSettings.DEFAULT_SCOPED_SETTINGS;
        boolean includeDefaults = randomBoolean();
        for (String index: indices) {
            mappings.put(index, createMappingsForIndex());

            List<AliasMetadata> aliasMetadataList = new ArrayList<>();
            int aliasesNum = randomIntBetween(0, 3);
            for (int i=0; i<aliasesNum; i++) {
                aliasMetadataList.add(GetAliasesResponseTests.createAliasMetadata());
            }
            CollectionUtil.timSort(aliasMetadataList, Comparator.comparing(AliasMetadata::alias));
            aliases.put(index, Collections.unmodifiableList(aliasMetadataList));

            Settings.Builder builder = Settings.builder();
            builder.put(RandomCreateIndexGenerator.randomIndexSettings());
            settings.put(index, builder.build());

            if (includeDefaults) {
                defaultSettings.put(index, indexScopedSettings.diff(settings.get(index), Settings.EMPTY));
            }

            if (randomBoolean()) {
                dataStreams.put(index, randomAlphaOfLength(5).toLowerCase(Locale.ROOT));
            }
        }
        return new org.elasticsearch.action.admin.indices.get.GetIndexResponse(indices,
            mappings.build(), aliases.build(), settings.build(), defaultSettings.build(), dataStreams.build());
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
        assertMapEquals(serverTestInstance.getDataStreams(), clientInstance.getDataStreams());
    }

    private static MappingMetadata createMappingsForIndex() {
        int typeCount = rarely() ? 0 : 1;
        MappingMetadata mmd = new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, Collections.emptyMap());
        for (int i = 0; i < typeCount; i++) {
            if (rarely() == false) { // rarely have no fields
                Map<String, Object> mappings = new HashMap<>();
                mappings.put("field-" + i, randomFieldMapping());
                if (randomBoolean()) {
                    mappings.put("field2-" + i, randomFieldMapping());
                }

                String typeName = MapperService.SINGLE_MAPPING_NAME;
                mmd = new MappingMetadata(typeName, mappings);
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
