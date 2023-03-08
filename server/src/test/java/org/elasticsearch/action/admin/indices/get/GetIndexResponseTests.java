/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.get;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponseTests;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponseTests;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.RandomCreateIndexGenerator;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class GetIndexResponseTests extends AbstractWireSerializingTestCase<GetIndexResponse> {

    @Override
    protected Writeable.Reader<GetIndexResponse> instanceReader() {
        return GetIndexResponse::new;
    }

    @Override
    protected GetIndexResponse createTestInstance() {
        String[] indices = generateRandomStringArray(5, 5, false, false);
        Map<String, MappingMetadata> mappings = new HashMap<>();
        Map<String, List<AliasMetadata>> aliases = new HashMap<>();
        Map<String, Settings> settings = new HashMap<>();
        Map<String, Settings> defaultSettings = new HashMap<>();
        Map<String, String> dataStreams = new HashMap<>();
        IndexScopedSettings indexScopedSettings = IndexScopedSettings.DEFAULT_SCOPED_SETTINGS;
        boolean includeDefaults = randomBoolean();
        for (String index : indices) {
            mappings.put(index, GetMappingsResponseTests.createMappingsForIndex());

            List<AliasMetadata> aliasMetadataList = new ArrayList<>();
            int aliasesNum = randomIntBetween(0, 3);
            for (int i = 0; i < aliasesNum; i++) {
                aliasMetadataList.add(
                    GetAliasesResponseTests.createAliasMetadata(
                        s -> aliasMetadataList.stream().map(AliasMetadata::alias).toList().contains(s)
                    )
                );
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
        return new GetIndexResponse(indices, mappings, aliases, settings, defaultSettings, dataStreams);
    }

    @Override
    protected GetIndexResponse mutateInstance(GetIndexResponse instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public void testChunking() throws IOException {
        AbstractChunkedSerializingTestCase.assertChunkCount(createTestInstance(), response -> response.getIndices().length + 2);
    }
}
