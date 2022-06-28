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
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.RandomCreateIndexGenerator;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;

public class GetIndexResponseTests extends AbstractSerializingTestCase<GetIndexResponse> {

    @Override
    protected GetIndexResponse doParseInstance(XContentParser parser) throws IOException {
        return GetIndexResponse.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<GetIndexResponse> instanceReader() {
        return GetIndexResponse::new;
    }

    @Override
    protected GetIndexResponse createTestInstance() {
        String[] indices = generateRandomStringArray(5, 5, false, false);
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetadata>> mappings = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, List<AliasMetadata>> aliases = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, Settings> settings = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, Settings> defaultSettings = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, String> dataStreams = ImmutableOpenMap.builder();
        IndexScopedSettings indexScopedSettings = IndexScopedSettings.DEFAULT_SCOPED_SETTINGS;
        boolean includeDefaults = randomBoolean();
        for (String index : indices) {
            // rarely have no types
            int typeCount = rarely() ? 0 : 1;
            mappings.put(index, GetMappingsResponseTests.createMappingsForIndex(typeCount, true));

            List<AliasMetadata> aliasMetadataList = new ArrayList<>();
            int aliasesNum = randomIntBetween(0, 3);
            for (int i = 0; i < aliasesNum; i++) {
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
        return new GetIndexResponse(
            indices,
            mappings.build(),
            aliases.build(),
            settings.build(),
            defaultSettings.build(),
            dataStreams.build()
        );
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // we do not want to add new fields at the root (index-level), or inside the blocks
        return f -> f.equals("") || f.contains(".settings") || f.contains(".defaults") || f.contains(".mappings") || f.contains(".aliases");
    }

    /**
     * For xContent roundtrip testing we force the xContent output to still contain types because the parser still expects them.
     * The new typeless parsing is implemented in the client side GetIndexResponse.
     */
    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap(BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER, "true"));
    }
}
