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

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponseTests;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponseTests;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.RandomCreateIndexGenerator;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.IndexSettings.INDEX_REFRESH_INTERVAL_SETTING;

public class GetIndexResponseTests extends AbstractStreamableXContentTestCase<GetIndexResponse> {

    /**
     * The following byte response was generated from the v6.3.0 tag
     */
    private static final String TEST_6_3_0_RESPONSE_BYTES =
        "AQhteV9pbmRleAEIbXlfaW5kZXgBA2RvYwNkb2OePID6KURGTACqVkrLTM1JiTdUsqpWKqksSFWyUiouKcrMS1eqrQUAAAD//" +
            "wMAAAABCG15X2luZGV4AgZhbGlhczEAAQJyMQECcjEGYWxpYXMyAX8jNXYiREZMAKpWKkktylWyqlaqTE0sUrIyMjA0q60FAAAA//" +
            "8DAAAAAQhteV9pbmRleAIYaW5kZXgubnVtYmVyX29mX3JlcGxpY2FzAAExFmluZGV4Lm51bWJlcl9vZl9zaGFyZHMAATI=";
    private static final GetIndexResponse TEST_6_3_0_RESPONSE_INSTANCE = getExpectedTest630Response();

    @Override
    protected GetIndexResponse doParseInstance(XContentParser parser) throws IOException {
        return GetIndexResponse.fromXContent(parser);
    }

    @Override
    protected GetIndexResponse createBlankInstance() {
        return new GetIndexResponse();
    }

    @Override
    protected GetIndexResponse createTestInstance() {
        return createTestInstance(randomBoolean());
    }

    private GetIndexResponse createTestInstance(boolean randomTypeName) {
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
            mappings.put(index, GetMappingsResponseTests.createMappingsForIndex(typeCount, randomTypeName));

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
        return new GetIndexResponse(
            indices, mappings.build(), aliases.build(), settings.build(), defaultSettings.build()
        );
    }

    @Override
    protected GetIndexResponse createXContextTestInstance(XContentType xContentType) {
        // don't use random type names for XContent roundtrip tests because we cannot parse them back anymore
        return createTestInstance(false);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        //we do not want to add new fields at the root (index-level), or inside the blocks
        return
            f -> f.equals("") || f.contains(".settings") || f.contains(".defaults") || f.contains(".mappings") ||
            f.contains(".aliases");
    }

    private static ImmutableOpenMap<String, List<AliasMetaData>> getTestAliases(String indexName) {
        ImmutableOpenMap.Builder<String, List<AliasMetaData>> aliases = ImmutableOpenMap.builder();
        List<AliasMetaData> indexAliases = new ArrayList<>();
        indexAliases.add(new AliasMetaData.Builder("alias1").routing("r1").build());
        indexAliases.add(new AliasMetaData.Builder("alias2").filter("{\"term\": {\"year\": 2016}}").build());
        aliases.put(indexName, Collections.unmodifiableList(indexAliases));
        return aliases.build();
    }

    private static ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> getTestMappings(String indexName) {
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> mappings = ImmutableOpenMap.builder();
        ImmutableOpenMap.Builder<String, MappingMetaData> indexMappings = ImmutableOpenMap.builder();
        try {
            indexMappings.put(
                "doc",
                new MappingMetaData("doc",
                    Collections.singletonMap("field_1", Collections.singletonMap("type", "string"))
                )
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        mappings.put(indexName, indexMappings.build());
        return mappings.build();
    }

    private static ImmutableOpenMap<String, Settings> getTestSettings(String indexName) {
        ImmutableOpenMap.Builder<String, Settings> settings = ImmutableOpenMap.builder();
        Settings.Builder indexSettings = Settings.builder();
        indexSettings.put(SETTING_NUMBER_OF_SHARDS, 2);
        indexSettings.put(SETTING_NUMBER_OF_REPLICAS, 1);
        settings.put(indexName, indexSettings.build());
        return settings.build();
    }

    private static GetIndexResponse getExpectedTest630Response() {
        // The only difference between this snippet and the one used for generation TEST_6_3_0_RESPONSE_BYTES is the
        // constructor for GetIndexResponse which also takes defaultSettings now.
        String indexName = "my_index";
        String indices[] = { indexName };
        return
            new GetIndexResponse(
                indices, getTestMappings(indexName), getTestAliases(indexName), getTestSettings(indexName),
                ImmutableOpenMap.of()
            );
    }

    private static GetIndexResponse getResponseWithDefaultSettings() {
        String indexName = "my_index";
        String indices[] = { indexName };
        ImmutableOpenMap.Builder<String, Settings> defaultSettings = ImmutableOpenMap.builder();
        Settings.Builder indexDefaultSettings = Settings.builder();
        indexDefaultSettings.put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), "1s");
        defaultSettings.put(indexName, indexDefaultSettings.build());
        return
            new GetIndexResponse(
                indices, getTestMappings(indexName), getTestAliases(indexName), getTestSettings(indexName),
                defaultSettings.build()
            );
    }

    public void testCanDecode622Response() throws IOException {
        StreamInput si = StreamInput.wrap(Base64.getDecoder().decode(TEST_6_3_0_RESPONSE_BYTES));
        si.setVersion(Version.V_6_3_0);
        GetIndexResponse response = new GetIndexResponse();
        response.readFrom(si);

        Assert.assertEquals(TEST_6_3_0_RESPONSE_INSTANCE, response);
    }

    public void testCanOutput622Response() throws IOException {
        GetIndexResponse responseWithExtraFields = getResponseWithDefaultSettings();
        BytesStreamOutput bso = new BytesStreamOutput();
        bso.setVersion(Version.V_6_3_0);
        responseWithExtraFields.writeTo(bso);
        String base64OfResponse = Base64.getEncoder().encodeToString(BytesReference.toBytes(bso.bytes()));

        Assert.assertEquals(TEST_6_3_0_RESPONSE_BYTES, base64OfResponse);
    }
}
