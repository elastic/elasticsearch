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

package org.elasticsearch.action.admin.indices.alias;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.AliasMetaData.Builder;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class GetAliasesResponseTests extends AbstractStreamableXContentTestCase<GetAliasesResponse> {

    @Override
    protected GetAliasesResponse doParseInstance(XContentParser parser) throws IOException {
        return GetAliasesResponse.fromXContent(parser);
    }

    @Override
    protected GetAliasesResponse createTestInstance() {
        return createTestItem();
    }

    @Override
    protected GetAliasesResponse createBlankInstance() {
        return new GetAliasesResponse(null);
    }

    @Override
    protected GetAliasesResponse mutateInstance(GetAliasesResponse response) {
        switch (randomInt(2)) {
        case 0:
            return new GetAliasesResponse(mutateAliases(response.getAliases()), response.status(), response.errorMessage());
        case 1:
            return new GetAliasesResponse(response.getAliases(),
                    randomValueOtherThan(response.status(), () -> randomFrom(RestStatus.values())), response.errorMessage());
        case 2:
            if (response.status() == RestStatus.OK) {
                return new GetAliasesResponse(response.getAliases(),
                        randomValueOtherThan(response.status(), () -> randomFrom(RestStatus.values())), randomAlphaOfLengthBetween(5, 100));
            }
            return new GetAliasesResponse(response.getAliases(), response.status(), randomAlphaOfLengthBetween(5, 100));
        default:
            throw new UnsupportedOperationException();
        }
    }

    private static ImmutableOpenMap<String, List<AliasMetaData>> mutateAliases(ImmutableOpenMap<String, List<AliasMetaData>> aliases) {
        if (aliases.isEmpty()) {
            return createIndicesAliasesMap(1, 3).build();
        }

        if (randomBoolean()) {
            ImmutableOpenMap.Builder<String, List<AliasMetaData>> builder = ImmutableOpenMap.builder(aliases);
            ImmutableOpenMap<String, List<AliasMetaData>> list = createIndicesAliasesMap(1, 2).build();
            list.forEach(e -> builder.put(e.key, e.value));
            return builder.build();
        }

        Set<String> indices = new HashSet<>();
        Iterator<String> keys = aliases.keysIt();
        while (keys.hasNext()) {
            indices.add(keys.next());
        }

        List<String> indicesToBeModified = randomSubsetOf(randomIntBetween(1, indices.size()), indices);
        ImmutableOpenMap.Builder<String, List<AliasMetaData>> builder = ImmutableOpenMap.builder();

        for (String index : indices) {
            List<AliasMetaData> list = new ArrayList<>(aliases.get(index));
            if (indicesToBeModified.contains(index)) {
                if (randomBoolean() || list.isEmpty()) {
                    list.add(createAliasMetaData());
                } else {
                    int aliasIndex = randomInt(list.size() - 1);
                    AliasMetaData aliasMetaData = list.get(aliasIndex);
                    list.add(aliasIndex, mutateAliasMetaData(aliasMetaData));
                }
            }
            builder.put(index, list);
        }
        return builder.build();
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return p -> p.equals("") // do not add elements at the top-level as any element at this level is parsed as a new index
                || p.endsWith(".aliases") // do not add new alias
                || p.contains(".filter"); // do not insert random data into AliasMetaData#filter
    }

    private static GetAliasesResponse createTestItem() {
        RestStatus status = randomFrom(RestStatus.OK, RestStatus.NOT_FOUND);
        String errorMessage = RestStatus.OK == status ? null : randomAlphaOfLengthBetween(5, 10);
        return new GetAliasesResponse(createIndicesAliasesMap(0, 5).build(), status, errorMessage);
    }

    private static ImmutableOpenMap.Builder<String, List<AliasMetaData>> createIndicesAliasesMap(int min, int max) {
        ImmutableOpenMap.Builder<String, List<AliasMetaData>> builder = ImmutableOpenMap.builder();
        int indicesNum = randomIntBetween(min, max);
        for (int i = 0; i < indicesNum; i++) {
            String index = randomAlphaOfLength(5);
            List<AliasMetaData> aliasMetaData = new ArrayList<>();
            int aliasesNum = randomIntBetween(0, 3);
            for (int alias = 0; alias < aliasesNum; alias++) {
                aliasMetaData.add(createAliasMetaData());
            }
            builder.put(index, aliasMetaData);
        }
        return builder;
    }

    private static AliasMetaData createAliasMetaData() {
        Builder builder = AliasMetaData.builder(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) {
            builder.routing(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.searchRouting(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.indexRouting(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.filter("{\"term\":{\"year\":2016}}");
        }
        return builder.build();
    }

    private static AliasMetaData mutateAliasMetaData(AliasMetaData alias) {
        boolean changeAlias = randomBoolean();
        AliasMetaData.Builder builder = AliasMetaData.builder(changeAlias ? randomAlphaOfLengthBetween(2, 5) : alias.getAlias());
        builder.searchRouting(alias.searchRouting());
        builder.indexRouting(alias.indexRouting());
        builder.filter(alias.filter());

        if (false == changeAlias) {
            if (randomBoolean()) {
                builder.searchRouting(alias.searchRouting() + randomAlphaOfLengthBetween(1, 3));
            } else {
                builder.indexRouting(alias.indexRouting() + randomAlphaOfLengthBetween(1, 3));
            }
        }
        return builder.build();
    }

    public void testFromXContentWithMissingAndFoundAlias() throws IOException {
        String xContent =
                "{" +
                "  \"error\": \"alias [something] missing\"," +
                "  \"status\": 404," +
                "  \"index\": {" +
                "    \"aliases\": {" +
                "      \"alias\": {}" +
                "    }" +
                "  }" +
                "}";
        final String index = "index";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
            GetAliasesResponse response = GetAliasesResponse.fromXContent(parser);
            assertThat(response.status(), equalTo(RestStatus.NOT_FOUND));
            assertThat(response.errorMessage(), equalTo("alias [something] missing"));
            assertThat(response.getAliases().size(), equalTo(1));
            assertThat(response.getAliases().get(index).size(), equalTo(1));
            assertThat(response.getAliases().get(index).get(0), notNullValue());
            assertThat(response.getAliases().get(index).get(0).alias(), equalTo("alias"));
        }
    }

    public void testFromXContentWithElasticsearchException() throws IOException {
        String xContent =
                "{" +
                "  \"error\": {" +
                "    \"root_cause\": [" +
                "      {" +
                "        \"type\": \"index_not_found_exception\"," +
                "        \"reason\": \"no such index\"," +
                "        \"resource.type\": \"index_or_alias\"," +
                "        \"resource.id\": \"index\"," +
                "        \"index_uuid\": \"_na_\"," +
                "        \"index\": \"index\"" +
                "      }" +
                "    ]," +
                "    \"type\": \"index_not_found_exception\"," +
                "    \"reason\": \"no such index\"," +
                "    \"resource.type\": \"index_or_alias\"," +
                "    \"resource.id\": \"index\"," +
                "    \"index_uuid\": \"_na_\"," +
                "    \"index\": \"index\"" +
                "  }," +
                "  \"status\": 404" +
                "}";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
            GetAliasesResponse getAliasesResponse = GetAliasesResponse.fromXContent(parser);
            assertThat(getAliasesResponse.status(), equalTo(RestStatus.NOT_FOUND));
            assertThat(getAliasesResponse.errorMessage(),
                    equalTo("Elasticsearch exception [type=index_not_found_exception, reason=no such index]"));
        }
    }

    public void testFromXContentWithNoAliasFound() throws IOException {
        String xContent =
                "{" +
                "  \"error\": \"alias [aa] missing\"," +
                "  \"status\": 404" +
                "}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
            GetAliasesResponse getAliasesResponse = GetAliasesResponse.fromXContent(parser);
            assertThat(getAliasesResponse.status(), equalTo(RestStatus.NOT_FOUND));
            assertThat(getAliasesResponse.errorMessage(), equalTo("alias [aa] missing"));
        }
    }

    public void testSerializationBwc() throws IOException {
        final Version targetNodeVersion = randomVersion(random());
        final GetAliasesResponse outResponse = createTestInstance();

        try (final ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
                final OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);) {
            out.setVersion(targetNodeVersion);
            outResponse.writeTo(out);

            try (final ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
                    final InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);) {
                final GetAliasesResponse inResponse = new GetAliasesResponse(null);
                in.setVersion(targetNodeVersion);
                inResponse.readFrom(in);

                assertThat(outResponse.getAliases(), equalTo(inResponse.getAliases()));
                if (targetNodeVersion.onOrAfter(Version.V_7_0_0_alpha1)) {
                    // if (targetNodeVersion.onOrAfter(Version.V_6_4_0_ID)) {
                    assertThat(outResponse.status(), equalTo(inResponse.status()));
                    assertThat(outResponse.errorMessage(), equalTo(inResponse.errorMessage()));
                } else {
                    assertThat(inResponse.status(), equalTo(RestStatus.OK));
                    assertThat(inResponse.errorMessage(), equalTo(null));
                }
            }
        }
    }

}
