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

package org.elasticsearch.client;

import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;

public class GetAliasesResponseTests extends AbstractXContentTestCase<GetAliasesResponse> {

    @Override
    protected GetAliasesResponse createTestInstance() {
        RestStatus status = randomFrom(RestStatus.OK, RestStatus.NOT_FOUND);
        String errorMessage = RestStatus.OK == status ? null : randomAlphaOfLengthBetween(5, 10);
        return new GetAliasesResponse(status, errorMessage, createIndicesAliasesMap(0, 5));
    }

    private static Map<String, Set<AliasMetaData>> createIndicesAliasesMap(int min, int max) {
        Map<String, Set<AliasMetaData>> map = new HashMap<>();
        int indicesNum = randomIntBetween(min, max);
        for (int i = 0; i < indicesNum; i++) {
            String index = randomAlphaOfLength(5);
            Set<AliasMetaData> aliasMetaData = new HashSet<>();
            int aliasesNum = randomIntBetween(0, 3);
            for (int alias = 0; alias < aliasesNum; alias++) {
                aliasMetaData.add(createAliasMetaData());
            }
            map.put(index, aliasMetaData);
        }
        return map;
    }

    private static AliasMetaData createAliasMetaData() {
        AliasMetaData.Builder builder = AliasMetaData.builder(randomAlphaOfLengthBetween(3, 10));
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

    @Override
    protected GetAliasesResponse doParseInstance(XContentParser parser) throws IOException {
        return GetAliasesResponse.fromXContent(parser);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return p -> p.equals("") // do not add elements at the top-level as any element at this level is parsed as a new index
                || p.endsWith(".aliases") // do not add new alias
                || p.contains(".filter"); // do not insert random data into AliasMetaData#filter
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testEqualsAndHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createTestInstance(),
                response -> new GetAliasesResponse(response.status(), response.getErrorMessage(), response.getAliases()),
                GetAliasesResponseTests::mutateInstance);
    }

    private static GetAliasesResponse mutateInstance(GetAliasesResponse response) {
        switch (randomInt(2)) {
            case 0:
                return new GetAliasesResponse(response.status(), response.getErrorMessage(), mutateAliases(response.getAliases()));
            case 1:
                return new GetAliasesResponse(randomValueOtherThan(response.status(), () -> randomFrom(RestStatus.values())),
                        response.getErrorMessage(), response.getAliases());
            case 2:
                if (response.status() == RestStatus.OK) {
                    return new GetAliasesResponse(randomValueOtherThan(response.status(), () -> randomFrom(RestStatus.values())),
                            randomAlphaOfLengthBetween(5, 100), response.getAliases());
                }
                return new GetAliasesResponse(response.status(), randomAlphaOfLengthBetween(5, 100), response.getAliases());
            default:
                throw new UnsupportedOperationException();
        }
    }

    private static Map<String, Set<AliasMetaData>> mutateAliases(Map<String, Set<AliasMetaData>> aliases) {
        if (aliases.isEmpty()) {
            return createIndicesAliasesMap(1, 3);
        }

        if (randomBoolean()) {
            Map<String, Set<AliasMetaData>> updatedMap = new HashMap<>(aliases);
            Map<String, Set<AliasMetaData>> list = createIndicesAliasesMap(1, 2);
            list.forEach((key, value) -> updatedMap.put(key, value));
            return updatedMap;
        }

        Set<String> indices = new HashSet<>(aliases.keySet());

        List<String> indicesToBeModified = randomSubsetOf(randomIntBetween(1, indices.size()), indices);
        Map<String, Set<AliasMetaData>> map = new HashMap<>();

        for (String index : indices) {
            Set<AliasMetaData> set = new HashSet<>(aliases.get(index));
            if (indicesToBeModified.contains(index)) {
                if (randomBoolean() || set.isEmpty()) {
                    set.add(createAliasMetaData());
                } else {
                    AliasMetaData aliasMetaData = set.iterator().next();
                    set.add(mutateAliasMetaData(aliasMetaData));
                }
            }
            map.put(index, set);
        }
        return map;
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
            assertThat(getAliasesResponse.getErrorMessage(),
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
            assertThat(getAliasesResponse.getErrorMessage(), equalTo("alias [aa] missing"));
        }
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
            assertThat(response.getErrorMessage(), equalTo("alias [something] missing"));
            assertThat(response.getAliases().size(), equalTo(1));
            assertThat(response.getAliases().get(index).size(), equalTo(1));
            AliasMetaData aliasMetaData = response.getAliases().get(index).iterator().next();
            assertThat(aliasMetaData.alias(), equalTo("alias"));
        }
    }
}
