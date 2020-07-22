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

import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class GetAliasesResponseTests extends AbstractXContentTestCase<GetAliasesResponse> {

    @Override
    protected GetAliasesResponse createTestInstance() {
        RestStatus status = randomFrom(RestStatus.OK, RestStatus.NOT_FOUND);
        String errorMessage = RestStatus.OK == status ? null : randomAlphaOfLengthBetween(5, 10);
        return new GetAliasesResponse(status, errorMessage, createIndicesAliasesMap(0, 5));
    }

    private static Map<String, Set<AliasMetadata>> createIndicesAliasesMap(int min, int max) {
        Map<String, Set<AliasMetadata>> map = new HashMap<>();
        int indicesNum = randomIntBetween(min, max);
        for (int i = 0; i < indicesNum; i++) {
            String index = randomAlphaOfLength(5);
            Set<AliasMetadata> aliasMetadata = new HashSet<>();
            int aliasesNum = randomIntBetween(0, 3);
            for (int alias = 0; alias < aliasesNum; alias++) {
                aliasMetadata.add(createAliasMetadata());
            }
            map.put(index, aliasMetadata);
        }
        return map;
    }

    public static AliasMetadata createAliasMetadata() {
        AliasMetadata.Builder builder = AliasMetadata.builder(randomAlphaOfLengthBetween(3, 10));
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
                || p.contains(".filter"); // do not insert random data into AliasMetadata#filter
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected void assertEqualInstances(GetAliasesResponse expectedInstance, GetAliasesResponse newInstance) {
        assertEquals(expectedInstance.getAliases(), newInstance.getAliases());
        assertEquals(expectedInstance.status(), newInstance.status());
        assertEquals(expectedInstance.getError(), newInstance.getError());
        assertNull(expectedInstance.getException());
        assertNull(newInstance.getException());
    }

    public void testFromXContentWithElasticsearchException() throws IOException {
        String xContent =
                "{" +
                "  \"error\": {" +
                "    \"root_cause\": [" +
                "      {" +
                "        \"type\": \"index_not_found_exception\"," +
                "        \"reason\": \"no such index [index]\"," +
                "        \"resource.type\": \"index_or_alias\"," +
                "        \"resource.id\": \"index\"," +
                "        \"index_uuid\": \"_na_\"," +
                "        \"index\": \"index\"" +
                "      }" +
                "    ]," +
                "    \"type\": \"index_not_found_exception\"," +
                "    \"reason\": \"no such index [index]\"," +
                "    \"resource.type\": \"index_or_alias\"," +
                "    \"resource.id\": \"index\"," +
                "    \"index_uuid\": \"_na_\"," +
                "    \"index\": \"index\"" +
                "  }," +
                "  \"status\": 404" +
                "}";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
            GetAliasesResponse getAliasesResponse = GetAliasesResponse.fromXContent(parser);
            assertThat(getAliasesResponse.getError(), nullValue());
            assertThat(getAliasesResponse.status(), equalTo(RestStatus.NOT_FOUND));
            assertThat(getAliasesResponse.getException().getMessage(),
                    equalTo("Elasticsearch exception [type=index_not_found_exception, reason=no such index [index]]"));
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
            assertThat(getAliasesResponse.getError(), equalTo("alias [aa] missing"));
            assertThat(getAliasesResponse.getException(), nullValue());
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
            assertThat(response.getError(), equalTo("alias [something] missing"));
            assertThat(response.getAliases().size(), equalTo(1));
            assertThat(response.getAliases().get(index).size(), equalTo(1));
            AliasMetadata aliasMetadata = response.getAliases().get(index).iterator().next();
            assertThat(aliasMetadata.alias(), equalTo("alias"));
            assertThat(response.getException(), nullValue());
        }
    }
}
