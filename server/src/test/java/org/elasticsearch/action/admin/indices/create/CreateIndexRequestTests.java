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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;

public class CreateIndexRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("foo");
        String mapping = JsonXContent.contentBuilder().startObject().startObject("type").endObject().endObject().string();
        request.mapping("my_type", mapping, XContentType.JSON);

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            request.writeTo(output);

            try (StreamInput in = output.bytes().streamInput()) {
                CreateIndexRequest serialized = new CreateIndexRequest();
                serialized.readFrom(in);
                assertEquals(request.index(), serialized.index());
                assertEquals(mapping, serialized.mappings().get("my_type"));
            }
        }
    }

    public void testToXContent() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("foo");

        String mapping = JsonXContent.contentBuilder().startObject().startObject("type").endObject().endObject().string();
        request.mapping("my_type", mapping, XContentType.JSON);

        Alias alias = new Alias("test_alias");
        alias.routing("1");
        alias.filter("{\"term\":{\"year\":2016}}");
        request.alias(alias);

        Settings.Builder settings = Settings.builder();
        settings.put(SETTING_NUMBER_OF_SHARDS, 10);
        request.settings(settings);

        String actualRequestBody = Strings.toString(request);

        String expectedRequestBody = "{\"settings\":{\"index\":{\"number_of_shards\":\"10\"}}," +
            "\"mappings\":{\"my_type\":{\"type\":{}}}," +
            "\"aliases\":{\"test_alias\":{\"filter\":{\"term\":{\"year\":2016}},\"routing\":\"1\"}}}";

        assertEquals(expectedRequestBody, actualRequestBody);
    }

    public void testToAndFromXContent() throws IOException {

        final CreateIndexRequest createIndexRequest = createTestItem();

        boolean humanReadable = randomBoolean();
        final XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(createIndexRequest, xContentType, EMPTY_PARAMS, humanReadable);

        CreateIndexRequest parsedCreateIndexRequest = new CreateIndexRequest(createIndexRequest.index());
        parsedCreateIndexRequest.source(originalBytes, xContentType);

        assertMappingsEqual(createIndexRequest.mappings(), parsedCreateIndexRequest.mappings());
        assertAliasesEqual(createIndexRequest.aliases(), parsedCreateIndexRequest.aliases());
        assertEquals(createIndexRequest.settings(), parsedCreateIndexRequest.settings());
    }

    private void assertMappingsEqual(Map<String, String> expected, Map<String, String> actual) throws IOException {
        assertEquals(expected.keySet(), actual.keySet());

        for (Map.Entry<String, String> expectedEntry : expected.entrySet()) {
            String expectedValue = expectedEntry.getValue();
            String actualValue = actual.get(expectedEntry.getKey());
            XContentParser expectedJson = createParser(XContentType.JSON.xContent(), expectedValue);
            XContentParser actualJson = createParser(XContentType.JSON.xContent(), actualValue);
            assertEquals(expectedJson.mapOrdered(), actualJson.mapOrdered());
        }
    }

    private static void assertAliasesEqual(Set<Alias> expected, Set<Alias> actual) throws IOException {
        assertEquals(expected, actual);

        for (Alias expectedAlias : expected) {
            for (Alias actualAlias : actual) {
                if (expectedAlias.equals(actualAlias)) {
                    // As Alias#equals only looks at name, we check the equality of the other Alias parameters here.
                    assertEquals(expectedAlias.filter(), actualAlias.filter());
                    assertEquals(expectedAlias.indexRouting(), actualAlias.indexRouting());
                    assertEquals(expectedAlias.searchRouting(), actualAlias.searchRouting());
                }
            }
        }
    }

    /**
     * Returns a random {@link CreateIndexRequest}.
     */
    private static CreateIndexRequest createTestItem() throws IOException {
        String index = randomAlphaOfLength(5);

        CreateIndexRequest request = new CreateIndexRequest(index);

        int aliasesNo = randomIntBetween(0, 2);
        for (int i = 0; i < aliasesNo; i++) {
            request.alias(randomAlias());
        }

        if (randomBoolean()) {
            String type = randomAlphaOfLength(5);
            request.mapping(type, randomMapping(type));
        }

        if (randomBoolean()) {
            request.settings(randomIndexSettings());
        }

        return request;
    }

    private static Settings randomIndexSettings() {
        Settings.Builder builder = Settings.builder();

        if (randomBoolean()) {
            int numberOfShards = randomIntBetween(1, 10);
            builder.put(SETTING_NUMBER_OF_SHARDS, numberOfShards);
        }

        if (randomBoolean()) {
            int numberOfReplicas = randomIntBetween(1, 10);
            builder.put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas);
        }

        return builder.build();
    }

    private static XContentBuilder randomMapping(String type) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject().startObject(type);

        randomMappingFields(builder, true);

        builder.endObject().endObject();
        return builder;
    }

    private static void randomMappingFields(XContentBuilder builder, boolean allowObjectField) throws IOException {
        builder.startObject("properties");

        int fieldsNo = randomIntBetween(0, 5);
        for (int i = 0; i < fieldsNo; i++) {
            builder.startObject(randomAlphaOfLength(5));

            if (allowObjectField && randomBoolean()) {
                randomMappingFields(builder, false);
            } else {
                builder.field("type", "text");
            }

            builder.endObject();
        }

        builder.endObject();
    }

    private static Alias randomAlias() {
        Alias alias = new Alias(randomAlphaOfLength(5));

        if (randomBoolean()) {
            if (randomBoolean()) {
                alias.routing(randomAlphaOfLength(5));
            } else {
                if (randomBoolean()) {
                    alias.indexRouting(randomAlphaOfLength(5));
                }
                if (randomBoolean()) {
                    alias.searchRouting(randomAlphaOfLength(5));
                }
            }
        }

        if (randomBoolean()) {
            alias.filter("{\"term\":{\"year\":2016}}");
        }

        return alias;
    }
}
