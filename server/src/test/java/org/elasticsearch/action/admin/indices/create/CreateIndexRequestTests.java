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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;

public class CreateIndexRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("foo");
        String mapping = Strings.toString(JsonXContent.contentBuilder().startObject().startObject("_doc").endObject().endObject());
        request.mapping(mapping);

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            request.writeTo(output);

            try (StreamInput in = output.bytes().streamInput()) {
                CreateIndexRequest serialized = new CreateIndexRequest(in);
                assertEquals(request.index(), serialized.index());
                assertEquals("{\"_doc\":{}}", serialized.mappings());
            }
        }
    }

    public void testTopLevelKeys() {
        String createIndex =
                "{\n"
                + "  \"FOO_SHOULD_BE_ILLEGAL_HERE\": {\n"
                + "    \"BAR_IS_THE_SAME\": 42\n"
                + "  },\n"
                + "  \"mappings\": {\n"
                + "    \"test\": {\n"
                + "      \"properties\": {\n"
                + "        \"field1\": {\n"
                + "          \"type\": \"text\"\n"
                + "       }\n"
                + "     }\n"
                + "    }\n"
                + "  }\n"
                + "}";

        CreateIndexRequest request = new CreateIndexRequest();
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class,
                () -> {request.source(createIndex, XContentType.JSON);});
        assertEquals("unknown key [FOO_SHOULD_BE_ILLEGAL_HERE] for create index", e.getMessage());
    }

    public void testMappingKeyedByType() throws IOException {
        CreateIndexRequest request1 = new CreateIndexRequest("foo");
        CreateIndexRequest request2 = new CreateIndexRequest("bar");
        {
            XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            builder.startObject().startObject("properties")
                .startObject("field1")
                    .field("type", "text")
                .endObject()
                .startObject("field2")
                    .startObject("properties")
                        .startObject("field21")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject().endObject();
            request1.mapping(builder);
            builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            builder.startObject().startObject("_doc")
                .startObject("properties")
                    .startObject("field1")
                        .field("type", "text")
                    .endObject()
                    .startObject("field2")
                        .startObject("properties")
                            .startObject("field21")
                                .field("type", "keyword")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject().endObject();
            request2.mapping(builder);
            assertEquals(request1.mappings(), request2.mappings());
        }
    }

    public void testSettingsType() throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        builder.startObject().startArray("settings").endArray().endObject();

        CreateIndexRequest parsedCreateIndexRequest = new CreateIndexRequest();
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> parsedCreateIndexRequest.source(builder));
        assertThat(e.getMessage(), equalTo("key [settings] must be an object"));
    }

    public static void assertMappingsEqual(Map<String, String> expected, Map<String, String> actual) throws IOException {
        assertEquals(expected.keySet(), actual.keySet());

        for (Map.Entry<String, String> expectedEntry : expected.entrySet()) {
            String expectedValue = expectedEntry.getValue();
            String actualValue = actual.get(expectedEntry.getKey());
            try (XContentParser expectedJson = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE, expectedValue);
                 XContentParser actualJson = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE, actualValue)){
                assertEquals(expectedJson.map(), actualJson.map());
            }
        }
    }

    public static void assertAliasesEqual(Set<Alias> expected, Set<Alias> actual) throws IOException {
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
}
