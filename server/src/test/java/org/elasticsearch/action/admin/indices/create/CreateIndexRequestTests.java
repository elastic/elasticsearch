/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.RandomCreateIndexGenerator;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;

public class CreateIndexRequestTests extends AbstractWireSerializingTestCase<CreateIndexRequest> {

    public void testSimpleSerialization() throws IOException {
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
        String createIndex = """
            {
              "FOO_SHOULD_BE_ILLEGAL_HERE": {
                "BAR_IS_THE_SAME": 42
              },
              "mappings": {
                "test": {
                  "properties": {
                    "field1": {
                      "type": "text"
                   }
                 }
                }
              }
            }""";

        CreateIndexRequest request = new CreateIndexRequest();
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> { request.source(createIndex, XContentType.JSON); }
        );
        assertEquals("unknown key [FOO_SHOULD_BE_ILLEGAL_HERE] for create index", e.getMessage());
    }

    public void testMappingKeyedByType() throws IOException {
        CreateIndexRequest request1 = new CreateIndexRequest("foo");
        CreateIndexRequest request2 = new CreateIndexRequest("bar");
        {
            XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            builder.startObject()
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
                .endObject();
            request1.mapping(builder);
            builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            builder.startObject()
                .startObject("_doc")
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
                .endObject()
                .endObject();
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

    public void testAlias() throws IOException {
        XContentBuilder aliases1 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("aliases")
            .startObject("filtered-data")
            .startObject("bool")
            .startObject("filter")
            .startObject("term")
            .field("a", "b")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new CreateIndexRequest().source(aliases1));
        assertThat(e.getMessage(), containsString("Unknown field [bool] in alias [filtered-data]"));

        XContentBuilder aliases2 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("aliases")
            .startObject("filtered-data")
            .startArray("filter")
            .startObject()
            .startObject("term")
            .field("a", "b")
            .endObject()
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .endObject();
        e = expectThrows(IllegalArgumentException.class, () -> new CreateIndexRequest().source(aliases2));
        assertThat(e.getMessage(), containsString("Unknown token [START_ARRAY] in alias [filtered-data]"));
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

    @Override
    protected Writeable.Reader<CreateIndexRequest> instanceReader() {
        return CreateIndexRequest::new;
    }

    @Override
    protected CreateIndexRequest createTestInstance() {
        return RandomCreateIndexGenerator.randomCreateIndexRequest();
    }

    @Override
    protected CreateIndexRequest mutateInstance(CreateIndexRequest instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
