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

package org.elasticsearch.action.admin.indices.mapping.put;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.RandomCreateIndexGenerator;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;

public class PutMappingRequestTests extends ESTestCase {

    public void testValidation() {
        PutMappingRequest r = new PutMappingRequest("myindex");
        ActionRequestValidationException ex = r.validate();
        assertNotNull("type validation should fail", ex);
        assertTrue(ex.getMessage().contains("type is missing"));

        r.type("");
        ex = r.validate();
        assertNotNull("type validation should fail", ex);
        assertTrue(ex.getMessage().contains("type is empty"));

        r.type("mytype");
        ex = r.validate();
        assertNotNull("source validation should fail", ex);
        assertTrue(ex.getMessage().contains("source is missing"));

        r.source("", XContentType.JSON);
        ex = r.validate();
        assertNotNull("source validation should fail", ex);
        assertTrue(ex.getMessage().contains("source is empty"));

        r.source("somevalidmapping", XContentType.JSON);
        ex = r.validate();
        assertNull("validation should succeed", ex);

        r.setConcreteIndex(new Index("foo", "bar"));
        ex = r.validate();
        assertNotNull("source validation should fail", ex);
        assertEquals(ex.getMessage(),
            "Validation Failed: 1: either concrete index or unresolved indices can be set," +
                " concrete index: [[foo/bar]] and indices: [myindex];");
    }

    public void testBuildFromSimplifiedDef() {
        // test that method rejects input where input varargs fieldname/properites are not paired correctly
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> PutMappingRequest.buildFromSimplifiedDef("type", "only_field"));
        assertEquals("mapping source must be pairs of fieldnames and properties definition.", e.getMessage());
    }

    public void testPutMappingRequestSerialization() throws IOException {
        PutMappingRequest request = new PutMappingRequest("foo");
        String mapping = Strings.toString(YamlXContent.contentBuilder().startObject().field("foo", "bar").endObject());
        request.source(mapping, XContentType.YAML);
        assertEquals(XContentHelper.convertToJson(new BytesArray(mapping), false, XContentType.YAML), request.source());

        final Version version = randomFrom(Version.CURRENT, Version.V_5_3_0, Version.V_5_3_1, Version.V_5_3_2, Version.V_5_4_0);
        try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
            bytesStreamOutput.setVersion(version);
            request.writeTo(bytesStreamOutput);
            try (StreamInput in = StreamInput.wrap(bytesStreamOutput.bytes().toBytesRef().bytes)) {
                in.setVersion(version);
                PutMappingRequest serialized = new PutMappingRequest();
                serialized.readFrom(in);

                String source = serialized.source();
                assertEquals(XContentHelper.convertToJson(new BytesArray(mapping), false, XContentType.YAML), source);
            }
        }
    }

    public void testToXContent() throws IOException {
        PutMappingRequest request = new PutMappingRequest("foo");
        request.type("my_type");

        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("properties");
        mapping.startObject("email");
        mapping.field("type", "text");
        mapping.endObject();
        mapping.endObject();
        mapping.endObject();
        request.source(mapping);

        String actualRequestBody = Strings.toString(request);
        String expectedRequestBody = "{\"properties\":{\"email\":{\"type\":\"text\"}}}";
        assertEquals(expectedRequestBody, actualRequestBody);
    }

    public void testToXContentWithEmptySource() throws IOException {
        PutMappingRequest request = new PutMappingRequest("foo");
        request.type("my_type");

        String actualRequestBody = Strings.toString(request);
        String expectedRequestBody = "{}";
        assertEquals(expectedRequestBody, actualRequestBody);
    }

    public void testToAndFromXContent() throws IOException {

        final PutMappingRequest putMappingRequest = createTestItem();

        boolean humanReadable = randomBoolean();
        final XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(putMappingRequest, xContentType, EMPTY_PARAMS, humanReadable);

        PutMappingRequest parsedPutMappingRequest = new PutMappingRequest();
        parsedPutMappingRequest.source(originalBytes, xContentType);

        assertMappingsEqual(putMappingRequest.source(), parsedPutMappingRequest.source());
    }

    private void assertMappingsEqual(String expected, String actual) throws IOException {

        try (XContentParser expectedJson = createParser(XContentType.JSON.xContent(), expected);
            XContentParser actualJson = createParser(XContentType.JSON.xContent(), actual)) {
            assertEquals(expectedJson.mapOrdered(), actualJson.mapOrdered());
        }
    }

    /**
     * Returns a random {@link PutMappingRequest}.
     */
    private static PutMappingRequest createTestItem() throws IOException {
        String index = randomAlphaOfLength(5);

        PutMappingRequest request = new PutMappingRequest(index);

        String type = randomAlphaOfLength(5);
        request.type(type);
        request.source(randomMapping());

        return request;
    }

    private static XContentBuilder randomMapping() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        if (randomBoolean()) {
            RandomCreateIndexGenerator.randomMappingFields(builder, true);
        }

        builder.endObject();
        return builder;
    }
}
