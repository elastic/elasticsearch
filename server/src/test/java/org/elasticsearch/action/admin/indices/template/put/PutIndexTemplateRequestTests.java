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
package org.elasticsearch.action.admin.indices.template.put;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class PutIndexTemplateRequestTests extends AbstractXContentTestCase<PutIndexTemplateRequest> {

    // bwc for #21009
    public void testPutIndexTemplateRequest510() throws IOException {
        PutIndexTemplateRequest putRequest = new PutIndexTemplateRequest("test");
        putRequest.patterns(Collections.singletonList("test*"));
        putRequest.order(5);

        PutIndexTemplateRequest multiPatternRequest = new PutIndexTemplateRequest("test");
        multiPatternRequest.patterns(Arrays.asList("test*", "*test2", "*test3*"));
        multiPatternRequest.order(5);

        // These bytes were retrieved by Base64 encoding the result of the above with 5_0_0 code.
        // Note: Instead of a list for the template, in 5_0_0 the element was provided as a string.
        String putRequestBytes = "ADwDAAR0ZXN0BXRlc3QqAAAABQAAAAAAAA==";
        BytesArray bytes = new BytesArray(Base64.getDecoder().decode(putRequestBytes));

        try (StreamInput in = bytes.streamInput()) {
            in.setVersion(Version.V_5_0_0);
            PutIndexTemplateRequest readRequest = new PutIndexTemplateRequest();
            readRequest.readFrom(in);
            assertEquals(putRequest.patterns(), readRequest.patterns());
            assertEquals(putRequest.order(), readRequest.order());

            BytesStreamOutput output = new BytesStreamOutput();
            output.setVersion(Version.V_5_0_0);
            readRequest.writeTo(output);
            assertEquals(bytes.toBytesRef(), output.bytes().toBytesRef());

            // test that multi templates are reverse-compatible.
            // for the bwc case, if multiple patterns, use only the first pattern seen.
            output.reset();
            multiPatternRequest.writeTo(output);
            assertEquals(bytes.toBytesRef(), output.bytes().toBytesRef());
        }
    }

    public void testPutIndexTemplateRequestSerializationXContent() throws IOException {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest("foo");
        String mapping = Strings.toString(YamlXContent.contentBuilder().startObject().field("foo", "bar").endObject());
        request.patterns(Collections.singletonList("foo"));
        request.mapping("bar", mapping, XContentType.YAML);
        assertNotEquals(mapping, request.mappings().get("bar"));
        assertEquals(XContentHelper.convertToJson(new BytesArray(mapping), false, XContentType.YAML), request.mappings().get("bar"));

        final Version version = randomFrom(Version.CURRENT, Version.V_5_3_0, Version.V_5_3_1, Version.V_5_3_2, Version.V_5_4_0);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            request.writeTo(out);

            try (StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes)) {
                in.setVersion(version);
                PutIndexTemplateRequest serialized = new PutIndexTemplateRequest();
                serialized.readFrom(in);
                assertEquals(XContentHelper.convertToJson(new BytesArray(mapping), false, XContentType.YAML),
                    serialized.mappings().get("bar"));
            }
        }
    }

    public void testPutIndexTemplateRequestSerializationXContentBwc() throws IOException {
        final byte[] data = Base64.getDecoder().decode("ADwDAANmb28IdGVtcGxhdGUAAAAAAAABA2Jhcg8tLS0KZm9vOiAiYmFyIgoAAAAAAAAAAAAAAAA=");
        final Version version = randomFrom(Version.V_5_0_0, Version.V_5_0_1, Version.V_5_0_2,
            Version.V_5_1_1, Version.V_5_1_2, Version.V_5_2_0);
        try (StreamInput in = StreamInput.wrap(data)) {
            in.setVersion(version);
            PutIndexTemplateRequest request = new PutIndexTemplateRequest();
            request.readFrom(in);
            String mapping = Strings.toString(YamlXContent.contentBuilder().startObject().field("foo", "bar").endObject());
            assertNotEquals(mapping, request.mappings().get("bar"));
            assertEquals(XContentHelper.convertToJson(new BytesArray(mapping), false, XContentType.YAML), request.mappings().get("bar"));
            assertEquals("foo", request.name());
            assertEquals("template", request.patterns().get(0));
        }
    }

    public void testValidateErrorMessage() throws Exception {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        ActionRequestValidationException withoutNameAndPattern = request.validate();
        assertThat(withoutNameAndPattern.getMessage(), containsString("name is missing"));
        assertThat(withoutNameAndPattern.getMessage(), containsString("index patterns are missing"));

        request.name("foo");
        ActionRequestValidationException withoutIndexPatterns = request.validate();
        assertThat(withoutIndexPatterns.validationErrors(), hasSize(1));
        assertThat(withoutIndexPatterns.getMessage(), containsString("index patterns are missing"));

        request.patterns(Collections.singletonList("test-*"));
        ActionRequestValidationException noError = request.validate();
        assertThat(noError, is(nullValue()));
    }

    @Override
    protected PutIndexTemplateRequest createTestInstance() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest();
        request.name("test");
        if (randomBoolean()) {
            request.version(randomInt());
        }
        if (randomBoolean()) {
            request.order(randomInt());
        }
        request.patterns(Arrays.asList(generateRandomStringArray(20, 100, false, false)));
        int numAlias = between(0, 5);
        for (int i = 0; i < numAlias; i++) {
            Alias alias = new Alias(randomRealisticUnicodeOfLengthBetween(1, 10));
            if (randomBoolean()) {
                alias.indexRouting(randomRealisticUnicodeOfLengthBetween(1, 10));
            }
            if (randomBoolean()) {
                alias.searchRouting(randomRealisticUnicodeOfLengthBetween(1, 10));
            }
            request.alias(alias);
        }
        if (randomBoolean()) {
            try {
                request.mapping("doc", XContentFactory.jsonBuilder().startObject()
                    .startObject("doc").startObject("properties")
                    .startObject("field-" + randomInt()).field("type", randomFrom("keyword", "text")).endObject()
                    .endObject().endObject().endObject());
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }
        if (randomBoolean()) {
            request.settings(Settings.builder().put("setting1", randomLong()).put("setting2", randomTimeValue()).build());
        }
        return request;
    }

    @Override
    protected PutIndexTemplateRequest doParseInstance(XContentParser parser) throws IOException {
        return new PutIndexTemplateRequest().source(parser.map());
    }

    @Override
    protected void assertEqualInstances(PutIndexTemplateRequest expected, PutIndexTemplateRequest actual) {
        assertNotSame(expected, actual);
        assertThat(actual.version(), equalTo(expected.version()));
        assertThat(actual.order(), equalTo(expected.order()));
        assertThat(actual.patterns(), equalTo(expected.patterns()));
        assertThat(actual.aliases(), equalTo(expected.aliases()));
        assertThat(actual.mappings(), equalTo(expected.mappings()));
        assertThat(actual.settings(), equalTo(expected.settings()));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
