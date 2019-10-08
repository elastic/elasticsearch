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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class PutIndexTemplateRequestTests extends AbstractXContentTestCase<PutIndexTemplateRequest> {

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

    public void testMappingKeyedByType() throws IOException {
        PutIndexTemplateRequest request1 = new PutIndexTemplateRequest("foo");
        PutIndexTemplateRequest request2 = new PutIndexTemplateRequest("bar");
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
            request1.mapping("type1", builder);
            builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            builder.startObject().startObject("type1")
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
            request2.mapping("type1", builder);
            assertEquals(request1.mappings(), request2.mappings());
        }
        {
            request1 = new PutIndexTemplateRequest("foo");
            request2 = new PutIndexTemplateRequest("bar");
            String nakedMapping = "{\"properties\": {\"foo\": {\"type\": \"integer\"}}}";
            request1.mapping("type2", nakedMapping, XContentType.JSON);
            request2.mapping("type2", "{\"type2\": " + nakedMapping + "}", XContentType.JSON);
            assertEquals(request1.mappings(), request2.mappings());
        }
        {
            request1 = new PutIndexTemplateRequest("foo");
            request2 = new PutIndexTemplateRequest("bar");
            Map<String , Object> nakedMapping = MapBuilder.<String, Object>newMapBuilder()
                    .put("properties", MapBuilder.<String, Object>newMapBuilder()
                            .put("bar", MapBuilder.<String, Object>newMapBuilder()
                                    .put("type", "scaled_float")
                                    .put("scaling_factor", 100)
                            .map())
                    .map())
            .map();
            request1.mapping("type3", nakedMapping);
            request2.mapping("type3", MapBuilder.<String, Object>newMapBuilder().put("type3", nakedMapping).map());
            assertEquals(request1.mappings(), request2.mappings());
        }
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
            // some ASCII or Latin-1 control characters, especially newline, can lead to
            // problems with yaml parsers, that's why we filter them here (see #30911)
            Alias alias = new Alias(randomRealisticUnicodeOfLengthBetween(1, 10).replaceAll("\\p{Cc}", ""));
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
