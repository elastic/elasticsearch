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
package org.elasticsearch.client.indices;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.client.AbstractRequestTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class PutIndexTemplateRequestTests extends AbstractRequestTestCase<PutIndexTemplateRequest,
    org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest> {

    public void testValidateErrorMessage() throws Exception {
        expectThrows(IllegalArgumentException.class, () -> new PutIndexTemplateRequest(null, null));
        expectThrows(IllegalArgumentException.class, () -> new PutIndexTemplateRequest("test", List.of("index")).name(null));
        Exception e = expectThrows(IllegalArgumentException.class, () -> new PutIndexTemplateRequest("test", null));
        assertThat(e.getMessage(), containsString("index patterns are missing"));
        e = expectThrows(IllegalArgumentException.class, () -> new PutIndexTemplateRequest("test", List.of()));
        assertThat(e.getMessage(), containsString("index patterns are missing"));
        new PutIndexTemplateRequest("test", List.of("index"));
    }

    @Override
    protected PutIndexTemplateRequest createClientTestInstance() {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest("test",
            List.of(ESTestCase.generateRandomStringArray(20, 100, false, false)));
        if (randomBoolean()) {
            request.version(randomInt());
        }
        if (randomBoolean()) {
            request.order(randomInt());
        }
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
                request.mapping(XContentFactory.jsonBuilder().startObject()
                    .startObject("_doc")
                    .startObject("properties")
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
    protected org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest doParseToServerInstance(
        XContentParser parser) throws IOException {
        return new org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest("test").source(parser.map());
    }

    @Override
    protected void assertInstances(org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest serverInstance,
                                   PutIndexTemplateRequest clientTestInstance) {
        assertNotSame(serverInstance, clientTestInstance);
        assertThat(serverInstance.version(), equalTo(clientTestInstance.version()));
        assertThat(serverInstance.order(), equalTo(clientTestInstance.order()));
        assertThat(serverInstance.patterns(), equalTo(clientTestInstance.patterns()));
        assertThat(serverInstance.aliases(), equalTo(clientTestInstance.aliases()));
        String mapping = null;
        if (clientTestInstance.mappings() != null) {
            try {
                mapping = XContentHelper.convertToJson(clientTestInstance.mappings(), false, XContentType.JSON);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        assertThat(serverInstance.mappings(), equalTo(mapping));
        assertThat(serverInstance.settings(), equalTo(clientTestInstance.settings()));
    }

}
