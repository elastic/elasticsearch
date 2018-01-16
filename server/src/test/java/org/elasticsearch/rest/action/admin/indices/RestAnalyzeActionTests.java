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
package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;

public class RestAnalyzeActionTests extends ESTestCase {

    public void testParseXContentForAnalyzeRequest() throws Exception {
        XContentParser content = createParser(XContentFactory.jsonBuilder()
            .startObject()
                .field("text", "THIS IS A TEST")
                .field("tokenizer", "keyword")
                .array("filter", "lowercase")
            .endObject());

        AnalyzeRequest analyzeRequest = new AnalyzeRequest("for test");

        RestAnalyzeAction.buildFromContent(content, analyzeRequest);

        assertThat(analyzeRequest.text().length, equalTo(1));
        assertThat(analyzeRequest.text(), equalTo(new String[]{"THIS IS A TEST"}));
        assertThat(analyzeRequest.tokenizer().name, equalTo("keyword"));
        assertThat(analyzeRequest.tokenFilters().size(), equalTo(1));
        for (AnalyzeRequest.NameOrDefinition filter : analyzeRequest.tokenFilters()) {
            assertThat(filter.name, equalTo("lowercase"));
        }
    }

    public void testParseXContentForAnalyzeRequestWithCustomFilters() throws Exception {
        XContentParser content = createParser(XContentFactory.jsonBuilder()
            .startObject()
                .field("text", "THIS IS A TEST")
                .field("tokenizer", "keyword")
                .startArray("filter")
                    .value("lowercase")
                    .startObject()
                        .field("type", "stop")
                        .array("stopwords", "foo", "buzz")
                    .endObject()
                .endArray()
                .startArray("char_filter")
                    .startObject()
                        .field("type", "mapping")
                        .array("mappings", "ph => f", "qu => q")
                    .endObject()
                .endArray()
                .field("normalizer", "normalizer")
            .endObject());

        AnalyzeRequest analyzeRequest = new AnalyzeRequest("for test");

        RestAnalyzeAction.buildFromContent(content, analyzeRequest);

        assertThat(analyzeRequest.text().length, equalTo(1));
        assertThat(analyzeRequest.text(), equalTo(new String[]{"THIS IS A TEST"}));
        assertThat(analyzeRequest.tokenizer().name, equalTo("keyword"));
        assertThat(analyzeRequest.tokenFilters().size(), equalTo(2));
        assertThat(analyzeRequest.tokenFilters().get(0).name, equalTo("lowercase"));
        assertThat(analyzeRequest.tokenFilters().get(1).definition, notNullValue());
        assertThat(analyzeRequest.charFilters().size(), equalTo(1));
        assertThat(analyzeRequest.charFilters().get(0).definition, notNullValue());
        assertThat(analyzeRequest.normalizer(), equalTo("normalizer"));
    }

    public void testParseXContentForAnalyzeRequestWithInvalidJsonThrowsException() throws Exception {
        RestAnalyzeAction action = new RestAnalyzeAction(Settings.EMPTY, mock(RestController.class));
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withContent(new BytesArray("{invalid_json}"), XContentType.JSON).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.handleRequest(request, null, null));
        assertThat(e.getMessage(), equalTo("Failed to parse request body"));
    }

    public void testParseXContentForAnalyzeRequestWithUnknownParamThrowsException() throws Exception {
        AnalyzeRequest analyzeRequest = new AnalyzeRequest("for test");
        XContentParser invalidContent = createParser(XContentFactory.jsonBuilder()
            .startObject()
                .field("text", "THIS IS A TEST")
                .field("unknown", "keyword")
            .endObject());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> RestAnalyzeAction.buildFromContent(invalidContent, analyzeRequest));
        assertThat(e.getMessage(), startsWith("Unknown parameter [unknown]"));
    }

    public void testParseXContentForAnalyzeRequestWithInvalidStringExplainParamThrowsException() throws Exception {
        AnalyzeRequest analyzeRequest = new AnalyzeRequest("for test");
        XContentParser invalidExplain = createParser(XContentFactory.jsonBuilder()
            .startObject()
                .field("explain", "fals")
            .endObject());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> RestAnalyzeAction.buildFromContent(invalidExplain, analyzeRequest));
        assertThat(e.getMessage(), startsWith("explain must be either 'true' or 'false'"));
    }

    public void testParseXContentForAnalyzeRequestWithInvalidNormalizerThrowsException() throws Exception {
        AnalyzeRequest analyzeRequest = new AnalyzeRequest("for test");
        XContentParser invalidExplain = createParser(XContentFactory.jsonBuilder()
            .startObject()
            .field("normalizer", true)
            .endObject());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> RestAnalyzeAction.buildFromContent(invalidExplain, analyzeRequest));
        assertThat(e.getMessage(), startsWith("normalizer should be normalizer's name"));
    }

    public void testDeprecatedParamIn2xException() throws Exception {
        {
            XContentParser parser = createParser(XContentFactory.jsonBuilder()
                    .startObject()
                        .field("text", "THIS IS A TEST")
                        .field("tokenizer", "keyword")
                        .array("filters", "lowercase")
                    .endObject());
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> RestAnalyzeAction.buildFromContent(parser,
                    new AnalyzeRequest("for test")));
            assertThat(e.getMessage(), startsWith("Unknown parameter [filters]"));
        }

        {
            XContentParser parser = createParser(XContentFactory.jsonBuilder()
                    .startObject()
                        .field("text", "THIS IS A TEST")
                        .field("tokenizer", "keyword")
                        .array("token_filters", "lowercase")
                    .endObject());
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> RestAnalyzeAction.buildFromContent(parser,
                    new AnalyzeRequest("for test")));
            assertThat(e.getMessage(), startsWith("Unknown parameter [token_filters]"));
        }

        {
            XContentParser parser = createParser(XContentFactory.jsonBuilder()
                    .startObject()
                        .field("text", "THIS IS A TEST")
                        .field("tokenizer", "keyword")
                        .array("char_filters", "lowercase")
                    .endObject());
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> RestAnalyzeAction.buildFromContent(parser,
                    new AnalyzeRequest("for test")));
            assertThat(e.getMessage(), startsWith("Unknown parameter [char_filters]"));
        }

        {
            XContentParser parser = createParser(XContentFactory.jsonBuilder()
                    .startObject()
                        .field("text", "THIS IS A TEST")
                        .field("tokenizer", "keyword")
                        .array("token_filter", "lowercase")
                    .endObject());
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> RestAnalyzeAction.buildFromContent(parser,
                    new AnalyzeRequest("for test")));
            assertThat(e.getMessage(), startsWith("Unknown parameter [token_filter]"));
        }
    }
}
