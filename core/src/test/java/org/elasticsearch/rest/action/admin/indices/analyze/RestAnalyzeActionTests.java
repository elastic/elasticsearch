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
package org.elasticsearch.rest.action.admin.indices.analyze;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

public class RestAnalyzeActionTests extends ESTestCase {

    @Test
    public void testParseXContentForAnalyzeRequest() throws Exception {
        BytesReference content =  XContentFactory.jsonBuilder()
            .startObject()
            .field("text", "THIS IS A TEST")
            .field("tokenizer", "keyword")
            .array("filter", "lowercase")
            .endObject().bytes();

        AnalyzeRequest analyzeRequest = new AnalyzeRequest("for test");

        RestAnalyzeAction.buildFromContent(content, analyzeRequest, new ParseFieldMatcher(Settings.EMPTY));

        assertThat(analyzeRequest.text().length, equalTo(1));
        assertThat(analyzeRequest.text(), equalTo(new String[]{"THIS IS A TEST"}));
        assertThat(analyzeRequest.tokenizer(), equalTo("keyword"));
        assertThat(analyzeRequest.tokenFilters(), equalTo(new String[]{"lowercase"}));
    }

    @Test
    public void testParseXContentForAnalyzeRequestWithInvalidJsonThrowsException() throws Exception {
        AnalyzeRequest analyzeRequest = new AnalyzeRequest("for test");

        try {
            RestAnalyzeAction.buildFromContent(new BytesArray("{invalid_json}"), analyzeRequest, new ParseFieldMatcher(Settings.EMPTY));
            fail("shouldn't get here");
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), equalTo("Failed to parse request body"));
        }
    }

    @Test
    public void testParseXContentForAnalyzeRequestWithUnknownParamThrowsException() throws Exception {
        AnalyzeRequest analyzeRequest = new AnalyzeRequest("for test");
        BytesReference invalidContent = XContentFactory.jsonBuilder()
            .startObject()
            .field("text", "THIS IS A TEST")
            .field("unknown", "keyword")
            .endObject().bytes();

        try {
            RestAnalyzeAction.buildFromContent(invalidContent, analyzeRequest, new ParseFieldMatcher(Settings.EMPTY));
            fail("shouldn't get here");
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), startsWith("Unknown parameter [unknown]"));
        }
    }

    @Test
    public void testParseXContentForAnalyzeRequestWithInvalidStringExplainParamThrowsException() throws Exception {
        AnalyzeRequest analyzeRequest = new AnalyzeRequest("for test");
        BytesReference invalidExplain = XContentFactory.jsonBuilder()
            .startObject()
            .field("explain", "fals")
            .endObject().bytes();
        try {
            RestAnalyzeAction.buildFromContent(invalidExplain, analyzeRequest, new ParseFieldMatcher(Settings.EMPTY));
            fail("shouldn't get here");
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), startsWith("explain must be either 'true' or 'false'"));
        }
    }

    @Test
    public void testDeprecatedParams() throws Exception {
        BytesReference content =  XContentFactory.jsonBuilder()
            .startObject()
            .field("text", "THIS IS A TEST")
            .field("tokenizer", "keyword")
            .array("filters", "lowercase")
            .endObject().bytes();

        AnalyzeRequest analyzeRequest = new AnalyzeRequest("for test");

        RestAnalyzeAction.buildFromContent(content, analyzeRequest, new ParseFieldMatcher(Settings.EMPTY));

        assertThat(analyzeRequest.text().length, equalTo(1));
        assertThat(analyzeRequest.text(), equalTo(new String[]{"THIS IS A TEST"}));
        assertThat(analyzeRequest.tokenizer(), equalTo("keyword"));
        assertThat(analyzeRequest.tokenFilters(), equalTo(new String[]{"lowercase"}));


        content =  XContentFactory.jsonBuilder()
            .startObject()
            .field("text", "THIS IS A TEST")
            .field("tokenizer", "keyword")
            .array("token_filters", "lowercase")
            .endObject().bytes();

        analyzeRequest = new AnalyzeRequest("for test");

        RestAnalyzeAction.buildFromContent(content, analyzeRequest, new ParseFieldMatcher(Settings.EMPTY));

        assertThat(analyzeRequest.text().length, equalTo(1));
        assertThat(analyzeRequest.text(), equalTo(new String[]{"THIS IS A TEST"}));
        assertThat(analyzeRequest.tokenizer(), equalTo("keyword"));
        assertThat(analyzeRequest.tokenFilters(), equalTo(new String[]{"lowercase"}));

        content =  XContentFactory.jsonBuilder()
            .startObject()
            .field("text", "THIS IS A TEST")
            .field("tokenizer", "keyword")
            .array("char_filters", "lowercase")
            .endObject().bytes();

        analyzeRequest = new AnalyzeRequest("for test");

        RestAnalyzeAction.buildFromContent(content, analyzeRequest, new ParseFieldMatcher(Settings.EMPTY));

        assertThat(analyzeRequest.text().length, equalTo(1));
        assertThat(analyzeRequest.text(), equalTo(new String[]{"THIS IS A TEST"}));
        assertThat(analyzeRequest.tokenizer(), equalTo("keyword"));
        assertThat(analyzeRequest.charFilters(), equalTo(new String[]{"lowercase"}));
    }


}
