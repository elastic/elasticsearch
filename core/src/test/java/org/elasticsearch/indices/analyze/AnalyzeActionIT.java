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
package org.elasticsearch.indices.analyze;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.rest.action.admin.indices.analyze.RestAnalyzeAction;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class AnalyzeActionIT extends ESIntegTestCase {
    
    @Test
    public void simpleAnalyzerTests() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")));
        ensureGreen();

        for (int i = 0; i < 10; i++) {
            AnalyzeResponse analyzeResponse = client().admin().indices().prepareAnalyze(indexOrAlias(), "this is a test").get();
            assertThat(analyzeResponse.getTokens().size(), equalTo(4));
            AnalyzeResponse.AnalyzeToken token = analyzeResponse.getTokens().get(0);
            assertThat(token.getTerm(), equalTo("this"));
            assertThat(token.getStartOffset(), equalTo(0));
            assertThat(token.getEndOffset(), equalTo(4));
            assertThat(token.getPosition(), equalTo(0));
            token = analyzeResponse.getTokens().get(1);
            assertThat(token.getTerm(), equalTo("is"));
            assertThat(token.getStartOffset(), equalTo(5));
            assertThat(token.getEndOffset(), equalTo(7));
            assertThat(token.getPosition(), equalTo(1));
            token = analyzeResponse.getTokens().get(2);
            assertThat(token.getTerm(), equalTo("a"));
            assertThat(token.getStartOffset(), equalTo(8));
            assertThat(token.getEndOffset(), equalTo(9));
            assertThat(token.getPosition(), equalTo(2));
            token = analyzeResponse.getTokens().get(3);
            assertThat(token.getTerm(), equalTo("test"));
            assertThat(token.getStartOffset(), equalTo(10));
            assertThat(token.getEndOffset(), equalTo(14));
            assertThat(token.getPosition(), equalTo(3));
        }
    }
    
    @Test
    public void analyzeNumericField() throws IOException {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).addMapping("test", "long", "type=long", "double", "type=double"));
        ensureGreen("test");

        try {
            client().admin().indices().prepareAnalyze(indexOrAlias(), "123").setField("long").get();
            fail("shouldn't get here");
        } catch (IllegalArgumentException ex) {
            //all good
        }
        try {
            client().admin().indices().prepareAnalyze(indexOrAlias(), "123.0").setField("double").get();
            fail("shouldn't get here");
        } catch (IllegalArgumentException ex) {
            //all good
        }
    }

    @Test
    public void analyzeWithNoIndex() throws Exception {

        AnalyzeResponse analyzeResponse = client().admin().indices().prepareAnalyze("THIS IS A TEST").setAnalyzer("simple").get();
        assertThat(analyzeResponse.getTokens().size(), equalTo(4));

        analyzeResponse = client().admin().indices().prepareAnalyze("THIS IS A TEST").setTokenizer("keyword").setTokenFilters("lowercase").get();
        assertThat(analyzeResponse.getTokens().size(), equalTo(1));
        assertThat(analyzeResponse.getTokens().get(0).getTerm(), equalTo("this is a test"));

        analyzeResponse = client().admin().indices().prepareAnalyze("THIS IS A TEST").setTokenizer("standard").setTokenFilters("lowercase", "reverse").get();
        assertThat(analyzeResponse.getTokens().size(), equalTo(4));
        AnalyzeResponse.AnalyzeToken token = analyzeResponse.getTokens().get(0);
        assertThat(token.getTerm(), equalTo("siht"));
        token = analyzeResponse.getTokens().get(1);
        assertThat(token.getTerm(), equalTo("si"));
        token = analyzeResponse.getTokens().get(2);
        assertThat(token.getTerm(), equalTo("a"));
        token = analyzeResponse.getTokens().get(3);
        assertThat(token.getTerm(), equalTo("tset"));

        analyzeResponse = client().admin().indices().prepareAnalyze("of course").setTokenizer("standard").setTokenFilters("stop").get();
        assertThat(analyzeResponse.getTokens().size(), equalTo(1));
        assertThat(analyzeResponse.getTokens().get(0).getTerm(), equalTo("course"));
        assertThat(analyzeResponse.getTokens().get(0).getPosition(), equalTo(1));
        assertThat(analyzeResponse.getTokens().get(0).getStartOffset(), equalTo(3));
        assertThat(analyzeResponse.getTokens().get(0).getEndOffset(), equalTo(9));

    }

    @Test
    public void analyzeWithCharFilters() throws Exception {

        assertAcked(prepareCreate("test").addAlias(new Alias("alias"))
                .setSettings(settingsBuilder().put(indexSettings())
                        .put("index.analysis.char_filter.custom_mapping.type", "mapping")
                        .putArray("index.analysis.char_filter.custom_mapping.mappings", "ph=>f", "qu=>q")
                        .put("index.analysis.analyzer.custom_with_char_filter.tokenizer", "standard")
                        .putArray("index.analysis.analyzer.custom_with_char_filter.char_filter", "custom_mapping")));
        ensureGreen();

        AnalyzeResponse analyzeResponse = client().admin().indices().prepareAnalyze("<h2><b>THIS</b> IS A</h2> <a href=\"#\">TEST</a>").setTokenizer("standard").setCharFilters("html_strip").get();
        assertThat(analyzeResponse.getTokens().size(), equalTo(4));

        analyzeResponse = client().admin().indices().prepareAnalyze("THIS IS A <b>TEST</b>").setTokenizer("keyword").setTokenFilters("lowercase").setCharFilters("html_strip").get();
        assertThat(analyzeResponse.getTokens().size(), equalTo(1));
        assertThat(analyzeResponse.getTokens().get(0).getTerm(), equalTo("this is a test"));

        analyzeResponse = client().admin().indices().prepareAnalyze(indexOrAlias(), "jeff quit phish").setTokenizer("keyword").setTokenFilters("lowercase").setCharFilters("custom_mapping").get();
        assertThat(analyzeResponse.getTokens().size(), equalTo(1));
        assertThat(analyzeResponse.getTokens().get(0).getTerm(), equalTo("jeff qit fish"));

        analyzeResponse = client().admin().indices().prepareAnalyze(indexOrAlias(), "<a href=\"#\">jeff quit fish</a>").setTokenizer("standard").setCharFilters("html_strip", "custom_mapping").get();
        assertThat(analyzeResponse.getTokens().size(), equalTo(3));
        AnalyzeResponse.AnalyzeToken token = analyzeResponse.getTokens().get(0);
        assertThat(token.getTerm(), equalTo("jeff"));
        token = analyzeResponse.getTokens().get(1);
        assertThat(token.getTerm(), equalTo("qit"));
        token = analyzeResponse.getTokens().get(2);
        assertThat(token.getTerm(), equalTo("fish"));
    }

    @Test
    public void analyzerWithFieldOrTypeTests() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")));
        ensureGreen();

        client().admin().indices().preparePutMapping("test")
                .setType("document").setSource("simple", "type=string,analyzer=simple").get();

        for (int i = 0; i < 10; i++) {
            final AnalyzeRequestBuilder requestBuilder = client().admin().indices().prepareAnalyze("THIS IS A TEST");
            requestBuilder.setIndex(indexOrAlias());
            requestBuilder.setField("document.simple");
            AnalyzeResponse analyzeResponse = requestBuilder.get();
            assertThat(analyzeResponse.getTokens().size(), equalTo(4));
            AnalyzeResponse.AnalyzeToken token = analyzeResponse.getTokens().get(3);
            assertThat(token.getTerm(), equalTo("test"));
            assertThat(token.getStartOffset(), equalTo(10));
            assertThat(token.getEndOffset(), equalTo(14));
        }
    }

    @Test // issue #5974
    public void testThatStandardAndDefaultAnalyzersAreSame() throws Exception {
        AnalyzeResponse response = client().admin().indices().prepareAnalyze("this is a test").setAnalyzer("standard").get();
        assertTokens(response, "this", "is", "a", "test");

        response = client().admin().indices().prepareAnalyze("this is a test").setAnalyzer("default").get();
        assertTokens(response, "this", "is", "a", "test");

        response = client().admin().indices().prepareAnalyze("this is a test").get();
        assertTokens(response, "this", "is", "a", "test");
    }

    private void assertTokens(AnalyzeResponse response, String ... tokens) {
        assertThat(response.getTokens(), hasSize(tokens.length));
        for (int i = 0; i < tokens.length; i++) {
            assertThat(response.getTokens().get(i).getTerm(), is(tokens[i]));
        }
    }

    private static String indexOrAlias() {
        return randomBoolean() ? "test" : "alias";
    }

    @Test
    public void testParseXContentForAnalyzeReuqest() throws Exception {
        BytesReference content =  XContentFactory.jsonBuilder()
            .startObject()
            .field("text", "THIS IS A TEST")
            .field("tokenizer", "keyword")
            .array("filters", "lowercase")
            .endObject().bytes();

        AnalyzeRequest analyzeRequest = new AnalyzeRequest("for test");

        RestAnalyzeAction.buildFromContent(content, analyzeRequest);

        assertThat(analyzeRequest.text().length, equalTo(1));
        assertThat(analyzeRequest.text(), equalTo(new String[]{"THIS IS A TEST"}));
        assertThat(analyzeRequest.tokenizer(), equalTo("keyword"));
        assertThat(analyzeRequest.tokenFilters(), equalTo(new String[]{"lowercase"}));
    }

    @Test
    public void testParseXContentForAnalyzeRequestWithInvalidJsonThrowsException() throws Exception {
        AnalyzeRequest analyzeRequest = new AnalyzeRequest("for test");
        BytesReference invalidContent =  XContentFactory.jsonBuilder().startObject().value("invalid_json").endObject().bytes();

        try {
            RestAnalyzeAction.buildFromContent(invalidContent, analyzeRequest);
            fail("shouldn't get here");
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), equalTo("Failed to parse request body"));
        }
    }

    @Test
    public void testParseXContentForAnalyzeRequestWithUnknownParamThrowsException() throws Exception {
        AnalyzeRequest analyzeRequest = new AnalyzeRequest("for test");
        BytesReference invalidContent =XContentFactory.jsonBuilder()
            .startObject()
            .field("text", "THIS IS A TEST")
            .field("unknown", "keyword")
            .endObject().bytes();

        try {
            RestAnalyzeAction.buildFromContent(invalidContent, analyzeRequest);
            fail("shouldn't get here");
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), startsWith("Unknown parameter [unknown]"));
        }
    }

    @Test
    public void analyzerWithMultiValues() throws Exception {

        assertAcked(prepareCreate("test").addAlias(new Alias("alias")));
        ensureGreen();

        client().admin().indices().preparePutMapping("test")
            .setType("document").setSource("simple", "type=string,analyzer=simple,position_increment_gap=100").get();

        String[] texts = new String[]{"THIS IS A TEST", "THE SECOND TEXT"};

        final AnalyzeRequestBuilder requestBuilder = client().admin().indices().prepareAnalyze();
        requestBuilder.setText(texts);
        requestBuilder.setIndex(indexOrAlias());
        requestBuilder.setField("simple");
        AnalyzeResponse analyzeResponse = requestBuilder.get();
        assertThat(analyzeResponse.getTokens().size(), equalTo(7));
        AnalyzeResponse.AnalyzeToken token = analyzeResponse.getTokens().get(3);
        assertThat(token.getTerm(), equalTo("test"));
        assertThat(token.getPosition(), equalTo(3));
        assertThat(token.getStartOffset(), equalTo(10));
        assertThat(token.getEndOffset(), equalTo(14));

        token = analyzeResponse.getTokens().get(5);
        assertThat(token.getTerm(), equalTo("second"));
        assertThat(token.getPosition(), equalTo(105));
        assertThat(token.getStartOffset(), equalTo(19));
        assertThat(token.getEndOffset(), equalTo(25));

    }

}
