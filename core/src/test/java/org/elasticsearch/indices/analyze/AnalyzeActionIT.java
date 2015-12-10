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
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.action.admin.indices.analyze.RestAnalyzeAction;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.core.IsNull;

import java.io.IOException;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;


/**
 *
 */
public class AnalyzeActionIT extends ESIntegTestCase {
    public void testSimpleAnalyzerTests() throws Exception {
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

    public void testAnalyzeNumericField() throws IOException {
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

    public void testAnalyzeWithNoIndex() throws Exception {
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

    public void testAnalyzeWithCharFilters() throws Exception {
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

    public void testAnalyzerWithFieldOrTypeTests() throws Exception {
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

    // issue #5974
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

    public void testParseXContentForAnalyzeReuqest() throws Exception {
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
    }

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

    public void testParseXContentForAnalyzeRequestWithUnknownParamThrowsException() throws Exception {
        AnalyzeRequest analyzeRequest = new AnalyzeRequest("for test");
        BytesReference invalidContent =XContentFactory.jsonBuilder()
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

    public void testAnalyzerWithMultiValues() throws Exception {
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

    public void testDetailAnalyze() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias"))
            .setSettings(
                settingsBuilder()
                    .put("index.analysis.char_filter.my_mapping.type", "mapping")
                    .putArray("index.analysis.char_filter.my_mapping.mappings", "PH=>F")
                    .put("index.analysis.analyzer.test_analyzer.type", "custom")
                    .put("index.analysis.analyzer.test_analyzer.position_increment_gap", "100")
                    .put("index.analysis.analyzer.test_analyzer.tokenizer", "standard")
                    .putArray("index.analysis.analyzer.test_analyzer.char_filter", "my_mapping")
                    .putArray("index.analysis.analyzer.test_analyzer.filter", "snowball")));
        ensureGreen();

        for (int i = 0; i < 10; i++) {
            AnalyzeResponse analyzeResponse = admin().indices().prepareAnalyze().setIndex(indexOrAlias()).setText("THIS IS A PHISH")
                .setExplain(true).setCharFilters("my_mapping").setTokenizer("keyword").setTokenFilters("lowercase").get();

            assertThat(analyzeResponse.detail().analyzer(), IsNull.nullValue());
            //charfilters
            // global charfilter is not change text.
            assertThat(analyzeResponse.detail().charfilters().length, equalTo(1));
            assertThat(analyzeResponse.detail().charfilters()[0].getName(), equalTo("my_mapping"));
            assertThat(analyzeResponse.detail().charfilters()[0].getTexts().length, equalTo(1));
            assertThat(analyzeResponse.detail().charfilters()[0].getTexts()[0], equalTo("THIS IS A FISH"));
            //tokenizer
            assertThat(analyzeResponse.detail().tokenizer().getName(), equalTo("keyword"));
            assertThat(analyzeResponse.detail().tokenizer().getTokens().length, equalTo(1));
            assertThat(analyzeResponse.detail().tokenizer().getTokens()[0].getTerm(), equalTo("THIS IS A FISH"));
            assertThat(analyzeResponse.detail().tokenizer().getTokens()[0].getStartOffset(), equalTo(0));
            assertThat(analyzeResponse.detail().tokenizer().getTokens()[0].getEndOffset(), equalTo(15));
            //tokenfilters
            assertThat(analyzeResponse.detail().tokenfilters().length, equalTo(1));
            assertThat(analyzeResponse.detail().tokenfilters()[0].getName(), equalTo("lowercase"));
            assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens().length, equalTo(1));
            assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[0].getTerm(), equalTo("this is a fish"));
            assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[0].getPosition(), equalTo(0));
            assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[0].getStartOffset(), equalTo(0));
            assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[0].getEndOffset(), equalTo(15));
        }
    }

    public void testDetailAnalyzeWithNoIndex() throws Exception {
        //analyzer only
        AnalyzeResponse analyzeResponse = client().admin().indices().prepareAnalyze("THIS IS A TEST")
            .setExplain(true).setAnalyzer("simple").get();

        assertThat(analyzeResponse.detail().tokenizer(), IsNull.nullValue());
        assertThat(analyzeResponse.detail().tokenfilters(), IsNull.nullValue());
        assertThat(analyzeResponse.detail().charfilters(), IsNull.nullValue());
        assertThat(analyzeResponse.detail().analyzer().getName(), equalTo("simple"));
        assertThat(analyzeResponse.detail().analyzer().getTokens().length, equalTo(4));
    }

    public void testDetailAnalyzeCustomAnalyzerWithNoIndex() throws Exception {
        //analyzer only
        AnalyzeResponse analyzeResponse = client().admin().indices().prepareAnalyze("THIS IS A TEST")
            .setExplain(true).setAnalyzer("simple").get();

        assertThat(analyzeResponse.detail().tokenizer(), IsNull.nullValue());
        assertThat(analyzeResponse.detail().tokenfilters(), IsNull.nullValue());
        assertThat(analyzeResponse.detail().charfilters(), IsNull.nullValue());
        assertThat(analyzeResponse.detail().analyzer().getName(), equalTo("simple"));
        assertThat(analyzeResponse.detail().analyzer().getTokens().length, equalTo(4));

        //custom analyzer
        analyzeResponse = client().admin().indices().prepareAnalyze("<text>THIS IS A TEST</text>")
            .setExplain(true).setCharFilters("html_strip").setTokenizer("keyword").setTokenFilters("lowercase").get();
        assertThat(analyzeResponse.detail().analyzer(), IsNull.nullValue());
        //charfilters
        // global charfilter is not change text.
        assertThat(analyzeResponse.detail().charfilters().length, equalTo(1));
        assertThat(analyzeResponse.detail().charfilters()[0].getName(), equalTo("html_strip"));
        assertThat(analyzeResponse.detail().charfilters()[0].getTexts().length, equalTo(1));
        assertThat(analyzeResponse.detail().charfilters()[0].getTexts()[0], equalTo("\nTHIS IS A TEST\n"));
        //tokenizer
        assertThat(analyzeResponse.detail().tokenizer().getName(), equalTo("keyword"));
        assertThat(analyzeResponse.detail().tokenizer().getTokens().length, equalTo(1));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[0].getTerm(), equalTo("\nTHIS IS A TEST\n"));
        //tokenfilters
        assertThat(analyzeResponse.detail().tokenfilters().length, equalTo(1));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getName(), equalTo("lowercase"));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens().length, equalTo(1));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[0].getTerm(), equalTo("\nthis is a test\n"));


        //check other attributes
        analyzeResponse = client().admin().indices().prepareAnalyze("This is troubled")
            .setExplain(true).setTokenizer("standard").setTokenFilters("snowball").get();

        assertThat(analyzeResponse.detail().tokenfilters().length, equalTo(1));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getName(), equalTo("snowball"));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens().length, equalTo(3));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[2].getTerm(), equalTo("troubl"));
        String[] expectedAttributesKey = {
            "bytes",
            "positionLength",
            "keyword"};
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[2].getAttributes().size(), equalTo(expectedAttributesKey.length));
        Object extendedAttribute;

        for (String key : expectedAttributesKey) {
            extendedAttribute = analyzeResponse.detail().tokenfilters()[0].getTokens()[2].getAttributes().get(key);
            assertThat(extendedAttribute, notNullValue());
        }
    }

    public void testDetailAnalyzeSpecifyAttributes() throws Exception {
        AnalyzeResponse analyzeResponse = client().admin().indices().prepareAnalyze("This is troubled")
            .setExplain(true).setTokenizer("standard").setTokenFilters("snowball").setAttributes("keyword").get();

        assertThat(analyzeResponse.detail().tokenfilters().length, equalTo(1));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getName(), equalTo("snowball"));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens().length, equalTo(3));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[2].getTerm(), equalTo("troubl"));
        String[] expectedAttributesKey = {
            "keyword"};
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[2].getAttributes().size(), equalTo(expectedAttributesKey.length));
        Object extendedAttribute;

        for (String key : expectedAttributesKey) {
            extendedAttribute = analyzeResponse.detail().tokenfilters()[0].getTokens()[2].getAttributes().get(key);
            assertThat(extendedAttribute, notNullValue());
        }
    }

    public void testDetailAnalyzeWithMultiValues() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")));
        ensureGreen();
        client().admin().indices().preparePutMapping("test")
            .setType("document").setSource("simple", "type=string,analyzer=simple,position_increment_gap=100").get();

        String[] texts = new String[]{"THIS IS A TEST", "THE SECOND TEXT"};
        AnalyzeResponse analyzeResponse = client().admin().indices().prepareAnalyze().setIndex(indexOrAlias()).setText(texts)
            .setExplain(true).setField("simple").setText(texts).execute().get();

        assertThat(analyzeResponse.detail().analyzer().getName(), equalTo("simple"));
        assertThat(analyzeResponse.detail().analyzer().getTokens().length, equalTo(7));
        AnalyzeResponse.AnalyzeToken token = analyzeResponse.detail().analyzer().getTokens()[3];

        assertThat(token.getTerm(), equalTo("test"));
        assertThat(token.getPosition(), equalTo(3));
        assertThat(token.getStartOffset(), equalTo(10));
        assertThat(token.getEndOffset(), equalTo(14));

        token = analyzeResponse.detail().analyzer().getTokens()[5];
        assertThat(token.getTerm(), equalTo("second"));
        assertThat(token.getPosition(), equalTo(105));
        assertThat(token.getStartOffset(), equalTo(19));
        assertThat(token.getEndOffset(), equalTo(25));
    }

    public void testDetailAnalyzeWithMultiValuesWithCustomAnalyzer() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias"))
            .setSettings(
                settingsBuilder()
                    .put("index.analysis.char_filter.my_mapping.type", "mapping")
                    .putArray("index.analysis.char_filter.my_mapping.mappings", "PH=>F")
                    .put("index.analysis.analyzer.test_analyzer.type", "custom")
                    .put("index.analysis.analyzer.test_analyzer.position_increment_gap", "100")
                    .put("index.analysis.analyzer.test_analyzer.tokenizer", "standard")
                    .putArray("index.analysis.analyzer.test_analyzer.char_filter", "my_mapping")
                    .putArray("index.analysis.analyzer.test_analyzer.filter", "snowball", "lowercase")));
        ensureGreen();

        client().admin().indices().preparePutMapping("test")
            .setType("document").setSource("simple", "type=string,analyzer=simple,position_increment_gap=100").get();

        //only analyzer =
        String[] texts = new String[]{"this is a PHISH", "the troubled text"};
        AnalyzeResponse analyzeResponse = client().admin().indices().prepareAnalyze().setIndex(indexOrAlias()).setText(texts)
            .setExplain(true).setAnalyzer("test_analyzer").setText(texts).execute().get();

        // charfilter
        assertThat(analyzeResponse.detail().charfilters().length, equalTo(1));
        assertThat(analyzeResponse.detail().charfilters()[0].getName(), equalTo("my_mapping"));
        assertThat(analyzeResponse.detail().charfilters()[0].getTexts().length, equalTo(2));
        assertThat(analyzeResponse.detail().charfilters()[0].getTexts()[0], equalTo("this is a FISH"));
        assertThat(analyzeResponse.detail().charfilters()[0].getTexts()[1], equalTo("the troubled text"));

        // tokenizer
        assertThat(analyzeResponse.detail().tokenizer().getName(), equalTo("standard"));
        assertThat(analyzeResponse.detail().tokenizer().getTokens().length, equalTo(7));
        AnalyzeResponse.AnalyzeToken token = analyzeResponse.detail().tokenizer().getTokens()[3];

        assertThat(token.getTerm(), equalTo("FISH"));
        assertThat(token.getPosition(), equalTo(3));
        assertThat(token.getStartOffset(), equalTo(10));
        assertThat(token.getEndOffset(), equalTo(15));

        token = analyzeResponse.detail().tokenizer().getTokens()[5];
        assertThat(token.getTerm(), equalTo("troubled"));
        assertThat(token.getPosition(), equalTo(105));
        assertThat(token.getStartOffset(), equalTo(20));
        assertThat(token.getEndOffset(), equalTo(28));

        // tokenfilter(snowball)
        assertThat(analyzeResponse.detail().tokenfilters().length, equalTo(2));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getName(), equalTo("snowball"));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens().length, equalTo(7));
        token = analyzeResponse.detail().tokenfilters()[0].getTokens()[3];

        assertThat(token.getTerm(), equalTo("FISH"));
        assertThat(token.getPosition(), equalTo(3));
        assertThat(token.getStartOffset(), equalTo(10));
        assertThat(token.getEndOffset(), equalTo(15));

        token = analyzeResponse.detail().tokenfilters()[0].getTokens()[5];
        assertThat(token.getTerm(), equalTo("troubl"));
        assertThat(token.getPosition(), equalTo(105));
        assertThat(token.getStartOffset(), equalTo(20));
        assertThat(token.getEndOffset(), equalTo(28));

        // tokenfilter(lowercase)
        assertThat(analyzeResponse.detail().tokenfilters()[1].getName(), equalTo("lowercase"));
        assertThat(analyzeResponse.detail().tokenfilters()[1].getTokens().length, equalTo(7));
        token = analyzeResponse.detail().tokenfilters()[1].getTokens()[3];

        assertThat(token.getTerm(), equalTo("fish"));
        assertThat(token.getPosition(), equalTo(3));
        assertThat(token.getStartOffset(), equalTo(10));
        assertThat(token.getEndOffset(), equalTo(15));

        token = analyzeResponse.detail().tokenfilters()[0].getTokens()[5];
        assertThat(token.getTerm(), equalTo("troubl"));
        assertThat(token.getPosition(), equalTo(105));
        assertThat(token.getStartOffset(), equalTo(20));
        assertThat(token.getEndOffset(), equalTo(28));


    }

}
