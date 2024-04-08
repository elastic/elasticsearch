/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indices.analyze;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.MockKeywordPlugin;
import org.hamcrest.core.IsNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

@ClusterScope(minNumDataNodes = 2)
public class AnalyzeActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(MockKeywordPlugin.class);
    }

    public void testSimpleAnalyzerTests() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")));
        ensureGreen();

        for (int i = 0; i < 10; i++) {
            AnalyzeAction.Response analyzeResponse = indicesAdmin().prepareAnalyze(indexOrAlias(), "this is a test").get();
            assertThat(analyzeResponse.getTokens().size(), equalTo(4));
            AnalyzeAction.AnalyzeToken token = analyzeResponse.getTokens().get(0);
            assertThat(token.getTerm(), equalTo("this"));
            assertThat(token.getStartOffset(), equalTo(0));
            assertThat(token.getEndOffset(), equalTo(4));
            assertThat(token.getPosition(), equalTo(0));
            assertThat(token.getPositionLength(), equalTo(1));
            token = analyzeResponse.getTokens().get(1);
            assertThat(token.getTerm(), equalTo("is"));
            assertThat(token.getStartOffset(), equalTo(5));
            assertThat(token.getEndOffset(), equalTo(7));
            assertThat(token.getPosition(), equalTo(1));
            assertThat(token.getPositionLength(), equalTo(1));
            token = analyzeResponse.getTokens().get(2);
            assertThat(token.getTerm(), equalTo("a"));
            assertThat(token.getStartOffset(), equalTo(8));
            assertThat(token.getEndOffset(), equalTo(9));
            assertThat(token.getPosition(), equalTo(2));
            assertThat(token.getPositionLength(), equalTo(1));
            token = analyzeResponse.getTokens().get(3);
            assertThat(token.getTerm(), equalTo("test"));
            assertThat(token.getStartOffset(), equalTo(10));
            assertThat(token.getEndOffset(), equalTo(14));
            assertThat(token.getPosition(), equalTo(3));
            assertThat(token.getPositionLength(), equalTo(1));
        }
    }

    public void testAnalyzeNumericField() throws IOException {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setMapping("long", "type=long", "double", "type=double"));
        ensureGreen("test");

        expectThrows(IllegalArgumentException.class, indicesAdmin().prepareAnalyze(indexOrAlias(), "123").setField("long"));
        expectThrows(IllegalArgumentException.class, indicesAdmin().prepareAnalyze(indexOrAlias(), "123.0").setField("double"));
    }

    public void testAnalyzeWithNoIndex() throws Exception {
        AnalyzeAction.Response analyzeResponse = indicesAdmin().prepareAnalyze("THIS IS A TEST").setAnalyzer("simple").get();
        assertThat(analyzeResponse.getTokens().size(), equalTo(4));

        analyzeResponse = indicesAdmin().prepareAnalyze("THIS IS A TEST").setTokenizer("keyword").addTokenFilter("lowercase").get();
        assertThat(analyzeResponse.getTokens().size(), equalTo(1));
        assertThat(analyzeResponse.getTokens().get(0).getTerm(), equalTo("this is a test"));

        analyzeResponse = indicesAdmin().prepareAnalyze("THIS IS A TEST").setTokenizer("standard").addTokenFilter("lowercase").get();
        assertThat(analyzeResponse.getTokens().size(), equalTo(4));
        AnalyzeAction.AnalyzeToken token = analyzeResponse.getTokens().get(0);
        assertThat(token.getTerm(), equalTo("this"));
        token = analyzeResponse.getTokens().get(1);
        assertThat(token.getTerm(), equalTo("is"));
        token = analyzeResponse.getTokens().get(2);
        assertThat(token.getTerm(), equalTo("a"));
        token = analyzeResponse.getTokens().get(3);
        assertThat(token.getTerm(), equalTo("test"));

        analyzeResponse = indicesAdmin().prepareAnalyze("of course").setTokenizer("standard").addTokenFilter("stop").get();
        assertThat(analyzeResponse.getTokens().size(), equalTo(1));
        assertThat(analyzeResponse.getTokens().get(0).getTerm(), equalTo("course"));
        assertThat(analyzeResponse.getTokens().get(0).getPosition(), equalTo(1));
        assertThat(analyzeResponse.getTokens().get(0).getStartOffset(), equalTo(3));
        assertThat(analyzeResponse.getTokens().get(0).getEndOffset(), equalTo(9));
        assertThat(analyzeResponse.getTokens().get(0).getPositionLength(), equalTo(1));
    }

    public void testAnalyzerWithFieldOrTypeTests() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")));
        ensureGreen();

        indicesAdmin().preparePutMapping("test").setSource("simple", "type=text,analyzer=simple").get();

        for (int i = 0; i < 10; i++) {
            final AnalyzeRequestBuilder requestBuilder = indicesAdmin().prepareAnalyze("THIS IS A TEST");
            requestBuilder.setIndex(indexOrAlias());
            requestBuilder.setField("document.simple");
            AnalyzeAction.Response analyzeResponse = requestBuilder.get();
            assertThat(analyzeResponse.getTokens().size(), equalTo(4));
            AnalyzeAction.AnalyzeToken token = analyzeResponse.getTokens().get(3);
            assertThat(token.getTerm(), equalTo("test"));
            assertThat(token.getStartOffset(), equalTo(10));
            assertThat(token.getEndOffset(), equalTo(14));
            assertThat(token.getPositionLength(), equalTo(1));
        }
    }

    // issue #5974
    public void testThatStandardAndDefaultAnalyzersAreSame() throws Exception {
        AnalyzeAction.Response response = indicesAdmin().prepareAnalyze("this is a test").setAnalyzer("standard").get();
        assertTokens(response, "this", "is", "a", "test");

        response = indicesAdmin().prepareAnalyze("this is a test").setAnalyzer("default").get();
        assertTokens(response, "this", "is", "a", "test");

        response = indicesAdmin().prepareAnalyze("this is a test").get();
        assertTokens(response, "this", "is", "a", "test");
    }

    private void assertTokens(AnalyzeAction.Response response, String... tokens) {
        assertThat(response.getTokens(), hasSize(tokens.length));
        for (int i = 0; i < tokens.length; i++) {
            assertThat(response.getTokens().get(i).getTerm(), is(tokens[i]));
        }
    }

    private static String indexOrAlias() {
        return randomBoolean() ? "test" : "alias";
    }

    public void testAnalyzerWithMultiValues() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")));
        ensureGreen();

        indicesAdmin().preparePutMapping("test").setSource("simple", "type=text,analyzer=simple,position_increment_gap=100").get();

        String[] texts = new String[] { "THIS IS A TEST", "THE SECOND TEXT" };

        final AnalyzeRequestBuilder requestBuilder = indicesAdmin().prepareAnalyze();
        requestBuilder.setText(texts);
        requestBuilder.setIndex(indexOrAlias());
        requestBuilder.setField("simple");
        AnalyzeAction.Response analyzeResponse = requestBuilder.get();
        assertThat(analyzeResponse.getTokens().size(), equalTo(7));
        AnalyzeAction.AnalyzeToken token = analyzeResponse.getTokens().get(3);
        assertThat(token.getTerm(), equalTo("test"));
        assertThat(token.getPosition(), equalTo(3));
        assertThat(token.getStartOffset(), equalTo(10));
        assertThat(token.getEndOffset(), equalTo(14));
        assertThat(token.getPositionLength(), equalTo(1));

        token = analyzeResponse.getTokens().get(5);
        assertThat(token.getTerm(), equalTo("second"));
        assertThat(token.getPosition(), equalTo(105));
        assertThat(token.getStartOffset(), equalTo(19));
        assertThat(token.getEndOffset(), equalTo(25));
        assertThat(token.getPositionLength(), equalTo(1));
    }

    public void testDetailAnalyzeWithNoIndex() throws Exception {
        // analyzer only
        AnalyzeAction.Response analyzeResponse = indicesAdmin().prepareAnalyze("THIS IS A TEST")
            .setExplain(true)
            .setAnalyzer("simple")
            .get();

        assertThat(analyzeResponse.detail().tokenizer(), IsNull.nullValue());
        assertThat(analyzeResponse.detail().tokenfilters(), IsNull.nullValue());
        assertThat(analyzeResponse.detail().charfilters(), IsNull.nullValue());
        assertThat(analyzeResponse.detail().analyzer().getName(), equalTo("simple"));
        assertThat(analyzeResponse.detail().analyzer().getTokens().length, equalTo(4));
    }

    public void testDetailAnalyzeCustomAnalyzerWithNoIndex() throws Exception {
        // analyzer only
        AnalyzeAction.Response analyzeResponse = indicesAdmin().prepareAnalyze("THIS IS A TEST")
            .setExplain(true)
            .setAnalyzer("simple")
            .get();

        assertThat(analyzeResponse.detail().tokenizer(), IsNull.nullValue());
        assertThat(analyzeResponse.detail().tokenfilters(), IsNull.nullValue());
        assertThat(analyzeResponse.detail().charfilters(), IsNull.nullValue());
        assertThat(analyzeResponse.detail().analyzer().getName(), equalTo("simple"));
        assertThat(analyzeResponse.detail().analyzer().getTokens().length, equalTo(4));

        // custom analyzer
        analyzeResponse = indicesAdmin().prepareAnalyze("THIS IS A TEST")
            .setExplain(true)
            .setTokenizer("keyword")
            .addTokenFilter("lowercase")
            .get();
        assertThat(analyzeResponse.detail().analyzer(), IsNull.nullValue());
        // tokenizer
        assertThat(analyzeResponse.detail().tokenizer().getName(), equalTo("keyword"));
        assertThat(analyzeResponse.detail().tokenizer().getTokens().length, equalTo(1));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[0].getTerm(), equalTo("THIS IS A TEST"));
        // tokenfilters
        assertThat(analyzeResponse.detail().tokenfilters().length, equalTo(1));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getName(), equalTo("lowercase"));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens().length, equalTo(1));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[0].getTerm(), equalTo("this is a test"));

        // check other attributes
        analyzeResponse = indicesAdmin().prepareAnalyze("This is troubled")
            .setExplain(true)
            .setTokenizer("standard")
            .addTokenFilter("lowercase")
            .get();

        assertThat(analyzeResponse.detail().tokenfilters().length, equalTo(1));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getName(), equalTo("lowercase"));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens().length, equalTo(3));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[2].getTerm(), equalTo("troubled"));
        String[] expectedAttributesKey = { "bytes", "termFrequency", "positionLength" };
        assertThat(
            analyzeResponse.detail().tokenfilters()[0].getTokens()[2].getAttributes().keySet(),
            equalTo(new HashSet<>(Arrays.asList(expectedAttributesKey)))
        );
    }

    public void testDetailAnalyzeWithMultiValues() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")));
        ensureGreen();
        indicesAdmin().preparePutMapping("test").setSource("simple", "type=text,analyzer=simple,position_increment_gap=100").get();

        String[] texts = new String[] { "THIS IS A TEST", "THE SECOND TEXT" };
        AnalyzeAction.Response analyzeResponse = indicesAdmin().prepareAnalyze()
            .setIndex(indexOrAlias())
            .setText(texts)
            .setExplain(true)
            .setField("simple")
            .setText(texts)
            .execute()
            .get();

        assertThat(analyzeResponse.detail().analyzer().getName(), equalTo("simple"));
        assertThat(analyzeResponse.detail().analyzer().getTokens().length, equalTo(7));
        AnalyzeAction.AnalyzeToken token = analyzeResponse.detail().analyzer().getTokens()[3];

        assertThat(token.getTerm(), equalTo("test"));
        assertThat(token.getPosition(), equalTo(3));
        assertThat(token.getStartOffset(), equalTo(10));
        assertThat(token.getEndOffset(), equalTo(14));
        assertThat(token.getPositionLength(), equalTo(1));

        token = analyzeResponse.detail().analyzer().getTokens()[5];
        assertThat(token.getTerm(), equalTo("second"));
        assertThat(token.getPosition(), equalTo(105));
        assertThat(token.getStartOffset(), equalTo(19));
        assertThat(token.getEndOffset(), equalTo(25));
        assertThat(token.getPositionLength(), equalTo(1));
    }

    public void testNonExistTokenizer() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            indicesAdmin().prepareAnalyze("this is a test").setAnalyzer("not_exist_analyzer")
        );
        assertThat(e.getMessage(), startsWith("failed to find global analyzer"));
    }

    public void testCustomTokenFilterInRequest() throws Exception {
        Map<String, Object> stopFilterSettings = new HashMap<>();
        stopFilterSettings.put("type", "stop");
        stopFilterSettings.put("stopwords", new String[] { "foo", "buzz" });
        AnalyzeAction.Response analyzeResponse = indicesAdmin().prepareAnalyze()
            .setText("Foo buzz test")
            .setTokenizer("standard")
            .addTokenFilter("lowercase")
            .addTokenFilter(stopFilterSettings)
            .setExplain(true)
            .get();

        // tokenizer
        assertThat(analyzeResponse.detail().tokenizer().getName(), equalTo("standard"));
        assertThat(analyzeResponse.detail().tokenizer().getTokens().length, equalTo(3));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[0].getTerm(), equalTo("Foo"));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[0].getStartOffset(), equalTo(0));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[0].getEndOffset(), equalTo(3));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[0].getPosition(), equalTo(0));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[0].getPositionLength(), equalTo(1));

        assertThat(analyzeResponse.detail().tokenizer().getTokens()[1].getTerm(), equalTo("buzz"));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[1].getStartOffset(), equalTo(4));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[1].getEndOffset(), equalTo(8));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[1].getPosition(), equalTo(1));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[1].getPositionLength(), equalTo(1));

        assertThat(analyzeResponse.detail().tokenizer().getTokens()[2].getTerm(), equalTo("test"));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[2].getStartOffset(), equalTo(9));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[2].getEndOffset(), equalTo(13));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[2].getPosition(), equalTo(2));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[2].getPositionLength(), equalTo(1));

        // tokenfilter(lowercase)
        assertThat(analyzeResponse.detail().tokenfilters().length, equalTo(2));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getName(), equalTo("lowercase"));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens().length, equalTo(3));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[0].getTerm(), equalTo("foo"));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[0].getStartOffset(), equalTo(0));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[0].getEndOffset(), equalTo(3));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[0].getPosition(), equalTo(0));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[0].getPositionLength(), equalTo(1));

        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[1].getTerm(), equalTo("buzz"));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[1].getStartOffset(), equalTo(4));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[1].getEndOffset(), equalTo(8));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[1].getPosition(), equalTo(1));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[1].getPositionLength(), equalTo(1));

        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[2].getTerm(), equalTo("test"));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[2].getStartOffset(), equalTo(9));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[2].getEndOffset(), equalTo(13));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[2].getPosition(), equalTo(2));
        assertThat(analyzeResponse.detail().tokenfilters()[0].getTokens()[2].getPositionLength(), equalTo(1));

        // tokenfilter({"type": "stop", "stopwords": ["foo", "buzz"]})
        assertThat(analyzeResponse.detail().tokenfilters()[1].getName(), equalTo("__anonymous__stop"));
        assertThat(analyzeResponse.detail().tokenfilters()[1].getTokens().length, equalTo(1));

        assertThat(analyzeResponse.detail().tokenfilters()[1].getTokens()[0].getTerm(), equalTo("test"));
        assertThat(analyzeResponse.detail().tokenfilters()[1].getTokens()[0].getStartOffset(), equalTo(9));
        assertThat(analyzeResponse.detail().tokenfilters()[1].getTokens()[0].getEndOffset(), equalTo(13));
        assertThat(analyzeResponse.detail().tokenfilters()[1].getTokens()[0].getPosition(), equalTo(2));
        assertThat(analyzeResponse.detail().tokenfilters()[1].getTokens()[0].getPositionLength(), equalTo(1));
    }

    public void testAnalyzeKeywordField() throws IOException {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setMapping("keyword", "type=keyword"));
        ensureGreen("test");

        AnalyzeAction.Response analyzeResponse = indicesAdmin().prepareAnalyze(indexOrAlias(), "ABC").setField("keyword").get();
        assertThat(analyzeResponse.getTokens().size(), equalTo(1));
        AnalyzeAction.AnalyzeToken token = analyzeResponse.getTokens().get(0);
        assertThat(token.getTerm(), equalTo("ABC"));
        assertThat(token.getStartOffset(), equalTo(0));
        assertThat(token.getEndOffset(), equalTo(3));
        assertThat(token.getPosition(), equalTo(0));
        assertThat(token.getPositionLength(), equalTo(1));
    }

    public void testAnalyzeNormalizedKeywordField() throws IOException {
        assertAcked(
            prepareCreate("test").addAlias(new Alias("alias"))
                .setSettings(
                    Settings.builder()
                        .put(indexSettings())
                        .put("index.analysis.normalizer.my_normalizer.type", "custom")
                        .putList("index.analysis.normalizer.my_normalizer.filter", "lowercase")
                )
                .setMapping("keyword", "type=keyword,normalizer=my_normalizer")
        );
        ensureGreen("test");

        AnalyzeAction.Response analyzeResponse = indicesAdmin().prepareAnalyze(indexOrAlias(), "ABC").setField("keyword").get();
        assertThat(analyzeResponse.getTokens().size(), equalTo(1));
        AnalyzeAction.AnalyzeToken token = analyzeResponse.getTokens().get(0);
        assertThat(token.getTerm(), equalTo("abc"));
        assertThat(token.getStartOffset(), equalTo(0));
        assertThat(token.getEndOffset(), equalTo(3));
        assertThat(token.getPosition(), equalTo(0));
        assertThat(token.getPositionLength(), equalTo(1));
    }

    /**
     * Input text that doesn't produce tokens should return an empty token list
     */
    public void testZeroTokenAnalysis() throws IOException {
        assertAcked(prepareCreate("test"));
        ensureGreen("test");

        AnalyzeAction.Response analyzeResponse = indicesAdmin().prepareAnalyze("test", ".").get();
        assertNotNull(analyzeResponse.getTokens());
        assertThat(analyzeResponse.getTokens().size(), equalTo(0));
    }

}
