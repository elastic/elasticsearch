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

package org.elasticsearch.analysis.common;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

/**
 * Contains java integration tests that were in server module, but due to that fact
 * that char filter, tokenizer, token filter or analyzer has been moved to common analysis
 * are now part of this test.
 */
public class ServerTestsThatRelyOnCommonAnalysisIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CommonAnalysisPlugin.class);
    }

    // see #5120
    // Note: moved from org.elasticsearch.search.query.SearchQueryIT as it relies on this module now.
    public void testNGramCopyField() {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(Settings.builder()
            .put(indexSettings())
            .put(IndexSettings.MAX_NGRAM_DIFF_SETTING.getKey(), 9)
            .put("index.analysis.analyzer.my_ngram_analyzer.type", "custom")
            .put("index.analysis.analyzer.my_ngram_analyzer.tokenizer", "my_ngram_tokenizer")
            .put("index.analysis.tokenizer.my_ngram_tokenizer.type", "nGram")
            .put("index.analysis.tokenizer.my_ngram_tokenizer.min_gram", "1")
            .put("index.analysis.tokenizer.my_ngram_tokenizer.max_gram", "10")
            .putList("index.analysis.tokenizer.my_ngram_tokenizer.token_chars", new String[0]));
        assertAcked(builder.addMapping("test", "origin", "type=text,copy_to=meta", "meta", "type=text,analyzer=my_ngram_analyzer"));
        // we only have ngrams as the index analyzer so searches will get standard analyzer


        client().prepareIndex("test", "test", "1").setSource("origin", "C.A1234.5678")
            .setRefreshPolicy(IMMEDIATE)
            .get();

        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(matchQuery("meta", "1234"))
            .get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch("test")
            .setQuery(matchQuery("meta", "1234.56"))
            .get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch("test")
            .setQuery(termQuery("meta", "A1234"))
            .get();
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch("test")
            .setQuery(termQuery("meta", "a1234"))
            .get();
        assertHitCount(searchResponse, 0L); // it's upper case

        searchResponse = client().prepareSearch("test")
            .setQuery(matchQuery("meta", "A1234").analyzer("my_ngram_analyzer"))
            .get(); // force ngram analyzer
        assertHitCount(searchResponse, 1L);

        searchResponse = client().prepareSearch("test")
            .setQuery(matchQuery("meta", "a1234").analyzer("my_ngram_analyzer"))
            .get(); // this one returns a hit since it's default operator is OR
        assertHitCount(searchResponse, 1L);
    }

    // Note: moved from org.elasticsearch.indices.analyze.AnalyzeActionIT
    public void testCustomTokenizerInRequest() throws Exception {
        Map<String, Object> tokenizerSettings = new HashMap<>();
        tokenizerSettings.put("type", "nGram");
        tokenizerSettings.put("min_gram", 2);
        tokenizerSettings.put("max_gram", 2);

        AnalyzeResponse analyzeResponse = client().admin().indices()
            .prepareAnalyze()
            .setText("good")
            .setTokenizer(tokenizerSettings)
            .setExplain(true)
            .get();

        //tokenizer
        assertThat(analyzeResponse.detail().tokenizer().getName(), equalTo("_anonymous_tokenizer"));
        assertThat(analyzeResponse.detail().tokenizer().getTokens().length, equalTo(3));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[0].getTerm(), equalTo("go"));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[0].getStartOffset(), equalTo(0));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[0].getEndOffset(), equalTo(2));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[0].getPosition(), equalTo(0));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[0].getPositionLength(), equalTo(1));

        assertThat(analyzeResponse.detail().tokenizer().getTokens()[1].getTerm(), equalTo("oo"));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[1].getStartOffset(), equalTo(1));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[1].getEndOffset(), equalTo(3));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[1].getPosition(), equalTo(1));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[1].getPositionLength(), equalTo(1));

        assertThat(analyzeResponse.detail().tokenizer().getTokens()[2].getTerm(), equalTo("od"));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[2].getStartOffset(), equalTo(2));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[2].getEndOffset(), equalTo(4));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[2].getPosition(), equalTo(2));
        assertThat(analyzeResponse.detail().tokenizer().getTokens()[2].getPositionLength(), equalTo(1));
    }

}
