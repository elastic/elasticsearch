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

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHighlight;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class HighlighterWithAnalyzersTests extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CommonAnalysisPlugin.class);
    }

    public void testNgramHighlightingWithBrokenPositions() throws IOException {
        assertAcked(prepareCreate("test")
                .addMapping("test", jsonBuilder()
                        .startObject()
                            .startObject("test")
                                .startObject("properties")
                                    .startObject("name")
                                        .field("type", "text")
                                        .startObject("fields")
                                            .startObject("autocomplete")
                                                .field("type", "text")
                                                .field("analyzer", "autocomplete")
                                                .field("search_analyzer", "search_autocomplete")
                                                .field("term_vector", "with_positions_offsets")
                                            .endObject()
                                        .endObject()
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject())
                .setSettings(Settings.builder()
                        .put(indexSettings())
                        .put(IndexSettings.MAX_NGRAM_DIFF_SETTING.getKey(), 19)
                        .put("analysis.tokenizer.autocomplete.max_gram", 20)
                        .put("analysis.tokenizer.autocomplete.min_gram", 1)
                        .put("analysis.tokenizer.autocomplete.token_chars", "letter,digit")
                        .put("analysis.tokenizer.autocomplete.type", "nGram")
                        .put("analysis.filter.wordDelimiter.type", "word_delimiter")
                        .putList("analysis.filter.wordDelimiter.type_table",
                                "& => ALPHANUM", "| => ALPHANUM", "! => ALPHANUM",
                                "? => ALPHANUM", ". => ALPHANUM", "- => ALPHANUM",
                                "# => ALPHANUM", "% => ALPHANUM", "+ => ALPHANUM",
                                ", => ALPHANUM", "~ => ALPHANUM", ": => ALPHANUM",
                                "/ => ALPHANUM", "^ => ALPHANUM", "$ => ALPHANUM",
                                "@ => ALPHANUM", ") => ALPHANUM", "( => ALPHANUM",
                                "] => ALPHANUM", "[ => ALPHANUM", "} => ALPHANUM",
                                "{ => ALPHANUM")
                        .put("analysis.filter.wordDelimiter.type.split_on_numerics", false)
                        .put("analysis.filter.wordDelimiter.generate_word_parts", true)
                        .put("analysis.filter.wordDelimiter.generate_number_parts", false)
                        .put("analysis.filter.wordDelimiter.catenate_words", true)
                        .put("analysis.filter.wordDelimiter.catenate_numbers", true)
                        .put("analysis.filter.wordDelimiter.catenate_all", false)

                        .put("analysis.analyzer.autocomplete.tokenizer", "autocomplete")
                        .putList("analysis.analyzer.autocomplete.filter",
                                "lowercase", "wordDelimiter")
                        .put("analysis.analyzer.search_autocomplete.tokenizer", "whitespace")
                        .putList("analysis.analyzer.search_autocomplete.filter",
                                "lowercase", "wordDelimiter")));
        client().prepareIndex("test", "test", "1")
            .setSource("name", "ARCOTEL Hotels Deutschland").get();
        refresh();
        SearchResponse search = client().prepareSearch("test").setTypes("test")
                .setQuery(matchQuery("name.autocomplete", "deut tel").operator(Operator.OR))
                .highlighter(new HighlightBuilder().field("name.autocomplete")).get();
        assertHighlight(search, 0, "name.autocomplete", 0,
                equalTo("ARCO<em>TEL</em> Ho<em>tel</em>s <em>Deut</em>schland"));
    }

    public void testMultiPhraseCutoff() throws IOException {
        /*
         * MultiPhraseQuery can literally kill an entire node if there are too many terms in the
         * query. We cut off and extract terms if there are more than 16 terms in the query
         */
        assertAcked(prepareCreate("test")
                .addMapping("test", "body", "type=text,analyzer=custom_analyzer,"
                        + "search_analyzer=custom_analyzer,term_vector=with_positions_offsets")
                .setSettings(
                        Settings.builder().put(indexSettings())
                                .put("analysis.filter.wordDelimiter.type", "word_delimiter")
                                .put("analysis.filter.wordDelimiter.type.split_on_numerics", false)
                                .put("analysis.filter.wordDelimiter.generate_word_parts", true)
                                .put("analysis.filter.wordDelimiter.generate_number_parts", true)
                                .put("analysis.filter.wordDelimiter.catenate_words", true)
                                .put("analysis.filter.wordDelimiter.catenate_numbers", true)
                                .put("analysis.filter.wordDelimiter.catenate_all", false)
                                .put("analysis.analyzer.custom_analyzer.tokenizer", "whitespace")
                                .putList("analysis.analyzer.custom_analyzer.filter",
                                        "lowercase", "wordDelimiter"))
        );

        ensureGreen();
        client().prepareIndex("test", "test", "1")
            .setSource("body", "Test: http://www.facebook.com http://elasticsearch.org "
                    + "http://xing.com http://cnn.com http://quora.com http://twitter.com this is "
                    + "a test for highlighting feature Test: http://www.facebook.com "
                    + "http://elasticsearch.org http://xing.com http://cnn.com http://quora.com "
                    + "http://twitter.com this is a test for highlighting feature")
            .get();
        refresh();
        SearchResponse search = client().prepareSearch()
                .setQuery(matchPhraseQuery("body", "Test: http://www.facebook.com "))
                .highlighter(new HighlightBuilder().field("body").highlighterType("fvh")).get();
        assertHighlight(search, 0, "body", 0, startsWith("<em>Test: http://www.facebook.com</em>"));
        search = client()
                .prepareSearch()
                .setQuery(matchPhraseQuery("body", "Test: http://www.facebook.com "
                        + "http://elasticsearch.org http://xing.com http://cnn.com "
                        + "http://quora.com http://twitter.com this is a test for highlighting "
                        + "feature Test: http://www.facebook.com http://elasticsearch.org "
                        + "http://xing.com http://cnn.com http://quora.com http://twitter.com this "
                        + "is a test for highlighting feature"))
                .highlighter(new HighlightBuilder().field("body").highlighterType("fvh")).execute().actionGet();
        assertHighlight(search, 0, "body", 0, equalTo("<em>Test</em>: "
                + "<em>http://www.facebook.com</em> <em>http://elasticsearch.org</em> "
                + "<em>http://xing.com</em> <em>http://cnn.com</em> http://quora.com"));
    }
}
