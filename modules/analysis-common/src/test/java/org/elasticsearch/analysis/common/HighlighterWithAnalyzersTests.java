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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhrasePrefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.highlight;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHighlight;
import static org.hamcrest.Matchers.anyOf;
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
                        .put("analysis.tokenizer.autocomplete.type", "ngram")
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
        SearchResponse search = client().prepareSearch("test")
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

    public void testSynonyms() throws IOException {
        Settings.Builder builder = Settings.builder()
            .put(indexSettings())
            .put("index.analysis.analyzer.synonym.tokenizer", "standard")
            .putList("index.analysis.analyzer.synonym.filter", "synonym", "lowercase")
            .put("index.analysis.filter.synonym.type", "synonym")
            .putList("index.analysis.filter.synonym.synonyms", "fast,quick");

        assertAcked(prepareCreate("test").setSettings(builder.build())
            .addMapping("type1", "field1",
                "type=text,term_vector=with_positions_offsets,search_analyzer=synonym," +
                    "analyzer=standard,index_options=offsets"));
        ensureGreen();

        client().prepareIndex("test", "type1", "0").setSource(
            "field1", "The quick brown fox jumps over the lazy dog").get();
        refresh();
        for (String highlighterType : new String[] {"plain", "fvh", "unified"}) {
            logger.info("--> highlighting (type=" + highlighterType + ") and searching on field1");
            SearchSourceBuilder source = searchSource()
                .query(matchQuery("field1", "quick brown fox").operator(Operator.AND))
                .highlighter(
                    highlight()
                        .field("field1")
                        .order("score")
                        .preTags("<x>")
                        .postTags("</x>")
                        .highlighterType(highlighterType));
            SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();
            assertHighlight(searchResponse, 0, "field1", 0, 1,
                equalTo("The <x>quick</x> <x>brown</x> <x>fox</x> jumps over the lazy dog"));

            source = searchSource()
                .query(matchQuery("field1", "fast brown fox").operator(Operator.AND))
                .highlighter(highlight().field("field1").order("score").preTags("<x>").postTags("</x>"));
            searchResponse = client().search(searchRequest("test").source(source)).actionGet();
            assertHighlight(searchResponse, 0, "field1", 0, 1,
                equalTo("The <x>quick</x> <x>brown</x> <x>fox</x> jumps over the lazy dog"));
        }
    }

    public void testPhrasePrefix() throws IOException {
        Settings.Builder builder = Settings.builder()
            .put(indexSettings())
            .put("index.analysis.analyzer.synonym.tokenizer", "standard")
            .putList("index.analysis.analyzer.synonym.filter", "synonym", "lowercase")
            .put("index.analysis.filter.synonym.type", "synonym")
            .putList("index.analysis.filter.synonym.synonyms", "quick => fast");

        assertAcked(prepareCreate("first_test_index").setSettings(builder.build()).addMapping("type1", type1TermVectorMapping()));

        ensureGreen();

        client().prepareIndex("first_test_index", "type1", "0").setSource(
            "field0", "The quick brown fox jumps over the lazy dog",
            "field1", "The quick brown fox jumps over the lazy dog").get();
        client().prepareIndex("first_test_index", "type1", "1").setSource("field1",
            "The quick browse button is a fancy thing, right bro?").get();
        refresh();
        logger.info("--> highlighting and searching on field0");

        SearchSourceBuilder source = searchSource()
            .query(matchPhrasePrefixQuery("field0", "bro"))
            .highlighter(highlight().field("field0").order("score").preTags("<x>").postTags("</x>"));
        SearchResponse searchResponse = client().search(searchRequest("first_test_index").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field0", 0, 1, equalTo("The quick <x>brown</x> fox jumps over the lazy dog"));

        source = searchSource()
            .query(matchPhrasePrefixQuery("field0", "quick bro"))
            .highlighter(highlight().field("field0").order("score").preTags("<x>").postTags("</x>"));

        searchResponse = client().search(searchRequest("first_test_index").source(source)).actionGet();
        assertHighlight(searchResponse, 0, "field0", 0, 1,
            equalTo("The <x>quick</x> <x>brown</x> fox jumps over the lazy dog"));

        logger.info("--> highlighting and searching on field1");
        source = searchSource()
            .query(boolQuery()
                .should(matchPhrasePrefixQuery("field1", "test"))
                .should(matchPhrasePrefixQuery("field1", "bro"))
            )
            .highlighter(highlight().field("field1").order("score").preTags("<x>").postTags("</x>"));

        searchResponse = client().search(searchRequest("first_test_index").source(source)).actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
        for (int i = 0; i < 2; i++) {
            assertHighlight(searchResponse, i, "field1", 0, 1, anyOf(
                equalTo("The quick <x>browse</x> button is a fancy thing, right <x>bro</x>?"),
                equalTo("The quick <x>brown</x> fox jumps over the lazy dog")));
        }

        source = searchSource()
            .query(matchPhrasePrefixQuery("field1", "quick bro"))
            .highlighter(highlight().field("field1").order("score").preTags("<x>").postTags("</x>"));

        searchResponse = client().search(searchRequest("first_test_index").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field1", 0, 1, anyOf(
            equalTo("The <x>quick</x> <x>browse</x> button is a fancy thing, right bro?"),
            equalTo("The <x>quick</x> <x>brown</x> fox jumps over the lazy dog")));
        assertHighlight(searchResponse, 1, "field1", 0, 1, anyOf(
            equalTo("The <x>quick</x> <x>browse</x> button is a fancy thing, right bro?"),
            equalTo("The <x>quick</x> <x>brown</x> fox jumps over the lazy dog")));

        assertAcked(prepareCreate("second_test_index").setSettings(builder.build()).addMapping("doc",
            "field4", "type=text,term_vector=with_positions_offsets,analyzer=synonym",
            "field3", "type=text,analyzer=synonym"));
        // with synonyms
        client().prepareIndex("second_test_index", "doc", "0").setSource(
            "type", "type2",
            "field4", "The quick brown fox jumps over the lazy dog",
            "field3", "The quick brown fox jumps over the lazy dog").get();
        client().prepareIndex("second_test_index", "doc", "1").setSource(
            "type", "type2",
            "field4", "The quick browse button is a fancy thing, right bro?").get();
        client().prepareIndex("second_test_index", "doc", "2").setSource(
            "type", "type2",
            "field4", "a quick fast blue car").get();
        refresh();

        source = searchSource().postFilter(termQuery("type", "type2")).query(matchPhrasePrefixQuery("field3", "fast bro"))
            .highlighter(highlight().field("field3").order("score").preTags("<x>").postTags("</x>"));

        searchResponse = client().search(searchRequest("second_test_index").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field3", 0, 1,
            equalTo("The <x>quick</x> <x>brown</x> fox jumps over the lazy dog"));

        logger.info("--> highlighting and searching on field4");
        source = searchSource().postFilter(termQuery("type", "type2")).query(matchPhrasePrefixQuery("field4", "the fast bro"))
            .highlighter(highlight().field("field4").order("score").preTags("<x>").postTags("</x>"));
        searchResponse = client().search(searchRequest("second_test_index").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field4", 0, 1, anyOf(
            equalTo("<x>The</x> <x>quick</x> <x>browse</x> button is a fancy thing, right bro?"),
            equalTo("<x>The</x> <x>quick</x> <x>brown</x> fox jumps over the lazy dog")));
        assertHighlight(searchResponse, 1, "field4", 0, 1, anyOf(
            equalTo("<x>The</x> <x>quick</x> <x>browse</x> button is a fancy thing, right bro?"),
            equalTo("<x>The</x> <x>quick</x> <x>brown</x> fox jumps over the lazy dog")));

        logger.info("--> highlighting and searching on field4");
        source = searchSource().postFilter(termQuery("type", "type2"))
            .query(matchPhrasePrefixQuery("field4", "a fast quick blue ca"))
            .highlighter(highlight().field("field4").order("score").preTags("<x>").postTags("</x>"));
        searchResponse = client().search(searchRequest("second_test_index").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field4", 0, 1,
            anyOf(equalTo("<x>a quick fast blue car</x>"),
                equalTo("<x>a</x> <x>quick</x> <x>fast</x> <x>blue</x> <x>car</x>")));
    }

    public static XContentBuilder type1TermVectorMapping() throws IOException {
        return XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties")
            .startObject("field1").field("type", "text").field("term_vector", "with_positions_offsets").endObject()
            .startObject("field2").field("type", "text").field("term_vector", "with_positions_offsets").endObject()
            .endObject()
            .endObject().endObject();
    }
}
