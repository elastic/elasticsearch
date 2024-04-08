/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhrasePrefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.highlight;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHighlight;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class HighlighterWithAnalyzersTests extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CommonAnalysisPlugin.class);
    }

    public void testNgramHighlightingWithBrokenPositions() throws IOException {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("_doc")
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
                    .endObject()
            )
                .setSettings(
                    Settings.builder()
                        .put(indexSettings())
                        .put(IndexSettings.MAX_NGRAM_DIFF_SETTING.getKey(), 19)
                        .put("analysis.tokenizer.autocomplete.max_gram", 20)
                        .put("analysis.tokenizer.autocomplete.min_gram", 1)
                        .put("analysis.tokenizer.autocomplete.token_chars", "letter,digit")
                        .put("analysis.tokenizer.autocomplete.type", "ngram")
                        .put("analysis.filter.wordDelimiter.type", "word_delimiter")
                        .putList(
                            "analysis.filter.wordDelimiter.type_table",
                            "& => ALPHANUM",
                            "| => ALPHANUM",
                            "! => ALPHANUM",
                            "? => ALPHANUM",
                            ". => ALPHANUM",
                            "- => ALPHANUM",
                            "# => ALPHANUM",
                            "% => ALPHANUM",
                            "+ => ALPHANUM",
                            ", => ALPHANUM",
                            "~ => ALPHANUM",
                            ": => ALPHANUM",
                            "/ => ALPHANUM",
                            "^ => ALPHANUM",
                            "$ => ALPHANUM",
                            "@ => ALPHANUM",
                            ") => ALPHANUM",
                            "( => ALPHANUM",
                            "] => ALPHANUM",
                            "[ => ALPHANUM",
                            "} => ALPHANUM",
                            "{ => ALPHANUM"
                        )
                        .put("analysis.filter.wordDelimiter.type.split_on_numerics", false)
                        .put("analysis.filter.wordDelimiter.generate_word_parts", true)
                        .put("analysis.filter.wordDelimiter.generate_number_parts", false)
                        .put("analysis.filter.wordDelimiter.catenate_words", true)
                        .put("analysis.filter.wordDelimiter.catenate_numbers", true)
                        .put("analysis.filter.wordDelimiter.catenate_all", false)

                        .put("analysis.analyzer.autocomplete.tokenizer", "autocomplete")
                        .putList("analysis.analyzer.autocomplete.filter", "lowercase", "wordDelimiter")
                        .put("analysis.analyzer.search_autocomplete.tokenizer", "whitespace")
                        .putList("analysis.analyzer.search_autocomplete.filter", "lowercase", "wordDelimiter")
                )
        );
        prepareIndex("test").setId("1").setSource("name", "ARCOTEL Hotels Deutschland").get();
        refresh();
        assertResponse(
            prepareSearch("test").setQuery(matchQuery("name.autocomplete", "deut tel").operator(Operator.OR))
                .highlighter(new HighlightBuilder().field("name.autocomplete")),
            response -> assertHighlight(
                response,
                0,
                "name.autocomplete",
                0,
                equalTo("ARCO<em>TEL</em> Ho<em>tel</em>s <em>Deut</em>schland")
            )
        );

    }

    public void testMultiPhraseCutoff() throws IOException {
        /*
         * MultiPhraseQuery can literally kill an entire node if there are too many terms in the
         * query. We cut off and extract terms if there are more than 16 terms in the query
         */
        assertAcked(
            prepareCreate("test").setMapping(
                "body",
                "type=text,analyzer=custom_analyzer," + "search_analyzer=custom_analyzer,term_vector=with_positions_offsets"
            )
                .setSettings(
                    Settings.builder()
                        .put(indexSettings())
                        .put("analysis.filter.wordDelimiter.type", "word_delimiter")
                        .put("analysis.filter.wordDelimiter.type.split_on_numerics", false)
                        .put("analysis.filter.wordDelimiter.generate_word_parts", true)
                        .put("analysis.filter.wordDelimiter.generate_number_parts", true)
                        .put("analysis.filter.wordDelimiter.catenate_words", true)
                        .put("analysis.filter.wordDelimiter.catenate_numbers", true)
                        .put("analysis.filter.wordDelimiter.catenate_all", false)
                        .put("analysis.analyzer.custom_analyzer.tokenizer", "whitespace")
                        .putList("analysis.analyzer.custom_analyzer.filter", "lowercase", "wordDelimiter")
                )
        );

        ensureGreen();
        prepareIndex("test").setId("1")
            .setSource(
                "body",
                "Test: http://www.facebook.com http://elasticsearch.org "
                    + "http://xing.com http://cnn.com http://quora.com http://twitter.com this is "
                    + "a test for highlighting feature Test: http://www.facebook.com "
                    + "http://elasticsearch.org http://xing.com http://cnn.com http://quora.com "
                    + "http://twitter.com this is a test for highlighting feature"
            )
            .get();
        refresh();
        assertResponse(
            prepareSearch().setQuery(matchPhraseQuery("body", "Test: http://www.facebook.com "))
                .highlighter(new HighlightBuilder().field("body").highlighterType("fvh")),
            response -> assertHighlight(response, 0, "body", 0, startsWith("<em>Test: http://www.facebook.com</em>"))
        );

        assertResponse(
            prepareSearch().setQuery(
                matchPhraseQuery(
                    "body",
                    "Test: http://www.facebook.com "
                        + "http://elasticsearch.org http://xing.com http://cnn.com "
                        + "http://quora.com http://twitter.com this is a test for highlighting "
                        + "feature Test: http://www.facebook.com http://elasticsearch.org "
                        + "http://xing.com http://cnn.com http://quora.com http://twitter.com this "
                        + "is a test for highlighting feature"
                )
            ).highlighter(new HighlightBuilder().field("body").highlighterType("fvh")),
            response -> assertHighlight(
                response,
                0,
                "body",
                0,
                equalTo(
                    "<em>Test</em>: "
                        + "<em>http://www.facebook.com</em> <em>http://elasticsearch.org</em> "
                        + "<em>http://xing.com</em> <em>http://cnn.com</em> http://quora.com"
                )
            )
        );
    }

    public void testSynonyms() throws IOException {
        Settings.Builder builder = Settings.builder()
            .put(indexSettings())
            .put("index.analysis.analyzer.synonym.tokenizer", "standard")
            .putList("index.analysis.analyzer.synonym.filter", "synonym", "lowercase")
            .put("index.analysis.filter.synonym.type", "synonym")
            .putList("index.analysis.filter.synonym.synonyms", "fast,quick");

        assertAcked(
            prepareCreate("test").setSettings(builder.build())
                .setMapping(
                    "field1",
                    "type=text,term_vector=with_positions_offsets,search_analyzer=synonym," + "analyzer=standard,index_options=offsets"
                )
        );
        ensureGreen();

        prepareIndex("test").setId("0").setSource("field1", "The quick brown fox jumps over the lazy dog").get();
        refresh();
        for (String highlighterType : new String[] { "plain", "fvh", "unified" }) {
            logger.info("--> highlighting (type=" + highlighterType + ") and searching on field1");
            assertResponse(
                prepareSearch("test").setQuery(matchQuery("field1", "quick brown fox").operator(Operator.AND))
                    .highlighter(
                        highlight().field("field1").order("score").preTags("<x>").postTags("</x>").highlighterType(highlighterType)
                    ),
                resp -> {
                    assertHighlight(resp, 0, "field1", 0, 1, equalTo("The <x>quick</x> <x>brown</x> <x>fox</x> jumps over the lazy dog"));
                }
            );
            assertResponse(
                prepareSearch("test").setQuery(matchQuery("field1", "fast brown fox").operator(Operator.AND))
                    .highlighter(
                        highlight().field("field1").order("score").preTags("<x>").postTags("</x>").highlighterType(highlighterType)
                    ),
                resp -> {
                    assertHighlight(resp, 0, "field1", 0, 1, equalTo("The <x>quick</x> <x>brown</x> <x>fox</x> jumps over the lazy dog"));
                }
            );
        }
    }

    public void testPhrasePrefix() throws IOException {
        Settings.Builder builder = Settings.builder()
            .put(indexSettings())
            .put("index.analysis.analyzer.synonym.tokenizer", "standard")
            .putList("index.analysis.analyzer.synonym.filter", "synonym", "lowercase")
            .put("index.analysis.filter.synonym.type", "synonym")
            .putList("index.analysis.filter.synonym.synonyms", "quick => fast");

        assertAcked(prepareCreate("first_test_index").setSettings(builder.build()).setMapping(type1TermVectorMapping()));

        ensureGreen();

        prepareIndex("first_test_index").setId("0")
            .setSource("field0", "The quick brown fox jumps over the lazy dog", "field1", "The quick brown fox jumps over the lazy dog")
            .get();
        prepareIndex("first_test_index").setId("1").setSource("field1", "The quick browse button is a fancy thing, right bro?").get();
        refresh();
        logger.info("--> highlighting and searching on field0");

        assertResponse(
            prepareSearch("first_test_index").setQuery(matchPhrasePrefixQuery("field0", "bro"))
                .highlighter(highlight().field("field0").order("score").preTags("<x>").postTags("</x>")),
            resp -> {
                assertHighlight(resp, 0, "field0", 0, 1, equalTo("The quick <x>brown</x> fox jumps over the lazy dog"));
            }
        );

        assertResponse(
            prepareSearch("first_test_index").setQuery(matchPhrasePrefixQuery("field0", "quick bro"))
                .highlighter(highlight().field("field0").order("score").preTags("<x>").postTags("</x>")),
            resp -> {
                assertHighlight(resp, 0, "field0", 0, 1, equalTo("The <x>quick brown</x> fox jumps over the lazy dog"));
            }
        );

        logger.info("--> highlighting and searching on field1");
        assertResponse(
            prepareSearch("first_test_index").setQuery(
                boolQuery().should(matchPhrasePrefixQuery("field1", "test")).should(matchPhrasePrefixQuery("field1", "bro"))
            ).highlighter(highlight().field("field1").order("score").preTags("<x>").postTags("</x>")),
            resp -> {
                assertThat(resp.getHits().getTotalHits().value, equalTo(2L));
                for (int i = 0; i < 2; i++) {
                    assertHighlight(
                        resp,
                        i,
                        "field1",
                        0,
                        1,
                        anyOf(
                            equalTo("The quick <x>browse</x> button is a fancy thing, right <x>bro</x>?"),
                            equalTo("The quick <x>brown</x> fox jumps over the lazy dog")
                        )
                    );
                }
            }
        );

        assertResponse(
            prepareSearch("first_test_index").setQuery(matchPhrasePrefixQuery("field1", "quick bro"))
                .highlighter(highlight().field("field1").order("score").preTags("<x>").postTags("</x>")),
            resp -> {
                for (int i = 0; i < 2; i++) {
                    assertHighlight(
                        resp,
                        i,
                        "field1",
                        0,
                        1,
                        anyOf(
                            equalTo("The <x>quick browse</x> button is a fancy thing, right bro?"),
                            equalTo("The <x>quick brown</x> fox jumps over the lazy dog")
                        )
                    );
                }
            }
        );

        // with synonyms
        assertAcked(
            prepareCreate("second_test_index").setSettings(builder.build())
                .setMapping(
                    "field4",
                    "type=text,term_vector=with_positions_offsets,analyzer=synonym",
                    "field3",
                    "type=text,analyzer=synonym"
                )
        );
        prepareIndex("second_test_index").setId("0")
            .setSource(
                "type",
                "type2",
                "field4",
                "The quick brown fox jumps over the lazy dog",
                "field3",
                "The quick brown fox jumps over the lazy dog"
            )
            .get();
        prepareIndex("second_test_index").setId("1")
            .setSource("type", "type2", "field4", "The quick browse button is a fancy thing, right bro?")
            .get();
        prepareIndex("second_test_index").setId("2").setSource("type", "type2", "field4", "a quick fast blue car").get();
        refresh();

        assertResponse(
            prepareSearch("second_test_index").setQuery(matchPhrasePrefixQuery("field3", "fast bro"))
                .highlighter(highlight().field("field3").order("score").preTags("<x>").postTags("</x>")),
            resp -> {
                assertHighlight(resp, 0, "field3", 0, 1, equalTo("The <x>quick brown</x> fox jumps over the lazy dog"));
            }
        );

        assertResponse(
            prepareSearch("second_test_index").setQuery(matchPhrasePrefixQuery("field4", "the fast bro"))
                .highlighter(highlight().field("field4").order("score").preTags("<x>").postTags("</x>")),
            resp -> {
                for (int i = 0; i < 2; i++) {
                    assertHighlight(
                        resp,
                        i,
                        "field4",
                        0,
                        1,
                        anyOf(
                            equalTo("<x>The quick browse</x> button is a fancy thing, right bro?"),
                            equalTo("<x>The quick brown</x> fox jumps over the lazy dog")
                        )
                    );
                }
            }
        );

        logger.info("--> highlighting and searching on field4");
        assertResponse(
            prepareSearch("second_test_index").setQuery(matchPhrasePrefixQuery("field4", "a fast quick blue ca"))
                .setPostFilter(termQuery("type", "type2"))
                .highlighter(highlight().field("field4").order("score").preTags("<x>").postTags("</x>")),
            resp -> {
                assertHighlight(
                    resp,
                    0,
                    "field4",
                    0,
                    1,
                    anyOf(equalTo("<x>a quick fast blue car</x>"), equalTo("<x>a</x> <x>quick</x> <x>fast</x> <x>blue</x> <x>car</x>"))
                );
            }
        );
    }

    public static XContentBuilder type1TermVectorMapping() throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("field1")
            .field("type", "text")
            .field("term_vector", "with_positions_offsets")
            .endObject()
            .startObject("field2")
            .field("type", "text")
            .field("term_vector", "with_positions_offsets")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
    }
}
