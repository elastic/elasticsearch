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
package org.elasticsearch.search.highlight;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.index.search.MatchQuery.Type;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder.Field;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.boostingQuery;
import static org.elasticsearch.index.query.QueryBuilders.commonTermsQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.fuzzyQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhrasePrefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.regexpQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.typeQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.highlight;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHighlight;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNotHighlighted;
import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class HighlighterSearchIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testHighlightingWithWildcardName() throws IOException {
        // test the kibana case with * as fieldname that will try highlight all fields including meta fields
        XContentBuilder mappings = jsonBuilder();
        mappings.startObject();
        mappings.startObject("type")
                .startObject("properties")
                .startObject("text")
                .field("type", "text")
                .field("analyzer", "keyword")
                .field("index_options", "offsets")
                .field("term_vector", "with_positions_offsets")
                .endObject()
                .endObject()
                .endObject();
        mappings.endObject();
        assertAcked(prepareCreate("test")
                .addMapping("type", mappings));
        ensureYellow();
        client().prepareIndex("test", "type", "1")
                .setSource(jsonBuilder().startObject().field("text", "text").endObject())
                .get();
        refresh();
        String highlighter = randomFrom("plain", "postings", "fvh");
        SearchResponse search = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("text", "text")))
                .highlighter(new HighlightBuilder().field(new Field("*").highlighterType(highlighter))).get();
        assertHighlight(search, 0, "text", 0, equalTo("<em>text</em>"));
    }

    public void testHighlightingWhenFieldsAreNotStoredThereIsNoSource() throws IOException {
        XContentBuilder mappings = jsonBuilder();
        mappings.startObject();
        mappings.startObject("type")
                .startObject("_source")
                .field("enabled", false)
                .endObject()
                .startObject("properties")
                .startObject("unstored_field")
                .field("index_options", "offsets")
                .field("term_vector", "with_positions_offsets")
                .field("type", "text")
                .field("store", false)
                .endObject()
                .startObject("text")
                .field("index_options", "offsets")
                .field("term_vector", "with_positions_offsets")
                .field("type", "text")
                .field("store", true)
                .endObject()
                .endObject()
                .endObject();
        mappings.endObject();
        assertAcked(prepareCreate("test")
                .addMapping("type", mappings));
        ensureYellow();
        client().prepareIndex("test", "type", "1")
                .setSource(jsonBuilder().startObject().field("unstored_text", "text").field("text", "text").endObject())
                .get();
        refresh();
        String highlighter = randomFrom("plain", "postings", "fvh");
        SearchResponse search = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("text", "text")))
                .highlighter(new HighlightBuilder().field(new Field("*").highlighterType(highlighter))).get();
        assertHighlight(search, 0, "text", 0, equalTo("<em>text</em>"));
        search = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("text", "text")))
                .highlighter(new HighlightBuilder().field(new Field("unstored_text"))).get();
        assertNoFailures(search);
        assertThat(search.getHits().getAt(0).getHighlightFields().size(), equalTo(0));
    }

    // see #3486
    public void testHighTermFrequencyDoc() throws IOException {
        assertAcked(prepareCreate("test")
                .addMapping("test", "name", "type=text,term_vector=with_positions_offsets,store=" + randomBoolean()));
        ensureYellow();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 6000; i++) {
            builder.append("abc").append(" ");
        }
        client().prepareIndex("test", "test", "1")
            .setSource("name", builder.toString())
            .get();
        refresh();
        SearchResponse search = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("name", "abc")))
                .highlighter(new HighlightBuilder().field("name")).get();
        assertHighlight(search, 0, "name", 0, startsWith("<em>abc</em> <em>abc</em> <em>abc</em> <em>abc</em>"));
    }

    public void testNgramHighlightingWithBrokenPositions() throws IOException {
        assertAcked(prepareCreate("test")
                .addMapping("test", jsonBuilder()
                        .startObject()
                        .startObject("test")
                        .startObject("properties")
                        .startObject("name")
                        .startObject("fields")
                        .startObject("autocomplete")
                        .field("type", "text")
                        .field("analyzer", "autocomplete")
                        .field("search_analyzer", "search_autocomplete")
                        .field("term_vector", "with_positions_offsets")
                        .endObject()
                        .endObject()
                        .field("type", "text")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject())
                .setSettings(Settings.builder()
                        .put(indexSettings())
                        .put("analysis.tokenizer.autocomplete.max_gram", 20)
                        .put("analysis.tokenizer.autocomplete.min_gram", 1)
                        .put("analysis.tokenizer.autocomplete.token_chars", "letter,digit")
                        .put("analysis.tokenizer.autocomplete.type", "nGram")
                        .put("analysis.filter.wordDelimiter.type", "word_delimiter")
                        .putArray("analysis.filter.wordDelimiter.type_table",
                                "& => ALPHANUM", "| => ALPHANUM", "! => ALPHANUM",
                                "? => ALPHANUM", ". => ALPHANUM", "- => ALPHANUM", "# => ALPHANUM", "% => ALPHANUM",
                                "+ => ALPHANUM", ", => ALPHANUM", "~ => ALPHANUM", ": => ALPHANUM", "/ => ALPHANUM",
                                "^ => ALPHANUM", "$ => ALPHANUM", "@ => ALPHANUM", ") => ALPHANUM", "( => ALPHANUM",
                                "] => ALPHANUM", "[ => ALPHANUM", "} => ALPHANUM", "{ => ALPHANUM")

                        .put("analysis.filter.wordDelimiter.type.split_on_numerics", false)
                        .put("analysis.filter.wordDelimiter.generate_word_parts", true)
                        .put("analysis.filter.wordDelimiter.generate_number_parts", false)
                        .put("analysis.filter.wordDelimiter.catenate_words", true)
                        .put("analysis.filter.wordDelimiter.catenate_numbers", true)
                        .put("analysis.filter.wordDelimiter.catenate_all", false)

                        .put("analysis.analyzer.autocomplete.tokenizer", "autocomplete")
                        .putArray("analysis.analyzer.autocomplete.filter", "lowercase", "wordDelimiter")
                        .put("analysis.analyzer.search_autocomplete.tokenizer", "whitespace")
                        .putArray("analysis.analyzer.search_autocomplete.filter", "lowercase", "wordDelimiter")));
        ensureYellow();
        client().prepareIndex("test", "test", "1")
            .setSource("name", "ARCOTEL Hotels Deutschland").get();
        refresh();
        SearchResponse search = client().prepareSearch("test").setTypes("test")
                .setQuery(matchQuery("name.autocomplete", "deut tel").operator(Operator.OR))
                .highlighter(new HighlightBuilder().field("name.autocomplete")).execute().actionGet();
        assertHighlight(search, 0, "name.autocomplete", 0, equalTo("ARCO<em>TEL</em> Ho<em>tel</em>s <em>Deut</em>schland"));
    }

    public void testMultiPhraseCutoff() throws IOException {
        /*
         * MultiPhraseQuery can literally kill an entire node if there are too many terms in the
         * query. We cut off and extract terms if there are more than 16 terms in the query
         */
        assertAcked(prepareCreate("test")
                .addMapping("test", "body", "type=text,analyzer=custom_analyzer,search_analyzer=custom_analyzer,term_vector=with_positions_offsets")
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
                                .putArray("analysis.analyzer.custom_analyzer.filter", "lowercase", "wordDelimiter"))
        );

        ensureGreen();
        client().prepareIndex("test", "test", "1")
            .setSource("body", "Test: http://www.facebook.com http://elasticsearch.org http://xing.com http://cnn.com http://quora.com http://twitter.com this is a test for highlighting feature Test: http://www.facebook.com http://elasticsearch.org http://xing.com http://cnn.com http://quora.com http://twitter.com this is a test for highlighting feature")
            .get();
        refresh();
        SearchResponse search = client().prepareSearch().setQuery(matchQuery("body", "Test: http://www.facebook.com ").type(Type.PHRASE))
                .highlighter(new HighlightBuilder().field("body")).execute().actionGet();
        assertHighlight(search, 0, "body", 0, startsWith("<em>Test: http://www.facebook.com</em>"));
        search = client()
                .prepareSearch()
                .setQuery(
                        matchQuery(
                                "body",
                                "Test: http://www.facebook.com http://elasticsearch.org http://xing.com http://cnn.com http://quora.com http://twitter.com this is a test for highlighting feature Test: http://www.facebook.com http://elasticsearch.org http://xing.com http://cnn.com http://quora.com http://twitter.com this is a test for highlighting feature")
                                .type(Type.PHRASE)).highlighter(new HighlightBuilder().field("body")).execute().actionGet();
        assertHighlight(
                search,
                0,
                "body",
                0,
                equalTo("<em>Test</em>: <em>http://www.facebook.com</em> <em>http://elasticsearch.org</em> <em>http://xing.com</em> <em>http://cnn.com</em> http://quora.com"));
    }

    public void testNgramHighlighting() throws IOException {
        assertAcked(prepareCreate("test")
                .addMapping("test",
                        "name", "type=text,analyzer=name_index_analyzer,search_analyzer=name_search_analyzer,term_vector=with_positions_offsets",
                        "name2", "type=text,analyzer=name2_index_analyzer,search_analyzer=name_search_analyzer,term_vector=with_positions_offsets")
                .setSettings(Settings.builder()
                        .put(indexSettings())
                        .put("analysis.filter.my_ngram.max_gram", 20)
                        .put("analysis.filter.my_ngram.min_gram", 1)
                        .put("analysis.filter.my_ngram.type", "ngram")
                        .put("analysis.tokenizer.my_ngramt.max_gram", 20)
                        .put("analysis.tokenizer.my_ngramt.min_gram", 1)
                        .put("analysis.tokenizer.my_ngramt.token_chars", "letter,digit")
                        .put("analysis.tokenizer.my_ngramt.type", "ngram")
                        .put("analysis.analyzer.name_index_analyzer.tokenizer", "my_ngramt")
                        .put("analysis.analyzer.name2_index_analyzer.tokenizer", "whitespace")
                        .put("analysis.analyzer.name2_index_analyzer.filter", "my_ngram")
                        .put("analysis.analyzer.name_search_analyzer.tokenizer", "whitespace")));
        client().prepareIndex("test", "test", "1")
            .setSource("name", "logicacmg ehemals avinci - the know how company",
                       "name2", "logicacmg ehemals avinci - the know how company").get();
        refresh();
        ensureGreen();
        SearchResponse search = client().prepareSearch().setQuery(matchQuery("name", "logica m"))
                .highlighter(new HighlightBuilder().field("name")).get();
        assertHighlight(search, 0, "name", 0, equalTo("<em>logica</em>c<em>m</em>g ehe<em>m</em>als avinci - the know how co<em>m</em>pany"));

        search = client().prepareSearch().setQuery(matchQuery("name", "logica ma")).highlighter(new HighlightBuilder().field("name")).get();
        assertHighlight(search, 0, "name", 0, equalTo("<em>logica</em>cmg ehe<em>ma</em>ls avinci - the know how company"));

        search = client().prepareSearch().setQuery(matchQuery("name", "logica")).highlighter(new HighlightBuilder().field("name")).get();
        assertHighlight(search, 0, "name", 0, equalTo("<em>logica</em>cmg ehemals avinci - the know how company"));

        search = client().prepareSearch().setQuery(matchQuery("name2", "logica m")).highlighter(new HighlightBuilder().field("name2"))
                .get();
        assertHighlight(search, 0, "name2", 0, equalTo("<em>logicacmg</em> <em>ehemals</em> avinci - the know how <em>company</em>"));

        search = client().prepareSearch().setQuery(matchQuery("name2", "logica ma")).highlighter(new HighlightBuilder().field("name2"))
                .get();
        assertHighlight(search, 0, "name2", 0, equalTo("<em>logicacmg</em> <em>ehemals</em> avinci - the know how company"));

        search = client().prepareSearch().setQuery(matchQuery("name2", "logica")).highlighter(new HighlightBuilder().field("name2")).get();
        assertHighlight(search, 0, "name2", 0, equalTo("<em>logicacmg</em> ehemals avinci - the know how company"));
    }

    public void testEnsureNoNegativeOffsets() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1",
                        "no_long_term", "type=text,term_vector=with_positions_offsets",
                        "long_term", "type=text,term_vector=with_positions_offsets"));
        ensureYellow();

        client().prepareIndex("test", "type1", "1")
                .setSource("no_long_term", "This is a test where foo is highlighed and should be highlighted",
                        "long_term", "This is a test thisisaverylongwordandmakessurethisfails where foo is highlighed and should be highlighted")
                .get();
        refresh();

        SearchResponse search = client().prepareSearch()
                .setQuery(matchQuery("long_term", "thisisaverylongwordandmakessurethisfails foo highlighed"))
                .highlighter(new HighlightBuilder().field("long_term", 18, 1))
                .get();
        assertHighlight(search, 0, "long_term", 0, 1, equalTo("<em>thisisaverylongwordandmakessurethisfails</em>"));

        search = client().prepareSearch()
                .setQuery(matchQuery("no_long_term", "test foo highlighed").type(Type.PHRASE).slop(3))
                .highlighter(new HighlightBuilder().field("no_long_term", 18, 1).postTags("</b>").preTags("<b>"))
                .get();
        assertNotHighlighted(search, 0, "no_long_term");

        search = client().prepareSearch()
                .setQuery(matchQuery("no_long_term", "test foo highlighed").type(Type.PHRASE).slop(3))
                .highlighter(new HighlightBuilder().field("no_long_term", 30, 1).postTags("</b>").preTags("<b>"))
                .get();

        assertHighlight(search, 0, "no_long_term", 0, 1, equalTo("a <b>test</b> where <b>foo</b> is <b>highlighed</b> and"));
    }

    public void testSourceLookupHighlightingUsingPlainHighlighter() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        // we don't store title and don't use term vector, now lets see if it works...
                        .startObject("title").field("type", "text").field("store", false).field("term_vector", "no").endObject()
                        .startObject("attachments").startObject("properties").startObject("body").field("type", "text").field("store", false).field("term_vector", "no").endObject().endObject().endObject()
                        .endObject().endObject().endObject()));
        ensureYellow();

        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[5];
        for (int i = 0; i < indexRequestBuilders.length; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("title", "This is a test on the highlighting bug present in elasticsearch")
                            .startArray("attachments").startObject().field("body", "attachment 1").endObject().startObject().field("body", "attachment 2").endObject().endArray()
                            .endObject());
        }
        indexRandom(true, indexRequestBuilders);

        SearchResponse search = client().prepareSearch()
                .setQuery(matchQuery("title", "bug"))
                .highlighter(new HighlightBuilder().field("title", -1, 0))
                .get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            assertHighlight(search, i, "title", 0, equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
        }

        search = client().prepareSearch()
                .setQuery(matchQuery("attachments.body", "attachment"))
                .highlighter(new HighlightBuilder().field("attachments.body", -1, 0))
                .get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            assertHighlight(search, i, "attachments.body", 0, equalTo("<em>attachment</em> 1"));
            assertHighlight(search, i, "attachments.body", 1, equalTo("<em>attachment</em> 2"));
        }
    }

    public void testSourceLookupHighlightingUsingFastVectorHighlighter() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        // we don't store title, now lets see if it works...
                        .startObject("title").field("type", "text").field("store", false).field("term_vector", "with_positions_offsets").endObject()
                        .startObject("attachments").startObject("properties").startObject("body").field("type", "text").field("store", false).field("term_vector", "with_positions_offsets").endObject().endObject().endObject()
                        .endObject().endObject().endObject()));
        ensureYellow();

        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[5];
        for (int i = 0; i < indexRequestBuilders.length; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("title", "This is a test on the highlighting bug present in elasticsearch")
                            .startArray("attachments").startObject().field("body", "attachment 1").endObject().startObject().field("body", "attachment 2").endObject().endArray()
                            .endObject());
        }
        indexRandom(true, indexRequestBuilders);

        SearchResponse search = client().prepareSearch()
                .setQuery(matchQuery("title", "bug"))
                .highlighter(new HighlightBuilder().field("title", -1, 0))
                .get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            assertHighlight(search, i, "title", 0, equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
        }

        search = client().prepareSearch()
                .setQuery(matchQuery("attachments.body", "attachment"))
                .highlighter(new HighlightBuilder().field("attachments.body", -1, 2))
                .execute().get();

        for (int i = 0; i < 5; i++) {
            assertHighlight(search, i, "attachments.body", 0, equalTo("<em>attachment</em> 1"));
            assertHighlight(search, i, "attachments.body", 1, equalTo("<em>attachment</em> 2"));
        }
    }

    public void testSourceLookupHighlightingUsingPostingsHighlighter() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        // we don't store title, now lets see if it works...
                        .startObject("title").field("type", "text").field("store", false).field("index_options", "offsets").endObject()
                        .startObject("attachments").startObject("properties").startObject("body").field("type", "text").field("store", false).field("index_options", "offsets").endObject().endObject().endObject()
                        .endObject().endObject().endObject()));
        ensureYellow();

        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[5];
        for (int i = 0; i < indexRequestBuilders.length; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .array("title", "This is a test on the highlighting bug present in elasticsearch. Hopefully it works.",
                                    "This is the second bug to perform highlighting on.")
                            .startArray("attachments").startObject().field("body", "attachment for this test").endObject().startObject().field("body", "attachment 2").endObject().endArray()
                            .endObject());
        }
        indexRandom(true, indexRequestBuilders);

        SearchResponse search = client().prepareSearch()
                .setQuery(matchQuery("title", "bug"))
                //asking for the whole field to be highlighted
                .highlighter(new HighlightBuilder().field("title", -1, 0)).get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            assertHighlight(search, i, "title", 0, equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch. Hopefully it works."));
            assertHighlight(search, i, "title", 1, 2, equalTo("This is the second <em>bug</em> to perform highlighting on."));
        }

        search = client().prepareSearch()
                .setQuery(matchQuery("title", "bug"))
                //sentences will be generated out of each value
                .highlighter(new HighlightBuilder().field("title")).get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            assertHighlight(search, i, "title", 0, equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch."));
            assertHighlight(search, i, "title", 1, 2, equalTo("This is the second <em>bug</em> to perform highlighting on."));
        }

        search = client().prepareSearch()
                .setQuery(matchQuery("attachments.body", "attachment"))
                .highlighter(new HighlightBuilder().field("attachments.body", -1, 2))
                .get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            assertHighlight(search, i, "attachments.body", 0, equalTo("<em>attachment</em> for this test"));
            assertHighlight(search, i, "attachments.body", 1, 2, equalTo("<em>attachment</em> 2"));
        }
    }

    public void testHighlightIssue1994() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "title", "type=text,store=false", "titleTV", "type=text,store=false,term_vector=with_positions_offsets"));
        ensureYellow();

        indexRandom(false, client().prepareIndex("test", "type1", "1")
                .setSource("title", new String[]{"This is a test on the highlighting bug present in elasticsearch", "The bug is bugging us"},
                        "titleTV", new String[]{"This is a test on the highlighting bug present in elasticsearch", "The bug is bugging us"}));

        indexRandom(true, client().prepareIndex("test", "type1", "2")
                .setSource("titleTV", new String[]{"some text to highlight", "highlight other text"}));

        SearchResponse search = client().prepareSearch()
                .setQuery(matchQuery("title", "bug"))
                .highlighter(new HighlightBuilder().field("title", -1, 2).field("titleTV", -1, 2).requireFieldMatch(false))
                .get();

        assertHighlight(search, 0, "title", 0, equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
        assertHighlight(search, 0, "title", 1, 2, equalTo("The <em>bug</em> is bugging us"));
        assertHighlight(search, 0, "titleTV", 0, equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
        assertHighlight(search, 0, "titleTV", 1, 2, equalTo("The <em>bug</em> is bugging us"));

        search = client().prepareSearch()
                .setQuery(matchQuery("titleTV", "highlight"))
                .highlighter(new HighlightBuilder().field("titleTV", -1, 2))
                .get();

        assertHighlight(search, 0, "titleTV", 0, equalTo("some text to <em>highlight</em>"));
        assertHighlight(search, 0, "titleTV", 1, 2, equalTo("<em>highlight</em> other text"));
    }

    public void testGlobalHighlightingSettingsOverriddenAtFieldLevel() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field1", new String[]{"this is a test", "this is the second test"},
                        "field2", new String[]{"this is another test", "yet another test"}).get();
        refresh();

        logger.info("--> highlighting and searching on field1 and field2 produces different tags");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "test"))
                .highlighter(highlight().order("score").preTags("<global>").postTags("</global>").fragmentSize(1).numOfFragments(1)
                        .field(new HighlightBuilder.Field("field1").numOfFragments(2))
                        .field(new HighlightBuilder.Field("field2").preTags("<field2>").postTags("</field2>").fragmentSize(50).requireFieldMatch(false)));

        SearchResponse searchResponse = client().prepareSearch("test").setSource(source).get();

        assertHighlight(searchResponse, 0, "field1", 0, 2, equalTo(" <global>test</global>"));
        assertHighlight(searchResponse, 0, "field1", 1, 2, equalTo(" <global>test</global>"));
        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("this is another <field2>test</field2>"));
    }

    // Issue #5175
    public void testHighlightingOnWildcardFields() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1",
                        "field-postings", "type=text,index_options=offsets",
                        "field-fvh", "type=text,term_vector=with_positions_offsets",
                        "field-plain", "type=text"));
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field-postings", "This is the first test sentence. Here is the second one.",
                        "field-fvh", "This is the test with term_vectors",
                        "field-plain", "This is the test for the plain highlighter").get();
        refresh();

        logger.info("--> highlighting and searching on field*");
        SearchSourceBuilder source = searchSource()
                //postings hl doesn't support require_field_match, its field needs to be queried directly
                .query(termQuery("field-postings", "test"))
                .highlighter(highlight().field("field*").preTags("<xxx>").postTags("</xxx>").requireFieldMatch(false));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field-postings", 0, 1, equalTo("This is the first <xxx>test</xxx> sentence."));
        assertHighlight(searchResponse, 0, "field-fvh", 0, 1, equalTo("This is the <xxx>test</xxx> with term_vectors"));
        assertHighlight(searchResponse, 0, "field-plain", 0, 1, equalTo("This is the <xxx>test</xxx> for the plain highlighter"));
    }

    public void testForceSourceWithSourceDisabled() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1")
                        .startObject("_source").field("enabled", false).endObject()
                        .startObject("properties")
                        .startObject("field1").field("type", "text").field("store", true).field("index_options", "offsets")
                        .field("term_vector", "with_positions_offsets").endObject()
                        .startObject("field2").field("type", "text").endObject()
                        .endObject().endObject().endObject()));

        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field1", "The quick brown fox jumps over the lazy dog", "field2", "second field content").get();
        refresh();

        //works using stored field
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(termQuery("field1", "quick"))
                .highlighter(new HighlightBuilder().field(new Field("field1").preTags("<xxx>").postTags("</xxx>")))
                .get();
        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));

        assertFailures(client().prepareSearch("test")
                        .setQuery(termQuery("field1", "quick"))
                        .highlighter(
                                new HighlightBuilder().field(new Field("field1").preTags("<xxx>").postTags("</xxx>")
                                        .highlighterType("plain").forceSource(true))),
                RestStatus.BAD_REQUEST,
                containsString("source is forced for fields [field1] but type [type1] has disabled _source"));

        assertFailures(client().prepareSearch("test")
                        .setQuery(termQuery("field1", "quick"))
                        .highlighter(
                                new HighlightBuilder().field(new Field("field1").preTags("<xxx>").postTags("</xxx>").highlighterType("fvh")
                                        .forceSource(true))),
                RestStatus.BAD_REQUEST,
                containsString("source is forced for fields [field1] but type [type1] has disabled _source"));

        assertFailures(client().prepareSearch("test")
                .setQuery(termQuery("field1", "quick"))
                        .highlighter(
                                new HighlightBuilder().field(new Field("field1").preTags("<xxx>").postTags("</xxx>")
                                        .highlighterType("postings").forceSource(true))),
                RestStatus.BAD_REQUEST,
                containsString("source is forced for fields [field1] but type [type1] has disabled _source"));

        SearchSourceBuilder searchSource = SearchSourceBuilder.searchSource().query(termQuery("field1", "quick"))
                .highlighter(highlight().forceSource(true).field("field1"));
        assertFailures(client().prepareSearch("test").setSource(searchSource),
                RestStatus.BAD_REQUEST,
                containsString("source is forced for fields [field1] but type [type1] has disabled _source"));

        searchSource = SearchSourceBuilder.searchSource().query(termQuery("field1", "quick"))
                .highlighter(highlight().forceSource(true).field("field*"));
        assertFailures(client().prepareSearch("test").setSource(searchSource),
                RestStatus.BAD_REQUEST,
                matches("source is forced for fields \\[field\\d, field\\d\\] but type \\[type1\\] has disabled _source"));
    }

    public void testPlainHighlighter() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog").get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "test"))
                .highlighter(highlight().field("field1").order("score").preTags("<xxx>").postTags("</xxx>"));

        SearchResponse searchResponse = client().prepareSearch("test").setSource(source).get();

        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field1");
        source = searchSource()
                .query(termQuery("_all", "test"))
                .highlighter(highlight().field("field1").order("score").preTags("<xxx>").postTags("</xxx>").requireFieldMatch(false));

        searchResponse = client().prepareSearch("test").setSource(source).get();

        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(termQuery("_all", "quick"))
                .highlighter(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>").requireFieldMatch(false));

        searchResponse = client().prepareSearch("test").setSource(source).get();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(prefixQuery("_all", "qui"))
                .highlighter(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>").requireFieldMatch(false));

        searchResponse = client().prepareSearch("test").setSource(source).get();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));

        logger.info("--> searching on _all with constant score, highlighting on field2");
        source = searchSource()
                .query(constantScoreQuery(prefixQuery("_all", "qui")))
                .highlighter(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>").requireFieldMatch(false));

        searchResponse = client().prepareSearch("test").setSource(source).get();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));

        logger.info("--> searching on _all with constant score, highlighting on field2");
        source = searchSource()
                .query(boolQuery().should(constantScoreQuery(prefixQuery("_all", "qui"))))
                .highlighter(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>").requireFieldMatch(false));

        searchResponse = client().prepareSearch("test").setSource(source).get();
        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));
    }

    public void testFastVectorHighlighter() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", type1TermVectorMapping()));
        ensureGreen();

        indexRandom(true, client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog"));

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "test"))
                .highlighter(highlight().field("field1", 100, 0).order("score").preTags("<xxx>").postTags("</xxx>"));

        SearchResponse searchResponse = client().prepareSearch("test").setSource(source).get();

        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field1");
        source = searchSource()
                .query(termQuery("_all", "test"))
                .highlighter(highlight().field("field1", 100, 0).order("score").preTags("<xxx>").postTags("</xxx>").requireFieldMatch(false));

        searchResponse = client().prepareSearch("test").setSource(source).get();

        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(termQuery("_all", "quick"))
                .highlighter(highlight().field("field2", 100, 0).order("score").preTags("<xxx>").postTags("</xxx>").requireFieldMatch(false));

        searchResponse = client().prepareSearch("test").setSource(source).get();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(prefixQuery("_all", "qui"))
                .highlighter(highlight().field("field2", 100, 0).order("score").preTags("<xxx>").postTags("</xxx>").requireFieldMatch(false));

        searchResponse = client().prepareSearch("test").setSource(source).get();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));

        logger.info("--> searching with boundary characters");
        source = searchSource()
                .query(matchQuery("field2", "quick"))
                .highlighter(highlight().field("field2", 30, 1).boundaryChars(new char[] {' '}));

        searchResponse = client().prepareSearch("test").setSource(source).get();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <em>quick</em> brown fox jumps over"));

        logger.info("--> searching with boundary characters on the field");
        source = searchSource()
                .query(matchQuery("field2", "quick"))
                .highlighter(highlight().field(new Field("field2").fragmentSize(30).numOfFragments(1).boundaryChars(new char[] {' '})));

        searchResponse = client().prepareSearch("test").setSource(source).get();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <em>quick</em> brown fox jumps over"));

    }

    /**
     * The FHV can spend a long time highlighting degenerate documents if
     * phraseLimit is not set. Its default is now reasonably low.
     */
    public void testFVHManyMatches() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", type1TermVectorMapping()));
        ensureGreen();

        // Index one megabyte of "t   " over and over and over again
        String pattern = "t   ";
        String value = new String(new char[1024 * 256 / pattern.length()]).replace("\0", pattern);
        client().prepareIndex("test", "type1")
                .setSource("field1", value).get();
        refresh();

        logger.info("--> highlighting and searching on field1 with default phrase limit");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "t"))
                .highlighter(highlight().highlighterType("fvh").field("field1", 20, 1).order("score").preTags("<xxx>").postTags("</xxx>"));
        SearchResponse defaultPhraseLimit = client().search(searchRequest("test").source(source)).actionGet();
        assertHighlight(defaultPhraseLimit, 0, "field1", 0, 1, containsString("<xxx>t</xxx>"));

        logger.info("--> highlighting and searching on field1 with large phrase limit");
        source = searchSource()
                .query(termQuery("field1", "t"))
                .highlighter(highlight().highlighterType("fvh").field("field1", 20, 1).order("score").preTags("<xxx>").postTags("</xxx>").phraseLimit(30000));
        SearchResponse largePhraseLimit = client().search(searchRequest("test").source(source)).actionGet();
        assertHighlight(largePhraseLimit, 0, "field1", 0, 1, containsString("<xxx>t</xxx>"));

        /*
         * I hate comparing times because it can be inconsistent but default is
         * in the neighborhood of 300ms and the large phrase limit is in the
         * neighborhood of 8 seconds.
         */
        assertThat(defaultPhraseLimit.getTookInMillis(), lessThan(largePhraseLimit.getTookInMillis()));
    }


    public void testMatchedFieldsFvhRequireFieldMatch() throws Exception {
        checkMatchedFieldsCase(true);
    }

    public void testMatchedFieldsFvhNoRequireFieldMatch() throws Exception {
        checkMatchedFieldsCase(false);
    }

    private void checkMatchedFieldsCase(boolean requireFieldMatch) throws Exception {
        assertAcked(prepareCreate("test")
            .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties")
                    .startObject("foo")
                        .field("type", "text")
                        .field("term_vector", "with_positions_offsets")
                        .field("store", true)
                        .field("analyzer", "english")
                        .startObject("fields")
                            .startObject("plain")
                                .field("type", "text")
                                .field("term_vector", "with_positions_offsets")
                                .field("analyzer", "standard")
                            .endObject()
                        .endObject()
                    .endObject()
                    .startObject("bar")
                        .field("type", "text")
                        .field("term_vector", "with_positions_offsets")
                        .field("store", true)
                        .field("analyzer", "english")
                        .startObject("fields")
                            .startObject("plain")
                                .field("type", "text")
                                .field("term_vector", "with_positions_offsets")
                                .field("analyzer", "standard")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject().endObject().endObject()));
        ensureGreen();

        index("test", "type1", "1",
                "foo", "running with scissors");
        index("test", "type1", "2",
                "foo", "cat cat junk junk junk junk junk junk junk cats junk junk",
                "bar", "cat cat junk junk junk junk junk junk junk cats junk junk");
        index("test", "type1", "3",
                "foo", "weird",
                "bar", "result");
        refresh();

        Field fooField = new Field("foo").numOfFragments(1).order("score").fragmentSize(25)
                .highlighterType("fvh").requireFieldMatch(requireFieldMatch);
        SearchRequestBuilder req = client().prepareSearch("test").highlighter(new HighlightBuilder().field(fooField));

        // First check highlighting without any matched fields set
        SearchResponse resp = req.setQuery(queryStringQuery("running scissors").field("foo")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>running</em> with <em>scissors</em>"));

        // And that matching a subfield doesn't automatically highlight it
        resp = req.setQuery(queryStringQuery("foo.plain:running scissors").field("foo")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("running with <em>scissors</em>"));

        // Add the subfield to the list of matched fields but don't match it.  Everything should still work
        // like before we added it.
        fooField = new Field("foo").numOfFragments(1).order("score").fragmentSize(25).highlighterType("fvh")
                .requireFieldMatch(requireFieldMatch);
        fooField.matchedFields("foo", "foo.plain");
        req = client().prepareSearch("test").highlighter(new HighlightBuilder().field(fooField));
        resp = req.setQuery(queryStringQuery("running scissors").field("foo")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>running</em> with <em>scissors</em>"));


        // Now make half the matches come from the stored field and half from just a matched field.
        resp = req.setQuery(queryStringQuery("foo.plain:running scissors").field("foo")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>running</em> with <em>scissors</em>"));

        // Now remove the stored field from the matched field list.  That should work too.
        fooField = new Field("foo").numOfFragments(1).order("score").fragmentSize(25).highlighterType("fvh")
                .requireFieldMatch(requireFieldMatch);
        fooField.matchedFields("foo.plain");
        req = client().prepareSearch("test").highlighter(new HighlightBuilder().field(fooField));
        resp = req.setQuery(queryStringQuery("foo.plain:running scissors").field("foo")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>running</em> with scissors"));

        // Now make sure boosted fields don't blow up when matched fields is both the subfield and stored field.
        fooField = new Field("foo").numOfFragments(1).order("score").fragmentSize(25).highlighterType("fvh")
                .requireFieldMatch(requireFieldMatch);
        fooField.matchedFields("foo", "foo.plain");
        req = client().prepareSearch("test").highlighter(new HighlightBuilder().field(fooField));
        resp = req.setQuery(queryStringQuery("foo.plain:running^5 scissors").field("foo")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>running</em> with <em>scissors</em>"));

        // Now just all matches are against the matched field.  This still returns highlighting.
        resp = req.setQuery(queryStringQuery("foo.plain:running foo.plain:scissors").field("foo")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>running</em> with <em>scissors</em>"));

        // And all matched field via the queryString's field parameter, just in case
        resp = req.setQuery(queryStringQuery("running scissors").field("foo.plain")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>running</em> with <em>scissors</em>"));

        // Finding the same string two ways is ok too
        resp = req.setQuery(queryStringQuery("run foo.plain:running^5 scissors").field("foo")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>running</em> with <em>scissors</em>"));

        // But we use the best found score when sorting fragments
        resp = req.setQuery(queryStringQuery("cats foo.plain:cats^5").field("foo")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("junk junk <em>cats</em> junk junk"));

        // which can also be written by searching on the subfield
        resp = req.setQuery(queryStringQuery("cats").field("foo").field("foo.plain", 5)).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("junk junk <em>cats</em> junk junk"));

        // Speaking of two fields, you can have two fields, only one of which has matchedFields enabled
        QueryBuilder twoFieldsQuery = queryStringQuery("cats").field("foo").field("foo.plain", 5)
                .field("bar").field("bar.plain", 5);
        Field barField = new Field("bar").numOfFragments(1).order("score").fragmentSize(25).highlighterType("fvh")
                .requireFieldMatch(requireFieldMatch);
        resp = req.setQuery(twoFieldsQuery).highlighter(new HighlightBuilder().field(fooField).field(barField)).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("junk junk <em>cats</em> junk junk"));
        assertHighlight(resp, 0, "bar", 0, equalTo("<em>cat</em> <em>cat</em> junk junk junk junk"));
        // And you can enable matchedField highlighting on both
        barField.matchedFields("bar", "bar.plain");
        resp = req.setQuery(twoFieldsQuery).highlighter(new HighlightBuilder().field(fooField).field(barField)).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("junk junk <em>cats</em> junk junk"));
        assertHighlight(resp, 0, "bar", 0, equalTo("junk junk <em>cats</em> junk junk"));

        // Setting a matchedField that isn't searched/doesn't exist is simply ignored.
        barField.matchedFields("bar", "candy");
        resp = req.setQuery(twoFieldsQuery).highlighter(new HighlightBuilder().field(fooField).field(barField)).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("junk junk <em>cats</em> junk junk"));
        assertHighlight(resp, 0, "bar", 0, equalTo("<em>cat</em> <em>cat</em> junk junk junk junk"));

        // If the stored field doesn't have a value it doesn't matter what you match, you get nothing.
        barField.matchedFields("bar", "foo.plain");
        resp = req.setQuery(queryStringQuery("running scissors").field("foo.plain").field("bar"))
                .highlighter(new HighlightBuilder().field(fooField).field(barField)).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>running</em> with <em>scissors</em>"));
        assertThat(resp.getHits().getAt(0).getHighlightFields(), not(hasKey("bar")));

        // If the stored field is found but the matched field isn't then you don't get a result either.
        fooField.matchedFields("bar.plain");
        resp = req.setQuery(queryStringQuery("running scissors").field("foo").field("foo.plain").field("bar").field("bar.plain"))
                .highlighter(new HighlightBuilder().field(fooField).field(barField)).get();
        assertThat(resp.getHits().getAt(0).getHighlightFields(), not(hasKey("foo")));

        // But if you add the stored field to the list of matched fields then you'll get a result again
        fooField.matchedFields("foo", "bar.plain");
        resp = req.setQuery(queryStringQuery("running scissors").field("foo").field("foo.plain").field("bar").field("bar.plain"))
                .highlighter(new HighlightBuilder().field(fooField).field(barField)).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>running</em> with <em>scissors</em>"));
        assertThat(resp.getHits().getAt(0).getHighlightFields(), not(hasKey("bar")));

        // You _can_ highlight fields that aren't subfields of one another.
        resp = req.setQuery(queryStringQuery("weird").field("foo").field("foo.plain").field("bar").field("bar.plain"))
                .highlighter(new HighlightBuilder().field(fooField).field(barField)).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>weird</em>"));
        assertHighlight(resp, 0, "bar", 0, equalTo("<em>resul</em>t"));

        assertFailures(req.setQuery(queryStringQuery("result").field("foo").field("foo.plain").field("bar").field("bar.plain")),
                 RestStatus.INTERNAL_SERVER_ERROR, containsString("IndexOutOfBoundsException"));
    }

    public void testFastVectorHighlighterManyDocs() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", type1TermVectorMapping()));
        ensureGreen();

        int COUNT = between(20, 100);
        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[COUNT];
        for (int i = 0; i < COUNT; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field1", "test " + i);
        }
        logger.info("--> indexing docs");
        indexRandom(true, indexRequestBuilders);

        logger.info("--> searching explicitly on field1 and highlighting on it");
        SearchResponse searchResponse = client().prepareSearch()
                .setSize(COUNT)
                .setQuery(termQuery("field1", "test"))
                .highlighter(new HighlightBuilder().field("field1", 100, 0))
                .get();
        for (int i = 0; i < COUNT; i++) {
            SearchHit hit = searchResponse.getHits().getHits()[i];
            // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
            assertHighlight(searchResponse, i, "field1", 0, 1, equalTo("<em>test</em> " + hit.id()));
        }

        logger.info("--> searching explicitly _all and highlighting on _all");
        searchResponse = client().prepareSearch()
                .setSize(COUNT)
                .setQuery(termQuery("_all", "test"))
                .highlighter(new HighlightBuilder().field("_all", 100, 0))
                .get();
        for (int i = 0; i < COUNT; i++) {
            SearchHit hit = searchResponse.getHits().getHits()[i];
            assertHighlight(searchResponse, i, "_all", 0, 1, equalTo("<em>test</em> " + hit.id()));
        }
    }

    public XContentBuilder type1TermVectorMapping() throws IOException {
        return XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("_all").field("store", true).field("term_vector", "with_positions_offsets").endObject()
                .startObject("properties")
                .startObject("field1").field("type", "text").field("term_vector", "with_positions_offsets").endObject()
                .startObject("field2").field("type", "text").field("term_vector", "with_positions_offsets").endObject()
                .endObject()
                .endObject().endObject();
    }

    public void testSameContent() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "title", "type=text,store=true,term_vector=with_positions_offsets"));
        ensureYellow();

        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[5];
        for (int i = 0; i < 5; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a test on the highlighting bug present in elasticsearch");
        }
        indexRandom(true, indexRequestBuilders);

        SearchResponse search = client().prepareSearch()
                .setQuery(matchQuery("title", "bug"))
                .highlighter(new HighlightBuilder().field("title", -1, 0))
                .get();

        for (int i = 0; i < 5; i++) {
            assertHighlight(search, i, "title", 0, 1, equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
        }
    }

    public void testFastVectorHighlighterOffsetParameter() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "title", "type=text,store=true,term_vector=with_positions_offsets").get());
        ensureYellow();

        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[5];
        for (int i = 0; i < 5; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a test on the highlighting bug present in elasticsearch");
        }
        indexRandom(true, indexRequestBuilders);

        SearchResponse search = client().prepareSearch()
                .setQuery(matchQuery("title", "bug"))
                .highlighter(new HighlightBuilder().field("title", 30, 1, 10))
                .get();

        for (int i = 0; i < 5; i++) {
            // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
            assertHighlight(search, i, "title", 0, 1, equalTo("highlighting <em>bug</em> present in elasticsearch"));
        }
    }

    public void testEscapeHtml() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "title", "type=text,store=true"));
        ensureYellow();

        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[5];
        for (int i = 0; i < indexRequestBuilders.length; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a html escaping highlighting test for *&? elasticsearch");
        }
        indexRandom(true, indexRequestBuilders);

        SearchResponse search = client().prepareSearch()
                .setQuery(matchQuery("title", "test"))
                .highlighter(new HighlightBuilder().encoder("html").field("title", 50, 1, 10))
                .get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            assertHighlight(search, i, "title", 0, 1, equalTo("This is a html escaping highlighting <em>test</em> for *&amp;? elasticsearch"));
        }
    }

    public void testEscapeHtmlVector() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "title", "type=text,store=true,term_vector=with_positions_offsets"));
        ensureYellow();

        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[5];
        for (int i = 0; i < 5; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a html escaping highlighting test for *&? elasticsearch");
        }
        indexRandom(true, indexRequestBuilders);

        SearchResponse search = client().prepareSearch()
                .setQuery(matchQuery("title", "test"))
                .highlighter(new HighlightBuilder().encoder("html").field("title", 30, 1, 10))
                .get();

        for (int i = 0; i < 5; i++) {
            assertHighlight(search, i, "title", 0, 1, equalTo("highlighting <em>test</em> for *&amp;? elasticsearch"));
        }
    }

    public void testMultiMapperVectorWithStore() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "text").field("store", true).field("term_vector", "with_positions_offsets").field("analyzer", "classic")
                        .startObject("fields")
                        .startObject("key").field("type", "text").field("store", true).field("term_vector", "with_positions_offsets").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject()));
        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource("title", "this is a test").get();
        refresh();
        // simple search on body with standard analyzer with a simple field query
        SearchResponse search = client().prepareSearch()
                .setQuery(matchQuery("title", "this is a test"))
                .highlighter(new HighlightBuilder().encoder("html").field("title", 50, 1))
                .get();

        assertHighlight(search, 0, "title", 0, 1, equalTo("this is a <em>test</em>"));

        // search on title.key and highlight on title
        search = client().prepareSearch()
                .setQuery(matchQuery("title.key", "this is a test"))
                .highlighter(new HighlightBuilder().encoder("html").field("title.key", 50, 1))
                .get();

        assertHighlight(search, 0, "title.key", 0, 1, equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em>"));
    }

    public void testMultiMapperVectorFromSource() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "text").field("store", false).field("term_vector", "with_positions_offsets").field("analyzer", "classic")
                        .startObject("fields")
                        .startObject("key").field("type", "text").field("store", false).field("term_vector", "with_positions_offsets").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject()));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("title", "this is a test").get();
        refresh();

        // simple search on body with standard analyzer with a simple field query
        SearchResponse search = client().prepareSearch()
                .setQuery(matchQuery("title", "this is a test"))
                .highlighter(new HighlightBuilder().encoder("html").field("title", 50, 1))
                .get();

        assertHighlight(search, 0, "title", 0, 1, equalTo("this is a <em>test</em>"));

        // search on title.key and highlight on title.key
        search = client().prepareSearch()
                .setQuery(matchQuery("title.key", "this is a test"))
                .highlighter(new HighlightBuilder().encoder("html").field("title.key", 50, 1))
                .get();

        assertHighlight(search, 0, "title.key", 0, 1, equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em>"));
    }

    public void testMultiMapperNoVectorWithStore() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "text").field("store", true).field("term_vector", "no").field("analyzer", "classic")
                        .startObject("fields")
                        .startObject("key").field("type", "text").field("store", true).field("term_vector", "no").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject()));

        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource("title", "this is a test").get();
        refresh();

        // simple search on body with standard analyzer with a simple field query
        SearchResponse search = client().prepareSearch()
                .setQuery(matchQuery("title", "this is a test"))
                .highlighter(new HighlightBuilder().encoder("html").field("title", 50, 1))
                .get();

        assertHighlight(search, 0, "title", 0, 1, equalTo("this is a <em>test</em>"));

        // search on title.key and highlight on title
        search = client().prepareSearch()
                .setQuery(matchQuery("title.key", "this is a test"))
                .highlighter(new HighlightBuilder().encoder("html").field("title.key", 50, 1))
                .get();

        assertHighlight(search, 0, "title.key", 0, 1, equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em>"));
    }

    public void testMultiMapperNoVectorFromSource() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "text").field("store", false).field("term_vector", "no").field("analyzer", "classic")
                        .startObject("fields")
                        .startObject("key").field("type", "text").field("store", false).field("term_vector", "no").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject()));
        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource("title", "this is a test").get();
        refresh();

        // simple search on body with standard analyzer with a simple field query
        SearchResponse search = client().prepareSearch()
                .setQuery(matchQuery("title", "this is a test"))
                .highlighter(new HighlightBuilder().encoder("html").field("title", 50, 1))
                .get();

        assertHighlight(search, 0, "title", 0, 1, equalTo("this is a <em>test</em>"));

        // search on title.key and highlight on title.key
        search = client().prepareSearch()
                .setQuery(matchQuery("title.key", "this is a test"))
                .highlighter(new HighlightBuilder().encoder("html").field("title.key", 50, 1))
                .get();

        assertHighlight(search, 0, "title.key", 0, 1, equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em>"));
    }

    public void testFastVectorHighlighterShouldFailIfNoTermVectors() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "title", "type=text,store=true,term_vector=no"));
        ensureGreen();

        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[5];
        for (int i = 0; i < 5; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a test for the enabling fast vector highlighter");
        }
        indexRandom(true, indexRequestBuilders);

        SearchResponse search = client().prepareSearch()
                .setQuery(matchPhraseQuery("title", "this is a test"))
                .highlighter(new HighlightBuilder().field("title", 50, 1, 10))
                .get();
        assertNoFailures(search);

        assertFailures(client().prepareSearch()
                .setQuery(matchPhraseQuery("title", "this is a test"))
                        .highlighter(new HighlightBuilder().field("title", 50, 1, 10).highlighterType("fvh")),
                RestStatus.BAD_REQUEST,
                containsString("the field [title] should be indexed with term vector with position offsets to be used with fast vector highlighter"));

        //should not fail if there is a wildcard
        assertNoFailures(client().prepareSearch()
                .setQuery(matchPhraseQuery("title", "this is a test"))
                .highlighter(new HighlightBuilder().field("tit*", 50, 1, 10).highlighterType("fvh")).get());
    }

    public void testDisableFastVectorHighlighter() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "title", "type=text,store=true,term_vector=with_positions_offsets,analyzer=classic"));
        ensureGreen();

        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[5];
        for (int i = 0; i < indexRequestBuilders.length; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a test for the workaround for the fast vector highlighting SOLR-3724");
        }
        indexRandom(true, indexRequestBuilders);

        SearchResponse search = client().prepareSearch()
                .setQuery(matchPhraseQuery("title", "test for the workaround"))
                .highlighter(new HighlightBuilder().field("title", 50, 1, 10))
                .get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            // Because of SOLR-3724 nothing is highlighted when FVH is used
            assertNotHighlighted(search, i, "title");
        }

        // Using plain highlighter instead of FVH
        search = client().prepareSearch()
                .setQuery(matchPhraseQuery("title", "test for the workaround"))
                .highlighter(new HighlightBuilder().field("title", 50, 1, 10).highlighterType("plain"))
                .get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            assertHighlight(search, i, "title", 0, 1, equalTo("This is a <em>test</em> for the <em>workaround</em> for the fast vector highlighting SOLR-3724"));
        }

        // Using plain highlighter instead of FVH on the field level
        search = client().prepareSearch()
                .setQuery(matchPhraseQuery("title", "test for the workaround"))
                .highlighter(
                        new HighlightBuilder().field(new HighlightBuilder.Field("title").highlighterType("plain")).highlighterType(
                                "plain"))
                .get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            assertHighlight(search, i, "title", 0, 1, equalTo("This is a <em>test</em> for the <em>workaround</em> for the fast vector highlighting SOLR-3724"));
        }
    }

    public void testFSHHighlightAllMvFragments() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "tags", "type=text,term_vector=with_positions_offsets"));
        ensureGreen();
        client().prepareIndex("test", "type1", "1")
                .setSource("tags", new String[]{
                        "this is a really long tag i would like to highlight",
                        "here is another one that is very long and has the tag token near the end"}).get();
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("tags", "tag"))
                .highlighter(new HighlightBuilder().field("tags", -1, 0)).get();

        assertHighlight(response, 0, "tags", 0, equalTo("this is a really long <em>tag</em> i would like to highlight"));
        assertHighlight(response, 0, "tags", 1, 2, equalTo("here is another one that is very long and has the <em>tag</em> token near the end"));
    }

    public void testBoostingQuery() {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog").get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(boostingQuery(termQuery("field2", "brown"), termQuery("field2", "foobar")).negativeBoost(0.5f))
                .highlighter(highlight().field("field2").order("score").preTags("<x>").postTags("</x>"));

        SearchResponse searchResponse = client().prepareSearch("test").setSource(source).get();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The quick <x>brown</x> fox jumps over the lazy dog"));
    }

    @AwaitsFix(bugUrl="Broken now that BoostingQuery does not extend BooleanQuery anymore")
    public void testBoostingQueryTermVector() throws IOException {
        assertAcked(prepareCreate("test").addMapping("type1", type1TermVectorMapping()));
        ensureGreen();
        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog")
                .get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(boostingQuery(termQuery("field2", "brown"), termQuery("field2", "foobar")).negativeBoost(0.5f))
                .highlighter(highlight().field("field2").order("score").preTags("<x>").postTags("</x>"));

        SearchResponse searchResponse = client().prepareSearch("test").setSource(source).get();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The quick <x>brown</x> fox jumps over the lazy dog"));
    }

    public void testCommonTermsQuery() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog")
                .get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(commonTermsQuery("field2", "quick brown").cutoffFrequency(100))
                .highlighter(highlight().field("field2").order("score").preTags("<x>").postTags("</x>"));

        SearchResponse searchResponse = client().prepareSearch("test").setSource(source).get();
        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <x>quick</x> <x>brown</x> fox jumps over the lazy dog"));
    }

    public void testCommonTermsTermVector() throws IOException {
        assertAcked(prepareCreate("test").addMapping("type1", type1TermVectorMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog").get();
        refresh();
        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource().query(commonTermsQuery("field2", "quick brown").cutoffFrequency(100))
                .highlighter(highlight().field("field2").order("score").preTags("<x>").postTags("</x>"));

        SearchResponse searchResponse = client().prepareSearch("test").setSource(source).get();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <x>quick</x> <x>brown</x> fox jumps over the lazy dog"));
    }

    public void testPhrasePrefix() throws IOException {
        Builder builder = Settings.builder()
                .put(indexSettings())
                .put("index.analysis.analyzer.synonym.tokenizer", "whitespace")
                .putArray("index.analysis.analyzer.synonym.filter", "synonym", "lowercase")
                .put("index.analysis.filter.synonym.type", "synonym")
                .putArray("index.analysis.filter.synonym.synonyms", "quick => fast");

        assertAcked(prepareCreate("test").setSettings(builder.build()).addMapping("type1", type1TermVectorMapping())
                .addMapping("type2", "_all", "store=true,term_vector=with_positions_offsets",
                        "field4", "type=text,term_vector=with_positions_offsets,analyzer=synonym",
                        "field3", "type=text,analyzer=synonym"));
        ensureGreen();

        client().prepareIndex("test", "type1", "0")
                .setSource("field0", "The quick brown fox jumps over the lazy dog", "field1", "The quick brown fox jumps over the lazy dog").get();
        client().prepareIndex("test", "type1", "1")
                .setSource("field1", "The quick browse button is a fancy thing, right bro?").get();
        refresh();
        logger.info("--> highlighting and searching on field0");
        SearchSourceBuilder source = searchSource()
                .query(matchPhrasePrefixQuery("field0", "quick bro"))
                .highlighter(highlight().field("field0").order("score").preTags("<x>").postTags("</x>"));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field0", 0, 1, equalTo("The <x>quick</x> <x>brown</x> fox jumps over the lazy dog"));

        logger.info("--> highlighting and searching on field1");
        source = searchSource()
                .query(matchPhrasePrefixQuery("field1", "quick bro"))
                .highlighter(highlight().field("field1").order("score").preTags("<x>").postTags("</x>"));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field1", 0, 1, anyOf(equalTo("The <x>quick browse</x> button is a fancy thing, right bro?"), equalTo("The <x>quick brown</x> fox jumps over the lazy dog")));
        assertHighlight(searchResponse, 1, "field1", 0, 1, anyOf(equalTo("The <x>quick browse</x> button is a fancy thing, right bro?"), equalTo("The <x>quick brown</x> fox jumps over the lazy dog")));

        // with synonyms
        client().prepareIndex("test", "type2", "0")
                .setSource("field4", "The quick brown fox jumps over the lazy dog", "field3", "The quick brown fox jumps over the lazy dog").get();
        client().prepareIndex("test", "type2", "1")
                .setSource("field4", "The quick browse button is a fancy thing, right bro?").get();
        client().prepareIndex("test", "type2", "2")
                .setSource("field4", "a quick fast blue car").get();
        refresh();

        source = searchSource().postFilter(typeQuery("type2")).query(matchPhrasePrefixQuery("field3", "fast bro"))
                .highlighter(highlight().field("field3").order("score").preTags("<x>").postTags("</x>"));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field3", 0, 1, equalTo("The <x>quick</x> <x>brown</x> fox jumps over the lazy dog"));

        logger.info("--> highlighting and searching on field4");
        source = searchSource().postFilter(typeQuery("type2")).query(matchPhrasePrefixQuery("field4", "the fast bro"))
                .highlighter(highlight().field("field4").order("score").preTags("<x>").postTags("</x>"));
        searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field4", 0, 1, anyOf(equalTo("<x>The quick browse</x> button is a fancy thing, right bro?"), equalTo("<x>The quick brown</x> fox jumps over the lazy dog")));
        assertHighlight(searchResponse, 1, "field4", 0, 1, anyOf(equalTo("<x>The quick browse</x> button is a fancy thing, right bro?"), equalTo("<x>The quick brown</x> fox jumps over the lazy dog")));

        logger.info("--> highlighting and searching on field4");
        source = searchSource().postFilter(typeQuery("type2")).query(matchPhrasePrefixQuery("field4", "a fast quick blue ca"))
                .highlighter(highlight().field("field4").order("score").preTags("<x>").postTags("</x>"));
        searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field4", 0, 1, equalTo("<x>a quick fast blue car</x>"));
    }

    public void testPlainHighlightDifferentFragmenter() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "tags", "type=text"));
        ensureGreen();
        client().prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject().field("tags",
                        "this is a really long tag i would like to highlight",
                        "here is another one that is very long tag and has the tag token near the end").endObject()).get();
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("tags", "long tag").type(MatchQuery.Type.PHRASE))
                .highlighter(
                        new HighlightBuilder().field(new HighlightBuilder.Field("tags").fragmentSize(-1).numOfFragments(2)
                                .fragmenter("simple"))).get();

        assertHighlight(response, 0, "tags", 0, equalTo("this is a really <em>long</em> <em>tag</em> i would like to highlight"));
        assertHighlight(response, 0, "tags", 1, 2, equalTo("here is another one that is very <em>long</em> <em>tag</em> and has the tag token near the end"));

        response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("tags", "long tag").type(MatchQuery.Type.PHRASE))
                .highlighter(
                        new HighlightBuilder().field(new HighlightBuilder.Field("tags").fragmentSize(-1).numOfFragments(2)
                                .fragmenter("span"))).get();

        assertHighlight(response, 0, "tags", 0, equalTo("this is a really <em>long</em> <em>tag</em> i would like to highlight"));
        assertHighlight(response, 0, "tags", 1, 2, equalTo("here is another one that is very <em>long</em> <em>tag</em> and has the tag token near the end"));

        assertFailures(client().prepareSearch("test")
                        .setQuery(QueryBuilders.matchQuery("tags", "long tag").type(MatchQuery.Type.PHRASE))
                        .highlighter(
                                new HighlightBuilder().field(new HighlightBuilder.Field("tags").fragmentSize(-1).numOfFragments(2)
                                        .fragmenter("invalid"))),
                RestStatus.BAD_REQUEST,
                containsString("unknown fragmenter option [invalid] for the field [tags]"));
    }

    public void testPlainHighlighterMultipleFields() {
        createIndex("test");
        ensureGreen();

        index("test", "type1", "1", "field1", "The <b>quick<b> brown fox", "field2", "The <b>slow<b> brown fox");
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("field1", "fox"))
                .highlighter(
                        new HighlightBuilder().field(
                                new HighlightBuilder.Field("field1").preTags("<1>").postTags("</1>").requireFieldMatch(true)).field(
                                new HighlightBuilder.Field("field2").preTags("<2>").postTags("</2>").requireFieldMatch(false)))
                .get();
        assertHighlight(response, 0, "field1", 0, 1, equalTo("The <b>quick<b> brown <1>fox</1>"));
        assertHighlight(response, 0, "field2", 0, 1, equalTo("The <b>slow<b> brown <2>fox</2>"));
    }

    public void testFastVectorHighlighterMultipleFields() {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "field1", "type=text,term_vector=with_positions_offsets", "field2", "type=text,term_vector=with_positions_offsets"));
        ensureGreen();

        index("test", "type1", "1", "field1", "The <b>quick<b> brown fox", "field2", "The <b>slow<b> brown fox");
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("field1", "fox"))
                .highlighter(
                        new HighlightBuilder().field(
                                new HighlightBuilder.Field("field1").preTags("<1>").postTags("</1>").requireFieldMatch(true)).field(
                                new HighlightBuilder.Field("field2").preTags("<2>").postTags("</2>").requireFieldMatch(false)))
                .get();
        assertHighlight(response, 0, "field1", 0, 1, equalTo("The <b>quick<b> brown <1>fox</1>"));
        assertHighlight(response, 0, "field2", 0, 1, equalTo("The <b>slow<b> brown <2>fox</2>"));
    }

    public void testMissingStoredField() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "highlight_field", "type=text,store=true"));
        ensureGreen();
        client().prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject()
                        .field("field", "highlight")
                        .endObject()).get();
        refresh();

        // This query used to fail when the field to highlight was absent
        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("field", "highlight").type(MatchQuery.Type.BOOLEAN))
                .highlighter(
                        new HighlightBuilder().field(new HighlightBuilder.Field("highlight_field").fragmentSize(-1).numOfFragments(1)
                                .fragmenter("simple"))).get();
        assertThat(response.getHits().hits()[0].highlightFields().isEmpty(), equalTo(true));
    }

    // Issue #3211
    public void testNumericHighlighting() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("test", "text", "type=text",
                        "byte", "type=byte", "short", "type=short", "int", "type=integer", "long", "type=long",
                        "float", "type=float", "double", "type=double"));
        ensureGreen();

        client().prepareIndex("test", "test", "1").setSource("text", "elasticsearch test",
                "byte", 25, "short", 42, "int", 100, "long", -1, "float", 3.2f, "double", 42.42).get();
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("text", "test").type(MatchQuery.Type.BOOLEAN))
                .highlighter(
                        new HighlightBuilder().field("text").field("byte").field("short").field("int").field("long").field("float")
                                .field("double"))
                .get();
        // Highlighting of numeric fields is not supported, but it should not raise errors
        // (this behavior is consistent with version 0.20)
        assertHitCount(response, 1L);
    }

    // Issue #3200
    public void testResetTwice() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(Settings.builder()
                        .put(indexSettings())
                        .put("analysis.analyzer.my_analyzer.type", "pattern")
                        .put("analysis.analyzer.my_analyzer.pattern", "\\s+")
                        .build())
                .addMapping("type", "text", "type=text,analyzer=my_analyzer"));
        ensureGreen();
        client().prepareIndex("test", "type", "1")
                .setSource("text", "elasticsearch test").get();
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("text", "test").type(MatchQuery.Type.BOOLEAN))
                .highlighter(new HighlightBuilder().field("text")).execute().actionGet();
        // PatternAnalyzer will throw an exception if it is resetted twice
        assertHitCount(response, 1L);
    }

    public void testHighlightUsesHighlightQuery() throws IOException {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "text", "type=text," + randomStoreField() + "term_vector=with_positions_offsets,index_options=offsets"));
        ensureGreen();

        index("test", "type1", "1", "text", "Testing the highlight query feature");
        refresh();

        HighlightBuilder.Field field = new HighlightBuilder.Field("text");

        HighlightBuilder highlightBuilder = new HighlightBuilder().field(field);
        SearchRequestBuilder search = client().prepareSearch("test").setQuery(QueryBuilders.matchQuery("text", "testing"))
                .highlighter(highlightBuilder);
        Matcher<String> searchQueryMatcher = equalTo("<em>Testing</em> the highlight query feature");

        field.highlighterType("plain");
        SearchResponse response = search.get();
        assertHighlight(response, 0, "text", 0, searchQueryMatcher);
        field.highlighterType("fvh");
        response = search.get();
        assertHighlight(response, 0, "text", 0, searchQueryMatcher);
        field.highlighterType("postings");
        response = search.get();
        assertHighlight(response, 0, "text", 0, searchQueryMatcher);

        field = new HighlightBuilder.Field("text");

        Matcher<String> hlQueryMatcher = equalTo("Testing the highlight <em>query</em> feature");
        field.highlightQuery(matchQuery("text", "query"));
        highlightBuilder = new HighlightBuilder().field(field);
        search = client().prepareSearch("test").setQuery(QueryBuilders.matchQuery("text", "testing")).highlighter(highlightBuilder);

        field.highlighterType("fvh");
        response = search.get();
        assertHighlight(response, 0, "text", 0, hlQueryMatcher);

        field.highlighterType("plain");
        response = search.get();
        assertHighlight(response, 0, "text", 0, hlQueryMatcher);

        field.highlighterType("postings");
        response = search.get();
        assertHighlight(response, 0, "text", 0, hlQueryMatcher);

        // Make sure the highlightQuery is taken into account when it is set on the highlight context instead of the field
        highlightBuilder.highlightQuery(matchQuery("text", "query"));
        field.highlighterType("fvh").highlightQuery(null);
        response = search.get();
        assertHighlight(response, 0, "text", 0, hlQueryMatcher);

        field.highlighterType("plain");
        response = search.get();
        assertHighlight(response, 0, "text", 0, hlQueryMatcher);

        field.highlighterType("postings");
        response = search.get();
        assertHighlight(response, 0, "text", 0, hlQueryMatcher);
    }

    private static String randomStoreField() {
        if (randomBoolean()) {
            return "store=true,";
        }
        return "";
    }

    public void testHighlightNoMatchSize() throws IOException {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "text", "type=text," + randomStoreField() + "term_vector=with_positions_offsets,index_options=offsets"));
        ensureGreen();

        String text = "I am pretty long so some of me should get cut off. Second sentence";
        index("test", "type1", "1", "text", text);
        refresh();

        // When you don't set noMatchSize you don't get any results if there isn't anything to highlight.
        HighlightBuilder.Field field = new HighlightBuilder.Field("text")
                .fragmentSize(21)
                .numOfFragments(1)
                .highlighterType("plain");
        SearchResponse response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("fvh");
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("postings");
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "text");

        // When noMatchSize is set to 0 you also shouldn't get any
        field.highlighterType("plain").noMatchSize(0);
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("fvh");
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("postings");
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "text");

        // When noMatchSize is between 0 and the size of the string
        field.highlighterType("plain").noMatchSize(21);
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so"));

        // The FVH also works but the fragment is longer than the plain highlighter because of boundary_max_scan
        field.highlighterType("fvh");
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so some"));

        // Postings hl also works but the fragment is the whole first sentence (size ignored)
        field.highlighterType("postings");
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so some of me should get cut off."));

        // We can also ask for a fragment longer than the input string and get the whole string
        field.highlighterType("plain").noMatchSize(text.length() * 2);
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo(text));

        field.highlighterType("fvh");
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo(text));

        //no difference using postings hl as the noMatchSize is ignored (just needs to be greater than 0)
        field.highlighterType("postings");
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so some of me should get cut off."));

        // We can also ask for a fragment exactly the size of the input field and get the whole field
        field.highlighterType("plain").noMatchSize(text.length());
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo(text));

        field.highlighterType("fvh");
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo(text));

        //no difference using postings hl as the noMatchSize is ignored (just needs to be greater than 0)
        field.highlighterType("postings");
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so some of me should get cut off."));

        // You can set noMatchSize globally in the highlighter as well
        field.highlighterType("plain").noMatchSize(null);
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field).noMatchSize(21)).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so"));

        field.highlighterType("fvh");
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field).noMatchSize(21)).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so some"));

        field.highlighterType("postings");
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field).noMatchSize(21)).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so some of me should get cut off."));

        // We don't break if noMatchSize is less than zero though
        field.highlighterType("plain").noMatchSize(randomIntBetween(Integer.MIN_VALUE, -1));
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("fvh");
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("postings");
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "text");
    }

    public void testHighlightNoMatchSizeWithMultivaluedFields() throws IOException {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "text", "type=text," + randomStoreField() + "term_vector=with_positions_offsets,index_options=offsets"));
        ensureGreen();

        String text1 = "I am pretty long so some of me should get cut off. We'll see how that goes.";
        String text2 = "I am short";
        index("test", "type1", "1", "text", new String[] {text1, text2});
        refresh();

        // The no match fragment should come from the first value of a multi-valued field
        HighlightBuilder.Field field = new HighlightBuilder.Field("text")
                .fragmentSize(21)
                .numOfFragments(1)
                .highlighterType("plain")
                .noMatchSize(21);
        SearchResponse response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so"));

        field.highlighterType("fvh");
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so some"));

        // Postings hl also works but the fragment is the whole first sentence (size ignored)
        field.highlighterType("postings");
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so some of me should get cut off."));

        // And noMatchSize returns nothing when the first entry is empty string!
        index("test", "type1", "2", "text", new String[] {"", text2});
        refresh();

        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery("type1").addIds("2");
        field.highlighterType("plain");
        response = client().prepareSearch("test")
                .setQuery(idsQueryBuilder)
.highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("fvh");
        response = client().prepareSearch("test")
                .setQuery(idsQueryBuilder)
.highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("postings");
        response = client().prepareSearch("test")
                .setQuery(idsQueryBuilder)
.highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "text");

        // But if the field was actually empty then you should get no highlighting field
        index("test", "type1", "3", "text", new String[] {});
        refresh();
        idsQueryBuilder = QueryBuilders.idsQuery("type1").addIds("3");
        field.highlighterType("plain");
        response = client().prepareSearch("test")
                .setQuery(idsQueryBuilder)
.highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("fvh");
        response = client().prepareSearch("test")
                .setQuery(idsQueryBuilder)
.highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("postings");
        response = client().prepareSearch("test")
                .setQuery(idsQueryBuilder)
.highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "text");

        // Same for if the field doesn't even exist on the document
        index("test", "type1", "4");
        refresh();

        idsQueryBuilder = QueryBuilders.idsQuery("type1").addIds("4");
        field.highlighterType("plain");
        response = client().prepareSearch("test")
                .setQuery(idsQueryBuilder)
.highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("fvh");
        response = client().prepareSearch("test")
                .setQuery(idsQueryBuilder)
.highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("fvh");
        response = client().prepareSearch("test")
                .setQuery(idsQueryBuilder)
.highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "postings");

        // Again same if the field isn't mapped
        field = new HighlightBuilder.Field("unmapped")
                .highlighterType("plain")
                .noMatchSize(21);
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("fvh");
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("postings");
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertNotHighlighted(response, 0, "text");
    }

    public void testHighlightNoMatchSizeNumberOfFragments() throws IOException {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "text", "type=text," + randomStoreField() + "term_vector=with_positions_offsets,index_options=offsets"));
        ensureGreen();

        String text1 = "This is the first sentence. This is the second sentence." + HighlightUtils.PARAGRAPH_SEPARATOR;
        String text2 = "This is the third sentence. This is the fourth sentence.";
        String text3 = "This is the fifth sentence";
        index("test", "type1", "1", "text", new String[] {text1, text2, text3});
        refresh();

        // The no match fragment should come from the first value of a multi-valued field
        HighlightBuilder.Field field = new HighlightBuilder.Field("text")
                .fragmentSize(1)
                .numOfFragments(0)
                .highlighterType("plain")
                .noMatchSize(20);
        SearchResponse response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("This is the first"));

        field.highlighterType("fvh");
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("This is the first sentence"));

        // Postings hl also works but the fragment is the whole first sentence (size ignored)
        field.highlighterType("postings");
        response = client().prepareSearch("test").highlighter(new HighlightBuilder().field(field)).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("This is the first sentence."));

        //if there's a match we only return the values with matches (whole value as number_of_fragments == 0)
        MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("text", "third fifth");
        field.highlighterType("plain");
        response = client().prepareSearch("test").setQuery(queryBuilder).highlighter(new HighlightBuilder().field(field)).get();
        assertHighlight(response, 0, "text", 0, 2, equalTo("This is the <em>third</em> sentence. This is the fourth sentence."));
        assertHighlight(response, 0, "text", 1, 2, equalTo("This is the <em>fifth</em> sentence"));

        field.highlighterType("fvh");
        response = client().prepareSearch("test").setQuery(queryBuilder).highlighter(new HighlightBuilder().field(field)).get();
        assertHighlight(response, 0, "text", 0, 2, equalTo("This is the <em>third</em> sentence. This is the fourth sentence."));
        assertHighlight(response, 0, "text", 1, 2, equalTo("This is the <em>fifth</em> sentence"));

        field.highlighterType("postings");
        response = client().prepareSearch("test").setQuery(queryBuilder).highlighter(new HighlightBuilder().field(field)).get();
        assertHighlight(response, 0, "text", 0, 2, equalTo("This is the <em>third</em> sentence. This is the fourth sentence."));
        assertHighlight(response, 0, "text", 1, 2, equalTo("This is the <em>fifth</em> sentence"));
    }

    public void testPostingsHighlighter() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy quick dog").get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "test"))
                .highlighter(highlight().field("field1").preTags("<xxx>").postTags("</xxx>"));
        SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on field1, highlighting on field1");
        source = searchSource()
                .query(termQuery("field1", "test"))
                .highlighter(highlight().field("field1").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on field2, highlighting on field2");
        source = searchSource()
                .query(termQuery("field2", "quick"))
                .highlighter(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy <xxx>quick</xxx> dog"));

        logger.info("--> searching on field2, highlighting on field2");
        source = searchSource()
                .query(matchPhraseQuery("field2", "quick brown"))
                .highlighter(highlight().field("field2").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        //phrase query results in highlighting all different terms regardless of their positions
        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <xxx>quick</xxx> <xxx>brown</xxx> fox jumps over the lazy <xxx>quick</xxx> dog"));

        //lets fall back to the standard highlighter then, what people would do to highlight query matches
        logger.info("--> searching on field2, highlighting on field2, falling back to the plain highlighter");
        source = searchSource()
                .query(matchPhraseQuery("_all", "quick brown"))
                .highlighter(highlight().field("field2").preTags("<xxx>").postTags("</xxx>").highlighterType("plain").requireFieldMatch(false));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <xxx>quick</xxx> <xxx>brown</xxx> fox jumps over the lazy quick dog"));
    }

    public void testPostingsHighlighterMultipleFields() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()).get());
        ensureGreen();

        index("test", "type1", "1", "field1", "The <b>quick<b> brown fox. Second sentence.", "field2", "The <b>slow<b> brown fox. Second sentence.");
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("field1", "fox"))
                .highlighter(
                        new HighlightBuilder().field(new HighlightBuilder.Field("field1").preTags("<1>").postTags("</1>")
                                .requireFieldMatch(true)))
                .get();
        assertHighlight(response, 0, "field1", 0, 1, equalTo("The <b>quick<b> brown <1>fox</1>."));
    }

    public void testPostingsHighlighterNumberOfFragments() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1", "1")
                .setSource("field1", "The quick brown fox jumps over the lazy dog. The lazy red fox jumps over the quick dog. The quick brown dog jumps over the lazy fox.",
                        "field2", "The quick brown fox jumps over the lazy dog. The lazy red fox jumps over the quick dog. The quick brown dog jumps over the lazy fox.").get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "fox"))
                .highlighter(highlight()
                        .field(new HighlightBuilder.Field("field1").numOfFragments(5).preTags("<field1>").postTags("</field1>")));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field1", 0, equalTo("The quick brown <field1>fox</field1> jumps over the lazy dog."));
        assertHighlight(searchResponse, 0, "field1", 1, equalTo("The lazy red <field1>fox</field1> jumps over the quick dog."));
        assertHighlight(searchResponse, 0, "field1", 2, 3, equalTo("The quick brown dog jumps over the lazy <field1>fox</field1>."));

        client().prepareIndex("test", "type1", "2")
                .setSource("field1", new String[]{"The quick brown fox jumps over the lazy dog. Second sentence not finished", "The lazy red fox jumps over the quick dog.", "The quick brown dog jumps over the lazy fox."}).get();
        refresh();

        source = searchSource()
                .query(termQuery("field1", "fox"))
                .highlighter(highlight()
                        .field(new HighlightBuilder.Field("field1").numOfFragments(0).preTags("<field1>").postTags("</field1>")));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();
        assertHitCount(searchResponse, 2L);

        for (SearchHit searchHit : searchResponse.getHits()) {
            if ("1".equals(searchHit.id())) {
                assertHighlight(searchHit, "field1", 0, 1, equalTo("The quick brown <field1>fox</field1> jumps over the lazy dog. The lazy red <field1>fox</field1> jumps over the quick dog. The quick brown dog jumps over the lazy <field1>fox</field1>."));
            } else if ("2".equals(searchHit.id())) {
                assertHighlight(searchHit, "field1", 0, 3, equalTo("The quick brown <field1>fox</field1> jumps over the lazy dog. Second sentence not finished"));
                assertHighlight(searchHit, "field1", 1, 3, equalTo("The lazy red <field1>fox</field1> jumps over the quick dog."));
                assertHighlight(searchHit, "field1", 2, 3, equalTo("The quick brown dog jumps over the lazy <field1>fox</field1>."));
            } else {
                fail("Only hits with id 1 and 2 are returned");
            }
        }
    }

    public void testMultiMatchQueryHighlight() throws IOException {
        String[] highlighterTypes = new String[] {"fvh", "plain", "postings"};
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("_all").field("store", true).field("index_options", "offsets").endObject()
                .startObject("properties")
                .startObject("field1").field("type", "text").field("index_options", "offsets").field("term_vector", "with_positions_offsets").endObject()
                .startObject("field2").field("type", "text").field("index_options", "offsets").field("term_vector", "with_positions_offsets").endObject()
                .endObject()
                .endObject().endObject();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();
        client().prepareIndex("test", "type1")
                .setSource("field1", "The quick brown fox jumps over",
                        "field2", "The quick brown fox jumps over").get();
        refresh();
        final int iters = scaledRandomIntBetween(20, 30);
        for (int i = 0; i < iters; i++) {
            String highlighterType = rarely() ? null : RandomPicks.randomFrom(random(), highlighterTypes);
            MultiMatchQueryBuilder.Type[] supportedQueryTypes;
            if ("postings".equals(highlighterType)) {
                //phrase_prefix is not supported by postings highlighter, as it rewrites against an empty reader, the prefix will never match any term
                supportedQueryTypes = new MultiMatchQueryBuilder.Type[]{MultiMatchQueryBuilder.Type.BEST_FIELDS, MultiMatchQueryBuilder.Type.CROSS_FIELDS, MultiMatchQueryBuilder.Type.MOST_FIELDS, MultiMatchQueryBuilder.Type.PHRASE};
            } else {
                supportedQueryTypes = MultiMatchQueryBuilder.Type.values();
            }
            MultiMatchQueryBuilder.Type matchQueryType = RandomPicks.randomFrom(random(), supportedQueryTypes);
            final MultiMatchQueryBuilder multiMatchQueryBuilder = multiMatchQuery("the quick brown fox", "field1", "field2").type(matchQueryType);

            SearchSourceBuilder source = searchSource()
                    .query(multiMatchQueryBuilder)
                    .highlighter(highlight().highlightQuery(randomBoolean() ? multiMatchQueryBuilder : null).highlighterType(highlighterType)
                            .field(new Field("field1").requireFieldMatch(true).preTags("<field1>").postTags("</field1>")));
            logger.info("Running multi-match type: [{}] highlight with type: [{}]", matchQueryType, highlighterType);
            SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();
            assertHitCount(searchResponse, 1L);
            assertHighlight(searchResponse, 0, "field1", 0, anyOf(equalTo("<field1>The quick brown fox</field1> jumps over"),
                    equalTo("<field1>The</field1> <field1>quick</field1> <field1>brown</field1> <field1>fox</field1> jumps over")));
        }
    }

    public void testPostingsHighlighterOrderByScore() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field1", new String[]{"This sentence contains one match, not that short. This sentence contains two sentence matches. This one contains no matches.",
                        "This is the second value's first sentence. This one contains no matches. This sentence contains three sentence occurrences (sentence).",
                        "One sentence match here and scored lower since the text is quite long, not that appealing. This one contains no matches."}).get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "sentence"))
                .highlighter(highlight().field("field1").order("score"));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        Map<String,HighlightField> highlightFieldMap = searchResponse.getHits().getAt(0).highlightFields();
        assertThat(highlightFieldMap.size(), equalTo(1));
        HighlightField field1 = highlightFieldMap.get("field1");
        assertThat(field1.fragments().length, equalTo(5));
        assertThat(field1.fragments()[0].string(), equalTo("This <em>sentence</em> contains three <em>sentence</em> occurrences (<em>sentence</em>)."));
        assertThat(field1.fragments()[1].string(), equalTo("This <em>sentence</em> contains two <em>sentence</em> matches."));
        assertThat(field1.fragments()[2].string(), equalTo("This is the second value's first <em>sentence</em>."));
        assertThat(field1.fragments()[3].string(), equalTo("This <em>sentence</em> contains one match, not that short."));
        assertThat(field1.fragments()[4].string(), equalTo("One <em>sentence</em> match here and scored lower since the text is quite long, not that appealing."));
    }

    public void testPostingsHighlighterEscapeHtml() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "title", "type=text," + randomStoreField() + "index_options=offsets"));
        ensureYellow();

        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[5];
        for (int i = 0; i < 5; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a html escaping highlighting test for *&? elasticsearch");
        }
        indexRandom(true, indexRequestBuilders);

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchQuery("title", "test"))
                .highlighter(new HighlightBuilder().field("title").encoder("html")).get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            assertHighlight(searchResponse, i, "title", 0, 1, equalTo("This is a html escaping highlighting <em>test</em> for *&amp;?"));
        }
    }

    public void testPostingsHighlighterMultiMapperWithStore() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1")
                        .startObject("properties")
                        .startObject("title").field("type", "text").field("store", true).field("index_options", "offsets").field("analyzer", "classic")
                        .startObject("fields")
                        .startObject("key").field("type", "text").field("store", true).field("index_options", "offsets").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject()));
        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource("title", "this is a test . Second sentence.").get();
        refresh();
        // simple search on body with standard analyzer with a simple field query
        SearchResponse searchResponse = client().prepareSearch()
                //lets make sure we analyze the query and we highlight the resulting terms
                .setQuery(matchQuery("title", "This is a Test"))
.highlighter(new HighlightBuilder().field("title")).get();

        assertHitCount(searchResponse, 1L);
        SearchHit hit = searchResponse.getHits().getAt(0);
        //stopwords are not highlighted since not indexed
        assertHighlight(hit, "title", 0, 1, equalTo("this is a <em>test</em> ."));

        // search on title.key and highlight on title
        searchResponse = client().prepareSearch()
                .setQuery(matchQuery("title.key", "this is a test"))
                .highlighter(new HighlightBuilder().field("title.key")).get();
        assertHitCount(searchResponse, 1L);

        //stopwords are now highlighted since we used only whitespace analyzer here
        assertHighlight(searchResponse, 0, "title.key", 0, 1, equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em> ."));
    }

    public void testPostingsHighlighterMultiMapperFromSource() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "text").field("store", false).field("index_options", "offsets").field("analyzer", "classic")
                        .startObject("fields")
                        .startObject("key").field("type", "text").field("store", false).field("index_options", "offsets").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject()));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("title", "this is a test").get();
        refresh();

        // simple search on body with standard analyzer with a simple field query
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchQuery("title", "this is a test"))
                .highlighter(new HighlightBuilder().field("title"))
                .get();

        assertHighlight(searchResponse, 0, "title", 0, 1, equalTo("this is a <em>test</em>"));

        // search on title.key and highlight on title.key
        searchResponse = client().prepareSearch()
                .setQuery(matchQuery("title.key", "this is a test"))
                .highlighter(new HighlightBuilder().field("title.key")).get();

        assertHighlight(searchResponse, 0, "title.key", 0, 1, equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em>"));
    }

    public void testPostingsHighlighterShouldFailIfNoOffsets() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "text").field("store", true).field("index_options", "docs").endObject()
                        .endObject().endObject().endObject()));
        ensureGreen();

        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[5];
        for (int i = 0; i < indexRequestBuilders.length; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a test for the postings highlighter");
        }
        indexRandom(true, indexRequestBuilders);

        SearchResponse search = client().prepareSearch()
                .setQuery(matchQuery("title", "this is a test"))
                .highlighter(new HighlightBuilder().field("title"))
                .get();
        assertNoFailures(search);

        assertFailures(client().prepareSearch()
                        .setQuery(matchQuery("title", "this is a test"))
                        .highlighter(new HighlightBuilder().field("title").highlighterType("postings")),
                RestStatus.BAD_REQUEST,
                containsString("the field [title] should be indexed with positions and offsets in the postings list to be used with postings highlighter"));


        assertFailures(client().prepareSearch()
                        .setQuery(matchQuery("title", "this is a test"))
                        .highlighter(new HighlightBuilder().field("title").highlighterType("postings")),
                RestStatus.BAD_REQUEST,
                containsString("the field [title] should be indexed with positions and offsets in the postings list to be used with postings highlighter"));

        //should not fail if there is a wildcard
        assertNoFailures(client().prepareSearch()
                        .setQuery(matchQuery("title", "this is a test"))
                .highlighter(new HighlightBuilder().field("tit*").highlighterType("postings")).get());
    }

    public void testPostingsHighlighterBoostingQuery() throws IOException {
        assertAcked(prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();
        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog! Second sentence.")
                .get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(boostingQuery(termQuery("field2", "brown"), termQuery("field2", "foobar")).negativeBoost(0.5f))
                .highlighter(highlight().field("field2").preTags("<x>").postTags("</x>"));
        SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The quick <x>brown</x> fox jumps over the lazy dog!"));
    }

    public void testPostingsHighlighterCommonTermsQuery() throws IOException {
        assertAcked(prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog! Second sentence.").get();
        refresh();
        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource().query(commonTermsQuery("field2", "quick brown").cutoffFrequency(100))
                .highlighter(highlight().field("field2").preTags("<x>").postTags("</x>"));
        SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();
        assertHitCount(searchResponse, 1L);

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <x>quick</x> <x>brown</x> fox jumps over the lazy dog!"));
    }

    private static XContentBuilder type1PostingsffsetsMapping() throws IOException {
        return XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties")
                .startObject("field1").field("type", "text").field("index_options", "offsets").endObject()
                .startObject("field2").field("type", "text").field("index_options", "offsets").endObject()
                .endObject()
                .endObject().endObject();
    }

    public void testPostingsHighlighterPrefixQuery() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog! Second sentence.").get();
        refresh();
        logger.info("--> highlighting and searching on field2");

        SearchSourceBuilder source = searchSource().query(prefixQuery("field2", "qui"))
                .highlighter(highlight().field("field2"));
        SearchResponse searchResponse = client().prepareSearch("test").setSource(source).get();
        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <em>quick</em> brown fox jumps over the lazy dog!"));

    }

    public void testPostingsHighlighterFuzzyQuery() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog! Second sentence.").get();
        refresh();
        logger.info("--> highlighting and searching on field2");
        SearchSourceBuilder source = searchSource().query(fuzzyQuery("field2", "quck"))
                .highlighter(highlight().field("field2"));
        SearchResponse searchResponse = client().prepareSearch("test").setSource(source).get();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <em>quick</em> brown fox jumps over the lazy dog!"));
    }

    public void testPostingsHighlighterRegexpQuery() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog! Second sentence.").get();
        refresh();
        logger.info("--> highlighting and searching on field2");
        SearchSourceBuilder source = searchSource().query(regexpQuery("field2", "qu[a-l]+k"))
                .highlighter(highlight().field("field2"));
        SearchResponse searchResponse = client().prepareSearch("test").setSource(source).get();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <em>quick</em> brown fox jumps over the lazy dog!"));
    }

    public void testPostingsHighlighterWildcardQuery() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog! Second sentence.").get();
        refresh();
        logger.info("--> highlighting and searching on field2");
        SearchSourceBuilder source = searchSource().query(wildcardQuery("field2", "qui*"))
                .highlighter(highlight().field("field2"));
        SearchResponse searchResponse = client().prepareSearch("test").setSource(source).get();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <em>quick</em> brown fox jumps over the lazy dog!"));

        source = searchSource().query(wildcardQuery("field2", "qu*k"))
                .highlighter(highlight().field("field2"));
        searchResponse = client().prepareSearch("test").setSource(source).get();
        assertHitCount(searchResponse, 1L);

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <em>quick</em> brown fox jumps over the lazy dog!"));
    }

    public void testPostingsHighlighterTermRangeQuery() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "aaab").get();
        refresh();
        logger.info("--> highlighting and searching on field2");
        SearchSourceBuilder source = searchSource().query(rangeQuery("field2").gte("aaaa").lt("zzzz"))
                .highlighter(highlight().field("field2"));
        SearchResponse searchResponse = client().prepareSearch("test").setSource(source).get();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("<em>aaab</em>"));
    }

    public void testPostingsHighlighterQueryString() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog! Second sentence.").get();
        refresh();
        logger.info("--> highlighting and searching on field2");
        SearchSourceBuilder source = searchSource().query(queryStringQuery("qui*").defaultField("field2"))
                .highlighter(highlight().field("field2"));
        SearchResponse searchResponse = client().prepareSearch("test").setSource(source).get();
        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <em>quick</em> brown fox jumps over the lazy dog!"));
    }

    public void testPostingsHighlighterRegexpQueryWithinConstantScoreQuery() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "The photography word will get highlighted").get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource().query(constantScoreQuery(regexpQuery("field1", "pho[a-z]+")))
                .highlighter(highlight().field("field1"));
        SearchResponse searchResponse = client().prepareSearch("test").setSource(source).get();
        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("The <em>photography</em> word will get highlighted"));
    }

    public void testPostingsHighlighterMultiTermQueryMultipleLevels() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "The photography word will get highlighted").get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource().query(boolQuery()
                .should(boolQuery().mustNot(QueryBuilders.existsQuery("field1")))
                .should(matchQuery("field1", "test"))
                .should(constantScoreQuery(queryStringQuery("field1:photo*"))))
                .highlighter(highlight().field("field1"));
        SearchResponse searchResponse = client().prepareSearch("test").setSource(source).get();
        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("The <em>photography</em> word will get highlighted"));
    }

    public void testPostingsHighlighterPrefixQueryWithinBooleanQuery() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "The photography word will get highlighted").get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource().query(boolQuery().must(prefixQuery("field1", "photo")).should(matchQuery("field1", "test").minimumShouldMatch("0")))
                .highlighter(highlight().field("field1"));
        SearchResponse searchResponse = client().prepareSearch("test").setSource(source).get();
        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("The <em>photography</em> word will get highlighted"));
    }

    public void testPostingsHighlighterQueryStringWithinFilteredQuery() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "The photography word will get highlighted").get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource().query(boolQuery()
                .must(queryStringQuery("field1:photo*"))
                .mustNot(existsQuery("field_null")))
                .highlighter(highlight().field("field1"));
        SearchResponse searchResponse = client().prepareSearch("test").setSource(source).get();
        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("The <em>photography</em> word will get highlighted"));
    }

    public void testPostingsHighlighterManyDocs() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        int COUNT = between(20, 100);
        Map<String, String> prefixes = new HashMap<>(COUNT);

        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[COUNT];
        for (int i = 0; i < COUNT; i++) {
            //generating text with word to highlight in a different position
            //(https://github.com/elastic/elasticsearch/issues/4103)
            String prefix = randomAsciiOfLengthBetween(5, 30);
            prefixes.put(String.valueOf(i), prefix);
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field1", "Sentence " + prefix
                    + " test. Sentence two.");
        }
        logger.info("--> indexing docs");
        indexRandom(true, indexRequestBuilders);

        logger.info("--> searching explicitly on field1 and highlighting on it");
        SearchRequestBuilder searchRequestBuilder = client().prepareSearch()
                .setSize(COUNT)
                .setQuery(termQuery("field1", "test"))
                .highlighter(new HighlightBuilder().field("field1"));
        SearchResponse searchResponse =
                searchRequestBuilder.get();
        assertHitCount(searchResponse, COUNT);
        assertThat(searchResponse.getHits().hits().length, equalTo(COUNT));
        for (SearchHit hit : searchResponse.getHits()) {
            String prefix = prefixes.get(hit.id());
            assertHighlight(hit, "field1", 0, 1, equalTo("Sentence " + prefix + " <em>test</em>."));
        }
    }

    public void testDoesNotHighlightTypeName() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("typename").startObject("properties")
                .startObject("foo").field("type", "text")
                    .field("index_options", "offsets")
                    .field("term_vector", "with_positions_offsets")
                .endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").addMapping("typename", mapping));
        ensureGreen();

        indexRandom(true, client().prepareIndex("test", "typename").setSource("foo", "test typename"));

        for (String highlighter: new String[] {"plain", "fvh", "postings"}) {
            SearchResponse response = client().prepareSearch("test").setTypes("typename").setQuery(matchQuery("foo", "test"))
                    .highlighter(new HighlightBuilder().field("foo").highlighterType(highlighter).requireFieldMatch(false)).get();
            assertHighlight(response, 0, "foo", 0, 1, equalTo("<em>test</em> typename"));
        }
    }

    public void testDoesNotHighlightAliasFilters() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("typename").startObject("properties")
                .startObject("foo").field("type", "text")
                    .field("index_options", "offsets")
                    .field("term_vector", "with_positions_offsets")
                .endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").addMapping("typename", mapping));
        assertAcked(client().admin().indices().prepareAliases().addAlias("test", "filtered_alias", matchQuery("foo", "japanese")));
        ensureGreen();

        indexRandom(true, client().prepareIndex("test", "typename").setSource("foo", "test japanese"));

        for (String highlighter: new String[] {"plain", "fvh", "postings"}) {
            SearchResponse response = client().prepareSearch("filtered_alias").setTypes("typename").setQuery(matchQuery("foo", "test"))
                    .highlighter(new HighlightBuilder().field("foo").highlighterType(highlighter).requireFieldMatch(false)).get();
            assertHighlight(response, 0, "foo", 0, 1, equalTo("<em>test</em> japanese"));
        }
    }

    @AwaitsFix(bugUrl="Broken now that BoostingQuery does not extend BooleanQuery anymore")
    public void testFastVectorHighlighterPhraseBoost() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", type1TermVectorMapping()));
        phraseBoostTestCase("fvh");
    }

    public void testPostingsHighlighterPhraseBoost() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        phraseBoostTestCase("postings");
    }

    /**
     * Test phrase boosting over normal term matches.  Note that this will never pass with the plain highlighter
     * because it doesn't support the concept of terms having a different weight based on position.
     * @param highlighterType highlighter to test
     */
    private void phraseBoostTestCase(String highlighterType) {
        ensureGreen();
        StringBuilder text = new StringBuilder();
        text.append("words words junk junk junk junk junk junk junk junk highlight junk junk junk junk together junk\n");
        for (int i = 0; i<10; i++) {
            text.append("junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk\n");
        }
        text.append("highlight words together\n");
        for (int i = 0; i<10; i++) {
            text.append("junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk junk\n");
        }
        index("test", "type1", "1", "field1", text.toString());
        refresh();

        // Match queries
        phraseBoostTestCaseForClauses(highlighterType, 100f,
                matchQuery("field1", "highlight words together"),
                matchPhraseQuery("field1", "highlight words together"));

        // Query string with a single field
        phraseBoostTestCaseForClauses(highlighterType, 100f,
                queryStringQuery("highlight words together").field("field1"),
                queryStringQuery("\"highlight words together\"").field("field1").autoGeneratePhraseQueries(true));

        // Query string with a single field without dismax
        phraseBoostTestCaseForClauses(highlighterType, 100f,
                queryStringQuery("highlight words together").field("field1").useDisMax(false),
                queryStringQuery("\"highlight words together\"").field("field1").useDisMax(false).autoGeneratePhraseQueries(true));

        // Query string with more than one field
        phraseBoostTestCaseForClauses(highlighterType, 100f,
                queryStringQuery("highlight words together").field("field1").field("field2"),
                queryStringQuery("\"highlight words together\"").field("field1").field("field2").autoGeneratePhraseQueries(true));

        // Query string boosting the field
        phraseBoostTestCaseForClauses(highlighterType, 1f,
                queryStringQuery("highlight words together").field("field1"),
                queryStringQuery("\"highlight words together\"").field("field1", 100).autoGeneratePhraseQueries(true));
    }

    private <P extends AbstractQueryBuilder<P>> void
            phraseBoostTestCaseForClauses(String highlighterType, float boost, QueryBuilder terms, P phrase) {
        Matcher<String> highlightedMatcher = Matchers.either(containsString("<em>highlight words together</em>")).or(
                containsString("<em>highlight</em> <em>words</em> <em>together</em>"));
        SearchRequestBuilder search = client().prepareSearch("test").highlighter(
                new HighlightBuilder().field("field1", 100, 1).order("score").highlighterType(highlighterType).requireFieldMatch(true));

        // Try with a bool query
        phrase.boost(boost);
        SearchResponse response = search.setQuery(boolQuery().must(terms).should(phrase)).get();
        assertHighlight(response, 0, "field1", 0, 1, highlightedMatcher);
        phrase.boost(1);
        // Try with a boosting query
        response = search.setQuery(boostingQuery(phrase, terms).boost(boost).negativeBoost(1)).get();
        assertHighlight(response, 0, "field1", 0, 1, highlightedMatcher);
        // Try with a boosting query using a negative boost
        response = search.setQuery(boostingQuery(phrase, terms).boost(1).negativeBoost(1/boost)).get();
        assertHighlight(response, 0, "field1", 0, 1, highlightedMatcher);
    }

    public void testGeoFieldHighlightingWithDifferentHighlighters() throws IOException {
        // check that we do not get an exception for geo_point fields in case someone tries to highlight
        // it accidentially with a wildcard
        // see https://github.com/elastic/elasticsearch/issues/17537
        XContentBuilder mappings = jsonBuilder();
        mappings.startObject();
        mappings.startObject("type")
            .startObject("properties")
            .startObject("geo_point")
            .field("type", "geo_point")
            .field("geohash", true)
            .endObject()
            .startObject("text")
            .field("type", "text")
            .field("term_vector", "with_positions_offsets_payloads")
            .field("index_options", "offsets")
            .endObject()
            .endObject()
            .endObject();
        mappings.endObject();
        assertAcked(prepareCreate("test")
            .addMapping("type", mappings));
        ensureYellow();

        client().prepareIndex("test", "type", "1")
            .setSource(jsonBuilder().startObject().field("text", "Arbitrary text field which will should not cause a failure").endObject())
            .get();
        refresh();
        String highlighterType = randomFrom("plain", "fvh", "postings");
        QueryBuilder query = QueryBuilders.boolQuery().should(QueryBuilders.geoBoundingBoxQuery("geo_point")
            .setCorners(61.10078883158897, -170.15625, -64.92354174306496, 118.47656249999999))
            .should(QueryBuilders.termQuery("text", "failure"));
        SearchResponse search = client().prepareSearch().setSource(
            new SearchSourceBuilder().query(query)
                .highlighter(new HighlightBuilder().field("*").highlighterType(highlighterType))).get();
        assertNoFailures(search);
        assertThat(search.getHits().totalHits(), equalTo(1L));
        assertThat(search.getHits().getAt(0).highlightFields().get("text").fragments().length, equalTo(1));
    }

    public void testKeywordFieldHighlighting() throws IOException {
        // check that keyword highlighting works
        XContentBuilder mappings = jsonBuilder();
        mappings.startObject();
        mappings.startObject("type")
            .startObject("properties")
            .startObject("keyword_field")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();
        mappings.endObject();
        assertAcked(prepareCreate("test")
            .addMapping("type", mappings));
        ensureYellow();

        client().prepareIndex("test", "type", "1")
            .setSource(jsonBuilder().startObject().field("keyword_field", "some text").endObject())
            .get();
        refresh();
        SearchResponse search = client().prepareSearch().setSource(
            new SearchSourceBuilder().query(QueryBuilders.matchQuery("keyword_field", "some text")).highlighter(new HighlightBuilder().field("*")))
            .get();
        assertNoFailures(search);
        assertThat(search.getHits().totalHits(), equalTo(1L));
        assertThat(search.getHits().getAt(0).getHighlightFields().get("keyword_field").getFragments()[0].string(), equalTo("<em>some text</em>"));
    }

    public void testStringFieldHighlighting() throws IOException {
        // check that string field highlighting on old indexes works
        XContentBuilder mappings = jsonBuilder();
        mappings.startObject();
        mappings.startObject("type")
            .startObject("properties")
            .startObject("string_field")
            .field("type", "string")
            .endObject()
            .endObject()
            .endObject();
        mappings.endObject();
        assertAcked(prepareCreate("test")
            .addMapping("type", mappings)
        .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_3_2)));
        ensureYellow();

        client().prepareIndex("test", "type", "1")
            .setSource(jsonBuilder().startObject().field("string_field", "some text").endObject())
            .get();
        refresh();
        SearchResponse search = client().prepareSearch().setSource(
            new SearchSourceBuilder().query(QueryBuilders.matchQuery("string_field", "some text")).highlighter(new HighlightBuilder().field("*")))
            .get();
        assertNoFailures(search);
        assertThat(search.getHits().totalHits(), equalTo(1L));
        assertThat(search.getHits().getAt(0).getHighlightFields().get("string_field").getFragments()[0].string(), equalTo("<em>some</em> <em>text</em>"));
    }
}
