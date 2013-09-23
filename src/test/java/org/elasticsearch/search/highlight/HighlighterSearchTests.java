/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.highlight;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.*;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.MatchQueryBuilder.Type;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder.Field;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH;
import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.highlight;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class HighlighterSearchTests extends ElasticsearchIntegrationTest {
    
    @Test
    // see #3486
    public void testHighTermFrequencyDoc() throws ElasticSearchException, IOException {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("test", "name", "type=string,term_vector=with_positions_offsets,store=" + (randomBoolean() ? "yes" : "no"))
                .setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", between(1, 5))));
        ensureYellow();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 6000; i++) {
            builder.append("abc").append(" ");
        }
        client().prepareIndex("test", "test", "1")
            .setSource("name", builder.toString())
            .get();
        refresh();
        SearchResponse search = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("name", "abc"))).addHighlightedField("name").get();
        assertHighlight(search, 0, "name", 0, startsWith("<em>abc</em> <em>abc</em> <em>abc</em> <em>abc</em>"));
    }

    @Test
    public void testNgramHighlightingWithBrokenPositions() throws ElasticSearchException, IOException {
        assertAcked(prepareCreate("test")
                .addMapping("test", jsonBuilder()
                        .startObject()
                        .startObject("test")
                        .startObject("properties")
                        .startObject("name")
                        .startObject("fields")
                        .startObject("autocomplete")
                        .field("type", "string")
                        .field("index_analyzer", "autocomplete")
                        .field("search_analyzer", "search_autocomplete")
                        .field("term_vector", "with_positions_offsets")
                        .endObject()
                        .startObject("name")
                        .field("type", "string")
                        .endObject()
                        .endObject()
                        .field("type", "multi_field")
                        .endObject()
                        .endObject()
                        .endObject())
                .setSettings(ImmutableSettings.settingsBuilder()
                        .put("index.number_of_shards", 1)
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
        SearchResponse search = client().prepareSearch("test").setTypes("test").setQuery(matchQuery("name.autocomplete", "deut tel").operator(Operator.OR)).addHighlightedField("name.autocomplete").execute().actionGet();
        assertHighlight(search, 0, "name.autocomplete", 0, equalTo("ARCO<em>TEL</em> Ho<em>tel</em>s <em>Deut</em>schland"));
    }
    
    @Test 
    public void testMultiPhraseCutoff() throws ElasticSearchException, IOException {
        /*
         * MultiPhraseQuery can literally kill an entire node if there are too many terms in the
         * query. We cut off and extract terms if there are more than 16 terms in the query
         */
        assertAcked(prepareCreate("test")
                .addMapping("test", "body", "type=string,index_analyzer=custom_analyzer,search_analyzer=custom_analyzer,term_vector=with_positions_offsets")
                .setSettings(
                        ImmutableSettings.settingsBuilder().put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
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
        SearchResponse search = client().prepareSearch().setQuery(matchQuery("body", "Test: http://www.facebook.com ").type(Type.PHRASE)).addHighlightedField("body").execute().actionGet();
        assertHighlight(search, 0, "body", 0, startsWith("<em>Test: http://www.facebook.com</em>"));
        search = client().prepareSearch().setQuery(matchQuery("body", "Test: http://www.facebook.com http://elasticsearch.org http://xing.com http://cnn.com http://quora.com http://twitter.com this is a test for highlighting feature Test: http://www.facebook.com http://elasticsearch.org http://xing.com http://cnn.com http://quora.com http://twitter.com this is a test for highlighting feature").type(Type.PHRASE)).addHighlightedField("body").execute().actionGet();
        assertHighlight(search, 0, "body", 0, equalTo("<em>Test</em>: <em>http</em>://<em>www</em>.<em>facebook</em>.<em>com</em> <em>http</em>://<em>elasticsearch</em>.<em>org</em> <em>http</em>://<em>xing</em>.<em>com</em> <em>http</em>://<em>cnn</em>.<em>com</em> <em>http</em>://<em>quora</em>.com"));
    }
    
    @Test
    public void testNgramHighlightingPreLucene42() throws ElasticSearchException, IOException {

        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("test",
                        "name", "type=string,index_analyzer=name_index_analyzer,search_analyzer=name_search_analyzer," + randomStoreField() + "term_vector=with_positions_offsets",
                        "name2", "type=string,index_analyzer=name2_index_analyzer,search_analyzer=name_search_analyzer," + randomStoreField() + "term_vector=with_positions_offsets")
                .setSettings(ImmutableSettings.settingsBuilder()
                        .put("index.number_of_shards", 2)
                        .put("analysis.filter.my_ngram.max_gram", 20)
                        .put("analysis.filter.my_ngram.version", "4.1")
                        .put("analysis.filter.my_ngram.min_gram", 1)
                        .put("analysis.filter.my_ngram.type", "ngram")
                        .put("analysis.tokenizer.my_ngramt.max_gram", 20)
                        .put("analysis.tokenizer.my_ngramt.version", "4.1")
                        .put("analysis.tokenizer.my_ngramt.min_gram", 1)
                        .put("analysis.tokenizer.my_ngramt.type", "ngram")
                        .put("analysis.analyzer.name_index_analyzer.tokenizer", "my_ngramt")
                        .put("analysis.analyzer.name2_index_analyzer.tokenizer", "whitespace")
                        .putArray("analysis.analyzer.name2_index_analyzer.filter", "lowercase", "my_ngram")
                        .put("analysis.analyzer.name_search_analyzer.tokenizer", "whitespace")
                        .put("analysis.analyzer.name_search_analyzer.filter", "lowercase")));
        ensureYellow();
        client().prepareIndex("test", "test", "1")
            .setSource("name", "logicacmg ehemals avinci - the know how company",
                    "name2", "logicacmg ehemals avinci - the know how company").get();
        client().prepareIndex("test", "test", "2")
            .setSource("name", "avinci, unilog avinci, logicacmg, logica",
                    "name2", "avinci, unilog avinci, logicacmg, logica").get();
        refresh();

        SearchResponse search = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("name", "logica m"))).addHighlightedField("name").get();
        assertHighlight(search, 0, "name", 0, equalTo("<em>logica</em>c<em>m</em>g ehe<em>m</em>als avinci - the know how co<em>m</em>pany"));
        assertHighlight(search, 1, "name", 0, equalTo("avinci, unilog avinci, <em>logica</em>c<em>m</em>g, <em>logica</em>"));

        search = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("name", "logica ma"))).addHighlightedField("name").get();
        assertHighlight(search, 0, "name", 0, equalTo("<em>logica</em>cmg ehe<em>ma</em>ls avinci - the know how company"));
        assertHighlight(search, 1, "name", 0, equalTo("avinci, unilog avinci, <em>logica</em>cmg, <em>logica</em>"));

        search = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("name", "logica"))).addHighlightedField("name").get();
        assertHighlight(search, 0, "name", 0, equalTo("<em>logica</em>cmg ehemals avinci - the know how company"));

        search = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("name2", "logica m"))).addHighlightedField("name2").get();
        assertHighlight(search, 0, "name2", 0, equalTo("<em>logica</em>c<em>m</em>g ehe<em>m</em>als avinci - the know how co<em>m</em>pany"));
        assertHighlight(search, 1, "name2", 0, equalTo("avinci, unilog avinci, <em>logica</em>c<em>m</em>g, <em>logica</em>"));

        search = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("name2", "logica ma"))).addHighlightedField("name2").get();
        assertHighlight(search, 0, "name2", 0, equalTo("<em>logica</em>cmg ehe<em>ma</em>ls avinci - the know how company"));
        assertHighlight(search, 1, "name2", 0, equalTo("avinci, unilog avinci, <em>logica</em>cmg, <em>logica</em>"));

        search = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("name2", "logica"))).addHighlightedField("name2").get();
        assertHighlight(search, 0, "name2", 0, equalTo("<em>logica</em>cmg ehemals avinci - the know how company"));
        assertHighlight(search, 1, "name2", 0, equalTo("avinci, unilog avinci, <em>logica</em>cmg, <em>logica</em>"));

    }
    
    @Test
    public void testNgramHighlighting() throws ElasticSearchException, IOException {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("test",
                        "name", "type=string,index_analyzer=name_index_analyzer,search_analyzer=name_search_analyzer,term_vector=with_positions_offsets",
                        "name2", "type=string,index_analyzer=name2_index_analyzer,search_analyzer=name_search_analyzer,term_vector=with_positions_offsets")
                .setSettings(ImmutableSettings.settingsBuilder()
                        .put("index.number_of_shards", 2)
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
        SearchResponse search = client().prepareSearch().setQuery(matchQuery("name", "logica m")).addHighlightedField("name").get();
        assertHighlight(search, 0, "name", 0, equalTo("<em>logica</em>c<em>m</em>g ehe<em>m</em>als avinci - the know how co<em>m</em>pany"));
        
        search = client().prepareSearch().setQuery(matchQuery("name", "logica ma")).addHighlightedField("name").get();
        assertHighlight(search, 0, "name", 0, equalTo("<em>logica</em>cmg ehe<em>ma</em>ls avinci - the know how company"));

        search = client().prepareSearch().setQuery(matchQuery("name", "logica")).addHighlightedField("name").get();
        assertHighlight(search, 0, "name", 0, equalTo("<em>logica</em>cmg ehemals avinci - the know how company"));
        
        search = client().prepareSearch().setQuery(matchQuery("name2", "logica m")).addHighlightedField("name2").get();
        assertHighlight(search, 0, "name2", 0, equalTo("<em>logicacmg</em> <em>ehemals</em> avinci - the know how <em>company</em>"));
        
        search = client().prepareSearch().setQuery(matchQuery("name2", "logica ma")).addHighlightedField("name2").get();
        assertHighlight(search, 0, "name2", 0, equalTo("<em>logicacmg</em> <em>ehemals</em> avinci - the know how company"));

        search = client().prepareSearch().setQuery(matchQuery("name2", "logica")).addHighlightedField("name2").get();
        assertHighlight(search, 0, "name2", 0, equalTo("<em>logicacmg</em> ehemals avinci - the know how company"));
    }
    
    @Test
    public void testEnsureNoNegativeOffsets() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1",
                        "no_long_term", "type=string,term_vector=with_positions_offsets",
                        "long_term", "type=string,term_vector=with_positions_offsets"));
        ensureYellow();

        client().prepareIndex("test", "type1", "1")
                .setSource("no_long_term", "This is a test where foo is highlighed and should be highlighted",
                        "long_term", "This is a test thisisaverylongwordandmakessurethisfails where foo is highlighed and should be highlighted")
                .get();
        refresh();

        SearchResponse search = client().prepareSearch()
                .setQuery(matchQuery("long_term", "thisisaverylongwordandmakessurethisfails foo highlighed"))
                .addHighlightedField("long_term", 18, 1)
                .get();
        assertHighlight(search, 0, "long_term", 0, 1, equalTo("<em>thisisaverylongwordandmakessurethisfails</em>"));

        search = client().prepareSearch()
                .setQuery(matchQuery("no_long_term", "test foo highlighed").type(Type.PHRASE).slop(3))
                .addHighlightedField("no_long_term", 18, 1).setHighlighterPostTags("</b>").setHighlighterPreTags("<b>")
                .get();
        assertNotHighlighted(search, 0, "no_long_term");

        search = client().prepareSearch()
                .setQuery(matchQuery("no_long_term", "test foo highlighed").type(Type.PHRASE).slop(3))
                .addHighlightedField("no_long_term", 30, 1).setHighlighterPostTags("</b>").setHighlighterPreTags("<b>")
                .get();

        assertHighlight(search, 0, "no_long_term", 0, 1, equalTo("a <b>test</b> where <b>foo</b> is <b>highlighed</b> and"));
    }
    
    @Test
    public void testSourceLookupHighlightingUsingPlainHighlighter() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        // we don't store title and don't use term vector, now lets see if it works...
                        .startObject("title").field("type", "string").field("store", "no").field("term_vector", "no").endObject()
                        .startObject("attachments").startObject("properties").startObject("body").field("type", "string").field("store", "no").field("term_vector", "no").endObject().endObject().endObject()
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
                .setQuery(fieldQuery("title", "bug"))
                .addHighlightedField("title", -1, 0)
                .get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            assertHighlight(search, i, "title", 0, equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
        }

        search = client().prepareSearch()
                .setQuery(fieldQuery("attachments.body", "attachment"))
                .addHighlightedField("attachments.body", -1, 0)
                .get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            assertHighlight(search, i, "attachments.body", 0, equalTo("<em>attachment</em> 1"));
            assertHighlight(search, i, "attachments.body", 1, equalTo("<em>attachment</em> 2"));
        }
    }

    @Test
    public void testSourceLookupHighlightingUsingFastVectorHighlighter() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        // we don't store title, now lets see if it works...
                        .startObject("title").field("type", "string").field("store", "no").field("term_vector", "with_positions_offsets").endObject()
                        .startObject("attachments").startObject("properties").startObject("body").field("type", "string").field("store", "no").field("term_vector", "with_positions_offsets").endObject().endObject().endObject()
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
                .setQuery(fieldQuery("title", "bug"))
                .addHighlightedField("title", -1, 0)
                .get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            assertHighlight(search, i, "title", 0, equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
        }

        search = client().prepareSearch()
                .setQuery(fieldQuery("attachments.body", "attachment"))
                .addHighlightedField("attachments.body", -1, 2)
                .execute().get();

        for (int i = 0; i < 5; i++) {
            assertHighlight(search, i, "attachments.body", 0, equalTo("<em>attachment</em> 1"));
            assertHighlight(search, i, "attachments.body", 1, equalTo("<em>attachment</em> 2"));
        }
    }

    @Test
    public void testSourceLookupHighlightingUsingPostingsHighlighter() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        // we don't store title, now lets see if it works...
                        .startObject("title").field("type", "string").field("store", "no").field("index_options", "offsets").endObject()
                        .startObject("attachments").startObject("properties").startObject("body").field("type", "string").field("store", "no").field("index_options", "offsets").endObject().endObject().endObject()
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
                .addHighlightedField("title", -1, 0).get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            assertHighlight(search, i, "title", 0, equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch. Hopefully it works."));
            assertHighlight(search, i, "title", 1, 2, equalTo("This is the second <em>bug</em> to perform highlighting on."));
        }

        search = client().prepareSearch()
                .setQuery(matchQuery("title", "bug"))
                //sentences will be generated out of each value
                .addHighlightedField("title").get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            assertHighlight(search, i, "title", 0, equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch."));
            assertHighlight(search, i, "title", 1, 2, equalTo("This is the second <em>bug</em> to perform highlighting on."));
        }

        search = client().prepareSearch()
                .setQuery(matchQuery("attachments.body", "attachment"))
                .addHighlightedField("attachments.body", -1, 2)
                .get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            assertHighlight(search, i, "attachments.body", 0, equalTo("<em>attachment</em> for this test"));
            assertHighlight(search, i, "attachments.body", 1, 2, equalTo("<em>attachment</em> 2"));
        }
    }

    @Test
    public void testHighlightIssue1994() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", "title", "type=string,store=no", "titleTV", "type=string,store=no,term_vector=with_positions_offsets"));
        ensureYellow();

        indexRandom(false, client().prepareIndex("test", "type1", "1")
                .setSource("title", new String[]{"This is a test on the highlighting bug present in elasticsearch", "The bug is bugging us"},
                        "titleTV", new String[]{"This is a test on the highlighting bug present in elasticsearch", "The bug is bugging us"}));

        indexRandom(true, client().prepareIndex("test", "type1", "2")
                .setSource("titleTV", new String[]{"some text to highlight", "highlight other text"}));

        SearchResponse search = client().prepareSearch()
                .setQuery(fieldQuery("title", "bug"))
                .addHighlightedField("title", -1, 2)
                .addHighlightedField("titleTV", -1, 2)
                .get();

        assertHighlight(search, 0, "title", 0,  equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
        assertHighlight(search, 0, "title", 1, 2, equalTo("The <em>bug</em> is bugging us"));
        assertHighlight(search, 0, "titleTV", 0,  equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
        assertHighlight(search, 0, "titleTV", 1, 2,  equalTo("The <em>bug</em> is bugging us"));

        search = client().prepareSearch()
                .setQuery(fieldQuery("titleTV", "highlight"))
                .addHighlightedField("titleTV", -1, 2)
                .get();

        assertHighlight(search, 0, "titleTV", 0, equalTo("some text to <em>highlight</em>"));
        assertHighlight(search, 0, "titleTV", 1, 2, equalTo("<em>highlight</em> other text"));
    }

    @Test
    public void testGlobalHighlightingSettingsOverriddenAtFieldLevel() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "this is another test").get();
        refresh();

        logger.info("--> highlighting and searching on field1 and field2 produces different tags");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "test"))
                .from(0).size(60).explain(true)
                .highlight(highlight().order("score").preTags("<global>").postTags("</global>")
                        .field(new HighlightBuilder.Field("field1"))
                        .field(new HighlightBuilder.Field("field2").preTags("<field2>").postTags("</field2>")));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();

        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("this is a <global>test</global>"));
        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("this is another <field2>test</field2>"));
    }

    @Test
    public void testHighlightingOnWildcardFields() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "this is another test").get();
        refresh();

        logger.info("--> highlighting and searching on field*");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "test"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field*").order("score").preTags("<xxx>").postTags("</xxx>"));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();

        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("this is a <xxx>test</xxx>"));
        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("this is another <xxx>test</xxx>"));
    }

    @Test
    public void testPlainHighlighter() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog").get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "test"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field1").order("score").preTags("<xxx>").postTags("</xxx>"));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();

        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field1");
        source = searchSource()
                .query(termQuery("_all", "test"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field1").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();

        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(termQuery("_all", "quick"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(prefixQuery("_all", "qui"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));

        logger.info("--> searching on _all with constant score, highlighting on field2");
        source = searchSource()
                .query(constantScoreQuery(prefixQuery("_all", "qui")))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));

        logger.info("--> searching on _all with constant score, highlighting on field2");
        source = searchSource()
                .query(boolQuery().should(constantScoreQuery(prefixQuery("_all", "qui"))))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));
    }

    @Test
    public void testFastVectorHighlighter() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type1", type1TermVectorMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog").get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "test"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field1", 100, 0).order("score").preTags("<xxx>").postTags("</xxx>"));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();

        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field1");
        source = searchSource()
                .query(termQuery("_all", "test"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field1", 100, 0).order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();

        // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(termQuery("_all", "quick"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2", 100, 0).order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();

        // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(prefixQuery("_all", "qui"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2", 100, 0).order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();

        // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));
    }

    @Test
    public void testMatchedFieldsFvhRequireFieldMatch() throws Exception {
        checkMatchedFieldsCase(true);
    }

    @Test
    public void testMatchedFieldsFvhNoRequireFieldMatch() throws Exception {
        checkMatchedFieldsCase(false);
    }

    private void checkMatchedFieldsCase(boolean requireFieldMatch) throws Exception {
        client().admin().indices().prepareCreate("test")
            .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties")
                    .startObject("foo")
                        .field("type", "multi_field")
                        .startObject("fields")
                            .startObject("foo")
                                .field("type", "string")
                                .field("termVector", "with_positions_offsets")
                                .field("store", "yes")
                                .field("analyzer", "english")
                            .endObject()
                            .startObject("plain")
                                .field("type", "string")
                                .field("termVector", "with_positions_offsets")
                                .field("analyzer", "standard")
                            .endObject()
                        .endObject()
                    .endObject()
                    .startObject("bar")
                        .field("type", "multi_field")
                        .startObject("fields")
                            .startObject("bar")
                                .field("type", "string")
                                .field("termVector", "with_positions_offsets")
                                .field("store", "yes")
                                .field("analyzer", "english")
                            .endObject()
                            .startObject("plain")
                                .field("type", "string")
                                .field("termVector", "with_positions_offsets")
                                .field("analyzer", "standard")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()).execute().actionGet();
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
        Field barField = new Field("bar").numOfFragments(1).order("score").fragmentSize(25)
                .highlighterType("fvh").requireFieldMatch(requireFieldMatch);
        SearchRequestBuilder req = client().prepareSearch("test").addHighlightedField(fooField);

        // First check highlighting without any matched fields set
        SearchResponse resp = req.setQuery(queryString("running scissors").field("foo")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>running</em> with <em>scissors</em>"));

        // And that matching a subfield doesn't automatically highlight it
        resp = req.setQuery(queryString("foo.plain:running scissors").field("foo")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("running with <em>scissors</em>"));

        // Add the subfield to the list of matched fields but don't match it.  Everything should still work
        // like before we added it.
        fooField.matchedFields("foo", "foo.plain");
        resp = req.setQuery(queryString("running scissors").field("foo")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>running</em> with <em>scissors</em>"));

        // Now make half the matches come from the stored field and half from just a matched field.
        resp = req.setQuery(queryString("foo.plain:running scissors").field("foo")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>running</em> with <em>scissors</em>"));

        // Now remove the stored field from the matched field list.  That should work too.
        fooField.matchedFields("foo.plain");
        resp = req.setQuery(queryString("foo.plain:running scissors").field("foo")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>running</em> with scissors"));

        // Now make sure boosted fields don't blow up when matched fields is both the subfield and stored field.
        fooField.matchedFields("foo", "foo.plain");
        resp = req.setQuery(queryString("foo.plain:running^5 scissors").field("foo")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>running</em> with <em>scissors</em>"));

        // Now just all matches are against the matched field.  This still returns highlighting.
        resp = req.setQuery(queryString("foo.plain:running foo.plain:scissors").field("foo")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>running</em> with <em>scissors</em>"));

        // And all matched field via the queryString's field parameter, just in case
        resp = req.setQuery(queryString("running scissors").field("foo.plain")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>running</em> with <em>scissors</em>"));

        // Finding the same string two ways is ok too
        resp = req.setQuery(queryString("run foo.plain:running^5 scissors").field("foo")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>running</em> with <em>scissors</em>"));

        // But we use the best found score when sorting fragments
        resp = req.setQuery(queryString("cats foo.plain:cats^5").field("foo")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("junk junk <em>cats</em> junk junk"));

        // which can also be written by searching on the subfield
        resp = req.setQuery(queryString("cats").field("foo").field("foo.plain^5")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("junk junk <em>cats</em> junk junk"));

        // Speaking of two fields, you can have two fields, only one of which has matchedFields enabled
        QueryBuilder twoFieldsQuery = queryString("cats").field("foo").field("foo.plain^5")
                .field("bar").field("bar.plain^5");
        resp = req.setQuery(twoFieldsQuery).addHighlightedField(barField).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("junk junk <em>cats</em> junk junk"));
        assertHighlight(resp, 0, "bar", 0, equalTo("<em>cat</em> <em>cat</em> junk junk junk junk"));

        // And you can enable matchedField highlighting on both
        barField.matchedFields("bar", "bar.plain");
        resp = req.get();
        assertHighlight(resp, 0, "foo", 0, equalTo("junk junk <em>cats</em> junk junk"));
        assertHighlight(resp, 0, "bar", 0, equalTo("junk junk <em>cats</em> junk junk"));

        // Setting a matchedField that isn't searched/doesn't exist is simply ignored.
        barField.matchedFields("bar", "candy");
        resp = req.get();
        assertHighlight(resp, 0, "foo", 0, equalTo("junk junk <em>cats</em> junk junk"));
        assertHighlight(resp, 0, "bar", 0, equalTo("<em>cat</em> <em>cat</em> junk junk junk junk"));

        // If the stored field doesn't have a value it doesn't matter what you match, you get nothing.
        barField.matchedFields("bar", "foo.plain");
        resp = req.setQuery(queryString("running scissors").field("foo.plain").field("bar")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>running</em> with <em>scissors</em>"));
        assertThat(resp.getHits().getAt(0).getHighlightFields(), not(hasKey("bar")));

        // If the stored field is found but the matched field isn't then you don't get a result either.
        fooField.matchedFields("bar.plain");
        resp = req.setQuery(queryString("running scissors").field("foo").field("foo.plain").field("bar").field("bar.plain")).get();
        assertThat(resp.getHits().getAt(0).getHighlightFields(), not(hasKey("foo")));

        // But if you add the stored field to the list of matched fields then you'll get a result again
        fooField.matchedFields("foo", "bar.plain");
        resp = req.setQuery(queryString("running scissors").field("foo").field("foo.plain").field("bar").field("bar.plain")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>running</em> with <em>scissors</em>"));
        assertThat(resp.getHits().getAt(0).getHighlightFields(), not(hasKey("bar")));

        // You _can_ highlight fields that aren't subfields of one another.
        resp = req.setQuery(queryString("weird").field("foo").field("foo.plain").field("bar").field("bar.plain")).get();
        assertHighlight(resp, 0, "foo", 0, equalTo("<em>weird</em>"));
        assertHighlight(resp, 0, "bar", 0, equalTo("<em>resul</em>t"));

        //But be careful.  It'll blow up if there is a result paste the end of the field.
        resp = req.setQuery(queryString("result").field("foo").field("foo.plain").field("bar").field("bar.plain")).get();
        assertThat("Expected ShardFailures", resp.getShardFailures().length, greaterThan(0));
    }

    @Test
    @Slow
    public void testFastVectorHighlighterManyDocs() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type1", type1TermVectorMapping()));
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
                .addHighlightedField("field1", 100, 0)
                .get();
        for (int i = 0; i < COUNT; i++) {
            SearchHit hit = searchResponse.getHits().getHits()[i];
            // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
            assertHighlight(searchResponse, i, "field1", 0, 1, equalTo("<em>test</em> " + hit.id()));
        }

        logger.info("--> searching explicitly on field1 and highlighting on it, with DFS");
        searchResponse = client().prepareSearch()
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSize(COUNT)
                .setQuery(termQuery("field1", "test"))
                .addHighlightedField("field1", 100, 0)
                .get();
        for (int i = 0; i < COUNT; i++) {
            SearchHit hit = searchResponse.getHits().getHits()[i];
            assertHighlight(searchResponse, i, "field1", 0, 1, equalTo("<em>test</em> " + hit.id()));
        }

        logger.info("--> searching explicitly _all and highlighting on _all");
        searchResponse = client().prepareSearch()
                .setSize(COUNT)
                .setQuery(termQuery("_all", "test"))
                .addHighlightedField("_all", 100, 0)
                .get();
        for (int i = 0; i < COUNT; i++) {
            SearchHit hit = searchResponse.getHits().getHits()[i];
            assertHighlight(searchResponse, i, "_all", 0, 1, equalTo("<em>test</em> " + hit.id() + " "));
        }
    }

    public XContentBuilder type1TermVectorMapping() throws IOException {
        return XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("_all").field("store", "yes").field("termVector", "with_positions_offsets").endObject()
                .startObject("properties")
                .startObject("field1").field("type", "string").field("termVector", "with_positions_offsets").endObject()
                .startObject("field2").field("type", "string").field("termVector", "with_positions_offsets").endObject()
                .endObject()
                .endObject().endObject();
    }

    @Test
    public void testSameContent() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", "title", "type=string,store=yes,term_vector=with_positions_offsets"));
        ensureYellow();

        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[5];
        for (int i = 0; i < 5; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a test on the highlighting bug present in elasticsearch");
        }
        indexRandom(true, indexRequestBuilders);

        SearchResponse search = client().prepareSearch()
                .setQuery(fieldQuery("title", "bug"))
                .addHighlightedField("title", -1, 0)
                .get();

        for (int i = 0; i < 5; i++) {
            assertHighlight(search, i, "title", 0, 1, equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
        }
    }

    @Test
    public void testFastVectorHighlighterOffsetParameter() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", "title", "type=string,store=yes,term_vector=with_positions_offsets").get());
        ensureYellow();

        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[5];
        for (int i = 0; i < 5; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a test on the highlighting bug present in elasticsearch");
        }
        indexRandom(true, indexRequestBuilders);

        SearchResponse search = client().prepareSearch()
                .setQuery(fieldQuery("title", "bug"))
                .addHighlightedField("title", 30, 1, 10)
                .get();

        for (int i = 0; i < 5; i++) {
            // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
            assertHighlight(search, i, "title", 0, 1, equalTo("highlighting <em>bug</em> present in elasticsearch"));
        }
    }

    @Test
    public void testEscapeHtml() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", "title", "type=string,store=yes"));
        ensureYellow();

        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[5];
        for (int i = 0; i < indexRequestBuilders.length; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a html escaping highlighting test for *&? elasticsearch");
        }
        indexRandom(true, indexRequestBuilders);

        SearchResponse search = client().prepareSearch()
                .setQuery(fieldQuery("title", "test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title", 50, 1, 10)
                .get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            assertHighlight(search, i, "title", 0, 1, equalTo("This is a html escaping highlighting <em>test</em> for *&amp;? elasticsearch"));
        }
    }

    @Test
    public void testEscapeHtml_vector() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", "title", "type=string,store=yes,term_vector=with_positions_offsets"));
        ensureYellow();

        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[5];
        for (int i = 0; i < 5; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a html escaping highlighting test for *&? elasticsearch");
        }
        indexRandom(true, indexRequestBuilders);

        SearchResponse search = client().prepareSearch()
                .setQuery(fieldQuery("title", "test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title", 30, 1, 10)
                .get();

        for (int i = 0; i < 5; i++) {
            assertHighlight(search, i, "title", 0, 1, equalTo("highlighting <em>test</em> for *&amp;? elasticsearch"));
        }
    }

    @Test
    public void testMultiMapperVectorWithStore() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "multi_field").startObject("fields")
                        .startObject("title").field("type", "string").field("store", "yes").field("term_vector", "with_positions_offsets").endObject()
                        .startObject("key").field("type", "string").field("store", "yes").field("term_vector", "with_positions_offsets").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject()));
        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource("title", "this is a test").get();
        refresh();
        // simple search on body with standard analyzer with a simple field query
        SearchResponse search = client().prepareSearch()
                .setQuery(fieldQuery("title", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title", 50, 1)
                .get();

        assertHighlight(search, 0, "title", 0, 1, equalTo("this is a <em>test</em>"));

        // search on title.key and highlight on title
        search = client().prepareSearch()
                .setQuery(fieldQuery("title.key", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title.key", 50, 1)
                .get();

        assertHighlight(search, 0, "title.key", 0, 1, equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em>"));
    }

    @Test
    public void testMultiMapperVectorFromSource() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "multi_field").startObject("fields")
                        .startObject("title").field("type", "string").field("store", "no").field("term_vector", "with_positions_offsets").endObject()
                        .startObject("key").field("type", "string").field("store", "no").field("term_vector", "with_positions_offsets").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject()));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("title", "this is a test").get();
        refresh();

        // simple search on body with standard analyzer with a simple field query
        SearchResponse search = client().prepareSearch()
                .setQuery(fieldQuery("title", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title", 50, 1)
                .get();

        assertHighlight(search, 0, "title", 0, 1, equalTo("this is a <em>test</em>"));

        // search on title.key and highlight on title.key
        search = client().prepareSearch()
                .setQuery(fieldQuery("title.key", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title.key", 50, 1)
                .get();

        assertHighlight(search, 0, "title.key", 0, 1, equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em>"));
    }

    @Test
    public void testMultiMapperNoVectorWithStore() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "multi_field").startObject("fields")
                        .startObject("title").field("type", "string").field("store", "yes").field("term_vector", "no").endObject()
                        .startObject("key").field("type", "string").field("store", "yes").field("term_vector", "no").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject()));

        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource("title", "this is a test").get();
        refresh();

        // simple search on body with standard analyzer with a simple field query
        SearchResponse search = client().prepareSearch()
                .setQuery(fieldQuery("title", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title", 50, 1)
                .get();

        assertHighlight(search, 0, "title", 0, 1, equalTo("this is a <em>test</em>"));

        // search on title.key and highlight on title
        search = client().prepareSearch()
                .setQuery(fieldQuery("title.key", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title.key", 50, 1)
                .get();

        assertHighlight(search, 0, "title.key", 0, 1, equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em>"));
    }

    @Test
    public void testMultiMapperNoVectorFromSource() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "multi_field").startObject("fields")
                        .startObject("title").field("type", "string").field("store", "no").field("term_vector", "no").endObject()
                        .startObject("key").field("type", "string").field("store", "no").field("term_vector", "no").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject()));
        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource("title", "this is a test").get();
        refresh();

        // simple search on body with standard analyzer with a simple field query
        SearchResponse search = client().prepareSearch()
                .setQuery(fieldQuery("title", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title", 50, 1)
                .get();

        assertHighlight(search, 0, "title", 0, 1, equalTo("this is a <em>test</em>"));

        // search on title.key and highlight on title.key
        search = client().prepareSearch()
                .setQuery(fieldQuery("title.key", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title.key", 50, 1)
                .get();

        assertHighlight(search, 0, "title.key", 0, 1, equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em>"));
    }

    @Test
    public void testFastVectorHighlighterShouldFailIfNoTermVectors() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", "title", "type=string,store=yes,term_vector=no"));
        ensureGreen();

        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[5];
        for (int i = 0; i < 5; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a test for the enabling fast vector highlighter");
        }
        indexRandom(true, indexRequestBuilders);

        SearchResponse search = client().prepareSearch()
                .setQuery(matchPhraseQuery("title", "this is a test"))
                .addHighlightedField("title", 50, 1, 10)
                .get();
        assertNoFailures(search);

        search = client().prepareSearch()
                .setQuery(matchPhraseQuery("title", "this is a test"))
                .addHighlightedField("title", 50, 1, 10)
                .setHighlighterType("fast-vector-highlighter")
                .execute().actionGet();
        assertThat(search.getFailedShards(), equalTo(2));
    }

    @Test
    public void testDisableFastVectorHighlighter() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", "title", "type=string,store=yes,term_vector=with_positions_offsets,analyzer=classic"));
        ensureGreen();

        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[5];
        for (int i = 0; i < indexRequestBuilders.length; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a test for the workaround for the fast vector highlighting SOLR-3724");
        }
        indexRandom(true, indexRequestBuilders);

        SearchResponse search = client().prepareSearch()
                .setQuery(matchPhraseQuery("title", "test for the workaround"))
                .addHighlightedField("title", 50, 1, 10)
                .get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            // Because of SOLR-3724 nothing is highlighted when FVH is used
            assertNotHighlighted(search, i, "title");
        }

        // Using plain highlighter instead of FVH
        search = client().prepareSearch()
                .setQuery(matchPhraseQuery("title", "test for the workaround"))
                .addHighlightedField("title", 50, 1, 10)
                .setHighlighterType("highlighter")
                .get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            assertHighlight(search, i, "title", 0, 1, equalTo("This is a <em>test</em> for the <em>workaround</em> for the fast vector highlighting SOLR-3724"));
        }

        // Using plain highlighter instead of FVH on the field level
        search = client().prepareSearch()
                .setQuery(matchPhraseQuery("title", "test for the workaround"))
                .addHighlightedField(new HighlightBuilder.Field("title").highlighterType("highlighter"))
                .setHighlighterType("highlighter")
                .get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            assertHighlight(search, i, "title", 0, 1, equalTo("This is a <em>test</em> for the <em>workaround</em> for the fast vector highlighting SOLR-3724"));
        }
    }

    @Test
    public void testFSHHighlightAllMvFragments() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .addMapping("type1", "tags", "type=string,term_vector=with_positions_offsets"));
        ensureGreen();
        client().prepareIndex("test", "type1", "1")
                .setSource("tags", new String[]{
                        "this is a really long tag i would like to highlight",
                        "here is another one that is very long and has the tag token near the end"}).get();
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("tags", "tag"))
                .addHighlightedField("tags", -1, 0).get();

        assertHighlight(response, 0, "tags", 0, equalTo("this is a really long <em>tag</em> i would like to highlight"));
        assertHighlight(response, 0, "tags", 1, 2, equalTo("here is another one that is very long and has the <em>tag</em> token near the end"));
    }
    
    @Test
    public void testBoostingQuery() {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog").get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(boostingQuery().positive(termQuery("field2", "brown")).negative(termQuery("field2", "foobar")).negativeBoost(0.5f))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2").order("score").preTags("<x>").postTags("</x>"));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The quick <x>brown</x> fox jumps over the lazy dog"));
    }
    
    @Test
    public void testBoostingQueryTermVector() throws ElasticSearchException, IOException {
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type1", type1TermVectorMapping()));
        ensureGreen();
        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog")
                .get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(boostingQuery().positive(termQuery("field2", "brown")).negative(termQuery("field2", "foobar")).negativeBoost(0.5f))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2").order("score").preTags("<x>").postTags("</x>"));

        SearchResponse searchResponse = client().search(
                searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The quick <x>brown</x> fox jumps over the lazy dog"));
    }

    @Test
    public void testCommonTermsQuery() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog")
                .get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(commonTerms("field2", "quick brown").cutoffFrequency(100))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2").order("score").preTags("<x>").postTags("</x>"));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <x>quick</x> <x>brown</x> fox jumps over the lazy dog"));
    }

    @Test
    public void testCommonTermsTermVector() throws ElasticSearchException, IOException {
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type1", type1TermVectorMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog").get();
        refresh();
        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource().query(commonTerms("field2", "quick brown").cutoffFrequency(100)).from(0).size(60)
                .explain(true).highlight(highlight().field("field2").order("score").preTags("<x>").postTags("</x>"));

        SearchResponse searchResponse = client().search(
                searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <x>quick</x> <x>brown</x> fox jumps over the lazy dog"));
    }

    @Test
    public void testPhrasePrefix() throws ElasticSearchException, IOException {
        Builder builder = ImmutableSettings.builder();
        builder.put("index.analysis.analyzer.synonym.tokenizer", "whitespace");
        builder.putArray("index.analysis.analyzer.synonym.filter", "synonym", "lowercase");
        builder.put("index.analysis.filter.synonym.type", "synonym");
        builder.putArray("index.analysis.filter.synonym.synonyms", "quick => fast");

        assertAcked(client().admin().indices().prepareCreate("test").setSettings(builder.build()).addMapping("type1", type1TermVectorMapping())
                .addMapping("type2", "_all", "store=yes,termVector=with_positions_offsets",
                        "field4", "type=string,term_vector=with_positions_offsets,analyzer=synonym",
                        "field3", "type=string,analyzer=synonym"));
        ensureGreen();

        client().prepareIndex("test", "type1", "0")
                .setSource("field0", "The quick brown fox jumps over the lazy dog", "field1", "The quick brown fox jumps over the lazy dog").get();
        client().prepareIndex("test", "type1", "1")
                .setSource("field1", "The quick browse button is a fancy thing, right bro?").get();
        refresh();
        logger.info("--> highlighting and searching on field0");
        SearchSourceBuilder source = searchSource()
                .query(matchPhrasePrefixQuery("field0", "quick bro"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field0").order("score").preTags("<x>").postTags("</x>"));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();

        assertHighlight(searchResponse, 0, "field0", 0, 1, equalTo("The <x>quick</x> <x>brown</x> fox jumps over the lazy dog"));

        logger.info("--> highlighting and searching on field1");
        source = searchSource()
                .query(matchPhrasePrefixQuery("field1", "quick bro"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field1").order("score").preTags("<x>").postTags("</x>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();

        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("The <x>quick browse</x> button is a fancy thing, right bro?"));
        assertHighlight(searchResponse, 1, "field1", 0, 1, equalTo("The <x>quick brown</x> fox jumps over the lazy dog"));

        // with synonyms
        client().prepareIndex("test", "type2", "0")
                .setSource("field4", "The quick brown fox jumps over the lazy dog", "field3", "The quick brown fox jumps over the lazy dog").get();
        client().prepareIndex("test", "type2", "1")
                .setSource("field4", "The quick browse button is a fancy thing, right bro?").get();
        client().prepareIndex("test", "type2", "2")
                .setSource("field4", "a quick fast blue car").get();
        refresh();

        source = searchSource().filter(FilterBuilders.typeFilter("type2")).query(matchPhrasePrefixQuery("field3", "fast bro")).from(0).size(60).explain(true)
                .highlight(highlight().field("field3").order("score").preTags("<x>").postTags("</x>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();

        assertHighlight(searchResponse, 0, "field3", 0, 1, equalTo("The <x>quick</x> <x>brown</x> fox jumps over the lazy dog"));

        logger.info("--> highlighting and searching on field4");
        source = searchSource().filter(FilterBuilders.typeFilter("type2")).query(matchPhrasePrefixQuery("field4", "the fast bro")).from(0).size(60).explain(true)
                .highlight(highlight().field("field4").order("score").preTags("<x>").postTags("</x>"));
        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();

        assertHighlight(searchResponse, 0, "field4", 0, 1, equalTo("<x>The quick browse</x> button is a fancy thing, right bro?"));
        assertHighlight(searchResponse, 1, "field4", 0, 1, equalTo("<x>The quick brown</x> fox jumps over the lazy dog"));

        logger.info("--> highlighting and searching on field4");
        source = searchSource().filter(FilterBuilders.typeFilter("type2")).query(matchPhrasePrefixQuery("field4", "a fast quick blue ca")).from(0).size(60).explain(true)
                .highlight(highlight().field("field4").order("score").preTags("<x>").postTags("</x>"));
        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();

        assertHighlight(searchResponse, 0, "field4", 0, 1, equalTo("<x>a quick fast blue car</x>"));
    }

    @Test
    public void testPlainHighlightDifferentFragmenter() throws Exception {
        assertAcked(prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .addMapping("type1", "tags", "type=string"));
        ensureGreen();
        client().prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject().field("tags",
                        "this is a really long tag i would like to highlight",
                        "here is another one that is very long tag and has the tag token near the end").endObject()).get();
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("tags", "long tag").type(MatchQueryBuilder.Type.PHRASE))
                .addHighlightedField(new HighlightBuilder.Field("tags")
                        .fragmentSize(-1).numOfFragments(2).fragmenter("simple")).get();

        assertHighlight(response, 0, "tags", 0, equalTo("this is a really <em>long</em> <em>tag</em> i would like to highlight"));
        assertHighlight(response, 0, "tags", 1, 2, equalTo("here is another one that is very <em>long</em> <em>tag</em> and has the tag token near the end"));

        response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("tags", "long tag").type(MatchQueryBuilder.Type.PHRASE))
                .addHighlightedField(new HighlightBuilder.Field("tags")
                        .fragmentSize(-1).numOfFragments(2).fragmenter("span")).get();

        assertHighlight(response, 0, "tags", 0, equalTo("this is a really <em>long</em> <em>tag</em> i would like to highlight"));
        assertHighlight(response, 0, "tags", 1, 2, equalTo("here is another one that is very <em>long</em> <em>tag</em> and has the tag token near the end"));

        try {
            client().prepareSearch("test")
                    .setQuery(QueryBuilders.matchQuery("tags", "long tag").type(MatchQueryBuilder.Type.PHRASE))
                    .addHighlightedField(new HighlightBuilder.Field("tags")
                            .fragmentSize(-1).numOfFragments(2).fragmenter("invalid")).get();
            fail("Shouldn't get here");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.shardFailures()[0].status(), equalTo(RestStatus.BAD_REQUEST));
        }
    }

    @Test
    public void testPlainHighlighterMultipleFields() {
        createIndex("test");
        ensureGreen();

        index("test", "type1", "1", "field1", "The <b>quick<b> brown fox", "field2", "The <b>slow<b> brown fox");
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("field1", "fox"))
                .addHighlightedField(new HighlightBuilder.Field("field1").preTags("<1>").postTags("</1>").requireFieldMatch(true))
                .addHighlightedField(new HighlightBuilder.Field("field2").preTags("<2>").postTags("</2>").requireFieldMatch(false))
                .get();
        assertHighlight(response, 0, "field1", 0, 1, equalTo("The <b>quick<b> brown <1>fox</1>"));
        assertHighlight(response, 0, "field2", 0, 1, equalTo("The <b>slow<b> brown <2>fox</2>"));
    }

    @Test
    public void testFastVectorHighlighterMultipleFields() {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=string,term_vectors=with_positions_offsets", "field2", "type=string,term_vectors=with_positions_offsets"));
        ensureGreen();

        index("test", "type1", "1", "field1", "The <b>quick<b> brown fox", "field2", "The <b>slow<b> brown fox");
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("field1", "fox"))
                .addHighlightedField(new HighlightBuilder.Field("field1").preTags("<1>").postTags("</1>").requireFieldMatch(true))
                .addHighlightedField(new HighlightBuilder.Field("field2").preTags("<2>").postTags("</2>").requireFieldMatch(false))
                .get();
        assertHighlight(response, 0, "field1", 0, 1, equalTo("The <b>quick<b> brown <1>fox</1>"));
        assertHighlight(response, 0, "field2", 0, 1, equalTo("The <b>slow<b> brown <2>fox</2>"));
    }

    @Test
    public void testMissingStoredField() throws Exception {
        assertAcked(prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .addMapping("type1", "highlight_field", "type=string,store=yes"));
        ensureGreen();
        client().prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject()
                        .field("field", "highlight")
                        .endObject()).get();
        refresh();

        // This query used to fail when the field to highlight was absent
        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("field", "highlight").type(MatchQueryBuilder.Type.BOOLEAN))
                .addHighlightedField(new HighlightBuilder.Field("highlight_field")
                        .fragmentSize(-1).numOfFragments(1).fragmenter("simple")).get();
        assertThat(response.getHits().hits()[0].highlightFields().isEmpty(), equalTo(true));
    }

    @Test
    // https://github.com/elasticsearch/elasticsearch/issues/3211
    public void testNumericHighlighting() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("test", "text", "type=string,index=analyzed",
                        "byte", "type=byte", "short", "type=short", "int", "type=integer", "long", "type=long",
                        "float", "type=float", "double", "type=double"));
        ensureGreen();

        client().prepareIndex("test", "test", "1").setSource("text", "elasticsearch test",
                "byte", 25, "short", 42, "int", 100, "long", -1, "float", 3.2f, "double", 42.42).get();
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("text", "test").type(MatchQueryBuilder.Type.BOOLEAN))
                .addHighlightedField("text")
                .addHighlightedField("byte")
                .addHighlightedField("short")
                .addHighlightedField("int")
                .addHighlightedField("long")
                .addHighlightedField("float")
                .addHighlightedField("double")
                .get();
        // Highlighting of numeric fields is not supported, but it should not raise errors
        // (this behavior is consistent with version 0.20)
        assertHitCount(response, 1l);
    }

    @Test
    // https://github.com/elasticsearch/elasticsearch/issues/3200
    public void testResetTwice() throws Exception {
        assertAcked(prepareCreate("test")
                .setSettings(ImmutableSettings.builder()
                        .put("analysis.analyzer.my_analyzer.type", "pattern")
                        .put("analysis.analyzer.my_analyzer.pattern", "\\s+")
                        .build())
                .addMapping("type", "text", "type=string,analyzer=my_analyzer"));
        ensureGreen();
        client().prepareIndex("test", "type", "1")
                .setSource("text", "elasticsearch test").get();
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("text", "test").type(MatchQueryBuilder.Type.BOOLEAN))
                .addHighlightedField("text").execute().actionGet();
        // PatternAnalyzer will throw an exception if it is resetted twice
        assertHitCount(response, 1l);
    }

    @Test
    public void testHighlightUsesHighlightQuery() throws IOException {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "text", "type=string," + randomStoreField() + "term_vector=with_positions_offsets,index_options=offsets"));
        ensureGreen();

        index("test", "type1", "1", "text", "Testing the highlight query feature");
        refresh();

        HighlightBuilder.Field field = new HighlightBuilder.Field("text");

        SearchRequestBuilder search = client().prepareSearch("test").setQuery(QueryBuilders.matchQuery("text", "testing"))
                .addHighlightedField(field);
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


        Matcher<String> hlQueryMatcher = equalTo("Testing the highlight <em>query</em> feature");
        field.highlightQuery(matchQuery("text", "query"));

        field.highlighterType("fvh");
        response = search.get();
        assertHighlight(response, 0, "text", 0, hlQueryMatcher);

        field.highlighterType("plain");
        response = search.get();
        assertHighlight(response, 0, "text", 0, hlQueryMatcher);

        field.highlighterType("postings");
        response = search.get();
        assertHighlight(response, 0, "text", 0, hlQueryMatcher);

        // Make sure the the highlightQuery is taken into account when it is set on the highlight context instead of the field
        search.setHighlighterQuery(matchQuery("text", "query"));
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
            return "store=yes,";
        }
        return "";
    }

    @Test
    public void testHighlightNoMatchSize() throws IOException {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "text", "type=string," + randomStoreField() + "term_vector=with_positions_offsets,index_options=offsets"));
        ensureGreen();

        String text = "I am pretty long so some of me should get cut off. Second sentence";
        index("test", "type1", "1", "text", text);
        refresh();

        // When you don't set noMatchSize you don't get any results if there isn't anything to highlight.
        HighlightBuilder.Field field = new HighlightBuilder.Field("text")
                .fragmentSize(21)
                .numOfFragments(1)
                .highlighterType("plain");
        SearchResponse response = client().prepareSearch("test").addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("fvh");
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("postings");
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "text");

        // When noMatchSize is set to 0 you also shouldn't get any
        field.highlighterType("plain").noMatchSize(0);
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("fvh");
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("postings");
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "text");

        // When noMatchSize is between 0 and the size of the string
        field.highlighterType("plain").noMatchSize(21);
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so"));

        // The FVH also works but the fragment is longer than the plain highlighter because of boundary_max_scan
        field.highlighterType("fvh");
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so some"));

        // Postings hl also works but the fragment is the whole first sentence (size ignored)
        field.highlighterType("postings");
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so some of me should get cut off."));

        // We can also ask for a fragment longer than the input string and get the whole string
        field.highlighterType("plain").noMatchSize(text.length() * 2);
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo(text));

        field.highlighterType("fvh");
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo(text));

        //no difference using postings hl as the noMatchSize is ignored (just needs to be greater than 0)
        field.highlighterType("postings");
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so some of me should get cut off."));

        // We can also ask for a fragment exactly the size of the input field and get the whole field
        field.highlighterType("plain").noMatchSize(text.length());
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo(text));

        field.highlighterType("fvh");
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo(text));

        //no difference using postings hl as the noMatchSize is ignored (just needs to be greater than 0)
        field.highlighterType("postings");
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so some of me should get cut off."));

        // You can set noMatchSize globally in the highlighter as well
        field.highlighterType("plain").noMatchSize(null);
        response = client().prepareSearch("test").setHighlighterNoMatchSize(21).addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so"));

        field.highlighterType("fvh");
        response = client().prepareSearch("test").setHighlighterNoMatchSize(21).addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so some"));

        field.highlighterType("postings");
        response = client().prepareSearch("test").setHighlighterNoMatchSize(21).addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so some of me should get cut off."));

        // We don't break if noMatchSize is less than zero though
        field.highlighterType("plain").noMatchSize(randomIntBetween(Integer.MIN_VALUE, -1));
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("fvh");
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("postings");
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "text");
    }

    @Test
    public void testHighlightNoMatchSizeWithMultivaluedFields() throws IOException {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "text", "type=string," + randomStoreField() + "term_vector=with_positions_offsets,index_options=offsets"));
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
        SearchResponse response = client().prepareSearch("test").addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so"));

        field.highlighterType("fvh");
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so some"));

        // Postings hl also works but the fragment is the whole first sentence (size ignored)
        field.highlighterType("postings");
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("I am pretty long so some of me should get cut off."));

        // And noMatchSize returns nothing when the first entry is empty string!
        index("test", "type1", "2", "text", new String[] {"", text2});
        refresh();

        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery("type1").addIds("2");
        field.highlighterType("plain");
        response = client().prepareSearch("test")
                .setQuery(idsQueryBuilder)
                .addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("fvh");
        response = client().prepareSearch("test")
                .setQuery(idsQueryBuilder)
                .addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("postings");
        response = client().prepareSearch("test")
                .setQuery(idsQueryBuilder)
                .addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "text");

        // But if the field was actually empty then you should get no highlighting field
        index("test", "type1", "3", "text", new String[] {});
        refresh();
        idsQueryBuilder = QueryBuilders.idsQuery("type1").addIds("3");
        field.highlighterType("plain");
        response = client().prepareSearch("test")
                .setQuery(idsQueryBuilder)
                .addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("fvh");
        response = client().prepareSearch("test")
                .setQuery(idsQueryBuilder)
                .addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("postings");
        response = client().prepareSearch("test")
                .setQuery(idsQueryBuilder)
                .addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "text");

        // Same for if the field doesn't even exist on the document
        index("test", "type1", "4");
        refresh();

        idsQueryBuilder = QueryBuilders.idsQuery("type1").addIds("4");
        field.highlighterType("plain");
        response = client().prepareSearch("test")
                .setQuery(idsQueryBuilder)
                .addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("fvh");
        response = client().prepareSearch("test")
                .setQuery(idsQueryBuilder)
                .addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("fvh");
        response = client().prepareSearch("test")
                .setQuery(idsQueryBuilder)
                .addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "postings");

        // Again same if the field isn't mapped
        field = new HighlightBuilder.Field("unmapped")
                .highlighterType("plain")
                .noMatchSize(21);
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("fvh");
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "text");

        field.highlighterType("postings");
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertNotHighlighted(response, 0, "text");
    }

    @Test
    public void testHighlightNoMatchSizeNumberOfFragments() throws IOException {
        assertAcked(prepareCreate("test")
                .addMapping("type1", "text", "type=string," + randomStoreField() + "term_vector=with_positions_offsets,index_options=offsets"));
        ensureGreen();

        String text1 = "This is the first sentence. This is the second sentence.";
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
        SearchResponse response = client().prepareSearch("test").addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("This is the first"));

        field.highlighterType("fvh");
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("This is the first sentence"));

        // Postings hl also works but the fragment is the whole first sentence (size ignored)
        field.highlighterType("postings");
        response = client().prepareSearch("test").addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 1, equalTo("This is the first sentence."));

        //if there's a match we only return the values with matches (whole value as number_of_fragments == 0)
        MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("text", "third fifth");
        field.highlighterType("plain");
        response = client().prepareSearch("test").setQuery(queryBuilder).addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 2, equalTo("This is the <em>third</em> sentence. This is the fourth sentence."));
        assertHighlight(response, 0, "text", 1, 2, equalTo("This is the <em>fifth</em> sentence"));

        field.highlighterType("fvh");
        response = client().prepareSearch("test").setQuery(queryBuilder).addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 2, equalTo("This is the <em>third</em> sentence. This is the fourth sentence."));
        assertHighlight(response, 0, "text", 1, 2, equalTo("This is the <em>fifth</em> sentence"));

        field.highlighterType("postings");
        response = client().prepareSearch("test").setQuery(queryBuilder).addHighlightedField(field).get();
        assertHighlight(response, 0, "text", 0, 2, equalTo("This is the <em>third</em> sentence. This is the fourth sentence."));
        assertHighlight(response, 0, "text", 1, 2, equalTo("This is the <em>fifth</em> sentence"));
    }

    @Test
    public void testPostingsHighlighter() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy quick dog").get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "test"))
                .highlight(highlight().field("field1").preTags("<xxx>").postTags("</xxx>"));
        SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field1");
        source = searchSource()
                .query(termQuery("_all", "test"))
                .highlight(highlight().field("field1").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(termQuery("_all", "quick"))
                .highlight(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy <xxx>quick</xxx> dog"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(matchPhraseQuery("_all", "quick brown"))
                .highlight(highlight().field("field2").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        //phrase query results in highlighting all different terms regardless of their positions
        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <xxx>quick</xxx> <xxx>brown</xxx> fox jumps over the lazy <xxx>quick</xxx> dog"));

        //lets fall back to the standard highlighter then, what people would do to highlight query matches
        logger.info("--> searching on _all, highlighting on field2, falling back to the plain highlighter");
        source = searchSource()
                .query(matchPhraseQuery("_all", "quick brown"))
                .highlight(highlight().field("field2").preTags("<xxx>").postTags("</xxx>").highlighterType("highlighter"));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <xxx>quick</xxx> <xxx>brown</xxx> fox jumps over the lazy quick dog"));
    }

    @Test
    public void testPostingsHighlighterMultipleFields() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()).get());
        ensureGreen();

        index("test", "type1", "1", "field1", "The <b>quick<b> brown fox. Second sentence.", "field2", "The <b>slow<b> brown fox. Second sentence.");
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("field1", "fox"))
                .addHighlightedField(new HighlightBuilder.Field("field1").preTags("<1>").postTags("</1>").requireFieldMatch(true))
                .addHighlightedField(new HighlightBuilder.Field("field2").preTags("<2>").postTags("</2>").requireFieldMatch(false))
                .get();
        assertHighlight(response, 0, "field1", 0, 1, equalTo("The <b>quick<b> brown <1>fox</1>."));
        assertHighlight(response, 0, "field2", 0, 1, equalTo("The <b>slow<b> brown <2>fox</2>."));
    }

    @Test
    public void testPostingsHighlighterNumberOfFragments() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1", "1")
                .setSource("field1", "The quick brown fox jumps over the lazy dog. The lazy red fox jumps over the quick dog. The quick brown dog jumps over the lazy fox.",
                        "field2", "The quick brown fox jumps over the lazy dog. The lazy red fox jumps over the quick dog. The quick brown dog jumps over the lazy fox.").get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "fox"))
                .highlight(highlight()
                        .field(new HighlightBuilder.Field("field1").numOfFragments(5).preTags("<field1>").postTags("</field1>"))
                        .field(new HighlightBuilder.Field("field2").numOfFragments(2).preTags("<field2>").postTags("</field2>")));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field1", 0, equalTo("The quick brown <field1>fox</field1> jumps over the lazy dog."));
        assertHighlight(searchResponse, 0, "field1", 1, equalTo("The lazy red <field1>fox</field1> jumps over the quick dog."));
        assertHighlight(searchResponse, 0, "field1", 2, 3, equalTo("The quick brown dog jumps over the lazy <field1>fox</field1>."));

        assertHighlight(searchResponse, 0, "field2", 0, equalTo("The quick brown <field2>fox</field2> jumps over the lazy dog."));
        assertHighlight(searchResponse, 0, "field2", 1, 2, equalTo("The lazy red <field2>fox</field2> jumps over the quick dog."));

        client().prepareIndex("test", "type1", "2")
                .setSource("field1", new String[]{"The quick brown fox jumps over the lazy dog. Second sentence not finished", "The lazy red fox jumps over the quick dog.", "The quick brown dog jumps over the lazy fox."}).get();
        refresh();

        source = searchSource()
                .query(termQuery("field1", "fox"))
                .highlight(highlight()
                        .field(new HighlightBuilder.Field("field1").numOfFragments(0).preTags("<field1>").postTags("</field1>")));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();
        assertHitCount(searchResponse, 2l);

        for (SearchHit searchHit : searchResponse.getHits()) {
            if ("1".equals(searchHit.id())) {
                assertHighlight(searchHit, "field1", 0, 1, equalTo("The quick brown <field1>fox</field1> jumps over the lazy dog. The lazy red <field1>fox</field1> jumps over the quick dog. The quick brown dog jumps over the lazy <field1>fox</field1>."));
            } else if ("2".equals(searchHit.id())) {
                assertHighlight(searchHit, "field1", 0, equalTo("The quick brown <field1>fox</field1> jumps over the lazy dog. Second sentence not finished"));
                assertHighlight(searchHit, "field1", 1, equalTo("The lazy red <field1>fox</field1> jumps over the quick dog."));
                assertHighlight(searchHit, "field1", 2, 3, equalTo("The quick brown dog jumps over the lazy <field1>fox</field1>."));
            } else {
                fail("Only hits with id 1 and 2 are returned");
            }
        }
    }

    @Test
    public void testPostingsHighlighterRequireFieldMatch() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field1", "The quick brown fox jumps over the lazy dog. The lazy red fox jumps over the quick dog. The quick brown dog jumps over the lazy fox.",
                        "field2", "The quick brown fox jumps over the lazy dog. The lazy red fox jumps over the quick dog. The quick brown dog jumps over the lazy fox.").get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "fox"))
                .highlight(highlight()
                        .field(new HighlightBuilder.Field("field1").requireFieldMatch(true).preTags("<field1>").postTags("</field1>"))
                        .field(new HighlightBuilder.Field("field2").requireFieldMatch(true).preTags("<field2>").postTags("</field2>")));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        //field2 is not returned highlighted because of the require field match option set to true
        assertNotHighlighted(searchResponse, 0, "field2");
        assertHighlight(searchResponse, 0, "field1", 0, equalTo("The quick brown <field1>fox</field1> jumps over the lazy dog."));
        assertHighlight(searchResponse, 0, "field1", 1, equalTo("The lazy red <field1>fox</field1> jumps over the quick dog."));
        assertHighlight(searchResponse, 0, "field1", 2, 3, equalTo("The quick brown dog jumps over the lazy <field1>fox</field1>."));

        logger.info("--> highlighting and searching on field1 and field2 - require field match set to false");
        source = searchSource()
                .query(termQuery("field1", "fox"))
                .highlight(highlight()
                        .field(new HighlightBuilder.Field("field1").requireFieldMatch(false).preTags("<field1>").postTags("</field1>"))
                        .field(new HighlightBuilder.Field("field2").requireFieldMatch(false).preTags("<field2>").postTags("</field2>")));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field1", 0, equalTo("The quick brown <field1>fox</field1> jumps over the lazy dog."));
        assertHighlight(searchResponse, 0, "field1", 1, equalTo("The lazy red <field1>fox</field1> jumps over the quick dog."));
        assertHighlight(searchResponse, 0, "field1", 2, 3, equalTo("The quick brown dog jumps over the lazy <field1>fox</field1>."));

        //field2 is now returned highlighted thanks to require_field_match set to false
        assertHighlight(searchResponse, 0, "field2", 0, equalTo("The quick brown <field2>fox</field2> jumps over the lazy dog."));
        assertHighlight(searchResponse, 0, "field2", 1, equalTo("The lazy red <field2>fox</field2> jumps over the quick dog."));
        assertHighlight(searchResponse, 0, "field2", 2, 3, equalTo("The quick brown dog jumps over the lazy <field2>fox</field2>."));

        logger.info("--> highlighting and searching on field1 and field2 via multi_match query");
        source = searchSource()
                .query(multiMatchQuery("fox", "field1", "field2"))
                .highlight(highlight()
                        .field(new HighlightBuilder.Field("field1").requireFieldMatch(true).preTags("<field1>").postTags("</field1>"))
                        .field(new HighlightBuilder.Field("field2").requireFieldMatch(true).preTags("<field2>").postTags("</field2>")));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();
        assertHitCount(searchResponse, 1l);

        assertHighlight(searchResponse, 0, "field1", 0, equalTo("The quick brown <field1>fox</field1> jumps over the lazy dog."));
        assertHighlight(searchResponse, 0, "field1", 1, equalTo("The lazy red <field1>fox</field1> jumps over the quick dog."));
        assertHighlight(searchResponse, 0, "field1", 2, 3, equalTo("The quick brown dog jumps over the lazy <field1>fox</field1>."));
        //field2 is now returned highlighted thanks to the multi_match query on both fields
        assertHighlight(searchResponse, 0, "field2", 0, equalTo("The quick brown <field2>fox</field2> jumps over the lazy dog."));
        assertHighlight(searchResponse, 0, "field2", 1, equalTo("The lazy red <field2>fox</field2> jumps over the quick dog."));
        assertHighlight(searchResponse, 0, "field2", 2, 3, equalTo("The quick brown dog jumps over the lazy <field2>fox</field2>."));
    }

    @Test
    public void testPostingsHighlighterOrderByScore() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field1", new String[]{"This sentence contains one match, not that short. This sentence contains two sentence matches. This one contains no matches.",
                        "This is the second value's first sentence. This one contains no matches. This sentence contains three sentence occurrences (sentence).",
                        "One sentence match here and scored lower since the text is quite long, not that appealing. This one contains no matches."}).get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "sentence"))
                .highlight(highlight().field("field1").order("score"));

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

        //lets use now number_of_fragments = 0, so that we highlight per value without breaking them into snippets, but we sort the values by score
        source = searchSource()
                .query(termQuery("field1", "sentence"))
                .highlight(highlight().field("field1", -1, 0).order("score"));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();
        assertHighlight(searchResponse, 0, "field1", 0, equalTo("This is the second value's first <em>sentence</em>. This one contains no matches. This <em>sentence</em> contains three <em>sentence</em> occurrences (<em>sentence</em>)."));
        assertHighlight(searchResponse, 0, "field1", 1, equalTo("This <em>sentence</em> contains one match, not that short. This <em>sentence</em> contains two <em>sentence</em> matches. This one contains no matches."));
        assertHighlight(searchResponse, 0, "field1", 2, 3, equalTo("One <em>sentence</em> match here and scored lower since the text is quite long, not that appealing. This one contains no matches."));
    }

    @Test
    public void testPostingsHighlighterEscapeHtml() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", "title", "type=string," + randomStoreField() + "index_options=offsets"));
        ensureYellow();

        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[5];
        for (int i = 0; i < 5; i++) {
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a html escaping highlighting test for *&? elasticsearch");
        }
        indexRandom(true, indexRequestBuilders);

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchQuery("title", "test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title").get();

        for (int i = 0; i < indexRequestBuilders.length; i++) {
            assertHighlight(searchResponse, i, "title", 0, 1, equalTo("This is a html escaping highlighting <em>test</em> for *&amp;?"));
        }
    }

    @Test
    public void testPostingsHighlighterMultiMapperWithStore() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1")
                        //just to make sure that we hit the stored fields rather than the _source
                        .startObject("_source").field("enabled", false).endObject()
                        .startObject("properties")
                        .startObject("title").field("type", "multi_field").startObject("fields")
                        .startObject("title").field("type", "string").field("store", "yes").field("index_options", "offsets").endObject()
                        .startObject("key").field("type", "string").field("store", "yes").field("index_options", "offsets").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject()));
        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource("title", "this is a test . Second sentence.").get();
        refresh();
        // simple search on body with standard analyzer with a simple field query
        SearchResponse searchResponse = client().prepareSearch()
                //lets make sure we analyze the query and we highlight the resulting terms
                .setQuery(matchQuery("title", "This is a Test"))
                .addHighlightedField("title").get();

        assertHitCount(searchResponse, 1l);
        SearchHit hit = searchResponse.getHits().getAt(0);
        assertThat(hit.source(), nullValue());
        //stopwords are not highlighted since not indexed
        assertHighlight(hit, "title", 0, 1, equalTo("this is a <em>test</em> ."));

        // search on title.key and highlight on title
        searchResponse = client().prepareSearch()
                .setQuery(matchQuery("title.key", "this is a test"))
                .addHighlightedField("title.key").get();
        assertHitCount(searchResponse, 1l);

        //stopwords are now highlighted since we used only whitespace analyzer here
        assertHighlight(searchResponse, 0, "title.key", 0, 1, equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em> ."));
    }

    @Test
    public void testPostingsHighlighterMultiMapperFromSource() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "multi_field").startObject("fields")
                        .startObject("title").field("type", "string").field("store", "no").field("index_options", "offsets").endObject()
                        .startObject("key").field("type", "string").field("store", "no").field("index_options", "offsets").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject()));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("title", "this is a test").get();
        refresh();

        // simple search on body with standard analyzer with a simple field query
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchQuery("title", "this is a test"))
                .addHighlightedField("title")
                .get();

        assertHighlight(searchResponse, 0, "title", 0, 1, equalTo("this is a <em>test</em>"));

        // search on title.key and highlight on title.key
        searchResponse = client().prepareSearch()
                .setQuery(matchQuery("title.key", "this is a test"))
                .addHighlightedField("title.key").get();

        assertHighlight(searchResponse, 0, "title.key", 0, 1, equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em>"));
    }

    @Test
    public void testPostingsHighlighterShouldFailIfNoOffsets() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "string").field("store", "yes").field("index_options", "docs").endObject()
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
                .addHighlightedField("title")
                .get();
        assertNoFailures(search);

        search = client().prepareSearch()
                .setQuery(matchQuery("title", "this is a test"))
                .addHighlightedField("title")
                .setHighlighterType("postings-highlighter")
                .get();
        assertThat(search.getFailedShards(), equalTo(2));
        for (ShardSearchFailure shardSearchFailure : search.getShardFailures()) {
            assertThat(shardSearchFailure.reason(), containsString("the field [title] should be indexed with positions and offsets in the postings list to be used with postings highlighter"));
        }

        search = client().prepareSearch()
                .setQuery(matchQuery("title", "this is a test"))
                .addHighlightedField("title")
                .setHighlighterType("postings")
                .get();

        assertThat(search.getFailedShards(), equalTo(2));
        for (ShardSearchFailure shardSearchFailure : search.getShardFailures()) {
            assertThat(shardSearchFailure.reason(), containsString("the field [title] should be indexed with positions and offsets in the postings list to be used with postings highlighter"));
        }
    }

    @Test
    public void testPostingsHighlighterBoostingQuery() throws ElasticSearchException, IOException {
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();
        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog! Second sentence.")
                .get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(boostingQuery().positive(termQuery("field2", "brown")).negative(termQuery("field2", "foobar")).negativeBoost(0.5f))
                .highlight(highlight().field("field2").preTags("<x>").postTags("</x>"));
        SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The quick <x>brown</x> fox jumps over the lazy dog!"));
    }

    @Test
    public void testPostingsHighlighterCommonTermsQuery() throws ElasticSearchException, IOException {
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog! Second sentence.").get();
        refresh();
        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource().query(commonTerms("field2", "quick brown").cutoffFrequency(100))
                .highlight(highlight().field("field2").preTags("<x>").postTags("</x>"));
        SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();
        assertHitCount(searchResponse, 1l);

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <x>quick</x> <x>brown</x> fox jumps over the lazy dog!"));
    }

    public XContentBuilder type1PostingsffsetsMapping() throws IOException {
        return XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("_all").field("store", "yes").field("index_options", "offsets").endObject()
                .startObject("properties")
                .startObject("field1").field("type", "string").field("index_options", "offsets").endObject()
                .startObject("field2").field("type", "string").field("index_options", "offsets").endObject()
                .endObject()
                .endObject().endObject();
    }

    private static final String[] REWRITE_METHODS = new String[]{"constant_score_auto", "scoring_boolean", "constant_score_boolean",
            "constant_score_filter", "top_terms_boost_50", "top_terms_50"};

    @Test
    public void testPostingsHighlighterPrefixQuery() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog! Second sentence.").get();
        refresh();
        logger.info("--> highlighting and searching on field2");

        for (String rewriteMethod : REWRITE_METHODS) {
            SearchSourceBuilder source = searchSource().query(prefixQuery("field2", "qui").rewrite(rewriteMethod))
                    .highlight(highlight().field("field2"));
            SearchResponse searchResponse = client().search(searchRequest("test").source(source)
                    .searchType(randomBoolean() ? SearchType.DFS_QUERY_THEN_FETCH : SearchType.QUERY_THEN_FETCH)).get();
            assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <em>quick</em> brown fox jumps over the lazy dog!"));
        }
    }

    @Test
    public void testPostingsHighlighterFuzzyQuery() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog! Second sentence.").get();
        refresh();
        logger.info("--> highlighting and searching on field2");
        SearchSourceBuilder source = searchSource().query(fuzzyQuery("field2", "quck"))
                .highlight(highlight().field("field2"));
        SearchResponse searchResponse = client().search(searchRequest("test").source(source)
                .searchType(randomBoolean() ? SearchType.DFS_QUERY_THEN_FETCH : SearchType.QUERY_THEN_FETCH)).get();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <em>quick</em> brown fox jumps over the lazy dog!"));
    }

    @Test
    public void testPostingsHighlighterRegexpQuery() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog! Second sentence.").get();
        refresh();
        logger.info("--> highlighting and searching on field2");
        for (String rewriteMethod : REWRITE_METHODS) {
            SearchSourceBuilder source = searchSource().query(regexpQuery("field2", "qu[a-l]+k").rewrite(rewriteMethod))
                    .highlight(highlight().field("field2"));
            SearchResponse searchResponse = client().search(searchRequest("test").source(source)
                    .searchType(randomBoolean() ? SearchType.DFS_QUERY_THEN_FETCH : SearchType.QUERY_THEN_FETCH)).get();

            assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <em>quick</em> brown fox jumps over the lazy dog!"));
        }
    }

    @Test
    public void testPostingsHighlighterWildcardQuery() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog! Second sentence.").get();
        refresh();
        logger.info("--> highlighting and searching on field2");
        for (String rewriteMethod : REWRITE_METHODS) {
            SearchSourceBuilder source = searchSource().query(wildcardQuery("field2", "qui*").rewrite(rewriteMethod))
                    .highlight(highlight().field("field2"));
            SearchResponse searchResponse = client().search(searchRequest("test").source(source)
                    .searchType(randomBoolean() ? SearchType.DFS_QUERY_THEN_FETCH : SearchType.QUERY_THEN_FETCH)).get();

            assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <em>quick</em> brown fox jumps over the lazy dog!"));

            source = searchSource().query(wildcardQuery("field2", "qu*k").rewrite(rewriteMethod))
                    .highlight(highlight().field("field2"));
            searchResponse = client().search(searchRequest("test").source(source)
                    .searchType(randomBoolean() ? SearchType.DFS_QUERY_THEN_FETCH : SearchType.QUERY_THEN_FETCH)).get();
            assertHitCount(searchResponse, 1l);

            assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <em>quick</em> brown fox jumps over the lazy dog!"));
        }
    }

    @Test
    public void testPostingsHighlighterTermRangeQuery() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "aaab").get();
        refresh();
        logger.info("--> highlighting and searching on field2");
        SearchSourceBuilder source = searchSource().query(rangeQuery("field2").gte("aaaa").lt("zzzz"))
                .highlight(highlight().field("field2"));
        SearchResponse searchResponse = client().search(searchRequest("test").source(source)
                .searchType(randomBoolean() ? SearchType.DFS_QUERY_THEN_FETCH : SearchType.QUERY_THEN_FETCH)).get();

        assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("<em>aaab</em>"));
    }

    @Test
    public void testPostingsHighlighterQueryString() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog! Second sentence.").get();
        refresh();
        logger.info("--> highlighting and searching on field2");
        for (String rewriteMethod : REWRITE_METHODS) {
            SearchSourceBuilder source = searchSource().query(queryString("qui*").defaultField("field2").rewrite(rewriteMethod))
                    .highlight(highlight().field("field2"));
            SearchResponse searchResponse = client().search(searchRequest("test").source(source)
                    .searchType(randomBoolean() ? SearchType.DFS_QUERY_THEN_FETCH : SearchType.QUERY_THEN_FETCH)).get();

            assertHighlight(searchResponse, 0, "field2", 0, 1, equalTo("The <em>quick</em> brown fox jumps over the lazy dog!"));
        }
    }

    @Test
    @Slow
    public void testPostingsHighlighterManyDocs() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()));
        ensureGreen();

        int COUNT = between(20, 100);
        Map<String, String> prefixes = new HashMap<String, String>(COUNT);

        IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[COUNT];
        for (int i = 0; i < COUNT; i++) {
            //generating text with word to highlight in a different position
            //(https://github.com/elasticsearch/elasticsearch/issues/4103)
            String prefix = randomAsciiOfLengthBetween(5, 30);
            prefixes.put(String.valueOf(i), prefix);
            indexRequestBuilders[i] = client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field1", "Sentence " + prefix
                    + " test. Sentence two.");
        }
        logger.info("--> indexing docs");
        indexRandom(true, indexRequestBuilders);

        logger.info("--> searching explicitly on field1 and highlighting on it");
        SearchResponse searchResponse = client().prepareSearch()
                .setSize(COUNT)
                .setQuery(termQuery("field1", "test"))
                .addHighlightedField("field1")
                .get();
        assertHitCount(searchResponse, (long)COUNT);
        assertThat(searchResponse.getHits().hits().length, equalTo(COUNT));
        for (SearchHit hit : searchResponse.getHits()) {
            String prefix = prefixes.get(hit.id());
            assertHighlight(hit, "field1", 0, 1, equalTo("Sentence " + prefix + " <em>test</em>."));
        }

        logger.info("--> searching explicitly on field1 and highlighting on it, with DFS");
        searchResponse = client().prepareSearch()
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSize(COUNT)
                .setQuery(termQuery("field1", "test"))
                .addHighlightedField("field1")
                .get();
        assertHitCount(searchResponse, (long)COUNT);
        assertThat(searchResponse.getHits().hits().length, equalTo(COUNT));
        for (SearchHit hit : searchResponse.getHits()) {
            String prefix = prefixes.get(hit.id());
            assertHighlight(hit, "field1", 0, 1, equalTo("Sentence " + prefix + " <em>test</em>."));
        }
    }

    @Test //https://github.com/elasticsearch/elasticsearch/issues/4116
    public void testPostingsHighlighterCustomIndexName() {
        assertAcked(client().admin().indices().prepareCreate("test")
            .addMapping("type1", "field1", "type=string,index_options=offsets,index_name=my_field"));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("field1", "First sentence. Second sentence.").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchQuery("field1", "first")).addHighlightedField("field1").get();
        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("<em>First</em> sentence."));

        searchResponse = client().prepareSearch("test").setQuery(matchQuery("my_field", "first")).addHighlightedField("field1").get();
        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("<em>First</em> sentence."));

        searchResponse = client().prepareSearch("test").setQuery(matchQuery("my_field", "first")).addHighlightedField("my_field").get();
        assertHighlight(searchResponse, 0, "my_field", 0, 1, equalTo("<em>First</em> sentence."));

        searchResponse = client().prepareSearch("test").setQuery(matchQuery("my_field", "first"))
                .addHighlightedField("field1").setHighlighterRequireFieldMatch(true).get();
        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("<em>First</em> sentence."));
    }

    @Test
    public void testFastVectorHighlighterCustomIndexName() {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=string,term_vector=with_positions_offsets,index_name=my_field"));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("field1", "First sentence. Second sentence.").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchQuery("field1", "first")).addHighlightedField("field1").get();
        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("<em>First</em> sentence. Second sentence."));

        searchResponse = client().prepareSearch("test").setQuery(matchQuery("my_field", "first")).addHighlightedField("field1").get();
        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("<em>First</em> sentence. Second sentence."));

        searchResponse = client().prepareSearch("test").setQuery(matchQuery("my_field", "first")).addHighlightedField("my_field").get();
        assertHighlight(searchResponse, 0, "my_field", 0, 1, equalTo("<em>First</em> sentence. Second sentence."));

        searchResponse = client().prepareSearch("test").setQuery(matchQuery("my_field", "first"))
                .addHighlightedField("field1").setHighlighterRequireFieldMatch(true).get();
        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("<em>First</em> sentence. Second sentence."));
    }

    @Test
    public void testPlainHighlighterCustomIndexName() {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=string,index_name=my_field"));
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("field1", "First sentence. Second sentence.").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchQuery("field1", "first")).addHighlightedField("field1").get();
        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("<em>First</em> sentence. Second sentence."));

        searchResponse = client().prepareSearch("test").setQuery(matchQuery("my_field", "first")).addHighlightedField("field1").get();
        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("<em>First</em> sentence. Second sentence."));

        searchResponse = client().prepareSearch("test").setQuery(matchQuery("my_field", "first")).addHighlightedField("my_field").get();
        assertHighlight(searchResponse, 0, "my_field", 0, 1, equalTo("<em>First</em> sentence. Second sentence."));

        searchResponse = client().prepareSearch("test").setQuery(matchQuery("my_field", "first"))
                .addHighlightedField("field1").setHighlighterRequireFieldMatch(true).get();
        assertHighlight(searchResponse, 0, "field1", 0, 1, equalTo("<em>First</em> sentence. Second sentence."));
    }
}
