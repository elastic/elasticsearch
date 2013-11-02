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
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.MatchQueryBuilder.Type;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
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
public class HighlighterSearchTests extends AbstractIntegrationTest {
    
    @Test
    // see #3486
    public void testHighTermFrequencyDoc() throws ElasticSearchException, IOException {
        wipeIndex("test");
        client().admin().indices().prepareCreate("test")
        .addMapping("test", jsonBuilder()
                .startObject()
                    .startObject("test")
                        .startObject("properties")
                            .startObject("name")
                                .field("type", "string")
                                .field("term_vector", "with_positions_offsets")
                                .field("store", randomBoolean() ? "yes" : "no")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject())
        .setSettings(ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", between(1, 5)))
        .execute().actionGet();
        ensureYellow();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 6000; i++) {
            builder.append("abc").append(" ");
        }
        client().prepareIndex("test", "test", "1")
            .setSource(XContentFactory.jsonBuilder()
                    .startObject()
                        .field("name", builder.toString())
                    .endObject())
            .execute().actionGet();
        refresh();
        SearchResponse search = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("name", "abc"))).addHighlightedField("name").execute().actionGet();
        assertHighlight(search, 0, "name", 0, startsWith("<em>abc</em> <em>abc</em> <em>abc</em> <em>abc</em>"));
    }


    @Test
    public void testNgramHighlightingWithBrokenPositions() throws ElasticSearchException, IOException {
        prepareCreate("test")
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
                .putArray("analysis.analyzer.search_autocomplete.filter", "lowercase", "wordDelimiter"))
        .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();
        client().prepareIndex("test", "test", "1")
            .setSource(XContentFactory.jsonBuilder()
                    .startObject()
                        .field( "name", "ARCOTEL Hotels Deutschland")
                    .endObject())
            .setRefresh(true).execute().actionGet();
        SearchResponse search = client().prepareSearch("test").setTypes("test").setQuery(matchQuery("name.autocomplete", "deut tel").operator(Operator.OR)).addHighlightedField("name.autocomplete").execute().actionGet();
        assertHighlight(search, 0, "name.autocomplete", 0, equalTo("ARCO<em>TEL</em> Ho<em>tel</em>s <em>Deut</em>schland"));
    }
    
    @Test 
    public void testMultiPhraseCutoff() throws ElasticSearchException, IOException {
        /*
         * MultiPhraseQuery can literally kill an entire node if there are too many terms in the
         * query. We cut off and extract terms if there are more than 16 terms in the query
         */
        XContentBuilder builder = jsonBuilder().
                startObject().
                    field("test").
                    startObject().
                        field("properties").
                        startObject().
                            field("body").
                            startObject().
                                field("type", "string").
                                field("index_analyzer", "custom_analyzer").
                                field("search_analyzer", "custom_analyzer").
                                field("term_vector", "with_positions_offsets").
                            endObject().
                        endObject().
                    endObject().
                endObject();

        assertAcked(prepareCreate("test").addMapping("test", builder).setSettings(
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
            .setSource(XContentFactory.jsonBuilder()
                    .startObject()
                        .field("body", "Test: http://www.facebook.com http://elasticsearch.org http://xing.com http://cnn.com http://quora.com http://twitter.com this is a test for highlighting feature Test: http://www.facebook.com http://elasticsearch.org http://xing.com http://cnn.com http://quora.com http://twitter.com this is a test for highlighting feature")
                    .endObject())
            .execute().actionGet();
        refresh();
        SearchResponse search = client().prepareSearch().setQuery(matchQuery("body", "Test: http://www.facebook.com ").type(Type.PHRASE)).addHighlightedField("body").execute().actionGet();
        assertHighlight(search, 0, "body", 0, startsWith("<em>Test: http://www.facebook.com</em>"));
        search = client().prepareSearch().setQuery(matchQuery("body", "Test: http://www.facebook.com http://elasticsearch.org http://xing.com http://cnn.com http://quora.com http://twitter.com this is a test for highlighting feature Test: http://www.facebook.com http://elasticsearch.org http://xing.com http://cnn.com http://quora.com http://twitter.com this is a test for highlighting feature").type(Type.PHRASE)).addHighlightedField("body").execute().actionGet();
        assertHighlight(search, 0, "body", 0, equalTo("<em>Test</em>: <em>http</em>://<em>www</em>.<em>facebook</em>.<em>com</em> <em>http</em>://<em>elasticsearch</em>.<em>org</em> <em>http</em>://<em>xing</em>.<em>com</em> <em>http</em>://<em>cnn</em>.<em>com</em> <em>http</em>://<em>quora</em>.com"));
    }
    
    @Test
    public void testNgramHighlightingPreLucene42() throws ElasticSearchException, IOException {
        boolean[] doStore = {true, false};
        for (boolean store : doStore) {
            wipeIndex("test");
            client().admin().indices().prepareCreate("test")
            .addMapping("test", jsonBuilder()
                    .startObject()
                        .startObject("test")
                            .startObject("properties")
                                .startObject("name")
                                    .field("type", "string")
                                    .field("index_analyzer", "name_index_analyzer")
                                    .field("search_analyzer", "name_search_analyzer")
                                    .field("term_vector", "with_positions_offsets")
                                    .field("store", store ? "yes" : "no")
                                .endObject()
                                .startObject("name2")
                                    .field("type", "string")
                                    .field("index_analyzer", "name2_index_analyzer")
                                    .field("search_analyzer", "name_search_analyzer")
                                    .field("term_vector", "with_positions_offsets")
                                    .field("store", store ? "yes" : "no")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject())
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
                    .put("analysis.analyzer.name_search_analyzer.filter", "lowercase"))
            .execute().actionGet();
            ensureYellow();
            client().prepareIndex("test", "test", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                            .field("name", "logicacmg ehemals avinci - the know how company")
                            .field("name2", "logicacmg ehemals avinci - the know how company")
                        .endObject())
                .execute().actionGet();
            
            client().prepareIndex("test", "test", "2")
            .setSource(XContentFactory.jsonBuilder()
                    .startObject()
                        .field("name", "avinci, unilog avinci, logicacmg, logica" )
                        .field("name2", "avinci, unilog avinci, logicacmg, logica")
                    .endObject())
            .execute().actionGet();
            refresh();
           
            SearchResponse search = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("name", "logica m"))).addHighlightedField("name").execute().actionGet();
            assertHighlight(search, 0, "name", 0, equalTo("<em>logica</em>c<em>m</em>g ehe<em>m</em>als avinci - the know how co<em>m</em>pany"));
            assertHighlight(search, 1, "name", 0, equalTo("avinci, unilog avinci, <em>logica</em>c<em>m</em>g, <em>logica</em>"));
            
            search = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("name", "logica ma"))).addHighlightedField("name").execute()
                    .actionGet();
            assertHighlight(search, 0, "name", 0, equalTo("<em>logica</em>cmg ehe<em>ma</em>ls avinci - the know how company"));
            assertHighlight(search, 1, "name", 0, equalTo("avinci, unilog avinci, <em>logica</em>cmg, <em>logica</em>"));
    
            search = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("name", "logica"))).addHighlightedField("name").execute().actionGet();
            assertHighlight(search, 0, "name", 0, equalTo("<em>logica</em>cmg ehemals avinci - the know how company"));
            
            search = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("name2", "logica m"))).addHighlightedField("name2").execute().actionGet();
            assertHighlight(search, 0, "name2", 0, equalTo("<em>logica</em>c<em>m</em>g ehe<em>m</em>als avinci - the know how co<em>m</em>pany"));
            assertHighlight(search, 1, "name2", 0, equalTo("avinci, unilog avinci, <em>logica</em>c<em>m</em>g, <em>logica</em>"));
    
            search = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("name2", "logica ma"))).addHighlightedField("name2").execute()
                    .actionGet();
            assertHighlight(search, 0, "name2", 0, equalTo("<em>logica</em>cmg ehe<em>ma</em>ls avinci - the know how company"));
            assertHighlight(search, 1, "name2", 0, equalTo("avinci, unilog avinci, <em>logica</em>cmg, <em>logica</em>"));
    
    
            search = client().prepareSearch().setQuery(constantScoreQuery(matchQuery("name2", "logica"))).addHighlightedField("name2").execute().actionGet();
            assertHighlight(search, 0, "name2", 0, equalTo("<em>logica</em>cmg ehemals avinci - the know how company"));
            assertHighlight(search, 1, "name2", 0, equalTo("avinci, unilog avinci, <em>logica</em>cmg, <em>logica</em>"));
        }

    }
    
    @Test
    public void testNgramHighlighting() throws ElasticSearchException, IOException {
        client().admin().indices().prepareCreate("test")
        .addMapping("test", jsonBuilder()
                .startObject()
                    .startObject("test")
                        .startObject("properties")
                            .startObject("name")
                                .field("type", "string")
                                .field("index_analyzer", "name_index_analyzer")
                                .field("search_analyzer", "name_search_analyzer")
                                .field("term_vector", "with_positions_offsets")
                            .endObject()
                            .startObject("name2")
                                .field("type", "string")
                                .field("index_analyzer", "name2_index_analyzer")
                                .field("search_analyzer", "name_search_analyzer")
                                .field("term_vector", "with_positions_offsets")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject())
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
                .put("analysis.analyzer.name_search_analyzer.tokenizer", "whitespace"))
        .execute().actionGet();
        client().prepareIndex("test", "test", "1")
            .setSource(XContentFactory.jsonBuilder()
                    .startObject()
                        .field("name", "logicacmg ehemals avinci - the know how company")
                        .field("name2", "logicacmg ehemals avinci - the know how company")
                    .endObject())
            .execute().actionGet();
        refresh();
        ensureGreen();
        SearchResponse search = client().prepareSearch().setQuery(matchQuery("name", "logica m")).addHighlightedField("name").execute().actionGet();
        assertHighlight(search, 0, "name", 0, equalTo("<em>logica</em>c<em>m</em>g ehe<em>m</em>als avinci - the know how co<em>m</em>pany"));
        
        search = client().prepareSearch().setQuery(matchQuery("name", "logica ma")).addHighlightedField("name").execute()
                .actionGet();
        assertHighlight(search, 0, "name", 0, equalTo("<em>logica</em>cmg ehe<em>ma</em>ls avinci - the know how company"));

        search = client().prepareSearch().setQuery(matchQuery("name", "logica")).addHighlightedField("name").execute().actionGet();
        assertHighlight(search, 0, "name", 0, equalTo("<em>logica</em>cmg ehemals avinci - the know how company"));
        
      
        
        search = client().prepareSearch().setQuery(matchQuery("name2", "logica m")).addHighlightedField("name2").execute().actionGet();
        assertHighlight(search, 0, "name2", 0, equalTo("<em>logicacmg</em> <em>ehemals</em> avinci - the know how <em>company</em>"));
        
        search = client().prepareSearch().setQuery(matchQuery("name2", "logica ma")).addHighlightedField("name2").execute()
                .actionGet();
        assertHighlight(search, 0, "name2", 0, equalTo("<em>logicacmg</em> <em>ehemals</em> avinci - the know how company"));

        search = client().prepareSearch().setQuery(matchQuery("name2", "logica")).addHighlightedField("name2").execute().actionGet();
        assertHighlight(search, 0, "name2", 0, equalTo("<em>logicacmg</em> ehemals avinci - the know how company"));

        
    }
    
    @Test
    public void testEnsureNoNegativeOffsets() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        // we don't store title, now lets see if it works...
                        .startObject("no_long_term").field("type", "string").field("store", "no").field("term_vector", "with_positions_offsets").endObject()
                        .startObject("long_term").field("type", "string").field("store", "no").field("term_vector", "with_positions_offsets").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        client().prepareIndex("test", "type1", "1")
                .setSource(XContentFactory.jsonBuilder().startObject()
                        .startArray("no_long_term")
                        .value("This is a test where foo is highlighed and should be highlighted")
                        .endArray()
                        .startArray("long_term")
                        .value("This is a test thisisaverylongwordandmakessurethisfails where foo is highlighed and should be highlighted")
                        .endArray()
                        .endObject())
                .setRefresh(true).execute().actionGet();


        SearchResponse search = client().prepareSearch()
                .setQuery(matchQuery("long_term", "thisisaverylongwordandmakessurethisfails foo highlighed"))
                .addHighlightedField("long_term", 18, 1)
                .execute().actionGet();

        assertThat(search.getHits().totalHits(), equalTo(1l));
        assertThat(search.getHits().hits().length, equalTo(1));

        assertThat(search.getHits().hits()[0].highlightFields().get("long_term").fragments().length, equalTo(1));
        assertThat(search.getHits().hits()[0].highlightFields().get("long_term").fragments()[0].string(), equalTo("<em>thisisaverylongwordandmakessurethisfails</em>"));
        
        
        search = client().prepareSearch()
                .setQuery(matchQuery("no_long_term", "test foo highlighed").type(Type.PHRASE).slop(3))
                .addHighlightedField("no_long_term", 18, 1).setHighlighterPostTags("</b>").setHighlighterPreTags("<b>")
                .execute().actionGet();
        assertThat(search.getHits().totalHits(), equalTo(1l));
        assertThat(search.getHits().hits().length, equalTo(1));
        assertThat(search.getHits().hits()[0].highlightFields().size(), equalTo(0));
       

        search = client().prepareSearch()
                .setQuery(matchQuery("no_long_term", "test foo highlighed").type(Type.PHRASE).slop(3))
                .addHighlightedField("no_long_term", 30, 1).setHighlighterPostTags("</b>").setHighlighterPreTags("<b>")
                .execute().actionGet();
        
        assertThat(search.getHits().totalHits(), equalTo(1l));
        assertThat(search.getHits().hits().length, equalTo(1));

        assertThat(search.getHits().hits()[0].highlightFields().get("no_long_term").fragments().length, equalTo(1));
        assertThat(search.getHits().hits()[0].highlightFields().get("no_long_term").fragments()[0].string(), equalTo("a <b>test</b> where <b>foo</b> is <b>highlighed</b> and"));
    }
    
    @Test
    public void testSourceLookupHighlightingUsingPlainHighlighter() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        // we don't store title and don't use term vector, now lets see if it works...
                        .startObject("title").field("type", "string").field("store", "no").field("term_vector", "no").endObject()
                        .startObject("attachments").startObject("properties").startObject("body").field("type", "string").field("store", "no").field("term_vector", "no").endObject().endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("title", "This is a test on the highlighting bug present in elasticsearch")
                            .startArray("attachments").startObject().field("body", "attachment 1").endObject().startObject().field("body", "attachment 2").endObject().endArray()
                            .endObject())
                    .setRefresh(true).execute().actionGet();
        }

        SearchResponse search = client().prepareSearch()
                .setQuery(fieldQuery("title", "bug"))
                .addHighlightedField("title", -1, 0)
                .execute().actionGet();

        assertNoFailures(search);

        assertThat(search.getHits().totalHits(), equalTo(5l));
        assertThat(search.getHits().hits().length, equalTo(5));

        for (SearchHit hit : search.getHits()) {
            assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
        }

        search = client().prepareSearch()
                .setQuery(fieldQuery("attachments.body", "attachment"))
                .addHighlightedField("attachments.body", -1, 0)
                .execute().actionGet();

        assertNoFailures(search);

        assertThat(search.getHits().totalHits(), equalTo(5l));
        assertThat(search.getHits().hits().length, equalTo(5));

        for (SearchHit hit : search.getHits()) {
            assertThat(hit.highlightFields().get("attachments.body").fragments()[0].string(), equalTo("<em>attachment</em> 1"));
            assertThat(hit.highlightFields().get("attachments.body").fragments()[1].string(), equalTo("<em>attachment</em> 2"));
        }
    }

    @Test
    public void testSourceLookupHighlightingUsingFastVectorHighlighter() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        // we don't store title, now lets see if it works...
                        .startObject("title").field("type", "string").field("store", "no").field("term_vector", "with_positions_offsets").endObject()
                        .startObject("attachments").startObject("properties").startObject("body").field("type", "string").field("store", "no").field("term_vector", "with_positions_offsets").endObject().endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("title", "This is a test on the highlighting bug present in elasticsearch")
                            .startArray("attachments").startObject().field("body", "attachment 1").endObject().startObject().field("body", "attachment 2").endObject().endArray()
                            .endObject())
                    .setRefresh(true).execute().actionGet();
        }

        SearchResponse search = client().prepareSearch()
                .setQuery(fieldQuery("title", "bug"))
                .addHighlightedField("title", -1, 0)
                .execute().actionGet();

        assertNoFailures(search);

        assertThat(search.getHits().totalHits(), equalTo(5l));
        assertThat(search.getHits().hits().length, equalTo(5));

        for (SearchHit hit : search.getHits()) {
            assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
        }

        search = client().prepareSearch()
                .setQuery(fieldQuery("attachments.body", "attachment"))
                .addHighlightedField("attachments.body", -1, 2)
                .execute().actionGet();

        assertNoFailures(search);

        assertThat(search.getHits().totalHits(), equalTo(5l));
        assertThat(search.getHits().hits().length, equalTo(5));

        for (SearchHit hit : search.getHits()) {
            assertThat(hit.highlightFields().get("attachments.body").fragments()[0].string(), equalTo("<em>attachment</em> 1"));
            assertThat(hit.highlightFields().get("attachments.body").fragments()[1].string(), equalTo("<em>attachment</em> 2"));
        }
    }

    @Test
    public void testSourceLookupHighlightingUsingPostingsHighlighter() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        // we don't store title, now lets see if it works...
                        .startObject("title").field("type", "string").field("store", "no").field("index_options", "offsets").endObject()
                        .startObject("attachments").startObject("properties").startObject("body").field("type", "string").field("store", "no").field("index_options", "offsets").endObject().endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .array("title", "This is a test on the highlighting bug present in elasticsearch. Hopefully it works.",
                                    "This is the second bug to perform highlighting on.")
                            .startArray("attachments").startObject().field("body", "attachment for this test").endObject().startObject().field("body", "attachment 2").endObject().endArray()
                            .endObject())
                    .setRefresh(true).execute().actionGet();
        }

        SearchResponse search = client().prepareSearch()
                .setQuery(matchQuery("title", "bug"))
                //asking for the whole field to be highlighted
                .addHighlightedField("title", -1, 0)
                .execute().actionGet();

        assertNoFailures(search);

        assertThat(search.getHits().totalHits(), equalTo(5l));
        assertThat(search.getHits().hits().length, equalTo(5));

        for (SearchHit hit : search.getHits()) {
            Text[] fragments = hit.highlightFields().get("title").fragments();
            assertThat(fragments.length, equalTo(2));
            assertThat(fragments[0].string(), equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch. Hopefully it works."));
            assertThat(fragments[1].string(), equalTo("This is the second <em>bug</em> to perform highlighting on."));
        }

        search = client().prepareSearch()
                .setQuery(matchQuery("title", "bug"))
                //sentences will be generated out of each value
                .addHighlightedField("title")
                .execute().actionGet();

        assertNoFailures(search);

        assertThat(search.getHits().totalHits(), equalTo(5l));
        assertThat(search.getHits().hits().length, equalTo(5));

        for (SearchHit hit : search.getHits()) {
            Text[] fragments = hit.highlightFields().get("title").fragments();
            assertThat(fragments.length, equalTo(2));
            assertThat(fragments[0].string(), equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch."));
            assertThat(fragments[1].string(), equalTo("This is the second <em>bug</em> to perform highlighting on."));
        }

        search = client().prepareSearch()
                .setQuery(matchQuery("attachments.body", "attachment"))
                .addHighlightedField("attachments.body", -1, 2)
                .execute().actionGet();

        assertNoFailures(search);

        assertThat(search.getHits().totalHits(), equalTo(5l));
        assertThat(search.getHits().hits().length, equalTo(5));

        for (SearchHit hit : search.getHits()) {
            //shorter fragments are scored higher
            assertThat(hit.highlightFields().get("attachments.body").fragments()[0].string(), equalTo("<em>attachment</em> for this test"));
            assertThat(hit.highlightFields().get("attachments.body").fragments()[1].string(), equalTo("<em>attachment</em> 2"));
        }
    }

    @Test
    public void testHighlightIssue1994() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        // we don't store title, now lets see if it works...
                        .startObject("title").field("type", "string").field("store", "no").endObject()
                        .startObject("titleTV").field("type", "string").field("store", "no").field("term_vector", "with_positions_offsets").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        client().prepareIndex("test", "type1", "1")
                .setSource(XContentFactory.jsonBuilder().startObject()
                        .startArray("title")
                        .value("This is a test on the highlighting bug present in elasticsearch")
                        .value("The bug is bugging us")
                        .endArray()
                        .startArray("titleTV")
                        .value("This is a test on the highlighting bug present in elasticsearch")
                        .value("The bug is bugging us")
                        .endArray()
                        .endObject())
                .setRefresh(true).execute().actionGet();


        client().prepareIndex("test", "type1", "2")
                .setSource(XContentFactory.jsonBuilder().startObject()
                        .startArray("titleTV")
                        .value("some text to highlight")
                        .value("highlight other text")
                        .endArray()
                        .endObject())
                .setRefresh(true).execute().actionGet();

        SearchResponse search = client().prepareSearch()
                .setQuery(fieldQuery("title", "bug"))
                .addHighlightedField("title", -1, 2)
                .addHighlightedField("titleTV", -1, 2)
                .execute().actionGet();

        assertThat(search.getHits().totalHits(), equalTo(1l));
        assertThat(search.getHits().hits().length, equalTo(1));

        assertThat(search.getHits().hits()[0].highlightFields().get("title").fragments().length, equalTo(2));
        assertThat(search.getHits().hits()[0].highlightFields().get("title").fragments()[0].string(), equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
        assertThat(search.getHits().hits()[0].highlightFields().get("title").fragments()[1].string(), equalTo("The <em>bug</em> is bugging us"));
        assertThat(search.getHits().hits()[0].highlightFields().get("titleTV").fragments().length, equalTo(2));
        assertThat(search.getHits().hits()[0].highlightFields().get("titleTV").fragments()[0].string(), equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
//        assertThat(search.hits().hits()[0].highlightFields().get("titleTV").fragments()[0].string(), equalTo("highlighting <em>bug</em> present in elasticsearch")); // FastVectorHighlighter starts highlighting from startOffset - margin
        assertThat(search.getHits().hits()[0].highlightFields().get("titleTV").fragments()[1].string(), equalTo("The <em>bug</em> is bugging us"));

        search = client().prepareSearch()
                .setQuery(fieldQuery("titleTV", "highlight"))
                .addHighlightedField("titleTV", -1, 2)
                .execute().actionGet();

        assertThat(search.getHits().totalHits(), equalTo(1l));
        assertThat(search.getHits().hits().length, equalTo(1));
        assertThat(search.getHits().hits()[0].highlightFields().get("titleTV").fragments().length, equalTo(2));
        assertThat(search.getHits().hits()[0].highlightFields().get("titleTV").fragments()[0].string(), equalTo("some text to <em>highlight</em>"));
        assertThat(search.getHits().hits()[0].highlightFields().get("titleTV").fragments()[1].string(), equalTo("<em>highlight</em> other text"));
    }

    @Test
    public void testGlobalHighlightingSettingsOverriddenAtFieldLevel() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "this is another test")
                .setRefresh(true).execute().actionGet();

        logger.info("--> highlighting and searching on field1 and field2 produces different tags");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "test"))
                .from(0).size(60).explain(true)
                .highlight(highlight().order("score").preTags("<global>").postTags("</global>")
                        .field(new HighlightBuilder.Field("field1"))
                        .field(new HighlightBuilder.Field("field2").preTags("<field2>").postTags("</field2>")));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field1").fragments()[0].string(), equalTo("this is a <global>test</global>"));
        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("this is another <field2>test</field2>"));
    }

    @Test
    public void testHighlightingOnWildcardFields() throws Exception {
        createIndex("test");
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "this is another test")
                .setRefresh(true).execute().actionGet();

        logger.info("--> highlighting and searching on field*");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "test"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field*").order("score").preTags("<xxx>").postTags("</xxx>"));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field1").fragments()[0].string(), equalTo("this is a <xxx>test</xxx>"));
        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("this is another <xxx>test</xxx>"));
    }

    @Test
    public void testPlainHighlighter() throws Exception {
        createIndex("test");
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog")
                .setRefresh(true).execute().actionGet();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "test"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field1").order("score").preTags("<xxx>").postTags("</xxx>"));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field1").fragments()[0].string(), equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field1");
        source = searchSource()
                .query(termQuery("_all", "test"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field1").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field1").fragments()[0].string(), equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(termQuery("_all", "quick"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(prefixQuery("_all", "qui"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));

        logger.info("--> searching on _all with constant score, highlighting on field2");
        source = searchSource()
                .query(constantScoreQuery(prefixQuery("_all", "qui")))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));

        logger.info("--> searching on _all with constant score, highlighting on field2");
        source = searchSource()
                .query(boolQuery().should(constantScoreQuery(prefixQuery("_all", "qui"))))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));
    }

    @Test
    public void testFastVectorHighlighter() throws Exception {
        client().admin().indices().prepareCreate("test").addMapping("type1", type1TermVectorMapping()).execute().actionGet();
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog")
                .setRefresh(true).execute().actionGet();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "test"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field1", 100, 0).order("score").preTags("<xxx>").postTags("</xxx>"));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field1").fragments()[0].string(), equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field1");
        source = searchSource()
                .query(termQuery("_all", "test"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field1", 100, 0).order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field1").fragments()[0].string(), equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(termQuery("_all", "quick"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2", 100, 0).order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(prefixQuery("_all", "qui"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2", 100, 0).order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));
    }

    @Test
    @Slow
    public void testFastVectorHighlighterManyDocs() throws Exception {
        client().admin().indices().prepareCreate("test").addMapping("type1", type1TermVectorMapping()).execute().actionGet();
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        int COUNT = between(20, 100);
        logger.info("--> indexing docs");
        for (int i = 0; i < COUNT; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field1", "test " + i).execute().actionGet();
            if (i % 5 == 0) {
                // flush so we get updated readers and segmented readers
                client().admin().indices().prepareFlush().execute().actionGet();
            }
        }

        client().admin().indices().prepareRefresh().execute().actionGet();

        logger.info("--> searching explicitly on field1 and highlighting on it");
        SearchResponse searchResponse = client().prepareSearch()
                .setSize(COUNT)
                .setQuery(termQuery("field1", "test"))
                .addHighlightedField("field1", 100, 0)
                .execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo((long) COUNT));
        assertThat(searchResponse.getHits().hits().length, equalTo(COUNT));
        for (SearchHit hit : searchResponse.getHits()) {
            // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
            assertThat(hit.highlightFields().get("field1").fragments()[0].string(), equalTo("<em>test</em> " + hit.id()));
        }

        logger.info("--> searching explicitly on field1 and highlighting on it, with DFS");
        searchResponse = client().prepareSearch()
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setSize(COUNT)
                .setQuery(termQuery("field1", "test"))
                .addHighlightedField("field1", 100, 0)
                .execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo((long) COUNT));
        assertThat(searchResponse.getHits().hits().length, equalTo(COUNT));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.highlightFields().get("field1").fragments()[0].string(), equalTo("<em>test</em> " + hit.id()));
        }

        logger.info("--> searching explicitly _all and highlighting on _all");
        searchResponse = client().prepareSearch()
                .setSize(COUNT)
                .setQuery(termQuery("_all", "test"))
                .addHighlightedField("_all", 100, 0)
                .execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo((long) COUNT));
        assertThat(searchResponse.getHits().hits().length, equalTo(COUNT));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.highlightFields().get("_all").fragments()[0].string(), equalTo("<em>test</em> " + hit.id() + " "));
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
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "string").field("store", "yes").field("term_vector", "with_positions_offsets").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a test on the highlighting bug present in elasticsearch").setRefresh(true).execute().actionGet();
        }

        SearchResponse search = client().prepareSearch()
                .setQuery(fieldQuery("title", "bug"))
                .addHighlightedField("title", -1, 0)
                .execute().actionGet();

        assertThat(search.getHits().totalHits(), equalTo(5l));
        assertThat(search.getHits().hits().length, equalTo(5));
        assertThat(search.getFailedShards(), equalTo(0));

        for (SearchHit hit : search.getHits()) {
            // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
            assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("This is a test on the highlighting <em>bug</em> present in elasticsearch"));
        }
    }

    @Test
    public void testFastVectorHighlighterOffsetParameter() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "string").field("store", "yes").field("term_vector", "with_positions_offsets").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a test on the highlighting bug present in elasticsearch").setRefresh(true).execute().actionGet();
        }

        SearchResponse search = client().prepareSearch()
                .setQuery(fieldQuery("title", "bug"))
                .addHighlightedField("title", 30, 1, 10)
                .execute().actionGet();

        assertNoFailures(search);

        assertThat(search.getHits().totalHits(), equalTo(5l));
        assertThat(search.getHits().hits().length, equalTo(5));

        for (SearchHit hit : search.getHits()) {
            // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
            assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("highlighting <em>bug</em> present in elasticsearch"));
        }
    }

    @Test
    public void testEscapeHtml() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "string").field("store", "yes")
                        .endObject().endObject().endObject())
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a html escaping highlighting test for *&? elasticsearch").setRefresh(true).execute().actionGet();
        }

        SearchResponse search = client().prepareSearch()
                .setQuery(fieldQuery("title", "test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title", 50, 1, 10)
                .execute().actionGet();

        assertNoFailures(search);


        assertThat(search.getHits().totalHits(), equalTo(5l));
        assertThat(search.getHits().hits().length, equalTo(5));

        for (SearchHit hit : search.getHits()) {
            // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
            assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("This is a html escaping highlighting <em>test</em> for *&amp;? elasticsearch"));
        }
    }

    @Test
    public void testEscapeHtml_vector() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "string").field("store", "yes").field("term_vector", "with_positions_offsets").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a html escaping highlighting test for *&? elasticsearch").setRefresh(true).execute().actionGet();
        }

        SearchResponse search = client().prepareSearch()
                .setQuery(fieldQuery("title", "test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title", 30, 1, 10)
                .execute().actionGet();


        assertNoFailures(search);

        assertThat(search.getHits().totalHits(), equalTo(5l));
        assertThat(search.getHits().hits().length, equalTo(5));

        for (SearchHit hit : search.getHits()) {
            assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("highlighting <em>test</em> for *&amp;? elasticsearch"));
        }
    }

    @Test
    public void testMultiMapperVectorWithStore() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "multi_field").startObject("fields")
                        .startObject("title").field("type", "string").field("store", "yes").field("term_vector", "with_positions_offsets").endObject()
                        .startObject("key").field("type", "string").field("store", "yes").field("term_vector", "with_positions_offsets").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource("title", "this is a test").execute().actionGet();
        refresh();
        // simple search on body with standard analyzer with a simple field query
        SearchResponse search = client().prepareSearch()
                .setQuery(fieldQuery("title", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title", 50, 1)
                .execute().actionGet();
        

        SearchHit hit = search.getHits().getAt(0);
        assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("this is a <em>test</em>"));

        // search on title.key and highlight on title
        search = client().prepareSearch()
                .setQuery(fieldQuery("title.key", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title.key", 50, 1)
                .execute().actionGet();
        assertNoFailures(search);

        hit = search.getHits().getAt(0);
        assertThat(hit.highlightFields().get("title.key").fragments()[0].string(), equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em>"));
    }

    @Test
    public void testMultiMapperVectorFromSource() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "multi_field").startObject("fields")
                        .startObject("title").field("type", "string").field("store", "no").field("term_vector", "with_positions_offsets").endObject()
                        .startObject("key").field("type", "string").field("store", "no").field("term_vector", "with_positions_offsets").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("title", "this is a test").execute().actionGet();
        refresh();

        // simple search on body with standard analyzer with a simple field query
        SearchResponse search = client().prepareSearch()
                .setQuery(fieldQuery("title", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title", 50, 1)
                .execute().actionGet();
        assertNoFailures(search);

        SearchHit hit = search.getHits().getAt(0);
        assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("this is a <em>test</em>"));

        // search on title.key and highlight on title.key
        search = client().prepareSearch()
                .setQuery(fieldQuery("title.key", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title.key", 50, 1)
                .execute().actionGet();
        assertNoFailures(search);

        hit = search.getHits().getAt(0);
        assertThat(hit.highlightFields().get("title.key").fragments()[0].string(), equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em>"));
    }

    @Test
    public void testMultiMapperNoVectorWithStore() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "multi_field").startObject("fields")
                        .startObject("title").field("type", "string").field("store", "yes").field("term_vector", "no").endObject()
                        .startObject("key").field("type", "string").field("store", "yes").field("term_vector", "no").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource("title", "this is a test").execute().actionGet();
        refresh();

        // simple search on body with standard analyzer with a simple field query
        SearchResponse search = client().prepareSearch()
                .setQuery(fieldQuery("title", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title", 50, 1)
                .execute().actionGet();
        assertNoFailures(search);

        SearchHit hit = search.getHits().getAt(0);
        assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("this is a <em>test</em>"));

        // search on title.key and highlight on title
        search = client().prepareSearch()
                .setQuery(fieldQuery("title.key", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title.key", 50, 1)
                .execute().actionGet();
        assertNoFailures(search);

        hit = search.getHits().getAt(0);
        assertThat(hit.highlightFields().get("title.key").fragments()[0].string(), equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em>"));
    }

    @Test
    public void testMultiMapperNoVectorFromSource() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "multi_field").startObject("fields")
                        .startObject("title").field("type", "string").field("store", "no").field("term_vector", "no").endObject()
                        .startObject("key").field("type", "string").field("store", "no").field("term_vector", "no").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        ensureGreen();
        client().prepareIndex("test", "type1", "1").setSource("title", "this is a test").execute().actionGet();
        refresh();

        // simple search on body with standard analyzer with a simple field query
        SearchResponse search = client().prepareSearch()
                .setQuery(fieldQuery("title", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title", 50, 1)
                .execute().actionGet();
        assertNoFailures(search);

        SearchHit hit = search.getHits().getAt(0);
        assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("this is a <em>test</em>"));

        // search on title.key and highlight on title.key
        search = client().prepareSearch()
                .setQuery(fieldQuery("title.key", "this is a test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title.key", 50, 1)
                .execute().actionGet();
        assertNoFailures(search);

        hit = search.getHits().getAt(0);
        assertThat(hit.highlightFields().get("title.key").fragments()[0].string(), equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em>"));
    }

    @Test
    public void testFastVectorHighlighterShouldFailIfNoTermVectors() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "string").field("store", "yes").field("term_vector", "no").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        ensureGreen();

        for (int i = 0; i < 5; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a test for the enabling fast vector highlighter").setRefresh(true).execute().actionGet();
        }
        refresh();

        SearchResponse search = client().prepareSearch()
                .setQuery(matchPhraseQuery("title", "this is a test"))
                .addHighlightedField("title", 50, 1, 10)
                .execute().actionGet();

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
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "string").field("store", "yes").field("term_vector", "with_positions_offsets").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        ensureGreen();
        
        for (int i = 0; i < 5; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a test for the workaround for the fast vector highlighting SOLR-3724").execute().actionGet();
        }
        refresh();
        SearchResponse search = client().prepareSearch()
                .setQuery(matchPhraseQuery("title", "test for the workaround"))
                .addHighlightedField("title", 50, 1, 10)
                .execute().actionGet();

        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));

        assertThat(search.getHits().totalHits(), equalTo(5l));
        assertThat(search.getHits().hits().length, equalTo(5));

        for (SearchHit hit : search.getHits()) {
            // Because of SOLR-3724 nothing is highlighted when FVH is used
            assertThat(hit.highlightFields().isEmpty(), equalTo(true));
        }

        // Using plain highlighter instead of FVH
        search = client().prepareSearch()
                .setQuery(matchPhraseQuery("title", "test for the workaround"))
                .addHighlightedField("title", 50, 1, 10)
                .setHighlighterType("highlighter")
                .execute().actionGet();

        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));

        assertThat(search.getHits().totalHits(), equalTo(5l));
        assertThat(search.getHits().hits().length, equalTo(5));

        for (SearchHit hit : search.getHits()) {
            // With plain highlighter terms are highlighted correctly
            assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("This is a <em>test</em> for the <em>workaround</em> for the fast vector highlighting SOLR-3724"));
        }

        // Using plain highlighter instead of FVH on the field level
        search = client().prepareSearch()
                .setQuery(matchPhraseQuery("title", "test for the workaround"))
                .addHighlightedField(new HighlightBuilder.Field("title").highlighterType("highlighter"))
                .setHighlighterType("highlighter")
                .execute().actionGet();

        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));

        assertThat(search.getHits().totalHits(), equalTo(5l));
        assertThat(search.getHits().hits().length, equalTo(5));

        for (SearchHit hit : search.getHits()) {
            // With plain highlighter terms are highlighted correctly
            assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("This is a <em>test</em> for the <em>workaround</em> for the fast vector highlighting SOLR-3724"));
        }
    }

    @Test
    public void testFSHHighlightAllMvFragments() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("tags").field("type", "string").field("term_vector", "with_positions_offsets").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        ensureGreen();
        client().prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject().field("tags",
                        "this is a really long tag i would like to highlight",
                        "here is another one that is very long and has the tag token near the end").endObject())
                .execute().actionGet();
        refresh();
        
        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("tags", "tag"))
                .addHighlightedField("tags", -1, 0)
                .execute().actionGet();

        assertThat(response.getHits().hits()[0].highlightFields().get("tags").fragments().length, equalTo(2));
        assertThat(response.getHits().hits()[0].highlightFields().get("tags").fragments()[0].string(), equalTo("this is a really long <em>tag</em> i would like to highlight"));
        assertThat(response.getHits().hits()[0].highlightFields().get("tags").fragments()[1].string(), equalTo("here is another one that is very long and has the <em>tag</em> token near the end"));
    }
    
    
    @Test
    public void testBoostingQuery() {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog")
                .execute().actionGet();
        refresh();


        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(boostingQuery().positive(termQuery("field2", "brown")).negative(termQuery("field2", "foobar")).negativeBoost(0.5f))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2").order("score").preTags("<x>").postTags("</x>"));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("The quick <x>brown</x> fox jumps over the lazy dog"));
    }
    
    @Test
    public void testBoostingQueryTermVector() throws ElasticSearchException, IOException {
        client().admin().indices().prepareCreate("test").addMapping("type1", type1TermVectorMapping()).execute().actionGet();
        ensureGreen();
        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog")
                .execute().actionGet();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(boostingQuery().positive(termQuery("field2", "brown")).negative(termQuery("field2", "foobar")).negativeBoost(0.5f))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2").order("score").preTags("<x>").postTags("</x>"));

        SearchResponse searchResponse = client().search(
                searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(),
                equalTo("The quick <x>brown</x> fox jumps over the lazy dog"));
    }


    @Test
    public void testCommonTermsQuery() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog")
                .execute().actionGet();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(commonTerms("field2", "quick brown").cutoffFrequency(100))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2").order("score").preTags("<x>").postTags("</x>"));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("The <x>quick</x> <x>brown</x> fox jumps over the lazy dog"));
    }

    @Test
    public void testCommonTermsTermVector() throws ElasticSearchException, IOException {
        client().admin().indices().prepareCreate("test").addMapping("type1", type1TermVectorMapping()).execute().actionGet();
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog")
                .execute().actionGet();
        refresh();
        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource().query(commonTerms("field2", "quick brown").cutoffFrequency(100)).from(0).size(60)
                .explain(true).highlight(highlight().field("field2").order("score").preTags("<x>").postTags("</x>"));

        SearchResponse searchResponse = client().search(
                searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(),
                equalTo("The <x>quick</x> <x>brown</x> fox jumps over the lazy dog"));
    }


    @Test
    public void testPhrasePrefix() throws ElasticSearchException, IOException {
        Builder builder = ImmutableSettings.builder();
        builder.put("index.analysis.analyzer.synonym.tokenizer", "whitespace");
        builder.putArray("index.analysis.analyzer.synonym.filter", "synonym", "lowercase");
        builder.put("index.analysis.filter.synonym.type", "synonym");
        builder.putArray("index.analysis.filter.synonym.synonyms", "quick => fast");

        XContentBuilder type2Mapping = XContentFactory.jsonBuilder().startObject().startObject("type2")
                .startObject("_all").field("store", "yes").field("termVector", "with_positions_offsets").endObject()
                .startObject("properties")
                .startObject("field4").field("type", "string").field("termVector", "with_positions_offsets").field("analyzer", "synonym").endObject()
                .startObject("field3").field("type", "string").field("analyzer", "synonym").endObject()
                .endObject()
                .endObject().endObject();


        client().admin().indices().prepareCreate("test").setSettings(builder.build()).addMapping("type1", type1TermVectorMapping()).addMapping("type2", type2Mapping).execute().actionGet();
        ensureGreen();

        client().prepareIndex("test", "type1", "0")
                .setSource("field0", "The quick brown fox jumps over the lazy dog", "field1", "The quick brown fox jumps over the lazy dog")
                .execute().actionGet();
        client().prepareIndex("test", "type1", "1")
                .setSource("field1", "The quick browse button is a fancy thing, right bro?")
                .execute().actionGet();
        refresh();
        logger.info("--> highlighting and searching on field0");
        SearchSourceBuilder source = searchSource()
                .query(matchPhrasePrefixQuery("field0", "quick bro"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field0").order("score").preTags("<x>").postTags("</x>"));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field0").fragments()[0].string(), equalTo("The <x>quick</x> <x>brown</x> fox jumps over the lazy dog"));

        logger.info("--> highlighting and searching on field1");
        source = searchSource()
                .query(matchPhrasePrefixQuery("field1", "quick bro"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field1").order("score").preTags("<x>").postTags("</x>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field1").fragments()[0].string(), equalTo("The <x>quick browse</x> button is a fancy thing, right bro?"));
        assertThat(searchResponse.getHits().getAt(1).highlightFields().get("field1").fragments()[0].string(), equalTo("The <x>quick brown</x> fox jumps over the lazy dog"));

        // with synonyms
        client().prepareIndex("test", "type2", "0")
                .setSource("field4", "The quick brown fox jumps over the lazy dog", "field3", "The quick brown fox jumps over the lazy dog")
                .setRefresh(true).execute().actionGet();
        client().prepareIndex("test", "type2", "1")
                .setSource("field4", "The quick browse button is a fancy thing, right bro?").setRefresh(true).execute().actionGet();
        client().prepareIndex("test", "type2", "2")
                .setSource("field4", "a quick fast blue car")
                .setRefresh(true).execute().actionGet();

        source = searchSource().filter(FilterBuilders.typeFilter("type2")).query(matchPhrasePrefixQuery("field3", "fast bro")).from(0).size(60).explain(true)
                .highlight(highlight().field("field3").order("score").preTags("<x>").postTags("</x>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field3").fragments()[0].string(),
                equalTo("The <x>quick</x> <x>brown</x> fox jumps over the lazy dog"));

        logger.info("--> highlighting and searching on field4");
        source = searchSource().filter(FilterBuilders.typeFilter("type2")).query(matchPhrasePrefixQuery("field4", "the fast bro")).from(0).size(60).explain(true)
                .highlight(highlight().field("field4").order("score").preTags("<x>").postTags("</x>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field4").fragments()[0].string(),
                equalTo("<x>The quick browse</x> button is a fancy thing, right bro?"));
        assertThat(searchResponse.getHits().getAt(1).highlightFields().get("field4").fragments()[0].string(),
                equalTo("<x>The quick brown</x> fox jumps over the lazy dog"));

        logger.info("--> highlighting and searching on field4");
        source = searchSource().filter(FilterBuilders.typeFilter("type2")).query(matchPhrasePrefixQuery("field4", "a fast quick blue ca")).from(0).size(60).explain(true)
                .highlight(highlight().field("field4").order("score").preTags("<x>").postTags("</x>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH)).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field4").fragments()[0].string(),
                equalTo("<x>a quick fast blue car</x>"));

    }

    @Test
    public void testPlainHighlightDifferentFragmenter() throws Exception {
        prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("tags").field("type", "string").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        ensureGreen();
        client().prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject().field("tags",
                        "this is a really long tag i would like to highlight",
                        "here is another one that is very long tag and has the tag token near the end").endObject())
                .setRefresh(true).execute().actionGet();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("tags", "long tag").type(MatchQueryBuilder.Type.PHRASE))
                .addHighlightedField(new HighlightBuilder.Field("tags")
                        .fragmentSize(-1).numOfFragments(2).fragmenter("simple"))
                .execute().actionGet();
        assertThat(response.getHits().hits()[0].highlightFields().get("tags").fragments().length, equalTo(2));
        assertThat(response.getHits().hits()[0].highlightFields().get("tags").fragments()[0].string(), equalTo("this is a really <em>long</em> <em>tag</em> i would like to highlight"));
        assertThat(response.getHits().hits()[0].highlightFields().get("tags").fragments()[1].string(), equalTo("here is another one that is very <em>long</em> <em>tag</em> and has the tag token near the end"));

        response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("tags", "long tag").type(MatchQueryBuilder.Type.PHRASE))
                .addHighlightedField(new HighlightBuilder.Field("tags")
                        .fragmentSize(-1).numOfFragments(2).fragmenter("span"))
                .execute().actionGet();
        assertThat(response.getHits().hits()[0].highlightFields().get("tags").fragments().length, equalTo(2));
        assertThat(response.getHits().hits()[0].highlightFields().get("tags").fragments()[0].string(), equalTo("this is a really <em>long</em> <em>tag</em> i would like to highlight"));
        assertThat(response.getHits().hits()[0].highlightFields().get("tags").fragments()[1].string(), equalTo("here is another one that is very <em>long</em> <em>tag</em> and has the tag token near the end"));

        try {
            client().prepareSearch("test")
                    .setQuery(QueryBuilders.matchQuery("tags", "long tag").type(MatchQueryBuilder.Type.PHRASE))
                    .addHighlightedField(new HighlightBuilder.Field("tags")
                            .fragmentSize(-1).numOfFragments(2).fragmenter("invalid"))
                    .execute().actionGet();
            fail("Shouldn't get here");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.shardFailures()[0].status(), equalTo(RestStatus.BAD_REQUEST));
        }
    }

    @Test
    public void testMissingStoredField() throws Exception {
        prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("highlight_field").field("type", "string").field("store", "yes").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
        ensureGreen();
        client().prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject()
                    .field("field", "highlight")
                .endObject())
                .setRefresh(true).execute().actionGet();

        // This query used to fail when the field to highlight was absent
        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("field", "highlight").type(MatchQueryBuilder.Type.BOOLEAN))
                .addHighlightedField(new HighlightBuilder.Field("highlight_field")
                        .fragmentSize(-1).numOfFragments(1).fragmenter("simple"))
                .execute().actionGet();
        assertThat(response.getHits().hits()[0].highlightFields().isEmpty(), equalTo(true));
    }

    @Test
    // https://github.com/elasticsearch/elasticsearch/issues/3211
    public void testNumericHighlighting() throws Exception {
        wipeIndex("test");
        prepareCreate("test")
        .addMapping("test", jsonBuilder()
                .startObject()
                    .startObject("test")
                        .startObject("properties")
                            .startObject("text")
                                .field("type", "string")
                                .field("index", "analyzed")
                            .endObject()
                            .startObject("byte")
                                .field("type", "byte")
                            .endObject()
                            .startObject("short")
                                .field("type", "short")
                            .endObject()
                            .startObject("int")
                                .field("type", "integer")
                            .endObject()
                            .startObject("long")
                                .field("type", "long")
                            .endObject()
                            .startObject("float")
                                .field("type", "float")
                            .endObject()
                            .startObject("double")
                                .field("type", "double")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject())
        .execute().actionGet();

        ensureGreen();

        client().prepareIndex("test", "test", "1")
        .setSource(jsonBuilder().startObject()
            .field("text", "elasticsearch test")
            .field("byte", 25)
            .field("short", 42)
            .field("int", 100)
            .field("long", -1)
            .field("float", 3.2f)
            .field("double", 42.42)
        .endObject())
        .setRefresh(true).execute().actionGet();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("text", "test").type(MatchQueryBuilder.Type.BOOLEAN))
                .addHighlightedField("text")
                .addHighlightedField("byte")
                .addHighlightedField("short")
                .addHighlightedField("int")
                .addHighlightedField("long")
                .addHighlightedField("float")
                .addHighlightedField("double")
                .execute().actionGet();
        assertThat(response.getHits().totalHits(), equalTo(1L));
        // Highlighting of numeric fields is not supported, but it should not raise errors
        // (this behavior is consistent with version 0.20)
        assertThat(response.getFailedShards(), equalTo(0));
    }

    @Test
    // https://github.com/elasticsearch/elasticsearch/issues/3200
    public void testResetTwice() throws Exception {
        prepareCreate("test")
            .setSettings(ImmutableSettings.builder()
                .put("analysis.analyzer.my_analyzer.type", "pattern")
                .put("analysis.analyzer.my_analyzer.pattern", "\\s+")
            .build())
            .addMapping("type", jsonBuilder()
                .startObject()
                    .startObject("type")
                        .startObject("properties")
                            .startObject("text")
                                .field("type", "string")
                                .field("analyzer", "my_analyzer")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject())
            .execute().actionGet();

        ensureGreen();
        client().prepareIndex("test", "type", "1")
            .setSource(jsonBuilder().startObject()
                    .field("text", "elasticsearch test")
                .endObject())
            .setRefresh(true)
            .execute().actionGet();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("text", "test").type(MatchQueryBuilder.Type.BOOLEAN))
                .addHighlightedField("text").execute().actionGet();
        assertThat(response.getHits().totalHits(), equalTo(1L));
        // PatternAnalyzer will throw an exception if it is resetted twice
        assertThat(response.getFailedShards(), equalTo(0));
    }

    @Test
    public void testHighlightUsesHighlightQuery() throws IOException {
        prepareCreate("test")
                .addMapping("type1", "text", "type=string,store=yes,term_vector=with_positions_offsets")
                .get();
        ensureGreen();

        index("test", "type1", "1", "text", "some stuff stuff stuff stuff stuff to highlight against the stuff phrase");
        refresh();

        // Make sure the fvh doesn't highlight in the same way as we're going to do with a scoreQuery because
        // that would invalidate the test results.
        Matcher<String> highlightedMatcher = anyOf(
                containsString("<em>stuff phrase</em>"),            //t FHV normally does this
                containsString("<em>stuff</em> <em>phrase</em>"));  // Plain normally does this
        HighlightBuilder.Field field = new HighlightBuilder.Field("text")
                .fragmentSize(20)
                .numOfFragments(1)
                .highlighterType("fvh");
        SearchRequestBuilder search = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchQuery("text", "stuff"))
                .setHighlighterOrder("score")
                .addHighlightedField(field);
        SearchResponse response = search.get();
        assertHighlight(response, 0, "text", 0, not(highlightedMatcher));

        // And do the same for the plain highlighter
        field.highlighterType("plain");
        response = search.get();
        assertHighlight(response, 0, "text", 0, not(highlightedMatcher));

        // Make sure the fvh takes the highlightQuery into account
        field.highlighterType("fvh").highlightQuery(matchPhraseQuery("text", "stuff phrase"));
        response = search.get();
        assertHighlight(response, 0, "text", 0, highlightedMatcher);

        // And do the same for the plain highlighter
        field.highlighterType("plain");
        response = search.get();
        assertHighlight(response, 0, "text", 0, highlightedMatcher);
        // Note that the plain highlighter doesn't join the highlighted elements for us

        // Make sure the fvh takes the highlightQuery into account when it is set on the highlight context instead of the field
        search.setHighlighterQuery(matchPhraseQuery("text", "stuff phrase"));
        field.highlighterType("fvh").highlightQuery(null);
        response = search.get();
        assertHighlight(response, 0, "text", 0, highlightedMatcher);

        // And do the same for the plain highlighter
        field.highlighterType("plain");
        response = search.get();
        assertHighlight(response, 0, "text", 0, highlightedMatcher);
        // Note that the plain highlighter doesn't join the highlighted elements for us
    }

    private static String randomStoreField() {
        if (randomBoolean()) {
            return "store=yes,";
        }
        return "";
    }

    public void testHighlightNoMatchSize() throws IOException {

        prepareCreate("test")
            .addMapping("type1", "text", "type=string," + randomStoreField() + "term_vector=with_positions_offsets,index_options=offsets")
            .get();
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
        prepareCreate("test")
            .addMapping("type1", "text", "type=string," + randomStoreField() + "term_vector=with_positions_offsets,index_options=offsets")
            .get();
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
        prepareCreate("test")
                .addMapping("type1", "text", "type=string," + randomStoreField() + "term_vector=with_positions_offsets,index_options=offsets")
                .get();
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
        client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()).get();
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy quick dog").setRefresh(true).get();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "test"))
                .highlight(highlight().field("field1").preTags("<xxx>").postTags("</xxx>"));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();
        assertHitCount(searchResponse, 1l);

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field1").fragments()[0].string(), equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field1");
        source = searchSource()
                .query(termQuery("_all", "test"))
                .highlight(highlight().field("field1").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();
        assertHitCount(searchResponse, 1l);

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field1").fragments()[0].string(), equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(termQuery("_all", "quick"))
                .highlight(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();
        assertHitCount(searchResponse, 1l);

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy <xxx>quick</xxx> dog"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(matchPhraseQuery("_all", "quick brown"))
                .highlight(highlight().field("field2").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();
        assertHitCount(searchResponse, 1l);
        //phrase query results in highlighting all different terms regardless of their positions
        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("The <xxx>quick</xxx> <xxx>brown</xxx> fox jumps over the lazy <xxx>quick</xxx> dog"));

        //lets fall back to the standard highlighter then, what people would do to highlight query matches
        logger.info("--> searching on _all, highlighting on field2, falling back to the plain highlighter");
        source = searchSource()
                .query(matchPhraseQuery("_all", "quick brown"))
                .highlight(highlight().field("field2").preTags("<xxx>").postTags("</xxx>").highlighterType("highlighter"));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();
        assertHitCount(searchResponse, 1l);

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("The <xxx>quick</xxx> <xxx>brown</xxx> fox jumps over the lazy quick dog"));
    }

    @Test
    public void testPostingsHighlighterMultipleFields() throws Exception {
        client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()).get();
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test1", "field2", "this is a test2", "field3", "this is a test3").setRefresh(true).get();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(boolQuery()
                        .should(termQuery("field1", "test1"))
                        .should(termQuery("field2", "test2"))
                        .should(termQuery("field3", "test3")))
                .highlight(highlight().preTags("<xxx>").postTags("</xxx>").requireFieldMatch(false)
                        .field("field1").field("field2").field(new HighlightBuilder.Field("field3").preTags("<x3>").postTags("</x3>")));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();
        assertHitCount(searchResponse, 1l);

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field1").fragments()[0].string(), equalTo("this is a <xxx>test1</xxx>"));
        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("this is a <xxx>test2</xxx>"));
        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field3").fragments()[0].string(), equalTo("this is a <x3>test3</x3>"));
    }

    @Test
    public void testPostingsHighlighterNumberOfFragments() throws Exception {
        client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()).get();
        ensureGreen();

        client().prepareIndex("test", "type1", "1")
                .setSource("field1", "The quick brown fox jumps over the lazy dog. The lazy red fox jumps over the quick dog. The quick brown dog jumps over the lazy fox.",
                        "field2", "The quick brown fox jumps over the lazy dog. The lazy red fox jumps over the quick dog. The quick brown dog jumps over the lazy fox.")
                .setRefresh(true).get();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "fox"))
                .highlight(highlight()
                        .field(new HighlightBuilder.Field("field1").numOfFragments(5).preTags("<field1>").postTags("</field1>"))
                        .field(new HighlightBuilder.Field("field2").numOfFragments(2).preTags("<field2>").postTags("</field2>")));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();
        assertHitCount(searchResponse, 1l);


        Map<String,HighlightField> highlightFieldMap = searchResponse.getHits().getAt(0).highlightFields();
        assertThat(highlightFieldMap.size(), equalTo(2));
        HighlightField field1 = highlightFieldMap.get("field1");
        assertThat(field1.fragments().length, equalTo(3));
        assertThat(field1.fragments()[0].string(), equalTo("The quick brown <field1>fox</field1> jumps over the lazy dog."));
        assertThat(field1.fragments()[1].string(), equalTo("The lazy red <field1>fox</field1> jumps over the quick dog."));
        assertThat(field1.fragments()[2].string(), equalTo("The quick brown dog jumps over the lazy <field1>fox</field1>."));

        HighlightField field2 = highlightFieldMap.get("field2");
        assertThat(field2.fragments().length, equalTo(2));
        assertThat(field2.fragments()[0].string(), equalTo("The quick brown <field2>fox</field2> jumps over the lazy dog."));
        assertThat(field2.fragments()[1].string(), equalTo("The lazy red <field2>fox</field2> jumps over the quick dog."));


        client().prepareIndex("test", "type1", "2")
                .setSource("field1", new String[]{"The quick brown fox jumps over the lazy dog. Second sentence not finished", "The lazy red fox jumps over the quick dog.", "The quick brown dog jumps over the lazy fox."})
                .setRefresh(true).get();

        source = searchSource()
                .query(termQuery("field1", "fox"))
                .highlight(highlight()
                        .field(new HighlightBuilder.Field("field1").numOfFragments(0).preTags("<field1>").postTags("</field1>")));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();
        assertHitCount(searchResponse, 2l);

        for (SearchHit searchHit : searchResponse.getHits()) {
            highlightFieldMap = searchHit.highlightFields();
            assertThat(highlightFieldMap.size(), equalTo(1));
            field1 = highlightFieldMap.get("field1");
            assertThat(field1, notNullValue());
            if ("1".equals(searchHit.id())) {
                assertThat(field1.fragments().length, equalTo(1));
                assertThat(field1.fragments()[0].string(), equalTo("The quick brown <field1>fox</field1> jumps over the lazy dog. The lazy red <field1>fox</field1> jumps over the quick dog. The quick brown dog jumps over the lazy <field1>fox</field1>."));
            } else if ("2".equals(searchHit.id())) {
                assertThat(field1.fragments().length, equalTo(3));
                assertThat(field1.fragments()[0].string(), equalTo("The quick brown <field1>fox</field1> jumps over the lazy dog. Second sentence not finished"));
                assertThat(field1.fragments()[1].string(), equalTo("The lazy red <field1>fox</field1> jumps over the quick dog."));
                assertThat(field1.fragments()[2].string(), equalTo("The quick brown dog jumps over the lazy <field1>fox</field1>."));
            } else {
                fail("Only hits with id 1 and 2 are returned");
            }
        }
    }

    @Test
    public void testPostingsHighlighterRequireFieldMatch() throws Exception {
        client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()).get();
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field1", "The quick brown fox jumps over the lazy dog. The lazy red fox jumps over the quick dog. The quick brown dog jumps over the lazy fox.",
                        "field2", "The quick brown fox jumps over the lazy dog. The lazy red fox jumps over the quick dog. The quick brown dog jumps over the lazy fox.")
                .setRefresh(true).get();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "fox"))
                .highlight(highlight()
                        .field(new HighlightBuilder.Field("field1").requireFieldMatch(true).preTags("<field1>").postTags("</field1>"))
                        .field(new HighlightBuilder.Field("field2").requireFieldMatch(true).preTags("<field2>").postTags("</field2>")));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();
        assertHitCount(searchResponse, 1l);

        //field2 is not returned highlighted because of the require field match option set to true
        Map<String,HighlightField> highlightFieldMap = searchResponse.getHits().getAt(0).highlightFields();
        assertThat(highlightFieldMap.size(), equalTo(1));
        HighlightField field1 = highlightFieldMap.get("field1");
        assertThat(field1.fragments().length, equalTo(3));
        assertThat(field1.fragments()[0].string(), equalTo("The quick brown <field1>fox</field1> jumps over the lazy dog."));
        assertThat(field1.fragments()[1].string(), equalTo("The lazy red <field1>fox</field1> jumps over the quick dog."));
        assertThat(field1.fragments()[2].string(), equalTo("The quick brown dog jumps over the lazy <field1>fox</field1>."));


        logger.info("--> highlighting and searching on field1 and field2 - require field match set to false");
        source = searchSource()
                .query(termQuery("field1", "fox"))
                .highlight(highlight()
                        .field(new HighlightBuilder.Field("field1").requireFieldMatch(false).preTags("<field1>").postTags("</field1>"))
                        .field(new HighlightBuilder.Field("field2").requireFieldMatch(false).preTags("<field2>").postTags("</field2>")));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();
        assertHitCount(searchResponse, 1l);

        //field2 is now returned highlighted thanks to the multi_match query on both fields
        highlightFieldMap = searchResponse.getHits().getAt(0).highlightFields();
        assertThat(highlightFieldMap.size(), equalTo(2));
        field1 = highlightFieldMap.get("field1");
        assertThat(field1.fragments().length, equalTo(3));
        assertThat(field1.fragments()[0].string(), equalTo("The quick brown <field1>fox</field1> jumps over the lazy dog."));
        assertThat(field1.fragments()[1].string(), equalTo("The lazy red <field1>fox</field1> jumps over the quick dog."));
        assertThat(field1.fragments()[2].string(), equalTo("The quick brown dog jumps over the lazy <field1>fox</field1>."));

        HighlightField field2 = highlightFieldMap.get("field2");
        assertThat(field2.fragments().length, equalTo(3));
        assertThat(field2.fragments()[0].string(), equalTo("The quick brown <field2>fox</field2> jumps over the lazy dog."));
        assertThat(field2.fragments()[1].string(), equalTo("The lazy red <field2>fox</field2> jumps over the quick dog."));
        assertThat(field2.fragments()[2].string(), equalTo("The quick brown dog jumps over the lazy <field2>fox</field2>."));


        logger.info("--> highlighting and searching on field1 and field2 via multi_match query");
        source = searchSource()
                .query(multiMatchQuery("fox", "field1", "field2"))
                .highlight(highlight()
                        .field(new HighlightBuilder.Field("field1").requireFieldMatch(true).preTags("<field1>").postTags("</field1>"))
                        .field(new HighlightBuilder.Field("field2").requireFieldMatch(true).preTags("<field2>").postTags("</field2>")));

        searchResponse = client().search(searchRequest("test").source(source)).actionGet();
        assertHitCount(searchResponse, 1l);

        //field2 is now returned highlighted thanks to the multi_match query on both fields
        highlightFieldMap = searchResponse.getHits().getAt(0).highlightFields();
        assertThat(highlightFieldMap.size(), equalTo(2));
        field1 = highlightFieldMap.get("field1");
        assertThat(field1.fragments().length, equalTo(3));
        assertThat(field1.fragments()[0].string(), equalTo("The quick brown <field1>fox</field1> jumps over the lazy dog."));
        assertThat(field1.fragments()[1].string(), equalTo("The lazy red <field1>fox</field1> jumps over the quick dog."));
        assertThat(field1.fragments()[2].string(), equalTo("The quick brown dog jumps over the lazy <field1>fox</field1>."));

        field2 = highlightFieldMap.get("field2");
        assertThat(field2.fragments().length, equalTo(3));
        assertThat(field2.fragments()[0].string(), equalTo("The quick brown <field2>fox</field2> jumps over the lazy dog."));
        assertThat(field2.fragments()[1].string(), equalTo("The lazy red <field2>fox</field2> jumps over the quick dog."));
        assertThat(field2.fragments()[2].string(), equalTo("The quick brown dog jumps over the lazy <field2>fox</field2>."));
    }

    @Test
    public void testPostingsHighlighterOrderByScore() throws Exception {
        client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()).get();
        ensureGreen();

        client().prepareIndex("test", "type1")
                .setSource("field1", new String[]{"This sentence contains one match, not that short. This sentence contains two sentence matches. This one contains no matches.",
                "This is the second value's first sentence. This one contains no matches. This sentence contains three sentence occurrences (sentence).",
                "One sentence match here and scored lower since the text is quite long, not that appealing. This one contains no matches."})
                .setRefresh(true).get();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "sentence"))
                .highlight(highlight().field("field1").order("score"));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();
        assertHitCount(searchResponse, 1l);

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
        assertHitCount(searchResponse, 1l);

        highlightFieldMap = searchResponse.getHits().getAt(0).highlightFields();
        assertThat(highlightFieldMap.size(), equalTo(1));
        field1 = highlightFieldMap.get("field1");
        assertThat(field1.fragments().length, equalTo(3));
        assertThat(field1.fragments()[0].string(), equalTo("This is the second value's first <em>sentence</em>. This one contains no matches. This <em>sentence</em> contains three <em>sentence</em> occurrences (<em>sentence</em>)."));
        assertThat(field1.fragments()[1].string(), equalTo("This <em>sentence</em> contains one match, not that short. This <em>sentence</em> contains two <em>sentence</em> matches. This one contains no matches."));
        assertThat(field1.fragments()[2].string(), equalTo("One <em>sentence</em> match here and scored lower since the text is quite long, not that appealing. This one contains no matches."));
    }

    @Test
    public void testPostingsHighlighterEscapeHtml() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "string").field("store", "yes").field("index_options", "offsets").endObject()
                        .endObject().endObject().endObject())
                .get();
        ensureYellow();
        for (int i = 0; i < 5; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a html escaping highlighting test for *&? elasticsearch").setRefresh(true).execute().actionGet();
        }

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchQuery("title", "test"))
                .setHighlighterEncoder("html")
                .addHighlightedField("title").get();

        assertHitCount(searchResponse, 5l);
        assertThat(searchResponse.getHits().hits().length, equalTo(5));

        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("This is a html escaping highlighting <em>test</em> for *&amp;?"));
        }
    }

    @Test
    public void testPostingsHighlighterMultiMapperWithStore() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1")
                        //just to make sure that we hit the stored fields rather than the _source
                        .startObject("_source").field("enabled", false).endObject()
                        .startObject("properties")
                        .startObject("title").field("type", "multi_field").startObject("fields")
                        .startObject("title").field("type", "string").field("store", "yes").field("index_options", "offsets").endObject()
                        .startObject("key").field("type", "string").field("store", "yes").field("index_options", "offsets").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();
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
        assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("this is a <em>test</em> ."));

        // search on title.key and highlight on title
        searchResponse = client().prepareSearch()
                .setQuery(matchQuery("title.key", "this is a test"))
                .addHighlightedField("title.key").get();
        assertHitCount(searchResponse, 1l);

        hit = searchResponse.getHits().getAt(0);
        //stopwords are now highlighted since we used only whitespace analyzer here
        assertThat(hit.highlightFields().get("title.key").fragments()[0].string(), equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em> ."));
    }

    @Test
    public void testPostingsHighlighterMultiMapperFromSource() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "multi_field").startObject("fields")
                        .startObject("title").field("type", "string").field("store", "no").field("index_options", "offsets").endObject()
                        .startObject("key").field("type", "string").field("store", "no").field("index_options", "offsets").field("analyzer", "whitespace").endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject())
                .get();
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("title", "this is a test").get();
        refresh();

        // simple search on body with standard analyzer with a simple field query
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchQuery("title", "this is a test"))
                .addHighlightedField("title")
                .execute().actionGet();

        assertHitCount(searchResponse, 1l);

        SearchHit hit = searchResponse.getHits().getAt(0);
        assertThat(hit.highlightFields().get("title").fragments()[0].string(), equalTo("this is a <em>test</em>"));

        // search on title.key and highlight on title.key
        searchResponse = client().prepareSearch()
                .setQuery(matchQuery("title.key", "this is a test"))
                .addHighlightedField("title.key")
                .get();
        assertHitCount(searchResponse, 1l);

        hit = searchResponse.getHits().getAt(0);
        assertThat(hit.highlightFields().get("title.key").fragments()[0].string(), equalTo("<em>this</em> <em>is</em> <em>a</em> <em>test</em>"));
    }

    @Test
    public void testPostingsHighlighterShouldFailIfNoOffsets() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("title").field("type", "string").field("store", "yes").field("index_options", "docs").endObject()
                        .endObject().endObject().endObject())
                .get();
        ensureGreen();

        for (int i = 0; i < 5; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource("title", "This is a test for the postings highlighter").setRefresh(true).get();
        }
        refresh();

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
        client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()).get();
        ensureGreen();
        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog! Second sentence.")
                .get();
        refresh();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(boostingQuery().positive(termQuery("field2", "brown")).negative(termQuery("field2", "foobar")).negativeBoost(0.5f))
                .highlight(highlight().field("field2").preTags("<x>").postTags("</x>"));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();
        assertHitCount(searchResponse, 1l);

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(),
                equalTo("The quick <x>brown</x> fox jumps over the lazy dog!"));
    }

    @Test
    public void testPostingsHighlighterCommonTermsQuery() throws ElasticSearchException, IOException {
        client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()).get();
        ensureGreen();

        client().prepareIndex("test", "type1").setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog! Second sentence.").get();
        refresh();
        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource().query(commonTerms("field2", "quick brown").cutoffFrequency(100))
                .highlight(highlight().field("field2").preTags("<x>").postTags("</x>"));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source)).actionGet();
        assertHitCount(searchResponse, 1l);

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(),
                equalTo("The <x>quick</x> <x>brown</x> fox jumps over the lazy dog!"));
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
            assertHitCount(searchResponse, 1l);

            assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(),
                    equalTo("The <em>quick</em> brown fox jumps over the lazy dog!"));
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
        assertHitCount(searchResponse, 1l);

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(),
                equalTo("The <em>quick</em> brown fox jumps over the lazy dog!"));
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
            assertHitCount(searchResponse, 1l);

            assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(),
                    equalTo("The <em>quick</em> brown fox jumps over the lazy dog!"));
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
            assertHitCount(searchResponse, 1l);

            assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(),
                    equalTo("The <em>quick</em> brown fox jumps over the lazy dog!"));

            source = searchSource().query(wildcardQuery("field2", "qu*k").rewrite(rewriteMethod))
                    .highlight(highlight().field("field2"));
            searchResponse = client().search(searchRequest("test").source(source)
                    .searchType(randomBoolean() ? SearchType.DFS_QUERY_THEN_FETCH : SearchType.QUERY_THEN_FETCH)).get();
            assertHitCount(searchResponse, 1l);

            assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(),
                    equalTo("The <em>quick</em> brown fox jumps over the lazy dog!"));

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
        assertHitCount(searchResponse, 1l);

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(),
                equalTo("<em>aaab</em>"));
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
            assertHitCount(searchResponse, 1l);

            assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(),
                    equalTo("The <em>quick</em> brown fox jumps over the lazy dog!"));
        }
    }

    @Test
    @Slow
    public void testPostingsHighlighterManyDocs() throws Exception {
        client().admin().indices().prepareCreate("test").addMapping("type1", type1PostingsffsetsMapping()).get();
        ensureGreen();

        int COUNT = between(20, 100);
        logger.info("--> indexing docs");
        for (int i = 0; i < COUNT; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource("field1", "Sentence test " + i + ". Sentence two.").get();
        }
        refresh();

        logger.info("--> searching explicitly on field1 and highlighting on it");
        SearchResponse searchResponse = client().prepareSearch()
                .setSize(COUNT)
                .setQuery(termQuery("field1", "test"))
                .addHighlightedField("field1")
                .get();
        assertHitCount(searchResponse, (long)COUNT);
        assertThat(searchResponse.getHits().hits().length, equalTo(COUNT));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.highlightFields().get("field1").fragments()[0].string(), equalTo("Sentence <em>test</em> " + hit.id() + "."));
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
            assertThat(hit.highlightFields().get("field1").fragments()[0].string(), equalTo("Sentence <em>test</em> " + hit.id() + "."));
        }
    }
}
