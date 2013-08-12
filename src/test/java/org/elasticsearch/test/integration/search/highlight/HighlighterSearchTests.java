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

package org.elasticsearch.test.integration.search.highlight;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Priority;
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
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH;
import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.highlight;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHighlight;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

/**
 *
 */
public class HighlighterSearchTests extends AbstractSharedClusterTest {
    
    @Override
    protected int numberOfNodes() {
        return 4; // why 4?
    }
    
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
        run(addMapping(prepareCreate("test"), "test",
                new Object[] {
                "body", "type", "string", "index_analyzer", "custom_analyzer", "search_analyzer", "custom_analyzer",
                        "term_vector", "with_positions_offsets" }).setSettings(
                ImmutableSettings.settingsBuilder()                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("analysis.filter.wordDelimiter.type", "word_delimiter")
                .put("analysis.filter.wordDelimiter.type.split_on_numerics", false)
                .put("analysis.filter.wordDelimiter.generate_word_parts", true)
                .put("analysis.filter.wordDelimiter.generate_number_parts", true)
                .put("analysis.filter.wordDelimiter.catenate_words", true)
                .put("analysis.filter.wordDelimiter.catenate_numbers", true)
                .put("analysis.filter.wordDelimiter.catenate_all", false)
                .put("analysis.analyzer.custom_analyzer.tokenizer", "whitespace")
                .putArray("analysis.analyzer.custom_analyzer.filter", "lowercase", "wordDelimiter")));

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
        client().admin().indices().prepareCreate("test").execute().actionGet();
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

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

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field1").fragments()[0].string(), equalTo("this is a <global>test</global>"));
        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("this is another <field2>test</field2>"));
    }

    @Test
    public void testHighlightingOnWildcardFields() throws Exception {
        client().admin().indices().prepareCreate("test").execute().actionGet();
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "this is another test")
                .setRefresh(true).execute().actionGet();

        logger.info("--> highlighting and searching on field*");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "test"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field*").order("score").preTags("<xxx>").postTags("</xxx>"));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field1").fragments()[0].string(), equalTo("this is a <xxx>test</xxx>"));
        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("this is another <xxx>test</xxx>"));
    }

    @Test
    public void testPlainHighlighter() throws Exception {
        client().admin().indices().prepareCreate("test").execute().actionGet();
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        client().prepareIndex("test", "type1")
                .setSource("field1", "this is a test", "field2", "The quick brown fox jumps over the lazy dog")
                .setRefresh(true).execute().actionGet();

        logger.info("--> highlighting and searching on field1");
        SearchSourceBuilder source = searchSource()
                .query(termQuery("field1", "test"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field1").order("score").preTags("<xxx>").postTags("</xxx>"));

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field1").fragments()[0].string(), equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field1");
        source = searchSource()
                .query(termQuery("_all", "test"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field1").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field1").fragments()[0].string(), equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(termQuery("_all", "quick"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(prefixQuery("_all", "qui"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));

        logger.info("--> searching on _all with constant score, highlighting on field2");
        source = searchSource()
                .query(constantScoreQuery(prefixQuery("_all", "qui")))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));

        logger.info("--> searching on _all with constant score, highlighting on field2");
        source = searchSource()
                .query(boolQuery().should(constantScoreQuery(prefixQuery("_all", "qui"))))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2").order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
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

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field1").fragments()[0].string(), equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field1");
        source = searchSource()
                .query(termQuery("_all", "test"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field1", 100, 0).order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field1").fragments()[0].string(), equalTo("this is a <xxx>test</xxx>"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(termQuery("_all", "quick"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2", 100, 0).order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));

        // LUCENE 3.1 UPGRADE: Caused adding the space at the end...
        assertThat(searchResponse.getHits().getAt(0).highlightFields().get("field2").fragments()[0].string(), equalTo("The <xxx>quick</xxx> brown fox jumps over the lazy dog"));

        logger.info("--> searching on _all, highlighting on field2");
        source = searchSource()
                .query(prefixQuery("_all", "qui"))
                .from(0).size(60).explain(true)
                .highlight(highlight().field("field2", 100, 0).order("score").preTags("<xxx>").postTags("</xxx>"));

        searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
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

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
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
                searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
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

        SearchResponse searchResponse = client().search(searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
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
                searchRequest("test").source(source).searchType(QUERY_THEN_FETCH).scroll(timeValueMinutes(10))).actionGet();
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
    public void testHighlightComplexPhraseQuery() throws Exception {
        prepareCreate("test")
            .setSettings(ImmutableSettings.builder()
                .put("analysis.analyzer.code.type", "custom")
                .put("analysis.analyzer.code.tokenizer", "code")
                .put("analysis.analyzer.code.filter", "code,lowercase")
                .put("analysis.tokenizer.code.type", "pattern")
                .put("analysis.tokenizer.code.pattern", "[.,:;/\"<>(){}\\[\\]\\s]")
                .put("analysis.filter.code.type", "word_delimiter")
                .put("analysis.filter.code.generate_word_parts", "true")
                .put("analysis.filter.code.generate_number_parts", "true")
                .put("analysis.filter.code.catenate_words", "false")
                .put("analysis.filter.code.catenate_numbers", "false")
                .put("analysis.filter.code.catenate_all", "false")
                .put("analysis.filter.code.split_on_case_change", "true")
                .put("analysis.filter.code.preserve_original", "true")
                .put("analysis.filter.code.split_on_numerics", "true")
                .put("analysis.filter.code.stem_english_possessive", "false")
                .build())
            .addMapping("type", jsonBuilder()
                    .startObject()
                        .startObject("type")
                            .startObject("properties")
                                .startObject("text")
                                    .field("type", "string")
                                    .field("analyzer", "code")
                                    .field("term_vector", "with_positions_offsets")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject())
                .execute().actionGet();

        ensureGreen();
        client().prepareIndex("test", "type", "1")
            .setSource(jsonBuilder().startObject()
                    .field("text", "def log_worker_status( worker )\n  pass")
                .endObject())
            .setRefresh(true)
            .execute().actionGet();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchPhraseQuery("text", "def log_worker_status( worker )"))
                .addHighlightedField("text").execute().actionGet();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(1L));
        assertThat(response.getHits().getAt(0).getHighlightFields().get("text").fragments()[0].string(), equalTo("<em>def log_worker_status( worker</em> )\n  pass"));
    }

}
