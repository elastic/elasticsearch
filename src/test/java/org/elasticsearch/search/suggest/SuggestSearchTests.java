/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.suggest;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.suggest.SuggestBuilder.SuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder.DirectCandidateGenerator;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.search.suggest.SuggestBuilder.phraseSuggestion;
import static org.elasticsearch.search.suggest.SuggestBuilder.termSuggestion;
import static org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder.candidateGenerator;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration tests for term and phrase suggestions.  Many of these tests many requests that vary only slightly from one another.  Where
 * possible these tests should declare for the first request, make the request, modify the configuration for the next request, make that
 * request, modify again, request again, etc.  This makes it very obvious what changes between requests.
 */
public class SuggestSearchTests extends ElasticsearchIntegrationTest {
    @Test // see #3037
    public void testSuggestModes() throws IOException {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.analysis.analyzer.biword.tokenizer", "standard")
                .putArray("index.analysis.analyzer.biword.filter", "shingler", "lowercase")
                .put("index.analysis.filter.shingler.type", "shingle")
                .put("index.analysis.filter.shingler.min_shingle_size", 2)
                .put("index.analysis.filter.shingler.max_shingle_size", 3));
        
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties")
                .startObject("name")
                    .field("type", "multi_field")
                    .field("path", "just_name")
                    .startObject("fields")
                        .startObject("name")
                            .field("type", "string")
                        .endObject()
                        .startObject("name_shingled")
                            .field("type", "string")
                            .field("index_analyzer", "biword")
                            .field("search_analyzer", "standard")
                        .endObject()
                    .endObject()
                .endObject()
                .endObject()
                .endObject().endObject();
        assertAcked(builder.addMapping("type1", mapping));
        ensureGreen();
        

        index("test", "type1", "1", "name", "I like iced tea");
        index("test", "type1", "2", "name", "I like tea.");
        index("test", "type1", "3", "name", "I like ice cream.");
        refresh();

        DirectCandidateGenerator generator = candidateGenerator("name").prefixLength(0).minWordLength(0).suggestMode("always").maxEdits(2);
        PhraseSuggestionBuilder phraseSuggestion = phraseSuggestion("did_you_mean").field("name_shingled")
                .addCandidateGenerator(generator)
                .gramSize(3);
        Suggest searchSuggest = searchSuggest(client(), "ice tea", phraseSuggestion);
        assertSuggestion(searchSuggest, 0, "did_you_mean", "iced tea");

        generator.suggestMode(null);
        searchSuggest = searchSuggest(client(), "ice tea", phraseSuggestion);
        assertSuggestionSize(searchSuggest, 0, 0, "did_you_mean");
    }
    
    @Test // see #2729
    public void testSizeOneShard() throws Exception {
        prepareCreate("test").setSettings(
                SETTING_NUMBER_OF_SHARDS, 1,
                SETTING_NUMBER_OF_REPLICAS, 0).get();
        ensureGreen();

        for (int i = 0; i < 15; i++) {
            index("test", "type1", Integer.toString(i), "text", "abc" + i);
        }
        refresh();

        SearchResponse search = client().prepareSearch().setQuery(matchQuery("text", "spellchecker")).get();
        assertThat("didn't ask for suggestions but got some", search.getSuggest(), nullValue());
        
        TermSuggestionBuilder termSuggestion = termSuggestion("test")
                .suggestMode("always") // Always, otherwise the results can vary between requests.
                .text("abcd")
                .field("text")
                .size(10);
        Suggest suggest = searchSuggest(client(), termSuggestion);
        assertSuggestion(suggest, 0, "test", 10, "abc0");

        termSuggestion.text("abcd").shardSize(5);
        suggest = searchSuggest(client(), termSuggestion);
        assertSuggestion(suggest, 0, "test", 5, "abc0");
    }
    
    @Test
    public void testUnmappedField() throws IOException, InterruptedException, ExecutionException {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
                .put(SETTING_NUMBER_OF_SHARDS, between(1,5))
                .put(SETTING_NUMBER_OF_REPLICAS, between(0, cluster().size() - 1))
                .put("index.analysis.analyzer.biword.tokenizer", "standard")
                .putArray("index.analysis.analyzer.biword.filter", "shingler", "lowercase")
                .put("index.analysis.filter.shingler.type", "shingle")
                .put("index.analysis.filter.shingler.min_shingle_size", 2)
                .put("index.analysis.filter.shingler.max_shingle_size", 3));
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties")
                .startObject("name")
                    .field("type", "multi_field")
                    .field("path", "just_name")
                    .startObject("fields")
                        .startObject("name")
                            .field("type", "string")
                        .endObject()
                        .startObject("name_shingled")
                            .field("type", "string")
                            .field("index_analyzer", "biword")
                            .field("search_analyzer", "standard")
                        .endObject()
                    .endObject()
                .endObject()
                .endObject()
                .endObject().endObject();
        assertAcked(builder.addMapping("type1", mapping));
        ensureGreen();

        indexRandom(true, client().prepareIndex("test", "type1").setSource("name", "I like iced tea"),
        client().prepareIndex("test", "type1").setSource("name", "I like tea."),
        client().prepareIndex("test", "type1").setSource("name", "I like ice cream."));
        refresh();

        PhraseSuggestionBuilder phraseSuggestion = phraseSuggestion("did_you_mean").field("name_shingled")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("name").prefixLength(0).minWordLength(0).suggestMode("always").maxEdits(2))
                .gramSize(3);
        Suggest searchSuggest = searchSuggest(client(), "ice tea", phraseSuggestion);
        assertSuggestion(searchSuggest, 0, 0, "did_you_mean", "iced tea");

        phraseSuggestion.field("nosuchField");
        {
            SearchRequestBuilder suggestBuilder = client().prepareSearch().setSearchType(SearchType.COUNT);
            suggestBuilder.setSuggestText("tetsting sugestion");
            suggestBuilder.addSuggestion(phraseSuggestion);
            assertThrows(suggestBuilder, SearchPhaseExecutionException.class);
        }
        {
            SearchRequestBuilder suggestBuilder = client().prepareSearch().setSearchType(SearchType.COUNT);
            suggestBuilder.setSuggestText("tetsting sugestion");
            suggestBuilder.addSuggestion(phraseSuggestion);
            assertThrows(suggestBuilder, SearchPhaseExecutionException.class);
        }
    }

    @Test
    public void testSimple() throws Exception {
        prepareCreate("test").setSettings(
                SETTING_NUMBER_OF_SHARDS, 1,
                SETTING_NUMBER_OF_REPLICAS, 0).get();
        ensureGreen();

        index("test", "type1", "1", "text", "abcd");
        index("test", "type1", "2", "text", "aacd");
        index("test", "type1", "3", "text", "abbd");
        index("test", "type1", "4", "text", "abcc");
        refresh();
        
        SearchResponse search = client().prepareSearch().setQuery(matchQuery("text", "spellcecker")).get();
        assertThat("didn't ask for suggestions but got some", search.getSuggest(), nullValue());
        
        TermSuggestionBuilder termSuggest = termSuggestion("test")
                .suggestMode("always") // Always, otherwise the results can vary between requests.
                .text("abcd")
                .field("text");
        Suggest suggest = searchSuggest(client(), termSuggest);
        assertSuggestion(suggest, 0, "test", "aacd", "abbd", "abcc");
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getText().string(), equalTo("abcd"));

        suggest = searchSuggest(client(), termSuggest);
        assertSuggestion(suggest, 0, "test", "aacd","abbd", "abcc");
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getText().string(), equalTo("abcd"));
    }

    @Test
    public void testEmpty() throws Exception {
        prepareCreate("test").setSettings(
                SETTING_NUMBER_OF_SHARDS, 5,
                SETTING_NUMBER_OF_REPLICAS, 0).get();
        ensureGreen();

        index("test", "type1", "1", "foo", "bar");
        refresh();

        TermSuggestionBuilder termSuggest = termSuggestion("test")
                .suggestMode("always") // Always, otherwise the results can vary between requests.
                .text("abcd")
                .field("text");
        Suggest suggest = searchSuggest(client(), termSuggest);
        assertSuggestionSize(suggest, 0, 0, "test");
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getText().string(), equalTo("abcd"));

        suggest = searchSuggest(client(), termSuggest);
        assertSuggestionSize(suggest, 0, 0, "test");
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getText().string(), equalTo("abcd"));
    }

    @Test
    public void testWithMultipleCommands() throws Exception {
        prepareCreate("test").setSettings(
                SETTING_NUMBER_OF_SHARDS, 5,
                SETTING_NUMBER_OF_REPLICAS, 0).get();
        ensureGreen();

        index("test", "typ1", "1", "field1", "prefix_abcd", "field2", "prefix_efgh");
        index("test", "typ1", "2", "field1", "prefix_aacd", "field2", "prefix_eeeh");
        index("test", "typ1", "3", "field1", "prefix_abbd", "field2", "prefix_efff");
        index("test", "typ1", "4", "field1", "prefix_abcc", "field2", "prefix_eggg");
        refresh();

        Suggest suggest = searchSuggest(client(),
                termSuggestion("size1")
                        .size(1).text("prefix_abcd").maxTermFreq(10).prefixLength(1).minDocFreq(0)
                        .field("field1").suggestMode("always"),
                termSuggestion("field2")
                        .field("field2").text("prefix_eeeh prefix_efgh")
                        .maxTermFreq(10).minDocFreq(0).suggestMode("always"),
                termSuggestion("accuracy")
                        .field("field2").text("prefix_efgh").setAccuracy(1f)
                        .maxTermFreq(10).minDocFreq(0).suggestMode("always"));
        assertSuggestion(suggest, 0, "size1", "prefix_aacd");
        assertThat(suggest.getSuggestion("field2").getEntries().get(0).getText().string(), equalTo("prefix_eeeh"));
        assertSuggestion(suggest, 0, "field2", "prefix_efgh");
        assertThat(suggest.getSuggestion("field2").getEntries().get(1).getText().string(), equalTo("prefix_efgh"));
        assertSuggestion(suggest, 1, "field2", "prefix_eeeh", "prefix_efff", "prefix_eggg");
        assertSuggestionSize(suggest, 0, 0, "accuracy");
    }

    @Test
    public void testSizeAndSort() throws Exception {
        prepareCreate("test").setSettings(
                SETTING_NUMBER_OF_SHARDS, 5,
                SETTING_NUMBER_OF_REPLICAS, 0).get();
        ensureGreen();

        Map<String, Integer> termsAndDocCount = new HashMap<String, Integer>();
        termsAndDocCount.put("prefix_aaad", 20);
        termsAndDocCount.put("prefix_abbb", 18);
        termsAndDocCount.put("prefix_aaca", 16);
        termsAndDocCount.put("prefix_abba", 14);
        termsAndDocCount.put("prefix_accc", 12);
        termsAndDocCount.put("prefix_addd", 10);
        termsAndDocCount.put("prefix_abaa", 8);
        termsAndDocCount.put("prefix_dbca", 6);
        termsAndDocCount.put("prefix_cbad", 4);
        termsAndDocCount.put("prefix_aacd", 1);
        termsAndDocCount.put("prefix_abcc", 1);
        termsAndDocCount.put("prefix_accd", 1);

        for (Map.Entry<String, Integer> entry : termsAndDocCount.entrySet()) {
            for (int i = 0; i < entry.getValue(); i++) {
                index("test", "type1", entry.getKey() + i, "field1", entry.getKey());
            }
        }
        refresh();

        Suggest suggest = searchSuggest(client(), "prefix_abcd",
                termSuggestion("size3SortScoreFirst")
                        .size(3).minDocFreq(0).field("field1").suggestMode("always"),
                termSuggestion("size10SortScoreFirst")
                        .size(10).minDocFreq(0).field("field1").suggestMode("always").shardSize(50),
                termSuggestion("size3SortScoreFirstMaxEdits1")
                        .maxEdits(1)
                        .size(10).minDocFreq(0).field("field1").suggestMode("always"),
                termSuggestion("size10SortFrequencyFirst")
                        .size(10).sort("frequency").shardSize(1000)
                        .minDocFreq(0).field("field1").suggestMode("always"));

        // The commented out assertions fail sometimes because suggestions are based off of shard frequencies instead of index frequencies.
        assertSuggestion(suggest, 0, "size3SortScoreFirst", "prefix_aacd", "prefix_abcc", "prefix_accd");
        assertSuggestion(suggest, 0, "size10SortScoreFirst", 10, "prefix_aacd", "prefix_abcc", "prefix_accd" /*, "prefix_aaad" */);
        assertSuggestion(suggest, 0, "size3SortScoreFirstMaxEdits1", "prefix_aacd", "prefix_abcc", "prefix_accd");
        assertSuggestion(suggest, 0, "size10SortFrequencyFirst", "prefix_aaad", "prefix_abbb", "prefix_aaca", "prefix_abba",
                "prefix_accc", "prefix_addd", "prefix_abaa", "prefix_dbca", "prefix_cbad", "prefix_aacd");

        // assertThat(suggest.get(3).getSuggestedWords().get("prefix_abcd").get(4).getTerm(), equalTo("prefix_abcc"));
        // assertThat(suggest.get(3).getSuggestedWords().get("prefix_abcd").get(4).getTerm(), equalTo("prefix_accd"));
    }
    
    @Test // see #2817
    public void testStopwordsOnlyPhraseSuggest() throws ElasticSearchException, IOException {
        prepareCreate("test").setSettings(
                SETTING_NUMBER_OF_SHARDS, 1,
                SETTING_NUMBER_OF_REPLICAS, 0).get();
        ensureGreen();

        index("test", "typ1", "1", "body", "this is a test");
        refresh();

        Suggest searchSuggest = searchSuggest(client(), "a an the",
                phraseSuggestion("simple_phrase").field("body").gramSize(1)
                        .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                        .size(1));
        assertSuggestionSize(searchSuggest, 0, 0, "simple_phrase");
    }
    
    @Test
    public void testPrefixLength() throws ElasticSearchException, IOException {  // Stopped here
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.analysis.analyzer.reverse.tokenizer", "standard")
                .putArray("index.analysis.analyzer.reverse.filter", "lowercase", "reverse")
                .put("index.analysis.analyzer.body.tokenizer", "standard")
                .putArray("index.analysis.analyzer.body.filter", "lowercase")
                .put("index.analysis.analyzer.bigram.tokenizer", "standard")
                .putArray("index.analysis.analyzer.bigram.filter", "my_shingle", "lowercase")
                .put("index.analysis.filter.my_shingle.type", "shingle")
                .put("index.analysis.filter.my_shingle.output_unigrams", false)
                .put("index.analysis.filter.my_shingle.min_shingle_size", 2)
                .put("index.analysis.filter.my_shingle.max_shingle_size", 2));
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("_all").field("store", "yes").field("termVector", "with_positions_offsets").endObject()
                .startObject("properties")
                .startObject("body").field("type", "string").field("analyzer", "body").endObject()
                .startObject("body_reverse").field("type", "string").field("analyzer", "reverse").endObject()
                .startObject("bigram").field("type", "string").field("analyzer", "bigram").endObject()
                .endObject()
                .endObject().endObject();
        assertAcked(builder.addMapping("type1", mapping));
        ensureGreen();

        index("test", "type1", "1", "body", "hello world");
        index("test", "type1", "2", "body", "hello world");
        index("test", "type1", "3", "body", "hello words");
        refresh();

        Suggest searchSuggest = searchSuggest(client(), "hello word",
                phraseSuggestion("simple_phrase").field("body")
                        .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").prefixLength(4).minWordLength(1).suggestMode("always"))
                        .size(1).confidence(1.0f));
        assertSuggestion(searchSuggest, 0, "simple_phrase", "hello words");
        
        searchSuggest = searchSuggest(client(), "hello word",
                phraseSuggestion("simple_phrase").field("body")
                        .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").prefixLength(2).minWordLength(1).suggestMode("always"))
                        .size(1).confidence(1.0f));
        assertSuggestion(searchSuggest, 0, "simple_phrase", "hello world");
    }
    
    
    @Test
    @Slow
    public void testMarvelHerosPhraseSuggest() throws ElasticSearchException, IOException {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
                .put("index.analysis.analyzer.reverse.tokenizer", "standard")
                .putArray("index.analysis.analyzer.reverse.filter", "lowercase", "reverse")
                .put("index.analysis.analyzer.body.tokenizer", "standard")
                .putArray("index.analysis.analyzer.body.filter", "lowercase")
                .put("index.analysis.analyzer.bigram.tokenizer", "standard")
                .putArray("index.analysis.analyzer.bigram.filter", "my_shingle", "lowercase")
                .put("index.analysis.filter.my_shingle.type", "shingle")
                .put("index.analysis.filter.my_shingle.output_unigrams", false)
                .put("index.analysis.filter.my_shingle.min_shingle_size", 2)
                .put("index.analysis.filter.my_shingle.max_shingle_size", 2));
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("_all")
                        .field("store", "yes")
                        .field("termVector", "with_positions_offsets")
                    .endObject()
                    .startObject("properties")
                        .startObject("body").
                            field("type", "string").
                            field("analyzer", "body")
                        .endObject()
                        .startObject("body_reverse").
                            field("type", "string").
                            field("analyzer", "reverse")
                         .endObject()
                         .startObject("bigram").
                             field("type", "string").
                             field("analyzer", "bigram")
                         .endObject()
                     .endObject()
                .endObject().endObject();
        ElasticsearchAssertions.assertAcked(builder.addMapping("type1", mapping));
        ensureGreen();

        for (String line: Resources.readLines(SuggestSearchTests.class.getResource("/config/names.txt"), Charsets.UTF_8)) {
            index("test", "type1", line, "body", line, "body_reverse", line, "bigram", line);
        }
        refresh();

        PhraseSuggestionBuilder phraseSuggest = phraseSuggestion("simple_phrase")
                .field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .size(1);
        Suggest searchSuggest = searchSuggest(client(), "american ame", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "american ace");
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("american ame"));

        phraseSuggest.realWordErrorLikelihood(0.95f);
        searchSuggest = searchSuggest(client(), "Xor the Got-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");
        // Check the "text" field this one time.
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel"));

        // Ask for highlighting
        phraseSuggest.highlight("<em>", "</em>");
        searchSuggest = searchSuggest(client(), "Xor the Got-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getHighlighted().string(), equalTo("<em>xorr</em> the <em>god</em> jewel"));

        // pass in a correct phrase
        phraseSuggest.highlight(null, null).confidence(0f).size(1).maxErrors(0.5f);
        searchSuggest = searchSuggest(client(), "Xorr the God-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");

        // pass in a correct phrase - set confidence to 2
        phraseSuggest.confidence(2f);
        searchSuggest = searchSuggest(client(), "Xorr the God-Jewel", phraseSuggest);
        assertSuggestionSize(searchSuggest, 0, 0, "simple_phrase");

        // pass in a correct phrase - set confidence to 0.99
        phraseSuggest.confidence(0.99f);
        searchSuggest = searchSuggest(client(), "Xorr the God-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");

        //test reverse suggestions with pre & post filter
        phraseSuggest
            .addCandidateGenerator(candidateGenerator("body").minWordLength(1).suggestMode("always"))
            .addCandidateGenerator(candidateGenerator("body_reverse").minWordLength(1).suggestMode("always").preFilter("reverse").postFilter("reverse"));
        searchSuggest = searchSuggest(client(), "xor the yod-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");

        // set all mass to trigrams (not indexed)
        phraseSuggest.clearCandidateGenerators()
            .addCandidateGenerator(candidateGenerator("body").minWordLength(1).suggestMode("always"))
            .smoothingModel(new PhraseSuggestionBuilder.LinearInterpolation(1,0,0));
        searchSuggest = searchSuggest(client(), "Xor the Got-Jewel", phraseSuggest);
        assertSuggestionSize(searchSuggest, 0, 0, "simple_phrase");

        // set all mass to bigrams
        phraseSuggest.smoothingModel(new PhraseSuggestionBuilder.LinearInterpolation(0,1,0));
        searchSuggest =  searchSuggest(client(), "Xor the Got-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");

        // distribute mass
        phraseSuggest.smoothingModel(new PhraseSuggestionBuilder.LinearInterpolation(0.4,0.4,0.2));
        searchSuggest = searchSuggest(client(), "Xor the Got-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");

        searchSuggest = searchSuggest(client(), "american ame", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "american ace");
        
        // try all smoothing methods
        phraseSuggest.smoothingModel(new PhraseSuggestionBuilder.LinearInterpolation(0.4,0.4,0.2));
        searchSuggest = searchSuggest(client(), "Xor the Got-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");

        phraseSuggest.smoothingModel(new PhraseSuggestionBuilder.Laplace(0.2));
        searchSuggest = searchSuggest(client(), "Xor the Got-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");

        phraseSuggest.smoothingModel(new PhraseSuggestionBuilder.StupidBackoff(0.1));
        searchSuggest = searchSuggest(client(), "Xor the Got-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");

        // check tokenLimit
        phraseSuggest.smoothingModel(null).tokenLimit(4);
        searchSuggest = searchSuggest(client(), "Xor the Got-Jewel", phraseSuggest);
        assertSuggestionSize(searchSuggest, 0, 0, "simple_phrase");

        phraseSuggest.tokenLimit(15).smoothingModel(new PhraseSuggestionBuilder.StupidBackoff(0.1));
        searchSuggest = searchSuggest(client(), "Xor the Got-Jewel Xor the Got-Jewel Xor the Got-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel xorr the god jewel xorr the god jewel");
        // Check the name this time because we're repeating it which is funky
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel Xor the Got-Jewel Xor the Got-Jewel"));
    }
    
    @Test
    public void testSizePararm() throws IOException {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put("index.analysis.analyzer.reverse.tokenizer", "standard")
                .putArray("index.analysis.analyzer.reverse.filter", "lowercase", "reverse")
                .put("index.analysis.analyzer.body.tokenizer", "standard")
                .putArray("index.analysis.analyzer.body.filter", "lowercase")
                .put("index.analysis.analyzer.bigram.tokenizer", "standard")
                .putArray("index.analysis.analyzer.bigram.filter", "my_shingle", "lowercase")
                .put("index.analysis.filter.my_shingle.type", "shingle")
                .put("index.analysis.filter.my_shingle.output_unigrams", false)
                .put("index.analysis.filter.my_shingle.min_shingle_size", 2)
                .put("index.analysis.filter.my_shingle.max_shingle_size", 2));
        
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("type1")
                        .startObject("_all")
                            .field("store", "yes")
                            .field("termVector", "with_positions_offsets")
                        .endObject()
                        .startObject("properties")
                            .startObject("body")
                                .field("type", "string")
                                .field("analyzer", "body")
                            .endObject()
                         .startObject("body_reverse")
                             .field("type", "string")
                             .field("analyzer", "reverse")
                         .endObject()
                         .startObject("bigram")
                             .field("type", "string")
                             .field("analyzer", "bigram")
                         .endObject()
                     .endObject()
                 .endObject()
             .endObject();
        assertAcked(builder.addMapping("type1", mapping));
        ensureGreen();

        String line = "xorr the god jewel";
        index("test", "type1", "1", "body", line, "body_reverse", line, "bigram", line);
        line = "I got it this time";
        index("test", "type1", "2", "body", line, "body_reverse", line, "bigram", line);
        refresh();

        PhraseSuggestionBuilder phraseSuggestion = phraseSuggestion("simple_phrase")
                .realWordErrorLikelihood(0.95f)
                .field("bigram")
                .gramSize(2)
                .analyzer("body")
                .addCandidateGenerator(candidateGenerator("body").minWordLength(1).prefixLength(1).suggestMode("always").size(1).accuracy(0.1f))
                .smoothingModel(new PhraseSuggestionBuilder.StupidBackoff(0.1))
                .maxErrors(1.0f)
                .size(5);
        Suggest searchSuggest = searchSuggest(client(), "Xorr the Gut-Jewel", phraseSuggestion);
        assertSuggestionSize(searchSuggest, 0, 0, "simple_phrase");

        // we allow a size of 2 now on the shard generator level so "god" will be found since it's LD2
        phraseSuggestion.clearCandidateGenerators()
                .addCandidateGenerator(candidateGenerator("body").minWordLength(1).prefixLength(1).suggestMode("always").size(2).accuracy(0.1f));
        searchSuggest = searchSuggest(client(), "Xorr the Gut-Jewel", phraseSuggestion);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");
    }
    
    @Test
    public void testPhraseBoundaryCases() throws ElasticSearchException, IOException {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
                .put("index.analysis.analyzer.body.tokenizer", "standard")
                .putArray("index.analysis.analyzer.body.filter", "lowercase")
                .put("index.analysis.analyzer.bigram.tokenizer", "standard")
                .putArray("index.analysis.analyzer.bigram.filter", "my_shingle", "lowercase")
                .put("index.analysis.analyzer.ngram.tokenizer", "standard")
                .putArray("index.analysis.analyzer.ngram.filter", "my_shingle2", "lowercase")
                .put("index.analysis.analyzer.myDefAnalyzer.tokenizer", "standard")
                .putArray("index.analysis.analyzer.myDefAnalyzer.filter", "shingle", "lowercase")
                .put("index.analysis.filter.my_shingle.type", "shingle")
                .put("index.analysis.filter.my_shingle.output_unigrams", false)
                .put("index.analysis.filter.my_shingle.min_shingle_size", 2)
                .put("index.analysis.filter.my_shingle.max_shingle_size", 2)
                .put("index.analysis.filter.my_shingle2.type", "shingle")
                .put("index.analysis.filter.my_shingle2.output_unigrams", true)
                .put("index.analysis.filter.my_shingle2.min_shingle_size", 2)
                .put("index.analysis.filter.my_shingle2.max_shingle_size", 2));
        
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                    .startObject().startObject("type1")
                    .startObject("_all").field("store", "yes").field("termVector", "with_positions_offsets").endObject()
                .startObject("properties")
                .startObject("body").field("type", "string").field("analyzer", "body").endObject()
                .startObject("bigram").field("type", "string").field("analyzer", "bigram").endObject()
                .startObject("ngram").field("type", "string").field("analyzer", "ngram").endObject()
                .endObject()
                .endObject().endObject();
        assertAcked(builder.addMapping("type1", mapping));
        ensureGreen();

        for (String line: Resources.readLines(SuggestSearchTests.class.getResource("/config/names.txt"), Charsets.UTF_8)) {
            index("test", "type1", line, "body", line, "bigram", line, "ngram", line);
        }
        refresh();

        // Lets make sure some things throw exceptions
        PhraseSuggestionBuilder phraseSuggestion = phraseSuggestion("simple_phrase")
                .field("bigram")
                .analyzer("body")
                .addCandidateGenerator(candidateGenerator("does_not_exist").minWordLength(1).suggestMode("always"))
                .realWordErrorLikelihood(0.95f)
                .maxErrors(0.5f)
                .size(1);
        try {
            searchSuggest(client(), "Xor the Got-Jewel", 5, phraseSuggestion);
            assert false : "field does not exists";
        } catch (SearchPhaseExecutionException e) {}

        phraseSuggestion.clearCandidateGenerators().analyzer(null);
        try {
            searchSuggest(client(), "Xor the Got-Jewel", 5, phraseSuggestion);
            assert false : "analyzer does only produce ngrams";
        } catch (SearchPhaseExecutionException e) {
        }

        phraseSuggestion.analyzer("bigram");
        try {
            searchSuggest(client(), "Xor the Got-Jewel", 5, phraseSuggestion);
            assert false : "analyzer does only produce ngrams";
        } catch (SearchPhaseExecutionException e) {
        }

        // Now we'll make sure some things don't
        phraseSuggestion.forceUnigrams(false);
        searchSuggest(client(), "Xor the Got-Jewel", phraseSuggestion);

        // Field doesn't produce unigrams but the analyzer does
        phraseSuggestion.forceUnigrams(true).field("bigram").analyzer("ngram");
        searchSuggest(client(), "Xor the Got-Jewel",
                phraseSuggestion);

        phraseSuggestion.field("ngram").analyzer("myDefAnalyzer")
                .addCandidateGenerator(candidateGenerator("body").minWordLength(1).suggestMode("always"));
        Suggest suggest = searchSuggest(client(), "Xor the Got-Jewel", phraseSuggestion);
        assertSuggestion(suggest, 0, "simple_phrase", "xorr the god jewel");

        phraseSuggestion.analyzer(null);
        suggest = searchSuggest(client(), "Xor the Got-Jewel", phraseSuggestion);
        assertSuggestion(suggest, 0, "simple_phrase", "xorr the god jewel");
    }

    @Test
    public void testDifferentShardSize() throws Exception {
        prepareCreate("text").setSettings(settingsBuilder()
                .put(SETTING_NUMBER_OF_SHARDS, 5)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)).get();
        ensureGreen();
        indexRandom(true, client().prepareIndex("text", "type1", "1").setSource("field1", "foobar1").setRouting("1"),
                client().prepareIndex("text", "type1", "2").setSource("field1", "foobar2").setRouting("2"),
                client().prepareIndex("text", "type1", "3").setSource("field1", "foobar3").setRouting("3"));

        Suggest suggest = searchSuggest(client(), "foobar",
                termSuggestion("simple")
                        .size(10).minDocFreq(0).field("field1").suggestMode("always"));
        ElasticsearchAssertions.assertSuggestionSize(suggest, 0, 3, "simple");
    }

    @Test // see #3469
    public void testShardFailures() throws IOException, InterruptedException {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
                .put(SETTING_NUMBER_OF_SHARDS, between(1, 5))
                .put(SETTING_NUMBER_OF_REPLICAS, between(0, cluster().size() - 1))
                .put("index.analysis.analyzer.suggest.tokenizer", "standard")
                .putArray("index.analysis.analyzer.suggest.filter", "standard", "lowercase", "shingler")
                .put("index.analysis.filter.shingler.type", "shingle")
                .put("index.analysis.filter.shingler.min_shingle_size", 2)
                .put("index.analysis.filter.shingler.max_shingle_size", 5)
                .put("index.analysis.filter.shingler.output_unigrams", true));
        
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties")
                    .startObject("name")
                        .field("type", "multi_field")
                        .field("path", "just_name")
                        .startObject("fields")
                            .startObject("name")
                                .field("type", "string")
                                .field("analyzer", "suggest")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject();
        assertAcked(builder.addMapping("type1", mapping));
        ensureGreen();

        index("test", "type2", "1", "foo", "bar");
        index("test", "type2", "2", "foo", "bar");
        index("test", "type2", "3", "foo", "bar");
        index("test", "type2", "4", "foo", "bar");
        index("test", "type2", "5", "foo", "bar");
        index("test", "type2", "1", "name", "Just testing the suggestions api");
        index("test", "type2", "2", "name", "An other title about equal length");
        // Note that the last document has to have about the same length as the other or cutoff rechecking will remove the useful suggestion.
        refresh();

        // When searching on a shard with a non existing mapping, we should fail
        SearchRequestBuilder request = client().prepareSearch().setSearchType(SearchType.COUNT)
            .setSuggestText("tetsting sugestion")
            .addSuggestion(phraseSuggestion("did_you_mean").field("fielddoesnotexist").maxErrors(5.0f));
        assertThrows(request, SearchPhaseExecutionException.class);

        // When searching on a shard which does not hold yet any document of an existing type, we should not fail
        SearchResponse searchResponse = client().prepareSearch().setSearchType(SearchType.COUNT)
            .setSuggestText("tetsting sugestion")
            .addSuggestion(phraseSuggestion("did_you_mean").field("name").maxErrors(5.0f))
            .get();
        ElasticsearchAssertions.assertNoFailures(searchResponse);
        ElasticsearchAssertions.assertSuggestion(searchResponse.getSuggest(), 0, 0, "did_you_mean", "testing suggestions");
    }

    @Test // see #3469
    public void testEmptyShards() throws IOException, InterruptedException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder().
                startObject().
                    startObject("type1").
                        startObject("properties").
                            startObject("name").
                                field("type", "multi_field").
                                field("path", "just_name").
                                startObject("fields").
                                    startObject("name").
                                        field("type", "string").
                                        field("analyzer", "suggest").
                                    endObject().
                                endObject().
                            endObject().
                        endObject().
                    endObject().
                endObject();
        ElasticsearchAssertions.assertAcked(prepareCreate("test").setSettings(settingsBuilder()
                .put(SETTING_NUMBER_OF_SHARDS, 5)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.analysis.analyzer.suggest.tokenizer", "standard")
                .putArray("index.analysis.analyzer.suggest.filter", "standard", "lowercase", "shingler")
                .put("index.analysis.filter.shingler.type", "shingle")
                .put("index.analysis.filter.shingler.min_shingle_size", 2)
                .put("index.analysis.filter.shingler.max_shingle_size", 5)
                .put("index.analysis.filter.shingler.output_unigrams", true)).addMapping("type1", mappingBuilder));
        ensureGreen();

        index("test", "type2", "1", "foo", "bar");
        index("test", "type2", "2", "foo", "bar");
        index("test", "type1", "1", "name", "Just testing the suggestions api");
        index("test", "type1", "2", "name", "An other title about equal length");
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
                .setSearchType(SearchType.COUNT)
                .setSuggestText("tetsting sugestion")
                .addSuggestion(phraseSuggestion("did_you_mean").field("name").maxErrors(5.0f))
                .get();

        assertNoFailures(searchResponse);
        assertSuggestion(searchResponse.getSuggest(), 0, 0, "did_you_mean", "testing suggestions");
    }

    /**
     * Searching for a rare phrase shouldn't provide any suggestions if confidence > 1.  This was possible before we rechecked the cutoff
     * score during the reduce phase.  Failures don't occur every time - maybe two out of five tries but we don't repeat it to save time.
     */
    @Test
    public void testSearchForRarePhrase() throws ElasticSearchException, IOException {
        // If there isn't enough chaf per shard then shards can become unbalanced, making the cutoff recheck this is testing do more harm then good.
        int chafPerShard = 100;
        int numberOfShards = between(2, 5);

        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
                .put(SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(SETTING_NUMBER_OF_REPLICAS, between(0, cluster().size() - 1))
                .put("index.analysis.analyzer.body.tokenizer", "standard")
                .putArray("index.analysis.analyzer.body.filter", "lowercase", "my_shingle")
                .put("index.analysis.filter.my_shingle.type", "shingle")
                .put("index.analysis.filter.my_shingle.output_unigrams", true)
                .put("index.analysis.filter.my_shingle.min_shingle_size", 2)
                .put("index.analysis.filter.my_shingle.max_shingle_size", 2));

        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("type1")
                        .startObject("_all")
                            .field("store", "yes")
                            .field("termVector", "with_positions_offsets")
                        .endObject()
                        .startObject("properties")
                            .startObject("body")
                                .field("type", "string")
                                .field("analyzer", "body")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        assertAcked(builder.addMapping("type1", mapping));
        ensureGreen();

        List<String> phrases = new ArrayList<String>();
        Collections.addAll(phrases, "nobel prize", "noble gases", "somethingelse prize", "pride and joy", "notes are fun");
        for (int i = 0; i < 8; i++) {
            phrases.add("noble somethingelse" + i);
        }
        for (int i = 0; i < numberOfShards * chafPerShard; i++) {
            phrases.add("chaff" + i);
        }
        for (String phrase: phrases) {
            index("test", "type1", phrase, "body", phrase);
        }
        refresh();

        Suggest searchSuggest = searchSuggest(client(), "nobel prize", phraseSuggestion("simple_phrase")
                .field("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always").maxTermFreq(.99f))
                .confidence(2f)
                .maxErrors(5f)
                .size(1));
        assertSuggestionSize(searchSuggest, 0, 0, "simple_phrase");

        searchSuggest = searchSuggest(client(), "noble prize", phraseSuggestion("simple_phrase")
                .field("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always").maxTermFreq(.99f))
                .confidence(2f)
                .maxErrors(5f)
                .size(1));
        assertSuggestion(searchSuggest, 0, 0, "simple_phrase", "nobel prize");
    }

    protected Suggest searchSuggest(Client client, SuggestionBuilder<?>... suggestion) {
        return searchSuggest(client(), null, suggestion);
    }

    protected Suggest searchSuggest(Client client, String suggestText, SuggestionBuilder<?>... suggestions) {
        return searchSuggest(client(), suggestText, 0, suggestions);
    }

    protected Suggest searchSuggest(Client client, String suggestText, int expectShardsFailed, SuggestionBuilder<?>... suggestions) {
        SearchRequestBuilder builder = client().prepareSearch().setSearchType(SearchType.COUNT);
        if (suggestText != null) {
            builder.setSuggestText(suggestText);
        }
        for (SuggestionBuilder<?> suggestion : suggestions) {
            builder.addSuggestion(suggestion);
        }
        SearchResponse actionGet = builder.execute().actionGet();
        assertThat(Arrays.toString(actionGet.getShardFailures()), actionGet.getFailedShards(), equalTo(expectShardsFailed));
        return actionGet.getSuggest();
    }
}
