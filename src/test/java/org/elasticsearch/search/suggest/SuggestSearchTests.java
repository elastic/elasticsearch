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

package org.elasticsearch.search.suggest;

import com.carrotsearch.randomizedtesting.annotations.Nightly;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
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
import static org.elasticsearch.search.suggest.SuggestBuilders.phraseSuggestion;
import static org.elasticsearch.search.suggest.SuggestBuilders.termSuggestion;
import static org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder.candidateGenerator;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

/**
 * Integration tests for term and phrase suggestions.  Many of these tests many requests that vary only slightly from one another.  Where
 * possible these tests should declare for the first request, make the request, modify the configuration for the next request, make that
 * request, modify again, request again, etc.  This makes it very obvious what changes between requests.
 */
public class SuggestSearchTests extends ElasticsearchIntegrationTest {

    @Test // see #3196
    public void testSuggestAcrossMultipleIndices() throws IOException {
        createIndex("test");
        ensureGreen();

        index("test", "type1", "1", "text", "abcd");
        index("test", "type1", "2", "text", "aacd");
        index("test", "type1", "3", "text", "abbd");
        index("test", "type1", "4", "text", "abcc");
        refresh();

        TermSuggestionBuilder termSuggest = termSuggestion("test")
                .suggestMode("always") // Always, otherwise the results can vary between requests.
                .text("abcd")
                .field("text");
        logger.info("--> run suggestions with one index");
        searchSuggest( termSuggest);
        createIndex("test_1");
        ensureGreen();

        index("test_1", "type1", "1", "text", "ab cd");
        index("test_1", "type1", "2", "text", "aa cd");
        index("test_1", "type1", "3", "text", "ab bd");
        index("test_1", "type1", "4", "text", "ab cc");
        refresh();
        termSuggest = termSuggestion("test")
                .suggestMode("always") // Always, otherwise the results can vary between requests.
                .text("ab cd")
                .minWordLength(1)
                .field("text");
        logger.info("--> run suggestions with two indices");
        searchSuggest( termSuggest);


        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties")
                .startObject("text").field("type", "string").field("analyzer", "keyword").endObject()
                .endObject()
                .endObject().endObject();
        assertAcked(prepareCreate("test_2").addMapping("type1", mapping));
        ensureGreen();

        index("test_2", "type1", "1", "text", "ab cd");
        index("test_2", "type1", "2", "text", "aa cd");
        index("test_2", "type1", "3", "text", "ab bd");
        index("test_2", "type1", "4", "text", "ab cc");
        index("test_2", "type1", "1", "text", "abcd");
        index("test_2", "type1", "2", "text", "aacd");
        index("test_2", "type1", "3", "text", "abbd");
        index("test_2", "type1", "4", "text", "abcc");
        refresh();

        termSuggest = termSuggestion("test")
                .suggestMode("always") // Always, otherwise the results can vary between requests.
                .text("ab cd")
                .minWordLength(1)
                .field("text");
        logger.info("--> run suggestions with three indices");
        try {
            searchSuggest( termSuggest);
            fail(" can not suggest across multiple indices with different analysis chains");
        } catch (ReduceSearchPhaseException ex) {
            assertThat(ex.getCause(), instanceOf(ElasticsearchIllegalStateException.class));
            assertThat(ex.getCause().getMessage(),
                    anyOf(endsWith("Suggest entries have different sizes actual [1] expected [2]"),
                            endsWith("Suggest entries have different sizes actual [2] expected [1]")));
        } catch (ElasticsearchIllegalStateException ex) {
            assertThat(ex.getMessage(), anyOf(endsWith("Suggest entries have different sizes actual [1] expected [2]"),
                    endsWith("Suggest entries have different sizes actual [2] expected [1]")));
        }


        termSuggest = termSuggestion("test")
                .suggestMode("always") // Always, otherwise the results can vary between requests.
                .text("ABCD")
                .minWordLength(1)
                .field("text");
        logger.info("--> run suggestions with four indices");
        try {
            searchSuggest( termSuggest);
            fail(" can not suggest across multiple indices with different analysis chains");
        } catch (ReduceSearchPhaseException ex) {
            assertThat(ex.getCause(), instanceOf(ElasticsearchIllegalStateException.class));
            assertThat(ex.getCause().getMessage(), anyOf(endsWith("Suggest entries have different text actual [ABCD] expected [abcd]"),
                    endsWith("Suggest entries have different text actual [abcd] expected [ABCD]")));
        } catch (ElasticsearchIllegalStateException ex) {
            assertThat(ex.getMessage(), anyOf(endsWith("Suggest entries have different text actual [ABCD] expected [abcd]"),
                    endsWith("Suggest entries have different text actual [abcd] expected [ABCD]")));
        }
    }

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
        Suggest searchSuggest = searchSuggest( "ice tea", phraseSuggestion);
        assertSuggestion(searchSuggest, 0, "did_you_mean", "iced tea");

        generator.suggestMode(null);
        searchSuggest = searchSuggest( "ice tea", phraseSuggestion);
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
        Suggest suggest = searchSuggest( termSuggestion);
        assertSuggestion(suggest, 0, "test", 10, "abc0");

        termSuggestion.text("abcd").shardSize(5);
        suggest = searchSuggest( termSuggestion);
        assertSuggestion(suggest, 0, "test", 5, "abc0");
    }
    
    @Test
    public void testUnmappedField() throws IOException, InterruptedException, ExecutionException {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
                .put(indexSettings())
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
        Suggest searchSuggest = searchSuggest( "ice tea", phraseSuggestion);
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
        createIndex("test");
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
        Suggest suggest = searchSuggest( termSuggest);
        assertSuggestion(suggest, 0, "test", "aacd", "abbd", "abcc");
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getText().string(), equalTo("abcd"));

        suggest = searchSuggest( termSuggest);
        assertSuggestion(suggest, 0, "test", "aacd","abbd", "abcc");
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getText().string(), equalTo("abcd"));
    }

    @Test
    public void testEmpty() throws Exception {
        createIndex("test");
        ensureGreen();

        index("test", "type1", "1", "foo", "bar");
        refresh();

        TermSuggestionBuilder termSuggest = termSuggestion("test")
                .suggestMode("always") // Always, otherwise the results can vary between requests.
                .text("abcd")
                .field("text");
        Suggest suggest = searchSuggest( termSuggest);
        assertSuggestionSize(suggest, 0, 0, "test");
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getText().string(), equalTo("abcd"));

        suggest = searchSuggest( termSuggest);
        assertSuggestionSize(suggest, 0, 0, "test");
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getText().string(), equalTo("abcd"));
    }

    @Test
    public void testWithMultipleCommands() throws Exception {
        createIndex("test");
        ensureGreen();

        index("test", "typ1", "1", "field1", "prefix_abcd", "field2", "prefix_efgh");
        index("test", "typ1", "2", "field1", "prefix_aacd", "field2", "prefix_eeeh");
        index("test", "typ1", "3", "field1", "prefix_abbd", "field2", "prefix_efff");
        index("test", "typ1", "4", "field1", "prefix_abcc", "field2", "prefix_eggg");
        refresh();

        Suggest suggest = searchSuggest(
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
        createIndex("test");
        ensureGreen();

        Map<String, Integer> termsAndDocCount = new HashMap<>();
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

        Suggest suggest = searchSuggest( "prefix_abcd",
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
    public void testStopwordsOnlyPhraseSuggest() throws ElasticsearchException, IOException {
        assertAcked(prepareCreate("test").addMapping("typ1", "body", "type=string,analyzer=stopwd").setSettings(
                settingsBuilder()
                        .put("index.analysis.analyzer.stopwd.tokenizer", "whitespace")
                        .putArray("index.analysis.analyzer.stopwd.filter", "stop")
        ));
        ensureGreen();
        index("test", "typ1", "1", "body", "this is a test");
        refresh();

        Suggest searchSuggest = searchSuggest( "a an the",
                phraseSuggestion("simple_phrase").field("body").gramSize(1)
                        .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                        .size(1));
        assertSuggestionSize(searchSuggest, 0, 0, "simple_phrase");
    }
    
    @Test
    public void testPrefixLength() throws ElasticsearchException, IOException {  // Stopped here
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
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

        Suggest searchSuggest = searchSuggest( "hello word",
                phraseSuggestion("simple_phrase").field("body")
                        .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").prefixLength(4).minWordLength(1).suggestMode("always"))
                        .size(1).confidence(1.0f));
        assertSuggestion(searchSuggest, 0, "simple_phrase", "hello words");
        
        searchSuggest = searchSuggest( "hello word",
                phraseSuggestion("simple_phrase").field("body")
                        .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").prefixLength(2).minWordLength(1).suggestMode("always"))
                        .size(1).confidence(1.0f));
        assertSuggestion(searchSuggest, 0, "simple_phrase", "hello world");
    }
    
    @Test
    @Slow
    @Nightly
    public void testMarvelHerosPhraseSuggest() throws ElasticsearchException, IOException {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
                .put(indexSettings())
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
        assertAcked(builder.addMapping("type1", mapping));
        ensureGreen();

        for (String line: Resources.readLines(SuggestSearchTests.class.getResource("/config/names.txt"), Charsets.UTF_8)) {
            index("test", "type1", line, "body", line, "body_reverse", line, "bigram", line);
        }
        refresh();

        PhraseSuggestionBuilder phraseSuggest = phraseSuggestion("simple_phrase")
                .field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .size(1);
        Suggest searchSuggest = searchSuggest( "american ame", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "american ace");
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("american ame"));

        phraseSuggest.realWordErrorLikelihood(0.95f);
        searchSuggest = searchSuggest( "Xor the Got-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");
        // Check the "text" field this one time.
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel"));

        // Ask for highlighting
        phraseSuggest.highlight("<em>", "</em>");
        searchSuggest = searchSuggest( "Xor the Got-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getHighlighted().string(), equalTo("<em>xorr</em> the <em>god</em> jewel"));

        // pass in a correct phrase
        phraseSuggest.highlight(null, null).confidence(0f).size(1).maxErrors(0.5f);
        searchSuggest = searchSuggest( "Xorr the God-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");

        // pass in a correct phrase - set confidence to 2
        phraseSuggest.confidence(2f);
        searchSuggest = searchSuggest( "Xorr the God-Jewel", phraseSuggest);
        assertSuggestionSize(searchSuggest, 0, 0, "simple_phrase");

        // pass in a correct phrase - set confidence to 0.99
        phraseSuggest.confidence(0.99f);
        searchSuggest = searchSuggest( "Xorr the God-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");

        //test reverse suggestions with pre & post filter
        phraseSuggest
            .addCandidateGenerator(candidateGenerator("body").minWordLength(1).suggestMode("always"))
            .addCandidateGenerator(candidateGenerator("body_reverse").minWordLength(1).suggestMode("always").preFilter("reverse").postFilter("reverse"));
        searchSuggest = searchSuggest( "xor the yod-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");

        // set all mass to trigrams (not indexed)
        phraseSuggest.clearCandidateGenerators()
            .addCandidateGenerator(candidateGenerator("body").minWordLength(1).suggestMode("always"))
            .smoothingModel(new PhraseSuggestionBuilder.LinearInterpolation(1,0,0));
        searchSuggest = searchSuggest( "Xor the Got-Jewel", phraseSuggest);
        assertSuggestionSize(searchSuggest, 0, 0, "simple_phrase");

        // set all mass to bigrams
        phraseSuggest.smoothingModel(new PhraseSuggestionBuilder.LinearInterpolation(0,1,0));
        searchSuggest =  searchSuggest( "Xor the Got-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");

        // distribute mass
        phraseSuggest.smoothingModel(new PhraseSuggestionBuilder.LinearInterpolation(0.4,0.4,0.2));
        searchSuggest = searchSuggest( "Xor the Got-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");

        searchSuggest = searchSuggest( "american ame", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "american ace");
        
        // try all smoothing methods
        phraseSuggest.smoothingModel(new PhraseSuggestionBuilder.LinearInterpolation(0.4,0.4,0.2));
        searchSuggest = searchSuggest( "Xor the Got-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");

        phraseSuggest.smoothingModel(new PhraseSuggestionBuilder.Laplace(0.2));
        searchSuggest = searchSuggest( "Xor the Got-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");

        phraseSuggest.smoothingModel(new PhraseSuggestionBuilder.StupidBackoff(0.1));
        searchSuggest = searchSuggest( "Xor the Got-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");

        // check tokenLimit
        phraseSuggest.smoothingModel(null).tokenLimit(4);
        searchSuggest = searchSuggest( "Xor the Got-Jewel", phraseSuggest);
        assertSuggestionSize(searchSuggest, 0, 0, "simple_phrase");

        phraseSuggest.tokenLimit(15).smoothingModel(new PhraseSuggestionBuilder.StupidBackoff(0.1));
        searchSuggest = searchSuggest( "Xor the Got-Jewel Xor the Got-Jewel Xor the Got-Jewel", phraseSuggest);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel xorr the god jewel xorr the god jewel");
        // Check the name this time because we're repeating it which is funky
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel Xor the Got-Jewel Xor the Got-Jewel"));
    }
    
    @Test
    public void testSizePararm() throws IOException {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
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
        Suggest searchSuggest = searchSuggest( "Xorr the Gut-Jewel", phraseSuggestion);
        assertSuggestionSize(searchSuggest, 0, 0, "simple_phrase");

        // we allow a size of 2 now on the shard generator level so "god" will be found since it's LD2
        phraseSuggestion.clearCandidateGenerators()
                .addCandidateGenerator(candidateGenerator("body").minWordLength(1).prefixLength(1).suggestMode("always").size(2).accuracy(0.1f));
        searchSuggest = searchSuggest( "Xorr the Gut-Jewel", phraseSuggestion);
        assertSuggestion(searchSuggest, 0, "simple_phrase", "xorr the god jewel");
    }

    @Test
    @Nightly
    public void testPhraseBoundaryCases() throws ElasticsearchException, IOException {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
                .put(indexSettings()).put(SETTING_NUMBER_OF_SHARDS, 1) // to get reliable statistics we should put this all into one shard
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

        NumShards numShards = getNumShards("test");

        // Lets make sure some things throw exceptions
        PhraseSuggestionBuilder phraseSuggestion = phraseSuggestion("simple_phrase")
                .field("bigram")
                .analyzer("body")
                .addCandidateGenerator(candidateGenerator("does_not_exist").minWordLength(1).suggestMode("always"))
                .realWordErrorLikelihood(0.95f)
                .maxErrors(0.5f)
                .size(1);
        try {
            searchSuggest( "Xor the Got-Jewel", numShards.numPrimaries, phraseSuggestion);
            fail("field does not exists");
        } catch (SearchPhaseExecutionException e) {}

        phraseSuggestion.clearCandidateGenerators().analyzer(null);
        try {
            searchSuggest( "Xor the Got-Jewel", numShards.numPrimaries, phraseSuggestion);
            fail("analyzer does only produce ngrams");
        } catch (SearchPhaseExecutionException e) {
        }

        phraseSuggestion.analyzer("bigram");
        try {
            searchSuggest( "Xor the Got-Jewel", numShards.numPrimaries, phraseSuggestion);
            fail("analyzer does only produce ngrams");
        } catch (SearchPhaseExecutionException e) {
        }

        // Now we'll make sure some things don't
        phraseSuggestion.forceUnigrams(false);
        searchSuggest( "Xor the Got-Jewel", phraseSuggestion);

        // Field doesn't produce unigrams but the analyzer does
        phraseSuggestion.forceUnigrams(true).field("bigram").analyzer("ngram");
        searchSuggest( "Xor the Got-Jewel",
                phraseSuggestion);

        phraseSuggestion.field("ngram").analyzer("myDefAnalyzer")
                .addCandidateGenerator(candidateGenerator("body").minWordLength(1).suggestMode("always"));
        Suggest suggest = searchSuggest( "Xor the Got-Jewel", phraseSuggestion);

        // "xorr the god jewel" and and "xorn the god jewel" have identical scores (we are only using unigrams to score), so we tie break by
        // earlier term (xorn):
        assertSuggestion(suggest, 0, "simple_phrase", "xorn the god jewel");

        phraseSuggestion.analyzer(null);
        suggest = searchSuggest( "Xor the Got-Jewel", phraseSuggestion);

        // In this case xorr has a better score than xorn because we set the field back to the default (my_shingle2) analyzer, so the
        // probability that the term is not in the dictionary but is NOT a misspelling is relatively high in this case compared to the
        // others that have no n-gram with the other terms in the phrase :) you can set this realWorldErrorLikelyhood
        assertSuggestion(suggest, 0, "simple_phrase", "xorr the god jewel");
    }

    @Test
    public void testDifferentShardSize() throws Exception {
        createIndex("test");
        ensureGreen();
        indexRandom(true, client().prepareIndex("text", "type1", "1").setSource("field1", "foobar1").setRouting("1"),
                client().prepareIndex("text", "type1", "2").setSource("field1", "foobar2").setRouting("2"),
                client().prepareIndex("text", "type1", "3").setSource("field1", "foobar3").setRouting("3"));

        Suggest suggest = searchSuggest( "foobar",
                termSuggestion("simple")
                        .size(10).minDocFreq(0).field("field1").suggestMode("always"));
        ElasticsearchAssertions.assertSuggestionSize(suggest, 0, 3, "simple");
    }

    @Test // see #3469
    public void testShardFailures() throws IOException, InterruptedException {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
                .put(indexSettings())
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
        assertAcked(prepareCreate("test").setSettings(settingsBuilder()
                .put(indexSettings())
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
    public void testSearchForRarePhrase() throws ElasticsearchException, IOException {
        // If there isn't enough chaf per shard then shards can become unbalanced, making the cutoff recheck this is testing do more harm then good.
        int chafPerShard = 100;

        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
                .put(indexSettings())
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

        NumShards test = getNumShards("test");

        List<String> phrases = new ArrayList<>();
        Collections.addAll(phrases, "nobel prize", "noble gases", "somethingelse prize", "pride and joy", "notes are fun");
        for (int i = 0; i < 8; i++) {
            phrases.add("noble somethingelse" + i);
        }
        for (int i = 0; i < test.numPrimaries * chafPerShard; i++) {
            phrases.add("chaff" + i);
        }
        for (String phrase: phrases) {
            index("test", "type1", phrase, "body", phrase);
        }
        refresh();

        Suggest searchSuggest = searchSuggest("nobel prize", phraseSuggestion("simple_phrase")
                .field("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always").maxTermFreq(.99f))
                .confidence(2f)
                .maxErrors(5f)
                .size(1));
        assertSuggestionSize(searchSuggest, 0, 0, "simple_phrase");

        searchSuggest = searchSuggest("noble prize", phraseSuggestion("simple_phrase")
                .field("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always").maxTermFreq(.99f))
                .confidence(2f)
                .maxErrors(5f)
                .size(1));
        assertSuggestion(searchSuggest, 0, 0, "simple_phrase", "nobel prize");
    }

    /**
     * If the suggester finds tons of options then picking the right one is slow without <<<INSERT SOLUTION HERE>>>.
     */
    @Test
    @Nightly
    public void suggestWithManyCandidates() throws InterruptedException, ExecutionException, IOException {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, 1) // A single shard will help to keep the tests repeatable.
                .put("index.analysis.analyzer.text.tokenizer", "standard")
                .putArray("index.analysis.analyzer.text.filter", "lowercase", "my_shingle")
                .put("index.analysis.filter.my_shingle.type", "shingle")
                .put("index.analysis.filter.my_shingle.output_unigrams", true)
                .put("index.analysis.filter.my_shingle.min_shingle_size", 2)
                .put("index.analysis.filter.my_shingle.max_shingle_size", 3));

        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("type1")
                        .startObject("properties")
                            .startObject("title")
                                .field("type", "string")
                                .field("analyzer", "text")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        assertAcked(builder.addMapping("type1", mapping));
        ensureGreen();

        ImmutableList.Builder<String> titles = ImmutableList.<String>builder();

        // We're going to be searching for:
        //   united states house of representatives elections in washington 2006
        // But we need to make sure we generate a ton of suggestions so we add a bunch of candidates.
        // Many of these candidates are drawn from page names on English Wikipedia.

        // Tons of different options very near the exact query term
        titles.add("United States House of Representatives Elections in Washington 1789");
        for (int year = 1790; year < 2014; year+= 2) {
            titles.add("United States House of Representatives Elections in Washington " + year);
        }
        // Six of these are near enough to be viable suggestions, just not the top one

        // But we can't stop there!  Titles that are just a year are pretty common so lets just add one per year
        // since 0.  Why not?
        for (int year = 0; year < 2015; year++) {
            titles.add(Integer.toString(year));
        }
        // That ought to provide more less good candidates for the last term

        // Now remove or add plural copies of every term we can
        titles.add("State");
        titles.add("Houses of Parliament");
        titles.add("Representative Government");
        titles.add("Election");

        // Now some possessive
        titles.add("Washington's Birthday");

        // And some conjugation
        titles.add("Unified Modeling Language");
        titles.add("Unite Against Fascism");
        titles.add("Stated Income Tax");
        titles.add("Media organizations housed within colleges");

        // And other stuff
        titles.add("Untied shoelaces");
        titles.add("Unit circle");
        titles.add("Untitled");
        titles.add("Unicef");
        titles.add("Unrated");
        titles.add("UniRed");
        titles.add("Jalan Uniten–Dengkil"); // Highway in Malaysia
        titles.add("UNITAS");
        titles.add("UNITER");
        titles.add("Un-Led-Ed");
        titles.add("STATS LLC");
        titles.add("Staples");
        titles.add("Skates");
        titles.add("Statues of the Liberators");
        titles.add("Staten Island");
        titles.add("Statens Museum for Kunst");
        titles.add("Hause"); // The last name or the German word, whichever.
        titles.add("Hose");
        titles.add("Hoses");
        titles.add("Howse Peak");
        titles.add("The Hoose-Gow");
        titles.add("Hooser");
        titles.add("Electron");
        titles.add("Electors");
        titles.add("Evictions");
        titles.add("Coronal mass ejection");
        titles.add("Wasington"); // A film?
        titles.add("Warrington"); // A town in England
        titles.add("Waddington"); // Lots of places have this name
        titles.add("Watlington"); // Ditto
        titles.add("Waplington"); // Yup, also a town
        titles.add("Washing of the Spears"); // Book

        for (char c = 'A'; c <= 'Z'; c++) {
            // Can't forget lists, glorious lists!
            titles.add("List of former members of the United States House of Representatives (" + c + ")");

            // Lots of people are named Washington <Middle Initial>. LastName
            titles.add("Washington " + c + ". Lastname");

            // Lets just add some more to be evil
            titles.add("United " + c);
            titles.add("States " + c);
            titles.add("House " + c);
            titles.add("Elections " + c);
            titles.add("2006 " + c);
            titles.add(c + " United");
            titles.add(c + " States");
            titles.add(c + " House");
            titles.add(c + " Elections");
            titles.add(c + " 2006");
        }

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (String title: titles.build()) {
            builders.add(client().prepareIndex("test", "type1").setSource("title", title));
        }
        indexRandom(true, builders);

        PhraseSuggestionBuilder suggest = phraseSuggestion("title")
                .field("title")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("title")
                        .suggestMode("always")
                        .maxTermFreq(.99f)
                        .size(1000) // Setting a silly high size helps of generate a larger list of candidates for testing.
                        .maxInspections(1000) // This too
                )
                .confidence(0f)
                .maxErrors(2f)
                .shardSize(30000)
                .size(30000);
        Suggest searchSuggest = searchSuggest("united states house of representatives elections in washington 2006", suggest);
        assertSuggestion(searchSuggest, 0, 0, "title", "united states house of representatives elections in washington 2006");
        assertSuggestionSize(searchSuggest, 0, 25480, "title");  // Just to prove that we've run through a ton of options

        suggest.size(1);
        long start = System.currentTimeMillis();
        searchSuggest = searchSuggest("united states house of representatives elections in washington 2006", suggest);
        long total = System.currentTimeMillis() - start;
        assertSuggestion(searchSuggest, 0, 0, "title", "united states house of representatives elections in washington 2006");
        // assertThat(total, lessThan(1000L)); // Takes many seconds without fix - just for debugging
    }

    @Test
    public void testPhraseSuggesterCollate() throws InterruptedException, ExecutionException, IOException {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(settingsBuilder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, 1) // A single shard will help to keep the tests repeatable.
                .put("index.analysis.analyzer.text.tokenizer", "standard")
                .putArray("index.analysis.analyzer.text.filter", "lowercase", "my_shingle")
                .put("index.analysis.filter.my_shingle.type", "shingle")
                .put("index.analysis.filter.my_shingle.output_unigrams", true)
                .put("index.analysis.filter.my_shingle.min_shingle_size", 2)
                .put("index.analysis.filter.my_shingle.max_shingle_size", 3));

        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("title")
                .field("type", "string")
                .field("analyzer", "text")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        assertAcked(builder.addMapping("type1", mapping));
        ensureGreen();

        ImmutableList.Builder<String> titles = ImmutableList.<String>builder();

        titles.add("United States House of Representatives Elections in Washington 2006");
        titles.add("United States House of Representatives Elections in Washington 2005");
        titles.add("State");
        titles.add("Houses of Parliament");
        titles.add("Representative Government");
        titles.add("Election");

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (String title: titles.build()) {
            builders.add(client().prepareIndex("test", "type1").setSource("title", title));
        }
        indexRandom(true, builders);

        // suggest without filtering
        PhraseSuggestionBuilder suggest = phraseSuggestion("title")
                .field("title")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("title")
                        .suggestMode("always")
                        .maxTermFreq(.99f)
                        .size(10)
                        .maxInspections(200)
                )
                .confidence(0f)
                .maxErrors(2f)
                .shardSize(30000)
                .size(10);
        Suggest searchSuggest = searchSuggest("united states house of representatives elections in washington 2006", suggest);
        assertSuggestionSize(searchSuggest, 0, 10, "title");

        // suggest with filtering
        String filterString = XContentFactory.jsonBuilder()
                    .startObject()
                        .startObject("match_phrase")
                            .field("title", "{{suggestion}}")
                        .endObject()
                    .endObject()
                .string();
        PhraseSuggestionBuilder filteredQuerySuggest = suggest.collateQuery(filterString);
        searchSuggest = searchSuggest("united states house of representatives elections in washington 2006", filteredQuerySuggest);
        assertSuggestionSize(searchSuggest, 0, 2, "title");

        // filtered suggest with no result (boundary case)
        searchSuggest = searchSuggest("Elections of Representatives Parliament", filteredQuerySuggest);
        assertSuggestionSize(searchSuggest, 0, 0, "title");

        NumShards numShards = getNumShards("test");

        // filtered suggest with bad query
        String incorrectFilterString = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("test")
                        .field("title", "{{suggestion}}")
                    .endObject()
                .endObject()
                .string();
        PhraseSuggestionBuilder incorrectFilteredSuggest = suggest.collateQuery(incorrectFilterString);
        try {
            searchSuggest("united states house of representatives elections in washington 2006", numShards.numPrimaries, incorrectFilteredSuggest);
            fail("Post query error has been swallowed");
        } catch(ElasticsearchException e) {
            // expected
        }

        // suggest with filter collation
        String filterStringAsFilter = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("query")
                .startObject("match_phrase")
                .field("title", "{{suggestion}}")
                .endObject()
                .endObject()
                .endObject()
                .string();

        PhraseSuggestionBuilder filteredFilterSuggest = suggest.collateQuery(null).collateFilter(filterStringAsFilter);
        searchSuggest = searchSuggest("united states house of representatives elections in washington 2006", filteredFilterSuggest);
        assertSuggestionSize(searchSuggest, 0, 2, "title");

        // filtered suggest with bad filter
        String filterStr = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("pprefix")
                        .field("title", "{{suggestion}}")
                .endObject()
                .endObject()
                .string();

        PhraseSuggestionBuilder in = suggest.collateQuery(null).collateFilter(filterStr);
        try {
            searchSuggest("united states house of representatives elections in washington 2006", numShards.numPrimaries, in);
            fail("Post filter error has been swallowed");
        } catch(ElasticsearchException e) {
            //expected
        }

        // collate script failure due to no additional params
        String collateWithParams = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("{{query_type}}")
                    .field("{{query_field}}", "{{suggestion}}")
                .endObject()
                .endObject()
                .string();


        PhraseSuggestionBuilder phraseSuggestWithNoParams = suggest.collateFilter(null).collateQuery(collateWithParams);
        try {
            searchSuggest("united states house of representatives elections in washington 2006", numShards.numPrimaries, phraseSuggestWithNoParams);
            fail("Malformed query (lack of additional params) should fail");
        } catch (ElasticsearchException e) {
            // expected
        }

        // collate script with additional params
        Map<String, Object> params = new HashMap<>();
        params.put("query_type", "match_phrase");
        params.put("query_field", "title");

        PhraseSuggestionBuilder phraseSuggestWithParams = suggest.collateFilter(null).collateQuery(collateWithParams).collateParams(params);
        searchSuggest = searchSuggest("united states house of representatives elections in washington 2006", phraseSuggestWithParams);
        assertSuggestionSize(searchSuggest, 0, 2, "title");

        //collate request defining both query/filter should fail
        PhraseSuggestionBuilder phraseSuggestWithFilterAndQuery = suggest.collateFilter(filterStringAsFilter).collateQuery(filterString);
        try {
            searchSuggest("united states house of representatives elections in washington 2006", numShards.numPrimaries, phraseSuggestWithFilterAndQuery);
            fail("expected parse failure, as both filter and query are set in collate");
        } catch (ElasticsearchException e) {
            // expected
        }

        // collate request with prune set to true
        PhraseSuggestionBuilder phraseSuggestWithParamsAndReturn = suggest.collateFilter(null).collateQuery(collateWithParams).collateParams(params).collatePrune(true);
        searchSuggest = searchSuggest("united states house of representatives elections in washington 2006", phraseSuggestWithParamsAndReturn);
        assertSuggestionSize(searchSuggest, 0, 10, "title");
        assertSuggestionPhraseCollateMatchExists(searchSuggest, "title", 2);

    }

    protected Suggest searchSuggest(SuggestionBuilder<?>... suggestion) {
        return searchSuggest(null, suggestion);
    }

    protected Suggest searchSuggest(String suggestText, SuggestionBuilder<?>... suggestions) {
        return searchSuggest(suggestText, 0, suggestions);
    }

    protected Suggest searchSuggest(String suggestText, int expectShardsFailed, SuggestionBuilder<?>... suggestions) {
        if (randomBoolean()) {
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
        } else {
            SuggestRequestBuilder builder = client().prepareSuggest();
            if (suggestText != null) {
                builder.setSuggestText(suggestText);
            }
            for (SuggestionBuilder<?> suggestion : suggestions) {
                builder.addSuggestion(suggestion);
            }

            SuggestResponse actionGet = builder.execute().actionGet();
            assertThat(Arrays.toString(actionGet.getShardFailures()), actionGet.getFailedShards(), equalTo(expectShardsFailed));
            if (expectShardsFailed > 0) {
                throw new SearchPhaseExecutionException("suggest", "Suggest execution failed", new ShardSearchFailure[0]);
            }
            return actionGet.getSuggest();
        }
    }
}
