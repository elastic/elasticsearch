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

package org.elasticsearch.test.integration.search.suggest;

import com.google.common.base.Charsets;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder.SuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.search.suggest.SuggestBuilder.phraseSuggestion;
import static org.elasticsearch.search.suggest.SuggestBuilder.termSuggestion;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSuggestionSize;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 */
public class SuggestSearchTests extends AbstractSharedClusterTest {
    
    @Test // see #3037
    public void testSuggestModes() throws IOException {
        Builder builder = ImmutableSettings.builder();
        builder.put("index.number_of_shards", 1).put("index.number_of_replicas", 0);
        builder.put("index.analysis.analyzer.biword.tokenizer", "standard");
        builder.putArray("index.analysis.analyzer.biword.filter", "shingler", "lowercase");
        builder.put("index.analysis.filter.shingler.type", "shingle");
        builder.put("index.analysis.filter.shingler.min_shingle_size", 2);
        builder.put("index.analysis.filter.shingler.max_shingle_size", 3);
        
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
        client().admin().indices().prepareDelete().execute().actionGet();
        client().admin().indices().prepareCreate("test").setSettings(builder.build()).addMapping("type1", mapping).execute().actionGet();
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();
        client().prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder().startObject().field("name", "I like iced tea").endObject()).execute().actionGet();
        client().prepareIndex("test", "type1")
        .setSource(XContentFactory.jsonBuilder().startObject().field("name", "I like tea.").endObject()).execute().actionGet();
        client().prepareIndex("test", "type1")
        .setSource(XContentFactory.jsonBuilder().startObject().field("name", "I like ice cream.").endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        Suggest searchSuggest = searchSuggest(
                client(),
                "ice tea",
                phraseSuggestion("did_you_mean").field("name_shingled")
                        .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("name").prefixLength(0).minWordLength(0).suggestMode("always").maxEdits(2))
                        .gramSize(3));
        ElasticsearchAssertions.assertSuggestion(searchSuggest, 0, 0, "did_you_mean", "iced tea");
        searchSuggest = searchSuggest(
                client(),
                "ice tea",
                phraseSuggestion("did_you_mean").field("name_shingled")
                        .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("name").prefixLength(0).minWordLength(0).maxEdits(2))
                        .gramSize(3));
        assertSuggestionSize(searchSuggest, 0, 0, "did_you_mean");
    }
    
    @Test // see #2729
    public void testSizeOneShard() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put(SETTING_NUMBER_OF_SHARDS, 1)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();
        for (int i = 0; i < 15; i++) {
            client().prepareIndex("test", "type1")
            .setSource(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("text", "abc" + i)
                    .endObject()
            )
            .execute().actionGet();
        }
        
        client().admin().indices().prepareRefresh().execute().actionGet();
        
        SearchResponse _search = client().prepareSearch()
        .setQuery(matchQuery("text", "spellchecker")).execute().actionGet();
        assertThat("didn't ask for suggestions but got some", _search.getSuggest(), nullValue());
        
        Suggest suggest = searchSuggest(client(), termSuggestion("test").suggestMode("always") // Always, otherwise the results can vary between requests.
                                .text("abcd")
                                .field("text").size(10));
        
        assertThat(suggest, notNullValue());
        assertThat(suggest.size(), equalTo(1));
        assertThat(suggest.getSuggestion("test").getName(), equalTo("test"));
        assertThat(suggest.getSuggestion("test").getEntries().size(), equalTo(1));
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getText().string(), equalTo("abcd"));
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getOptions().size(), equalTo(10));

        
        suggest = searchSuggest(client(), termSuggestion("test").suggestMode("always") // Always, otherwise the results can vary between requests.
                        .text("abcd")
                        .field("text").size(10).shardSize(5));
        
        assertThat(suggest, notNullValue());
        assertThat(suggest.size(), equalTo(1));
        assertThat(suggest.getSuggestion("test").getName(), equalTo("test"));
        assertThat(suggest.getSuggestion("test").getEntries().size(), equalTo(1));
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getText().string(), equalTo("abcd"));
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getOptions().size(), equalTo(5));
    }

    @Test
    public void testSimple() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put(SETTING_NUMBER_OF_SHARDS, 5)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        client().prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("text", "abcd")
                        .endObject()
                )
                .execute().actionGet();
        client().prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("text", "aacd")
                        .endObject()
                )
                .execute().actionGet();
        client().prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("text", "abbd")
                        .endObject()
                )
                .execute().actionGet();
        client().prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("text", "abcc")
                        .endObject()
                )
                .execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        
        SearchResponse _search = client().prepareSearch()
        .setQuery(matchQuery("text", "spellcecker")).execute().actionGet();
        assertThat("didn't ask for suggestions but got some", _search.getSuggest(), nullValue());
        
        Suggest suggest = searchSuggest(client(), termSuggestion("test").suggestMode("always") // Always, otherwise the results can vary between requests.
                                .text("abcd")
                                .field("text"));

        assertThat(suggest, notNullValue());
        assertThat(suggest.size(), equalTo(1));
        assertThat(suggest.getSuggestion("test").getName(), equalTo("test"));
        assertThat(suggest.getSuggestion("test").getEntries().size(), equalTo(1));
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getText().string(), equalTo("abcd"));
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getOptions().size(), equalTo(3));
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("aacd"));
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getOptions().get(1).getText().string(), equalTo("abbd"));
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getOptions().get(2).getText().string(), equalTo("abcc"));

        suggest = searchSuggest(client(), termSuggestion("test").suggestMode("always") // Always, otherwise the results can vary between requests.
                                .text("abcd")
                                .field("text"));

        assertThat(suggest, notNullValue());
        assertThat(suggest.size(), equalTo(1));
        assertThat(suggest.getSuggestion("test").getName(), equalTo("test"));
        assertThat(suggest.getSuggestion("test").getEntries().size(), equalTo(1));
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getOptions().size(), equalTo(3));
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("aacd"));
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getOptions().get(1).getText().string(), equalTo("abbd"));
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getOptions().get(2).getText().string(), equalTo("abcc"));
    }

    @Test
    public void testEmpty() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put(SETTING_NUMBER_OF_SHARDS, 5)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        Suggest suggest = searchSuggest(client(), termSuggestion("test").suggestMode("always") // Always, otherwise the results can vary between requests.
                                .text("abcd")
                                .field("text"));

        assertThat(suggest, notNullValue());
        assertThat(suggest.size(), equalTo(1));
        assertThat(suggest.getSuggestion("test").getName(), equalTo("test"));
        assertThat(suggest.getSuggestion("test").getEntries().size(), equalTo(1));
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getText().string(), equalTo("abcd"));
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getOptions().size(), equalTo(0));

        searchSuggest(client(), termSuggestion("test").suggestMode("always") // Always, otherwise the results can vary between requests.
                                .text("abcd")
                                .field("text"));

        assertThat(suggest, notNullValue());
        assertThat(suggest.size(), equalTo(1));
        assertThat(suggest.getSuggestion("test").getName(), equalTo("test"));
        assertThat(suggest.getSuggestion("test").getEntries().size(), equalTo(1));
        assertThat(suggest.getSuggestion("test").getEntries().get(0).getOptions().size(), equalTo(0));
    }

    @Test
    public void testWithMultipleCommands() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put(SETTING_NUMBER_OF_SHARDS, 5)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        client().prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field1", "prefix_abcd")
                        .field("field2", "prefix_efgh")
                        .endObject()
                )
                .execute().actionGet();
        client().prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field1", "prefix_aacd")
                        .field("field2", "prefix_eeeh")
                        .endObject()
                )
                .execute().actionGet();
        client().prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field1", "prefix_abbd")
                        .field("field2", "prefix_efff")
                        .endObject()
                )
                .execute().actionGet();
        client().prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field1", "prefix_abcc")
                        .field("field2", "prefix_eggg")
                        .endObject()
                )
                .execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        Suggest suggest = searchSuggest(client(), termSuggestion("size1")
                        .size(1).text("prefix_abcd").maxTermFreq(10).prefixLength(1).minDocFreq(0)
                        .field("field1").suggestMode("always"),
                termSuggestion("field2")
                        .field("field2").text("prefix_eeeh prefix_efgh")
                        .maxTermFreq(10).minDocFreq(0).suggestMode("always"),
                termSuggestion("accuracy")
                        .field("field2").text("prefix_efgh").setAccuracy(1f)
                        .maxTermFreq(10).minDocFreq(0).suggestMode("always"));

        assertThat(suggest, notNullValue());
        assertThat(suggest.size(), equalTo(3));
        assertThat(suggest.getSuggestion("size1").getName(), equalTo("size1"));
        assertThat(suggest.getSuggestion("size1").getEntries().size(), equalTo(1));
        assertThat(suggest.getSuggestion("size1").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(suggest.getSuggestion("size1").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("prefix_aacd"));
        assertThat(suggest.getSuggestion("field2").getName(), equalTo("field2"));
        assertThat(suggest.getSuggestion("field2").getEntries().size(), equalTo(2));
        assertThat(suggest.getSuggestion("field2").getEntries().get(0).getText().string(), equalTo("prefix_eeeh"));
        assertThat(suggest.getSuggestion("field2").getEntries().get(0).getOffset(), equalTo(0));
        assertThat(suggest.getSuggestion("field2").getEntries().get(0).getLength(), equalTo(11));
        assertThat(suggest.getSuggestion("field2").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(suggest.getSuggestion("field2").getEntries().get(1).getText().string(), equalTo("prefix_efgh"));
        assertThat(suggest.getSuggestion("field2").getEntries().get(1).getOffset(), equalTo(12));
        assertThat(suggest.getSuggestion("field2").getEntries().get(1).getLength(), equalTo(11));
        assertThat(suggest.getSuggestion("field2").getEntries().get(1).getOptions().size(), equalTo(3));
        assertThat(suggest.getSuggestion("field2").getEntries().get(1).getOptions().get(0).getText().string(), equalTo("prefix_eeeh"));
        assertThat(suggest.getSuggestion("field2").getEntries().get(1).getOptions().get(1).getText().string(), equalTo("prefix_efff"));
        assertThat(suggest.getSuggestion("field2").getEntries().get(1).getOptions().get(2).getText().string(), equalTo("prefix_eggg"));
        assertThat(suggest.getSuggestion("accuracy").getName(), equalTo("accuracy"));
        assertThat(suggest.getSuggestion("accuracy").getEntries().get(0).getOptions().isEmpty(), equalTo(true));
    }

    @Test
    public void testSizeAndSort() throws Exception {
        client().admin().indices().prepareDelete().execute().actionGet();
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put(SETTING_NUMBER_OF_SHARDS, 5)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

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
                client().prepareIndex("test", "type1")
                        .setSource(XContentFactory.jsonBuilder()
                                .startObject()
                                .field("field1", entry.getKey())
                                .endObject()
                        )
                        .execute().actionGet();
            }
        }
        client().admin().indices().prepareRefresh().execute().actionGet();

        Suggest suggest = searchSuggest(client(),"prefix_abcd",
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

        assertThat(suggest, notNullValue());
        assertThat(suggest.size(), equalTo(4));
        assertThat(suggest.getSuggestion("size3SortScoreFirst").getName(), equalTo("size3SortScoreFirst"));
        assertThat(suggest.getSuggestion("size3SortScoreFirst").getEntries().size(), equalTo(1));
        assertThat(suggest.getSuggestion("size3SortScoreFirst").getEntries().get(0).getOptions().size(), equalTo(3));
        assertThat(suggest.getSuggestion("size3SortScoreFirst").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("prefix_aacd"));
        assertThat(suggest.getSuggestion("size3SortScoreFirst").getEntries().get(0).getOptions().get(1).getText().string(), equalTo("prefix_abcc"));
        assertThat(suggest.getSuggestion("size3SortScoreFirst").getEntries().get(0).getOptions().get(2).getText().string(), equalTo("prefix_accd"));

        assertThat(suggest.getSuggestion("size10SortScoreFirst").getName(), equalTo("size10SortScoreFirst"));
        assertThat(suggest.getSuggestion("size10SortScoreFirst").getEntries().size(), equalTo(1));
        assertThat(suggest.getSuggestion("size10SortScoreFirst").getEntries().get(0).getOptions().size(), equalTo(10));
        assertThat(suggest.getSuggestion("size10SortScoreFirst").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("prefix_aacd"));
        assertThat(suggest.getSuggestion("size10SortScoreFirst").getEntries().get(0).getOptions().get(1).getText().string(), equalTo("prefix_abcc"));
        assertThat(suggest.getSuggestion("size10SortScoreFirst").getEntries().get(0).getOptions().get(2).getText().string(), equalTo("prefix_accd"));
        // This fails sometimes. Depending on how the docs are sharded. The suggested suggest corrections get the df on shard level, which
        // isn't correct comparing it to the index level.
//        assertThat(search.suggest().suggestions().get(1).getSuggestedWords().get("prefix_abcd").get(3).getTerm(), equalTo("prefix_aaad"));

        assertThat(suggest.getSuggestion("size3SortScoreFirstMaxEdits1").getName(), equalTo("size3SortScoreFirstMaxEdits1"));
        assertThat(suggest.getSuggestion("size3SortScoreFirstMaxEdits1").getEntries().size(), equalTo(1));
        assertThat(suggest.getSuggestion("size3SortScoreFirstMaxEdits1").getEntries().get(0).getOptions().size(), equalTo(3));
        assertThat(suggest.getSuggestion("size3SortScoreFirstMaxEdits1").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("prefix_aacd"));
        assertThat(suggest.getSuggestion("size3SortScoreFirstMaxEdits1").getEntries().get(0).getOptions().get(1).getText().string(), equalTo("prefix_abcc"));
        assertThat(suggest.getSuggestion("size3SortScoreFirstMaxEdits1").getEntries().get(0).getOptions().get(2).getText().string(), equalTo("prefix_accd"));

        assertThat(suggest.getSuggestion("size10SortFrequencyFirst").getName(), equalTo("size10SortFrequencyFirst"));
        assertThat(suggest.getSuggestion("size10SortFrequencyFirst").getEntries().size(), equalTo(1));
        assertThat(suggest.getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().size(), equalTo(10));
        assertThat(suggest.getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("prefix_aaad"));
        assertThat(suggest.getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().get(1).getText().string(), equalTo("prefix_abbb"));
        assertThat(suggest.getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().get(2).getText().string(), equalTo("prefix_aaca"));
        assertThat(suggest.getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().get(3).getText().string(), equalTo("prefix_abba"));
        assertThat(suggest.getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().get(4).getText().string(), equalTo("prefix_accc"));
        assertThat(suggest.getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().get(5).getText().string(), equalTo("prefix_addd"));
        assertThat(suggest.getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().get(6).getText().string(), equalTo("prefix_abaa"));
        assertThat(suggest.getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().get(7).getText().string(), equalTo("prefix_dbca"));
        assertThat(suggest.getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().get(8).getText().string(), equalTo("prefix_cbad"));
        assertThat(suggest.getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().get(9).getText().string(), equalTo("prefix_aacd"));
//        assertThat(search.suggest().suggestions().get(3).getSuggestedWords().get("prefix_abcd").get(4).getTerm(), equalTo("prefix_abcc"));
//        assertThat(search.suggest().suggestions().get(3).getSuggestedWords().get("prefix_abcd").get(4).getTerm(), equalTo("prefix_accd"));
    }
    
    @Test // see #2817
    public void testStopwordsOnlyPhraseSuggest() throws ElasticSearchException, IOException {
        client().admin().indices().prepareDelete().execute().actionGet();
        Builder builder = ImmutableSettings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0);
        client().admin().indices().prepareCreate("test").setSettings(builder.build()).execute().actionGet();
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();
        client().prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder().startObject().field("body", "this is a test").endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        Suggest searchSuggest = searchSuggest(
                client(),
                "a an the",
                phraseSuggestion("simple_phrase").field("body").gramSize(1)
                        .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                        .size(1));
        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(0));
    }
    
    @Test
    public void testPrefixLength() throws ElasticSearchException, IOException {
        Builder builder = ImmutableSettings.builder();
        builder.put("index.number_of_shards", 1).put("index.number_of_replicas", 0);
        builder.put("index.analysis.analyzer.reverse.tokenizer", "standard");
        builder.putArray("index.analysis.analyzer.reverse.filter", "lowercase", "reverse");
        builder.put("index.analysis.analyzer.body.tokenizer", "standard");
        builder.putArray("index.analysis.analyzer.body.filter", "lowercase");
        builder.put("index.analysis.analyzer.bigram.tokenizer", "standard");
        builder.putArray("index.analysis.analyzer.bigram.filter", "my_shingle", "lowercase");
        builder.put("index.analysis.filter.my_shingle.type", "shingle");
        builder.put("index.analysis.filter.my_shingle.output_unigrams", false);
        builder.put("index.analysis.filter.my_shingle.min_shingle_size", 2);
        builder.put("index.analysis.filter.my_shingle.max_shingle_size", 2);
        
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
        .startObject("_all").field("store", "yes").field("termVector", "with_positions_offsets").endObject()
        .startObject("properties")
        .startObject("body").field("type", "string").field("analyzer", "body").endObject()
        .startObject("body_reverse").field("type", "string").field("analyzer", "reverse").endObject()
        .startObject("bigram").field("type", "string").field("analyzer", "bigram").endObject()
        .endObject()
        .endObject().endObject();
        client().admin().indices().prepareDelete().execute().actionGet();
        client().admin().indices().prepareCreate("test").setSettings(builder.build()).addMapping("type1", mapping).execute().actionGet();
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();
        client().prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder().startObject().field("body", "hello world").endObject()).execute().actionGet();
        client().prepareIndex("test", "type1")
        .setSource(XContentFactory.jsonBuilder().startObject().field("body", "hello world").endObject()).execute().actionGet();
        client().prepareIndex("test", "type1")
        .setSource(XContentFactory.jsonBuilder().startObject().field("body", "hello words").endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        Suggest searchSuggest = searchSuggest(
                client(),
                "hello word",
                phraseSuggestion("simple_phrase").field("body")
                        .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").prefixLength(4).minWordLength(1).suggestMode("always"))
                        .size(1).confidence(1.0f));
        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("hello word"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("hello words"));
        
        
        searchSuggest = searchSuggest(
                client(),
                "hello word",
                phraseSuggestion("simple_phrase").field("body")
                        .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").prefixLength(2).minWordLength(1).suggestMode("always"))
                        .size(1).confidence(1.0f));
        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("hello word"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("hello world"));
        
    }
    
    
    @Test
    public void testMarvelHerosPhraseSuggest() throws ElasticSearchException, IOException {
        client().admin().indices().prepareDelete().execute().actionGet();
        Builder builder = ImmutableSettings.builder();
        builder.put("index.analysis.analyzer.reverse.tokenizer", "standard");
        builder.putArray("index.analysis.analyzer.reverse.filter", "lowercase", "reverse");
        builder.put("index.analysis.analyzer.body.tokenizer", "standard");
        builder.putArray("index.analysis.analyzer.body.filter", "lowercase");
        builder.put("index.analysis.analyzer.bigram.tokenizer", "standard");
        builder.putArray("index.analysis.analyzer.bigram.filter", "my_shingle", "lowercase");
        builder.put("index.analysis.filter.my_shingle.type", "shingle");
        builder.put("index.analysis.filter.my_shingle.output_unigrams", false);
        builder.put("index.analysis.filter.my_shingle.min_shingle_size", 2);
        builder.put("index.analysis.filter.my_shingle.max_shingle_size", 2);
        
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
        .startObject("_all").field("store", "yes").field("termVector", "with_positions_offsets").endObject()
        .startObject("properties")
        .startObject("body").field("type", "string").field("analyzer", "body").endObject()
        .startObject("body_reverse").field("type", "string").field("analyzer", "reverse").endObject()
        .startObject("bigram").field("type", "string").field("analyzer", "bigram").endObject()
        .endObject()
        .endObject().endObject();
        
        
        client().admin().indices().prepareCreate("test").setSettings(builder.build()).addMapping("type1", mapping).execute().actionGet();
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();
        BufferedReader reader = new BufferedReader(new InputStreamReader(SuggestSearchTests.class.getResourceAsStream("/config/names.txt"), Charsets.UTF_8));
        String line = null;
        while ((line = reader.readLine()) != null) {
            client().prepareIndex("test", "type1")
            .setSource(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("body", line)
                    .field("body_reverse", line)
                    .field("bigram", line)
                    .endObject()
            )
            .execute().actionGet();
        }
        client().admin().indices().prepareRefresh().execute().actionGet();
        
        Suggest searchSuggest = searchSuggest(client(), "american ame", phraseSuggestion("simple_phrase")
                .field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .size(1));

        
        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("american ame"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("american ace"));
        
        
        searchSuggest = searchSuggest(client(), "Xor the Got-Jewel",
                        phraseSuggestion("simple_phrase").
                            realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                            .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                            .maxErrors(0.5f)
                            .size(1));

        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
        
        // pass in a correct phrase
        searchSuggest = searchSuggest(client(), "Xorr the God-Jewel",
                phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .maxErrors(0.5f)
                .confidence(0.f)
                .size(1));
        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xorr the God-Jewel"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
        
        // pass in a correct phrase - set confidence to 2
        searchSuggest = searchSuggest(client(), "Xorr the God-Jewel",
                phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .maxErrors(0.5f)
                .confidence(2.f));
        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(0));
        
        
        // pass in a correct phrase - set confidence to 0.99
        searchSuggest = searchSuggest(client(), "Xorr the God-Jewel",
                phraseSuggestion("simple_phrase").
                    realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                    .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                    .maxErrors(0.5f)
                    .confidence(0.99f));

        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xorr the God-Jewel"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
        
        //test reverse suggestions with pre & post filter
        searchSuggest = searchSuggest(client(), "xor the yod-Jewel",
                phraseSuggestion("simple_phrase").
                    realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                    .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                    .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body_reverse").minWordLength(1).suggestMode("always").preFilter("reverse").postFilter("reverse"))
                    .maxErrors(0.5f)
                    .size(1));

        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("xor the yod-Jewel"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
        // set all mass to trigrams (not indexed)
        searchSuggest = searchSuggest(client(), "Xor the Got-Jewel",
                phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .smoothingModel(new PhraseSuggestionBuilder.LinearInterpolation(1,0,0))
                .maxErrors(0.5f)
                .size(1));

        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(0));
        
        
        // set all mass to bigrams
        searchSuggest =  searchSuggest(client(), "Xor the Got-Jewel",
                phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .smoothingModel(new PhraseSuggestionBuilder.LinearInterpolation(0,1,0))
                .maxErrors(0.5f)
                .size(1));

        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
        // distribute mass
        searchSuggest = searchSuggest(client(), "Xor the Got-Jewel",
                phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .smoothingModel(new PhraseSuggestionBuilder.LinearInterpolation(0.4,0.4,0.2))
                .maxErrors(0.5f)
                .size(1));

        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
        searchSuggest = searchSuggest(client(), "american ame",
                phraseSuggestion("simple_phrase")
                .field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .smoothingModel(new PhraseSuggestionBuilder.LinearInterpolation(0.4,0.4,0.2))
                .size(1));;

        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("american ame"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("american ace"));

        
        // try all smoothing methods
        searchSuggest = searchSuggest(client(), "Xor the Got-Jewel",
                phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .smoothingModel(new PhraseSuggestionBuilder.LinearInterpolation(0.4,0.4,0.2))
                .maxErrors(0.5f)
                .size(1));

        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
        searchSuggest = searchSuggest(client(), "Xor the Got-Jewel",
                phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .smoothingModel(new PhraseSuggestionBuilder.Laplace(0.2))
                .maxErrors(0.5f)
                .size(1));

        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
        searchSuggest = searchSuggest(client(), "Xor the Got-Jewel",
                phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .smoothingModel(new PhraseSuggestionBuilder.StupidBackoff(0.1))
                .maxErrors(0.5f).tokenLimit(5)
                .size(1));
        
        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
        // check tokenLimit
        
        searchSuggest = searchSuggest(client(), "Xor the Got-Jewel",
                phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .smoothingModel(new PhraseSuggestionBuilder.StupidBackoff(0.1))
                .maxErrors(0.5f)
                .size(1).tokenLimit(4));
        
        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(0));
        
        searchSuggest = searchSuggest(client(), "Xor the Got-Jewel Xor the Got-Jewel Xor the Got-Jewel",
                phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .smoothingModel(new PhraseSuggestionBuilder.StupidBackoff(0.1))
                .maxErrors(0.5f)
                .size(1).tokenLimit(15));
        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel Xor the Got-Jewel Xor the Got-Jewel"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel xorr the god jewel xorr the god jewel"));
        
    }
    
    @Test
    public void testSizePararm() throws IOException {
        client().admin().indices().prepareDelete().execute().actionGet();
        Builder builder = ImmutableSettings.builder();
        builder.put("index.number_of_shards", 1);
        builder.put("index.number_of_replicas", 1);
        builder.put("index.analysis.analyzer.reverse.tokenizer", "standard");
        builder.putArray("index.analysis.analyzer.reverse.filter", "lowercase", "reverse");
        builder.put("index.analysis.analyzer.body.tokenizer", "standard");
        builder.putArray("index.analysis.analyzer.body.filter", "lowercase");
        builder.put("index.analysis.analyzer.bigram.tokenizer", "standard");
        builder.putArray("index.analysis.analyzer.bigram.filter", "my_shingle", "lowercase");
        builder.put("index.analysis.filter.my_shingle.type", "shingle");
        builder.put("index.analysis.filter.my_shingle.output_unigrams", false);
        builder.put("index.analysis.filter.my_shingle.min_shingle_size", 2);
        builder.put("index.analysis.filter.my_shingle.max_shingle_size", 2);

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("_all")
                .field("store", "yes").field("termVector", "with_positions_offsets").endObject().startObject("properties")
                .startObject("body").field("type", "string").field("analyzer", "body").endObject().startObject("body_reverse")
                .field("type", "string").field("analyzer", "reverse").endObject().startObject("bigram").field("type", "string")
                .field("analyzer", "bigram").endObject().endObject().endObject().endObject();

        client().admin().indices().prepareCreate("test").setSettings(builder.build()).addMapping("type1", mapping).execute().actionGet();
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();
        String line = "xorr the god jewel";
        client().prepareIndex("test", "type1")
                .setSource(
                        XContentFactory.jsonBuilder().startObject().field("body", line).field("body_reverse", line).field("bigram", line)
                                .endObject()).execute().actionGet();
        line = "I got it this time";
        client().prepareIndex("test", "type1")
                .setSource(
                        XContentFactory.jsonBuilder().startObject().field("body", line).field("body_reverse", line).field("bigram", line)
                                .endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        Suggest searchSuggest = searchSuggest(client(), "Xorr the Gut-Jewel", phraseSuggestion("simple_phrase")
                .realWordErrorLikelihood(0.95f)
                .field("bigram")
                .gramSize(2)
                .analyzer("body")
                .addCandidateGenerator(
                        PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).prefixLength(1)
                                .suggestMode("always").size(1).accuracy(0.1f))
                .smoothingModel(new PhraseSuggestionBuilder.StupidBackoff(0.1)).maxErrors(1.0f).size(5));
     
        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(0));

        searchSuggest = searchSuggest(client(), "Xorr the Gut-Jewel",// we allow a size of 2 now on the shard generator level so "god" will be found since it's LD2
                        phraseSuggestion("simple_phrase")
                                .realWordErrorLikelihood(0.95f)
                                .field("bigram")
                                .gramSize(2)
                                .analyzer("body")
                                .addCandidateGenerator(
                                        PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).prefixLength(1)
                                                .suggestMode("always").size(2).accuracy(0.1f))
                                .smoothingModel(new PhraseSuggestionBuilder.StupidBackoff(0.1)).maxErrors(1.0f).size(5));
        assertThat(searchSuggest, notNullValue());
        assertThat(searchSuggest.size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xorr the Gut-Jewel"));
        assertThat(searchSuggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(),
                equalTo("xorr the god jewel"));
    }
    
    
    
    
    @Test
    public void testPhraseBoundaryCases() throws ElasticSearchException, IOException {
        client().admin().indices().prepareDelete().execute().actionGet();
        Builder builder = ImmutableSettings.builder();
        builder.put("index.analysis.analyzer.body.tokenizer", "standard");
        builder.putArray("index.analysis.analyzer.body.filter", "lowercase");
        builder.put("index.analysis.analyzer.bigram.tokenizer", "standard");
        builder.putArray("index.analysis.analyzer.bigram.filter", "my_shingle", "lowercase");
        builder.put("index.analysis.analyzer.ngram.tokenizer", "standard");
        builder.putArray("index.analysis.analyzer.ngram.filter", "my_shingle2", "lowercase");
        builder.put("index.analysis.analyzer.myDefAnalyzer.tokenizer", "standard");
        builder.putArray("index.analysis.analyzer.myDefAnalyzer.filter", "shingle", "lowercase");
        builder.put("index.analysis.filter.my_shingle.type", "shingle");
        builder.put("index.analysis.filter.my_shingle.output_unigrams", false);
        builder.put("index.analysis.filter.my_shingle.min_shingle_size", 2);
        builder.put("index.analysis.filter.my_shingle.max_shingle_size", 2);
        builder.put("index.analysis.filter.my_shingle2.type", "shingle");
        builder.put("index.analysis.filter.my_shingle2.output_unigrams", true);
        builder.put("index.analysis.filter.my_shingle2.min_shingle_size", 2);
        builder.put("index.analysis.filter.my_shingle2.max_shingle_size", 2);
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
        .startObject("_all").field("store", "yes").field("termVector", "with_positions_offsets").endObject()
        .startObject("properties")
        .startObject("body").field("type", "string").field("analyzer", "body").endObject()
        .startObject("bigram").field("type", "string").field("analyzer", "bigram").endObject()
        .startObject("ngram").field("type", "string").field("analyzer", "ngram").endObject()
        .endObject()
        .endObject().endObject();
        
        
        client().admin().indices().prepareCreate("test").setSettings(builder.build()).addMapping("type1", mapping).execute().actionGet();
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();
        BufferedReader reader = new BufferedReader(new InputStreamReader(SuggestSearchTests.class.getResourceAsStream("/config/names.txt"), Charsets.UTF_8));
        String line = null;
        while ((line = reader.readLine()) != null) {
            client().prepareIndex("test", "type1")
            .setSource(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("body", line)
                    .field("bigram", line)
                    .field("ngram", line)
                    .endObject()
            )
            .execute().actionGet();
        }
        client().admin().indices().prepareRefresh().execute().actionGet();
       
        try {
            searchSuggest(client(), "Xor the Got-Jewel", 5, 
                            phraseSuggestion("simple_phrase")
                                    .realWordErrorLikelihood(0.95f)
                                    .field("bigram")
                                    .gramSize(2)
                                    .analyzer("body")
                                    .addCandidateGenerator(
                                            PhraseSuggestionBuilder.candidateGenerator("does_not_exists").minWordLength(1)
                                                    .suggestMode("always")).maxErrors(0.5f).size(1));
            
            assert false : "field does not exists";
        } catch (SearchPhaseExecutionException e) {}
        
        try {
            searchSuggest(client(), "Xor the Got-Jewel", 5,
                    phraseSuggestion("simple_phrase").realWordErrorLikelihood(0.95f).field("bigram").maxErrors(0.5f).size(1));

            assert false : "analyzer does only produce ngrams";
        } catch (SearchPhaseExecutionException e) {
        }
        
        try {
            searchSuggest(client(), "Xor the Got-Jewel", 5,
                        phraseSuggestion("simple_phrase").realWordErrorLikelihood(0.95f).field("bigram").analyzer("bigram").maxErrors(0.5f)
                                .size(1));
            assert false : "analyzer does only produce ngrams";
        } catch (SearchPhaseExecutionException e) {
        }
        
        // don't force unigrams
        searchSuggest(client(), "Xor the Got-Jewel",
                phraseSuggestion("simple_phrase").realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("bigram").forceUnigrams(false).maxErrors(0.5f)
                        .size(1));
        
        
        searchSuggest(client(), "Xor the Got-Jewel",
                phraseSuggestion("simple_phrase").realWordErrorLikelihood(0.95f).field("bigram").analyzer("ngram").maxErrors(0.5f)
                        .size(1));
     
        Suggest suggest = searchSuggest(client(), "Xor the Got-Jewel",
                phraseSuggestion("simple_phrase").maxErrors(0.5f).field("ngram").analyzer("myDefAnalyzer")
                  .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                        .size(1));
        
        assertThat(suggest, notNullValue());
        assertThat(suggest.size(), equalTo(1));
        assertThat(suggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(suggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(suggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(suggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel"));
        assertThat(suggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
        suggest = searchSuggest(client(), "Xor the Got-Jewel",
                phraseSuggestion("simple_phrase").maxErrors(0.5f).field("ngram")
                  .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                        .size(1));
        
        assertThat(suggest, notNullValue());
        assertThat(suggest.size(), equalTo(1));
        assertThat(suggest.getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(suggest.getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(suggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(suggest.getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel"));
        assertThat(suggest.getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
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
        return actionGet.getSuggest();
    }
    
    @Test
    public void testDifferentShardSize() throws Exception {
        // test suggestion with explicitly added different shard sizes
        client().admin().indices().prepareDelete().execute().actionGet();
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put(SETTING_NUMBER_OF_SHARDS, 5)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0))
                .execute().actionGet();
        client().admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        client().prepareIndex("test", "type1", "1")
        .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .field("field1", "foobar1")
                .endObject()
        ).setRouting("1").execute().actionGet();

        client().prepareIndex("test", "type1", "2")
        .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .field("field1", "foobar2")
                .endObject()
        ).setRouting("2").execute().actionGet();
        
        client().prepareIndex("test", "type1", "3")
        .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .field("field1", "foobar3")
                .endObject()
        ).setRouting("1").execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        Suggest suggest = searchSuggest(client(), "foobar",
                termSuggestion("simple")
                        .size(10).minDocFreq(0).field("field1").suggestMode("always"));

        assertThat(suggest, notNullValue());
        assertThat(suggest.size(), equalTo(1));
        assertThat(suggest.getSuggestion("simple").getName(), equalTo("simple"));
        assertThat(suggest.getSuggestion("simple").getEntries().size(), equalTo(1));
        assertThat(suggest.getSuggestion("simple").getEntries().get(0).getOptions().size(), equalTo(3));
    }

}
