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

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.search.suggest.SuggestBuilder.phraseSuggestion;
import static org.elasticsearch.search.suggest.SuggestBuilder.termSuggestion;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 */
public class SuggestSearchTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("server1");
        startNode("server2");
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server1");
    }
    
    @Test // see #2729
    public void testSizeOneShard() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();
        client.admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put(SETTING_NUMBER_OF_SHARDS, 1)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0))
                .execute().actionGet();
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();
        for (int i = 0; i < 15; i++) {
            client.prepareIndex("test", "type1")
            .setSource(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("text", "abc" + i)
                    .endObject()
            )
            .execute().actionGet();
        }
        
        client.admin().indices().prepareRefresh().execute().actionGet();
        
        SearchResponse search = client.prepareSearch()
        .setQuery(matchQuery("text", "spellchecker")).execute().actionGet();
        assertThat("didn't ask for suggestions but got some", search.getSuggest(), nullValue());
        
        search = client.prepareSearch()
                .setQuery(matchQuery("text", "spellchecker"))
                .addSuggestion(
                        termSuggestion("test").suggestMode("always") // Always, otherwise the results can vary between requests.
                                .text("abcd")
                                .field("text").size(10))
                .execute().actionGet();

        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("test").getName(), equalTo("test"));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().get(0).getText().string(), equalTo("abcd"));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().get(0).getOptions().size(), equalTo(10));

        
        search = client.prepareSearch()
        .setQuery(matchQuery("text", "spellchecker"))
        .addSuggestion(
                termSuggestion("test").suggestMode("always") // Always, otherwise the results can vary between requests.
                        .text("abcd")
                        .field("text").size(10).shardSize(5))
        .execute().actionGet();
        
        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("test").getName(), equalTo("test"));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().get(0).getText().string(), equalTo("abcd"));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().get(0).getOptions().size(), equalTo(5));
    }

    @Test
    public void testSimple() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();
        client.admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put(SETTING_NUMBER_OF_SHARDS, 5)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0))
                .execute().actionGet();
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("text", "abcd")
                        .endObject()
                )
                .execute().actionGet();
        client.prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("text", "aacd")
                        .endObject()
                )
                .execute().actionGet();
        client.prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("text", "abbd")
                        .endObject()
                )
                .execute().actionGet();
        client.prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("text", "abcc")
                        .endObject()
                )
                .execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();
        
        SearchResponse search = client.prepareSearch()
        .setQuery(matchQuery("text", "spellcecker")).execute().actionGet();
        assertThat("didn't ask for suggestions but got some", search.getSuggest(), nullValue());
        
        search = client.prepareSearch()
                .setQuery(matchQuery("text", "spellcecker"))
                .addSuggestion(
                        termSuggestion("test").suggestMode("always") // Always, otherwise the results can vary between requests.
                                .text("abcd")
                                .field("text"))
                .execute().actionGet();

        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("test").getName(), equalTo("test"));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().get(0).getText().string(), equalTo("abcd"));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().get(0).getOptions().size(), equalTo(3));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("aacd"));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().get(0).getOptions().get(1).getText().string(), equalTo("abbd"));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().get(0).getOptions().get(2).getText().string(), equalTo("abcc"));

        client.prepareSearch()
                .addSuggestion(
                        termSuggestion("test").suggestMode("always") // Always, otherwise the results can vary between requests.
                                .text("abcd")
                                .field("text"))
                .execute().actionGet();

        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("test").getName(), equalTo("test"));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().get(0).getOptions().size(), equalTo(3));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("aacd"));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().get(0).getOptions().get(1).getText().string(), equalTo("abbd"));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().get(0).getOptions().get(2).getText().string(), equalTo("abcc"));
    }

    @Test
    public void testEmpty() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();
        client.admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put(SETTING_NUMBER_OF_SHARDS, 5)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0))
                .execute().actionGet();
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        SearchResponse search = client.prepareSearch()
                .setQuery(matchQuery("text", "spellcecker"))
                .addSuggestion(
                        termSuggestion("test").suggestMode("always") // Always, otherwise the results can vary between requests.
                                .text("abcd")
                                .field("text"))
                .execute().actionGet();

        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("test").getName(), equalTo("test"));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().get(0).getText().string(), equalTo("abcd"));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().get(0).getOptions().size(), equalTo(0));

        client.prepareSearch()
                .addSuggestion(
                        termSuggestion("test").suggestMode("always") // Always, otherwise the results can vary between requests.
                                .text("abcd")
                                .field("text"))
                .execute().actionGet();

        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("test").getName(), equalTo("test"));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("test").getEntries().get(0).getOptions().size(), equalTo(0));
    }

    @Test
    public void testWithMultipleCommands() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();
        client.admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put(SETTING_NUMBER_OF_SHARDS, 5)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0))
                .execute().actionGet();
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field1", "prefix_abcd")
                        .field("field2", "prefix_efgh")
                        .endObject()
                )
                .execute().actionGet();
        client.prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field1", "prefix_aacd")
                        .field("field2", "prefix_eeeh")
                        .endObject()
                )
                .execute().actionGet();
        client.prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field1", "prefix_abbd")
                        .field("field2", "prefix_efff")
                        .endObject()
                )
                .execute().actionGet();
        client.prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("field1", "prefix_abcc")
                        .field("field2", "prefix_eggg")
                        .endObject()
                )
                .execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse search = client.prepareSearch()
                .addSuggestion(termSuggestion("size1")
                        .size(1).text("prefix_abcd").maxTermFreq(10).minDocFreq(0)
                        .field("field1").suggestMode("always"))
                .addSuggestion(termSuggestion("field2")
                        .field("field2").text("prefix_eeeh prefix_efgh")
                        .maxTermFreq(10).minDocFreq(0).suggestMode("always"))
                .addSuggestion(termSuggestion("accuracy")
                        .field("field2").text("prefix_efgh").setAccuracy(1f)
                        .maxTermFreq(10).minDocFreq(0).suggestMode("always"))
                .execute().actionGet();

        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(3));
        assertThat(search.getSuggest().getSuggestion("size1").getName(), equalTo("size1"));
        assertThat(search.getSuggest().getSuggestion("size1").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("size1").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("size1").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("prefix_aacd"));
        assertThat(search.getSuggest().getSuggestion("field2").getName(), equalTo("field2"));
        assertThat(search.getSuggest().getSuggestion("field2").getEntries().size(), equalTo(2));
        assertThat(search.getSuggest().getSuggestion("field2").getEntries().get(0).getText().string(), equalTo("prefix_eeeh"));
        assertThat(search.getSuggest().getSuggestion("field2").getEntries().get(0).getOffset(), equalTo(0));
        assertThat(search.getSuggest().getSuggestion("field2").getEntries().get(0).getLength(), equalTo(11));
        assertThat(search.getSuggest().getSuggestion("field2").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("field2").getEntries().get(1).getText().string(), equalTo("prefix_efgh"));
        assertThat(search.getSuggest().getSuggestion("field2").getEntries().get(1).getOffset(), equalTo(12));
        assertThat(search.getSuggest().getSuggestion("field2").getEntries().get(1).getLength(), equalTo(11));
        assertThat(search.getSuggest().getSuggestion("field2").getEntries().get(1).getOptions().size(), equalTo(3));
        assertThat(search.getSuggest().getSuggestion("field2").getEntries().get(1).getOptions().get(0).getText().string(), equalTo("prefix_eeeh"));
        assertThat(search.getSuggest().getSuggestion("field2").getEntries().get(1).getOptions().get(1).getText().string(), equalTo("prefix_efff"));
        assertThat(search.getSuggest().getSuggestion("field2").getEntries().get(1).getOptions().get(2).getText().string(), equalTo("prefix_eggg"));
        assertThat(search.getSuggest().getSuggestion("accuracy").getName(), equalTo("accuracy"));
        assertThat(search.getSuggest().getSuggestion("accuracy").getEntries().get(0).getOptions().isEmpty(), equalTo(true));
    }

    @Test
    public void testSizeAndSort() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();
        client.admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put(SETTING_NUMBER_OF_SHARDS, 5)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0))
                .execute().actionGet();
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

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
                client.prepareIndex("test", "type1")
                        .setSource(XContentFactory.jsonBuilder()
                                .startObject()
                                .field("field1", entry.getKey())
                                .endObject()
                        )
                        .execute().actionGet();
            }
        }
        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse search = client.prepareSearch()
                .setSuggestText("prefix_abcd")
                .addSuggestion(termSuggestion("size3SortScoreFirst")
                        .size(3).minDocFreq(0).field("field1").suggestMode("always"))
                .addSuggestion(termSuggestion("size10SortScoreFirst")
                        .size(10).minDocFreq(0).field("field1").suggestMode("always").shardSize(50))
                .addSuggestion(termSuggestion("size3SortScoreFirstMaxEdits1")
                        .maxEdits(1)
                        .size(10).minDocFreq(0).field("field1").suggestMode("always"))
                .addSuggestion(termSuggestion("size10SortFrequencyFirst")
                        .size(10).sort("frequency").shardSize(1000)
                        .minDocFreq(0).field("field1").suggestMode("always"))
                .execute().actionGet();

        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(4));
        assertThat(search.getSuggest().getSuggestion("size3SortScoreFirst").getName(), equalTo("size3SortScoreFirst"));
        assertThat(search.getSuggest().getSuggestion("size3SortScoreFirst").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("size3SortScoreFirst").getEntries().get(0).getOptions().size(), equalTo(3));
        assertThat(search.getSuggest().getSuggestion("size3SortScoreFirst").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("prefix_aacd"));
        assertThat(search.getSuggest().getSuggestion("size3SortScoreFirst").getEntries().get(0).getOptions().get(1).getText().string(), equalTo("prefix_abcc"));
        assertThat(search.getSuggest().getSuggestion("size3SortScoreFirst").getEntries().get(0).getOptions().get(2).getText().string(), equalTo("prefix_accd"));

        assertThat(search.getSuggest().getSuggestion("size10SortScoreFirst").getName(), equalTo("size10SortScoreFirst"));
        assertThat(search.getSuggest().getSuggestion("size10SortScoreFirst").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("size10SortScoreFirst").getEntries().get(0).getOptions().size(), equalTo(10));
        assertThat(search.getSuggest().getSuggestion("size10SortScoreFirst").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("prefix_aacd"));
        assertThat(search.getSuggest().getSuggestion("size10SortScoreFirst").getEntries().get(0).getOptions().get(1).getText().string(), equalTo("prefix_abcc"));
        assertThat(search.getSuggest().getSuggestion("size10SortScoreFirst").getEntries().get(0).getOptions().get(2).getText().string(), equalTo("prefix_accd"));
        // This fails sometimes. Depending on how the docs are sharded. The suggested suggest corrections get the df on shard level, which
        // isn't correct comparing it to the index level.
//        assertThat(search.suggest().suggestions().get(1).getSuggestedWords().get("prefix_abcd").get(3).getTerm(), equalTo("prefix_aaad"));

        assertThat(search.getSuggest().getSuggestion("size3SortScoreFirstMaxEdits1").getName(), equalTo("size3SortScoreFirstMaxEdits1"));
        assertThat(search.getSuggest().getSuggestion("size3SortScoreFirstMaxEdits1").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("size3SortScoreFirstMaxEdits1").getEntries().get(0).getOptions().size(), equalTo(3));
        assertThat(search.getSuggest().getSuggestion("size3SortScoreFirstMaxEdits1").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("prefix_aacd"));
        assertThat(search.getSuggest().getSuggestion("size3SortScoreFirstMaxEdits1").getEntries().get(0).getOptions().get(1).getText().string(), equalTo("prefix_abcc"));
        assertThat(search.getSuggest().getSuggestion("size3SortScoreFirstMaxEdits1").getEntries().get(0).getOptions().get(2).getText().string(), equalTo("prefix_accd"));

        assertThat(search.getSuggest().getSuggestion("size10SortFrequencyFirst").getName(), equalTo("size10SortFrequencyFirst"));
        assertThat(search.getSuggest().getSuggestion("size10SortFrequencyFirst").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().size(), equalTo(10));
        assertThat(search.getSuggest().getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("prefix_aaad"));
        assertThat(search.getSuggest().getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().get(1).getText().string(), equalTo("prefix_abbb"));
        assertThat(search.getSuggest().getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().get(2).getText().string(), equalTo("prefix_aaca"));
        assertThat(search.getSuggest().getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().get(3).getText().string(), equalTo("prefix_abba"));
        assertThat(search.getSuggest().getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().get(4).getText().string(), equalTo("prefix_accc"));
        assertThat(search.getSuggest().getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().get(5).getText().string(), equalTo("prefix_addd"));
        assertThat(search.getSuggest().getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().get(6).getText().string(), equalTo("prefix_abaa"));
        assertThat(search.getSuggest().getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().get(7).getText().string(), equalTo("prefix_dbca"));
        assertThat(search.getSuggest().getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().get(8).getText().string(), equalTo("prefix_cbad"));
        assertThat(search.getSuggest().getSuggestion("size10SortFrequencyFirst").getEntries().get(0).getOptions().get(9).getText().string(), equalTo("prefix_aacd"));
//        assertThat(search.suggest().suggestions().get(3).getSuggestedWords().get("prefix_abcd").get(4).getTerm(), equalTo("prefix_abcc"));
//        assertThat(search.suggest().suggestions().get(3).getSuggestedWords().get("prefix_abcd").get(4).getTerm(), equalTo("prefix_accd"));
    }
    
    
    @Test
    public void testMarvelHerosPhraseSuggest() throws ElasticSearchException, IOException {
        client.admin().indices().prepareDelete().execute().actionGet();
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
        
        
        client.admin().indices().prepareCreate("test").setSettings(builder.build()).addMapping("type1", mapping).execute().actionGet();
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();
        BufferedReader reader = new BufferedReader(new InputStreamReader(SuggestSearchTests.class.getResourceAsStream("/config/names.txt")));
        String line = null;
        while ((line = reader.readLine()) != null) {
            client.prepareIndex("test", "type1")
            .setSource(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("body", line)
                    .field("body_reverse", line)
                    .field("bigram", line)
                    .endObject()
            )
            .execute().actionGet();
        }
        client.admin().indices().prepareRefresh().execute().actionGet();
        SearchResponse search = client.prepareSearch()
        .setSuggestText("american ame")
        .addSuggestion(phraseSuggestion("simple_phrase")
                .field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .size(1)).execute().actionGet();
        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("american ame"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("american ace"));
        
        
        search = client.prepareSearch()
        .setSearchType(SearchType.COUNT)
        .setSuggestText("Xor the Got-Jewel")
        .addSuggestion(phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .maxErrors(0.5f)
                .size(1))
        .execute().actionGet();
        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
        
        // pass in a correct phrase
        search = client.prepareSearch()
        .setSearchType(SearchType.COUNT)
        .setSuggestText("Xorr the God-Jewel")
        .addSuggestion(phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .maxErrors(0.5f)
                .confidence(0.f)
                .size(1))
        .execute().actionGet();
        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xorr the God-Jewel"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
        
        // pass in a correct phrase - set confidence to 2
        search = client.prepareSearch()
        .setSearchType(SearchType.COUNT)
        .setSuggestText("Xorr the God-Jewel")
        .addSuggestion(phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .maxErrors(0.5f)
                .confidence(2.f))
        .execute().actionGet();
        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(0));
        
        
        // pass in a correct phrase - set confidence to 0.99
        search = client.prepareSearch()
        .setSearchType(SearchType.COUNT)
        .setSuggestText("Xorr the God-Jewel")
        .addSuggestion(phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .maxErrors(0.5f)
                .confidence(0.99f))
        .execute().actionGet();
        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xorr the God-Jewel"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
        
        //test reverse suggestions with pre & post filter
        search = client.prepareSearch()
        .setSearchType(SearchType.COUNT)
        .setSuggestText("xor the yod-Jewel")
        .addSuggestion(phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body_reverse").minWordLength(1).suggestMode("always").preFilter("reverse").postFilter("reverse"))
                .maxErrors(0.5f)
                .size(1))
        .execute().actionGet();
        
        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("xor the yod-Jewel"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
        // set all mass to trigrams (not indexed)
        search = client.prepareSearch()
        .setSearchType(SearchType.COUNT)
        .setSuggestText("Xor the Got-Jewel")
        .addSuggestion(phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .smoothingModel(new PhraseSuggestionBuilder.LinearInterpolation(1,0,0))
                .maxErrors(0.5f)
                .size(1))
        .execute().actionGet();
        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(0));
        
        
        // set all mass to bigrams
        search = client.prepareSearch()
        .setSearchType(SearchType.COUNT)
        .setSuggestText("Xor the Got-Jewel")
        .addSuggestion(phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .smoothingModel(new PhraseSuggestionBuilder.LinearInterpolation(0,1,0))
                .maxErrors(0.5f)
                .size(1))
        .execute().actionGet();
        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
        // distribute mass
        search = client.prepareSearch()
        .setSearchType(SearchType.COUNT)
        .setSuggestText("Xor the Got-Jewel")
        .addSuggestion(phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .smoothingModel(new PhraseSuggestionBuilder.LinearInterpolation(0.4,0.4,0.2))
                .maxErrors(0.5f)
                .size(1))
        .execute().actionGet();
        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
        search = client.prepareSearch()
        .setSuggestText("american ame")
        .addSuggestion(phraseSuggestion("simple_phrase")
                .field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .smoothingModel(new PhraseSuggestionBuilder.LinearInterpolation(0.4,0.4,0.2))
                .size(1)).execute().actionGet();
        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("american ame"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("american ace"));

        
        // try all smoothing methods
        search = client.prepareSearch()
        .setSearchType(SearchType.COUNT)
        .setSuggestText("Xor the Got-Jewel")
        .addSuggestion(phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .smoothingModel(new PhraseSuggestionBuilder.LinearInterpolation(0.4,0.4,0.2))
                .maxErrors(0.5f)
                .size(1))
        .execute().actionGet();
        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
        search = client.prepareSearch()
        .setSearchType(SearchType.COUNT)
        .setSuggestText("Xor the Got-Jewel")
        .addSuggestion(phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .smoothingModel(new PhraseSuggestionBuilder.Laplace(0.2))
                .maxErrors(0.5f)
                .size(1))
        .execute().actionGet();
        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
        search = client.prepareSearch()
        .setSearchType(SearchType.COUNT)
        .setSuggestText("Xor the Got-Jewel")
        .addSuggestion(phraseSuggestion("simple_phrase").
                realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("body")
                .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                .smoothingModel(new PhraseSuggestionBuilder.StupidBackoff(0.1))
                .maxErrors(0.5f)
                .size(1))
        .execute().actionGet();
        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
    }
    
    
    
    
    @Test
    public void testPhraseBoundaryCases() throws ElasticSearchException, IOException {
        client.admin().indices().prepareDelete().execute().actionGet();
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
        
        
        client.admin().indices().prepareCreate("test").setSettings(builder.build()).addMapping("type1", mapping).execute().actionGet();
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();
        BufferedReader reader = new BufferedReader(new InputStreamReader(SuggestSearchTests.class.getResourceAsStream("/config/names.txt")));
        String line = null;
        while ((line = reader.readLine()) != null) {
            client.prepareIndex("test", "type1")
            .setSource(XContentFactory.jsonBuilder()
                    .startObject()
                    .field("body", line)
                    .field("bigram", line)
                    .field("ngram", line)
                    .endObject()
            )
            .execute().actionGet();
        }
        client.admin().indices().prepareRefresh().execute().actionGet();
       
        try {
            client.prepareSearch()
                    .setSearchType(SearchType.COUNT)
                    .setSuggestText("Xor the Got-Jewel")
                    .addSuggestion(
                            phraseSuggestion("simple_phrase")
                                    .realWordErrorLikelihood(0.95f)
                                    .field("bigram")
                                    .gramSize(2)
                                    .analyzer("body")
                                    .addCandidateGenerator(
                                            PhraseSuggestionBuilder.candidateGenerator("does_not_exists").minWordLength(1)
                                                    .suggestMode("always")).maxErrors(0.5f).size(1)).execute().actionGet();
            assert false : "field does not exists";
        } catch (SearchPhaseExecutionException e) {
        }
        
        try {
            client.prepareSearch()
                    .setSearchType(SearchType.COUNT)
                    .setSuggestText("Xor the Got-Jewel")
                    .addSuggestion(
                            phraseSuggestion("simple_phrase").realWordErrorLikelihood(0.95f).field("bigram").maxErrors(0.5f)
                                    .size(1)).execute().actionGet();
            assert false : "analyzer does only produce ngrams";
        } catch (SearchPhaseExecutionException e) {
        }
        try {
            client.prepareSearch()
                    .setSearchType(SearchType.COUNT)
                    .setSuggestText("Xor the Got-Jewel")
                    .addSuggestion(
                            phraseSuggestion("simple_phrase").realWordErrorLikelihood(0.95f).field("bigram").analyzer("bigram").maxErrors(0.5f)
                                    .size(1)).execute().actionGet();
            assert false : "analyzer does only produce ngrams";
        } catch (SearchPhaseExecutionException e) {
        }
        
        // don't force unigrams
        client.prepareSearch()
        .setSearchType(SearchType.COUNT)
        .setSuggestText("Xor the Got-Jewel")
        .addSuggestion(
                phraseSuggestion("simple_phrase").realWordErrorLikelihood(0.95f).field("bigram").gramSize(2).analyzer("bigram").forceUnigrams(false).maxErrors(0.5f)
                        .size(1)).execute().actionGet();
        
        
        client.prepareSearch()
        .setSearchType(SearchType.COUNT)
        .setSuggestText("Xor the Got-Jewel")
        .addSuggestion(
                phraseSuggestion("simple_phrase").realWordErrorLikelihood(0.95f).field("bigram").analyzer("ngram").maxErrors(0.5f)
                        .size(1)).execute().actionGet();
     
        
        SearchResponse search = client.prepareSearch()
        .setSearchType(SearchType.COUNT)
        .setSuggestText("Xor the Got-Jewel")
        .addSuggestion(
                phraseSuggestion("simple_phrase").maxErrors(0.5f).field("ngram").analyzer("myDefAnalyzer")
                  .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                        .size(1)).execute().actionGet();
        
        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
        search = client.prepareSearch()
        .setSearchType(SearchType.COUNT)
        .setSuggestText("Xor the Got-Jewel")
        .addSuggestion(
                phraseSuggestion("simple_phrase").maxErrors(0.5f).field("ngram")
                  .addCandidateGenerator(PhraseSuggestionBuilder.candidateGenerator("body").minWordLength(1).suggestMode("always"))
                        .size(1)).execute().actionGet();
        
        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getName(), equalTo("simple_phrase"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getText().string(), equalTo("Xor the Got-Jewel"));
        assertThat(search.getSuggest().getSuggestion("simple_phrase").getEntries().get(0).getOptions().get(0).getText().string(), equalTo("xorr the god jewel"));
        
    }
    
    
    
    @Test
    public void testDifferentShardSize() throws Exception {
        // test suggestion with explicitly added different shard sizes
        client.admin().indices().prepareDelete().execute().actionGet();
        client.admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put(SETTING_NUMBER_OF_SHARDS, 5)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0))
                .execute().actionGet();
        client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1")
        .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .field("field1", "foobar1")
                .endObject()
        ).setRouting("1").execute().actionGet();

        client.prepareIndex("test", "type1", "2")
        .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .field("field1", "foobar2")
                .endObject()
        ).setRouting("2").execute().actionGet();
        
        client.prepareIndex("test", "type1", "3")
        .setSource(XContentFactory.jsonBuilder()
                .startObject()
                .field("field1", "foobar3")
                .endObject()
        ).setRouting("1").execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse search = client.prepareSearch()
                .setSuggestText("foobar")
                .addSuggestion(termSuggestion("simple")
                        .size(10).minDocFreq(0).field("field1").suggestMode("always"))
                .execute().actionGet();
        assertThat(Arrays.toString(search.getShardFailures()), search.getFailedShards(), equalTo(0));
        assertThat(search.getSuggest(), notNullValue());
        assertThat(search.getSuggest().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple").getName(), equalTo("simple"));
        assertThat(search.getSuggest().getSuggestion("simple").getEntries().size(), equalTo(1));
        assertThat(search.getSuggest().getSuggestion("simple").getEntries().get(0).getOptions().size(), equalTo(3));
    }

}
