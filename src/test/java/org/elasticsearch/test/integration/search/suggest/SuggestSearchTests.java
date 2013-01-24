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

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.search.suggest.SuggestBuilder.fuzzySuggestion;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

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

    @Test
    public void testSimple() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();

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
                .setQuery(matchQuery("text", "spellcecker"))
                .addSuggestion(
                        fuzzySuggestion("test").setSuggestMode("always") // Always, otherwise the results can vary between requests.
                                .setText("abcd")
                                .setField("text"))
                .execute().actionGet();

        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));
        assertThat(search.suggest(), notNullValue());
        assertThat(search.suggest().getSuggestions().size(), equalTo(1));
        assertThat(search.suggest().getSuggestions().get(0).getName(), equalTo("test"));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().size(), equalTo(1));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().get(0).getTerm(), equalTo("abcd"));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().get(0).getSuggested().size(), equalTo(3));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().get(0).getSuggested().get(0).getTerm().string(), equalTo("aacd"));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().get(0).getSuggested().get(1).getTerm().string(), equalTo("abbd"));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().get(0).getSuggested().get(2).getTerm().string(), equalTo("abcc"));

        client.prepareSearch()
                .addSuggestion(
                        fuzzySuggestion("test").setSuggestMode("always") // Always, otherwise the results can vary between requests.
                                .setText("abcd")
                                .setField("text"))
                .execute().actionGet();

        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));
        assertThat(search.suggest(), notNullValue());
        assertThat(search.suggest().getSuggestions().size(), equalTo(1));
        assertThat(search.suggest().getSuggestions().get(0).getName(), equalTo("test"));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().size(), equalTo(1));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().get(0).getSuggested().size(), equalTo(3));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().get(0).getSuggested().get(0).getTerm().string(), equalTo("aacd"));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().get(0).getSuggested().get(1).getTerm().string(), equalTo("abbd"));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().get(0).getSuggested().get(2).getTerm().string(), equalTo("abcc"));
    }

    @Test
    public void testEmpty() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();

        SearchResponse search = client.prepareSearch()
                .setQuery(matchQuery("text", "spellcecker"))
                .addSuggestion(
                        fuzzySuggestion("test").setSuggestMode("always") // Always, otherwise the results can vary between requests.
                                .setText("abcd")
                                .setField("text"))
                .execute().actionGet();

        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));
        assertThat(search.suggest(), notNullValue());
        assertThat(search.suggest().getSuggestions().size(), equalTo(1));
        assertThat(search.suggest().getSuggestions().get(0).getName(), equalTo("test"));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().size(), equalTo(1));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().get(0).getTerm(), equalTo("abcd"));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().get(0).getSuggested().size(), equalTo(0));

        client.prepareSearch()
                .addSuggestion(
                        fuzzySuggestion("test").setSuggestMode("always") // Always, otherwise the results can vary between requests.
                                .setText("abcd")
                                .setField("text"))
                .execute().actionGet();

        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));
        assertThat(search.suggest(), notNullValue());
        assertThat(search.suggest().getSuggestions().size(), equalTo(1));
        assertThat(search.suggest().getSuggestions().get(0).getName(), equalTo("test"));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().size(), equalTo(1));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().get(0).getSuggested().size(), equalTo(0));
    }

    @Test
    public void testWithMultipleCommands() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();

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
                .addSuggestion(fuzzySuggestion("size1")
                        .setSize(1).setText("prefix_abcd").setMaxTermFreq(10).setMinDocFreq(0)
                        .setField("field1").setSuggestMode("always"))
                .addSuggestion(fuzzySuggestion("field2")
                        .setField("field2").setText("prefix_eeeh prefix_efgh")
                        .setMaxTermFreq(10).setMinDocFreq(0).setSuggestMode("always"))
                .addSuggestion(fuzzySuggestion("accuracy")
                        .setField("field2").setText("prefix_efgh").setAccuracy(1f)
                        .setMaxTermFreq(10).setMinDocFreq(0).setSuggestMode("always"))
                .execute().actionGet();

        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));
        assertThat(search.suggest(), notNullValue());
        assertThat(search.suggest().getSuggestions().size(), equalTo(3));
        assertThat(search.suggest().getSuggestions().get(0).getName(), equalTo("size1"));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().size(), equalTo(1));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().get(0).getSuggested().size(), equalTo(1));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().get(0).getSuggested().get(0).getTerm().string(), equalTo("prefix_aacd"));
        assertThat(search.suggest().getSuggestions().get(1).getName(), equalTo("field2"));
        assertThat(search.suggest().getSuggestions().get(1).getTerms().size(), equalTo(2));
        assertThat(search.suggest().getSuggestions().get(1).getTerms().get(0).getSuggested().size(), equalTo(1));
        assertThat(search.suggest().getSuggestions().get(1).getTerms().get(1).getSuggested().size(), equalTo(3));
        assertThat(search.suggest().getSuggestions().get(1).getTerms().get(1).getSuggested().get(0).getTerm().string(), equalTo("prefix_eeeh"));
        assertThat(search.suggest().getSuggestions().get(1).getTerms().get(1).getSuggested().get(1).getTerm().string(), equalTo("prefix_efff"));
        assertThat(search.suggest().getSuggestions().get(1).getTerms().get(1).getSuggested().get(2).getTerm().string(), equalTo("prefix_eggg"));
        assertThat(search.suggest().getSuggestions().get(2).getName(), equalTo("accuracy"));
        assertThat(search.suggest().getSuggestions().get(2).getTerms().get(0).getSuggested().isEmpty(), equalTo(true));
    }

    @Test
    public void testSizeAndSort() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();

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
                .addSuggestion(fuzzySuggestion("size3SortScoreFirst")
                        .setSize(3).setMinDocFreq(0).setField("field1").setSuggestMode("always"))
                .addSuggestion(fuzzySuggestion("size10SortScoreFirst")
                        .setSize(10).setMinDocFreq(0).setField("field1").setSuggestMode("always"))
                .addSuggestion(fuzzySuggestion("size3SortScoreFirstMaxEdits1")
                        .setMaxEdits(1)
                        .setSize(10).setMinDocFreq(0).setField("field1").setSuggestMode("always"))
                .addSuggestion(fuzzySuggestion("size10SortFrequencyFirst")
                        .setSize(10).setSort("frequency").setShardSize(1000)
                        .setMinDocFreq(0).setField("field1").setSuggestMode("always"))
                .execute().actionGet();

        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));
        assertThat(search.suggest(), notNullValue());
        assertThat(search.suggest().getSuggestions().size(), equalTo(4));
        assertThat(search.suggest().getSuggestions().get(0).getName(), equalTo("size3SortScoreFirst"));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().size(), equalTo(1));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().get(0).getSuggested().size(), equalTo(3));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().get(0).getSuggested().get(0).getTerm().string(), equalTo("prefix_aacd"));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().get(0).getSuggested().get(1).getTerm().string(), equalTo("prefix_abcc"));
        assertThat(search.suggest().getSuggestions().get(0).getTerms().get(0).getSuggested().get(2).getTerm().string(), equalTo("prefix_accd"));

        assertThat(search.suggest().getSuggestions().get(1).getName(), equalTo("size10SortScoreFirst"));
        assertThat(search.suggest().getSuggestions().get(1).getTerms().size(), equalTo(1));
        assertThat(search.suggest().getSuggestions().get(1).getTerms().get(0).getSuggested().size(), equalTo(10));
        assertThat(search.suggest().getSuggestions().get(1).getTerms().get(0).getSuggested().get(0).getTerm().string(), equalTo("prefix_aacd"));
        assertThat(search.suggest().getSuggestions().get(1).getTerms().get(0).getSuggested().get(1).getTerm().string(), equalTo("prefix_abcc"));
        assertThat(search.suggest().getSuggestions().get(1).getTerms().get(0).getSuggested().get(2).getTerm().string(), equalTo("prefix_accd"));
        // This fails sometimes. Depending on how the docs are sharded. The suggested suggest corrections get the df on shard level, which
        // isn't correct comparing it to the index level.
//        assertThat(search.suggest().suggestions().get(1).getSuggestedWords().get("prefix_abcd").get(3).getTerm(), equalTo("prefix_aaad"));

        assertThat(search.suggest().getSuggestions().get(2).getName(), equalTo("size3SortScoreFirstMaxEdits1"));
        assertThat(search.suggest().getSuggestions().get(2).getTerms().size(), equalTo(1));
        assertThat(search.suggest().getSuggestions().get(2).getTerms().get(0).getSuggested().size(), equalTo(3));
        assertThat(search.suggest().getSuggestions().get(2).getTerms().get(0).getSuggested().get(0).getTerm().string(), equalTo("prefix_aacd"));
        assertThat(search.suggest().getSuggestions().get(2).getTerms().get(0).getSuggested().get(1).getTerm().string(), equalTo("prefix_abcc"));
        assertThat(search.suggest().getSuggestions().get(2).getTerms().get(0).getSuggested().get(2).getTerm().string(), equalTo("prefix_accd"));

        assertThat(search.suggest().getSuggestions().get(3).getName(), equalTo("size10SortFrequencyFirst"));
        assertThat(search.suggest().getSuggestions().get(3).getTerms().size(), equalTo(1));
        assertThat(search.suggest().getSuggestions().get(3).getTerms().get(0).getSuggested().size(), equalTo(10));
        assertThat(search.suggest().getSuggestions().get(3).getTerms().get(0).getSuggested().get(0).getTerm().string(), equalTo("prefix_aaad"));
        assertThat(search.suggest().getSuggestions().get(3).getTerms().get(0).getSuggested().get(1).getTerm().string(), equalTo("prefix_abbb"));
        assertThat(search.suggest().getSuggestions().get(3).getTerms().get(0).getSuggested().get(2).getTerm().string(), equalTo("prefix_aaca"));
        assertThat(search.suggest().getSuggestions().get(3).getTerms().get(0).getSuggested().get(3).getTerm().string(), equalTo("prefix_abba"));
        assertThat(search.suggest().getSuggestions().get(3).getTerms().get(0).getSuggested().get(4).getTerm().string(), equalTo("prefix_accc"));
        assertThat(search.suggest().getSuggestions().get(3).getTerms().get(0).getSuggested().get(5).getTerm().string(), equalTo("prefix_addd"));
        assertThat(search.suggest().getSuggestions().get(3).getTerms().get(0).getSuggested().get(6).getTerm().string(), equalTo("prefix_abaa"));
        assertThat(search.suggest().getSuggestions().get(3).getTerms().get(0).getSuggested().get(7).getTerm().string(), equalTo("prefix_dbca"));
        assertThat(search.suggest().getSuggestions().get(3).getTerms().get(0).getSuggested().get(8).getTerm().string(), equalTo("prefix_cbad"));
        assertThat(search.suggest().getSuggestions().get(3).getTerms().get(0).getSuggested().get(9).getTerm().string(), equalTo("prefix_aacd"));
//        assertThat(search.suggest().suggestions().get(3).getSuggestedWords().get("prefix_abcd").get(4).getTerm(), equalTo("prefix_abcc"));
//        assertThat(search.suggest().suggestions().get(3).getSuggestedWords().get("prefix_abcd").get(4).getTerm(), equalTo("prefix_accd"));
    }


}
