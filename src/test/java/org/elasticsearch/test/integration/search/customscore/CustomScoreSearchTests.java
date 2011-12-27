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

package org.elasticsearch.test.integration.search.customscore;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.CustomFiltersScoreQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@Test
public class CustomScoreSearchTests extends AbstractNodesTests {

    private Client client;

    @BeforeMethod
    public void createNodes() throws Exception {
        startNode("server1");
        client = getClient();
    }

    @AfterMethod
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server1");
    }

    @Test
    public void testCustomScriptBoost() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().create(createIndexRequest("test")).actionGet();
        client.index(indexRequest("test").type("type1").id("1")
                .source(jsonBuilder().startObject().field("test", "value beck").field("num1", 1.0f).endObject())).actionGet();
        client.index(indexRequest("test").type("type1").id("2")
                .source(jsonBuilder().startObject().field("test", "value check").field("num1", 2.0f).endObject())).actionGet();
        client.admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("--- QUERY_THEN_FETCH");

        logger.info("running doc['num1'].value");
        SearchResponse response = client.search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).query(customScoreQuery(termQuery("test", "value")).script("doc['num1'].value")))
        ).actionGet();

        assertThat(response.hits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.hits().getAt(0).id(), response.hits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.hits().getAt(1).id(), response.hits().getAt(1).explanation());
        assertThat(response.hits().getAt(0).id(), equalTo("2"));
        assertThat(response.hits().getAt(1).id(), equalTo("1"));

        logger.info("running -doc['num1'].value");
        response = client.search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).query(customScoreQuery(termQuery("test", "value")).script("-doc['num1'].value")))
        ).actionGet();

        assertThat(response.hits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.hits().getAt(0).id(), response.hits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.hits().getAt(1).id(), response.hits().getAt(1).explanation());
        assertThat(response.hits().getAt(0).id(), equalTo("1"));
        assertThat(response.hits().getAt(1).id(), equalTo("2"));


        logger.info("running pow(doc['num1'].value, 2)");
        response = client.search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).query(customScoreQuery(termQuery("test", "value")).script("pow(doc['num1'].value, 2)")))
        ).actionGet();

        assertThat(response.hits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.hits().getAt(0).id(), response.hits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.hits().getAt(1).id(), response.hits().getAt(1).explanation());
        assertThat(response.hits().getAt(0).id(), equalTo("2"));
        assertThat(response.hits().getAt(1).id(), equalTo("1"));

        logger.info("running max(doc['num1'].value, 1)");
        response = client.search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).query(customScoreQuery(termQuery("test", "value")).script("max(doc['num1'].value, 1d)")))
        ).actionGet();

        assertThat(response.hits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.hits().getAt(0).id(), response.hits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.hits().getAt(1).id(), response.hits().getAt(1).explanation());
        assertThat(response.hits().getAt(0).id(), equalTo("2"));
        assertThat(response.hits().getAt(1).id(), equalTo("1"));

        logger.info("running doc['num1'].value * _score");
        response = client.search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).query(customScoreQuery(termQuery("test", "value")).script("doc['num1'].value * _score")))
        ).actionGet();

        assertThat(response.hits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.hits().getAt(0).id(), response.hits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.hits().getAt(1).id(), response.hits().getAt(1).explanation());
        assertThat(response.hits().getAt(0).id(), equalTo("2"));
        assertThat(response.hits().getAt(1).id(), equalTo("1"));

        logger.info("running param1 * param2 * _score");
        response = client.search(searchRequest()
                .searchType(SearchType.QUERY_THEN_FETCH)
                .source(searchSource().explain(true).query(customScoreQuery(termQuery("test", "value")).script("param1 * param2 * _score").param("param1", 2).param("param2", 2)))
        ).actionGet();

        assertThat(response.hits().totalHits(), equalTo(2l));
        logger.info("Hit[0] {} Explanation {}", response.hits().getAt(0).id(), response.hits().getAt(0).explanation());
        logger.info("Hit[1] {} Explanation {}", response.hits().getAt(1).id(), response.hits().getAt(1).explanation());
        assertThat(response.hits().getAt(0).id(), equalTo("1"));
        assertThat(response.hits().getAt(1).id(), equalTo("2"));
    }

    @Test
    public void testCustomFiltersScore() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.prepareIndex("test", "type", "1").setSource("field", "value1", "color", "red").execute().actionGet();
        client.prepareIndex("test", "type", "2").setSource("field", "value2", "color", "blue").execute().actionGet();
        client.prepareIndex("test", "type", "3").setSource("field", "value3", "color", "red").execute().actionGet();
        client.prepareIndex("test", "type", "4").setSource("field", "value4", "color", "blue").execute().actionGet();

        // make sure tests apply the boosts to the right value (i.e. get Max then multiply against base score, to test this
        // a non 1.0F boost for each field is needed.
        QueryBuilder allBoosted2Query = customScoreQuery(matchAllQuery()).script("_score * 2");

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch("test")
                .setQuery(customFiltersScoreQuery(allBoosted2Query)
                        .add(termFilter("field", "value4"), "_score * 2")
                        .add(termFilter("field", "value2"), "_score * 3"))
                .setExplain(true)
                .execute().actionGet();

        assertThat(Arrays.toString(searchResponse.shardFailures()), searchResponse.failedShards(), equalTo(0));

        assertThat(searchResponse.hits().totalHits(), equalTo(4l));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.hits().getAt(0).id(), searchResponse.hits().getAt(0).explanation());
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("2"));
        assertThat(searchResponse.hits().getAt(0).score(), equalTo(6.0f));
        assertThat(searchResponse.hits().getAt(1).id(), equalTo("4"));
        assertThat(searchResponse.hits().getAt(1).score(), equalTo(4.0f));
        assertThat(searchResponse.hits().getAt(2).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.hits().getAt(2).score(), equalTo(2.0f));
        assertThat(searchResponse.hits().getAt(3).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.hits().getAt(3).score(), equalTo(2.0f));

        searchResponse = client.prepareSearch("test")
                .setQuery(customFiltersScoreQuery(allBoosted2Query)
                        .add(termFilter("field", "value4"), 2)
                        .add(termFilter("field", "value2"), 3))
                .setExplain(true)
                .execute().actionGet();

        assertThat(Arrays.toString(searchResponse.shardFailures()), searchResponse.failedShards(), equalTo(0));

        assertThat(searchResponse.hits().totalHits(), equalTo(4l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("2"));
        assertThat(searchResponse.hits().getAt(0).score(), equalTo(6.0f));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.hits().getAt(0).id(), searchResponse.hits().getAt(0).explanation());
        assertThat(searchResponse.hits().getAt(1).id(), equalTo("4"));
        assertThat(searchResponse.hits().getAt(1).score(), equalTo(4.0f));
        assertThat(searchResponse.hits().getAt(2).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.hits().getAt(2).score(), equalTo(2.0f));
        assertThat(searchResponse.hits().getAt(3).id(), anyOf(equalTo("1"), equalTo("3")));
        assertThat(searchResponse.hits().getAt(3).score(), equalTo(2.0f));

        searchResponse = client.prepareSearch("test")
                .setQuery(customFiltersScoreQuery(allBoosted2Query).scoreMode("total")
                        .add(termFilter("field", "value4"), 2)
                        .add(termFilter("field", "value1"), 3)
                        .add(termFilter("color", "red"), 5))
                .setExplain(true)
                .execute().actionGet();

        assertThat(Arrays.toString(searchResponse.shardFailures()), searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(4l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.hits().getAt(0).score(), equalTo(16.0f));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.hits().getAt(0).id(), searchResponse.hits().getAt(0).explanation());

        searchResponse = client.prepareSearch("test")
                .setQuery(customFiltersScoreQuery(allBoosted2Query).scoreMode("max")
                        .add(termFilter("field", "value4"), 2)
                        .add(termFilter("field", "value1"), 3)
                        .add(termFilter("color", "red"), 5))
                .setExplain(true)
                .execute().actionGet();

        assertThat(Arrays.toString(searchResponse.shardFailures()), searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(4l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.hits().getAt(0).score(), equalTo(10.0f));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.hits().getAt(0).id(), searchResponse.hits().getAt(0).explanation());

        searchResponse = client.prepareSearch("test")
                .setQuery(customFiltersScoreQuery(allBoosted2Query).scoreMode("avg")
                        .add(termFilter("field", "value4"), 2)
                        .add(termFilter("field", "value1"), 3)
                        .add(termFilter("color", "red"), 5))
                .setExplain(true)
                .execute().actionGet();

        assertThat(Arrays.toString(searchResponse.shardFailures()), searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(4l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("3"));
        assertThat(searchResponse.hits().getAt(0).score(), equalTo(10.0f));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.hits().getAt(0).id(), searchResponse.hits().getAt(0).explanation());
        assertThat(searchResponse.hits().getAt(1).id(), equalTo("1"));
        assertThat(searchResponse.hits().getAt(1).score(), equalTo(8.0f));
        logger.info("--> Hit[1] {} Explanation {}", searchResponse.hits().getAt(1).id(), searchResponse.hits().getAt(1).explanation());

        searchResponse = client.prepareSearch("test")
                .setQuery(customFiltersScoreQuery(allBoosted2Query).scoreMode("min")
                        .add(termFilter("field", "value4"), 2)
                        .add(termFilter("field", "value1"), 3)
                        .add(termFilter("color", "red"), 5))
                .setExplain(true)
                .execute().actionGet();

        assertThat(Arrays.toString(searchResponse.shardFailures()), searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(4l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("3"));
        assertThat(searchResponse.hits().getAt(0).score(), equalTo(10.0f));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.hits().getAt(0).id(), searchResponse.hits().getAt(0).explanation());
        assertThat(searchResponse.hits().getAt(1).id(), equalTo("1"));
        assertThat(searchResponse.hits().getAt(1).score(), equalTo(6.0f));
        assertThat(searchResponse.hits().getAt(2).id(), equalTo("4"));
        assertThat(searchResponse.hits().getAt(2).score(), equalTo(4.0f));
        assertThat(searchResponse.hits().getAt(3).id(), equalTo("2"));
        assertThat(searchResponse.hits().getAt(3).score(), equalTo(2.0f));

        searchResponse = client.prepareSearch("test")
                .setQuery(customFiltersScoreQuery(allBoosted2Query).scoreMode("multiply")
                        .add(termFilter("field", "value4"), 2)
                        .add(termFilter("field", "value1"), 3)
                        .add(termFilter("color", "red"), 5))
                .setExplain(true)
                .execute().actionGet();

        assertThat(Arrays.toString(searchResponse.shardFailures()), searchResponse.failedShards(), equalTo(0));
        assertThat(searchResponse.hits().totalHits(), equalTo(4l));
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.hits().getAt(0).score(), equalTo(60.0f));
        logger.info("--> Hit[0] {} Explanation {}", searchResponse.hits().getAt(0).id(), searchResponse.hits().getAt(0).explanation());
        assertThat(searchResponse.hits().getAt(1).id(), equalTo("3"));
        assertThat(searchResponse.hits().getAt(1).score(), equalTo(10.0f));
        assertThat(searchResponse.hits().getAt(2).id(), equalTo("4"));
        assertThat(searchResponse.hits().getAt(2).score(), equalTo(4.0f));
        assertThat(searchResponse.hits().getAt(3).id(), equalTo("2"));
        assertThat(searchResponse.hits().getAt(3).score(), equalTo(2.0f));


    }

    @Test
    public void testCustomFiltersScoreWithGroups() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();
                                                                                                                                        // max   min   mult   total   avg
        client.prepareIndex("test", "type", "1").setSource("field", "value1", "color", "red", "extra", "thing").execute().actionGet();  //  x     x    30      12      8
        client.prepareIndex("test", "type", "2").setSource("field", "value2", "color", "blue").execute().actionGet();                   //  x     8     x       6      x
        client.prepareIndex("test", "type", "3").setSource("field", "value3", "color", "red").execute().actionGet();                    //  3     x    10      12     10
        client.prepareIndex("test", "type", "4").setSource("field", "value4", "color", "blue").execute().actionGet();                   //  4     6     4      10      4

        // make sure tests apply the boosts to the right value (i.e. get Max then multiply against base score, to test this
        // a non 1.0F boost for each field is needed.
        QueryBuilder allBoosted2Query = customScoreQuery(matchAllQuery()).script("_score * 2");

        client.admin().indices().prepareRefresh().execute().actionGet();

        CustomFiltersScoreQueryBuilder customQuery = customFiltersScoreQuery(allBoosted2Query)
                .add(customFiltersScoreGroup("max")
                        .add(termFilter("field", "value4"), 2)
                        .add(termFilter("field", "value3"), 1.5f))
                .add(customFiltersScoreGroup("min")
                        .add(termFilter("field", "value4"), 3)
                        .add(termFilter("field", "value2"), 4))
                .add(customFiltersScoreGroup("multiply")
                        .add(termFilter("field", "value4"), 2)
                        .add(termFilter("field", "value1"), 3)
                        .add(termFilter("color", "red"), 5)
                        .add(termFilter("extra", "thing"), 0.25f))
                .add(customFiltersScoreGroup("total")
                        .add(termFilter("field", "value4"), 2)
                        .add(termFilter("color", "blue"), 3)
                        .add(termFilter("color", "red"), 6))
                .add(customFiltersScoreGroup("avg")
                        .add(termFilter("field", "value4"), 2)
                        .add(termFilter("field", "value1"), 3)
                        .add(termFilter("color", "red"), 5)
                        .add(termFilter("extra", "thing"), 4));

        SearchResponse searchResponse = client.prepareSearch("test")
                .setQuery(customQuery)
                .setExplain(true)
                .execute().actionGet();

        assertThat(Arrays.toString(searchResponse.shardFailures()), searchResponse.failedShards(), equalTo(0));

        assertThat(searchResponse.hits().totalHits(), equalTo(4l));
        logger.info("--> Hit[0] {} Explanation \n{}", searchResponse.hits().getAt(0).id(), searchResponse.hits().getAt(0).explanation());
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.hits().getAt(0).score(), equalTo(30.0f));
        assertThat(searchResponse.hits().getAt(1).id(), equalTo("2"));
        assertThat(searchResponse.hits().getAt(1).score(), equalTo(8.0f));
        assertThat(searchResponse.hits().getAt(2).id(), equalTo("4"));
        assertThat(searchResponse.hits().getAt(2).score(), equalTo(4.0f));
        assertThat(searchResponse.hits().getAt(3).id(), equalTo("3"));
        assertThat(searchResponse.hits().getAt(3).score(), equalTo(3.0f));

        customQuery.scoreMode("max");

        searchResponse = client.prepareSearch("test")
                .setQuery(customQuery)
                .setExplain(true)
                .execute().actionGet();

        assertThat(Arrays.toString(searchResponse.shardFailures()), searchResponse.failedShards(), equalTo(0));

        assertThat(searchResponse.hits().totalHits(), equalTo(4l));
        logger.info("--> Hit[0] {} Explanation \n{}", searchResponse.hits().getAt(0).id(), searchResponse.hits().getAt(0).explanation());
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.hits().getAt(0).score(), equalTo(30.0f));
        assertThat(searchResponse.hits().getAt(1).id(), equalTo("3"));
        assertThat(searchResponse.hits().getAt(1).score(), equalTo(12.0f));
        assertThat(searchResponse.hits().getAt(2).id(), equalTo("4"));
        assertThat(searchResponse.hits().getAt(2).score(), equalTo(10.0f));
        assertThat(searchResponse.hits().getAt(3).id(), equalTo("2"));
        assertThat(searchResponse.hits().getAt(3).score(), equalTo(8.0f));

        customQuery.scoreMode("min");

        searchResponse = client.prepareSearch("test")
                .setQuery(customQuery)
                .setExplain(true)
                .execute().actionGet();

        assertThat(Arrays.toString(searchResponse.shardFailures()), searchResponse.failedShards(), equalTo(0));

        assertThat(searchResponse.hits().totalHits(), equalTo(4l));
        logger.info("--> Hit[0] {} Explanation \n{}", searchResponse.hits().getAt(0).id(), searchResponse.hits().getAt(0).explanation());
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.hits().getAt(0).score(), equalTo(8.0f));
        assertThat(searchResponse.hits().getAt(1).id(), equalTo("2"));
        assertThat(searchResponse.hits().getAt(1).score(), equalTo(6.0f));
        assertThat(searchResponse.hits().getAt(2).id(), equalTo("4"));
        assertThat(searchResponse.hits().getAt(2).score(), equalTo(4.0f));
        assertThat(searchResponse.hits().getAt(3).id(), equalTo("3"));
        assertThat(searchResponse.hits().getAt(3).score(), equalTo(3.0f));

        customQuery.scoreMode("total");

        searchResponse = client.prepareSearch("test")
                .setQuery(customQuery)
                .setExplain(true)
                .execute().actionGet();

        assertThat(Arrays.toString(searchResponse.shardFailures()), searchResponse.failedShards(), equalTo(0));

        assertThat(searchResponse.hits().totalHits(), equalTo(4l));
        logger.info("--> Hit[0] {} Explanation \n{}", searchResponse.hits().getAt(0).id(), searchResponse.hits().getAt(0).explanation());
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.hits().getAt(0).score(), equalTo(50.0f));
        assertThat(searchResponse.hits().getAt(1).id(), equalTo("3"));
        assertThat(searchResponse.hits().getAt(1).score(), equalTo(35.0f));
        assertThat(searchResponse.hits().getAt(2).id(), equalTo("4"));
        assertThat(searchResponse.hits().getAt(2).score(), equalTo(28.0f));
        assertThat(searchResponse.hits().getAt(3).id(), equalTo("2"));
        assertThat(searchResponse.hits().getAt(3).score(), equalTo(14.0f));


        customQuery.scoreMode("avg");

        searchResponse = client.prepareSearch("test")
                .setQuery(customQuery)
                .setExplain(true)
                .execute().actionGet();

        assertThat(Arrays.toString(searchResponse.shardFailures()), searchResponse.failedShards(), equalTo(0));

        assertThat(searchResponse.hits().totalHits(), equalTo(4l));
        logger.info("--> Hit[0] {} Explanation \n{}", searchResponse.hits().getAt(0).id(), searchResponse.hits().getAt(0).explanation());
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.hits().getAt(0).score(), equalTo(16.666666f));
        assertThat(searchResponse.hits().getAt(1).id(), equalTo("3"));
        assertThat(searchResponse.hits().getAt(1).score(), equalTo(8.75f));
        assertThat(searchResponse.hits().getAt(2).id(), equalTo("2"));
        assertThat(searchResponse.hits().getAt(2).score(), equalTo(7.0f));
        assertThat(searchResponse.hits().getAt(3).id(), equalTo("4"));
        assertThat(searchResponse.hits().getAt(3).score(), equalTo(5.6f));

        customQuery.scoreMode("multiply");

        searchResponse = client.prepareSearch("test")
                .setQuery(customQuery)
                .setExplain(true)
                .execute().actionGet();

        assertThat(Arrays.toString(searchResponse.shardFailures()), searchResponse.failedShards(), equalTo(0));

        assertThat(searchResponse.hits().totalHits(), equalTo(4l));
        logger.info("--> Hit[0] {} Explanation \n{}", searchResponse.hits().getAt(0).id(), searchResponse.hits().getAt(0).explanation());
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("4"));
        assertThat(searchResponse.hits().getAt(0).score(), equalTo(3840f));
        assertThat(searchResponse.hits().getAt(1).id(), equalTo("3"));
        assertThat(searchResponse.hits().getAt(1).score(), equalTo(3600f));
        assertThat(searchResponse.hits().getAt(2).id(), equalTo("1"));
        assertThat(searchResponse.hits().getAt(2).score(), equalTo(2880f));
        assertThat(searchResponse.hits().getAt(3).id(), equalTo("2"));
        assertThat(searchResponse.hits().getAt(3).score(), equalTo(48f));

        // test grouping where the group has no results
        customQuery = customFiltersScoreQuery(allBoosted2Query)
                .add(termFilter("field", "fail"), 2)
                .add(customFiltersScoreGroup("max")
                        .add(termFilter("color", "failing"), 999))
                .add(customFiltersScoreGroup("total")
                        .add(termFilter("color", "red"), 6)
                        .add(termFilter("field", "value1"), 3))
                .add(termFilter("field", "failure"), 3);

        searchResponse = client.prepareSearch("test")
                .setQuery(customQuery)
                .setExplain(true)
                .execute().actionGet();

        assertThat(Arrays.toString(searchResponse.shardFailures()), searchResponse.failedShards(), equalTo(0));

        assertThat(searchResponse.hits().totalHits(), equalTo(4l));
        logger.info("--> Hit[0] {} Explanation \n{}", searchResponse.hits().getAt(0).id(), searchResponse.hits().getAt(0).explanation());
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.hits().getAt(0).score(), equalTo(18f));
        assertThat(searchResponse.hits().getAt(1).id(), equalTo("3"));
        assertThat(searchResponse.hits().getAt(1).score(), equalTo(12f));
        assertThat(searchResponse.hits().getAt(2).id(), anyOf(equalTo("2"), equalTo("4")));
        assertThat(searchResponse.hits().getAt(2).score(), equalTo(2f));
        assertThat(searchResponse.hits().getAt(3).id(), anyOf(equalTo("2"), equalTo("4")));
        assertThat(searchResponse.hits().getAt(3).score(), equalTo(2f));

        // test nested grouping where more than one group has results, also double up the first implied group with another root group to make sure that accident works fine.
        customQuery = customFiltersScoreQuery(allBoosted2Query)
                .add(customFiltersScoreGroup("total")
                        .add(termFilter("field", "fail"), 2)
                        .add(customFiltersScoreGroup("max")
                                .add(termFilter("color", "blue"), 5)
                                .add(termFilter("field", "value2"), 1))
                        .add(customFiltersScoreGroup("total")
                                .add(termFilter("color", "red"), 6)
                                .add(termFilter("field", "value1"), 3))
                        .add(termFilter("field", "value4"), 2));

        searchResponse = client.prepareSearch("test")
                .setQuery(customQuery)
                .setExplain(true)
                .execute().actionGet();

        assertThat(Arrays.toString(searchResponse.shardFailures()), searchResponse.failedShards(), equalTo(0));

        assertThat(searchResponse.hits().totalHits(), equalTo(4l));
        logger.info("--> Hit[0] {} Explanation \n{}", searchResponse.hits().getAt(0).id(), searchResponse.hits().getAt(0).explanation());
        assertThat(searchResponse.hits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.hits().getAt(0).score(), equalTo(18f));
        assertThat(searchResponse.hits().getAt(1).id(), equalTo("4"));
        assertThat(searchResponse.hits().getAt(1).score(), equalTo(14f));
        assertThat(searchResponse.hits().getAt(2).id(), equalTo("3"));
        assertThat(searchResponse.hits().getAt(2).score(), equalTo(12f));
        assertThat(searchResponse.hits().getAt(3).id(), equalTo("2"));
        assertThat(searchResponse.hits().getAt(3).score(), equalTo(10f));
    }
}