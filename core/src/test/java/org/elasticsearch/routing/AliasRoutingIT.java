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

package org.elasticsearch.routing;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.cluster.metadata.AliasAction.newAddAliasAction;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class AliasRoutingIT extends ESIntegTestCase {

    @Override
    protected int minimumNumberOfShards() {
        return 2;
    }

    public void testAliasCrudRouting() throws Exception {
        createIndex("test");
        ensureGreen();
        assertAcked(admin().indices().prepareAliases().addAliasAction(newAddAliasAction("test", "alias0").routing("0")));

        logger.info("--> indexing with id [1], and routing [0] using alias");
        client().prepareIndex("alias0", "type1", "1").setSource("field", "value1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        logger.info("--> verifying get with no routing, should not find anything");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(false));
        }
        logger.info("--> verifying get with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "type1", "1").setRouting("0").execute().actionGet().isExists(), equalTo(true));
        }

        logger.info("--> verifying get with routing alias, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("alias0", "type1", "1").execute().actionGet().isExists(), equalTo(true));
        }

        logger.info("--> updating with id [1] and routing through alias");
        client().prepareUpdate("alias0", "type1", "1")
                .setUpsert(XContentFactory.jsonBuilder().startObject().field("field", 1).endObject())
                .setDoc("field", "value2")
                .execute().actionGet();
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("alias0", "type1", "1").execute().actionGet().isExists(), equalTo(true));
            assertThat(client().prepareGet("alias0", "type1", "1").execute().actionGet().getSourceAsMap().get("field").toString(), equalTo("value2"));
        }


        logger.info("--> deleting with no routing, should not delete anything");
        client().prepareDelete("test", "type1", "1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(false));
            assertThat(client().prepareGet("test", "type1", "1").setRouting("0").execute().actionGet().isExists(), equalTo(true));
            assertThat(client().prepareGet("alias0", "type1", "1").execute().actionGet().isExists(), equalTo(true));
        }

        logger.info("--> deleting with routing alias, should delete");
        client().prepareDelete("alias0", "type1", "1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(false));
            assertThat(client().prepareGet("test", "type1", "1").setRouting("0").execute().actionGet().isExists(), equalTo(false));
            assertThat(client().prepareGet("alias0", "type1", "1").execute().actionGet().isExists(), equalTo(false));
        }

        logger.info("--> indexing with id [1], and routing [0] using alias");
        client().prepareIndex("alias0", "type1", "1").setSource("field", "value1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        logger.info("--> verifying get with no routing, should not find anything");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(false));
        }
        logger.info("--> verifying get with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "type1", "1").setRouting("0").execute().actionGet().isExists(), equalTo(true));
            assertThat(client().prepareGet("alias0", "type1", "1").execute().actionGet().isExists(), equalTo(true));
        }
    }

    public void testAliasSearchRouting() throws Exception {
        createIndex("test");
        ensureGreen();
        assertAcked(admin().indices().prepareAliases()
                .addAliasAction(newAddAliasAction("test", "alias"))
                .addAliasAction(newAddAliasAction("test", "alias0").routing("0"))
                .addAliasAction(newAddAliasAction("test", "alias1").routing("1"))
                .addAliasAction(newAddAliasAction("test", "alias01").searchRouting("0,1")));

        logger.info("--> indexing with id [1], and routing [0] using alias");
        client().prepareIndex("alias0", "type1", "1").setSource("field", "value1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        logger.info("--> verifying get with no routing, should not find anything");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(false));
        }
        logger.info("--> verifying get with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("alias0", "type1", "1").execute().actionGet().isExists(), equalTo(true));
        }

        logger.info("--> search with no routing, should fine one");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(1L));
        }

        logger.info("--> search with wrong routing, should not find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch().setRouting("1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(0L));
            assertThat(client().prepareSearch().setSize(0).setRouting("1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(0L));
            assertThat(client().prepareSearch("alias1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(0L));
            assertThat(client().prepareSearch("alias1").setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(0L));
        }

        logger.info("--> search with correct routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch().setRouting("0").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(1L));
            assertThat(client().prepareSearch().setSize(0).setRouting("0").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(1L));
            assertThat(client().prepareSearch("alias0").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(1L));
            assertThat(client().prepareSearch("alias0").setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(1L));
        }

        logger.info("--> indexing with id [2], and routing [1] using alias");
        client().prepareIndex("alias1", "type1", "2").setSource("field", "value1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();

        logger.info("--> search with no routing, should fine two");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(2L));
            assertThat(client().prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(2L));
        }

        logger.info("--> search with 0 routing, should find one");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch().setRouting("0").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(1L));
            assertThat(client().prepareSearch().setSize(0).setRouting("0").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(1L));
            assertThat(client().prepareSearch("alias0").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(1L));
            assertThat(client().prepareSearch("alias0").setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(1L));
        }

        logger.info("--> search with 1 routing, should find one");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch().setRouting("1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(1L));
            assertThat(client().prepareSearch().setSize(0).setRouting("1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(1L));
            assertThat(client().prepareSearch("alias1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(1L));
            assertThat(client().prepareSearch("alias1").setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(1L));
        }

        logger.info("--> search with 0,1 routings , should find two");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch().setRouting("0", "1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(2L));
            assertThat(client().prepareSearch().setSize(0).setRouting("0", "1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(2L));
            assertThat(client().prepareSearch("alias01").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(2L));
            assertThat(client().prepareSearch("alias01").setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(2L));
        }

        logger.info("--> search with two routing aliases , should find two");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch("alias0", "alias1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(2L));
            assertThat(client().prepareSearch("alias0", "alias1").setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(2L));
        }

        logger.info("--> search with alias0, alias1 and alias01, should find two");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch("alias0", "alias1", "alias01").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(2L));
            assertThat(client().prepareSearch("alias0", "alias1", "alias01").setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(2L));
        }

        logger.info("--> search with test, alias0 and alias1, should find two");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch("test", "alias0", "alias1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(2L));
            assertThat(client().prepareSearch("test", "alias0", "alias1").setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(2L));
        }

    }

    public void testAliasSearchRoutingWithTwoIndices() throws Exception {
        createIndex("test-a");
        createIndex("test-b");
        ensureGreen();
        assertAcked(admin().indices().prepareAliases()
                .addAliasAction(newAddAliasAction("test-a", "alias-a0").routing("0"))
                .addAliasAction(newAddAliasAction("test-a", "alias-a1").routing("1"))
                .addAliasAction(newAddAliasAction("test-b", "alias-b0").routing("0"))
                .addAliasAction(newAddAliasAction("test-b", "alias-b1").routing("1"))
                .addAliasAction(newAddAliasAction("test-a", "alias-ab").searchRouting("0"))
                .addAliasAction(newAddAliasAction("test-b", "alias-ab").searchRouting("1")));
        ensureGreen(); // wait for events again to make sure we got the aliases on all nodes
        logger.info("--> indexing with id [1], and routing [0] using alias to test-a");
        client().prepareIndex("alias-a0", "type1", "1").setSource("field", "value1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        logger.info("--> verifying get with no routing, should not find anything");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test-a", "type1", "1").execute().actionGet().isExists(), equalTo(false));
        }
        logger.info("--> verifying get with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("alias-a0", "type1", "1").execute().actionGet().isExists(), equalTo(true));
        }

        logger.info("--> indexing with id [0], and routing [1] using alias to test-b");
        client().prepareIndex("alias-b1", "type1", "1").setSource("field", "value1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        logger.info("--> verifying get with no routing, should not find anything");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test-a", "type1", "1").execute().actionGet().isExists(), equalTo(false));
        }
        logger.info("--> verifying get with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("alias-b1", "type1", "1").execute().actionGet().isExists(), equalTo(true));
        }


        logger.info("--> search with alias-a1,alias-b0, should not find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch("alias-a1", "alias-b0").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(0L));
            assertThat(client().prepareSearch("alias-a1", "alias-b0").setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(0L));
        }

        logger.info("--> search with alias-ab, should find two");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch("alias-ab").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(2L));
            assertThat(client().prepareSearch("alias-ab").setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(2L));
        }

        logger.info("--> search with alias-a0,alias-b1 should find two");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch("alias-a0", "alias-b1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(2L));
            assertThat(client().prepareSearch("alias-a0", "alias-b1").setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(2L));
        }
    }

    /*
    See https://github.com/elastic/elasticsearch/issues/2682
    Searching on more than one index, if one of those is an alias with configured routing, the shards that belonged
    to the other indices (without routing) were not taken into account in PlainOperationRouting#searchShards.
    That affected the number of shards that we executed the search on, thus some documents were missing in the search results.
     */
    public void testAliasSearchRoutingWithConcreteAndAliasedIndices_issue2682() throws Exception {
        createIndex("index", "index_2");
        ensureGreen();
        assertAcked(admin().indices().prepareAliases()
                .addAliasAction(newAddAliasAction("index", "index_1").routing("1")));

        logger.info("--> indexing on index_1 which is an alias for index with routing [1]");
        client().prepareIndex("index_1", "type1", "1").setSource("field", "value1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        logger.info("--> indexing on index_2 which is a concrete index");
        client().prepareIndex("index_2", "type2", "2").setSource("field", "value2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();


        logger.info("--> search all on index_* should find two");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch("index_*").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(2L));
        }
    }

    /*
    See https://github.com/elastic/elasticsearch/pull/3268
    Searching on more than one index, if one of those is an alias with configured routing, the shards that belonged
    to the other indices (without routing) were not taken into account in PlainOperationRouting#searchShardsCount.
    That could cause returning 1, which led to forcing the QUERY_AND_FETCH mode.
    As a result, (size * number of hit shards) results were returned and no reduce phase was taking place.
     */
    public void testAliasSearchRoutingWithConcreteAndAliasedIndices_issue3268() throws Exception {
        createIndex("index", "index_2");
        ensureGreen();
        assertAcked(admin().indices().prepareAliases()
                .addAliasAction(newAddAliasAction("index", "index_1").routing("1")));

        logger.info("--> indexing on index_1 which is an alias for index with routing [1]");
        client().prepareIndex("index_1", "type1", "1").setSource("field", "value1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        logger.info("--> indexing on index_2 which is a concrete index");
        client().prepareIndex("index_2", "type2", "2").setSource("field", "value2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();

        SearchResponse searchResponse = client().prepareSearch("index_*").setSearchType(SearchType.QUERY_THEN_FETCH).setSize(1).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();

        logger.info("--> search all on index_* should find two");
        assertThat(searchResponse.getHits().totalHits(), equalTo(2L));
        //Let's make sure that, even though 2 docs are available, only one is returned according to the size we set in the request
        //Therefore the reduce phase has taken place, which proves that the QUERY_AND_FETCH search type wasn't erroneously forced.
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
    }

    public void testIndexingAliasesOverTime() throws Exception {
        createIndex("test");
        ensureGreen();
        logger.info("--> creating alias with routing [3]");
        assertAcked(admin().indices().prepareAliases()
                .addAliasAction(newAddAliasAction("test", "alias").routing("3")));

        logger.info("--> indexing with id [0], and routing [3]");
        client().prepareIndex("alias", "type1", "0").setSource("field", "value1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        logger.info("--> verifying get with no routing, should not find anything");

        logger.info("--> verifying get and search with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "type1", "0").setRouting("3").execute().actionGet().isExists(), equalTo(true));
            assertThat(client().prepareSearch("alias").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(1L));
            assertThat(client().prepareSearch("alias").setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(1L));
        }

        logger.info("--> creating alias with routing [4]");
        assertAcked(admin().indices().prepareAliases()
                .addAliasAction(newAddAliasAction("test", "alias").routing("4")));

        logger.info("--> verifying search with wrong routing should not find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch("alias").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(0L));
            assertThat(client().prepareSearch("alias").setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(0L));
        }

        logger.info("--> creating alias with search routing [3,4] and index routing 4");
        assertAcked(client().admin().indices().prepareAliases()
                .addAliasAction(newAddAliasAction("test", "alias").searchRouting("3,4").indexRouting("4")));

        logger.info("--> indexing with id [1], and routing [4]");
        client().prepareIndex("alias", "type1", "1").setSource("field", "value2").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        logger.info("--> verifying get with no routing, should not find anything");

        logger.info("--> verifying get and search with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "type1", "0").setRouting("3").execute().actionGet().isExists(), equalTo(true));
            assertThat(client().prepareGet("test", "type1", "1").setRouting("4").execute().actionGet().isExists(), equalTo(true));
            assertThat(client().prepareSearch("alias").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(2L));
            assertThat(client().prepareSearch("alias").setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().totalHits(), equalTo(2L));
        }
    }

}
