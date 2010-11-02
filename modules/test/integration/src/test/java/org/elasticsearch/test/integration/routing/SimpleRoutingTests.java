/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.test.integration.routing;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.xcontent.QueryBuilders;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class SimpleRoutingTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass public void createNodes() throws Exception {
        startNode("node1");
        startNode("node2");
        client = getClient();
    }

    @AfterClass public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node1");
    }

    @Test public void testSimpleCrudRouting() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        logger.info("--> indexing with id [1], and routing [0]");
        client.prepareIndex("test", "type1", "1").setRouting("0").setSource("field", "value1").setRefresh(true).execute().actionGet();
        logger.info("--> verifying get with no routing, should not find anything");
        for (int i = 0; i < 5; i++) {
            assertThat(client.prepareGet("test", "type1", "1").execute().actionGet().exists(), equalTo(false));
        }
        logger.info("--> verifying get with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client.prepareGet("test", "type1", "1").setRouting("0").execute().actionGet().exists(), equalTo(true));
        }

        logger.info("--> deleting with no routing, should not delete anything");
        client.prepareDelete("test", "type1", "1").setRefresh(true).execute().actionGet();
        for (int i = 0; i < 5; i++) {
            assertThat(client.prepareGet("test", "type1", "1").execute().actionGet().exists(), equalTo(false));
            assertThat(client.prepareGet("test", "type1", "1").setRouting("0").execute().actionGet().exists(), equalTo(true));
        }

        logger.info("--> deleting with routing, should delete");
        client.prepareDelete("test", "type1", "1").setRouting("0").setRefresh(true).execute().actionGet();
        for (int i = 0; i < 5; i++) {
            assertThat(client.prepareGet("test", "type1", "1").execute().actionGet().exists(), equalTo(false));
            assertThat(client.prepareGet("test", "type1", "1").setRouting("0").execute().actionGet().exists(), equalTo(false));
        }

        logger.info("--> indexing with id [1], and routing [0]");
        client.prepareIndex("test", "type1", "1").setRouting("0").setSource("field", "value1").setRefresh(true).execute().actionGet();
        logger.info("--> verifying get with no routing, should not find anything");
        for (int i = 0; i < 5; i++) {
            assertThat(client.prepareGet("test", "type1", "1").execute().actionGet().exists(), equalTo(false));
        }
        logger.info("--> verifying get with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client.prepareGet("test", "type1", "1").setRouting("0").execute().actionGet().exists(), equalTo(true));
        }

        logger.info("--> deleting_by_query with 1 as routing, should not delete anything");
        client.prepareDeleteByQuery().setQuery(matchAllQuery()).setRouting("1").execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 5; i++) {
            assertThat(client.prepareGet("test", "type1", "1").execute().actionGet().exists(), equalTo(false));
            assertThat(client.prepareGet("test", "type1", "1").setRouting("0").execute().actionGet().exists(), equalTo(true));
        }

        logger.info("--> deleting_by_query with , should delete");
        client.prepareDeleteByQuery().setQuery(matchAllQuery()).setRouting("0").execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();
        for (int i = 0; i < 5; i++) {
            assertThat(client.prepareGet("test", "type1", "1").execute().actionGet().exists(), equalTo(false));
            assertThat(client.prepareGet("test", "type1", "1").setRouting("0").execute().actionGet().exists(), equalTo(false));
        }
    }

    @Test public void testSimpleSearchRouting() {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        logger.info("--> indexing with id [1], and routing [0]");
        client.prepareIndex("test", "type1", "1").setRouting("0").setSource("field", "value1").setRefresh(true).execute().actionGet();
        logger.info("--> verifying get with no routing, should not find anything");
        for (int i = 0; i < 5; i++) {
            assertThat(client.prepareGet("test", "type1", "1").execute().actionGet().exists(), equalTo(false));
        }
        logger.info("--> verifying get with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client.prepareGet("test", "type1", "1").setRouting("0").execute().actionGet().exists(), equalTo(true));
        }

        logger.info("--> search with no routing, should fine one");
        for (int i = 0; i < 5; i++) {
            assertThat(client.prepareSearch().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().hits().totalHits(), equalTo(1l));
        }

        logger.info("--> search with wrong routing, should not find");
        for (int i = 0; i < 5; i++) {
            assertThat(client.prepareSearch().setRouting("1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().hits().totalHits(), equalTo(0l));
            assertThat(client.prepareCount().setRouting("1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().count(), equalTo(0l));
        }

        logger.info("--> search with correct routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client.prepareSearch().setRouting("0").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().hits().totalHits(), equalTo(1l));
            assertThat(client.prepareCount().setRouting("0").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().count(), equalTo(1l));
        }

        logger.info("--> indexing with id [2], and routing [1]");
        client.prepareIndex("test", "type1", "2").setRouting("1").setSource("field", "value1").setRefresh(true).execute().actionGet();

        logger.info("--> search with no routing, should fine two");
        for (int i = 0; i < 5; i++) {
            assertThat(client.prepareSearch().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().hits().totalHits(), equalTo(2l));
            assertThat(client.prepareCount().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().count(), equalTo(2l));
        }

        logger.info("--> search with 0 routing, should find one");
        for (int i = 0; i < 5; i++) {
            assertThat(client.prepareSearch().setRouting("0").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().hits().totalHits(), equalTo(1l));
            assertThat(client.prepareCount().setRouting("0").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().count(), equalTo(1l));
        }

        logger.info("--> search with 1 routing, should find one");
        for (int i = 0; i < 5; i++) {
            assertThat(client.prepareSearch().setRouting("1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().hits().totalHits(), equalTo(1l));
            assertThat(client.prepareCount().setRouting("1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().count(), equalTo(1l));
        }

        logger.info("--> search with 0,1 routings , should find two");
        for (int i = 0; i < 5; i++) {
            assertThat(client.prepareSearch().setRouting("0", "1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().hits().totalHits(), equalTo(2l));
            assertThat(client.prepareCount().setRouting("0", "1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().count(), equalTo(2l));
        }

        logger.info("--> search with 0,1,0 routings , should find two");
        for (int i = 0; i < 5; i++) {
            assertThat(client.prepareSearch().setRouting("0", "1", "0").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().hits().totalHits(), equalTo(2l));
            assertThat(client.prepareCount().setRouting("0", "1", "0").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().count(), equalTo(2l));
        }
    }
}
