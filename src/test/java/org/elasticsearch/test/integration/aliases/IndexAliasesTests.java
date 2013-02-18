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

package org.elasticsearch.test.integration.aliases;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Sets.newHashSet;
import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 *
 */
@Test
public class IndexAliasesTests extends AbstractNodesTests {

    protected Client client1;
    protected Client client2;
    protected Client[] clients;
    protected Random random = new Random();

    @BeforeClass
    public void startNodes() {
        Settings nodeSettings = ImmutableSettings.settingsBuilder()
                .put("action.auto_create_index", false)
                .build();

        startNode("server1", nodeSettings);
        startNode("server2", nodeSettings);
        client1 = getClient1();
        client2 = getClient2();
        clients = new Client[]{client1, client2};
    }

    @AfterClass
    public void closeNodes() {
        client1.close();
        client2.close();
        closeAllNodes();
    }

    protected Client getClient1() {
        return client("server1");
    }

    protected Client getClient2() {
        return client("server2");
    }

    protected Client getClient() {
        return clients[random.nextInt(clients.length)];
    }

    @Test
    public void testAliases() throws Exception {
        // delete all indices
        client1.admin().indices().prepareDelete().execute().actionGet();

        logger.info("--> creating index [test]");
        client1.admin().indices().create(createIndexRequest("test")).actionGet();

        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client1.admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus()).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        try {
            logger.info("--> indexing against [alias1], should fail");
            client1.index(indexRequest("alias1").setType("type1").setId("1").setSource(source("1", "test"))).actionGet();
            assert false : "index [alias1] should not exists";
        } catch (IndexMissingException e) {
            assertThat(e.index().name(), equalTo("alias1"));
        }

        logger.info("--> aliasing index [test] with [alias1]");
        client1.admin().indices().prepareAliases().addAlias("test", "alias1").execute().actionGet();

        logger.info("--> indexing against [alias1], should work now");
        IndexResponse indexResponse = client1.index(indexRequest("alias1").setType("type1").setId("1").setSource(source("1", "test"))).actionGet();
        assertThat(indexResponse.getIndex(), equalTo("test"));

        logger.info("--> creating index [test]");
        client1.admin().indices().create(createIndexRequest("test_x")).actionGet();

        logger.info("--> running cluster_health");
        clusterHealth = client1.admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus()).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> remove [alias1], Aliasing index [test_x] with [alias1]");
        client1.admin().indices().aliases(indexAliasesRequest().removeAlias("test", "alias1").addAlias("test_x", "alias1")).actionGet();
        Thread.sleep(300);

        logger.info("--> indexing against [alias1], should work against [test_x]");
        indexResponse = client1.index(indexRequest("alias1").setType("type1").setId("1").setSource(source("1", "test"))).actionGet();
        assertThat(indexResponse.getIndex(), equalTo("test_x"));
    }

    @Test
    public void testFailedFilter() throws Exception {
        // delete all indices
        client1.admin().indices().prepareDelete().execute().actionGet();

        logger.info("--> creating index [test]");
        client1.admin().indices().create(createIndexRequest("test")).actionGet();

        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client1.admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus()).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        try {
            logger.info("--> aliasing index [test] with [alias1] and filter [t]");
            client1.admin().indices().prepareAliases().addAlias("test", "alias1", "{ t }").execute().actionGet();
            assert false;
        } catch (Exception e) {
            // all is well
        }
    }

    @Test
    public void testFilteringAliases() throws Exception {
        // delete all indices
        client1.admin().indices().prepareDelete().execute().actionGet();

        logger.info("--> creating index [test]");
        client1.admin().indices().create(createIndexRequest("test")).actionGet();

        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client1.admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus()).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> aliasing index [test] with [alias1] and filter [user:kimchy]");
        FilterBuilder filter = termFilter("user", "kimchy");
        client1.admin().indices().prepareAliases().addAlias("test", "alias1", filter).execute().actionGet();

        // For now just making sure that filter was stored with the alias
        logger.info("--> making sure that filter was stored with alias [alias1] and filter [user:kimchy]");
        ClusterState clusterState = client1.admin().cluster().prepareState().execute().actionGet().getState();
        IndexMetaData indexMd = clusterState.metaData().index("test");
        assertThat(indexMd.aliases().get("alias1").filter().string(), equalTo("{\"term\":{\"user\":\"kimchy\"}}"));

    }

    @Test
    public void testSearchingFilteringAliasesSingleIndex() throws Exception {
        // delete all indices
        client1.admin().indices().prepareDelete().execute().actionGet();

        logger.info("--> creating index [test]");
        client1.admin().indices().create(createIndexRequest("test")).actionGet();

        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client1.admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus()).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> adding filtering aliases to index [test]");
        client1.admin().indices().prepareAliases().addAlias("test", "alias1").execute().actionGet();
        client1.admin().indices().prepareAliases().addAlias("test", "alias2").execute().actionGet();
        client1.admin().indices().prepareAliases().addAlias("test", "foos", termFilter("name", "foo")).execute().actionGet();
        client1.admin().indices().prepareAliases().addAlias("test", "bars", termFilter("name", "bar")).execute().actionGet();
        client1.admin().indices().prepareAliases().addAlias("test", "tests", termFilter("name", "test")).execute().actionGet();

        logger.info("--> indexing against [test]");
        client1.index(indexRequest("test").setType("type1").setId("1").setSource(source("1", "foo test")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test").setType("type1").setId("2").setSource(source("2", "bar test")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test").setType("type1").setId("3").setSource(source("3", "baz test")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test").setType("type1").setId("4").setSource(source("4", "something else")).setRefresh(true)).actionGet();

        logger.info("--> checking single filtering alias search");
        SearchResponse searchResponse = client1.prepareSearch("foos").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1");

        searchResponse = client1.prepareSearch("tests").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2", "3");

        searchResponse = client1.prepareSearch("foos", "bars").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2");

        logger.info("--> checking single non-filtering alias search");
        searchResponse = client1.prepareSearch("alias1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2", "3", "4");

        logger.info("--> checking non-filtering alias and filtering alias search");
        searchResponse = client1.prepareSearch("alias1", "foos").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2", "3", "4");

        logger.info("--> checking index and filtering alias search");
        searchResponse = client1.prepareSearch("test", "foos").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2", "3", "4");
    }

    @Test
    public void testSearchingFilteringAliasesTwoIndices() throws Exception {
        // delete all indices
        client1.admin().indices().prepareDelete().execute().actionGet();

        logger.info("--> creating index [test1]");
        client1.admin().indices().create(createIndexRequest("test1")).actionGet();

        logger.info("--> creating index [test2]");
        client1.admin().indices().create(createIndexRequest("test2")).actionGet();

        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client1.admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus()).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> adding filtering aliases to index [test1]");
        client1.admin().indices().prepareAliases().addAlias("test1", "aliasToTest1").execute().actionGet();
        client1.admin().indices().prepareAliases().addAlias("test1", "aliasToTests").execute().actionGet();
        client1.admin().indices().prepareAliases().addAlias("test1", "foos", termFilter("name", "foo")).execute().actionGet();
        client1.admin().indices().prepareAliases().addAlias("test1", "bars", termFilter("name", "bar")).execute().actionGet();

        logger.info("--> adding filtering aliases to index [test2]");
        client1.admin().indices().prepareAliases().addAlias("test2", "aliasToTest2").execute().actionGet();
        client1.admin().indices().prepareAliases().addAlias("test2", "aliasToTests").execute().actionGet();
        client1.admin().indices().prepareAliases().addAlias("test2", "foos", termFilter("name", "foo")).execute().actionGet();

        logger.info("--> indexing against [test1]");
        client1.index(indexRequest("test1").setType("type1").setId("1").setSource(source("1", "foo test")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test1").setType("type1").setId("2").setSource(source("2", "bar test")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test1").setType("type1").setId("3").setSource(source("3", "baz test")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test1").setType("type1").setId("4").setSource(source("4", "something else")).setRefresh(true)).actionGet();

        logger.info("--> indexing against [test2]");
        client1.index(indexRequest("test2").setType("type1").setId("5").setSource(source("5", "foo test")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test2").setType("type1").setId("6").setSource(source("6", "bar test")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test2").setType("type1").setId("7").setSource(source("7", "baz test")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test2").setType("type1").setId("8").setSource(source("8", "something else")).setRefresh(true)).actionGet();

        logger.info("--> checking filtering alias for two indices");
        SearchResponse searchResponse = client1.prepareSearch("foos").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "5");
        assertThat(client1.prepareCount("foos").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(2L));

        logger.info("--> checking filtering alias for one index");
        searchResponse = client1.prepareSearch("bars").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "2");
        assertThat(client1.prepareCount("bars").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(1L));

        logger.info("--> checking filtering alias for two indices and one complete index");
        searchResponse = client1.prepareSearch("foos", "test1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2", "3", "4", "5");
        assertThat(client1.prepareCount("foos", "test1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(5L));

        logger.info("--> checking filtering alias for two indices and non-filtering alias for one index");
        searchResponse = client1.prepareSearch("foos", "aliasToTest1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2", "3", "4", "5");
        assertThat(client1.prepareCount("foos", "aliasToTest1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(5L));

        logger.info("--> checking filtering alias for two indices and non-filtering alias for both indices");
        searchResponse = client1.prepareSearch("foos", "aliasToTests").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(8L));
        assertThat(client1.prepareCount("foos", "aliasToTests").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(8L));

        logger.info("--> checking filtering alias for two indices and non-filtering alias for both indices");
        searchResponse = client1.prepareSearch("foos", "aliasToTests").setQuery(QueryBuilders.termQuery("name", "something")).execute().actionGet();
        assertHits(searchResponse.getHits(), "4", "8");
        assertThat(client1.prepareCount("foos", "aliasToTests").setQuery(QueryBuilders.termQuery("name", "something")).execute().actionGet().getCount(), equalTo(2L));
    }

    @Test
    public void testSearchingFilteringAliasesMultipleIndices() throws Exception {
        // delete all indices
        client1.admin().indices().prepareDelete().execute().actionGet();

        logger.info("--> creating indices");
        client1.admin().indices().create(createIndexRequest("test1")).actionGet();
        client1.admin().indices().create(createIndexRequest("test2")).actionGet();
        client1.admin().indices().create(createIndexRequest("test3")).actionGet();

        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client1.admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus()).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> adding aliases to indices");
        client1.admin().indices().prepareAliases().addAlias("test1", "alias12").execute().actionGet();
        client1.admin().indices().prepareAliases().addAlias("test2", "alias12").execute().actionGet();

        logger.info("--> adding filtering aliases to indices");
        client1.admin().indices().prepareAliases().addAlias("test1", "filter1", termFilter("name", "test1")).execute().actionGet();

        client1.admin().indices().prepareAliases().addAlias("test2", "filter23", termFilter("name", "foo")).execute().actionGet();
        client1.admin().indices().prepareAliases().addAlias("test3", "filter23", termFilter("name", "foo")).execute().actionGet();

        client1.admin().indices().prepareAliases().addAlias("test1", "filter13", termFilter("name", "baz")).execute().actionGet();
        client1.admin().indices().prepareAliases().addAlias("test3", "filter13", termFilter("name", "baz")).execute().actionGet();

        logger.info("--> indexing against [test1]");
        client1.index(indexRequest("test1").setType("type1").setId("11").setSource(source("11", "foo test1")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test1").setType("type1").setId("12").setSource(source("12", "bar test1")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test1").setType("type1").setId("13").setSource(source("13", "baz test1")).setRefresh(true)).actionGet();

        client1.index(indexRequest("test2").setType("type1").setId("21").setSource(source("21", "foo test2")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test2").setType("type1").setId("22").setSource(source("22", "bar test2")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test2").setType("type1").setId("23").setSource(source("23", "baz test2")).setRefresh(true)).actionGet();

        client1.index(indexRequest("test3").setType("type1").setId("31").setSource(source("31", "foo test3")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test3").setType("type1").setId("32").setSource(source("32", "bar test3")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test3").setType("type1").setId("33").setSource(source("33", "baz test3")).setRefresh(true)).actionGet();

        logger.info("--> checking filtering alias for multiple indices");
        SearchResponse searchResponse = client1.prepareSearch("filter23", "filter13").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "21", "31", "13", "33");
        assertThat(client1.prepareCount("filter23", "filter13").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(4L));

        searchResponse = client1.prepareSearch("filter23", "filter1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "21", "31", "11", "12", "13");
        assertThat(client1.prepareCount("filter23", "filter1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(5L));

        searchResponse = client1.prepareSearch("filter13", "filter1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "11", "12", "13", "33");
        assertThat(client1.prepareCount("filter13", "filter1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(4L));

        searchResponse = client1.prepareSearch("filter13", "filter1", "filter23").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "11", "12", "13", "21", "31", "33");
        assertThat(client1.prepareCount("filter13", "filter1", "filter23").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(6L));

        searchResponse = client1.prepareSearch("filter23", "filter13", "test2").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "21", "22", "23", "31", "13", "33");
        assertThat(client1.prepareCount("filter23", "filter13", "test2").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(6L));

        searchResponse = client1.prepareSearch("filter23", "filter13", "test1", "test2").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "11", "12", "13", "21", "22", "23", "31", "33");
        assertThat(client1.prepareCount("filter23", "filter13", "test1", "test2").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(8L));

    }

    @Test
    public void testDeletingByQueryFilteringAliases() throws Exception {
        // delete all indices
        client1.admin().indices().prepareDelete().execute().actionGet();

        logger.info("--> creating index [test1]");
        client1.admin().indices().create(createIndexRequest("test1")).actionGet();

        logger.info("--> creating index [test2]");
        client1.admin().indices().create(createIndexRequest("test2")).actionGet();

        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client1.admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus()).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> adding filtering aliases to index [test1]");
        client1.admin().indices().prepareAliases().addAlias("test1", "aliasToTest1").execute().actionGet();
        client1.admin().indices().prepareAliases().addAlias("test1", "aliasToTests").execute().actionGet();
        client1.admin().indices().prepareAliases().addAlias("test1", "foos", termFilter("name", "foo")).execute().actionGet();
        client1.admin().indices().prepareAliases().addAlias("test1", "bars", termFilter("name", "bar")).execute().actionGet();
        client1.admin().indices().prepareAliases().addAlias("test1", "tests", termFilter("name", "test")).execute().actionGet();

        logger.info("--> adding filtering aliases to index [test2]");
        client1.admin().indices().prepareAliases().addAlias("test2", "aliasToTest2").execute().actionGet();
        client1.admin().indices().prepareAliases().addAlias("test2", "aliasToTests").execute().actionGet();
        client1.admin().indices().prepareAliases().addAlias("test2", "foos", termFilter("name", "foo")).execute().actionGet();
        client1.admin().indices().prepareAliases().addAlias("test2", "tests", termFilter("name", "test")).execute().actionGet();

        logger.info("--> indexing against [test1]");
        client1.index(indexRequest("test1").setType("type1").setId("1").setSource(source("1", "foo test")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test1").setType("type1").setId("2").setSource(source("2", "bar test")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test1").setType("type1").setId("3").setSource(source("3", "baz test")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test1").setType("type1").setId("4").setSource(source("4", "something else")).setRefresh(true)).actionGet();

        logger.info("--> indexing against [test2]");
        client1.index(indexRequest("test2").setType("type1").setId("5").setSource(source("5", "foo test")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test2").setType("type1").setId("6").setSource(source("6", "bar test")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test2").setType("type1").setId("7").setSource(source("7", "baz test")).setRefresh(true)).actionGet();
        client1.index(indexRequest("test2").setType("type1").setId("8").setSource(source("8", "something else")).setRefresh(true)).actionGet();

        logger.info("--> checking counts before delete");
        assertThat(client1.prepareCount("bars").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(1L));

        logger.info("--> delete by query from a single alias");
        client1.prepareDeleteByQuery("bars").setQuery(QueryBuilders.termQuery("name", "test")).execute().actionGet();
        client1.admin().indices().prepareRefresh().execute().actionGet();

        logger.info("--> verify that only one record was deleted");
        assertThat(client1.prepareCount("test1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(3L));

        logger.info("--> delete by query from an aliases pointing to two indices");
        client1.prepareDeleteByQuery("foos").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        client1.admin().indices().prepareRefresh().execute().actionGet();

        logger.info("--> verify that proper records were deleted");
        SearchResponse searchResponse = client1.prepareSearch("aliasToTests").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "3", "4", "6", "7", "8");

        logger.info("--> delete by query from an aliases and an index");
        client1.prepareDeleteByQuery("tests", "test2").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        client1.admin().indices().prepareRefresh().execute().actionGet();

        logger.info("--> verify that proper records were deleted");
        searchResponse = client1.prepareSearch("aliasToTests").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "4");
    }

    @Test
    public void testWaitForAliasCreationMultipleShards() throws Exception {
        // delete all indices
        client1.admin().indices().prepareDelete().execute().actionGet();

        logger.info("--> creating index [test]");
        client1.admin().indices().create(createIndexRequest("test")).actionGet();

        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client1.admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus()).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        for (int i = 0; i < 10; i++) {
            assertThat(client1.admin().indices().prepareAliases().addAlias("test", "alias" + i).execute().actionGet().isAcknowledged(), equalTo(true));
            client2.index(indexRequest("alias" + i).setType("type1").setId("1").setSource(source("1", "test")).setRefresh(true)).actionGet();
        }

    }

    @Test
    public void testWaitForAliasCreationSingleShard() throws Exception {
        // delete all indices
        client1.admin().indices().prepareDelete().execute().actionGet();

        logger.info("--> creating index [test]");
        client1.admin().indices().create(createIndexRequest("test").setSettings(settingsBuilder().put("index.numberOfReplicas", 0).put("index.numberOfShards", 1))).actionGet();

        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client1.admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus()).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        for (int i = 0; i < 10; i++) {
            assertThat(getClient().admin().indices().prepareAliases().addAlias("test", "alias" + i).execute().actionGet().isAcknowledged(), equalTo(true));
            getClient().index(indexRequest("alias" + i).setType("type1").setId("1").setSource(source("1", "test")).setRefresh(true)).actionGet();
        }
    }

    @Test
    public void testWaitForAliasSimultaneousUpdate() throws Exception {
        final int aliasCount = 10;
        // delete all indices
        client1.admin().indices().prepareDelete().execute().actionGet();

        logger.info("--> creating index [test]");
        client1.admin().indices().create(createIndexRequest("test")).actionGet();

        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client1.admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus()).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        ExecutorService executor = Executors.newFixedThreadPool(aliasCount);
        for (int i = 0; i < aliasCount; i++) {
            final String aliasName = "alias" + i;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    assertThat(client1.admin().indices().prepareAliases().addAlias("test", aliasName).execute().actionGet().isAcknowledged(), equalTo(true));
                    client2.index(indexRequest(aliasName).setType("type1").setId("1").setSource(source("1", "test")).setRefresh(true)).actionGet();
                }
            });
        }
        executor.shutdown();
        boolean done = executor.awaitTermination(10, TimeUnit.SECONDS);
        assertThat(done, equalTo(true));
        if (!done) {
            executor.shutdownNow();
        }
    }


    @Test
    public void testSameAlias() throws Exception {
        client1.admin().indices().prepareDelete().execute().actionGet();

        logger.info("--> creating index [test]");
        client1.admin().indices().create(createIndexRequest("test")).actionGet();

        logger.info("--> running cluster_health");
        ClusterHealthResponse clusterHealth = client1.admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus()).actionGet();
        logger.info("--> done cluster_health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> creating alias1 ");
        assertThat(client2.admin().indices().prepareAliases().addAlias("test", "alias1").execute().actionGet().isAcknowledged(), equalTo(true));
        TimeValue timeout = TimeValue.timeValueSeconds(2);
        logger.info("--> recreating alias1 ");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        assertThat(client2.admin().indices().prepareAliases().addAlias("test", "alias1").setTimeout(timeout).execute().actionGet().isAcknowledged(), equalTo(true));
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));

        logger.info("--> modifying alias1 to have a filter");
        stopWatch.start();
        assertThat(client2.admin().indices().prepareAliases().addAlias("test", "alias1", termFilter("name", "foo")).setTimeout(timeout).execute().actionGet().isAcknowledged(), equalTo(true));
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));

        logger.info("--> recreating alias1 with the same filter");
        stopWatch.start();
        assertThat(client2.admin().indices().prepareAliases().addAlias("test", "alias1", termFilter("name", "foo")).setTimeout(timeout).execute().actionGet().isAcknowledged(), equalTo(true));
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));

        logger.info("--> recreating alias1 with a different filter");
        stopWatch.start();
        assertThat(client2.admin().indices().prepareAliases().addAlias("test", "alias1", termFilter("name", "bar")).setTimeout(timeout).execute().actionGet().isAcknowledged(), equalTo(true));
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));

        logger.info("--> verify that filter was updated");
        AliasMetaData aliasMetaData = ((InternalNode) node("server1")).injector().getInstance(ClusterService.class).state().metaData().aliases().get("alias1").get("test");
        assertThat(aliasMetaData.getFilter().toString(), equalTo("{\"term\":{\"name\":\"bar\"}}"));

        logger.info("--> deleting alias1");
        stopWatch.start();
        assertThat(client2.admin().indices().prepareAliases().removeAlias("test", "alias1").setTimeout(timeout).execute().actionGet().isAcknowledged(), equalTo(true));
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));

        logger.info("--> deleting alias1 one more time");
        stopWatch.start();
        assertThat(client2.admin().indices().prepareAliases().removeAlias("test", "alias1").setTimeout(timeout).execute().actionGet().isAcknowledged(), equalTo(true));
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));
    }

    private void assertHits(SearchHits hits, String... ids) {
        assertThat(hits.totalHits(), equalTo((long) ids.length));
        Set<String> hitIds = newHashSet();
        for (SearchHit hit : hits.getHits()) {
            hitIds.add(hit.id());
        }
        assertThat(hitIds, containsInAnyOrder(ids));
    }

    private String source(String id, String nameValue) {
        return "{ type1 : { \"id\" : \"" + id + "\", \"name\" : \"" + nameValue + "\" } }";
    }
}
