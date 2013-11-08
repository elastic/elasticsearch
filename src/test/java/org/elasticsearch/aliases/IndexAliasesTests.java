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

package org.elasticsearch.aliases;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.alias.get.IndicesGetAliasesResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Sets.newHashSet;
import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class IndexAliasesTests extends ElasticsearchIntegrationTest {

    @Test
    public void testAliases() throws Exception {

        logger.info("--> creating index [test]");
        client().admin().indices().create(createIndexRequest("test")).actionGet();

        ensureGreen();
//
//        try {
//            logger.info("--> indexing against [alias1], should fail");
//            client().index(indexRequest("alias1").type("type1").id("1").source(source("1", "test"))).actionGet();
//            assert false : "index [alias1] should not exists";
//        } catch (IndexMissingException e) {
//            assertThat(e.index().name(), equalTo("alias1"));
//        } 
        // TODO this is bogus and should have a dedicated test

        logger.info("--> aliasing index [test] with [alias1]");
        client().admin().indices().prepareAliases().addAlias("test", "alias1").execute().actionGet();

        logger.info("--> indexing against [alias1], should work now");
        IndexResponse indexResponse = client().index(indexRequest("alias1").type("type1").id("1").source(source("1", "test"))).actionGet();
        assertThat(indexResponse.getIndex(), equalTo("test"));

        logger.info("--> creating index [test]");
        client().admin().indices().create(createIndexRequest("test_x")).actionGet();

        ensureGreen();

        logger.info("--> remove [alias1], Aliasing index [test_x] with [alias1]");
        client().admin().indices().aliases(indexAliasesRequest().removeAlias("test", "alias1").addAlias("test_x", "alias1")).actionGet();
        Thread.sleep(300);

        logger.info("--> indexing against [alias1], should work against [test_x]");
        indexResponse = client().index(indexRequest("alias1").type("type1").id("1").source(source("1", "test"))).actionGet();
        assertThat(indexResponse.getIndex(), equalTo("test_x"));
    }

    @Test
    public void testFailedFilter() throws Exception {

        logger.info("--> creating index [test]");
        client().admin().indices().create(createIndexRequest("test")).actionGet();

        ensureGreen();

        try {
            logger.info("--> aliasing index [test] with [alias1] and filter [t]");
            client().admin().indices().prepareAliases().addAlias("test", "alias1", "{ t }").execute().actionGet();
            assert false;
        } catch (Exception e) {
            // all is well
        }
    }

    @Test
    public void testFilteringAliases() throws Exception {

        logger.info("--> creating index [test]");
        client().admin().indices().create(createIndexRequest("test")).actionGet();

        ensureGreen();

        logger.info("--> aliasing index [test] with [alias1] and filter [user:kimchy]");
        FilterBuilder filter = termFilter("user", "kimchy");
        client().admin().indices().prepareAliases().addAlias("test", "alias1", filter).execute().actionGet();

        // For now just making sure that filter was stored with the alias
        logger.info("--> making sure that filter was stored with alias [alias1] and filter [user:kimchy]");
        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        IndexMetaData indexMd = clusterState.metaData().index("test");
        assertThat(indexMd.aliases().get("alias1").filter().string(), equalTo("{\"term\":{\"user\":\"kimchy\"}}"));

    }

    @Test
    public void testEmptyFilter() throws Exception {
        logger.info("--> creating index [test]");
        createIndex("test");
        ensureGreen();

        logger.info("--> aliasing index [test] with [alias1] and empty filter");
        IndicesAliasesResponse indicesAliasesResponse = client().admin().indices().prepareAliases().addAlias("test", "alias1", "{}").get();
        //just checking that the empty doesn't lead to issues
        assertThat(indicesAliasesResponse.isAcknowledged(), equalTo(true));
    }

    @Test
    public void testSearchingFilteringAliasesSingleIndex() throws Exception {

        logger.info("--> creating index [test]");
        client().admin().indices().create(createIndexRequest("test")).actionGet();

        ensureGreen();

        logger.info("--> adding filtering aliases to index [test]");
        client().admin().indices().prepareAliases().addAlias("test", "alias1").execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test", "alias2").execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test", "foos", termFilter("name", "foo")).execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test", "bars", termFilter("name", "bar")).execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test", "tests", termFilter("name", "test")).execute().actionGet();

        logger.info("--> indexing against [test]");
        client().index(indexRequest("test").type("type1").id("1").source(source("1", "foo test")).refresh(true)).actionGet();
        client().index(indexRequest("test").type("type1").id("2").source(source("2", "bar test")).refresh(true)).actionGet();
        client().index(indexRequest("test").type("type1").id("3").source(source("3", "baz test")).refresh(true)).actionGet();
        client().index(indexRequest("test").type("type1").id("4").source(source("4", "something else")).refresh(true)).actionGet();

        logger.info("--> checking single filtering alias search");
        SearchResponse searchResponse = client().prepareSearch("foos").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1");

        logger.info("--> checking single filtering alias wildcard search");
        searchResponse = client().prepareSearch("fo*").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1");

        searchResponse = client().prepareSearch("tests").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2", "3");

        logger.info("--> checking single filtering alias search with sort");
        searchResponse = client().prepareSearch("tests").setQuery(QueryBuilders.matchAllQuery()).addSort("_uid", SortOrder.ASC).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2", "3");

        logger.info("--> checking single filtering alias search with global facets");
        searchResponse = client().prepareSearch("tests").setQuery(QueryBuilders.matchQuery("name", "bar"))
                .addFacet(FacetBuilders.termsFacet("test").field("name").global(true))
                .execute().actionGet();
        assertThat(((TermsFacet) searchResponse.getFacets().facet("test")).getEntries().size(), equalTo(4));

        logger.info("--> checking single filtering alias search with global facets and sort");
        searchResponse = client().prepareSearch("tests").setQuery(QueryBuilders.matchQuery("name", "bar"))
                .addFacet(FacetBuilders.termsFacet("test").field("name").global(true))
                .addSort("_uid", SortOrder.ASC).execute().actionGet();
        assertThat(((TermsFacet) searchResponse.getFacets().facet("test")).getEntries().size(), equalTo(4));

        logger.info("--> checking single filtering alias search with non-global facets");
        searchResponse = client().prepareSearch("tests").setQuery(QueryBuilders.matchQuery("name", "bar"))
                .addFacet(FacetBuilders.termsFacet("test").field("name").global(false))
                .addSort("_uid", SortOrder.ASC).execute().actionGet();
        assertThat(((TermsFacet) searchResponse.getFacets().facet("test")).getEntries().size(), equalTo(2));

        searchResponse = client().prepareSearch("foos", "bars").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2");

        logger.info("--> checking single non-filtering alias search");
        searchResponse = client().prepareSearch("alias1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2", "3", "4");

        logger.info("--> checking non-filtering alias and filtering alias search");
        searchResponse = client().prepareSearch("alias1", "foos").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2", "3", "4");

        logger.info("--> checking index and filtering alias search");
        searchResponse = client().prepareSearch("test", "foos").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2", "3", "4");

        logger.info("--> checking index and alias wildcard search");
        searchResponse = client().prepareSearch("te*", "fo*").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2", "3", "4");
    }

    @Test
    public void testSearchingFilteringAliasesTwoIndices() throws Exception {

        logger.info("--> creating index [test1]");
        client().admin().indices().create(createIndexRequest("test1")).actionGet();

        logger.info("--> creating index [test2]");
        client().admin().indices().create(createIndexRequest("test2")).actionGet();

        ensureGreen();

        logger.info("--> adding filtering aliases to index [test1]");
        client().admin().indices().prepareAliases().addAlias("test1", "aliasToTest1").execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test1", "aliasToTests").execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test1", "foos", termFilter("name", "foo")).execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test1", "bars", termFilter("name", "bar")).execute().actionGet();

        logger.info("--> adding filtering aliases to index [test2]");
        client().admin().indices().prepareAliases().addAlias("test2", "aliasToTest2").execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test2", "aliasToTests").execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test2", "foos", termFilter("name", "foo")).execute().actionGet();

        logger.info("--> indexing against [test1]");
        client().index(indexRequest("test1").type("type1").id("1").source(source("1", "foo test")).refresh(true)).actionGet();
        client().index(indexRequest("test1").type("type1").id("2").source(source("2", "bar test")).refresh(true)).actionGet();
        client().index(indexRequest("test1").type("type1").id("3").source(source("3", "baz test")).refresh(true)).actionGet();
        client().index(indexRequest("test1").type("type1").id("4").source(source("4", "something else")).refresh(true)).actionGet();

        logger.info("--> indexing against [test2]");
        client().index(indexRequest("test2").type("type1").id("5").source(source("5", "foo test")).refresh(true)).actionGet();
        client().index(indexRequest("test2").type("type1").id("6").source(source("6", "bar test")).refresh(true)).actionGet();
        client().index(indexRequest("test2").type("type1").id("7").source(source("7", "baz test")).refresh(true)).actionGet();
        client().index(indexRequest("test2").type("type1").id("8").source(source("8", "something else")).refresh(true)).actionGet();

        logger.info("--> checking filtering alias for two indices");
        SearchResponse searchResponse = client().prepareSearch("foos").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "5");
        assertThat(client().prepareCount("foos").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(2L));

        logger.info("--> checking filtering alias for one index");
        searchResponse = client().prepareSearch("bars").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "2");
        assertThat(client().prepareCount("bars").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(1L));

        logger.info("--> checking filtering alias for two indices and one complete index");
        searchResponse = client().prepareSearch("foos", "test1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2", "3", "4", "5");
        assertThat(client().prepareCount("foos", "test1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(5L));

        logger.info("--> checking filtering alias for two indices and non-filtering alias for one index");
        searchResponse = client().prepareSearch("foos", "aliasToTest1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "1", "2", "3", "4", "5");
        assertThat(client().prepareCount("foos", "aliasToTest1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(5L));

        logger.info("--> checking filtering alias for two indices and non-filtering alias for both indices");
        searchResponse = client().prepareSearch("foos", "aliasToTests").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(8L));
        assertThat(client().prepareCount("foos", "aliasToTests").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(8L));

        logger.info("--> checking filtering alias for two indices and non-filtering alias for both indices");
        searchResponse = client().prepareSearch("foos", "aliasToTests").setQuery(QueryBuilders.termQuery("name", "something")).execute().actionGet();
        assertHits(searchResponse.getHits(), "4", "8");
        assertThat(client().prepareCount("foos", "aliasToTests").setQuery(QueryBuilders.termQuery("name", "something")).execute().actionGet().getCount(), equalTo(2L));
    }

    @Test
    public void testSearchingFilteringAliasesMultipleIndices() throws Exception {

        logger.info("--> creating indices");
        client().admin().indices().create(createIndexRequest("test1")).actionGet();
        client().admin().indices().create(createIndexRequest("test2")).actionGet();
        client().admin().indices().create(createIndexRequest("test3")).actionGet();

        ensureGreen();

        logger.info("--> adding aliases to indices");
        client().admin().indices().prepareAliases().addAlias("test1", "alias12").execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test2", "alias12").execute().actionGet();

        logger.info("--> adding filtering aliases to indices");
        client().admin().indices().prepareAliases().addAlias("test1", "filter1", termFilter("name", "test1")).execute().actionGet();

        client().admin().indices().prepareAliases().addAlias("test2", "filter23", termFilter("name", "foo")).execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test3", "filter23", termFilter("name", "foo")).execute().actionGet();

        client().admin().indices().prepareAliases().addAlias("test1", "filter13", termFilter("name", "baz")).execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test3", "filter13", termFilter("name", "baz")).execute().actionGet();

        logger.info("--> indexing against [test1]");
        client().index(indexRequest("test1").type("type1").id("11").source(source("11", "foo test1")).refresh(true)).actionGet();
        client().index(indexRequest("test1").type("type1").id("12").source(source("12", "bar test1")).refresh(true)).actionGet();
        client().index(indexRequest("test1").type("type1").id("13").source(source("13", "baz test1")).refresh(true)).actionGet();

        client().index(indexRequest("test2").type("type1").id("21").source(source("21", "foo test2")).refresh(true)).actionGet();
        client().index(indexRequest("test2").type("type1").id("22").source(source("22", "bar test2")).refresh(true)).actionGet();
        client().index(indexRequest("test2").type("type1").id("23").source(source("23", "baz test2")).refresh(true)).actionGet();

        client().index(indexRequest("test3").type("type1").id("31").source(source("31", "foo test3")).refresh(true)).actionGet();
        client().index(indexRequest("test3").type("type1").id("32").source(source("32", "bar test3")).refresh(true)).actionGet();
        client().index(indexRequest("test3").type("type1").id("33").source(source("33", "baz test3")).refresh(true)).actionGet();

        logger.info("--> checking filtering alias for multiple indices");
        SearchResponse searchResponse = client().prepareSearch("filter23", "filter13").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "21", "31", "13", "33");
        assertThat(client().prepareCount("filter23", "filter13").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(4L));

        searchResponse = client().prepareSearch("filter23", "filter1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "21", "31", "11", "12", "13");
        assertThat(client().prepareCount("filter23", "filter1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(5L));

        searchResponse = client().prepareSearch("filter13", "filter1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "11", "12", "13", "33");
        assertThat(client().prepareCount("filter13", "filter1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(4L));

        searchResponse = client().prepareSearch("filter13", "filter1", "filter23").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "11", "12", "13", "21", "31", "33");
        assertThat(client().prepareCount("filter13", "filter1", "filter23").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(6L));

        searchResponse = client().prepareSearch("filter23", "filter13", "test2").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "21", "22", "23", "31", "13", "33");
        assertThat(client().prepareCount("filter23", "filter13", "test2").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(6L));

        searchResponse = client().prepareSearch("filter23", "filter13", "test1", "test2").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "11", "12", "13", "21", "22", "23", "31", "33");
        assertThat(client().prepareCount("filter23", "filter13", "test1", "test2").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(8L));

    }

    @Test
    public void testDeletingByQueryFilteringAliases() throws Exception {

        logger.info("--> creating index [test1]");
        client().admin().indices().create(createIndexRequest("test1")).actionGet();

        logger.info("--> creating index [test2]");
        client().admin().indices().create(createIndexRequest("test2")).actionGet();

        ensureGreen();

        logger.info("--> adding filtering aliases to index [test1]");
        client().admin().indices().prepareAliases().addAlias("test1", "aliasToTest1").execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test1", "aliasToTests").execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test1", "foos", termFilter("name", "foo")).execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test1", "bars", termFilter("name", "bar")).execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test1", "tests", termFilter("name", "test")).execute().actionGet();

        logger.info("--> adding filtering aliases to index [test2]");
        client().admin().indices().prepareAliases().addAlias("test2", "aliasToTest2").execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test2", "aliasToTests").execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test2", "foos", termFilter("name", "foo")).execute().actionGet();
        client().admin().indices().prepareAliases().addAlias("test2", "tests", termFilter("name", "test")).execute().actionGet();

        logger.info("--> indexing against [test1]");
        client().index(indexRequest("test1").type("type1").id("1").source(source("1", "foo test")).refresh(true)).actionGet();
        client().index(indexRequest("test1").type("type1").id("2").source(source("2", "bar test")).refresh(true)).actionGet();
        client().index(indexRequest("test1").type("type1").id("3").source(source("3", "baz test")).refresh(true)).actionGet();
        client().index(indexRequest("test1").type("type1").id("4").source(source("4", "something else")).refresh(true)).actionGet();

        logger.info("--> indexing against [test2]");
        client().index(indexRequest("test2").type("type1").id("5").source(source("5", "foo test")).refresh(true)).actionGet();
        client().index(indexRequest("test2").type("type1").id("6").source(source("6", "bar test")).refresh(true)).actionGet();
        client().index(indexRequest("test2").type("type1").id("7").source(source("7", "baz test")).refresh(true)).actionGet();
        client().index(indexRequest("test2").type("type1").id("8").source(source("8", "something else")).refresh(true)).actionGet();

        logger.info("--> checking counts before delete");
        assertThat(client().prepareCount("bars").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(1L));

        logger.info("--> delete by query from a single alias");
        client().prepareDeleteByQuery("bars").setQuery(QueryBuilders.termQuery("name", "test")).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        logger.info("--> verify that only one record was deleted");
        assertThat(client().prepareCount("test1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getCount(), equalTo(3L));

        logger.info("--> delete by query from an aliases pointing to two indices");
        client().prepareDeleteByQuery("foos").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        logger.info("--> verify that proper records were deleted");
        SearchResponse searchResponse = client().prepareSearch("aliasToTests").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "3", "4", "6", "7", "8");

        logger.info("--> delete by query from an aliases and an index");
        client().prepareDeleteByQuery("tests", "test2").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        logger.info("--> verify that proper records were deleted");
        searchResponse = client().prepareSearch("aliasToTests").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertHits(searchResponse.getHits(), "4");
    }

    @Test
    public void testWaitForAliasCreationMultipleShards() throws Exception {

        logger.info("--> creating index [test]");
        client().admin().indices().create(createIndexRequest("test")).actionGet();

        ensureGreen();

        for (int i = 0; i < 10; i++) {
            assertThat(client().admin().indices().prepareAliases().addAlias("test", "alias" + i).execute().actionGet().isAcknowledged(), equalTo(true));
            client().index(indexRequest("alias" + i).type("type1").id("1").source(source("1", "test")).refresh(true)).actionGet();
        }

    }

    @Test
    public void testWaitForAliasCreationSingleShard() throws Exception {

        logger.info("--> creating index [test]");
        client().admin().indices().create(createIndexRequest("test").settings(settingsBuilder().put("index.numberOfReplicas", 0).put("index.numberOfShards", 1))).actionGet();

        ensureGreen();

        for (int i = 0; i < 10; i++) {
            assertThat(client().admin().indices().prepareAliases().addAlias("test", "alias" + i).execute().actionGet().isAcknowledged(), equalTo(true));
            client().index(indexRequest("alias" + i).type("type1").id("1").source(source("1", "test")).refresh(true)).actionGet();
        }
    }

    @Test
    public void testWaitForAliasSimultaneousUpdate() throws Exception {
        final int aliasCount = 10;

        logger.info("--> creating index [test]");
        client().admin().indices().create(createIndexRequest("test")).actionGet();

        ensureGreen();

        ExecutorService executor = Executors.newFixedThreadPool(aliasCount);
        for (int i = 0; i < aliasCount; i++) {
            final String aliasName = "alias" + i;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    assertThat(client().admin().indices().prepareAliases().addAlias("test", aliasName).execute().actionGet().isAcknowledged(), equalTo(true));
                    client().index(indexRequest(aliasName).type("type1").id("1").source(source("1", "test")).refresh(true)).actionGet();
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

        logger.info("--> creating index [test]");
        client().admin().indices().create(createIndexRequest("test")).actionGet();

        ensureGreen();

        logger.info("--> creating alias1 ");
        assertThat(client().admin().indices().prepareAliases().addAlias("test", "alias1").execute().actionGet().isAcknowledged(), equalTo(true));
        TimeValue timeout = TimeValue.timeValueSeconds(2);
        logger.info("--> recreating alias1 ");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        assertThat(client().admin().indices().prepareAliases().addAlias("test", "alias1").setTimeout(timeout).execute().actionGet().isAcknowledged(), equalTo(true));
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));

        logger.info("--> modifying alias1 to have a filter");
        stopWatch.start();
        assertThat(client().admin().indices().prepareAliases().addAlias("test", "alias1", termFilter("name", "foo")).setTimeout(timeout).execute().actionGet().isAcknowledged(), equalTo(true));
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));

        logger.info("--> recreating alias1 with the same filter");
        stopWatch.start();
        assertThat(client().admin().indices().prepareAliases().addAlias("test", "alias1", termFilter("name", "foo")).setTimeout(timeout).execute().actionGet().isAcknowledged(), equalTo(true));
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));

        logger.info("--> recreating alias1 with a different filter");
        stopWatch.start();
        assertThat(client().admin().indices().prepareAliases().addAlias("test", "alias1", termFilter("name", "bar")).setTimeout(timeout).execute().actionGet().isAcknowledged(), equalTo(true));
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));

        logger.info("--> verify that filter was updated");
        AliasMetaData aliasMetaData = cluster().clusterService().state().metaData().aliases().get("alias1").get("test");
        assertThat(aliasMetaData.getFilter().toString(), equalTo("{\"term\":{\"name\":\"bar\"}}"));

        logger.info("--> deleting alias1");
        stopWatch.start();
        assertThat(client().admin().indices().prepareAliases().removeAlias("test", "alias1").setTimeout(timeout).execute().actionGet().isAcknowledged(), equalTo(true));
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));

        logger.info("--> deleting alias1 one more time");
        stopWatch.start();
        assertThat(client().admin().indices().prepareAliases().removeAlias("test", "alias1").setTimeout(timeout).execute().actionGet().isAcknowledged(), equalTo(true));
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));
    }

    @Test
    public void testIndicesGetAliases() throws Exception {
        Settings indexSettings = ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .build();
        logger.info("--> creating indices [foobar, test, test123, foobarbaz, bazbar]");
        client().admin().indices().prepareCreate("foobar")
                .setSettings(indexSettings)
                .execute().actionGet();
        client().admin().indices().prepareCreate("test")
                .setSettings(indexSettings)
                .execute().actionGet();
        client().admin().indices().prepareCreate("test123")
                .setSettings(indexSettings)
                .execute().actionGet();
        client().admin().indices().prepareCreate("foobarbaz")
                .setSettings(indexSettings)
                .execute().actionGet();
        client().admin().indices().prepareCreate("bazbar")
                .setSettings(indexSettings)
                .execute().actionGet();

        ensureGreen();

        logger.info("--> creating aliases [alias1, alias2]");
        client().admin().indices().prepareAliases()
                .addAlias("foobar", "alias1")
                .execute().actionGet();

        IndicesAliasesResponse indicesAliasesResponse = client().admin().indices().prepareAliases()
                .addAlias("foobar", "alias2")
                .execute().actionGet();
        assertThat(indicesAliasesResponse.isAcknowledged(), equalTo(true));

        logger.info("--> getting alias1");
        IndicesGetAliasesResponse getResponse = client().admin().indices().prepareGetAliases("alias1").execute().actionGet();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(1));
        assertThat(getResponse.getAliases().get("foobar").size(), equalTo(1));
        assertThat(getResponse.getAliases().get("foobar").get(0), notNullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).alias(), equalTo("alias1"));
        assertThat(getResponse.getAliases().get("foobar").get(0).getFilter(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getSearchRouting(), nullValue());
        AliasesExistResponse existsResponse = client().admin().indices().prepareAliasesExist("alias1").execute().actionGet();
        assertThat(existsResponse.exists(), equalTo(true));

        logger.info("--> getting all aliases that start with alias*");
        getResponse = client().admin().indices().prepareGetAliases("alias*").execute().actionGet();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(1));
        assertThat(getResponse.getAliases().get("foobar").size(), equalTo(2));
        assertThat(getResponse.getAliases().get("foobar").get(0), notNullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).alias(), equalTo("alias2"));
        assertThat(getResponse.getAliases().get("foobar").get(0).getFilter(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getSearchRouting(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(1), notNullValue());
        assertThat(getResponse.getAliases().get("foobar").get(1).alias(), equalTo("alias1"));
        assertThat(getResponse.getAliases().get("foobar").get(1).getFilter(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(1).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(1).getSearchRouting(), nullValue());
        existsResponse = client().admin().indices().prepareAliasesExist("alias*").execute().actionGet();
        assertThat(existsResponse.exists(), equalTo(true));


        logger.info("--> creating aliases [bar, baz, foo]");
        client().admin().indices().prepareAliases()
                .addAlias("bazbar", "bar")
                .addAlias("bazbar", "bac", termFilter("field", "value"))
                .addAlias("foobar", "foo")
                .execute().actionGet();

        indicesAliasesResponse = client().admin().indices().prepareAliases()
                .addAliasAction(new AliasAction(AliasAction.Type.ADD, "foobar", "bac").routing("bla"))
                .execute().actionGet();
        assertThat(indicesAliasesResponse.isAcknowledged(), equalTo(true));

        logger.info("--> getting bar and baz for index bazbar");
        getResponse = client().admin().indices().prepareGetAliases("bar", "bac").addIndices("bazbar").execute().actionGet();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(1));
        assertThat(getResponse.getAliases().get("bazbar").size(), equalTo(2));
        assertThat(getResponse.getAliases().get("bazbar").get(0), notNullValue());
        assertThat(getResponse.getAliases().get("bazbar").get(0).alias(), equalTo("bac"));
        assertThat(getResponse.getAliases().get("bazbar").get(0).getFilter().string(), containsString("term"));
        assertThat(getResponse.getAliases().get("bazbar").get(0).getFilter().string(), containsString("field"));
        assertThat(getResponse.getAliases().get("bazbar").get(0).getFilter().string(), containsString("value"));
        assertThat(getResponse.getAliases().get("bazbar").get(0).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("bazbar").get(0).getSearchRouting(), nullValue());
        assertThat(getResponse.getAliases().get("bazbar").get(1), notNullValue());
        assertThat(getResponse.getAliases().get("bazbar").get(1).alias(), equalTo("bar"));
        assertThat(getResponse.getAliases().get("bazbar").get(1).getFilter(), nullValue());
        assertThat(getResponse.getAliases().get("bazbar").get(1).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("bazbar").get(1).getSearchRouting(), nullValue());
        existsResponse = client().admin().indices().prepareAliasesExist("bar", "bac")
                .addIndices("bazbar").execute().actionGet();
        assertThat(existsResponse.exists(), equalTo(true));

        logger.info("--> getting *b* for index baz*");
        getResponse = client().admin().indices().prepareGetAliases("*b*").addIndices("baz*").execute().actionGet();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(1));
        assertThat(getResponse.getAliases().get("bazbar").size(), equalTo(2));
        assertThat(getResponse.getAliases().get("bazbar").get(0), notNullValue());
        assertThat(getResponse.getAliases().get("bazbar").get(0).alias(), equalTo("bac"));
        assertThat(getResponse.getAliases().get("bazbar").get(0).getFilter().string(), containsString("term"));
        assertThat(getResponse.getAliases().get("bazbar").get(0).getFilter().string(), containsString("field"));
        assertThat(getResponse.getAliases().get("bazbar").get(0).getFilter().string(), containsString("value"));
        assertThat(getResponse.getAliases().get("bazbar").get(0).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("bazbar").get(0).getSearchRouting(), nullValue());
        assertThat(getResponse.getAliases().get("bazbar").get(1), notNullValue());
        assertThat(getResponse.getAliases().get("bazbar").get(1).alias(), equalTo("bar"));
        assertThat(getResponse.getAliases().get("bazbar").get(1).getFilter(), nullValue());
        assertThat(getResponse.getAliases().get("bazbar").get(1).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("bazbar").get(1).getSearchRouting(), nullValue());
        existsResponse = client().admin().indices().prepareAliasesExist("*b*")
                .addIndices("baz*").execute().actionGet();
        assertThat(existsResponse.exists(), equalTo(true));

        logger.info("--> getting *b* for index *bar");
        getResponse = client().admin().indices().prepareGetAliases("b*").addIndices("*bar").execute().actionGet();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(2));
        assertThat(getResponse.getAliases().get("bazbar").size(), equalTo(2));
        assertThat(getResponse.getAliases().get("bazbar").get(0), notNullValue());
        assertThat(getResponse.getAliases().get("bazbar").get(0).alias(), equalTo("bac"));
        assertThat(getResponse.getAliases().get("bazbar").get(0).getFilter().string(), containsString("term"));
        assertThat(getResponse.getAliases().get("bazbar").get(0).getFilter().string(), containsString("field"));
        assertThat(getResponse.getAliases().get("bazbar").get(0).getFilter().string(), containsString("value"));
        assertThat(getResponse.getAliases().get("bazbar").get(0).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("bazbar").get(0).getSearchRouting(), nullValue());
        assertThat(getResponse.getAliases().get("bazbar").get(1), notNullValue());
        assertThat(getResponse.getAliases().get("bazbar").get(1).alias(), equalTo("bar"));
        assertThat(getResponse.getAliases().get("bazbar").get(1).getFilter(), nullValue());
        assertThat(getResponse.getAliases().get("bazbar").get(1).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("bazbar").get(1).getSearchRouting(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0), notNullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).alias(), equalTo("bac"));
        assertThat(getResponse.getAliases().get("foobar").get(0).getFilter(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getIndexRouting(), equalTo("bla"));
        assertThat(getResponse.getAliases().get("foobar").get(0).getSearchRouting(), equalTo("bla"));
        existsResponse = client().admin().indices().prepareAliasesExist("b*")
                .addIndices("*bar").execute().actionGet();
        assertThat(existsResponse.exists(), equalTo(true));

        logger.info("--> getting f* for index *bar");
        getResponse = client().admin().indices().prepareGetAliases("f*").addIndices("*bar").execute().actionGet();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(1));
        assertThat(getResponse.getAliases().get("foobar").get(0), notNullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).alias(), equalTo("foo"));
        assertThat(getResponse.getAliases().get("foobar").get(0).getFilter(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getSearchRouting(), nullValue());
        existsResponse = client().admin().indices().prepareAliasesExist("f*")
                .addIndices("*bar").execute().actionGet();
        assertThat(existsResponse.exists(), equalTo(true));

        // alias at work
        logger.info("--> getting f* for index *bac");
        getResponse = client().admin().indices().prepareGetAliases("foo").addIndices("*bac").execute().actionGet();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(1));
        assertThat(getResponse.getAliases().get("foobar").size(), equalTo(1));
        assertThat(getResponse.getAliases().get("foobar").get(0), notNullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).alias(), equalTo("foo"));
        assertThat(getResponse.getAliases().get("foobar").get(0).getFilter(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getSearchRouting(), nullValue());
        existsResponse = client().admin().indices().prepareAliasesExist("foo")
                .addIndices("*bac").execute().actionGet();
        assertThat(existsResponse.exists(), equalTo(true));

        logger.info("--> getting foo for index foobar");
        getResponse = client().admin().indices().prepareGetAliases("foo").addIndices("foobar").execute().actionGet();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(1));
        assertThat(getResponse.getAliases().get("foobar").get(0), notNullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).alias(), equalTo("foo"));
        assertThat(getResponse.getAliases().get("foobar").get(0).getFilter(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getSearchRouting(), nullValue());
        existsResponse = client().admin().indices().prepareAliasesExist("foo")
                .addIndices("foobar").execute().actionGet();
        assertThat(existsResponse.exists(), equalTo(true));

        // alias at work again
        logger.info("--> getting * for index *bac");
        getResponse = client().admin().indices().prepareGetAliases("*").addIndices("*bac").execute().actionGet();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(2));
        assertThat(getResponse.getAliases().get("foobar").size(), equalTo(4));
        assertThat(getResponse.getAliases().get("bazbar").size(), equalTo(2));
        existsResponse = client().admin().indices().prepareAliasesExist("*")
                .addIndices("*bac").execute().actionGet();
        assertThat(existsResponse.exists(), equalTo(true));

        indicesAliasesResponse = client().admin().indices().prepareAliases()
                .removeAlias("foobar", "foo")
                .execute().actionGet();
        assertThat(indicesAliasesResponse.isAcknowledged(), equalTo(true));

        getResponse = client().admin().indices().prepareGetAliases("foo").addIndices("foobar").execute().actionGet();
        assertThat(getResponse.getAliases().isEmpty(), equalTo(true));
        existsResponse = client().admin().indices().prepareAliasesExist("foo")
                .addIndices("foobar").execute().actionGet();
        assertThat(existsResponse.exists(), equalTo(false));
    }

    @Test(expected = ActionRequestValidationException.class)
    public void testAddAliasNullIndex() {

        client().admin().indices().prepareAliases().addAliasAction(AliasAction.newAddAliasAction(null, "alias1"))
                .execute().actionGet();
    }

    @Test(expected = ActionRequestValidationException.class)
    public void testAddAliasEmptyIndex() {

        client().admin().indices().prepareAliases().addAliasAction(AliasAction.newAddAliasAction("", "alias1"))
                .execute().actionGet();
    }

    @Test(expected = ActionRequestValidationException.class)
    public void testAddAliasNullAlias() {

        client().admin().indices().prepareAliases().addAliasAction(AliasAction.newAddAliasAction("index1", null))
                .execute().actionGet();
    }

    @Test(expected = ActionRequestValidationException.class)
    public void testAddAliasEmptyAlias() {

        client().admin().indices().prepareAliases().addAliasAction(AliasAction.newAddAliasAction("index1", ""))
                .execute().actionGet();
    }

    @Test
    public void testAddAliasNullAliasNullIndex() {
        try {
            client().admin().indices().prepareAliases().addAliasAction(AliasAction.newAddAliasAction(null, null))
                    .execute().actionGet();
            assertTrue("Should throw " + ActionRequestValidationException.class.getSimpleName(), false);
        } catch (ActionRequestValidationException e) {
            assertThat(e.validationErrors(), notNullValue());
            assertThat(e.validationErrors().size(), equalTo(2));
        }
    }

    @Test
    public void testAddAliasEmptyAliasEmptyIndex() {
        try {
            client().admin().indices().prepareAliases().addAliasAction(AliasAction.newAddAliasAction("", ""))
                    .execute().actionGet();
            assertTrue("Should throw " + ActionRequestValidationException.class.getSimpleName(), false);
        } catch (ActionRequestValidationException e) {
            assertThat(e.validationErrors(), notNullValue());
            assertThat(e.validationErrors().size(), equalTo(2));
        }
    }

    @Test(expected = ActionRequestValidationException.class)
    public void tesRemoveAliasNullIndex() {
        client().admin().indices().prepareAliases().addAliasAction(AliasAction.newRemoveAliasAction(null, "alias1"))
                .execute().actionGet();
    }

    @Test(expected = ActionRequestValidationException.class)
    public void tesRemoveAliasEmptyIndex() {
        client().admin().indices().prepareAliases().addAliasAction(AliasAction.newRemoveAliasAction("", "alias1"))
                .execute().actionGet();
    }

    @Test(expected = ActionRequestValidationException.class)
    public void tesRemoveAliasNullAlias() {
        client().admin().indices().prepareAliases().addAliasAction(AliasAction.newRemoveAliasAction("index1", null))
                .execute().actionGet();
    }

    @Test(expected = ActionRequestValidationException.class)
    public void tesRemoveAliasEmptyAlias() {
        client().admin().indices().prepareAliases().addAliasAction(AliasAction.newRemoveAliasAction("index1", ""))
                .execute().actionGet();
    }

    @Test
    public void testRemoveAliasNullAliasNullIndex() {
        try {
            client().admin().indices().prepareAliases().addAliasAction(AliasAction.newAddAliasAction(null, null))
                    .execute().actionGet();
            assertTrue("Should throw " + ActionRequestValidationException.class.getSimpleName(), false);
        } catch (ActionRequestValidationException e) {
            assertThat(e.validationErrors(), notNullValue());
            assertThat(e.validationErrors().size(), equalTo(2));
        }
    }

    @Test
    public void testRemoveAliasEmptyAliasEmptyIndex() {
        try {
            client().admin().indices().prepareAliases().addAliasAction(AliasAction.newAddAliasAction("", ""))
                    .execute().actionGet();
            assertTrue("Should throw " + ActionRequestValidationException.class.getSimpleName(), false);
        } catch (ActionRequestValidationException e) {
            assertThat(e.validationErrors(), notNullValue());
            assertThat(e.validationErrors().size(), equalTo(2));
        }
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
