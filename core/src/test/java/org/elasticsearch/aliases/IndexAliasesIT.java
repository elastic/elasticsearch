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

package org.elasticsearch.aliases;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.action.admin.indices.AliasesNotFoundException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_METADATA_BLOCK;
import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_READ_ONLY_BLOCK;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.CollectionAssertions.hasKey;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class IndexAliasesIT extends ESIntegTestCase {
    public void testAliases() throws Exception {
        logger.info("--> creating index [test]");
        createIndex("test");

        ensureGreen();

        logger.info("--> aliasing index [test] with [alias1]");
        assertAcked(admin().indices().prepareAliases().addAlias("test", "alias1"));

        logger.info("--> indexing against [alias1], should work now");
        IndexResponse indexResponse = client().index(indexRequest("alias1").type("type1").id("1")
            .source(source("1", "test"), XContentType.JSON)).actionGet();
        assertThat(indexResponse.getIndex(), equalTo("test"));

        logger.info("--> creating index [test_x]");
        createIndex("test_x");

        ensureGreen();

        logger.info("--> remove [alias1], Aliasing index [test_x] with [alias1]");
        assertAcked(admin().indices().prepareAliases().removeAlias("test", "alias1").addAlias("test_x", "alias1"));

        logger.info("--> indexing against [alias1], should work against [test_x]");
        indexResponse = client().index(indexRequest("alias1").type("type1").id("1")
            .source(source("1", "test"), XContentType.JSON)).actionGet();
        assertThat(indexResponse.getIndex(), equalTo("test_x"));
    }

    public void testFailedFilter() throws Exception {
        logger.info("--> creating index [test]");
        createIndex("test");

        //invalid filter, invalid json
        Exception e = expectThrows(IllegalArgumentException.class,
                () -> admin().indices().prepareAliases().addAlias("test", "alias1", "abcde").get());
        assertThat(e.getMessage(), equalTo("failed to parse filter for alias [alias1]"));

        // valid json , invalid filter
        e = expectThrows(IllegalArgumentException.class,
                () -> admin().indices().prepareAliases().addAlias("test", "alias1", "{ \"test\": {} }").get());
        assertThat(e.getMessage(), equalTo("failed to parse filter for alias [alias1]"));
    }

    public void testFilteringAliases() throws Exception {
        logger.info("--> creating index [test]");
        assertAcked(prepareCreate("test").addMapping("type", "user", "type=text"));

        ensureGreen();

        logger.info("--> aliasing index [test] with [alias1] and filter [user:kimchy]");
        QueryBuilder filter = termQuery("user", "kimchy");
        assertAcked(admin().indices().prepareAliases().addAlias("test", "alias1", filter));

        // For now just making sure that filter was stored with the alias
        logger.info("--> making sure that filter was stored with alias [alias1] and filter [user:kimchy]");
        ClusterState clusterState = admin().cluster().prepareState().get().getState();
        IndexMetaData indexMd = clusterState.metaData().index("test");
        assertThat(indexMd.getAliases().get("alias1").filter().string(), equalTo("{\"term\":{\"user\":{\"value\":\"kimchy\",\"boost\":1.0}}}"));

    }

    public void testEmptyFilter() throws Exception {
        logger.info("--> creating index [test]");
        createIndex("test");
        ensureGreen();

        logger.info("--> aliasing index [test] with [alias1] and empty filter");
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                () -> admin().indices().prepareAliases().addAlias("test", "alias1", "{}").get());
        assertEquals("failed to parse filter for alias [alias1]", iae.getMessage());
    }

    public void testSearchingFilteringAliasesSingleIndex() throws Exception {
        logger.info("--> creating index [test]");
        assertAcked(prepareCreate("test").addMapping("type1", "id", "type=text", "name", "type=text,fielddata=true"));

        ensureGreen();

        logger.info("--> adding filtering aliases to index [test]");
        assertAcked(admin().indices().prepareAliases().addAlias("test", "alias1"));
        assertAcked(admin().indices().prepareAliases().addAlias("test", "alias2"));
        assertAcked(admin().indices().prepareAliases().addAlias("test", "foos", termQuery("name", "foo")));
        assertAcked(admin().indices().prepareAliases().addAlias("test", "bars", termQuery("name", "bar")));
        assertAcked(admin().indices().prepareAliases().addAlias("test", "tests", termQuery("name", "test")));

        logger.info("--> indexing against [test]");
        client().index(indexRequest("test").type("type1").id("1").source(source("1", "foo test"), XContentType.JSON)
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)).actionGet();
        client().index(indexRequest("test").type("type1").id("2").source(source("2", "bar test"), XContentType.JSON)
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)).actionGet();
        client().index(indexRequest("test").type("type1").id("3").source(source("3", "baz test"), XContentType.JSON)
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)).actionGet();
        client().index(indexRequest("test").type("type1").id("4").source(source("4", "something else"), XContentType.JSON)
                    .setRefreshPolicy(RefreshPolicy.IMMEDIATE)).actionGet();

        logger.info("--> checking single filtering alias search");
        SearchResponse searchResponse = client().prepareSearch("foos").setQuery(QueryBuilders.matchAllQuery()).get();
        assertHits(searchResponse.getHits(), "1");

        logger.info("--> checking single filtering alias wildcard search");
        searchResponse = client().prepareSearch("fo*").setQuery(QueryBuilders.matchAllQuery()).get();
        assertHits(searchResponse.getHits(), "1");

        searchResponse = client().prepareSearch("tests").setQuery(QueryBuilders.matchAllQuery()).get();
        assertHits(searchResponse.getHits(), "1", "2", "3");

        logger.info("--> checking single filtering alias search with sort");
        searchResponse = client().prepareSearch("tests").setQuery(QueryBuilders.matchAllQuery()).addSort("_index", SortOrder.ASC).get();
        assertHits(searchResponse.getHits(), "1", "2", "3");

        logger.info("--> checking single filtering alias search with global facets");
        searchResponse = client().prepareSearch("tests").setQuery(QueryBuilders.matchQuery("name", "bar"))
                .addAggregation(AggregationBuilders.global("global").subAggregation(AggregationBuilders.terms("test").field("name")))
                .get();
        assertSearchResponse(searchResponse);
        Global global = searchResponse.getAggregations().get("global");
        Terms terms = global.getAggregations().get("test");
        assertThat(terms.getBuckets().size(), equalTo(4));

        logger.info("--> checking single filtering alias search with global facets and sort");
        searchResponse = client().prepareSearch("tests").setQuery(QueryBuilders.matchQuery("name", "bar"))
                .addAggregation(AggregationBuilders.global("global").subAggregation(AggregationBuilders.terms("test").field("name")))
                .addSort("_index", SortOrder.ASC).get();
        assertSearchResponse(searchResponse);
        global = searchResponse.getAggregations().get("global");
        terms = global.getAggregations().get("test");
        assertThat(terms.getBuckets().size(), equalTo(4));

        logger.info("--> checking single filtering alias search with non-global facets");
        searchResponse = client().prepareSearch("tests").setQuery(QueryBuilders.matchQuery("name", "bar"))
                .addAggregation(AggregationBuilders.terms("test").field("name"))
                .addSort("_index", SortOrder.ASC).get();
        assertSearchResponse(searchResponse);
        terms = searchResponse.getAggregations().get("test");
        assertThat(terms.getBuckets().size(), equalTo(2));

        searchResponse = client().prepareSearch("foos", "bars").setQuery(QueryBuilders.matchAllQuery()).get();
        assertHits(searchResponse.getHits(), "1", "2");

        logger.info("--> checking single non-filtering alias search");
        searchResponse = client().prepareSearch("alias1").setQuery(QueryBuilders.matchAllQuery()).get();
        assertHits(searchResponse.getHits(), "1", "2", "3", "4");

        logger.info("--> checking non-filtering alias and filtering alias search");
        searchResponse = client().prepareSearch("alias1", "foos").setQuery(QueryBuilders.matchAllQuery()).get();
        assertHits(searchResponse.getHits(), "1", "2", "3", "4");

        logger.info("--> checking index and filtering alias search");
        searchResponse = client().prepareSearch("test", "foos").setQuery(QueryBuilders.matchAllQuery()).get();
        assertHits(searchResponse.getHits(), "1", "2", "3", "4");

        logger.info("--> checking index and alias wildcard search");
        searchResponse = client().prepareSearch("te*", "fo*").setQuery(QueryBuilders.matchAllQuery()).get();
        assertHits(searchResponse.getHits(), "1", "2", "3", "4");
    }

    public void testSearchingFilteringAliasesTwoIndices() throws Exception {
        logger.info("--> creating index [test1]");
        assertAcked(prepareCreate("test1").addMapping("type1", "name", "type=text"));
        logger.info("--> creating index [test2]");
        assertAcked(prepareCreate("test2").addMapping("type1", "name", "type=text"));
        ensureGreen();

        logger.info("--> adding filtering aliases to index [test1]");
        assertAcked(admin().indices().prepareAliases().addAlias("test1", "aliasToTest1"));
        assertAcked(admin().indices().prepareAliases().addAlias("test1", "aliasToTests"));
        assertAcked(admin().indices().prepareAliases().addAlias("test1", "foos", termQuery("name", "foo")));
        assertAcked(admin().indices().prepareAliases().addAlias("test1", "bars", termQuery("name", "bar")));

        logger.info("--> adding filtering aliases to index [test2]");
        assertAcked(admin().indices().prepareAliases().addAlias("test2", "aliasToTest2"));
        assertAcked(admin().indices().prepareAliases().addAlias("test2", "aliasToTests"));
        assertAcked(admin().indices().prepareAliases().addAlias("test2", "foos", termQuery("name", "foo")));

        logger.info("--> indexing against [test1]");
        client().index(indexRequest("test1").type("type1").id("1").source(source("1", "foo test"), XContentType.JSON)).get();
        client().index(indexRequest("test1").type("type1").id("2").source(source("2", "bar test"), XContentType.JSON)).get();
        client().index(indexRequest("test1").type("type1").id("3").source(source("3", "baz test"), XContentType.JSON)).get();
        client().index(indexRequest("test1").type("type1").id("4").source(source("4", "something else"), XContentType.JSON)).get();

        logger.info("--> indexing against [test2]");
        client().index(indexRequest("test2").type("type1").id("5").source(source("5", "foo test"), XContentType.JSON)).get();
        client().index(indexRequest("test2").type("type1").id("6").source(source("6", "bar test"), XContentType.JSON)).get();
        client().index(indexRequest("test2").type("type1").id("7").source(source("7", "baz test"), XContentType.JSON)).get();
        client().index(indexRequest("test2").type("type1").id("8").source(source("8", "something else"), XContentType.JSON)).get();

        refresh();

        logger.info("--> checking filtering alias for two indices");
        SearchResponse searchResponse = client().prepareSearch("foos").setQuery(QueryBuilders.matchAllQuery()).get();
        assertHits(searchResponse.getHits(), "1", "5");
        assertThat(client().prepareSearch("foos").setSize(0).setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits(), equalTo(2L));

        logger.info("--> checking filtering alias for one index");
        searchResponse = client().prepareSearch("bars").setQuery(QueryBuilders.matchAllQuery()).get();
        assertHits(searchResponse.getHits(), "2");
        assertThat(client().prepareSearch("bars").setSize(0).setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits(), equalTo(1L));

        logger.info("--> checking filtering alias for two indices and one complete index");
        searchResponse = client().prepareSearch("foos", "test1").setQuery(QueryBuilders.matchAllQuery()).get();
        assertHits(searchResponse.getHits(), "1", "2", "3", "4", "5");
        assertThat(client().prepareSearch("foos", "test1").setSize(0).setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits(), equalTo(5L));

        logger.info("--> checking filtering alias for two indices and non-filtering alias for one index");
        searchResponse = client().prepareSearch("foos", "aliasToTest1").setQuery(QueryBuilders.matchAllQuery()).get();
        assertHits(searchResponse.getHits(), "1", "2", "3", "4", "5");
        assertThat(client().prepareSearch("foos", "aliasToTest1").setSize(0).setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits(), equalTo(5L));

        logger.info("--> checking filtering alias for two indices and non-filtering alias for both indices");
        searchResponse = client().prepareSearch("foos", "aliasToTests").setQuery(QueryBuilders.matchAllQuery()).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(8L));
        assertThat(client().prepareSearch("foos", "aliasToTests").setSize(0).setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits(), equalTo(8L));

        logger.info("--> checking filtering alias for two indices and non-filtering alias for both indices");
        searchResponse = client().prepareSearch("foos", "aliasToTests").setQuery(QueryBuilders.termQuery("name", "something")).get();
        assertHits(searchResponse.getHits(), "4", "8");
        assertThat(client().prepareSearch("foos", "aliasToTests").setSize(0).setQuery(QueryBuilders.termQuery("name", "something")).get().getHits().getTotalHits(), equalTo(2L));
    }

    public void testSearchingFilteringAliasesMultipleIndices() throws Exception {
        logger.info("--> creating indices");
        createIndex("test1", "test2", "test3");

        assertAcked(client().admin().indices().preparePutMapping("test1", "test2", "test3")
                .setType("type1")
                .setSource("name", "type=text"));

        ensureGreen();

        logger.info("--> adding aliases to indices");
        assertAcked(admin().indices().prepareAliases().addAlias("test1", "alias12"));
        assertAcked(admin().indices().prepareAliases().addAlias("test2", "alias12"));

        logger.info("--> adding filtering aliases to indices");
        assertAcked(admin().indices().prepareAliases().addAlias("test1", "filter1", termQuery("name", "test1")));

        assertAcked(admin().indices().prepareAliases().addAlias("test2", "filter23", termQuery("name", "foo")));
        assertAcked(admin().indices().prepareAliases().addAlias("test3", "filter23", termQuery("name", "foo")));

        assertAcked(admin().indices().prepareAliases().addAlias("test1", "filter13", termQuery("name", "baz")));
        assertAcked(admin().indices().prepareAliases().addAlias("test3", "filter13", termQuery("name", "baz")));

        logger.info("--> indexing against [test1]");
        client().index(indexRequest("test1").type("type1").id("11").source(source("11", "foo test1"), XContentType.JSON)).get();
        client().index(indexRequest("test1").type("type1").id("12").source(source("12", "bar test1"), XContentType.JSON)).get();
        client().index(indexRequest("test1").type("type1").id("13").source(source("13", "baz test1"), XContentType.JSON)).get();

        client().index(indexRequest("test2").type("type1").id("21").source(source("21", "foo test2"), XContentType.JSON)).get();
        client().index(indexRequest("test2").type("type1").id("22").source(source("22", "bar test2"), XContentType.JSON)).get();
        client().index(indexRequest("test2").type("type1").id("23").source(source("23", "baz test2"), XContentType.JSON)).get();

        client().index(indexRequest("test3").type("type1").id("31").source(source("31", "foo test3"), XContentType.JSON)).get();
        client().index(indexRequest("test3").type("type1").id("32").source(source("32", "bar test3"), XContentType.JSON)).get();
        client().index(indexRequest("test3").type("type1").id("33").source(source("33", "baz test3"), XContentType.JSON)).get();

        refresh();

        logger.info("--> checking filtering alias for multiple indices");
        SearchResponse searchResponse = client().prepareSearch("filter23", "filter13").setQuery(QueryBuilders.matchAllQuery()).get();
        assertHits(searchResponse.getHits(), "21", "31", "13", "33");
        assertThat(client().prepareSearch("filter23", "filter13").setSize(0).setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits(), equalTo(4L));

        searchResponse = client().prepareSearch("filter23", "filter1").setQuery(QueryBuilders.matchAllQuery()).get();
        assertHits(searchResponse.getHits(), "21", "31", "11", "12", "13");
        assertThat(client().prepareSearch("filter23", "filter1").setSize(0).setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits(), equalTo(5L));

        searchResponse = client().prepareSearch("filter13", "filter1").setQuery(QueryBuilders.matchAllQuery()).get();
        assertHits(searchResponse.getHits(), "11", "12", "13", "33");
        assertThat(client().prepareSearch("filter13", "filter1").setSize(0).setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits(), equalTo(4L));

        searchResponse = client().prepareSearch("filter13", "filter1", "filter23").setQuery(QueryBuilders.matchAllQuery()).get();
        assertHits(searchResponse.getHits(), "11", "12", "13", "21", "31", "33");
        assertThat(client().prepareSearch("filter13", "filter1", "filter23").setSize(0).setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits(), equalTo(6L));

        searchResponse = client().prepareSearch("filter23", "filter13", "test2").setQuery(QueryBuilders.matchAllQuery()).get();
        assertHits(searchResponse.getHits(), "21", "22", "23", "31", "13", "33");
        assertThat(client().prepareSearch("filter23", "filter13", "test2").setSize(0).setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits(), equalTo(6L));

        searchResponse = client().prepareSearch("filter23", "filter13", "test1", "test2").setQuery(QueryBuilders.matchAllQuery()).get();
        assertHits(searchResponse.getHits(), "11", "12", "13", "21", "22", "23", "31", "33");
        assertThat(client().prepareSearch("filter23", "filter13", "test1", "test2").setSize(0).setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits(), equalTo(8L));
    }

    public void testDeletingByQueryFilteringAliases() throws Exception {
        logger.info("--> creating index [test1] and [test2");
        assertAcked(prepareCreate("test1").addMapping("type1", "name", "type=text"));
        assertAcked(prepareCreate("test2").addMapping("type1", "name", "type=text"));
        ensureGreen();

        logger.info("--> adding filtering aliases to index [test1]");
        assertAcked(admin().indices().prepareAliases().addAlias("test1", "aliasToTest1"));
        assertAcked(admin().indices().prepareAliases().addAlias("test1", "aliasToTests"));
        assertAcked(admin().indices().prepareAliases().addAlias("test1", "foos", termQuery("name", "foo")));
        assertAcked(admin().indices().prepareAliases().addAlias("test1", "bars", termQuery("name", "bar")));
        assertAcked(admin().indices().prepareAliases().addAlias("test1", "tests", termQuery("name", "test")));

        logger.info("--> adding filtering aliases to index [test2]");
        assertAcked(admin().indices().prepareAliases().addAlias("test2", "aliasToTest2"));
        assertAcked(admin().indices().prepareAliases().addAlias("test2", "aliasToTests"));
        assertAcked(admin().indices().prepareAliases().addAlias("test2", "foos", termQuery("name", "foo")));
        assertAcked(admin().indices().prepareAliases().addAlias("test2", "tests", termQuery("name", "test")));

        logger.info("--> indexing against [test1]");
        client().index(indexRequest("test1").type("type1").id("1").source(source("1", "foo test"), XContentType.JSON)).get();
        client().index(indexRequest("test1").type("type1").id("2").source(source("2", "bar test"), XContentType.JSON)).get();
        client().index(indexRequest("test1").type("type1").id("3").source(source("3", "baz test"), XContentType.JSON)).get();
        client().index(indexRequest("test1").type("type1").id("4").source(source("4", "something else"), XContentType.JSON)).get();

        logger.info("--> indexing against [test2]");
        client().index(indexRequest("test2").type("type1").id("5").source(source("5", "foo test"), XContentType.JSON)).get();
        client().index(indexRequest("test2").type("type1").id("6").source(source("6", "bar test"), XContentType.JSON)).get();
        client().index(indexRequest("test2").type("type1").id("7").source(source("7", "baz test"), XContentType.JSON)).get();
        client().index(indexRequest("test2").type("type1").id("8").source(source("8", "something else"), XContentType.JSON)).get();

        refresh();

        logger.info("--> checking counts before delete");
        assertThat(client().prepareSearch("bars").setSize(0).setQuery(QueryBuilders.matchAllQuery()).get().getHits().getTotalHits(), equalTo(1L));
    }

    public void testDeleteAliases() throws Exception {
        logger.info("--> creating index [test1] and [test2]");
        assertAcked(prepareCreate("test1").addMapping("type", "name", "type=text"));
        assertAcked(prepareCreate("test2").addMapping("type", "name", "type=text"));
        ensureGreen();

        logger.info("--> adding filtering aliases to index [test1]");
        assertAcked(admin().indices().prepareAliases().addAlias("test1", "aliasToTest1")
                .addAlias("test1", "aliasToTests")
                .addAlias("test1", "foos", termQuery("name", "foo"))
                .addAlias("test1", "bars", termQuery("name", "bar"))
                .addAlias("test1", "tests", termQuery("name", "test")));

        logger.info("--> adding filtering aliases to index [test2]");
        assertAcked(admin().indices().prepareAliases().addAlias("test2", "aliasToTest2")
                .addAlias("test2", "aliasToTests")
                .addAlias("test2", "foos", termQuery("name", "foo"))
                .addAlias("test2", "tests", termQuery("name", "test")));

        String[] indices = {"test1", "test2"};
        String[] aliases = {"aliasToTest1", "foos", "bars", "tests", "aliasToTest2", "aliasToTests"};

        admin().indices().prepareAliases().removeAlias(indices, aliases).get();

        AliasesExistResponse response = admin().indices().prepareAliasesExist(aliases).get();
        assertThat(response.exists(), equalTo(false));

        logger.info("--> creating index [foo_foo] and [bar_bar]");
        assertAcked(prepareCreate("foo_foo"));
        assertAcked(prepareCreate("bar_bar"));
        ensureGreen();

        logger.info("--> adding [foo] alias to [foo_foo] and [bar_bar]");
        assertAcked(admin().indices().prepareAliases().addAlias("foo_foo", "foo"));
        assertAcked(admin().indices().prepareAliases().addAlias("bar_bar", "foo"));

        assertAcked(admin().indices().prepareAliases().addAliasAction(AliasActions.remove().index("foo*").alias("foo")).execute().get());

        assertTrue(admin().indices().prepareAliasesExist("foo").get().exists());
        assertFalse(admin().indices().prepareAliasesExist("foo").setIndices("foo_foo").get().exists());
        assertTrue(admin().indices().prepareAliasesExist("foo").setIndices("bar_bar").get().exists());
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> admin().indices().prepareAliases()
                .addAliasAction(AliasActions.remove().index("foo").alias("foo")).execute().actionGet());
        assertEquals("The provided expression [foo] matches an alias, specify the corresponding concrete indices instead.",
                iae.getMessage());
    }

    public void testWaitForAliasCreationMultipleShards() throws Exception {
        logger.info("--> creating index [test]");
        createIndex("test");

        ensureGreen();

        for (int i = 0; i < 10; i++) {
            assertAcked(admin().indices().prepareAliases().addAlias("test", "alias" + i));
            client().index(indexRequest("alias" + i).type("type1").id("1").source(source("1", "test"), XContentType.JSON)).get();
        }
    }

    public void testWaitForAliasCreationSingleShard() throws Exception {
        logger.info("--> creating index [test]");
        assertAcked(admin().indices().create(createIndexRequest("test").settings(Settings.builder().put("index.number_of_replicas", 0).put("index.number_of_shards", 1))).get());

        ensureGreen();

        for (int i = 0; i < 10; i++) {
            assertAcked(admin().indices().prepareAliases().addAlias("test", "alias" + i));
            client().index(indexRequest("alias" + i).type("type1").id("1").source(source("1", "test"), XContentType.JSON)).get();
        }
    }

    public void testWaitForAliasSimultaneousUpdate() throws Exception {
        final int aliasCount = 10;

        logger.info("--> creating index [test]");
        createIndex("test");

        ensureGreen();

        ExecutorService executor = Executors.newFixedThreadPool(aliasCount);
        for (int i = 0; i < aliasCount; i++) {
            final String aliasName = "alias" + i;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    assertAcked(admin().indices().prepareAliases().addAlias("test", aliasName));
                    client().index(indexRequest(aliasName).type("type1").id("1").source(source("1", "test"), XContentType.JSON))
                        .actionGet();
                }
            });
        }
        executor.shutdown();
        boolean done = executor.awaitTermination(20, TimeUnit.SECONDS);
        assertThat(done, equalTo(true));
        if (!done) {
            executor.shutdownNow();
        }
    }

    public void testSameAlias() throws Exception {
        logger.info("--> creating index [test]");
        assertAcked(prepareCreate("test").addMapping("type", "name", "type=text"));
        ensureGreen();

        logger.info("--> creating alias1 ");
        assertAcked((admin().indices().prepareAliases().addAlias("test", "alias1")));
        TimeValue timeout = TimeValue.timeValueSeconds(2);
        logger.info("--> recreating alias1 ");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        assertAcked((admin().indices().prepareAliases().addAlias("test", "alias1").setTimeout(timeout)));
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));

        logger.info("--> modifying alias1 to have a filter");
        stopWatch.start();
        assertAcked((admin().indices().prepareAliases().addAlias("test", "alias1", termQuery("name", "foo")).setTimeout(timeout)));
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));

        logger.info("--> recreating alias1 with the same filter");
        stopWatch.start();
        assertAcked((admin().indices().prepareAliases().addAlias("test", "alias1", termQuery("name", "foo")).setTimeout(timeout)));
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));

        logger.info("--> recreating alias1 with a different filter");
        stopWatch.start();
        assertAcked((admin().indices().prepareAliases().addAlias("test", "alias1", termQuery("name", "bar")).setTimeout(timeout)));
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));

        logger.info("--> verify that filter was updated");
        AliasMetaData aliasMetaData = ((AliasOrIndex.Alias) internalCluster().clusterService().state().metaData().getAliasAndIndexLookup().get("alias1")).getFirstAliasMetaData();
        assertThat(aliasMetaData.getFilter().toString(), equalTo("{\"term\":{\"name\":{\"value\":\"bar\",\"boost\":1.0}}}"));

        logger.info("--> deleting alias1");
        stopWatch.start();
        assertAcked((admin().indices().prepareAliases().removeAlias("test", "alias1").setTimeout(timeout)));
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));


    }

    public void testIndicesRemoveNonExistingAliasResponds404() throws Exception {
        logger.info("--> creating index [test]");
        createIndex("test");
        ensureGreen();
        logger.info("--> deleting alias1 which does not exist");
        try {
            admin().indices().prepareAliases().removeAlias("test", "alias1").get();
            fail("Expected AliasesNotFoundException");
        } catch (AliasesNotFoundException e) {
            assertThat(e.getMessage(), containsString("[alias1] missing"));
        }
    }

    public void testIndicesGetAliases() throws Exception {
        logger.info("--> creating indices [foobar, test, test123, foobarbaz, bazbar]");
        createIndex("foobar");
        createIndex("test");
        createIndex("test123");
        createIndex("foobarbaz");
        createIndex("bazbar");

        assertAcked(client().admin().indices().preparePutMapping("foobar", "test", "test123", "foobarbaz", "bazbar")
                .setType("type").setSource("field", "type=text"));
        ensureGreen();

        logger.info("--> creating aliases [alias1, alias2]");
        assertAcked(admin().indices().prepareAliases().addAlias("foobar", "alias1").addAlias("foobar", "alias2"));

        logger.info("--> getting alias1");
        GetAliasesResponse getResponse = admin().indices().prepareGetAliases("alias1").get();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(5));
        assertThat(getResponse.getAliases().get("foobar").size(), equalTo(1));
        assertThat(getResponse.getAliases().get("foobar").get(0), notNullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).alias(), equalTo("alias1"));
        assertThat(getResponse.getAliases().get("foobar").get(0).getFilter(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getSearchRouting(), nullValue());
        assertTrue(getResponse.getAliases().get("test").isEmpty());
        assertTrue(getResponse.getAliases().get("test123").isEmpty());
        assertTrue(getResponse.getAliases().get("foobarbaz").isEmpty());
        assertTrue(getResponse.getAliases().get("bazbar").isEmpty());
        AliasesExistResponse existsResponse = admin().indices().prepareAliasesExist("alias1").get();
        assertThat(existsResponse.exists(), equalTo(true));

        logger.info("--> getting all aliases that start with alias*");
        getResponse = admin().indices().prepareGetAliases("alias*").get();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(5));
        assertThat(getResponse.getAliases().get("foobar").size(), equalTo(2));
        assertThat(getResponse.getAliases().get("foobar").get(0), notNullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).alias(), equalTo("alias1"));
        assertThat(getResponse.getAliases().get("foobar").get(0).getFilter(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getSearchRouting(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(1), notNullValue());
        assertThat(getResponse.getAliases().get("foobar").get(1).alias(), equalTo("alias2"));
        assertThat(getResponse.getAliases().get("foobar").get(1).getFilter(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(1).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(1).getSearchRouting(), nullValue());
        assertTrue(getResponse.getAliases().get("test").isEmpty());
        assertTrue(getResponse.getAliases().get("test123").isEmpty());
        assertTrue(getResponse.getAliases().get("foobarbaz").isEmpty());
        assertTrue(getResponse.getAliases().get("bazbar").isEmpty());
        existsResponse = admin().indices().prepareAliasesExist("alias*").get();
        assertThat(existsResponse.exists(), equalTo(true));


        logger.info("--> creating aliases [bar, baz, foo]");
        assertAcked(admin().indices().prepareAliases()
                .addAlias("bazbar", "bar")
                .addAlias("bazbar", "bac", termQuery("field", "value"))
                .addAlias("foobar", "foo"));

        assertAcked(admin().indices().prepareAliases()
                .addAliasAction(AliasActions.add().index("foobar").alias("bac").routing("bla")));

        logger.info("--> getting bar and baz for index bazbar");
        getResponse = admin().indices().prepareGetAliases("bar", "bac").addIndices("bazbar").get();
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
        existsResponse = admin().indices().prepareAliasesExist("bar", "bac")
                .addIndices("bazbar").get();
        assertThat(existsResponse.exists(), equalTo(true));

        logger.info("--> getting *b* for index baz*");
        getResponse = admin().indices().prepareGetAliases("*b*").addIndices("baz*").get();
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
        existsResponse = admin().indices().prepareAliasesExist("*b*")
                .addIndices("baz*").get();
        assertThat(existsResponse.exists(), equalTo(true));

        logger.info("--> getting *b* for index *bar");
        getResponse = admin().indices().prepareGetAliases("b*").addIndices("*bar").get();
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
        existsResponse = admin().indices().prepareAliasesExist("b*")
                .addIndices("*bar").get();
        assertThat(existsResponse.exists(), equalTo(true));

        logger.info("--> getting f* for index *bar");
        getResponse = admin().indices().prepareGetAliases("f*").addIndices("*bar").get();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(2));
        assertThat(getResponse.getAliases().get("foobar").get(0), notNullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).alias(), equalTo("foo"));
        assertThat(getResponse.getAliases().get("foobar").get(0).getFilter(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getSearchRouting(), nullValue());
        assertTrue(getResponse.getAliases().get("bazbar").isEmpty());
        existsResponse = admin().indices().prepareAliasesExist("f*")
                .addIndices("*bar").get();
        assertThat(existsResponse.exists(), equalTo(true));

        // alias at work
        logger.info("--> getting f* for index *bac");
        getResponse = admin().indices().prepareGetAliases("foo").addIndices("*bac").get();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(2));
        assertThat(getResponse.getAliases().get("foobar").size(), equalTo(1));
        assertThat(getResponse.getAliases().get("foobar").get(0), notNullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).alias(), equalTo("foo"));
        assertThat(getResponse.getAliases().get("foobar").get(0).getFilter(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getSearchRouting(), nullValue());
        assertTrue(getResponse.getAliases().get("bazbar").isEmpty());
        existsResponse = admin().indices().prepareAliasesExist("foo")
                .addIndices("*bac").get();
        assertThat(existsResponse.exists(), equalTo(true));

        logger.info("--> getting foo for index foobar");
        getResponse = admin().indices().prepareGetAliases("foo").addIndices("foobar").get();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(1));
        assertThat(getResponse.getAliases().get("foobar").get(0), notNullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).alias(), equalTo("foo"));
        assertThat(getResponse.getAliases().get("foobar").get(0).getFilter(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getSearchRouting(), nullValue());
        existsResponse = admin().indices().prepareAliasesExist("foo")
                .addIndices("foobar").get();
        assertThat(existsResponse.exists(), equalTo(true));

        // alias at work again
        logger.info("--> getting * for index *bac");
        getResponse = admin().indices().prepareGetAliases("*").addIndices("*bac").get();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(2));
        assertThat(getResponse.getAliases().get("foobar").size(), equalTo(4));
        assertThat(getResponse.getAliases().get("bazbar").size(), equalTo(2));
        existsResponse = admin().indices().prepareAliasesExist("*")
                .addIndices("*bac").get();
        assertThat(existsResponse.exists(), equalTo(true));

        assertAcked(admin().indices().prepareAliases()
                .removeAlias("foobar", "foo"));

        getResponse = admin().indices().prepareGetAliases("foo").addIndices("foobar").get();
        for (final ObjectObjectCursor<String, List<AliasMetaData>> entry : getResponse.getAliases()) {
            assertTrue(entry.value.isEmpty());
        }
        existsResponse = admin().indices().prepareAliasesExist("foo").addIndices("foobar").get();
        assertThat(existsResponse.exists(), equalTo(false));
    }

    public void testGetAllAliasesWorks() {
        createIndex("index1");
        createIndex("index2");

        assertAcked(admin().indices().prepareAliases().addAlias("index1", "alias1").addAlias("index2", "alias2"));

        GetAliasesResponse response = admin().indices().prepareGetAliases().get();
        assertThat(response.getAliases(), hasKey("index1"));
        assertThat(response.getAliases(), hasKey("index1"));
    }

    public void testCreateIndexWithAliases() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type", "field", "type=text")
                .addAlias(new Alias("alias1"))
                .addAlias(new Alias("alias2").filter(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("field"))))
                .addAlias(new Alias("alias3").indexRouting("index").searchRouting("search")));

        checkAliases();
    }

    public void testCreateIndexWithAliasesInSource() throws Exception {
        assertAcked(prepareCreate("test").setSource("{\n" +
                "    \"aliases\" : {\n" +
                "        \"alias1\" : {},\n" +
                "        \"alias2\" : {\"filter\" : {\"match_all\": {}}},\n" +
                "        \"alias3\" : { \"index_routing\" : \"index\", \"search_routing\" : \"search\"}\n" +
                "    }\n" +
                "}", XContentType.JSON));

        checkAliases();
    }

    public void testCreateIndexWithAliasesSource() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type", "field", "type=text")
                .setAliases("{\n" +
                        "        \"alias1\" : {},\n" +
                        "        \"alias2\" : {\"filter\" : {\"term\": {\"field\":\"value\"}}},\n" +
                        "        \"alias3\" : { \"index_routing\" : \"index\", \"search_routing\" : \"search\"}\n" +
                        "}"));

        checkAliases();
    }

    public void testCreateIndexWithAliasesFilterNotValid() {
        //non valid filter, invalid json
        CreateIndexRequestBuilder createIndexRequestBuilder = prepareCreate("test").addAlias(new Alias("alias2").filter("f"));

        try {
            createIndexRequestBuilder.get();
            fail("create index should have failed due to invalid alias filter");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("failed to parse filter for alias [alias2]"));
        }

        //valid json but non valid filter
        createIndexRequestBuilder = prepareCreate("test").addAlias(new Alias("alias2").filter("{ \"test\": {} }"));

        try {
            createIndexRequestBuilder.get();
            fail("create index should have failed due to invalid alias filter");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("failed to parse filter for alias [alias2]"));
        }
    }

    public void testAliasesCanBeAddedToIndicesOnly() throws Exception {
        logger.info("--> creating index [2017-05-20]");
        assertAcked(prepareCreate("2017-05-20"));
        ensureGreen();

        logger.info("--> adding [week_20] alias to [2017-05-20]");
        assertAcked(admin().indices().prepareAliases().addAlias("2017-05-20", "week_20"));

        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> admin().indices().prepareAliases()
                .addAliasAction(AliasActions.add().index("week_20").alias("tmp")).execute().actionGet());
        assertEquals("The provided expression [week_20] matches an alias, specify the corresponding concrete indices instead.",
                iae.getMessage());
        assertAcked(admin().indices().prepareAliases().addAliasAction(AliasActions.add().index("2017-05-20").alias("tmp")).execute().get());
    }

    // Before 2.0 alias filters were parsed at alias creation time, in order
    // for filters to work correctly ES required that fields mentioned in those
    // filters exist in the mapping.
    // From 2.0 and higher alias filters are parsed at request time and therefor
    // fields mentioned in filters don't need to exist in the mapping.
    public void testAddAliasWithFilterNoMapping() throws Exception {
        assertAcked(prepareCreate("test"));
        client().admin().indices().prepareAliases()
                .addAlias("test", "a", QueryBuilders.termQuery("field1", "term"))
                .get();
        client().admin().indices().prepareAliases()
                .addAlias("test", "a", QueryBuilders.rangeQuery("field2").from(0).to(1))
                .get();
        client().admin().indices().prepareAliases()
                .addAlias("test", "a", QueryBuilders.matchAllQuery())
                .get();
    }

    public void testAliasFilterWithNowInRangeFilterAndQuery() throws Exception {
        assertAcked(prepareCreate("my-index").addMapping("my-type", "timestamp", "type=date"));
        assertAcked(admin().indices().prepareAliases().addAlias("my-index", "filter1", rangeQuery("timestamp").from("2016-12-01").to("2016-12-31")));
        assertAcked(admin().indices().prepareAliases().addAlias("my-index", "filter2", rangeQuery("timestamp").from("2016-01-01").to("2016-12-31")));

        final int numDocs = scaledRandomIntBetween(5, 52);
        for (int i = 1; i <= numDocs; i++) {
            client().prepareIndex("my-index", "my-type").setSource("timestamp", "2016-12-12").get();
            if (i % 2 == 0) {
                refresh();
                SearchResponse response = client().prepareSearch("filter1").get();
                assertHitCount(response, i);

                response = client().prepareSearch("filter2").get();
                assertHitCount(response, i);
            }
        }
    }

    public void testAliasesWithBlocks() {
        createIndex("test");
        ensureGreen();

        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE)) {
            try {
                enableIndexBlock("test", block);

                assertAcked(admin().indices().prepareAliases().addAlias("test", "alias1").addAlias("test", "alias2"));
                assertAcked(admin().indices().prepareAliases().removeAlias("test", "alias1"));
                assertThat(admin().indices().prepareGetAliases("alias2").execute().actionGet().getAliases().get("test").size(), equalTo(1));
                assertThat(admin().indices().prepareAliasesExist("alias2").get().exists(), equalTo(true));
            } finally {
                disableIndexBlock("test", block);
            }
        }

        try {
            enableIndexBlock("test", SETTING_READ_ONLY);

            assertBlocked(admin().indices().prepareAliases().addAlias("test", "alias3"), INDEX_READ_ONLY_BLOCK);
            assertBlocked(admin().indices().prepareAliases().removeAlias("test", "alias2"), INDEX_READ_ONLY_BLOCK);
            assertThat(admin().indices().prepareGetAliases("alias2").execute().actionGet().getAliases().get("test").size(), equalTo(1));
            assertThat(admin().indices().prepareAliasesExist("alias2").get().exists(), equalTo(true));

        } finally {
            disableIndexBlock("test", SETTING_READ_ONLY);
        }

        try {
            enableIndexBlock("test", SETTING_BLOCKS_METADATA);

            assertBlocked(admin().indices().prepareAliases().addAlias("test", "alias3"), INDEX_METADATA_BLOCK);
            assertBlocked(admin().indices().prepareAliases().removeAlias("test", "alias2"), INDEX_METADATA_BLOCK);
            assertBlocked(admin().indices().prepareGetAliases("alias2"), INDEX_METADATA_BLOCK);
            assertBlocked(admin().indices().prepareAliasesExist("alias2"), INDEX_METADATA_BLOCK);

        } finally {
            disableIndexBlock("test", SETTING_BLOCKS_METADATA);
        }
    }

    public void testAliasActionRemoveIndex() throws InterruptedException, ExecutionException {
        assertAcked(prepareCreate("foo_foo"));
        assertAcked(prepareCreate("bar_bar"));
        assertAcked(admin().indices().prepareAliases().addAlias("foo_foo", "foo"));
        assertAcked(admin().indices().prepareAliases().addAlias("bar_bar", "foo"));

        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                () -> client().admin().indices().prepareAliases().removeIndex("foo").execute().actionGet());
        assertEquals("The provided expression [foo] matches an alias, specify the corresponding concrete indices instead.",
                iae.getMessage());

        assertAcked(client().admin().indices().prepareAliases().removeIndex("foo*").execute().get());
        assertFalse(client().admin().indices().prepareExists("foo_foo").execute().actionGet().isExists());
        assertTrue(admin().indices().prepareAliasesExist("foo").get().exists());
        assertTrue(client().admin().indices().prepareExists("bar_bar").execute().actionGet().isExists());
        assertTrue(admin().indices().prepareAliasesExist("foo").setIndices("bar_bar").get().exists());

        assertAcked(client().admin().indices().prepareAliases().removeIndex("bar_bar"));
        assertFalse(admin().indices().prepareAliasesExist("foo").get().exists());
        assertFalse(client().admin().indices().prepareExists("bar_bar").execute().actionGet().isExists());
    }

    public void testRemoveIndexAndReplaceWithAlias() throws InterruptedException, ExecutionException {
        assertAcked(client().admin().indices().prepareCreate("test"));
        indexRandom(true, client().prepareIndex("test_2", "test", "test").setSource("test", "test"));
        assertAcked(client().admin().indices().prepareAliases().addAlias("test_2", "test").removeIndex("test"));
        assertHitCount(client().prepareSearch("test").get(), 1);
    }

    private void checkAliases() {
        GetAliasesResponse getAliasesResponse = admin().indices().prepareGetAliases("alias1").get();
        assertThat(getAliasesResponse.getAliases().get("test").size(), equalTo(1));
        AliasMetaData aliasMetaData = getAliasesResponse.getAliases().get("test").get(0);
        assertThat(aliasMetaData.alias(), equalTo("alias1"));
        assertThat(aliasMetaData.filter(), nullValue());
        assertThat(aliasMetaData.indexRouting(), nullValue());
        assertThat(aliasMetaData.searchRouting(), nullValue());

        getAliasesResponse = admin().indices().prepareGetAliases("alias2").get();
        assertThat(getAliasesResponse.getAliases().get("test").size(), equalTo(1));
        aliasMetaData = getAliasesResponse.getAliases().get("test").get(0);
        assertThat(aliasMetaData.alias(), equalTo("alias2"));
        assertThat(aliasMetaData.filter(), notNullValue());
        assertThat(aliasMetaData.indexRouting(), nullValue());
        assertThat(aliasMetaData.searchRouting(), nullValue());

        getAliasesResponse = admin().indices().prepareGetAliases("alias3").get();
        assertThat(getAliasesResponse.getAliases().get("test").size(), equalTo(1));
        aliasMetaData = getAliasesResponse.getAliases().get("test").get(0);
        assertThat(aliasMetaData.alias(), equalTo("alias3"));
        assertThat(aliasMetaData.filter(), nullValue());
        assertThat(aliasMetaData.indexRouting(), equalTo("index"));
        assertThat(aliasMetaData.searchRouting(), equalTo("search"));
    }

    private void assertHits(SearchHits hits, String... ids) {
        assertThat(hits.getTotalHits(), equalTo((long) ids.length));
        Set<String> hitIds = new HashSet<>();
        for (SearchHit hit : hits.getHits()) {
            hitIds.add(hit.getId());
        }
        assertThat(hitIds, containsInAnyOrder(ids));
    }

    private String source(String id, String nameValue) {
        return "{ \"id\" : \"" + id + "\", \"name\" : \"" + nameValue + "\" }";
    }
}
