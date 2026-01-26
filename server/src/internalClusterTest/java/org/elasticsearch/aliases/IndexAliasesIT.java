/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.aliases;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.action.admin.indices.AliasesNotFoundException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_METADATA_BLOCK;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_READ_ONLY_BLOCK;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponses;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class IndexAliasesIT extends ESIntegTestCase {

    public void testAliases() throws Exception {
        logger.info("--> creating index [test]");
        createIndex("test");

        ensureGreen();

        assertAliasesVersionIncreases("test", () -> {
            logger.info("--> aliasing index [test] with [alias1]");
            assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test", "alias1", false));
        });

        logger.info("--> indexing against [alias1], should fail now");
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            client().index(new IndexRequest("alias1").id("1").source(source("2", "test"), XContentType.JSON))
        );
        assertThat(
            exception.getMessage(),
            equalTo(
                "no write index is defined for alias [alias1]."
                    + " The write index may be explicitly disabled using is_write_index=false or the alias points to multiple"
                    + " indices without one being designated as a write index"
            )
        );

        assertAliasesVersionIncreases("test", () -> {
            logger.info("--> aliasing index [test] with [alias1]");
            assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test", "alias1"));
        });

        logger.info("--> indexing against [alias1], should work now");
        DocWriteResponse indexResponse = client().index(new IndexRequest("alias1").id("1").source(source("1", "test"), XContentType.JSON))
            .actionGet();
        assertThat(indexResponse.getIndex(), equalTo("test"));

        logger.info("--> creating index [test_x]");
        createIndex("test_x");

        ensureGreen();

        assertAliasesVersionIncreases("test_x", () -> {
            logger.info("--> add index [test_x] with [alias1]");
            assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test_x", "alias1"));
        });

        logger.info("--> indexing against [alias1], should fail now");
        exception = expectThrows(
            IllegalArgumentException.class,
            client().index(new IndexRequest("alias1").id("1").source(source("2", "test"), XContentType.JSON))
        );
        assertThat(
            exception.getMessage(),
            equalTo(
                "no write index is defined for alias [alias1]."
                    + " The write index may be explicitly disabled using is_write_index=false or the alias points to multiple"
                    + " indices without one being designated as a write index"
            )
        );

        logger.info("--> deleting against [alias1], should fail now");
        exception = expectThrows(IllegalArgumentException.class, client().delete(new DeleteRequest("alias1").id("1")));
        assertThat(
            exception.getMessage(),
            equalTo(
                "no write index is defined for alias [alias1]."
                    + " The write index may be explicitly disabled using is_write_index=false or the alias points to multiple"
                    + " indices without one being designated as a write index"
            )
        );

        assertAliasesVersionIncreases("test_x", () -> {
            logger.info("--> remove aliasing index [test_x] with [alias1]");
            assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).removeAlias("test_x", "alias1"));
        });

        logger.info("--> indexing against [alias1], should work now");
        indexResponse = client().index(new IndexRequest("alias1").id("1").source(source("1", "test"), XContentType.JSON)).actionGet();
        assertThat(indexResponse.getIndex(), equalTo("test"));

        assertAliasesVersionIncreases("test_x", () -> {
            logger.info("--> add index [test_x] with [alias1] as write-index");
            assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test_x", "alias1", true));
        });

        logger.info("--> indexing against [alias1], should work now");
        indexResponse = client().index(new IndexRequest("alias1").id("1").source(source("1", "test"), XContentType.JSON)).actionGet();
        assertThat(indexResponse.getIndex(), equalTo("test_x"));

        logger.info("--> deleting against [alias1], should fail now");
        DeleteResponse deleteResponse = client().delete(new DeleteRequest("alias1").id("1")).actionGet();
        assertThat(deleteResponse.getIndex(), equalTo("test_x"));

        assertAliasesVersionIncreases("test_x", () -> {
            logger.info("--> remove [alias1], Aliasing index [test_x] with [alias1]");
            assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .removeAlias("test", "alias1")
                    .addAlias("test_x", "alias1")
            );
        });

        logger.info("--> indexing against [alias1], should work against [test_x]");
        indexResponse = client().index(new IndexRequest("alias1").id("1").source(source("1", "test"), XContentType.JSON)).actionGet();
        assertThat(indexResponse.getIndex(), equalTo("test_x"));
    }

    public void testFailedFilter() throws Exception {
        logger.info("--> creating index [test]");
        createIndex("test");

        // invalid filter, invalid json
        Exception e = expectThrows(
            IllegalArgumentException.class,
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test", "alias1", "abcde")
        );
        assertThat(e.getMessage(), equalTo("failed to parse filter for alias [alias1]"));

        // valid json , invalid filter
        e = expectThrows(
            IllegalArgumentException.class,
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test", "alias1", "{ \"test\": {} }")
        );
        assertThat(e.getMessage(), equalTo("failed to parse filter for alias [alias1]"));
    }

    public void testFilteringAliases() throws Exception {
        logger.info("--> creating index [test]");
        assertAcked(prepareCreate("test").setMapping("user", "type=text"));

        ensureGreen();

        logger.info("--> aliasing index [test] with [alias1] and filter [user:kimchy]");
        QueryBuilder filter = termQuery("user", "kimchy");
        assertAliasesVersionIncreases(
            "test",
            () -> assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test", "alias1", filter))
        );

        // For now just making sure that filter was stored with the alias
        logger.info("--> making sure that filter was stored with alias [alias1] and filter [user:kimchy]");
        ClusterState clusterState = admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        IndexMetadata indexMd = clusterState.metadata().getProject().index("test");
        assertThat(indexMd.getAliases().get("alias1").filter().string(), equalTo("""
            {"term":{"user":{"value":"kimchy"}}}"""));

    }

    public void testEmptyFilter() throws Exception {
        logger.info("--> creating index [test]");
        createIndex("test");
        ensureGreen();

        logger.info("--> aliasing index [test] with [alias1] and empty filter");
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test", "alias1", "{}")
        );
        assertEquals("failed to parse filter for alias [alias1]", iae.getMessage());
    }

    public void testSearchingFilteringAliasesSingleIndex() throws Exception {
        logger.info("--> creating index [test]");
        assertAcked(prepareCreate("test").setMapping("id", "type=text", "name", "type=text,fielddata=true"));

        ensureGreen();

        logger.info("--> adding filtering aliases to index [test]");

        assertAliasesVersionIncreases(
            "test",
            () -> assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test", "alias1"))
        );
        assertAliasesVersionIncreases(
            "test",
            () -> assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test", "alias2"))
        );
        assertAliasesVersionIncreases(
            "test",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test", "foos", termQuery("name", "foo"))
            )
        );
        assertAliasesVersionIncreases(
            "test",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test", "bars", termQuery("name", "bar"))
            )
        );
        assertAliasesVersionIncreases(
            "test",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("test", "tests", termQuery("name", "test"))
            )
        );

        logger.info("--> indexing against [test]");
        client().index(
            new IndexRequest("test").id("1").source(source("1", "foo test"), XContentType.JSON).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
        ).actionGet();
        client().index(
            new IndexRequest("test").id("2").source(source("2", "bar test"), XContentType.JSON).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
        ).actionGet();
        client().index(
            new IndexRequest("test").id("3").source(source("3", "baz test"), XContentType.JSON).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
        ).actionGet();
        client().index(
            new IndexRequest("test").id("4")
                .source(source("4", "something else"), XContentType.JSON)
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
        ).actionGet();

        assertResponses(
            searchResponse -> assertHits(searchResponse.getHits(), "1"),
            prepareSearch("foos").setQuery(QueryBuilders.matchAllQuery()),
            prepareSearch("fo*").setQuery(QueryBuilders.matchAllQuery())
        );

        assertResponses(
            searchResponse -> assertHits(searchResponse.getHits(), "1", "2", "3"),
            prepareSearch("tests").setQuery(QueryBuilders.matchAllQuery()),
            prepareSearch("tests").setQuery(QueryBuilders.matchAllQuery()).addSort("_index", SortOrder.ASC)
        );

        logger.info("--> checking single filtering alias search with global facets");
        assertNoFailuresAndResponse(
            prepareSearch("tests").setQuery(QueryBuilders.matchQuery("name", "bar"))
                .addAggregation(AggregationBuilders.global("global").subAggregation(AggregationBuilders.terms("test").field("name"))),
            searchResponse -> {
                SingleBucketAggregation global = searchResponse.getAggregations().get("global");
                Terms terms = global.getAggregations().get("test");
                assertThat(terms.getBuckets().size(), equalTo(4));
            }
        );

        logger.info("--> checking single filtering alias search with global facets and sort");
        assertNoFailuresAndResponse(
            prepareSearch("tests").setQuery(QueryBuilders.matchQuery("name", "bar"))
                .addAggregation(AggregationBuilders.global("global").subAggregation(AggregationBuilders.terms("test").field("name")))
                .addSort("_index", SortOrder.ASC),
            searchResponse -> {
                SingleBucketAggregation global = searchResponse.getAggregations().get("global");
                Terms terms = global.getAggregations().get("test");
                assertThat(terms.getBuckets().size(), equalTo(4));
            }
        );

        logger.info("--> checking single filtering alias search with non-global facets");
        assertNoFailuresAndResponse(
            prepareSearch("tests").setQuery(QueryBuilders.matchQuery("name", "bar"))
                .addAggregation(AggregationBuilders.terms("test").field("name"))
                .addSort("_index", SortOrder.ASC),
            searchResponse -> {
                Terms terms = searchResponse.getAggregations().get("test");
                assertThat(terms.getBuckets().size(), equalTo(2));
            }
        );
        assertResponse(
            prepareSearch("foos", "bars").setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertHits(searchResponse.getHits(), "1", "2")
        );

        assertResponses(
            searchResponse -> assertHits(searchResponse.getHits(), "1", "2", "3", "4"),
            prepareSearch("alias1").setQuery(QueryBuilders.matchAllQuery()),
            prepareSearch("alias1", "foos").setQuery(QueryBuilders.matchAllQuery()),
            prepareSearch("test", "foos").setQuery(QueryBuilders.matchAllQuery()),
            prepareSearch("te*", "fo*").setQuery(QueryBuilders.matchAllQuery())
        );
    }

    public void testSearchingFilteringAliasesTwoIndices() throws Exception {
        logger.info("--> creating indices [test1, test2]");
        assertAcked(prepareCreate("test1").setMapping("name", "type=text"), prepareCreate("test2").setMapping("name", "type=text"));
        ensureGreen();

        logger.info("--> adding filtering aliases to index [test1]");
        assertAliasesVersionIncreases(
            "test1",
            () -> assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test1", "aliasToTest1"))
        );
        assertAliasesVersionIncreases(
            "test1",
            () -> assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test1", "aliasToTests"))
        );
        assertAliasesVersionIncreases(
            "test1",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("test1", "foos", termQuery("name", "foo"))
            )
        );
        assertAliasesVersionIncreases(
            "test1",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("test1", "bars", termQuery("name", "bar"))
            )
        );

        logger.info("--> adding filtering aliases to index [test2]");
        assertAliasesVersionIncreases(
            "test2",
            () -> assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test2", "aliasToTest2"))
        );
        assertAliasesVersionIncreases(
            "test2",
            () -> assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test2", "aliasToTests"))
        );
        assertAliasesVersionIncreases(
            "test2",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("test2", "foos", termQuery("name", "foo"))
            )
        );

        logger.info("--> indexing against [test1]");
        client().index(new IndexRequest("test1").id("1").source(source("1", "foo test"), XContentType.JSON)).get();
        client().index(new IndexRequest("test1").id("2").source(source("2", "bar test"), XContentType.JSON)).get();
        client().index(new IndexRequest("test1").id("3").source(source("3", "baz test"), XContentType.JSON)).get();
        client().index(new IndexRequest("test1").id("4").source(source("4", "something else"), XContentType.JSON)).get();

        logger.info("--> indexing against [test2]");
        client().index(new IndexRequest("test2").id("5").source(source("5", "foo test"), XContentType.JSON)).get();
        client().index(new IndexRequest("test2").id("6").source(source("6", "bar test"), XContentType.JSON)).get();
        client().index(new IndexRequest("test2").id("7").source(source("7", "baz test"), XContentType.JSON)).get();
        client().index(new IndexRequest("test2").id("8").source(source("8", "something else"), XContentType.JSON)).get();

        refresh();

        logger.info("--> checking filtering alias for two indices");
        assertResponse(
            prepareSearch("foos").setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertHits(searchResponse.getHits(), "1", "5")
        );
        assertResponse(
            prepareSearch("foos").setSize(0).setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(2L))
        );

        logger.info("--> checking filtering alias for one index");
        assertResponse(
            prepareSearch("bars").setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertHits(searchResponse.getHits(), "2")
        );
        assertResponse(
            prepareSearch("bars").setSize(0).setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(1L))
        );

        logger.info("--> checking filtering alias for two indices and one complete index");
        assertResponse(
            prepareSearch("foos", "test1").setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertHits(searchResponse.getHits(), "1", "2", "3", "4", "5")
        );
        assertResponse(
            prepareSearch("foos", "test1").setSize(0).setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(5L))
        );

        logger.info("--> checking filtering alias for two indices and non-filtering alias for one index");
        assertResponse(
            prepareSearch("foos", "aliasToTest1").setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertHits(searchResponse.getHits(), "1", "2", "3", "4", "5")
        );
        assertResponse(
            prepareSearch("foos", "aliasToTest1").setSize(0).setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(5L))
        );

        logger.info("--> checking filtering alias for two indices and non-filtering alias for both indices");
        assertResponse(
            prepareSearch("foos", "aliasToTests").setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(8L))
        );
        assertResponse(
            prepareSearch("foos", "aliasToTests").setSize(0).setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(8L))
        );

        logger.info("--> checking filtering alias for two indices and non-filtering alias for both indices");
        assertResponse(
            prepareSearch("foos", "aliasToTests").setQuery(QueryBuilders.termQuery("name", "something")),
            searchResponse -> assertHits(searchResponse.getHits(), "4", "8")
        );
        assertResponse(
            prepareSearch("foos", "aliasToTests").setSize(0).setQuery(QueryBuilders.termQuery("name", "something")),
            searchResponse -> assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(2L))
        );
    }

    public void testSearchingFilteringAliasesMultipleIndices() throws Exception {
        logger.info("--> creating indices");
        createIndex("test1", "test2", "test3");

        assertAcked(indicesAdmin().preparePutMapping("test1", "test2", "test3").setSource("name", "type=text"));

        ensureGreen();

        logger.info("--> adding aliases to indices");
        assertAliasesVersionIncreases(
            "test1",
            () -> assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test1", "alias12"))
        );
        assertAliasesVersionIncreases(
            "test2",
            () -> assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test2", "alias12"))
        );

        logger.info("--> adding filtering aliases to indices");
        assertAliasesVersionIncreases(
            "test1",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("test1", "filter1", termQuery("name", "test1"))
            )
        );

        assertAliasesVersionIncreases(
            "test2",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("test2", "filter23", termQuery("name", "foo"))
            )
        );
        assertAliasesVersionIncreases(
            "test3",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("test3", "filter23", termQuery("name", "foo"))
            )
        );

        assertAliasesVersionIncreases(
            "test1",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("test1", "filter13", termQuery("name", "baz"))
            )
        );
        assertAliasesVersionIncreases(
            "test3",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("test3", "filter13", termQuery("name", "baz"))
            )
        );

        logger.info("--> indexing against [test1]");
        client().index(new IndexRequest("test1").id("11").source(source("11", "foo test1"), XContentType.JSON)).get();
        client().index(new IndexRequest("test1").id("12").source(source("12", "bar test1"), XContentType.JSON)).get();
        client().index(new IndexRequest("test1").id("13").source(source("13", "baz test1"), XContentType.JSON)).get();

        client().index(new IndexRequest("test2").id("21").source(source("21", "foo test2"), XContentType.JSON)).get();
        client().index(new IndexRequest("test2").id("22").source(source("22", "bar test2"), XContentType.JSON)).get();
        client().index(new IndexRequest("test2").id("23").source(source("23", "baz test2"), XContentType.JSON)).get();

        client().index(new IndexRequest("test3").id("31").source(source("31", "foo test3"), XContentType.JSON)).get();
        client().index(new IndexRequest("test3").id("32").source(source("32", "bar test3"), XContentType.JSON)).get();
        client().index(new IndexRequest("test3").id("33").source(source("33", "baz test3"), XContentType.JSON)).get();

        refresh();

        logger.info("--> checking filtering alias for multiple indices");
        assertResponse(
            prepareSearch("filter23", "filter13").setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertHits(searchResponse.getHits(), "21", "31", "13", "33")
        );
        assertResponses(
            searchResponse -> assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(4L)),
            prepareSearch("filter23", "filter13").setSize(0).setQuery(QueryBuilders.matchAllQuery()),
            prepareSearch("filter13", "filter1").setSize(0).setQuery(QueryBuilders.matchAllQuery())
        );
        assertResponse(
            prepareSearch("filter23", "filter1").setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertHits(searchResponse.getHits(), "21", "31", "11", "12", "13")
        );
        assertResponse(
            prepareSearch("filter23", "filter1").setSize(0).setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(5L))
        );
        assertResponse(
            prepareSearch("filter13", "filter1").setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertHits(searchResponse.getHits(), "11", "12", "13", "33")
        );
        assertResponse(
            prepareSearch("filter13", "filter1", "filter23").setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertHits(searchResponse.getHits(), "11", "12", "13", "21", "31", "33")
        );
        assertResponse(
            prepareSearch("filter13", "filter1", "filter23").setSize(0).setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(6L))
        );
        assertResponse(
            prepareSearch("filter23", "filter13", "test2").setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertHits(searchResponse.getHits(), "21", "22", "23", "31", "13", "33")
        );
        assertResponse(
            prepareSearch("filter23", "filter13", "test2").setSize(0).setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(6L))
        );
        assertResponse(
            prepareSearch("filter23", "filter13", "test1", "test2").setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertHits(searchResponse.getHits(), "11", "12", "13", "21", "22", "23", "31", "33")
        );
        assertResponse(
            prepareSearch("filter23", "filter13", "test1", "test2").setSize(0).setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(8L))
        );
    }

    public void testDeletingByQueryFilteringAliases() throws Exception {
        logger.info("--> creating index [test1] and [test2");
        assertAcked(prepareCreate("test1").setMapping("name", "type=text"), prepareCreate("test2").setMapping("name", "type=text"));
        ensureGreen();

        logger.info("--> adding filtering aliases to index [test1]");
        assertAliasesVersionIncreases(
            "test1",
            () -> assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test1", "aliasToTest1"))
        );
        assertAliasesVersionIncreases(
            "test1",
            () -> assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test1", "aliasToTests"))
        );
        assertAliasesVersionIncreases(
            "test1",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("test1", "foos", termQuery("name", "foo"))
            )
        );
        assertAliasesVersionIncreases(
            "test1",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("test1", "bars", termQuery("name", "bar"))
            )
        );
        assertAliasesVersionIncreases(
            "test1",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("test1", "tests", termQuery("name", "test"))
            )
        );

        logger.info("--> adding filtering aliases to index [test2]");
        assertAliasesVersionIncreases(
            "test2",
            () -> assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test2", "aliasToTest2"))
        );
        assertAliasesVersionIncreases(
            "test2",
            () -> assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test2", "aliasToTests"))
        );
        assertAliasesVersionIncreases(
            "test2",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("test2", "foos", termQuery("name", "foo"))
            )
        );
        assertAliasesVersionIncreases(
            "test2",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("test2", "tests", termQuery("name", "test"))
            )
        );

        logger.info("--> indexing against [test1]");
        client().index(new IndexRequest("test1").id("1").source(source("1", "foo test"), XContentType.JSON)).get();
        client().index(new IndexRequest("test1").id("2").source(source("2", "bar test"), XContentType.JSON)).get();
        client().index(new IndexRequest("test1").id("3").source(source("3", "baz test"), XContentType.JSON)).get();
        client().index(new IndexRequest("test1").id("4").source(source("4", "something else"), XContentType.JSON)).get();

        logger.info("--> indexing against [test2]");
        client().index(new IndexRequest("test2").id("5").source(source("5", "foo test"), XContentType.JSON)).get();
        client().index(new IndexRequest("test2").id("6").source(source("6", "bar test"), XContentType.JSON)).get();
        client().index(new IndexRequest("test2").id("7").source(source("7", "baz test"), XContentType.JSON)).get();
        client().index(new IndexRequest("test2").id("8").source(source("8", "something else"), XContentType.JSON)).get();

        refresh();

        logger.info("--> checking counts before delete");
        assertResponse(
            prepareSearch("bars").setSize(0).setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(1L))
        );
    }

    public void testDeleteAliases() throws Exception {
        logger.info("--> creating index [test1] and [test2]");
        assertAcked(prepareCreate("test1").setMapping("name", "type=text"), prepareCreate("test2").setMapping("name", "type=text"));
        ensureGreen();

        logger.info("--> adding filtering aliases to index [test1]");
        assertAliasesVersionIncreases(
            "test1",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("test1", "aliasToTest1")
                    .addAlias("test1", "aliasToTests")
                    .addAlias("test1", "foos", termQuery("name", "foo"))
                    .addAlias("test1", "bars", termQuery("name", "bar"))
                    .addAlias("test1", "tests", termQuery("name", "test"))
            )
        );

        logger.info("--> adding filtering aliases to index [test2]");
        assertAliasesVersionIncreases(
            "test2",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("test2", "aliasToTest2")
                    .addAlias("test2", "aliasToTests")
                    .addAlias("test2", "foos", termQuery("name", "foo"))
                    .addAlias("test2", "tests", termQuery("name", "test"))
            )
        );

        String[] indices = { "test1", "test2" };
        String[] aliases = { "aliasToTest1", "foos", "bars", "tests", "aliasToTest2", "aliasToTests" };

        assertAliasesVersionIncreases(
            indices,
            () -> indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).removeAlias(indices, aliases).get()
        );

        for (String alias : aliases) {
            assertTrue(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, alias).get().getAliases().isEmpty());
        }

        logger.info("--> creating index [foo_foo] and [bar_bar]");
        assertAcked(prepareCreate("foo_foo"), prepareCreate("bar_bar"));
        ensureGreen();

        logger.info("--> adding [foo] alias to [foo_foo] and [bar_bar]");
        assertAliasesVersionIncreases(
            "foo_foo",
            () -> assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("foo_foo", "foo"))
        );
        assertAliasesVersionIncreases(
            "bar_bar",
            () -> assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("bar_bar", "foo"))
        );

        assertAliasesVersionIncreases(
            "foo_foo",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAliasAction(AliasActions.remove().index("foo*").alias("foo"))
            )
        );

        assertFalse(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "foo").get().getAliases().isEmpty());
        assertTrue(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "foo").setIndices("foo_foo").get().getAliases().isEmpty());
        assertFalse(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "foo").setIndices("bar_bar").get().getAliases().isEmpty());
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.remove().index("foo").alias("foo"))
        );
        assertEquals(
            "The provided expression [foo] matches an alias, specify the corresponding concrete indices instead.",
            iae.getMessage()
        );
    }

    public void testWaitForAliasCreationMultipleShards() throws Exception {
        logger.info("--> creating index [test]");
        createIndex("test");

        ensureGreen();

        for (int i = 0; i < 10; i++) {
            final String aliasName = "alias" + i;
            assertAliasesVersionIncreases(
                "test",
                () -> assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test", aliasName))
            );
            client().index(new IndexRequest(aliasName).id("1").source(source("1", "test"), XContentType.JSON)).get();
        }
    }

    public void testWaitForAliasCreationSingleShard() throws Exception {
        logger.info("--> creating index [test]");
        assertAcked(indicesAdmin().create(new CreateIndexRequest("test").settings(indexSettings(1, 0))).get());

        ensureGreen();

        for (int i = 0; i < 10; i++) {
            final String aliasName = "alias" + i;
            assertAliasesVersionIncreases(
                "test",
                () -> assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test", aliasName))
            );
            client().index(new IndexRequest(aliasName).id("1").source(source("1", "test"), XContentType.JSON)).get();
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
                    assertAliasesVersionIncreases(
                        "test",
                        () -> assertAcked(
                            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test", aliasName)
                        )
                    );
                    client().index(new IndexRequest(aliasName).id("1").source(source("1", "test"), XContentType.JSON)).actionGet();
                }
            });
        }
        executor.shutdown();
        boolean done = executor.awaitTermination(20, TimeUnit.SECONDS);
        assertThat(done, equalTo(true));
        if (done == false) {
            executor.shutdownNow();
        }
    }

    public void testSameAlias() {
        logger.info("--> creating index [test]");
        assertAcked(prepareCreate("test").setMapping("name", "type=text"));
        ensureGreen();

        logger.info("--> creating alias1 ");
        assertAliasesVersionIncreases(
            "test",
            () -> assertAcked((indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test", "alias1")))
        );
        TimeValue timeout = TimeValue.timeValueSeconds(2);
        logger.info("--> recreating alias1 ");
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        assertAliasesVersionUnchanged(
            "test",
            () -> assertAcked(
                (indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test", "alias1").setTimeout(timeout))
            )
        );
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));

        logger.info("--> modifying alias1 to have a filter");
        stopWatch.start();
        final TermQueryBuilder fooFilter = termQuery("name", "foo");
        assertAliasesVersionIncreases(
            "test",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("test", "alias1", fooFilter)
                    .setTimeout(timeout)
            )
        );
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));

        logger.info("--> recreating alias1 with the same filter");
        stopWatch.start();
        assertAliasesVersionUnchanged(
            "test",
            () -> assertAcked(
                (indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("test", "alias1", fooFilter)
                    .setTimeout(timeout))
            )
        );
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));

        logger.info("--> recreating alias1 with a different filter");
        stopWatch.start();
        final TermQueryBuilder barFilter = termQuery("name", "bar");
        assertAliasesVersionIncreases(
            "test",
            () -> assertAcked(
                (indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("test", "alias1", barFilter)
                    .setTimeout(timeout))
            )
        );
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));

        logger.info("--> verify that filter was updated");
        ProjectMetadata metadata = internalCluster().clusterService().state().metadata().getProject();
        IndexAbstraction ia = metadata.getIndicesLookup().get("alias1");
        AliasMetadata aliasMetadata = AliasMetadata.getFirstAliasMetadata(metadata, ia);
        assertThat(aliasMetadata.getFilter().toString(), equalTo("""
            {"term":{"name":{"value":"bar"}}}"""));

        logger.info("--> deleting alias1");
        stopWatch.start();
        assertAliasesVersionIncreases(
            "test",
            () -> assertAcked(
                (indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .removeAlias("test", "alias1")
                    .setTimeout(timeout))
            )
        );
        assertThat(stopWatch.stop().lastTaskTime().millis(), lessThan(timeout.millis()));
    }

    public void testIndicesRemoveNonExistingAliasResponds404() throws Exception {
        logger.info("--> creating index [test]");
        createIndex("test");
        ensureGreen();
        logger.info("--> deleting alias1 which does not exist");
        try {
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).removeAlias("test", "alias1").get();
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

        assertAcked(indicesAdmin().preparePutMapping("foobar", "test", "test123", "foobarbaz", "bazbar").setSource("field", "type=text"));
        ensureGreen();

        logger.info("--> creating aliases [alias1, alias2]");
        assertAliasesVersionIncreases(
            "foobar",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("foobar", "alias1")
                    .addAlias("foobar", "alias2")
            )
        );

        logger.info("--> getting alias1");
        GetAliasesResponse getResponse = indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "alias1").get();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(1));
        assertThat(getResponse.getAliases().get("foobar").size(), equalTo(1));
        assertThat(getResponse.getAliases().get("foobar").get(0), notNullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).alias(), equalTo("alias1"));
        assertThat(getResponse.getAliases().get("foobar").get(0).getFilter(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getSearchRouting(), nullValue());
        assertFalse(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "alias1").get().getAliases().isEmpty());

        logger.info("--> getting all aliases that start with alias*");
        getResponse = indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "alias*").get();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(1));
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
        assertFalse(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "alias*").get().getAliases().isEmpty());

        logger.info("--> creating aliases [bar, baz, foo]");
        assertAliasesVersionIncreases(
            new String[] { "bazbar", "foobar" },
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("bazbar", "bar")
                    .addAlias("bazbar", "bac", termQuery("field", "value"))
                    .addAlias("foobar", "foo")
            )
        );

        assertAliasesVersionIncreases(
            "foobar",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAliasAction(AliasActions.add().index("foobar").alias("bac").routing("bla"))
            )
        );

        logger.info("--> getting bar and baz for index bazbar");
        getResponse = indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "bar", "bac").setIndices("bazbar").get();
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
        assertFalse(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "bar").get().getAliases().isEmpty());
        assertFalse(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "bac").get().getAliases().isEmpty());
        assertFalse(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "bar").setIndices("bazbar").get().getAliases().isEmpty());
        assertFalse(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "bac").setIndices("bazbar").get().getAliases().isEmpty());

        logger.info("--> getting *b* for index baz*");
        getResponse = indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "*b*").setIndices("baz*").get();
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
        assertFalse(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "*b*").setIndices("baz*").get().getAliases().isEmpty());

        logger.info("--> getting *b* for index *bar");
        getResponse = indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "b*").setIndices("*bar").get();
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
        assertFalse(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "b*").setIndices("*bar").get().getAliases().isEmpty());

        logger.info("--> getting f* for index *bar");
        getResponse = indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "f*").setIndices("*bar").get();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(1));
        assertThat(getResponse.getAliases().get("foobar").get(0), notNullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).alias(), equalTo("foo"));
        assertThat(getResponse.getAliases().get("foobar").get(0).getFilter(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getSearchRouting(), nullValue());
        assertFalse(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "f*").setIndices("*bar").get().getAliases().isEmpty());

        // alias at work
        logger.info("--> getting f* for index *bac");
        getResponse = indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "foo").setIndices("*bac").get();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(1));
        assertThat(getResponse.getAliases().get("foobar").size(), equalTo(1));
        assertThat(getResponse.getAliases().get("foobar").get(0), notNullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).alias(), equalTo("foo"));
        assertThat(getResponse.getAliases().get("foobar").get(0).getFilter(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getSearchRouting(), nullValue());
        assertFalse(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "foo").setIndices("*bac").get().getAliases().isEmpty());

        logger.info("--> getting foo for index foobar");
        getResponse = indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "foo").setIndices("foobar").get();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(1));
        assertThat(getResponse.getAliases().get("foobar").get(0), notNullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).alias(), equalTo("foo"));
        assertThat(getResponse.getAliases().get("foobar").get(0).getFilter(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getIndexRouting(), nullValue());
        assertThat(getResponse.getAliases().get("foobar").get(0).getSearchRouting(), nullValue());
        assertFalse(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "foo").setIndices("foobar").get().getAliases().isEmpty());

        for (String aliasName : new String[] { null, "_all", "*" }) {
            logger.info("--> getting {} alias for index foobar", aliasName);
            getResponse = aliasName != null
                ? indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, aliasName).setIndices("foobar").get()
                : indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT).setIndices("foobar").get();
            assertThat(getResponse, notNullValue());
            assertThat(getResponse.getAliases().size(), equalTo(1));
            assertThat(getResponse.getAliases().get("foobar").size(), equalTo(4));
            assertThat(getResponse.getAliases().get("foobar").get(0).alias(), equalTo("alias1"));
            assertThat(getResponse.getAliases().get("foobar").get(1).alias(), equalTo("alias2"));
            assertThat(getResponse.getAliases().get("foobar").get(2).alias(), equalTo("bac"));
            assertThat(getResponse.getAliases().get("foobar").get(3).alias(), equalTo("foo"));
        }

        // alias at work again
        logger.info("--> getting * for index *bac");
        getResponse = indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "*").setIndices("*bac").get();
        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getAliases().size(), equalTo(2));
        assertThat(getResponse.getAliases().get("foobar").size(), equalTo(4));
        assertThat(getResponse.getAliases().get("bazbar").size(), equalTo(2));
        assertFalse(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "*").setIndices("*bac").get().getAliases().isEmpty());

        assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).removeAlias("foobar", "foo"));

        getResponse = indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "foo").setIndices("foobar").get();
        for (final Map.Entry<String, List<AliasMetadata>> entry : getResponse.getAliases().entrySet()) {
            assertTrue(entry.getValue().isEmpty());
        }
        assertTrue(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "foo").setIndices("foobar").get().getAliases().isEmpty());
    }

    public void testGetAllAliasesWorks() {
        createIndex("index1");
        createIndex("index2");

        assertAliasesVersionIncreases(
            new String[] { "index1", "index2" },
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("index1", "alias1")
                    .addAlias("index2", "alias2")
            )
        );

        GetAliasesResponse response = indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT).get();
        assertThat(response.getAliases(), hasKey("index1"));
        assertThat(response.getAliases(), hasKey("index1"));
    }

    public void testCreateIndexWithAliases() {
        assertAcked(
            prepareCreate("test").setMapping("field", "type=text")
                .addAlias(new Alias("alias1"))
                .addAlias(new Alias("alias2").filter(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("field"))))
                .addAlias(new Alias("alias3").indexRouting("index").searchRouting("search"))
                .addAlias(new Alias("alias4").isHidden(true))
        );

        checkAliases();
    }

    public void testCreateIndexWithAliasesInSource() throws Exception {
        assertAcked(prepareCreate("test").setSource("""
            {
                "aliases" : {
                    "alias1" : {},
                    "alias2" : {"filter" : {"match_all": {}}},
                    "alias3" : { "index_routing" : "index", "search_routing" : "search"},
                    "alias4" : {"is_hidden":  true}
                }
            }""", XContentType.JSON));

        checkAliases();
    }

    public void testCreateIndexWithAliasesSource() {
        assertAcked(prepareCreate("test").setMapping("field", "type=text").setAliases("""
            {
                    "alias1" : {},
                    "alias2" : {"filter" : {"term": {"field":"value"}}},
                    "alias3" : { "index_routing" : "index", "search_routing" : "search"},
                    "alias4" : {"is_hidden":  true}
            }"""));

        checkAliases();
    }

    public void testCreateIndexWithAliasesFilterNotValid() {
        // non valid filter, invalid json
        CreateIndexRequestBuilder createIndexRequestBuilder = prepareCreate("test").addAlias(new Alias("alias2").filter("f"));

        try {
            createIndexRequestBuilder.get();
            fail("create index should have failed due to invalid alias filter");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("failed to parse filter for alias [alias2]"));
        }

        // valid json but non valid filter
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
        assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("2017-05-20", "week_20"));

        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.add().index("week_20").alias("tmp"))
        );
        assertEquals(
            "The provided expression [week_20] matches an alias, specify the corresponding concrete indices instead.",
            iae.getMessage()
        );
        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.add().index("2017-05-20").alias("tmp"))
        );
    }

    // Before 2.0 alias filters were parsed at alias creation time, in order
    // for filters to work correctly ES required that fields mentioned in those
    // filters exist in the mapping.
    // From 2.0 and higher alias filters are parsed at request time and therefor
    // fields mentioned in filters don't need to exist in the mapping.
    public void testAddAliasWithFilterNoMapping() throws Exception {
        assertAcked(prepareCreate("test"));
        assertAliasesVersionIncreases(
            "test",
            () -> indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAlias("test", "a", QueryBuilders.termQuery("field1", "term"))
                .get()
        );
        assertAliasesVersionIncreases(
            "test",
            () -> indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAlias("test", "a", QueryBuilders.rangeQuery("field2").from(0).to(1))
                .get()
        );
        assertAliasesVersionIncreases(
            "test",
            () -> indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAlias("test", "a", QueryBuilders.matchAllQuery())
                .get()
        );
    }

    public void testAliasFilterWithNowInRangeFilterAndQuery() {
        assertAcked(prepareCreate("my-index").setMapping("timestamp", "type=date"));
        assertAliasesVersionIncreases(
            "my-index",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("my-index", "filter1", rangeQuery("timestamp").from("2016-12-01").to("2016-12-31"))
            )
        );
        assertAliasesVersionIncreases(
            "my-index",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .addAlias("my-index", "filter2", rangeQuery("timestamp").from("2016-01-01").to("2016-12-31"))
            )
        );

        final int numDocs = scaledRandomIntBetween(5, 52);
        for (int i = 1; i <= numDocs; i++) {
            prepareIndex("my-index").setSource("timestamp", "2016-12-12").get();
            if (i % 2 == 0) {
                refresh();
                assertHitCount(prepareSearch("filter1"), i);
                assertHitCount(prepareSearch("filter2"), i);
            }
        }
    }

    public void testAliasesWithBlocks() {
        createIndex("test");
        ensureGreen();

        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE)) {
            try {
                enableIndexBlock("test", block);

                assertAliasesVersionIncreases(
                    "test",
                    () -> assertAcked(
                        indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                            .addAlias("test", "alias1")
                            .addAlias("test", "alias2")
                    )
                );
                assertAliasesVersionIncreases(
                    "test",
                    () -> assertAcked(
                        indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).removeAlias("test", "alias1")
                    )
                );
                assertThat(
                    indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "alias2").get().getAliases().get("test").size(),
                    equalTo(1)
                );
                assertFalse(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "alias2").get().getAliases().isEmpty());
            } finally {
                disableIndexBlock("test", block);
            }
        }

        try {
            enableIndexBlock("test", SETTING_READ_ONLY);

            assertAliasesVersionUnchanged(
                "test",
                () -> assertBlocked(
                    indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test", "alias3"),
                    INDEX_READ_ONLY_BLOCK
                )
            );
            assertAliasesVersionUnchanged(
                "test",
                () -> assertBlocked(
                    indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).removeAlias("test", "alias2"),
                    INDEX_READ_ONLY_BLOCK
                )
            );
            assertThat(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "alias2").get().getAliases().get("test").size(), equalTo(1));
            assertFalse(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "alias2").get().getAliases().isEmpty());

        } finally {
            disableIndexBlock("test", SETTING_READ_ONLY);
        }

        try {
            enableIndexBlock("test", SETTING_BLOCKS_METADATA);

            assertAliasesVersionUnchanged(
                "test",
                () -> assertBlocked(
                    indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test", "alias3"),
                    INDEX_METADATA_BLOCK
                )
            );
            assertAliasesVersionUnchanged(
                "test",
                () -> assertBlocked(
                    indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).removeAlias("test", "alias2"),
                    INDEX_METADATA_BLOCK
                )
            );
            assertBlocked(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "alias2"), INDEX_METADATA_BLOCK);
            assertBlocked(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "alias2"), INDEX_METADATA_BLOCK);

        } finally {
            disableIndexBlock("test", SETTING_BLOCKS_METADATA);
        }
    }

    public void testAliasActionRemoveIndex() {
        assertAcked(prepareCreate("foo_foo"), prepareCreate("bar_bar"));
        assertAliasesVersionIncreases(
            new String[] { "foo_foo", "bar_bar" },
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("bar_bar", "foo"),
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("foo_foo", "foo")
            )
        );

        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).removeIndex("foo")
        );
        assertEquals(
            "The provided expression [foo] matches an alias, specify the corresponding concrete indices instead.",
            iae.getMessage()
        );

        assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).removeIndex("foo*"));
        assertFalse(indexExists("foo_foo"));
        assertFalse(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "foo").get().getAliases().isEmpty());
        assertTrue(indexExists("bar_bar"));
        assertFalse(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "foo").setIndices("bar_bar").get().getAliases().isEmpty());

        assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).removeIndex("bar_bar"));
        assertTrue(indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "foo").get().getAliases().isEmpty());
        assertFalse(indexExists("bar_bar"));
    }

    public void testRemoveIndexAndReplaceWithAlias() throws InterruptedException, ExecutionException {
        assertAcked(indicesAdmin().prepareCreate("test"));
        indexRandom(true, prepareIndex("test_2").setId("test").setSource("test", "test"));
        assertAliasesVersionIncreases(
            "test_2",
            () -> assertAcked(
                indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test_2", "test").removeIndex("test")
            )
        );
        assertHitCount(prepareSearch("test"), 1);
    }

    public void testHiddenAliasesMustBeConsistent() {
        final String index1 = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        final String index2 = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        final String alias = randomAlphaOfLength(7).toLowerCase(Locale.ROOT);
        createIndex(index1, index2);

        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.add().index(index1).alias(alias))
        );

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.add().index(index2).alias(alias).isHidden(true))
        );
        logger.error("exception: {}", ex.getMessage());
        assertThat(ex.getMessage(), containsString("has is_hidden set to true on indices"));

        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.remove().index(index1).alias(alias))
        );
        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.add().index(index1).alias(alias).isHidden(false))
        );
        expectThrows(
            IllegalStateException.class,
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.add().index(index2).alias(alias).isHidden(true))
        );

        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.remove().index(index1).alias(alias))
        );
        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.add().index(index1).alias(alias).isHidden(true))
        );
        expectThrows(
            IllegalStateException.class,
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.add().index(index2).alias(alias).isHidden(false))
        );
        expectThrows(
            IllegalStateException.class,
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.add().index(index2).alias(alias))
        );

        // Both visible
        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.remove().index(index1).alias(alias))
        );
        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.add().index(index1).alias(alias).isHidden(false))
        );
        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.add().index(index2).alias(alias).isHidden(false))
        );

        // Both hidden
        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.remove().index(index1).alias(alias))
                .addAliasAction(AliasActions.remove().index(index2).alias(alias))
        );
        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.add().index(index1).alias(alias).isHidden(true))
        );
        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.add().index(index2).alias(alias).isHidden(true))
        );

        // Visible on one, then update it to hidden & add to a second as hidden simultaneously
        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.remove().index(index1).alias(alias))
                .addAliasAction(AliasActions.remove().index(index2).alias(alias))
        );
        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.add().index(index2).alias(alias).isHidden(false))
        );
        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.add().index(index1).alias(alias).isHidden(true))
                .addAliasAction(AliasActions.add().index(index2).alias(alias).isHidden(true))
        );
    }

    public void testIndexingAndQueryingHiddenAliases() throws Exception {
        final String writeIndex = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        final String nonWriteIndex = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        final String alias = "alias-" + randomAlphaOfLength(7).toLowerCase(Locale.ROOT);
        createIndex(writeIndex, nonWriteIndex);

        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(AliasActions.add().index(writeIndex).alias(alias).isHidden(true).writeIndex(true))
                .addAliasAction(AliasActions.add().index(nonWriteIndex).alias(alias).isHidden(true))
        );

        ensureGreen();

        // Put a couple docs in each index directly
        DocWriteResponse res = client().index(new IndexRequest(nonWriteIndex).id("1").source(source("1", "nonwrite"), XContentType.JSON))
            .get();
        assertThat(res.status().getStatus(), equalTo(201));
        res = client().index(new IndexRequest(writeIndex).id("2").source(source("2", "writeindex"), XContentType.JSON)).get();
        assertThat(res.status().getStatus(), equalTo(201));
        // And through the alias
        res = client().index(new IndexRequest(alias).id("3").source(source("3", "through alias"), XContentType.JSON)).get();
        assertThat(res.status().getStatus(), equalTo(201));

        refresh(writeIndex, nonWriteIndex);

        // Make sure that the doc written to the alias made it
        assertResponse(
            prepareSearch(writeIndex).setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertHits(searchResponse.getHits(), "2", "3")
        );

        assertResponses(
            searchResponse -> assertHits(searchResponse.getHits(), "1", "2", "3"),
            // Ensure that all docs can be gotten through the alias
            prepareSearch(alias).setQuery(QueryBuilders.matchAllQuery()),
            // And querying using a wildcard with indices options set to expand hidden
            prepareSearch("alias*").setQuery(QueryBuilders.matchAllQuery())
                .setIndicesOptions(IndicesOptions.fromOptions(false, false, true, false, true, true, true, false, false))
        );

        // And that querying the alias with a wildcard and no expand options fails
        assertResponse(
            prepareSearch("alias*").setQuery(QueryBuilders.matchAllQuery()),
            searchResponse -> assertThat(searchResponse.getHits().getHits(), emptyArray())
        );
    }

    public void testCreateIndexAndAliasWithSameNameFails() {
        final String indexName = "index-name";
        final IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            indicesAdmin().prepareCreate(indexName).addAlias(new Alias(indexName))
        );
        assertEquals("alias name [" + indexName + "] self-conflicts with index name", iae.getMessage());
    }

    public void testGetAliasAndAliasExistsForHiddenAliases() {
        final String writeIndex = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        final String nonWriteIndex = randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        final String alias = "alias-" + randomAlphaOfLength(7).toLowerCase(Locale.ROOT);
    }

    private void checkAliases() {
        GetAliasesResponse getAliasesResponse = indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "alias1").get();
        assertThat(getAliasesResponse.getAliases().get("test").size(), equalTo(1));
        AliasMetadata aliasMetadata = getAliasesResponse.getAliases().get("test").get(0);
        assertThat(aliasMetadata.alias(), equalTo("alias1"));
        assertThat(aliasMetadata.filter(), nullValue());
        assertThat(aliasMetadata.indexRouting(), nullValue());
        assertThat(aliasMetadata.searchRouting(), nullValue());
        assertThat(aliasMetadata.isHidden(), nullValue());

        getAliasesResponse = indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "alias2").get();
        assertThat(getAliasesResponse.getAliases().get("test").size(), equalTo(1));
        aliasMetadata = getAliasesResponse.getAliases().get("test").get(0);
        assertThat(aliasMetadata.alias(), equalTo("alias2"));
        assertThat(aliasMetadata.filter(), notNullValue());
        assertThat(aliasMetadata.indexRouting(), nullValue());
        assertThat(aliasMetadata.searchRouting(), nullValue());
        assertThat(aliasMetadata.isHidden(), nullValue());

        getAliasesResponse = indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "alias3").get();
        assertThat(getAliasesResponse.getAliases().get("test").size(), equalTo(1));
        aliasMetadata = getAliasesResponse.getAliases().get("test").get(0);
        assertThat(aliasMetadata.alias(), equalTo("alias3"));
        assertThat(aliasMetadata.filter(), nullValue());
        assertThat(aliasMetadata.indexRouting(), equalTo("index"));
        assertThat(aliasMetadata.searchRouting(), equalTo("search"));
        assertThat(aliasMetadata.isHidden(), nullValue());

        getAliasesResponse = indicesAdmin().prepareGetAliases(TEST_REQUEST_TIMEOUT, "alias4").get();
        assertThat(getAliasesResponse.getAliases().get("test").size(), equalTo(1));
        aliasMetadata = getAliasesResponse.getAliases().get("test").get(0);
        assertThat(aliasMetadata.alias(), equalTo("alias4"));
        assertThat(aliasMetadata.filter(), nullValue());
        assertThat(aliasMetadata.indexRouting(), nullValue());
        assertThat(aliasMetadata.searchRouting(), nullValue());
        assertThat(aliasMetadata.isHidden(), equalTo(true));
    }

    private void assertHits(SearchHits hits, String... ids) {
        assertThat(hits.getTotalHits().value(), equalTo((long) ids.length));
        Set<String> hitIds = new HashSet<>();
        for (SearchHit hit : hits.getHits()) {
            hitIds.add(hit.getId());
        }
        assertThat(hitIds, containsInAnyOrder(ids));
    }

    private String source(String id, String nameValue) {
        return "{ \"id\" : \"" + id + "\", \"name\" : \"" + nameValue + "\" }";
    }

    private void assertAliasesVersionIncreases(final String index, final Runnable runnable) {
        assertAliasesVersionIncreases(new String[] { index }, runnable);
    }

    private void assertAliasesVersionIncreases(final String[] indices, final Runnable runnable) {
        final var beforeAliasesVersions = new HashMap<String, Long>(indices.length);
        final var beforeMetadata = admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState().metadata();
        for (final var index : indices) {
            beforeAliasesVersions.put(index, beforeMetadata.getProject().index(index).getAliasesVersion());
        }
        runnable.run();
        final var afterMetadata = admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState().metadata();
        for (final String index : indices) {
            assertThat(afterMetadata.getProject().index(index).getAliasesVersion(), equalTo(1 + beforeAliasesVersions.get(index)));
        }
    }

    private void assertAliasesVersionUnchanged(final String index, final Runnable runnable) {
        final long beforeAliasesVersion = admin().cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .metadata()
            .getProject()
            .index(index)
            .getAliasesVersion();
        runnable.run();
        final long afterAliasesVersion = admin().cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .metadata()
            .getProject()
            .index(index)
            .getAliasesVersion();
        assertThat(afterAliasesVersion, equalTo(beforeAliasesVersion));
    }

}
