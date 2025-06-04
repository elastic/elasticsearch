/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.routing;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentFactory;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class SimpleRoutingIT extends ESIntegTestCase {

    @Override
    protected int minimumNumberOfShards() {
        return 2;
    }

    public String findNonMatchingRoutingValue(String index, String id) {
        ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).all().get().getState();
        IndexMetadata metadata = state.metadata().getProject().index(index);
        IndexMetadata withoutRoutingRequired = IndexMetadata.builder(metadata).putMapping("{}").build();
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(withoutRoutingRequired);
        int routing = -1;
        int idShard;
        int routingShard;
        do {
            idShard = indexRouting.getShard(id, null);
            routingShard = indexRouting.getShard(id, Integer.toString(++routing));
        } while (idShard == routingShard);

        return Integer.toString(routing);
    }

    public void testSimpleCrudRouting() throws Exception {
        createIndex("test");
        ensureGreen();
        String routingValue = findNonMatchingRoutingValue("test", "1");
        logger.info("--> indexing with id [1], and routing [{}]", routingValue);
        prepareIndex("test").setId("1")
            .setRouting(routingValue)
            .setSource("field", "value1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();
        logger.info("--> verifying get with no routing, should not find anything");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "1").get().isExists(), equalTo(false));
        }
        logger.info("--> verifying get with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "1").setRouting(routingValue).get().isExists(), equalTo(true));
        }

        logger.info("--> deleting with no routing, should not delete anything");
        client().prepareDelete("test", "1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "1").get().isExists(), equalTo(false));
            assertThat(client().prepareGet("test", "1").setRouting(routingValue).get().isExists(), equalTo(true));
        }

        logger.info("--> deleting with routing, should delete");
        client().prepareDelete("test", "1").setRouting(routingValue).setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "1").get().isExists(), equalTo(false));
            assertThat(client().prepareGet("test", "1").setRouting(routingValue).get().isExists(), equalTo(false));
        }

        logger.info("--> indexing with id [1], and routing [0]");
        prepareIndex("test").setId("1")
            .setRouting(routingValue)
            .setSource("field", "value1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();
        logger.info("--> verifying get with no routing, should not find anything");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "1").get().isExists(), equalTo(false));
        }
        logger.info("--> verifying get with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "1").setRouting(routingValue).get().isExists(), equalTo(true));
        }
    }

    public void testSimpleSearchRouting() {
        createIndex("test");
        ensureGreen();
        String routingValue = findNonMatchingRoutingValue("test", "1");

        logger.info("--> indexing with id [1], and routing [{}]", routingValue);
        prepareIndex("test").setId("1")
            .setRouting(routingValue)
            .setSource("field", "value1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();
        logger.info("--> verifying get with no routing, should not find anything");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "1").get().isExists(), equalTo(false));
        }
        logger.info("--> verifying get with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "1").setRouting(routingValue).get().isExists(), equalTo(true));
        }

        logger.info("--> search with no routing, should fine one");
        for (int i = 0; i < 5; i++) {
            assertHitCount(prepareSearch().setQuery(QueryBuilders.matchAllQuery()), 1L);
        }

        logger.info("--> search with wrong routing, should not find");
        for (int i = 0; i < 5; i++) {
            assertHitCount(
                0,
                prepareSearch().setRouting("1").setQuery(QueryBuilders.matchAllQuery()),
                prepareSearch().setSize(0).setRouting("1").setQuery(QueryBuilders.matchAllQuery())
            );
        }

        logger.info("--> search with correct routing, should find");
        for (int i = 0; i < 5; i++) {
            assertHitCount(
                1,
                prepareSearch().setRouting(routingValue).setQuery(QueryBuilders.matchAllQuery()),
                prepareSearch().setSize(0).setRouting(routingValue).setQuery(QueryBuilders.matchAllQuery())
            );
        }

        String secondRoutingValue = "1";
        logger.info("--> indexing with id [{}], and routing [{}]", routingValue, secondRoutingValue);
        prepareIndex("test").setId(routingValue)
            .setRouting(secondRoutingValue)
            .setSource("field", "value1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();

        logger.info("--> search with no routing, should fine two");
        for (int i = 0; i < 5; i++) {
            assertHitCount(prepareSearch().setQuery(QueryBuilders.matchAllQuery()), 2);
            assertHitCount(prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()), 2);
        }

        logger.info("--> search with {} routing, should find one", routingValue);
        for (int i = 0; i < 5; i++) {
            assertHitCount(prepareSearch().setRouting(routingValue).setQuery(QueryBuilders.matchAllQuery()), 1);
            assertHitCount(prepareSearch().setSize(0).setRouting(routingValue).setQuery(QueryBuilders.matchAllQuery()), 1);
        }

        logger.info("--> search with {} routing, should find one", secondRoutingValue);
        for (int i = 0; i < 5; i++) {
            assertHitCount(prepareSearch().setRouting("1").setQuery(QueryBuilders.matchAllQuery()), 1);
            assertHitCount(prepareSearch().setSize(0).setRouting(secondRoutingValue).setQuery(QueryBuilders.matchAllQuery()), 1);
        }

        logger.info("--> search with {},{} indexRoutings , should find two", routingValue, "1");
        for (int i = 0; i < 5; i++) {
            assertHitCount(prepareSearch().setRouting(routingValue, secondRoutingValue).setQuery(QueryBuilders.matchAllQuery()), 2);
            assertHitCount(
                prepareSearch().setSize(0).setRouting(routingValue, secondRoutingValue).setQuery(QueryBuilders.matchAllQuery()),
                2
            );
        }

        logger.info("--> search with {},{},{} indexRoutings , should find two", routingValue, secondRoutingValue, routingValue);
        for (int i = 0; i < 5; i++) {
            assertHitCount(
                prepareSearch().setRouting(routingValue, secondRoutingValue, routingValue).setQuery(QueryBuilders.matchAllQuery()),
                2
            );
            assertHitCount(
                prepareSearch().setSize(0)
                    .setRouting(routingValue, secondRoutingValue, routingValue)
                    .setQuery(QueryBuilders.matchAllQuery()),
                2
            );
        }
    }

    public void testRequiredRoutingCrudApis() throws Exception {
        indicesAdmin().prepareCreate("test")
            .addAlias(new Alias("alias"))
            .setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("_routing")
                    .field("required", true)
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();
        ensureGreen();
        String routingValue = findNonMatchingRoutingValue("test", "1");

        logger.info("--> indexing with id [1], and routing [{}]", routingValue);
        prepareIndex(indexOrAlias()).setId("1")
            .setRouting(routingValue)
            .setSource("field", "value1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();
        logger.info("--> verifying get with no routing, should fail");

        logger.info("--> indexing with id [1], with no routing, should fail");
        try {
            prepareIndex(indexOrAlias()).setId("1").setSource("field", "value1").get();
            fail("index with missing routing when routing is required should fail");
        } catch (ElasticsearchException e) {
            assertThat(e.unwrapCause(), instanceOf(RoutingMissingException.class));
        }

        logger.info("--> verifying get with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet(indexOrAlias(), "1").setRouting(routingValue).get().isExists(), equalTo(true));
        }

        logger.info("--> deleting with no routing, should fail");
        try {
            client().prepareDelete(indexOrAlias(), "1").get();
            fail("delete with missing routing when routing is required should fail");
        } catch (ElasticsearchException e) {
            assertThat(e.unwrapCause(), instanceOf(RoutingMissingException.class));
        }

        for (int i = 0; i < 5; i++) {
            try {
                client().prepareGet(indexOrAlias(), "1").get().isExists();
                fail("get with missing routing when routing is required should fail");
            } catch (RoutingMissingException e) {
                assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(e.getMessage(), equalTo("routing is required for [test]/[1]"));
            }
            assertThat(client().prepareGet(indexOrAlias(), "1").setRouting(routingValue).get().isExists(), equalTo(true));
        }

        try {
            client().prepareUpdate(indexOrAlias(), "1").setDoc(Requests.INDEX_CONTENT_TYPE, "field", "value2").get();
            fail("update with missing routing when routing is required should fail");
        } catch (ElasticsearchException e) {
            assertThat(e.unwrapCause(), instanceOf(RoutingMissingException.class));
        }

        client().prepareUpdate(indexOrAlias(), "1").setRouting(routingValue).setDoc(Requests.INDEX_CONTENT_TYPE, "field", "value2").get();
        indicesAdmin().prepareRefresh().get();

        for (int i = 0; i < 5; i++) {
            try {
                client().prepareGet(indexOrAlias(), "1").get().isExists();
                fail();
            } catch (RoutingMissingException e) {
                assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(e.getMessage(), equalTo("routing is required for [test]/[1]"));
            }
            GetResponse getResponse = client().prepareGet(indexOrAlias(), "1").setRouting(routingValue).get();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getSourceAsMap().get("field"), equalTo("value2"));
        }

        client().prepareDelete(indexOrAlias(), "1").setRouting(routingValue).setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();

        for (int i = 0; i < 5; i++) {
            try {
                client().prepareGet(indexOrAlias(), "1").get().isExists();
                fail();
            } catch (RoutingMissingException e) {
                assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(e.getMessage(), equalTo("routing is required for [test]/[1]"));
            }
            assertThat(client().prepareGet(indexOrAlias(), "1").setRouting(routingValue).get().isExists(), equalTo(false));
        }
    }

    public void testRequiredRoutingBulk() throws Exception {
        indicesAdmin().prepareCreate("test")
            .addAlias(new Alias("alias"))
            .setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("_routing")
                    .field("required", true)
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();
        ensureGreen();
        {
            String index = indexOrAlias();
            BulkResponse bulkResponse = client().prepareBulk()
                .add(new IndexRequest(index).id("1").source(Requests.INDEX_CONTENT_TYPE, "field", "value"))
                .get();
            assertThat(bulkResponse.getItems().length, equalTo(1));
            assertThat(bulkResponse.hasFailures(), equalTo(true));

            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                assertThat(bulkItemResponse.isFailed(), equalTo(true));
                assertThat(bulkItemResponse.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
                assertThat(bulkItemResponse.getFailure().getStatus(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(bulkItemResponse.getFailure().getCause(), instanceOf(RoutingMissingException.class));
                assertThat(bulkItemResponse.getFailureMessage(), containsString("routing is required for [test]/[1]"));
            }
        }

        {
            String index = indexOrAlias();
            BulkResponse bulkResponse = client().prepareBulk()
                .add(new IndexRequest(index).id("1").routing("0").source(Requests.INDEX_CONTENT_TYPE, "field", "value"))
                .get();
            assertThat(bulkResponse.hasFailures(), equalTo(false));
        }

        {
            BulkResponse bulkResponse = client().prepareBulk()
                .add(new UpdateRequest(indexOrAlias(), "1").doc(Requests.INDEX_CONTENT_TYPE, "field", "value2"))
                .get();
            assertThat(bulkResponse.getItems().length, equalTo(1));
            assertThat(bulkResponse.hasFailures(), equalTo(true));

            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                assertThat(bulkItemResponse.isFailed(), equalTo(true));
                assertThat(bulkItemResponse.getOpType(), equalTo(DocWriteRequest.OpType.UPDATE));
                assertThat(bulkItemResponse.getFailure().getStatus(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(bulkItemResponse.getFailure().getCause(), instanceOf(RoutingMissingException.class));
                assertThat(bulkItemResponse.getFailureMessage(), containsString("routing is required for [test]/[1]"));
            }
        }

        {
            BulkResponse bulkResponse = client().prepareBulk()
                .add(new UpdateRequest(indexOrAlias(), "1").doc(Requests.INDEX_CONTENT_TYPE, "field", "value2").routing("0"))
                .get();
            assertThat(bulkResponse.hasFailures(), equalTo(false));
        }

        {
            String index = indexOrAlias();
            BulkResponse bulkResponse = client().prepareBulk().add(new DeleteRequest(index).id("1")).get();
            assertThat(bulkResponse.getItems().length, equalTo(1));
            assertThat(bulkResponse.hasFailures(), equalTo(true));

            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                assertThat(bulkItemResponse.isFailed(), equalTo(true));
                assertThat(bulkItemResponse.getOpType(), equalTo(DocWriteRequest.OpType.DELETE));
                assertThat(bulkItemResponse.getFailure().getStatus(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(bulkItemResponse.getFailure().getCause(), instanceOf(RoutingMissingException.class));
                assertThat(bulkItemResponse.getFailureMessage(), containsString("routing is required for [test]/[1]"));
            }
        }

        {
            String index = indexOrAlias();
            BulkResponse bulkResponse = client().prepareBulk().add(new DeleteRequest(index).id("1").routing("0")).get();
            assertThat(bulkResponse.getItems().length, equalTo(1));
            assertThat(bulkResponse.hasFailures(), equalTo(false));
        }
    }

    public void testRequiredRoutingMappingVariousAPIs() throws Exception {

        indicesAdmin().prepareCreate("test")
            .addAlias(new Alias("alias"))
            .setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("_routing")
                    .field("required", true)
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();
        ensureGreen();
        String routingValue = findNonMatchingRoutingValue("test", "1");
        logger.info("--> indexing with id [1], and routing [{}]", routingValue);
        prepareIndex(indexOrAlias()).setId("1").setRouting(routingValue).setSource("field", "value1").get();
        logger.info("--> indexing with id [2], and routing [{}]", routingValue);
        prepareIndex(indexOrAlias()).setId("2")
            .setRouting(routingValue)
            .setSource("field", "value2")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();

        logger.info("--> verifying get with id [1] with routing [0], should succeed");
        assertThat(client().prepareGet(indexOrAlias(), "1").setRouting(routingValue).get().isExists(), equalTo(true));

        logger.info("--> verifying get with id [1], with no routing, should fail");
        try {
            client().prepareGet(indexOrAlias(), "1").get();
            fail();
        } catch (RoutingMissingException e) {
            assertThat(e.getMessage(), equalTo("routing is required for [test]/[1]"));
        }

        logger.info("--> verifying explain with id [2], with routing [0], should succeed");
        ExplainResponse explainResponse = client().prepareExplain(indexOrAlias(), "2")
            .setQuery(QueryBuilders.matchAllQuery())
            .setRouting(routingValue)
            .get();
        assertThat(explainResponse.isExists(), equalTo(true));
        assertThat(explainResponse.isMatch(), equalTo(true));

        logger.info("--> verifying explain with id [2], with no routing, should fail");
        try {
            client().prepareExplain(indexOrAlias(), "2").setQuery(QueryBuilders.matchAllQuery()).get();
            fail();
        } catch (RoutingMissingException e) {
            assertThat(e.getMessage(), equalTo("routing is required for [test]/[2]"));
        }

        logger.info("--> verifying term vector with id [1], with routing [0], should succeed");
        TermVectorsResponse termVectorsResponse = client().prepareTermVectors(indexOrAlias(), "1").setRouting(routingValue).get();
        assertThat(termVectorsResponse.isExists(), equalTo(true));
        assertThat(termVectorsResponse.getId(), equalTo("1"));

        try {
            client().prepareTermVectors(indexOrAlias(), "1").get();
            fail();
        } catch (RoutingMissingException e) {
            assertThat(e.getMessage(), equalTo("routing is required for [test]/[1]"));
        }

        UpdateResponse updateResponse = client().prepareUpdate(indexOrAlias(), "1")
            .setRouting(routingValue)
            .setDoc(Requests.INDEX_CONTENT_TYPE, "field1", "value1")
            .get();
        assertThat(updateResponse.getId(), equalTo("1"));
        assertThat(updateResponse.getVersion(), equalTo(2L));

        try {
            client().prepareUpdate(indexOrAlias(), "1").setDoc(Requests.INDEX_CONTENT_TYPE, "field1", "value1").get();
            fail();
        } catch (RoutingMissingException e) {
            assertThat(e.getMessage(), equalTo("routing is required for [test]/[1]"));
        }

        logger.info("--> verifying mget with ids [1,2], with routing [0], should succeed");
        MultiGetResponse multiGetResponse = client().prepareMultiGet()
            .add(new MultiGetRequest.Item(indexOrAlias(), "1").routing("0"))
            .add(new MultiGetRequest.Item(indexOrAlias(), "2").routing("0"))
            .get();
        assertThat(multiGetResponse.getResponses().length, equalTo(2));
        assertThat(multiGetResponse.getResponses()[0].isFailed(), equalTo(false));
        assertThat(multiGetResponse.getResponses()[0].getResponse().getId(), equalTo("1"));
        assertThat(multiGetResponse.getResponses()[1].isFailed(), equalTo(false));
        assertThat(multiGetResponse.getResponses()[1].getResponse().getId(), equalTo("2"));

        logger.info("--> verifying mget with ids [1,2], with no routing, should fail");
        multiGetResponse = client().prepareMultiGet()
            .add(new MultiGetRequest.Item(indexOrAlias(), "1"))
            .add(new MultiGetRequest.Item(indexOrAlias(), "2"))
            .get();
        assertThat(multiGetResponse.getResponses().length, equalTo(2));
        assertThat(multiGetResponse.getResponses()[0].isFailed(), equalTo(true));
        assertThat(multiGetResponse.getResponses()[0].getFailure().getId(), equalTo("1"));
        assertThat(multiGetResponse.getResponses()[0].getFailure().getMessage(), equalTo("routing is required for [test]/[1]"));
        assertThat(multiGetResponse.getResponses()[1].isFailed(), equalTo(true));
        assertThat(multiGetResponse.getResponses()[1].getFailure().getId(), equalTo("2"));
        assertThat(multiGetResponse.getResponses()[1].getFailure().getMessage(), equalTo("routing is required for [test]/[2]"));

        MultiTermVectorsResponse multiTermVectorsResponse = client().prepareMultiTermVectors()
            .add(new TermVectorsRequest(indexOrAlias(), "1").routing(routingValue))
            .add(new TermVectorsRequest(indexOrAlias(), "2").routing(routingValue))
            .get();
        assertThat(multiTermVectorsResponse.getResponses().length, equalTo(2));
        assertThat(multiTermVectorsResponse.getResponses()[0].getId(), equalTo("1"));
        assertThat(multiTermVectorsResponse.getResponses()[0].isFailed(), equalTo(false));
        assertThat(multiTermVectorsResponse.getResponses()[0].getResponse().getId(), equalTo("1"));
        assertThat(multiTermVectorsResponse.getResponses()[0].getResponse().isExists(), equalTo(true));
        assertThat(multiTermVectorsResponse.getResponses()[1].getId(), equalTo("2"));
        assertThat(multiTermVectorsResponse.getResponses()[1].isFailed(), equalTo(false));
        assertThat(multiTermVectorsResponse.getResponses()[1].getResponse().getId(), equalTo("2"));
        assertThat(multiTermVectorsResponse.getResponses()[1].getResponse().isExists(), equalTo(true));

        multiTermVectorsResponse = client().prepareMultiTermVectors()
            .add(new TermVectorsRequest(indexOrAlias(), "1"))
            .add(new TermVectorsRequest(indexOrAlias(), "2"))
            .get();
        assertThat(multiTermVectorsResponse.getResponses().length, equalTo(2));
        assertThat(multiTermVectorsResponse.getResponses()[0].getId(), equalTo("1"));
        assertThat(multiTermVectorsResponse.getResponses()[0].isFailed(), equalTo(true));
        assertThat(
            multiTermVectorsResponse.getResponses()[0].getFailure().getCause().getMessage(),
            equalTo("routing is required for [test]/[1]")
        );
        assertThat(multiTermVectorsResponse.getResponses()[0].getResponse(), nullValue());
        assertThat(multiTermVectorsResponse.getResponses()[1].getId(), equalTo("2"));
        assertThat(multiTermVectorsResponse.getResponses()[1].isFailed(), equalTo(true));
        assertThat(multiTermVectorsResponse.getResponses()[1].getResponse(), nullValue());
        assertThat(
            multiTermVectorsResponse.getResponses()[1].getFailure().getCause().getMessage(),
            equalTo("routing is required for [test]/[2]")
        );
    }

    private static String indexOrAlias() {
        return randomBoolean() ? "test" : "alias";
    }
}
