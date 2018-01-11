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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class SimpleRoutingIT extends ESIntegTestCase {

    @Override
    protected int minimumNumberOfShards() {
        return 2;
    }

    public void testSimpleCrudRouting() throws Exception {
        createIndex("test");
        ensureGreen();

        logger.info("--> indexing with id [1], and routing [0]");
        client().prepareIndex("test", "type1", "1").setRouting("0").setSource("field", "value1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .get();
        logger.info("--> verifying get with no routing, should not find anything");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(false));
        }
        logger.info("--> verifying get with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "type1", "1").setRouting("0").execute().actionGet().isExists(), equalTo(true));
        }

        logger.info("--> deleting with no routing, should not delete anything");
        client().prepareDelete("test", "type1", "1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(false));
            assertThat(client().prepareGet("test", "type1", "1").setRouting("0").execute().actionGet().isExists(), equalTo(true));
        }

        logger.info("--> deleting with routing, should delete");
        client().prepareDelete("test", "type1", "1").setRouting("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(false));
            assertThat(client().prepareGet("test", "type1", "1").setRouting("0").execute().actionGet().isExists(), equalTo(false));
        }

        logger.info("--> indexing with id [1], and routing [0]");
        client().prepareIndex("test", "type1", "1").setRouting("0").setSource("field", "value1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .get();
        logger.info("--> verifying get with no routing, should not find anything");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(false));
        }
        logger.info("--> verifying get with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "type1", "1").setRouting("0").execute().actionGet().isExists(), equalTo(true));
        }
    }

    public void testSimpleSearchRouting() {
        createIndex("test");
        ensureGreen();

        logger.info("--> indexing with id [1], and routing [0]");
        client().prepareIndex("test", "type1", "1").setRouting("0").setSource("field", "value1").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                .get();
        logger.info("--> verifying get with no routing, should not find anything");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "type1", "1").execute().actionGet().isExists(), equalTo(false));
        }
        logger.info("--> verifying get with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet("test", "type1", "1").setRouting("0").execute().actionGet().isExists(), equalTo(true));
        }

        logger.info("--> search with no routing, should fine one");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().getTotalHits(), equalTo(1L));
        }

        logger.info("--> search with wrong routing, should not find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch().setRouting("1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().getTotalHits(), equalTo(0L));
            assertThat(client().prepareSearch().setSize(0).setRouting("1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().getTotalHits(), equalTo(0L));
        }

        logger.info("--> search with correct routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch().setRouting("0").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().getTotalHits(), equalTo(1L));
            assertThat(client().prepareSearch().setSize(0).setRouting("0").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().getTotalHits(), equalTo(1L));
        }

        logger.info("--> indexing with id [2], and routing [1]");
        client().prepareIndex("test", "type1", "2").setRouting("1").setSource("field", "value1").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();

        logger.info("--> search with no routing, should fine two");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().getTotalHits(), equalTo(2L));
            assertThat(client().prepareSearch().setSize(0).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().getTotalHits(), equalTo(2L));
        }

        logger.info("--> search with 0 routing, should find one");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch().setRouting("0").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().getTotalHits(), equalTo(1L));
            assertThat(client().prepareSearch().setSize(0).setRouting("0").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().getTotalHits(), equalTo(1L));
        }

        logger.info("--> search with 1 routing, should find one");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch().setRouting("1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().getTotalHits(), equalTo(1L));
            assertThat(client().prepareSearch().setSize(0).setRouting("1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().getTotalHits(), equalTo(1L));
        }

        logger.info("--> search with 0,1 routings , should find two");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch().setRouting("0", "1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().getTotalHits(), equalTo(2L));
            assertThat(client().prepareSearch().setSize(0).setRouting("0", "1").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().getTotalHits(), equalTo(2L));
        }

        logger.info("--> search with 0,1,0 routings , should find two");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareSearch().setRouting("0", "1", "0").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().getTotalHits(), equalTo(2L));
            assertThat(client().prepareSearch().setSize(0).setRouting("0", "1", "0").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet().getHits().getTotalHits(), equalTo(2L));
        }
    }

    public void testRequiredRoutingCrudApis() throws Exception {
        client().admin().indices().prepareCreate("test").addAlias(new Alias("alias"))
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("_routing").field("required", true).endObject().endObject().endObject())
                .execute().actionGet();
        ensureGreen();

        logger.info("--> indexing with id [1], and routing [0]");
        client().prepareIndex(indexOrAlias(), "type1", "1").setRouting("0").setSource("field", "value1")
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        logger.info("--> verifying get with no routing, should fail");

        logger.info("--> indexing with id [1], with no routing, should fail");
        try {
            client().prepareIndex(indexOrAlias(), "type1", "1").setSource("field", "value1").get();
            fail("index with missing routing when routing is required should fail");
        } catch (ElasticsearchException e) {
            assertThat(e.unwrapCause(), instanceOf(RoutingMissingException.class));
        }

        logger.info("--> verifying get with routing, should find");
        for (int i = 0; i < 5; i++) {
            assertThat(client().prepareGet(indexOrAlias(), "type1", "1").setRouting("0").execute().actionGet().isExists(), equalTo(true));
        }

        logger.info("--> deleting with no routing, should fail");
        try {
            client().prepareDelete(indexOrAlias(), "type1", "1").get();
            fail("delete with missing routing when routing is required should fail");
        } catch (ElasticsearchException e) {
            assertThat(e.unwrapCause(), instanceOf(RoutingMissingException.class));
        }

        for (int i = 0; i < 5; i++) {
            try {
                client().prepareGet(indexOrAlias(), "type1", "1").execute().actionGet().isExists();
                fail("get with missing routing when routing is required should fail");
            } catch (RoutingMissingException e) {
                assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(e.getMessage(), equalTo("routing is required for [test]/[type1]/[1]"));
            }
            assertThat(client().prepareGet(indexOrAlias(), "type1", "1").setRouting("0").execute().actionGet().isExists(), equalTo(true));
        }

        try {
            client().prepareUpdate(indexOrAlias(), "type1", "1").setDoc(Requests.INDEX_CONTENT_TYPE, "field", "value2").execute().actionGet();
            fail("update with missing routing when routing is required should fail");
        } catch(ElasticsearchException e) {
            assertThat(e.unwrapCause(), instanceOf(RoutingMissingException.class));
        }

        client().prepareUpdate(indexOrAlias(), "type1", "1").setRouting("0").setDoc(Requests.INDEX_CONTENT_TYPE, "field", "value2").get();
        client().admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < 5; i++) {
            try {
                client().prepareGet(indexOrAlias(), "type1", "1").execute().actionGet().isExists();
                fail();
            } catch (RoutingMissingException e) {
                assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(e.getMessage(), equalTo("routing is required for [test]/[type1]/[1]"));
            }
            GetResponse getResponse = client().prepareGet(indexOrAlias(), "type1", "1").setRouting("0").execute().actionGet();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getSourceAsMap().get("field"), equalTo("value2"));
        }

        client().prepareDelete(indexOrAlias(), "type1", "1").setRouting("0").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();

        for (int i = 0; i < 5; i++) {
            try {
                client().prepareGet(indexOrAlias(), "type1", "1").execute().actionGet().isExists();
                fail();
            } catch (RoutingMissingException e) {
                assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(e.getMessage(), equalTo("routing is required for [test]/[type1]/[1]"));
            }
            assertThat(client().prepareGet(indexOrAlias(), "type1", "1").setRouting("0").execute().actionGet().isExists(), equalTo(false));
        }
    }

    public void testRequiredRoutingBulk() throws Exception {
        client().admin().indices().prepareCreate("test")
            .addAlias(new Alias("alias"))
            .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("_routing").field("required", true).endObject()
                .endObject().endObject())
            .execute().actionGet();
        ensureGreen();

        {
            BulkResponse bulkResponse = client().prepareBulk().add(Requests.indexRequest(indexOrAlias()).type("type1").id("1")
                .source(Requests.INDEX_CONTENT_TYPE, "field", "value")).execute().actionGet();
            assertThat(bulkResponse.getItems().length, equalTo(1));
            assertThat(bulkResponse.hasFailures(), equalTo(true));

            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                assertThat(bulkItemResponse.isFailed(), equalTo(true));
                assertThat(bulkItemResponse.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
                assertThat(bulkItemResponse.getFailure().getStatus(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(bulkItemResponse.getFailure().getCause(), instanceOf(RoutingMissingException.class));
                assertThat(bulkItemResponse.getFailureMessage(), containsString("routing is required for [test]/[type1]/[1]"));
            }
        }

        {
            BulkResponse bulkResponse = client().prepareBulk().add(Requests.indexRequest(indexOrAlias()).type("type1").id("1").routing("0")
                .source(Requests.INDEX_CONTENT_TYPE, "field", "value")).execute().actionGet();
            assertThat(bulkResponse.hasFailures(), equalTo(false));
        }

        {
            BulkResponse bulkResponse = client().prepareBulk().add(new UpdateRequest(indexOrAlias(), "type1", "1")
                .doc(Requests.INDEX_CONTENT_TYPE, "field", "value2"))
                .execute().actionGet();
            assertThat(bulkResponse.getItems().length, equalTo(1));
            assertThat(bulkResponse.hasFailures(), equalTo(true));

            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                assertThat(bulkItemResponse.isFailed(), equalTo(true));
                assertThat(bulkItemResponse.getOpType(), equalTo(DocWriteRequest.OpType.UPDATE));
                assertThat(bulkItemResponse.getFailure().getStatus(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(bulkItemResponse.getFailure().getCause(), instanceOf(RoutingMissingException.class));
                assertThat(bulkItemResponse.getFailureMessage(), containsString("routing is required for [test]/[type1]/[1]"));
            }
        }

        {
            BulkResponse bulkResponse = client().prepareBulk().add(new UpdateRequest(indexOrAlias(), "type1", "1")
                .doc(Requests.INDEX_CONTENT_TYPE, "field", "value2")
                .routing("0")).execute().actionGet();
            assertThat(bulkResponse.hasFailures(), equalTo(false));
        }

        {
            BulkResponse bulkResponse = client().prepareBulk().add(Requests.deleteRequest(indexOrAlias()).type("type1").id("1"))
                .execute().actionGet();
            assertThat(bulkResponse.getItems().length, equalTo(1));
            assertThat(bulkResponse.hasFailures(), equalTo(true));

            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                assertThat(bulkItemResponse.isFailed(), equalTo(true));
                assertThat(bulkItemResponse.getOpType(), equalTo(DocWriteRequest.OpType.DELETE));
                assertThat(bulkItemResponse.getFailure().getStatus(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(bulkItemResponse.getFailure().getCause(), instanceOf(RoutingMissingException.class));
                assertThat(bulkItemResponse.getFailureMessage(), containsString("routing is required for [test]/[type1]/[1]"));
            }
        }

        {
            BulkResponse bulkResponse = client().prepareBulk().add(Requests.deleteRequest(indexOrAlias()).type("type1").id("1")
                .routing("0")).execute().actionGet();
            assertThat(bulkResponse.getItems().length, equalTo(1));
            assertThat(bulkResponse.hasFailures(), equalTo(false));
        }
    }

    public void testRequiredRoutingMappingVariousAPIs() throws Exception {
        client().admin().indices().prepareCreate("test").addAlias(new Alias("alias"))
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("_routing").field("required", true).endObject().endObject().endObject())
                .execute().actionGet();
        ensureGreen();

        logger.info("--> indexing with id [1], and routing [0]");
        client().prepareIndex(indexOrAlias(), "type1", "1").setRouting("0").setSource("field", "value1").get();
        logger.info("--> indexing with id [2], and routing [0]");
        client().prepareIndex(indexOrAlias(), "type1", "2").setRouting("0").setSource("field", "value2")
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();

        logger.info("--> verifying get with id [1] with routing [0], should succeed");
        assertThat(client().prepareGet(indexOrAlias(), "type1", "1").setRouting("0").execute().actionGet().isExists(), equalTo(true));

        logger.info("--> verifying get with id [1], with no routing, should fail");
        try {
            client().prepareGet(indexOrAlias(), "type1", "1").get();
            fail();
        } catch (RoutingMissingException e) {
            assertThat(e.getMessage(), equalTo("routing is required for [test]/[type1]/[1]"));
        }

        logger.info("--> verifying explain with id [2], with routing [0], should succeed");
        ExplainResponse explainResponse = client().prepareExplain(indexOrAlias(), "type1", "2")
                .setQuery(QueryBuilders.matchAllQuery())
                .setRouting("0").get();
        assertThat(explainResponse.isExists(), equalTo(true));
        assertThat(explainResponse.isMatch(), equalTo(true));

        logger.info("--> verifying explain with id [2], with no routing, should fail");
        try {
            client().prepareExplain(indexOrAlias(), "type1", "2")
                    .setQuery(QueryBuilders.matchAllQuery()).get();
            fail();
        } catch (RoutingMissingException e) {
            assertThat(e.getMessage(), equalTo("routing is required for [test]/[type1]/[2]"));
        }

        logger.info("--> verifying term vector with id [1], with routing [0], should succeed");
        TermVectorsResponse termVectorsResponse = client().prepareTermVectors(indexOrAlias(), "type1", "1").setRouting("0").get();
        assertThat(termVectorsResponse.isExists(), equalTo(true));
        assertThat(termVectorsResponse.getId(), equalTo("1"));

        try {
            client().prepareTermVectors(indexOrAlias(), "type1", "1").get();
            fail();
        } catch (RoutingMissingException e) {
            assertThat(e.getMessage(), equalTo("routing is required for [test]/[type1]/[1]"));
        }

        UpdateResponse updateResponse = client().prepareUpdate(indexOrAlias(), "type1", "1").setRouting("0")
                .setDoc(Requests.INDEX_CONTENT_TYPE, "field1", "value1").get();
        assertThat(updateResponse.getId(), equalTo("1"));
        assertThat(updateResponse.getVersion(), equalTo(2L));

        try {
            client().prepareUpdate(indexOrAlias(), "type1", "1").setDoc(Requests.INDEX_CONTENT_TYPE, "field1", "value1").get();
            fail();
        } catch (RoutingMissingException e) {
            assertThat(e.getMessage(), equalTo("routing is required for [test]/[type1]/[1]"));
        }

        logger.info("--> verifying mget with ids [1,2], with routing [0], should succeed");
        MultiGetResponse multiGetResponse = client().prepareMultiGet()
                .add(new MultiGetRequest.Item(indexOrAlias(), "type1", "1").routing("0"))
                .add(new MultiGetRequest.Item(indexOrAlias(), "type1", "2").routing("0")).get();
        assertThat(multiGetResponse.getResponses().length, equalTo(2));
        assertThat(multiGetResponse.getResponses()[0].isFailed(), equalTo(false));
        assertThat(multiGetResponse.getResponses()[0].getResponse().getId(), equalTo("1"));
        assertThat(multiGetResponse.getResponses()[1].isFailed(), equalTo(false));
        assertThat(multiGetResponse.getResponses()[1].getResponse().getId(), equalTo("2"));

        logger.info("--> verifying mget with ids [1,2], with no routing, should fail");
        multiGetResponse = client().prepareMultiGet()
                .add(new MultiGetRequest.Item(indexOrAlias(), "type1", "1"))
                .add(new MultiGetRequest.Item(indexOrAlias(), "type1", "2")).get();
        assertThat(multiGetResponse.getResponses().length, equalTo(2));
        assertThat(multiGetResponse.getResponses()[0].isFailed(), equalTo(true));
        assertThat(multiGetResponse.getResponses()[0].getFailure().getId(), equalTo("1"));
        assertThat(multiGetResponse.getResponses()[0].getFailure().getMessage(), equalTo("routing is required for [test]/[type1]/[1]"));
        assertThat(multiGetResponse.getResponses()[1].isFailed(), equalTo(true));
        assertThat(multiGetResponse.getResponses()[1].getFailure().getId(), equalTo("2"));
        assertThat(multiGetResponse.getResponses()[1].getFailure().getMessage(), equalTo("routing is required for [test]/[type1]/[2]"));

        MultiTermVectorsResponse multiTermVectorsResponse = client().prepareMultiTermVectors()
                .add(new TermVectorsRequest(indexOrAlias(), "type1", "1").routing("0"))
                .add(new TermVectorsRequest(indexOrAlias(), "type1", "2").routing("0")).get();
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
                .add(new TermVectorsRequest(indexOrAlias(), "type1", "1"))
                .add(new TermVectorsRequest(indexOrAlias(), "type1", "2")).get();
        assertThat(multiTermVectorsResponse.getResponses().length, equalTo(2));
        assertThat(multiTermVectorsResponse.getResponses()[0].getId(), equalTo("1"));
        assertThat(multiTermVectorsResponse.getResponses()[0].isFailed(), equalTo(true));
        assertThat(multiTermVectorsResponse.getResponses()[0].getFailure().getCause().getMessage(), equalTo("routing is required for [test]/[type1]/[1]"));
        assertThat(multiTermVectorsResponse.getResponses()[0].getResponse(), nullValue());
        assertThat(multiTermVectorsResponse.getResponses()[1].getId(), equalTo("2"));
        assertThat(multiTermVectorsResponse.getResponses()[1].isFailed(), equalTo(true));
        assertThat(multiTermVectorsResponse.getResponses()[1].getResponse(),nullValue());
        assertThat(multiTermVectorsResponse.getResponses()[1].getFailure().getCause().getMessage(), equalTo("routing is required for [test]/[type1]/[2]"));
    }

    private static String indexOrAlias() {
        return randomBoolean() ? "test" : "alias";
    }
}
