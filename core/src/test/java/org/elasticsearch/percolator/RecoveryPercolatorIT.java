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

package org.elasticsearch.percolator;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.percolate.MultiPercolateRequestBuilder;
import org.elasticsearch.action.percolate.MultiPercolateResponse;
import org.elasticsearch.action.percolate.PercolateRequestBuilder;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.elasticsearch.action.percolate.PercolateSourceBuilder.docBuilder;
import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.percolator.PercolatorIT.convertFromTextArray;
import static org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import static org.elasticsearch.test.ESIntegTestCase.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertMatchCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0)
public class RecoveryPercolatorIT extends ESIntegTestCase {

    @Override
    protected int numberOfShards() {
        return 1;
    }

    @Test
    public void testRestartNodePercolator1() throws Exception {
        internalCluster().startNode();
        assertAcked(prepareCreate("test").addMapping("type1", "field1", "type=string").addMapping(PercolatorService.TYPE_NAME, "color", "type=string"));

        logger.info("--> register a query");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .get();

        PercolateResponse percolate = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc")
                        .field("field1", "value1")
                        .endObject().endObject())
                .get();
        assertThat(percolate.getMatches(), arrayWithSize(1));

        internalCluster().rollingRestart();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ensureYellow();

        percolate = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc")
                        .field("field1", "value1")
                        .endObject().endObject())
                .get();
        assertMatchCount(percolate, 1l);
        assertThat(percolate.getMatches(), arrayWithSize(1));
    }

    @Test
    public void testRestartNodePercolator2() throws Exception {
        internalCluster().startNode();
        assertAcked(prepareCreate("test").addMapping("type1", "field1", "type=string").addMapping(PercolatorService.TYPE_NAME, "color", "type=string"));

        logger.info("--> register a query");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .get();

        assertThat(client().prepareCount().setTypes(PercolatorService.TYPE_NAME).setQuery(matchAllQuery()).get().getCount(), equalTo(1l));

        PercolateResponse percolate = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc")
                        .field("field1", "value1")
                        .endObject().endObject())
                .get();
        assertMatchCount(percolate, 1l);
        assertThat(percolate.getMatches(), arrayWithSize(1));

        internalCluster().rollingRestart();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        CountResponse countResponse = client().prepareCount().setTypes(PercolatorService.TYPE_NAME).setQuery(matchAllQuery()).get();
        assertHitCount(countResponse, 1l);

        DeleteIndexResponse actionGet = client().admin().indices().prepareDelete("test").get();
        assertThat(actionGet.isAcknowledged(), equalTo(true));
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).get();
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(client().prepareCount().setTypes(PercolatorService.TYPE_NAME).setQuery(matchAllQuery()).get().getCount(), equalTo(0l));

        percolate = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc")
                        .field("field1", "value1")
                        .endObject().endObject())
                .get();
        assertMatchCount(percolate, 0l);
        assertThat(percolate.getMatches(), emptyArray());

        logger.info("--> register a query");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .get();

        assertThat(client().prepareCount().setTypes(PercolatorService.TYPE_NAME).setQuery(matchAllQuery()).get().getCount(), equalTo(1l));

        percolate = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc")
                        .field("field1", "value1")
                        .endObject().endObject())
                .get();
        assertMatchCount(percolate, 1l);
        assertThat(percolate.getMatches(), arrayWithSize(1));
    }

    @Test
    public void testLoadingPercolateQueriesDuringCloseAndOpen() throws Exception {
        internalCluster().startNode();
        internalCluster().startNode();

        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2)
                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)));
        ensureGreen();

        logger.info("--> Add dummy docs");
        client().prepareIndex("test", "type1", "1").setSource("field1", 0).get();
        client().prepareIndex("test", "type2", "1").setSource("field1", 1).get();

        logger.info("--> register a queries");
        for (int i = 1; i <= 100; i++) {
            client().prepareIndex("test", PercolatorService.TYPE_NAME, Integer.toString(i))
                    .setSource(jsonBuilder().startObject()
                            .field("query", rangeQuery("field1").from(0).to(i))
                            .endObject())
                    .get();
        }

        logger.info("--> Percolate doc with field1=95");
        PercolateResponse response = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", 95).endObject().endObject())
                .get();
        assertMatchCount(response, 6l);
        assertThat(response.getMatches(), arrayWithSize(6));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("95", "96", "97", "98", "99", "100"));

        logger.info("--> Close and open index to trigger percolate queries loading...");
        assertAcked(client().admin().indices().prepareClose("test"));
        assertAcked(client().admin().indices().prepareOpen("test"));
        ensureGreen();

        logger.info("--> Percolate doc with field1=100");
        response = client().preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", 100).endObject().endObject()).get();

        assertMatchCount(response, 1l);
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(response.getMatches()[0].getId().string(), equalTo("100"));
    }

    @Test
    public void testSinglePercolator_recovery() throws Exception {
        percolatorRecovery(false);
    }

    @Test
    public void testMultiPercolator_recovery() throws Exception {
        percolatorRecovery(true);
    }

    // 3 nodes, 2 primary + 2 replicas per primary, so each node should have a copy of the data.
    // We only start and stop nodes 2 and 3, so all requests should succeed and never be partial.
    private void percolatorRecovery(final boolean multiPercolate) throws Exception {
        internalCluster().startNode(settingsBuilder().put("node.stay", true));
        internalCluster().startNode(settingsBuilder().put("node.stay", false));
        internalCluster().startNode(settingsBuilder().put("node.stay", false));
        ensureGreen();
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 2)
                )
                .get();
        ensureGreen();

        final Client client = internalCluster().client(input -> input.getAsBoolean("node.stay", true));
        final int numQueries = randomIntBetween(50, 100);
        logger.info("--> register a queries");
        for (int i = 0; i < numQueries; i++) {
            client.prepareIndex("test", PercolatorService.TYPE_NAME, Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                    .get();
        }

        final String document = "{\"field\" : \"a\"}";
        client.prepareIndex("test", "type", "1")
                .setSource(document)
                .get();

        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        Runnable r = new Runnable() {
            @Override
            public void run() {
                try {
                    while (run.get()) {
                        if (multiPercolate) {
                            MultiPercolateRequestBuilder builder = client
                                    .prepareMultiPercolate();
                            int numPercolateRequest = randomIntBetween(50, 100);

                            for (int i = 0; i < numPercolateRequest; i++) {
                                PercolateRequestBuilder percolateBuilder = client.preparePercolate()
                                        .setIndices("test").setDocumentType("type");
                                if (randomBoolean()) {
                                    percolateBuilder.setGetRequest(Requests.getRequest("test").type("type").id("1"));
                                } else {
                                    percolateBuilder.setPercolateDoc(docBuilder().setDoc(document));
                                }
                                builder.add(percolateBuilder);
                            }

                            MultiPercolateResponse response = builder.get();
                            assertThat(response.items().length, equalTo(numPercolateRequest));
                            for (MultiPercolateResponse.Item item : response) {
                                assertThat(item.isFailure(), equalTo(false));
                                assertNoFailures(item.getResponse());
                                assertThat(item.getResponse().getSuccessfulShards(), equalTo(item.getResponse().getTotalShards()));
                                assertThat(item.getResponse().getCount(), equalTo((long) numQueries));
                                assertThat(item.getResponse().getMatches().length, equalTo(numQueries));
                            }
                        } else {
                            PercolateRequestBuilder percolateBuilder = client.preparePercolate()
                                    .setIndices("test").setDocumentType("type");
                            if (randomBoolean()) {
                                percolateBuilder.setPercolateDoc(docBuilder().setDoc(document));
                            } else {
                                percolateBuilder.setGetRequest(Requests.getRequest("test").type("type").id("1"));
                            }
                            PercolateResponse response = percolateBuilder.get();
                            assertNoFailures(response);
                            assertThat(response.getSuccessfulShards(), equalTo(response.getTotalShards()));
                            assertThat(response.getCount(), equalTo((long) numQueries));
                            assertThat(response.getMatches().length, equalTo(numQueries));
                        }
                    }
                } catch (Throwable t) {
                    logger.info("Error in percolate thread...", t);
                    run.set(false);
                    error.set(t);
                }
            }
        };
        Thread t = new Thread(r);
        t.start();
        Predicate<Settings> nodePredicate = input -> !input.getAsBoolean("node.stay", false);
        try {
            // 1 index, 2 primaries, 2 replicas per primary
            for (int i = 0; i < 4; i++) {
                internalCluster().stopRandomNode(nodePredicate);
                client.admin().cluster().prepareHealth("test")
                        .setWaitForEvents(Priority.LANGUID)
                        .setTimeout(TimeValue.timeValueMinutes(2))
                        .setWaitForYellowStatus()
                        .setWaitForActiveShards(4) // 2 nodes, so 4 shards (2 primaries, 2 replicas)
                        .get();
                assertThat(error.get(), nullValue());
                internalCluster().stopRandomNode(nodePredicate);
                client.admin().cluster().prepareHealth("test")
                        .setWaitForEvents(Priority.LANGUID)
                        .setTimeout(TimeValue.timeValueMinutes(2))
                        .setWaitForYellowStatus()
                        .setWaitForActiveShards(2) // 1 node, so 2 shards (2 primaries, 0 replicas)
                        .get();
                assertThat(error.get(), nullValue());
                internalCluster().startNode();
                client.admin().cluster().prepareHealth("test")
                        .setWaitForEvents(Priority.LANGUID)
                        .setTimeout(TimeValue.timeValueMinutes(2))
                        .setWaitForYellowStatus()
                        .setWaitForActiveShards(4)  // 2 nodes, so 4 shards (2 primaries, 2 replicas)
                        .get();
                assertThat(error.get(), nullValue());
                internalCluster().startNode();
                client.admin().cluster().prepareHealth("test")
                        .setWaitForEvents(Priority.LANGUID)
                        .setTimeout(TimeValue.timeValueMinutes(2))
                        .setWaitForGreenStatus() // We're confirm the shard settings, so green instead of yellow
                        .setWaitForActiveShards(6) // 3 nodes, so 6 shards (2 primaries, 4 replicas)
                        .get();
                assertThat(error.get(), nullValue());
            }
        } finally {
            run.set(false);
        }
        t.join();
        assertThat(error.get(), nullValue());
    }

}
