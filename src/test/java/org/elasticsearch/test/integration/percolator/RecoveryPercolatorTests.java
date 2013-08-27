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

package org.elasticsearch.test.integration.percolator;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.percolate.MultiPercolateRequestBuilder;
import org.elasticsearch.action.percolate.MultiPercolateResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.percolate.PercolateSourceBuilder.docBuilder;
import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.integration.percolator.PercolatorTests.convertFromTextArray;
import static org.elasticsearch.test.integration.percolator.TTLPercolatorTests.ensureGreen;
import static org.hamcrest.Matchers.*;

public class RecoveryPercolatorTests extends AbstractNodesTests {

    @After
    public void cleanAndCloseNodes() throws Exception {
        for (int i = 0; i < 10; i++) {
            if (node("node" + i) != null) {
                node("node" + i).stop();
                // since we store (by default) the index snapshot under the gateway, resetting it will reset the index data as well
                if (((InternalNode) node("node" + i)).injector().getInstance(NodeEnvironment.class).hasNodeFile()) {
                    ((InternalNode) node("node" + i)).injector().getInstance(Gateway.class).reset();
                }
            }
        }
        closeAllNodes();
    }
    
    

    @Override
    protected Settings getClassDefaultSettings() {
        return settingsBuilder().put("gateway.type", "local").build();
    }

    @Test
    public void testRestartNodePercolator1() throws Exception {
        logger.info("--> cleaning nodes");
        buildNode("node1");
        cleanAndCloseNodes();

        logger.info("--> starting 1 nodes");
        startNode("node1");

        Client client = client("node1");
        client.admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        logger.info("--> register a query");
        client.prepareIndex("test", "_percolator", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        PercolateResponse percolate = client.preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc")
                .field("field1", "value1")
                .endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches(), arrayWithSize(1));

        client.close();
        closeNode("node1");

        startNode("node1");
        client = client("node1");

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ClusterHealthResponse clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        percolate = client.preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc")
                .field("field1", "value1")
                .endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches(), arrayWithSize(1));
    }

    @Test
    public void testRestartNodePercolator2() throws Exception {
        logger.info("--> cleaning nodes");
        buildNode("node1");
        cleanAndCloseNodes();

        logger.info("--> starting 1 nodes");
        startNode("node1");

        Client client = client("node1");
        client.admin().indices().prepareCreate("test")
        .setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        logger.info("--> register a query");
        client.prepareIndex("test", "_percolator", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        assertThat(client.prepareCount().setTypes("_percolator").setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(1l));

        PercolateResponse percolate = client.preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc")
                .field("field1", "value1")
                .endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches(), arrayWithSize(1));

        client.close();
        closeNode("node1");

        startNode("node1");
        client = client("node1");

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ClusterHealthResponse clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        assertThat(client.prepareCount().setTypes("_percolator").setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(1l));

        DeleteIndexResponse actionGet = client.admin().indices().prepareDelete("test").execute().actionGet();
        assertThat(actionGet.isAcknowledged(), equalTo(true));
        client.admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        clusterHealth = client("node1").admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
        assertThat(client.prepareCount().setTypes("_percolator").setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(0l));

        percolate = client.preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc")
                .field("field1", "value1")
                .endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches(), emptyArray());

        logger.info("--> register a query");
        client.prepareIndex("test", "_percolator", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        assertThat(client.prepareCount().setTypes("_percolator").setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(1l));

        percolate = client.preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc")
                .field("field1", "value1")
                .endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches(), arrayWithSize(1));
    }

    @Test
    public void testLoadingPercolateQueriesDuringCloseAndOpen() throws Exception {
        Settings settings = settingsBuilder()
                .put("gateway.type", "local").build();
        logger.info("--> starting 2 nodes");
        startNode("node1", settings);
        startNode("node2", settings);

        Client client = client("node1");
        client.admin().indices().prepareDelete().execute().actionGet();
        ensureGreen(client);

        client.admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 2))
                .execute().actionGet();
        ensureGreen(client);

        logger.info("--> Add dummy docs");
        client.prepareIndex("test", "type1", "1").setSource("field1", 0).execute().actionGet();
        client.prepareIndex("test", "type2", "1").setSource("field1", "0").execute().actionGet();

        logger.info("--> register a queries");
        for (int i = 1; i <= 100; i++) {
            client.prepareIndex("test", "_percolator", Integer.toString(i))
                    .setSource(jsonBuilder().startObject()
                            .field("query", rangeQuery("field1").from(0).to(i))
                                    // The type must be set now, because two fields with the same name exist in different types.
                                    // Setting the type to `type1`, makes sure that the range query gets parsed to a Lucene NumericRangeQuery.
                            .field("type", "type1")
                            .endObject())
                    .execute().actionGet();
        }

        logger.info("--> Percolate doc with field1=95");
        PercolateResponse response = client.preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", 95).endObject().endObject())
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(6));
        assertThat(convertFromTextArray(response.getMatches(), "test"), arrayContainingInAnyOrder("95", "96", "97", "98", "99", "100"));

        logger.info("--> Close and open index to trigger percolate queries loading...");
        client.admin().indices().prepareClose("test").execute().actionGet();
        ensureGreen(client);
        client.admin().indices().prepareOpen("test").execute().actionGet();
        ensureGreen(client);

        logger.info("--> Percolate doc with field1=100");
        response = client.preparePercolate()
                .setIndices("test").setDocumentType("type1")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", 100).endObject().endObject())
                .execute().actionGet();
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
        Settings settings = settingsBuilder()
                .put("gateway.type", "none").build();
        logger.info("--> starting 3 nodes");
        startNode("node1", settings);
        startNode("node2", settings);
        startNode("node3", settings);

        final Client client = client("node1");
        client.admin().indices().prepareDelete().execute().actionGet();
        ensureGreen(client);

        client.admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 2)
                )
                .execute().actionGet();
        ensureGreen(client);

        final int numQueries = randomIntBetween(50, 100);
        logger.info("--> register a queries");
        for (int i = 0; i < numQueries; i++) {
            client.prepareIndex("test", "_percolator", Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                    .execute().actionGet();
        }

        client.prepareIndex("test", "type", "1")
                .setSource(jsonBuilder().startObject().field("field", "a"))
                .execute().actionGet();

        final AtomicBoolean run = new AtomicBoolean(true);
        final CountDownLatch done = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
        Runnable r = new Runnable() {
            @Override
            public void run() {
                try {
                    XContentBuilder doc = jsonBuilder().startObject().field("field", "a").endObject();
                    while (run.get()) {
                        NodesInfoResponse nodesInfoResponse = client.admin().cluster().prepareNodesInfo()
                                .execute().actionGet();
                        String node2Id = null;
                        String node3Id = null;
                        for (NodeInfo nodeInfo : nodesInfoResponse) {
                            if ("node2".equals(nodeInfo.getNode().getName())) {
                                node2Id = nodeInfo.getNode().id();
                            } else if ("node3".equals(nodeInfo.getNode().getName())) {
                                node3Id = nodeInfo.getNode().id();
                            }
                        }

                        String preference;
                        if (node2Id == null && node3Id == null) {
                            preference = "_local";
                        } else if (node2Id == null || node3Id == null) {
                            if (node2Id != null) {
                                preference = "_prefer_node:" + node2Id;
                            } else {
                                preference = "_prefer_node:" + node3Id;
                            }
                        } else {
                            preference = "_prefer_node:" + (randomBoolean() ? node2Id : node3Id);
                        }

                        if (multiPercolate) {
                            MultiPercolateRequestBuilder builder = client
                                    .prepareMultiPercolate();
                            int numPercolateRequest = randomIntBetween(50, 100);

                            for (int i = 0; i < numPercolateRequest; i++) {
                                if (randomBoolean()) {
                                    builder.add(
                                            client.preparePercolate()
                                                    .setPreference(preference)
                                                    .setGetRequest(Requests.getRequest("test").type("type").id("1"))
                                                    .setIndices("test").setDocumentType("type")
                                    );
                                } else {
                                    builder.add(
                                            client.preparePercolate()
                                                    .setPreference(preference)
                                                    .setIndices("test").setDocumentType("type")
                                                    .setPercolateDoc(docBuilder().setDoc(doc)));
                                }
                            }

                            MultiPercolateResponse response = builder.execute().actionGet();
                            assertThat(response.items().length, equalTo(numPercolateRequest));
                            for (MultiPercolateResponse.Item item : response) {
                                assertThat(item.isFailure(), equalTo(false));
                                assertNoFailures(item.getResponse());
                                assertThat(item.getResponse().getSuccessfulShards(), equalTo(2));
                                assertThat(item.getResponse().getCount(), equalTo((long) numQueries));
                                assertThat(item.getResponse().getMatches().length, equalTo(numQueries));
                            }
                        } else {
                            PercolateResponse response;
                            if (randomBoolean()) {
                                response = client.preparePercolate()
                                        .setIndices("test").setDocumentType("type")
                                        .setPercolateDoc(docBuilder().setDoc(doc))
                                        .setPreference(preference)
                                        .execute().actionGet();
                            } else {
                                response = client.preparePercolate()
                                        .setGetRequest(Requests.getRequest("test").type("type").id("1"))
                                        .setIndices("test").setDocumentType("type")
                                        .setPreference(preference)
                                        .execute().actionGet();
                            }
                            assertNoFailures(response);
                            assertThat(response.getSuccessfulShards(), equalTo(2));
                            assertThat(response.getCount(), equalTo((long) numQueries));
                            assertThat(response.getMatches().length, equalTo(numQueries));
                        }
                    }
                } catch (Throwable t) {
                    logger.info("Error in percolate thread...", t);
                    run.set(false);
                    error.set(t);
                } finally {
                    done.countDown();
                }
            }
        };
        new Thread(r).start();

        try {
            for (int i = 0; i < 4; i++) {
                closeNode("node3");
                client.admin().cluster().prepareHealth()
                        .setWaitForEvents(Priority.LANGUID)
                        .setWaitForYellowStatus()
                        .setWaitForNodes("2")
                        .execute().actionGet();
                assertThat(error.get(), nullValue());
                closeNode("node2");
                client.admin().cluster().prepareHealth()
                        .setWaitForEvents(Priority.LANGUID)
                        .setWaitForYellowStatus()
                        .setWaitForNodes("1")
                        .execute().actionGet();
                assertThat(error.get(), nullValue());
                startNode("node3");
                client.admin().cluster().prepareHealth()
                        .setWaitForEvents(Priority.LANGUID)
                        .setWaitForYellowStatus()
                        .setWaitForNodes("2")
                        .execute().actionGet();
                assertThat(error.get(), nullValue());
                startNode("node2");
                client.admin().cluster().prepareHealth()
                        .setWaitForEvents(Priority.LANGUID)
                        .setWaitForYellowStatus()
                        .setWaitForNodes("3")
                        .execute().actionGet();
                assertThat(error.get(), nullValue());
            }
        } finally {
            run.set(false);
        }
        done.await();
        assertThat(error.get(), nullValue());
    }

}
