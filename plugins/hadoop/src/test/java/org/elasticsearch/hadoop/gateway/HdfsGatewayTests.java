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

package org.elasticsearch.hadoop.gateway;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.common.settings.ImmutableSettings.*;
import static org.elasticsearch.node.NodeBuilder.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class HdfsGatewayTests {

    protected final ESLogger logger = Loggers.getLogger(getClass());

    private Node node;

    @BeforeMethod void setUpNodes() throws Exception {
        // start the node and reset the gateway
        node = buildNode();
        ((InternalNode) node).injector().getInstance(Gateway.class).reset();
        node.close();
        // now start the node clean
        node = buildNode().start();
    }

    private Node buildNode() {
        Settings settings = settingsBuilder()
                .put("gateway.type", "hdfs")
                .put("gateway.hdfs.uri", "file:///")
//                .put("gateway.hdfs.uri", "hdfs://training-vm.local:8022")
                .put("gateway.hdfs.path", "work/hdfs/gateway")
                .build();
        return nodeBuilder().settings(settingsBuilder().put(settings).put("node.name", "node1")).build();
    }

    @AfterMethod void closeNodes() throws Exception {
        node.stop();
        ((InternalNode) node).injector().getInstance(Gateway.class).reset();
        node.close();
    }

    @Test public void testHdfsGateway() throws Exception {
        // first, test meta data
        CreateIndexResponse createIndexResponse = node.client().admin().indices().create(createIndexRequest("test")).actionGet();
        assertThat(createIndexResponse.acknowledged(), equalTo(true));
        node.close();
        node = buildNode().start();
        Thread.sleep(500);
        try {
            node.client().admin().indices().create(createIndexRequest("test")).actionGet();
            assert false : "index should exists";
        } catch (IndexAlreadyExistsException e) {
            // all is well
        }

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ClusterHealthResponse clusterHealth = node.client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.status());
        assertThat(clusterHealth.timedOut(), equalTo(false));
        assertThat(clusterHealth.status(), equalTo(ClusterHealthStatus.YELLOW));

        // Translog tests
        // create a mapping
        PutMappingResponse putMappingResponse = node.client().admin().indices().putMapping(putMappingRequest("test").type("type1")
                .source(mappingSource())).actionGet();
        assertThat(putMappingResponse.acknowledged(), equalTo(true));

        // verify that mapping is there
        ClusterStateResponse clusterState = node.client().admin().cluster().state(clusterStateRequest()).actionGet();
        assertThat(clusterState.state().metaData().index("test").mapping("type1"), notNullValue());

        // create two and delete the first
        logger.info("Indexing #1");
        node.client().index(Requests.indexRequest("test").type("type1").id("1").source(source("1", "test"))).actionGet();
        logger.info("Indexing #2");
        node.client().index(Requests.indexRequest("test").type("type1").id("2").source(source("2", "test"))).actionGet();
        logger.info("Deleting #1");
        node.client().delete(deleteRequest("test").type("type1").id("1")).actionGet();

        // perform snapshot to the index
        logger.info("Gateway Snapshot");
        node.client().admin().indices().gatewaySnapshot(gatewaySnapshotRequest("test")).actionGet();
        logger.info("Gateway Snapshot (should be a no op)");
        // do it again, it should be a no op
        node.client().admin().indices().gatewaySnapshot(gatewaySnapshotRequest("test")).actionGet();

        logger.info("Closing the server");
        node.close();
        logger.info("Starting the server, should recover from the gateway (only translog should be populated)");
        node = buildNode().start();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        clusterHealth = node.client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.status());
        assertThat(clusterHealth.timedOut(), equalTo(false));
        assertThat(clusterHealth.status(), equalTo(ClusterHealthStatus.YELLOW));

        // verify that mapping is there
        clusterState = node.client().admin().cluster().state(clusterStateRequest()).actionGet();
        assertThat(clusterState.state().metaData().index("test").mapping("type1"), notNullValue());

        logger.info("Getting #1, should not exists");
        GetResponse getResponse = node.client().get(getRequest("test").type("type1").id("1")).actionGet();
        assertThat(getResponse.exists(), equalTo(false));
        logger.info("Getting #2");
        getResponse = node.client().get(getRequest("test").type("type1").id("2")).actionGet();
        assertThat(getResponse.sourceAsString(), equalTo(source("2", "test")));

        // Now flush and add some data (so we have index recovery as well)
        logger.info("Flushing, so we have actual content in the index files (#2 should be in the index)");
        node.client().admin().indices().flush(flushRequest("test")).actionGet();
        logger.info("Indexing #3, so we have something in the translog as well");
        node.client().index(Requests.indexRequest("test").type("type1").id("3").source(source("3", "test"))).actionGet();

        logger.info("Gateway Snapshot");
        node.client().admin().indices().gatewaySnapshot(gatewaySnapshotRequest("test")).actionGet();
        logger.info("Gateway Snapshot (should be a no op)");
        node.client().admin().indices().gatewaySnapshot(gatewaySnapshotRequest("test")).actionGet();

        logger.info("Closing the server");
        node.close();
        logger.info("Starting the server, should recover from the gateway (both index and translog)");
        node = buildNode().start();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        clusterHealth = node.client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.status());
        assertThat(clusterHealth.timedOut(), equalTo(false));
        assertThat(clusterHealth.status(), equalTo(ClusterHealthStatus.YELLOW));

        logger.info("Getting #1, should not exists");
        getResponse = node.client().get(getRequest("test").type("type1").id("1")).actionGet();
        assertThat(getResponse.exists(), equalTo(false));
        logger.info("Getting #2 (not from the translog, but from the index)");
        getResponse = node.client().get(getRequest("test").type("type1").id("2")).actionGet();
        assertThat(getResponse.sourceAsString(), equalTo(source("2", "test")));
        logger.info("Getting #3 (from the translog)");
        getResponse = node.client().get(getRequest("test").type("type1").id("3")).actionGet();
        assertThat(getResponse.sourceAsString(), equalTo(source("3", "test")));

        logger.info("Flushing, so we have actual content in the index files (#3 should be in the index now as well)");
        node.client().admin().indices().flush(flushRequest("test")).actionGet();

        logger.info("Gateway Snapshot");
        node.client().admin().indices().gatewaySnapshot(gatewaySnapshotRequest("test")).actionGet();
        logger.info("Gateway Snapshot (should be a no op)");
        node.client().admin().indices().gatewaySnapshot(gatewaySnapshotRequest("test")).actionGet();

        logger.info("Closing the server");
        node.close();
        logger.info("Starting the server, should recover from the gateway (just from the index, nothing in the translog)");
        node = buildNode().start();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        clusterHealth = node.client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.status());
        assertThat(clusterHealth.timedOut(), equalTo(false));
        assertThat(clusterHealth.status(), equalTo(ClusterHealthStatus.YELLOW));

        logger.info("Getting #1, should not exists");
        getResponse = node.client().get(getRequest("test").type("type1").id("1")).actionGet();
        assertThat(getResponse.exists(), equalTo(false));
        logger.info("Getting #2 (not from the translog, but from the index)");
        getResponse = node.client().get(getRequest("test").type("type1").id("2")).actionGet();
        assertThat(getResponse.sourceAsString(), equalTo(source("2", "test")));
        logger.info("Getting #3 (not from the translog, but from the index)");
        getResponse = node.client().get(getRequest("test").type("type1").id("3")).actionGet();
        assertThat(getResponse.sourceAsString(), equalTo(source("3", "test")));

        logger.info("Deleting the index");
        node.client().admin().indices().delete(deleteIndexRequest("test")).actionGet();
    }


    private String mappingSource() {
        return "{ type1 : { properties : { name : { type : \"string\" } } } }";
    }

    private String source(String id, String nameValue) {
        return "{ type1 : { \"id\" : \"" + id + "\", \"name\" : \"" + nameValue + "\" } }";
    }
}
