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

package org.elasticsearch.test.integration.gateway.local;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.*;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class LocalGatewayIndexStateTests extends AbstractNodesTests {

    private final ESLogger logger = Loggers.getLogger(LocalGatewayIndexStateTests.class);

    @AfterMethod public void cleanAndCloseNodes() throws Exception {
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

    @Test public void testMappingMetaDataParsed() throws Exception {
        logger.info("--> cleaning nodes");
        buildNode("node1", settingsBuilder().put("gateway.type", "local"));
        buildNode("node2", settingsBuilder().put("gateway.type", "local"));
        cleanAndCloseNodes();

        logger.info("--> starting 1 nodes");
        startNode("node1", settingsBuilder().put("gateway.type", "local"));

        logger.info("--> creating test index, with meta routing");
        client("node1").admin().indices().prepareCreate("test")
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("_routing").field("required", true).endObject().endObject().endObject())
                .execute().actionGet();

        logger.info("--> waiting for yellow status");
        ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth().setWaitForActiveShards(5).setWaitForYellowStatus().execute().actionGet();
        if (health.timedOut()) {
            ClusterStateResponse response = client("node1").admin().cluster().prepareState().execute().actionGet();
            System.out.println("" + response);
        }
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> verify meta _routing required exists");
        MappingMetaData mappingMd = client("node1").admin().cluster().prepareState().execute().actionGet().state().metaData().index("test").mapping("type1");
        assertThat(mappingMd.routing().required(), equalTo(true));

        logger.info("--> close node");
        closeNode("node1");

        logger.info("--> starting node again...");
        startNode("node1", settingsBuilder().put("gateway.type", "local"));

        logger.info("--> waiting for yellow status");
        health = client("node1").admin().cluster().prepareHealth().setWaitForActiveShards(5).setWaitForYellowStatus().execute().actionGet();
        if (health.timedOut()) {
            ClusterStateResponse response = client("node1").admin().cluster().prepareState().execute().actionGet();
            System.out.println("" + response);
        }
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> verify meta _routing required exists");
        mappingMd = client("node1").admin().cluster().prepareState().execute().actionGet().state().metaData().index("test").mapping("type1");
        assertThat(mappingMd.routing().required(), equalTo(true));
    }

    @Test public void testSimpleOpenClose() throws Exception {
        logger.info("--> cleaning nodes");
        buildNode("node1", settingsBuilder().put("gateway.type", "local").build());
        buildNode("node2", settingsBuilder().put("gateway.type", "local").build());
        cleanAndCloseNodes();

        logger.info("--> starting 2 nodes");
        startNode("node1", settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());
        startNode("node2", settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());

        logger.info("--> creating test index");
        client("node1").admin().indices().prepareCreate("test").execute().actionGet();

        logger.info("--> waiting for green status");
        ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        ClusterStateResponse stateResponse = client("node1").admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.state().metaData().index("test").state(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.state().routingTable().index("test").shards().size(), equalTo(2));
        assertThat(stateResponse.state().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(), equalTo(4));

        logger.info("--> indexing a simple document");
        client("node1").prepareIndex("test", "type1", "1").setSource("field1", "value1").execute().actionGet();

        logger.info("--> closing test index...");
        client("node1").admin().indices().prepareClose("test").execute().actionGet();

        stateResponse = client("node1").admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.state().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.state().routingTable().index("test"), nullValue());

        logger.info("--> verifying that the state is green");
        health = client("node1").admin().cluster().prepareHealth().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));
        assertThat(health.status(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> trying to index into a closed index ...");
        try {
            client("node1").prepareIndex("test", "type1", "1").setSource("field1", "value1").setTimeout("1s").execute().actionGet();
            assert false;
        } catch (ClusterBlockException e) {
            // all is well
        }

        logger.info("--> creating another index (test2) by indexing into it");
        client("node1").prepareIndex("test2", "type1", "1").setSource("field1", "value1").execute().actionGet();
        logger.info("--> verifying that the state is green");
        health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));
        assertThat(health.status(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("--> opening the first index again...");
        client("node1").admin().indices().prepareOpen("test").execute().actionGet();

        logger.info("--> verifying that the state is green");
        health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));
        assertThat(health.status(), equalTo(ClusterHealthStatus.GREEN));

        stateResponse = client("node1").admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.state().metaData().index("test").state(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.state().routingTable().index("test").shards().size(), equalTo(2));
        assertThat(stateResponse.state().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(), equalTo(4));

        logger.info("--> trying to get the indexed document on the first index");
        GetResponse getResponse = client("node1").prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(getResponse.exists(), equalTo(true));

        logger.info("--> closing test index...");
        client("node1").admin().indices().prepareClose("test").execute().actionGet();
        stateResponse = client("node1").admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.state().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.state().routingTable().index("test"), nullValue());

        logger.info("--> closing nodes...");
        closeNode("node2");
        closeNode("node1");

        logger.info("--> starting nodes again...");
        startNode("node1", settingsBuilder().put("gateway.type", "local").build());
        startNode("node2", settingsBuilder().put("gateway.type", "local").build());

        logger.info("--> waiting for two nodes and green status");
        health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        stateResponse = client("node1").admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.state().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.state().routingTable().index("test"), nullValue());

        logger.info("--> trying to index into a closed index ...");
        try {
            client("node1").prepareIndex("test", "type1", "1").setSource("field1", "value1").setTimeout("1s").execute().actionGet();
            assert false;
        } catch (ClusterBlockException e) {
            // all is well
        }

        logger.info("--> opening index...");
        client("node1").admin().indices().prepareOpen("test").execute().actionGet();

        logger.info("--> waiting for green status");
        health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        stateResponse = client("node1").admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.state().metaData().index("test").state(), equalTo(IndexMetaData.State.OPEN));
        assertThat(stateResponse.state().routingTable().index("test").shards().size(), equalTo(2));
        assertThat(stateResponse.state().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(), equalTo(4));

        logger.info("--> trying to get the indexed document on the first round (before close and shutdown)");
        getResponse = client("node1").prepareGet("test", "type1", "1").execute().actionGet();
        assertThat(getResponse.exists(), equalTo(true));

        logger.info("--> indexing a simple document");
        client("node1").prepareIndex("test", "type1", "2").setSource("field1", "value1").execute().actionGet();
    }

    @Test public void testJustMasterNode() throws Exception {
        logger.info("--> cleaning nodes");
        buildNode("node1", settingsBuilder().put("gateway.type", "local").build());
        buildNode("node2", settingsBuilder().put("gateway.type", "local").build());
        cleanAndCloseNodes();

        logger.info("--> starting 1 master node non data");
        startNode("node1", settingsBuilder().put("node.data", false).put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());

        logger.info("--> create an index");
        client("node1").admin().indices().prepareCreate("test").execute().actionGet();

        logger.info("--> closing master node");
        closeNode("node1");

        logger.info("--> starting 1 master node non data again");
        startNode("node1", settingsBuilder().put("node.data", false).put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());

        logger.info("--> waiting for test index to be created");
        ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth().setIndices("test").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> verify we have an index");
        ClusterStateResponse clusterStateResponse = client("node1").admin().cluster().prepareState().setFilterIndices("test").execute().actionGet();
        assertThat(clusterStateResponse.state().metaData().hasIndex("test"), equalTo(true));
    }

    @Test public void testJustMasterNodeAndJustDataNode() throws Exception {
        logger.info("--> cleaning nodes");
        buildNode("node1", settingsBuilder().put("gateway.type", "local").build());
        buildNode("node2", settingsBuilder().put("gateway.type", "local").build());
        cleanAndCloseNodes();

        logger.info("--> starting 1 master node non data");
        startNode("node1", settingsBuilder().put("node.data", false).put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());
        startNode("node2", settingsBuilder().put("node.master", false).put("gateway.type", "local").put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());

        logger.info("--> create an index");
        client("node1").admin().indices().prepareCreate("test").execute().actionGet();

        logger.info("--> waiting for test index to be created");
        ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth().setIndices("test").setWaitForYellowStatus().execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        client("node1").prepareIndex("test", "type1").setSource("field1", "value1").setTimeout("100ms").execute().actionGet();
    }

    @Test public void testTwoNodesSingleDoc() throws Exception {
        logger.info("--> cleaning nodes");
        buildNode("node1", settingsBuilder().put("gateway.type", "local").build());
        buildNode("node2", settingsBuilder().put("gateway.type", "local").build());
        cleanAndCloseNodes();

        logger.info("--> starting 2 nodes");
        startNode("node1", settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 5).put("index.number_of_replicas", 1).build());
        startNode("node2", settingsBuilder().put("gateway.type", "local").put("index.number_of_shards", 5).put("index.number_of_replicas", 1).build());

        logger.info("--> indexing a simple document");
        client("node1").prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefresh(true).execute().actionGet();

        logger.info("--> waiting for green status");
        ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> verify 1 doc in the index");
        for (int i = 0; i < 10; i++) {
            assertThat(client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(1l));
        }

        logger.info("--> closing test index...");
        client("node1").admin().indices().prepareClose("test").execute().actionGet();


        ClusterStateResponse stateResponse = client("node1").admin().cluster().prepareState().execute().actionGet();
        assertThat(stateResponse.state().metaData().index("test").state(), equalTo(IndexMetaData.State.CLOSE));
        assertThat(stateResponse.state().routingTable().index("test"), nullValue());

        logger.info("--> opening the index...");
        client("node1").admin().indices().prepareOpen("test").execute().actionGet();

        logger.info("--> waiting for green status");
        health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> verify 1 doc in the index");
        assertThat(client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(1l));
        for (int i = 0; i < 10; i++) {
            assertThat(client("node1").prepareCount().setQuery(matchAllQuery()).execute().actionGet().count(), equalTo(1l));
        }
    }
}
