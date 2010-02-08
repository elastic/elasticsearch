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

package org.elasticsearch.test.integration.indexlifecycle;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.server.internal.InternalServer;
import org.elasticsearch.test.integration.AbstractServersTests;
import org.elasticsearch.util.logging.Loggers;
import org.elasticsearch.util.settings.Settings;
import org.slf4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.cluster.routing.ShardRoutingState.*;
import static org.elasticsearch.util.settings.ImmutableSettings.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class IndexLifecycleActionTests extends AbstractServersTests {

    private final Logger logger = Loggers.getLogger(IndexLifecycleActionTests.class);

    @AfterMethod public void closeServers() {
        closeAllServers();
    }

    @Test public void testIndexLifecycleActionsWith11Shards1Backup() throws Exception {
        Settings settings = settingsBuilder()
                .putInt(SETTING_NUMBER_OF_SHARDS, 11)
                .putInt(SETTING_NUMBER_OF_REPLICAS, 1)
                .build();

        // start one server
        logger.info("Starting sever1");
        startServer("server1", settings);

        ClusterService clusterService1 = ((InternalServer) server("server1")).injector().getInstance(ClusterService.class);

        logger.info("Creating index [test]");
        client("server1").admin().indices().create(createIndexRequest("test")).actionGet();

        Thread.sleep(1000);

        ClusterState clusterState1 = clusterService1.state();
        RoutingNode routingNodeEntry1 = clusterState1.routingNodes().nodesToShards().get(clusterState1.nodes().localNodeId());
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(11));

        clusterState1 = client("server1").admin().cluster().state(clusterState()).actionGet().state();
        routingNodeEntry1 = clusterState1.routingNodes().nodesToShards().get(clusterState1.nodes().localNodeId());
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(11));

        logger.info("Starting server2");
        // start another server
        startServer("server2", settings);

        ClusterService clusterService2 = ((InternalServer) server("server2")).injector().getInstance(ClusterService.class);

        Thread.sleep(1500);

        clusterState1 = clusterService1.state();
        routingNodeEntry1 = clusterState1.routingNodes().nodesToShards().get(clusterState1.nodes().localNodeId());
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(11));

        ClusterState clusterState2 = clusterService2.state();
        RoutingNode routingNodeEntry2 = clusterState2.routingNodes().nodesToShards().get(clusterState2.nodes().localNodeId());
        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), equalTo(11));

        logger.info("Starting server3");
        // start another server
        startServer("server3", settings);

        ClusterService clusterService3 = ((InternalServer) server("server3")).injector().getInstance(ClusterService.class);

        Thread.sleep(1500);

        clusterState1 = clusterService1.state();
        routingNodeEntry1 = clusterState1.routingNodes().nodesToShards().get(clusterState1.nodes().localNodeId());
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), anyOf(equalTo(7), equalTo(8)));

        clusterState2 = clusterService2.state();
        routingNodeEntry2 = clusterState2.routingNodes().nodesToShards().get(clusterState2.nodes().localNodeId());
        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), anyOf(equalTo(7), equalTo(8)));

        ClusterState clusterState3 = clusterService3.state();
        RoutingNode routingNodeEntry3 = clusterState3.routingNodes().nodesToShards().get(clusterState3.nodes().localNodeId());
        assertThat(routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(7));

        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED) + routingNodeEntry2.numberOfShardsWithState(STARTED) + routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(22));

        logger.info("Closing server1");
        // kill the first server
        closeServer("server1");

        Thread.sleep(1500);

        clusterState2 = clusterService2.state();
        routingNodeEntry2 = clusterState2.routingNodes().nodesToShards().get(clusterState2.nodes().localNodeId());
        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), equalTo(11));

        clusterState3 = clusterService3.state();
        routingNodeEntry3 = clusterState3.routingNodes().nodesToShards().get(clusterState3.nodes().localNodeId());
        assertThat(routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(11));

        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED) + routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(22));

        logger.info("Deleting index [test]");
        // last, lets delete the index
        client("server2").admin().indices().delete(deleteIndexRequest("test")).actionGet();

        Thread.sleep(1500);
        clusterState2 = clusterService2.state();
        routingNodeEntry2 = clusterState2.routingNodes().nodesToShards().get(clusterState2.nodes().localNodeId());
        assertThat(routingNodeEntry2, nullValue());

        clusterState3 = clusterService3.state();
        routingNodeEntry3 = clusterState3.routingNodes().nodesToShards().get(clusterState3.nodes().localNodeId());
        assertThat(routingNodeEntry3, nullValue());
    }

    @Test public void testIndexLifecycleActionsWith11Shards0Backup() throws Exception {

        Settings settings = settingsBuilder()
                .putInt(SETTING_NUMBER_OF_SHARDS, 11)
                .putInt(SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

        // start one server
        logger.info("Starting server1");
        startServer("server1", settings);

        ClusterService clusterService1 = ((InternalServer) server("server1")).injector().getInstance(ClusterService.class);

        logger.info("Creating index [test]");
        client("server1").admin().indices().create(createIndexRequest("test")).actionGet();

        Thread.sleep(1000);

        ClusterState clusterState1 = clusterService1.state();
        RoutingNode routingNodeEntry1 = clusterState1.routingNodes().nodesToShards().get(clusterState1.nodes().localNodeId());
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(11));

        // start another server
        logger.info("Starting server2");
        startServer("server2", settings);

        ClusterService clusterService2 = ((InternalServer) server("server2")).injector().getInstance(ClusterService.class);

        Thread.sleep(2000);

        clusterState1 = clusterService1.state();
        routingNodeEntry1 = clusterState1.routingNodes().nodesToShards().get(clusterState1.nodes().localNodeId());
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), anyOf(equalTo(6), equalTo(5)));

        ClusterState clusterState2 = clusterService2.state();
        RoutingNode routingNodeEntry2 = clusterState2.routingNodes().nodesToShards().get(clusterState2.nodes().localNodeId());
        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), anyOf(equalTo(5), equalTo(6)));

        // start another server
        logger.info("Starting server3");
        startServer("server3");

        ClusterService clusterService3 = ((InternalServer) server("server3")).injector().getInstance(ClusterService.class);

        Thread.sleep(1500);

        clusterState1 = clusterService1.state();
        routingNodeEntry1 = clusterState1.routingNodes().nodesToShards().get(clusterState1.nodes().localNodeId());
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), anyOf(equalTo(5), equalTo(3)));

        clusterState2 = clusterService2.state();
        routingNodeEntry2 = clusterState2.routingNodes().nodesToShards().get(clusterState2.nodes().localNodeId());
        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), anyOf(equalTo(5), equalTo(3)));

        ClusterState clusterState3 = clusterService3.state();
        RoutingNode routingNodeEntry3 = clusterState3.routingNodes().nodesToShards().get(clusterState3.nodes().localNodeId());
        assertThat(routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(3));

        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED) + routingNodeEntry2.numberOfShardsWithState(STARTED) + routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(11));

        logger.info("Closing server1");
        // kill the first server
        closeServer("server1");

        Thread.sleep(2000);

        clusterState2 = clusterService2.state();
        routingNodeEntry2 = clusterState2.routingNodes().nodesToShards().get(clusterState2.nodes().localNodeId());
        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), anyOf(equalTo(5), equalTo(6)));

        clusterState3 = clusterService3.state();
        routingNodeEntry3 = clusterState3.routingNodes().nodesToShards().get(clusterState3.nodes().localNodeId());
        assertThat(routingNodeEntry3.numberOfShardsWithState(STARTED), anyOf(equalTo(5), equalTo(6)));

        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED) + routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(11));

        logger.info("Deleting index [test]");
        // last, lets delete the index
        client("server2").admin().indices().delete(deleteIndexRequest("test")).actionGet();

        Thread.sleep(2000);
        clusterState2 = clusterService2.state();
        routingNodeEntry2 = clusterState2.routingNodes().nodesToShards().get(clusterState2.nodes().localNodeId());
        assertThat(routingNodeEntry2, nullValue());

        clusterState3 = clusterService3.state();
        routingNodeEntry3 = clusterState3.routingNodes().nodesToShards().get(clusterState3.nodes().localNodeId());
        assertThat(routingNodeEntry3, nullValue());
    }

    @Test public void testTwoIndicesCreation() throws Exception {

        Settings settings = settingsBuilder()
                .putInt(SETTING_NUMBER_OF_SHARDS, 11)
                .putInt(SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

        // start one server
        startServer("server1", settings);
        client("server1").admin().indices().create(createIndexRequest("test1")).actionGet();
        client("server1").admin().indices().create(createIndexRequest("test2")).actionGet();
    }
}
