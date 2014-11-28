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

package org.elasticsearch.index;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.fd.FaultDetection;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import java.util.List;

import static org.elasticsearch.cluster.routing.ShardRoutingState.*;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test failure when index replication actions fail mid-flight
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0, transportClientRatio = 0)
public class TransportIndexFailuresTest extends ElasticsearchIntegrationTest {

    private static final Settings nodeSettings = ImmutableSettings.settingsBuilder()
            .put("discovery.type", "zen") // <-- To override the local setting if set externally
            .put(FaultDetection.SETTING_PING_TIMEOUT, "1s") // <-- for hitting simulated network failures quickly
            .put(FaultDetection.SETTING_PING_RETRIES, "1") // <-- for hitting simulated network failures quickly
            .put(DiscoverySettings.PUBLISH_TIMEOUT, "1s") // <-- for hitting simulated network failures quickly
            .put("discovery.zen.minimum_master_nodes", 1)
            .put(TransportModule.TRANSPORT_SERVICE_TYPE_KEY, MockTransportService.class.getName())
            .build();

    @Override
    protected int numberOfShards() {
        return 1;
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    @Test
    public void testNetworkPartitionDuringReplicaIndexOp() throws Exception {
        final String INDEX = "testidx";

        List<String> nodes = internalCluster().startNodesAsync(2, nodeSettings).get();

        // Create index test with 1 shard, 1 replica and ensure it is green
        createIndex(INDEX);
        ensureGreen(INDEX);

        // Disable allocation so the replica cannot be reallocated when it fails
        Settings s = ImmutableSettings.builder().put("cluster.routing.allocation.enable", "none").build();
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(s).get();

        // Determine which node holds the primary shard
        ClusterState state = getNodeClusterState(nodes.get(0));
        IndexShardRoutingTable shard = state.getRoutingTable().index(INDEX).shard(0);
        String primaryNode;
        String replicaNode;
        if (shard.getShards().get(0).primary()) {
            primaryNode = nodes.get(0);
            replicaNode = nodes.get(1);
        } else {
            primaryNode = nodes.get(1);
            replicaNode = nodes.get(0);
        }
        logger.info("--> primary shard is on {}", primaryNode);

        // Index a document to make sure everything works well
        IndexResponse resp = internalCluster().client(primaryNode).prepareIndex(INDEX, "doc").setSource("foo", "bar").get();
        assertThat("document exists on primary node",
                internalCluster().client(primaryNode).prepareGet(INDEX, "doc", resp.getId()).setPreference("_only_local").get().isExists(),
                equalTo(true));
        assertThat("document exists on replica node",
                internalCluster().client(replicaNode).prepareGet(INDEX, "doc", resp.getId()).setPreference("_only_local").get().isExists(),
                equalTo(true));

        // Disrupt the network so indexing requests fail to replicate
        logger.info("--> preventing index/replica operations");
        TransportService mockTransportService = internalCluster().getInstance(TransportService.class, primaryNode);
        ((MockTransportService) mockTransportService).addFailToSendNoConnectRule(
                internalCluster().getInstance(Discovery.class, replicaNode).localNode(),
                ImmutableSet.of(IndexAction.NAME + "[r]")
        );
        mockTransportService = internalCluster().getInstance(TransportService.class, replicaNode);
        ((MockTransportService) mockTransportService).addFailToSendNoConnectRule(
                internalCluster().getInstance(Discovery.class, primaryNode).localNode(),
                ImmutableSet.of(IndexAction.NAME + "[r]")
        );

        logger.info("--> indexing into primary");
        // the replica shard should now be marked as failed because the replication operation will fail
        resp = internalCluster().client(primaryNode).prepareIndex(INDEX, "doc").setSource("foo", "baz").get();
        // wait until the cluster reaches an exact yellow state, meaning replica has failed
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(client().admin().cluster().prepareHealth().get().getStatus(), equalTo(ClusterHealthStatus.YELLOW));
            }
        });
        assertThat("document should still be indexed and available",
                client().prepareGet(INDEX, "doc", resp.getId()).get().isExists(), equalTo(true));

        state = getNodeClusterState(randomFrom(nodes.toArray(Strings.EMPTY_ARRAY)));
        RoutingNodes rn = state.routingNodes();
        logger.info("--> counts: total: {}, unassigned: {}, initializing: {}, relocating: {}, started: {}",
                rn.shards(new Predicate<MutableShardRouting>() {
                    @Override
                    public boolean apply(org.elasticsearch.cluster.routing.MutableShardRouting input) {
                        return true;
                    }
                }).size(),
                rn.shardsWithState(UNASSIGNED).size(),
                rn.shardsWithState(INITIALIZING).size(),
                rn.shardsWithState(RELOCATING).size(),
                rn.shardsWithState(STARTED).size());
        logger.info("--> unassigned: {}, initializing: {}, relocating: {}, started: {}",
                rn.shardsWithState(UNASSIGNED),
                rn.shardsWithState(INITIALIZING),
                rn.shardsWithState(RELOCATING),
                rn.shardsWithState(STARTED));

        assertThat("only a single shard is now active (replica should be failed and not reallocated)",
                rn.shardsWithState(STARTED).size(), equalTo(1));
    }

    private ClusterState getNodeClusterState(String node) {
        return internalCluster().client(node).admin().cluster().prepareState().setLocal(true).get().getState();
    }
}
