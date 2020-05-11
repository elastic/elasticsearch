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

package org.elasticsearch.discovery;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.FollowersChecker;
import org.elasticsearch.cluster.coordination.JoinHelper;
import org.elasticsearch.cluster.coordination.LeaderChecker;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.Bridge;
import org.elasticsearch.test.disruption.NetworkDisruption.DisruptedLinks;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkDisconnect;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkLinkDisruptionType;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.disruption.SlowClusterStateProcessing;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportSettings;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public abstract class AbstractDisruptionTestCase extends ESIntegTestCase {

    static final TimeValue DISRUPTION_HEALING_OVERHEAD = TimeValue.timeValueSeconds(40); // we use 30s as timeout in many places.


    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(DEFAULT_SETTINGS).build();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder().put(super.indexSettings())
            // sync global checkpoint quickly so we can verify seq_no_stats aligned between all copies after tests.
            .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s").build();
    }

    @Override
    protected int numberOfShards() {
        return 3;
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    private boolean disableBeforeIndexDeletion;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        disableBeforeIndexDeletion = false;
    }

    @Override
    public void setDisruptionScheme(ServiceDisruptionScheme scheme) {
        if (scheme instanceof NetworkDisruption &&
                ((NetworkDisruption) scheme).getNetworkLinkDisruptionType() instanceof NetworkDisruption.NetworkUnresponsive) {
            // the network unresponsive disruption may leave operations in flight
            // this is because this disruption scheme swallows requests by design
            // as such, these operations will never be marked as finished
            disableBeforeIndexDeletion = true;
        }
        super.setDisruptionScheme(scheme);
    }

    @Override
    protected void beforeIndexDeletion() throws Exception {
        if (disableBeforeIndexDeletion == false) {
            super.beforeIndexDeletion();
            internalCluster().assertConsistentHistoryBetweenTranslogAndLuceneIndex();
            internalCluster().assertSeqNos();
            internalCluster().assertSameDocIdsOnShards();
        }
    }

    List<String> startCluster(int numberOfNodes) {
        InternalTestCluster internalCluster = internalCluster();
        List<String> nodes = internalCluster.startNodes(numberOfNodes);
        ensureStableCluster(numberOfNodes);
        return nodes;
    }

    static final Settings DEFAULT_SETTINGS = Settings.builder()
            .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "5s") // for hitting simulated network failures quickly
            .put(LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), 1) // for hitting simulated network failures quickly
            .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "5s") // for hitting simulated network failures quickly
            .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1) // for hitting simulated network failures quickly
            .put(JoinHelper.JOIN_TIMEOUT_SETTING.getKey(), "10s") // still long to induce failures but to long so test won't time out
            .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "5s") // <-- for hitting simulated network failures quickly
            .put(TransportSettings.CONNECT_TIMEOUT.getKey(), "10s") // Network delay disruption waits for the min between this
            // value and the time of disruption and does not recover immediately
            // when disruption is stop. We should make sure we recover faster
            // then the default of 30s, causing ensureGreen and friends to time out
            .build();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, InternalSettingsPlugin.class);
    }

    ClusterState getNodeClusterState(String node) {
        return client(node).admin().cluster().prepareState().setLocal(true).get().getState();
    }

    void assertNoMaster(final String node) throws Exception {
        assertNoMaster(node, null, TimeValue.timeValueSeconds(30));
    }

    void assertNoMaster(final String node, TimeValue maxWaitTime) throws Exception {
        assertNoMaster(node, null, maxWaitTime);
    }

    void assertNoMaster(final String node, @Nullable final ClusterBlock expectedBlocks, TimeValue maxWaitTime) throws Exception {
        assertBusy(() -> {
            ClusterState state = getNodeClusterState(node);
            final DiscoveryNodes nodes = state.nodes();
            assertNull("node [" + node + "] still has [" + nodes.getMasterNode() + "] as master", nodes.getMasterNode());
            if (expectedBlocks != null) {
                for (ClusterBlockLevel level : expectedBlocks.levels()) {
                    assertTrue("node [" + node + "] does have level [" + level + "] in it's blocks",
                        state.getBlocks().hasGlobalBlockWithLevel(level));
                }
            }
        }, maxWaitTime.getMillis(), TimeUnit.MILLISECONDS);
    }

    void assertDifferentMaster(final String node, final String oldMasterNode) throws Exception {
        assertBusy(() -> {
            ClusterState state = getNodeClusterState(node);
            String masterNode = null;
            if (state.nodes().getMasterNode() != null) {
                masterNode = state.nodes().getMasterNode().getName();
            }
            logger.trace("[{}] master is [{}]", node, state.nodes().getMasterNode());
            assertThat("node [" + node + "] still has [" + masterNode + "] as master",
                    oldMasterNode, not(equalTo(masterNode)));
        }, 30, TimeUnit.SECONDS);
    }

    void assertMaster(String masterNode, List<String> nodes) throws Exception {
        assertBusy(() -> {
            for (String node : nodes) {
                ClusterState state = getNodeClusterState(node);
                String failMsgSuffix = "cluster_state:\n" + state;
                assertThat("wrong node count on [" + node + "]. " + failMsgSuffix, state.nodes().getSize(), equalTo(nodes.size()));
                String otherMasterNodeName = state.nodes().getMasterNode() != null ? state.nodes().getMasterNode().getName() : null;
                assertThat("wrong master on node [" + node + "]. " + failMsgSuffix, otherMasterNodeName, equalTo(masterNode));
            }
        });
    }

    public ServiceDisruptionScheme addRandomDisruptionScheme() {
        // TODO: add partial partitions
        final DisruptedLinks disruptedLinks;
        if (randomBoolean()) {
            disruptedLinks = TwoPartitions.random(random(), internalCluster().getNodeNames());
        } else {
            disruptedLinks = Bridge.random(random(), internalCluster().getNodeNames());
        }
        final NetworkLinkDisruptionType disruptionType;
        switch (randomInt(2)) {
            case 0:
                disruptionType = new NetworkDisruption.NetworkUnresponsive();
                break;
            case 1:
                disruptionType = new NetworkDisconnect();
                break;
            case 2:
                disruptionType = NetworkDisruption.NetworkDelay.random(random());
                break;
            default:
                throw new IllegalArgumentException();
        }
        final ServiceDisruptionScheme scheme;
        if (rarely()) {
            scheme = new SlowClusterStateProcessing(random());
        } else {
            scheme = new NetworkDisruption(disruptedLinks, disruptionType);
        }
        setDisruptionScheme(scheme);
        return scheme;
    }

    NetworkDisruption addRandomDisruptionType(TwoPartitions partitions) {
        final NetworkLinkDisruptionType disruptionType;
        if (randomBoolean()) {
            disruptionType = new NetworkDisruption.NetworkUnresponsive();
        } else {
            disruptionType = new NetworkDisconnect();
        }
        NetworkDisruption partition = new NetworkDisruption(partitions, disruptionType);

        setDisruptionScheme(partition);

        return partition;
    }

    TwoPartitions isolateNode(String isolatedNode) {
        Set<String> side1 = new HashSet<>();
        Set<String> side2 = new HashSet<>(Arrays.asList(internalCluster().getNodeNames()));
        side1.add(isolatedNode);
        side2.remove(isolatedNode);

        return new TwoPartitions(side1, side2);
    }

}
