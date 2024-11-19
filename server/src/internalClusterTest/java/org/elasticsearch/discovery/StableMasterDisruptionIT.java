/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.discovery;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationDiagnosticsService;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.FollowersChecker;
import org.elasticsearch.cluster.coordination.LeaderChecker;
import org.elasticsearch.cluster.coordination.MasterHistoryService;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkLinkDisruptionType;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests relating to the loss of the master, but which work with the default fault detection settings which are rather lenient and will
 * not detect a master failure too quickly.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class StableMasterDisruptionIT extends ESIntegTestCase {

    @Before
    private void setBootstrapMasterNodeIndex() {
        internalCluster().setBootstrapMasterNodeIndex(0);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    /**
     * Test that no split brain occurs under partial network partition. See https://github.com/elastic/elasticsearch/issues/2488
     */
    public void testFailWithMinimumMasterNodesConfigured() throws Exception {
        List<String> nodes = internalCluster().startNodes(3);
        ensureStableCluster(3);

        // Figure out what is the elected master node
        final String masterNode = internalCluster().getMasterName();
        logger.info("---> legit elected master node={}", masterNode);

        // Pick a node that isn't the elected master.
        Set<String> nonMasters = new HashSet<>(nodes);
        nonMasters.remove(masterNode);
        final String unluckyNode = randomFrom(nonMasters.toArray(Strings.EMPTY_ARRAY));

        // Simulate a network issue between the unlucky node and elected master node in both directions.

        NetworkDisruption networkDisconnect = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(masterNode, unluckyNode),
            NetworkDisruption.DISCONNECT
        );
        setDisruptionScheme(networkDisconnect);
        networkDisconnect.startDisrupting();

        // Wait until elected master has removed that the unlucky node...
        ensureStableCluster(2, masterNode);

        // The unlucky node must report *no* master node, since it can't connect to master and in fact it should
        // continuously ping until network failures have been resolved. However
        // It may a take a bit before the node detects it has been cut off from the elected master
        ensureNoMaster(unluckyNode);
        // because it has had a master within the last 30s:
        assertGreenMasterStability(internalCluster().client(unluckyNode));

        networkDisconnect.stopDisrupting();

        // Wait until the master node sees all 3 nodes again.
        ensureStableCluster(3);

        // The elected master shouldn't have changed, since the unlucky node never could have elected itself as master
        assertThat(internalCluster().getMasterName(), equalTo(masterNode));
        assertGreenMasterStability(internalCluster().client());
    }

    private void assertGreenMasterStability(Client client) throws Exception {
        assertMasterStability(client, HealthStatus.GREEN, containsString("The cluster has a stable master node"));
    }

    private void assertMasterStability(Client client, HealthStatus expectedStatus, Matcher<String> expectedMatcher) throws Exception {
        assertBusy(() -> {
            GetHealthAction.Response healthResponse = client.execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(true, 1000))
                .get();
            String debugInformation = xContentToString(healthResponse);
            assertThat(debugInformation, healthResponse.findIndicator("master_is_stable").status(), equalTo(expectedStatus));
            assertThat(debugInformation, healthResponse.findIndicator("master_is_stable").symptom(), expectedMatcher);
        });
    }

    private String xContentToString(ChunkedToXContent xContent) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        xContent.toXContentChunked(ToXContent.EMPTY_PARAMS).forEachRemaining(xcontent -> {
            try {
                xcontent.toXContent(builder, ToXContent.EMPTY_PARAMS);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                fail(e.getMessage());
            }
        });
        return BytesReference.bytes(builder).utf8ToString();
    }

    private void ensureNoMaster(String node) throws Exception {
        assertBusy(
            () -> assertNull(
                client(node).admin()
                    .cluster()
                    .state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT).local(true))
                    .get()
                    .getState()
                    .nodes()
                    .getMasterNode()
            )
        );
    }

    /**
     * Verify that nodes fault detection detects a disconnected node after master reelection
     */
    public void testFollowerCheckerDetectsDisconnectedNodeAfterMasterReelection() throws Exception {
        testFollowerCheckerAfterMasterReelection(NetworkDisruption.DISCONNECT, Settings.EMPTY);
        assertGreenMasterStability(internalCluster().client());
    }

    /**
     * Verify that nodes fault detection detects an unresponsive node after master reelection
     */
    public void testFollowerCheckerDetectsUnresponsiveNodeAfterMasterReelection() throws Exception {
        testFollowerCheckerAfterMasterReelection(
            NetworkDisruption.UNRESPONSIVE,
            Settings.builder()
                .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
                .put(LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), "4")
                .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
                .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
                .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "10s")
                .build()
        );
        assertGreenMasterStability(internalCluster().client());
    }

    private void testFollowerCheckerAfterMasterReelection(NetworkLinkDisruptionType networkLinkDisruptionType, Settings settings)
        throws Exception {
        internalCluster().startNodes(4, settings);
        ensureStableCluster(4);

        logger.info("--> stopping current master");
        internalCluster().stopCurrentMasterNode();

        ensureStableCluster(3);

        final String master = internalCluster().getMasterName();
        final List<String> nonMasters = Arrays.stream(internalCluster().getNodeNames()).filter(n -> master.equals(n) == false).toList();
        final String isolatedNode = randomFrom(nonMasters);
        final String otherNode = nonMasters.get(nonMasters.get(0).equals(isolatedNode) ? 1 : 0);

        logger.info("--> isolating [{}]", isolatedNode);

        final NetworkDisruption networkDisruption = new NetworkDisruption(
            new TwoPartitions(singleton(isolatedNode), Sets.newHashSet(master, otherNode)),
            networkLinkDisruptionType
        );
        setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        logger.info("--> waiting for master to remove it");
        ensureStableCluster(2, master);
        ensureNoMaster(isolatedNode);

        networkDisruption.stopDisrupting();
        ensureStableCluster(3);
    }

    public void testNoMasterEligibleNodes() throws Exception {
        /*
         * In this test we have a single master-eligible node. We then stop the master. We set the master lookup threshold very low on the
         * data nodes, so when we run the master stability check on one of the data nodes, it will see that there has been no master
         * recently and there are no master eligible nodes, so it returns a RED status.
         */
        internalCluster().startMasterOnlyNodes(
            1,
            Settings.builder()
                .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
                .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
                .put(CoordinationDiagnosticsService.NO_MASTER_TRANSITIONS_THRESHOLD_SETTING.getKey(), 1)
                .build()
        );
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(
            2,
            Settings.builder()
                .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
                .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
                .put(CoordinationDiagnosticsService.NO_MASTER_TRANSITIONS_THRESHOLD_SETTING.getKey(), 1)
                .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), TimeValue.ZERO)
                .put(CoordinationDiagnosticsService.NODE_HAS_MASTER_LOOKUP_TIMEFRAME_SETTING.getKey(), new TimeValue(1, TimeUnit.SECONDS))
                .build()
        );
        ensureStableCluster(3);
        internalCluster().stopCurrentMasterNode();
        assertMasterStability(
            internalCluster().client(randomFrom(dataNodes)),
            HealthStatus.RED,
            containsString("No master eligible nodes found in the cluster")
        );
        for (String dataNode : dataNodes) {
            internalCluster().stopNode(dataNode);
        }
    }

    public void testCannotJoinLeader() throws Exception {
        /*
         * In this test we have a single master-eligible node. We create a cluster change event saying that the master went to null and
         * send it only to the master history on each data node. As a result, the PeerFinder still thinks it is the master. Since the
         * PeerFinder thinks there is a master but we have record of it being null in the history, the data node thinks that it has
         * problems joining the elected master and returns a RED status.
         */
        internalCluster().startMasterOnlyNodes(
            1,
            Settings.builder()
                .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
                .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
                .put(CoordinationDiagnosticsService.NO_MASTER_TRANSITIONS_THRESHOLD_SETTING.getKey(), 1)
                .build()
        );
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(
            2,
            Settings.builder()
                .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
                .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
                .put(CoordinationDiagnosticsService.NO_MASTER_TRANSITIONS_THRESHOLD_SETTING.getKey(), 1)
                .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), TimeValue.ZERO)
                .put(CoordinationDiagnosticsService.NODE_HAS_MASTER_LOOKUP_TIMEFRAME_SETTING.getKey(), new TimeValue(1, TimeUnit.SECONDS))
                .build()
        );
        ensureStableCluster(3);
        Iterable<MasterHistoryService> masterHistoryServices = internalCluster().getDataNodeInstances(MasterHistoryService.class);
        for (MasterHistoryService masterHistoryService : masterHistoryServices) {
            ClusterState state = new ClusterState.Builder(new ClusterName(internalCluster().getClusterName())).nodes(
                new DiscoveryNodes.Builder().masterNodeId(null)
            ).build();
            ClusterState previousState = new ClusterState.Builder(new ClusterName(internalCluster().getClusterName())).nodes(
                new DiscoveryNodes.Builder().masterNodeId("test").add(DiscoveryNodeUtils.create("test", "test"))
            ).build();
            ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent("test", state, previousState);
            masterHistoryService.getLocalMasterHistory().clusterChanged(clusterChangedEvent);
        }
        assertMasterStability(
            internalCluster().client(randomFrom(dataNodes)),
            HealthStatus.RED,
            containsString("has been elected master, but the node being queried")
        );
    }
}
