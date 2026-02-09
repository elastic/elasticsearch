/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService.HEARTBEAT_FREQUENCY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.stateless.recovery.TransportStatelessUnpromotableRelocationAction.START_HANDOFF_ACTION_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class SearchShardRelocationIT extends AbstractStatelessPluginIntegTestCase {

    public void testSearchShardRelocationWhenHostingNodeTerminates() throws Exception {
        // MAX_MISSED_HEARTBEATS x HEARTBEAT_FREQUENCY is how long it takes for the last master heartbeat to expire.
        // Speed up the time to master election after master restart.
        final Settings nodeSettings = Settings.builder()
            .put(HEARTBEAT_FREQUENCY.getKey(), TimeValue.timeValueSeconds(1))
            .put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 2)
            .build();
        var indexNode = startMasterAndIndexNode(nodeSettings);
        var searchNodeA = startSearchNode(nodeSettings);
        var searchNodeB = startSearchNode(nodeSettings);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        int numDocs = randomIntBetween(1, 100);
        indexDocs(indexName, numDocs);
        flushAndRefresh(indexName);

        var searchNodeCurrent = internalCluster().nodesInclude(indexName).contains(searchNodeA) ? searchNodeA : searchNodeB;
        var searchNodeNext = internalCluster().nodesInclude(indexName).contains(searchNodeA) ? searchNodeB : searchNodeA;

        if (rarely()) {
            internalCluster().restartNode(indexNode);
        }
        internalCluster().stopNode(searchNodeCurrent);
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), not(hasItem(searchNodeCurrent)));
        assertThat(internalCluster().nodesInclude(indexName), hasItem(searchNodeNext));
    }

    public void testUnpromotableRelocationsDoHandoff() {
        startMasterAndIndexNode();
        var searchNode1 = startSearchNode();
        var indexName = randomIdentifier();

        // The handoff is only sent during relocations
        MockTransportService.getInstance(searchNode1).addSendBehavior((connection, requestId, action, request, options) -> {
            assertThat(request, is(not(equalTo(START_HANDOFF_ACTION_NAME))));
            connection.sendRequest(requestId, action, request, options);
        });
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        var searchNode2 = startSearchNode();

        var startHandOffSent = new CountDownLatch(1);
        MockTransportService.getInstance(searchNode2).addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(START_HANDOFF_ACTION_NAME)) {
                startHandOffSent.countDown();
                assertThat(connection.getNode().getName(), is(equalTo(searchNode1)));
            }
            connection.sendRequest(requestId, action, request, options);
        });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNode1));
        safeAwait(startHandOffSent);
        ensureGreen(indexName);
    }

    public void testUnpromotableRelocationHandoffFailuresDoNotFailTheShardRelocation() {
        startMasterAndIndexNode();
        var searchNode1 = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        var searchNode2 = startSearchNode();

        var handoffFailedLatch = new CountDownLatch(1);
        MockTransportService.getInstance(searchNode1)
            .addRequestHandlingBehavior(START_HANDOFF_ACTION_NAME, (handler, request, channel, task) -> {
                channel.sendResponse(new ElasticsearchException("Boom"));
                handoffFailedLatch.countDown();
            });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNode1));
        safeAwait(handoffFailedLatch);
        ensureGreen(indexName);

        assertThatUnpromotableShardIsStartedInNode(indexName, searchNode2);
    }

    public void testUnpromotableRelocationHandoffWaitsUntilClusterStateConverges() {
        startMasterAndIndexNode(
            Settings.builder().put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMillis(500)).build()
        );
        var searchNode1 = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        var searchNode2 = startSearchNode();

        var handoffReceived = new CountDownLatch(1);
        var handoffFinished = new CountDownLatch(1);
        MockTransportService.getInstance(searchNode1)
            .addRequestHandlingBehavior(START_HANDOFF_ACTION_NAME, (handler, request, channel, task) -> {
                handoffReceived.countDown();
                handler.messageReceived(request, new TransportChannel() {
                    @Override
                    public String getProfileName() {
                        return channel.getProfileName();
                    }

                    @Override
                    public void sendResponse(TransportResponse response) {
                        channel.sendResponse(response);
                        handoffFinished.countDown();
                    }

                    @Override
                    public void sendResponse(Exception exception) {
                        assert false : exception;
                    }
                }, task);
            });

        var blockClusterStateProcessingDisruption = new BlockClusterStateProcessing(searchNode1, random());
        setDisruptionScheme(blockClusterStateProcessingDisruption);
        blockClusterStateProcessingDisruption.startDisrupting();

        // We shouldn't block on this request since it waits until the response is acknowledged, and since
        // we're delaying the cluster state processing, it's likely that the handoff would move on before
        // we can assert that the latch is still "active"
        var updateIndexSettingsFuture = indicesAdmin().prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNode1))
            .execute();
        safeAwait(handoffReceived);

        // Always go through searchNode2 to ensure that we get the latest cluster state
        var clusterState = client(searchNode2).admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        var indexShardRoutingTable = clusterState.routingTable(ProjectId.DEFAULT).index(indexName).shard(0);
        var initializingUnpromotableShards = indexShardRoutingTable.assignedUnpromotableShards()
            .stream()
            .filter(ShardRouting::initializing)
            .toList();
        assertThat(
            "Unexpected number of initializing unpromotable shards " + indexShardRoutingTable.unpromotableShards(),
            initializingUnpromotableShards,
            hasSize(1)
        );
        assertThat(
            initializingUnpromotableShards.get(0).currentNodeId(),
            is(equalTo(clusterState.nodes().resolveNode(searchNode2).getId()))
        );
        // Ensure that the handoff is still waiting for the cluster state to align
        assertThat(handoffFinished.getCount(), is(equalTo(1L)));

        blockClusterStateProcessingDisruption.stopDisrupting();
        safeAwait(handoffFinished);
        assertAcked(updateIndexSettingsFuture.actionGet());
        ensureGreen(indexName);

        assertThatUnpromotableShardIsStartedInNode(indexName, searchNode2);
    }

    public void testUnpromotableRelocationHandoffFailsIfTargetAllocationIdChanges() throws Exception {
        startMasterAndIndexNode();
        var searchNode1 = startSearchNode();
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        var searchNode2 = startSearchNode();

        var startHandoffReceived = new CountDownLatch(1);
        var handoffBlocked = new CountDownLatch(1);
        var handoffFailedSent = new CountDownLatch(1);
        MockTransportService.getInstance(searchNode1)
            .addRequestHandlingBehavior(START_HANDOFF_ACTION_NAME, (handler, request, channel, task) -> {
                startHandoffReceived.countDown();
                safeAwait(handoffBlocked);
                handler.messageReceived(request, new TransportChannel() {
                    @Override
                    public String getProfileName() {
                        return channel.getProfileName();
                    }

                    @Override
                    public void sendResponse(TransportResponse response) {
                        assert false : "Unexpected response " + response;
                    }

                    @Override
                    public void sendResponse(Exception exception) {
                        assertThat(exception.getMessage(), containsString("Invalid relocation state:"));
                        channel.sendResponse(exception);
                        handoffFailedSent.countDown();
                    }
                }, task);
            });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNode1));
        safeAwait(startHandoffReceived);

        var clusterService = internalCluster().getInstance(ClusterService.class, searchNode1);
        var recoveringAllocationIdChangedLatch = new CountDownLatch(1);
        if (randomBoolean()) {
            clusterService.addListener(event -> {
                if (event.nodesRemoved()) {
                    recoveringAllocationIdChangedLatch.countDown();
                }
            });
        } else {
            clusterService.addListener(event -> {
                if (event.routingTableChanged()) {
                    recoveringAllocationIdChangedLatch.countDown();
                }
            });
            var indicesService = internalCluster().getInstance(IndicesService.class, searchNode2);
            var shard = indicesService.indexServiceSafe(resolveIndex(indexName)).getShard(0);
            shard.failShard("test", new RuntimeException("boom"));
        }

        internalCluster().stopNode(searchNode2);

        safeAwait(recoveringAllocationIdChangedLatch);
        handoffBlocked.countDown();
        safeAwait(handoffFailedSent);

        ensureGreen(indexName);
        assertThatUnpromotableShardIsStartedInNode(indexName, searchNode1);
    }

    public void testUnpromotableRelocationHandoffTimeoutDoesNotFailRelocation() {
        var nodeSettings = Settings.builder()
            .put(
                TransportStatelessUnpromotableRelocationAction.START_HANDOFF_REQUEST_TIMEOUT_SETTING.getKey(),
                TimeValue.timeValueSeconds(1)
            )
            .build();
        startMasterAndIndexNode(nodeSettings);
        var searchNode1 = startSearchNode(nodeSettings);
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        var searchNode2 = startSearchNode(nodeSettings);

        var startHandoffSent = new CountDownLatch(1);
        var handoffBlocked = new CountDownLatch(1);
        MockTransportService.getInstance(searchNode1)
            .addRequestHandlingBehavior(START_HANDOFF_ACTION_NAME, (handler, request, channel, task) -> {
                startHandoffSent.countDown();
                safeAwait(handoffBlocked);
                handler.messageReceived(request, channel, task);
            });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNode1));
        safeAwait(startHandoffSent);
        ensureGreen(indexName);
        assertThatUnpromotableShardIsStartedInNode(indexName, searchNode2);
        handoffBlocked.countDown();
    }

    public void testUnpromotableRelocationHandoffClusterStateConvergenceTimeoutDoesNotFailRelocation() throws Exception {
        var nodeSettings = Settings.builder()
            .put(
                TransportStatelessUnpromotableRelocationAction.START_HANDOFF_CLUSTER_STATE_CONVERGENCE_TIMEOUT_SETTING.getKey(),
                TimeValue.timeValueMillis(200)
            )
            .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .build();
        var indexNode = startMasterAndIndexNode(nodeSettings);
        var searchNode1 = startSearchNode(nodeSettings);
        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        var searchNode2 = startSearchNode(nodeSettings);
        ensureStableCluster(3);

        MockTransportService.getInstance(searchNode1)
            .addRequestHandlingBehavior(
                START_HANDOFF_ACTION_NAME,
                (handler, request, channel, task) -> handler.messageReceived(request, new TransportChannel() {
                    @Override
                    public String getProfileName() {
                        return channel.getProfileName();
                    }

                    @Override
                    public void sendResponse(TransportResponse response) {
                        assert false : "Unexpected response " + response;
                    }

                    @Override
                    public void sendResponse(Exception exception) {
                        assertThat(exception.getMessage(), containsString("Cluster state convergence timed out"));
                        channel.sendResponse(exception);
                    }
                }, task)
            );

        // We cannot use a regular cluster disruption such as SlowClusterStateProcessing because it blocks the applier thread
        // and the cluster state observer timeouts are scheduled through that thread, therefore they won't be executed until
        // we stop the disruption.
        var blockApplyCommitInSearchNode1Latch = new CountDownLatch(1);
        var applyCommitSentToSearchNode1Latch = new CountDownLatch(1);
        MockTransportService.getInstance(searchNode1)
            .addRequestHandlingBehavior(Coordinator.COMMIT_STATE_ACTION_NAME, (handler, request, channel, task) -> {
                applyCommitSentToSearchNode1Latch.countDown();
                safeAwait(blockApplyCommitInSearchNode1Latch);
                handler.messageReceived(request, channel, task);
            });

        // Since we're delaying the commit cluster state requests we should wait for the response later on once we unblock these requests
        var updateSettingsRequest = client(indexNode).admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNode1))
            .execute();

        ensureGreen(indexName);
        safeAwait(applyCommitSentToSearchNode1Latch);
        assertThatUnpromotableShardIsStartedInNode(indexName, searchNode2);
        blockApplyCommitInSearchNode1Latch.countDown();
        assertAcked(updateSettingsRequest.get());
    }

    private static void assertThatUnpromotableShardIsStartedInNode(String indexName, String nodeName) {
        // We fetch the cluster state from the master to avoid fetching an outdated state from a node that has a state commit block.
        var masterName = internalCluster().getMasterName();
        var clusterState = client(masterName).admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        var nodeId = clusterState.nodes().resolveNode(nodeName).getId();

        var unpromotableShards = clusterState.routingTable(ProjectId.DEFAULT).index(indexName).shard(0).unpromotableShards();
        assertThat(unpromotableShards, hasSize(1));
        var unpromotableShardRouting = unpromotableShards.get(0);

        assertThat(unpromotableShardRouting.started(), is(true));
        assertThat(unpromotableShardRouting.relocating(), is(false));
        assertThat(unpromotableShardRouting.currentNodeId(), is(equalTo(nodeId)));
    }
}
