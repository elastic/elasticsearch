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

package co.elastic.elasticsearch.stateless.cluster.coordination;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;

import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING;
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class TransportConsistentClusterStateReadIT extends AbstractStatelessIntegTestCase {

    public void testConsistentClusterStateRead() throws Exception {
        var masterNode = startMasterOnlyNode();
        var indexNode = startIndexNode();
        ensureStableCluster(2);

        var currentState = client().admin().cluster().prepareState().get().getState();

        var client = randomBoolean() ? client(masterNode) : client(indexNode);

        var response = client.execute(
            TransportConsistentClusterStateReadAction.TYPE,
            new TransportConsistentClusterStateReadAction.Request()
        ).get();

        var consistentState = response.getState();

        assertThat(consistentState.term(), equalTo(currentState.term()));
        assertThat(consistentState.getVersion(), equalTo(currentState.getVersion()));
    }

    public void testReturnsErrorWhenNoMasterIsFound() throws Exception {
        var masterNode = startMasterOnlyNode(
            Settings.builder()
                // Add enough follower-check opportunities to let the elected master think that the node is still part of the cluster
                .put(FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "10s")
                .put(FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), "2")
                .build()
        );

        var indexNode = startIndexNode(
            Settings.builder()
                .put(LEADER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
                .put(LEADER_CHECK_TIMEOUT_SETTING.getKey(), "500ms")
                .put(LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
                .build()
        );
        ensureStableCluster(2);

        var disruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(Set.of(indexNode), Set.of(masterNode)),
            NetworkDisruption.UNRESPONSIVE
        );
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();

        // Wait until the index node decides that the master is not reachable anymore
        assertBusy(() -> {
            var currentState = internalCluster().getInstance(ClusterService.class, indexNode).state();
            assertThat(currentState.nodes().getMasterNodeId(), is(nullValue()));
        });

        var consistentReadFuture = client(indexNode).execute(
            TransportConsistentClusterStateReadAction.TYPE,
            new TransportConsistentClusterStateReadAction.Request()
        );

        var exception = expectThrows(Exception.class, consistentReadFuture::get);
        assertThat(exception.getCause(), is(notNullValue()));
        assertThat(exception.getCause(), is(instanceOf(MasterNotDiscoveredException.class)));
    }

    public void testWaitsUntilTheNodeRejoinsTheClusterToReturnAValidClusterState() throws Exception {
        var masterNode = startMasterOnlyNode(
            Settings.builder()
                .put(FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
                .put(LEADER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
                .put(FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
                .put(DISCOVERY_FIND_PEERS_INTERVAL_SETTING.getKey(), "100ms")
                .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
                .put(TransportSettings.CONNECT_TIMEOUT.getKey(), "5s")
                .build()
        );

        var indexNode = startIndexNode();
        ensureStableCluster(2);

        var indexNodeId = client().admin().cluster().prepareState().get().getState().nodes().resolveNode(indexNode).getId();

        var disruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(Set.of(indexNode), Set.of(masterNode)),
            NetworkDisruption.UNRESPONSIVE
        );
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();

        var indexName = randomIdentifier();
        // Trigger a cluster state update
        client(masterNode).admin().indices().prepareCreate(indexName).setWaitForActiveShards(0).get();

        if (randomBoolean()) {
            // Wait until the index-node is removed by the master node; notice that the leader check timeout uses the default, meaning that
            // the index node will consider that it can reach the master node
            assertBusy(() -> {
                var currentState = internalCluster().getInstance(ClusterService.class, masterNode).state();
                assertThat(currentState.nodes().get(indexNodeId), is(nullValue()));
            });
        }

        var consistentReadFuture = client(indexNode).execute(
            TransportConsistentClusterStateReadAction.TYPE,
            new TransportConsistentClusterStateReadAction.Request()
        );

        var indexNodeState = internalCluster().getInstance(ClusterService.class, indexNode).state();
        assertThat(indexNodeState.metadata().hasIndex(indexName), is(false));
        assertThat(consistentReadFuture.isDone(), is(equalTo(false)));

        disruption.stopDisrupting();
        internalCluster().clearDisruptionScheme();

        var consistentReadResponse = consistentReadFuture.get();
        var consistentReadResponseState = consistentReadResponse.getState();
        assertThat(consistentReadResponseState.metadata().hasIndex(indexName), is(true));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-serverless/issues/1734")
    public void testConsistentClusterStateReadIsNotReturnedUntilLastStateIsAppliedLocally() throws Exception {
        var masterNode = startMasterOnlyNode(Settings.builder().put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s").build());

        var indexNode = startIndexNode();
        ensureStableCluster(2);

        var disruption = new BlockClusterStateProcessing(indexNode, random());
        ;
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();

        // Trigger a cluster state update
        var indexName = randomIdentifier();
        client(masterNode).admin().indices().prepareCreate(indexName).setWaitForActiveShards(0).get();

        var consistentReadFuture = client(indexNode).execute(
            TransportConsistentClusterStateReadAction.TYPE,
            new TransportConsistentClusterStateReadAction.Request()
        );

        var indexNodeState = internalCluster().getInstance(ClusterService.class, indexNode).state();
        assertThat(indexNodeState.metadata().hasIndex(indexName), is(false));

        assertThat(consistentReadFuture.isDone(), is(equalTo(false)));

        disruption.stopDisrupting();
        internalCluster().clearDisruptionScheme();

        var consistentReadResponse = consistentReadFuture.get();
        var consistentReadResponseState = consistentReadResponse.getState();
        assertThat(consistentReadResponseState.metadata().hasIndex(indexName), is(true));
    }

    public void testCancellation() throws Exception {
        var masterNode = startMasterOnlyNode();

        var indexNode = startIndexNode();
        ensureStableCluster(2);

        var transport = (MockTransportService) internalCluster().getCurrentMasterNodeInstance(TransportService.class);
        AtomicReference<CheckedRunnable<Exception>> pendingReadMasterTermVersionRequest = new AtomicReference<>();
        transport.addRequestHandlingBehavior(
            TransportConsistentClusterStateReadAction.MASTER_NODE_ACTION,
            (handler, request, channel, task) -> pendingReadMasterTermVersionRequest.set(
                () -> handler.messageReceived(request, channel, task)
            )
        );

        var consistentReadFuture = client(indexNode).execute(
            TransportConsistentClusterStateReadAction.TYPE,
            new TransportConsistentClusterStateReadAction.Request()
        );

        assertThat(consistentReadFuture.isDone(), is(equalTo(false)));
        assertBusy(() -> assertThat(pendingReadMasterTermVersionRequest.get(), is(notNullValue())));

        var listTasksResponse = client(indexNode).admin()
            .cluster()
            .prepareListTasks()
            .setActions(TransportConsistentClusterStateReadAction.NAME)
            .get();
        assertThat(listTasksResponse.getTasks(), hasSize(1));
        var taskInfo = listTasksResponse.getTasks().get(0);

        var cancelTasksResponse = client(masterNode).admin().cluster().prepareCancelTasks().setTargetTaskId(taskInfo.taskId()).get();
        assertThat(cancelTasksResponse.getTasks(), hasSize(1));
        assertThat(cancelTasksResponse.getTasks().get(0).taskId(), equalTo(taskInfo.taskId()));

        pendingReadMasterTermVersionRequest.get().run();

        var exception = expectThrows(Exception.class, consistentReadFuture::get);
        assertThat(exception.getCause(), is(notNullValue()));
        assertThat(exception.getCause(), is(instanceOf(TaskCancelledException.class)));
    }
}
