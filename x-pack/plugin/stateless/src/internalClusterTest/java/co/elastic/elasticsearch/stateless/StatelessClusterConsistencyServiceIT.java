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

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.FollowersChecker;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class StatelessClusterConsistencyServiceIT extends AbstractStatelessIntegTestCase {

    public void testValidateClusterStateSuccessful() {
        startMasterOnlyNode();
        String indexNode = startIndexNode();
        ensureStableCluster(2);

        final StatelessClusterConsistencyService consistencyService = internalCluster().getInstance(
            StatelessClusterConsistencyService.class,
            indexNode
        );

        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        consistencyService.ensureClusterStateConsistentWithRootBlob(future, TimeValue.timeValueSeconds(30));
        future.actionGet();
    }

    public void testValidateClusterStateTimeout() {
        String masterNode = startMasterOnlyNode(
            Settings.builder()
                .put(FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
                .put(FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
                .build()
        );
        String indexNode = startIndexNode();
        ensureStableCluster(2);

        final MockTransportService indexNodeTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            indexNode
        );

        final ClusterService masterClusterService = internalCluster().getInstance(ClusterService.class, masterNode);
        final PlainActionFuture<Void> removedNode = new PlainActionFuture<>();
        masterClusterService.addListener(clusterChangedEvent -> {
            if (removedNode.isDone() == false && clusterChangedEvent.state().nodes().getDataNodes().isEmpty()) {
                removedNode.onResponse(null);
            }
        });

        indexNodeTransportService.addRequestHandlingBehavior(
            FollowersChecker.FOLLOWER_CHECK_ACTION_NAME,
            (handler, request, channel, task) -> {
                channel.sendResponse(new ElasticsearchException("simulated check failure"));
            }
        );

        removedNode.actionGet();

        final StatelessClusterConsistencyService consistencyService = internalCluster().getInstance(
            StatelessClusterConsistencyService.class,
            indexNode
        );

        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        consistencyService.ensureClusterStateConsistentWithRootBlob(future, TimeValue.timeValueMillis(100));

        expectThrows(ElasticsearchTimeoutException.class, future::actionGet);
    }

    public void testWaitForNextClusterStatePublish() {
        String masterNode = startMasterOnlyNode(
            Settings.builder()
                .put(FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
                .put(FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
                .build()
        );
        String indexNode = startIndexNode(
            Settings.builder()
                .put(DISCOVERY_FIND_PEERS_INTERVAL_SETTING.getKey(), "100ms")
                .put(LEADER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
                .put(LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
                .build()
        );
        ensureStableCluster(2);

        final MockTransportService indexNodeTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            indexNode
        );

        final ClusterService masterClusterService = internalCluster().getInstance(ClusterService.class, masterNode);
        final PlainActionFuture<Void> removedNode = new PlainActionFuture<>();
        final PlainActionFuture<Void> validationStarted = new PlainActionFuture<>();

        masterClusterService.addListener(clusterChangedEvent -> {
            if (removedNode.isDone() == false && clusterChangedEvent.state().nodes().getDataNodes().isEmpty()) {
                removedNode.onResponse(null);
            }
        });

        indexNodeTransportService.addRequestHandlingBehavior(
            FollowersChecker.FOLLOWER_CHECK_ACTION_NAME,
            (handler, request, channel, task) -> {
                if (validationStarted.isDone() == false) {
                    channel.sendResponse(new ElasticsearchException("simulated check failure"));
                } else {
                    handler.messageReceived(request, channel, task);
                }
            }
        );

        removedNode.actionGet();

        final StatelessClusterConsistencyService consistencyService = internalCluster().getInstance(
            StatelessClusterConsistencyService.class,
            indexNode
        );

        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        consistencyService.ensureClusterStateConsistentWithRootBlob(future, TimeValue.timeValueSeconds(30));

        validationStarted.onResponse(null);

        future.actionGet();

        ClusterState dataNodeState = consistencyService.state();
        ClusterState masterNodeState = masterClusterService.state();
        assertThat(dataNodeState.nodes().getNodeLeftGeneration(), equalTo(masterNodeState.nodes().getNodeLeftGeneration()));
    }

}
