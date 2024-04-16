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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.FollowersChecker;
import org.elasticsearch.cluster.coordination.JoinValidationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

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

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        consistencyService.ensureClusterStateConsistentWithRootBlob(future, TimeValue.timeValueSeconds(30));
        future.actionGet();
    }

    public void testValidateClusterStateHandlesContention() throws Exception {
        startMasterOnlyNode();
        String indexNode = startIndexNode();
        ensureStableCluster(2);

        final StatelessClusterConsistencyService consistencyService = internalCluster().getInstance(
            StatelessClusterConsistencyService.class,
            indexNode
        );

        AtomicReference<Exception> unexpectedException = new AtomicReference<>();

        int nThreads = randomIntBetween(5, 15);
        CountDownLatch latch = new CountDownLatch(nThreads);
        for (int i = 0; i < nThreads; ++i) {
            new Thread(() -> {
                PlainActionFuture<Void> future = new PlainActionFuture<>();
                consistencyService.ensureClusterStateConsistentWithRootBlob(future, TimeValue.timeValueSeconds(30));
                try {
                    future.actionGet();
                } catch (Exception e) {
                    unexpectedException.set(e);
                }
                latch.countDown();
            }).start();
        }
        safeAwait(latch);
        if (unexpectedException.get() != null) {
            throw unexpectedException.get();
        }
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

        final MockTransportService indexNodeTransportService = MockTransportService.getInstance(indexNode);

        final ClusterService masterClusterService = internalCluster().getInstance(ClusterService.class, masterNode);
        final PlainActionFuture<Void> removedNode = new PlainActionFuture<>();
        masterClusterService.addListener(clusterChangedEvent -> {
            if (removedNode.isDone() == false && clusterChangedEvent.state().nodes().getDataNodes().isEmpty()) {
                removedNode.onResponse(null);
            }
        });
        try {
            indexNodeTransportService.addRequestHandlingBehavior(
                FollowersChecker.FOLLOWER_CHECK_ACTION_NAME,
                (handler, request, channel, task) -> {
                    channel.sendResponse(new ElasticsearchException("simulated check failure"));
                }
            );

            indexNodeTransportService.addRequestHandlingBehavior(
                JoinValidationService.JOIN_VALIDATE_ACTION_NAME,
                (handler, request, channel, task) -> {
                    channel.sendResponse(new ElasticsearchException("simulated join validation failure"));
                }
            );

            removedNode.actionGet();

            final StatelessClusterConsistencyService consistencyService = internalCluster().getInstance(
                StatelessClusterConsistencyService.class,
                indexNode
            );

            PlainActionFuture<Void> future = new PlainActionFuture<>();
            consistencyService.ensureClusterStateConsistentWithRootBlob(future, TimeValue.timeValueMillis(100));
            expectThrows(ElasticsearchTimeoutException.class, future::actionGet);
        } finally {
            indexNodeTransportService.clearAllRules();
        }
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

        final ClusterService masterClusterService = internalCluster().getInstance(ClusterService.class, masterNode);
        final PlainActionFuture<Void> removedNode = new PlainActionFuture<>();
        final PlainActionFuture<Void> validationStarted = new PlainActionFuture<>();

        masterClusterService.addListener(clusterChangedEvent -> {
            if (removedNode.isDone() == false && clusterChangedEvent.state().nodes().getDataNodes().isEmpty()) {
                removedNode.onResponse(null);
            }
        });

        MockTransportService.getInstance(indexNode)
            .addRequestHandlingBehavior(FollowersChecker.FOLLOWER_CHECK_ACTION_NAME, (handler, request, channel, task) -> {
                if (validationStarted.isDone() == false) {
                    channel.sendResponse(new ElasticsearchException("simulated check failure"));
                } else {
                    handler.messageReceived(request, channel, task);
                }
            });

        removedNode.actionGet();

        final StatelessClusterConsistencyService consistencyService = internalCluster().getInstance(
            StatelessClusterConsistencyService.class,
            indexNode
        );

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        consistencyService.ensureClusterStateConsistentWithRootBlob(future, TimeValue.timeValueSeconds(30));

        validationStarted.onResponse(null);

        future.actionGet();

        ClusterState dataNodeState = consistencyService.state();
        ClusterState masterNodeState = masterClusterService.state();
        assertThat(dataNodeState.nodes().getNodeLeftGeneration(), equalTo(masterNodeState.nodes().getNodeLeftGeneration()));
    }

    public void testDelayedChecksCompleteSuccessfullyByImmediateCheck() {
        startMasterOnlyNode();
        String indexNode = startIndexNode(
            // Specifically set a very high value to make sure the background monitor doesn't interfere
            Settings.builder().put("stateless.translog.delayed_cluster_consistency_check.interval", "1m").build()
        );
        ensureStableCluster(2);

        var consistencyService = internalCluster().getInstance(StatelessClusterConsistencyService.class, indexNode);
        int count = randomIntBetween(1, 8);
        var delayedChecksCompleted = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            consistencyService.delayedEnsureClusterStateConsistentWithRootBlob(ActionListener.running(delayedChecksCompleted::countDown));
        }

        var immediateCheckListener = new SubscribableListener<Void>();
        consistencyService.ensureClusterStateConsistentWithRootBlob(immediateCheckListener, TimeValue.MAX_VALUE);

        safeAwait(immediateCheckListener);
        safeAwait(delayedChecksCompleted);
    }

    public void testDelayedChecksCompleteSuccessfullyByBackgroundMonitor() {
        startMasterOnlyNode();
        String indexNode = startIndexNode(
            Settings.builder().put("stateless.translog.delayed_cluster_consistency_check.interval", "100ms").build()
        );
        ensureStableCluster(2);

        var consistencyService = internalCluster().getInstance(StatelessClusterConsistencyService.class, indexNode);
        int count = randomIntBetween(1, 8);
        var delayedChecksCompleted = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            consistencyService.delayedEnsureClusterStateConsistentWithRootBlob(ActionListener.running(delayedChecksCompleted::countDown));
        }

        safeAwait(delayedChecksCompleted);
    }
}
