/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.CcrIntegTestCase;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.indexlifecycle.ExplainLifecycleRequest;
import org.elasticsearch.xpack.core.indexlifecycle.ExplainLifecycleResponse;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleExplainResponse;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Phase;
import org.elasticsearch.xpack.core.indexlifecycle.StartILMRequest;
import org.elasticsearch.xpack.core.indexlifecycle.StopILMRequest;
import org.elasticsearch.xpack.core.indexlifecycle.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.PutLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.StartILMAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.StopILMAction;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycle;
import org.junit.Before;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.ccr.CcrSettings.CCR_FOLLOWING_INDEX_SETTING;

public class CcrIndexLifecycleIT extends CcrIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(IndexLifecycle.class))
            .collect(Collectors.toList());
    }

    @Before
    public void setLowPollInterval() throws ExecutionException, InterruptedException {
        Settings lowPollInterval = Settings.builder()
            .put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), "1000ms")
            .build();
        leaderClient().admin().cluster().updateSettings(new ClusterUpdateSettingsRequest().persistentSettings(lowPollInterval)).get();
        followerClient().admin().cluster().updateSettings(new ClusterUpdateSettingsRequest().persistentSettings(lowPollInterval)).get();
    }

    public void testILMUnfollowFailsToRemoveRetentionLeases() throws Exception {
        final String leaderIndex = "leader";
        final String followerIndex = "follower";
        final String policyName = "unfollow_only_policy";
        final int numberOfShards = randomIntBetween(1, 4);
        final Map<String, String> indexSettings = new HashMap<>();
        indexSettings.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), "true");
        indexSettings.put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), policyName);
        final String leaderIndexSettings =
            getIndexSettings(numberOfShards, 0, indexSettings);
        assertAcked(leaderClient().admin().indices().prepareCreate(leaderIndex).setSource(leaderIndexSettings, XContentType.JSON).get());

        // Put a policy on the follower
        Map<String, LifecycleAction> hotActions = new HashMap<>();
        hotActions.put(org.elasticsearch.xpack.core.indexlifecycle.UnfollowAction.NAME,
            new org.elasticsearch.xpack.core.indexlifecycle.UnfollowAction());
        Map<String, Phase> phases = new HashMap<>();
        phases.put("hot", new Phase("hot", TimeValue.ZERO, hotActions));
        LifecyclePolicy policy = new LifecyclePolicy(policyName, phases);

        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(policy);
        assertAcked(followerClient().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get());


        // Set up the follower
        final PutFollowAction.Request followRequest = putFollow(leaderIndex, followerIndex);
        followerClient().execute(PutFollowAction.INSTANCE, followRequest).get();
        ensureFollowerGreen(true, followerIndex);


        // Pause ILM so that this policy doesn't proceed until we want it to
        assertAcked(followerClient().execute(StopILMAction.INSTANCE, new StopILMRequest()).get());

        // Set indexing complete and wait for it to be replicated
        assertAcked(leaderClient().admin().indices().prepareUpdateSettings(leaderIndex)
            .setSettings(Settings.builder().put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING.getKey(), "true"))
            .get());
        assertBusy(() -> assertEquals("true",
            followerClient().admin().indices()
                .getIndex(new GetIndexRequest().indices(followerIndex)).get()
                .getSetting(followerIndex, LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING.getKey())));

        // Disrupt requests to remove retention leases for these random shards
        final Set<Integer> shardIds =
            new HashSet<>(randomSubsetOf(
                randomIntBetween(1, numberOfShards),
                IntStream.range(0, numberOfShards).boxed().collect(Collectors.toSet())));

        final ClusterStateResponse followerClusterState = followerClient().admin().cluster().prepareState().clear().setNodes(true).get();
        try {
            for (final ObjectCursor<DiscoveryNode> senderNode : followerClusterState.getState().nodes().getNodes().values()) {
                final MockTransportService senderTransportService =
                    (MockTransportService) getFollowerCluster().getInstance(TransportService.class, senderNode.value.getName());
                final ClusterStateResponse leaderClusterState =
                    leaderClient().admin().cluster().prepareState().clear().setNodes(true).get();
                for (final ObjectCursor<DiscoveryNode> receiverNode : leaderClusterState.getState().nodes().getNodes().values()) {
                    final MockTransportService receiverTransportService =
                        (MockTransportService) getLeaderCluster().getInstance(TransportService.class, receiverNode.value.getName());
                    senderTransportService.addSendBehavior(receiverTransportService,
                        (connection, requestId, action, request, options) -> {
                            if (RetentionLeaseActions.Remove.ACTION_NAME.equals(action)) {
                                final RetentionLeaseActions.RemoveRequest removeRequest = (RetentionLeaseActions.RemoveRequest) request;
                                if (shardIds.contains(removeRequest.getShardId().id())) {
                                    throw randomBoolean()
                                        ? new ConnectTransportException(receiverNode.value, "connection failed")
                                        : new IndexShardClosedException(removeRequest.getShardId());
                                }
                            }
                            connection.sendRequest(requestId, action, request, options);
                        });
                }
            }

            // Start ILM back up and let it unfollow
            followerClient().execute(StartILMAction.INSTANCE, new StartILMRequest());

            // Wait for the policy to be complete
            assertBusy(() -> {
                ExplainLifecycleResponse explainResponse = followerClient().execute(ExplainLifecycleAction.INSTANCE,
                    new ExplainLifecycleRequest().indices(followerIndex)).get();
                IndexLifecycleExplainResponse indexResponse = explainResponse.getIndexResponses().get(followerIndex);
                assertEquals("completed", indexResponse.getPhase());
                assertEquals("completed", indexResponse.getAction());
                assertEquals("completed", indexResponse.getStep());
            });


            // Ensure the "follower" index has successfully unfollowed
            assertBusy(() -> assertNull(followerClient().admin().indices()
                .getIndex(new GetIndexRequest().indices(followerIndex)).get()
                .getSetting(followerIndex, CCR_FOLLOWING_INDEX_SETTING.getKey())));

        } finally {
            for (final ObjectCursor<DiscoveryNode> senderNode : followerClusterState.getState().nodes().getDataNodes().values()) {
                final MockTransportService senderTransportService =
                    (MockTransportService) getFollowerCluster().getInstance(TransportService.class, senderNode.value.getName());
                senderTransportService.clearAllRules();
            }
        }
    }
}
