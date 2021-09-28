/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.capacity.memory;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicyMetadata;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AutoscalingMemoryInfoServiceTests extends AutoscalingTestCase {

    private NodeStatsClient client;
    private AutoscalingMemoryInfoService service;
    private TimeValue fetchTimeout;
    private AutoscalingMetadata autoscalingMetadata;
    private Metadata metadata;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = new NodeStatsClient();
        final ClusterService clusterService = mock(ClusterService.class);
        Settings settings;
        if (randomBoolean()) {
            fetchTimeout = TimeValue.timeValueSeconds(15);
            settings = Settings.EMPTY;
        } else {
            fetchTimeout = TimeValue.timeValueMillis(randomLongBetween(1, 10000));
            settings = Settings.builder().put(AutoscalingMemoryInfoService.FETCH_TIMEOUT.getKey(), fetchTimeout).build();
        }
        when(clusterService.getSettings()).thenReturn(settings);
        Set<Setting<?>> settingsSet = Sets.union(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
            Set.of(AutoscalingMemoryInfoService.FETCH_TIMEOUT)
        );
        ClusterSettings clusterSettings = new ClusterSettings(settings, settingsSet);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        service = new AutoscalingMemoryInfoService(clusterService, client);
        autoscalingMetadata = randomAutoscalingMetadataOfPolicyCount(between(1, 8));
        metadata = Metadata.builder().putCustom(AutoscalingMetadata.NAME, autoscalingMetadata).build();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        client.close();
    }

    public void testAddRemoveNode() {
        if (randomBoolean()) {
            service.onClusterChanged(new ClusterChangedEvent("test", ClusterState.EMPTY_STATE, ClusterState.EMPTY_STATE));
        }
        ClusterState previousState = ClusterState.EMPTY_STATE;
        Set<DiscoveryNode> previousNodes = new HashSet<>();
        Set<DiscoveryNode> previousSucceededNodes = new HashSet<>();
        for (int i = 0; i < 5; ++i) {
            Set<DiscoveryNode> newNodes = IntStream.range(0, between(1, 10))
                .mapToObj(n -> newNode("test_" + n))
                .collect(Collectors.toSet());
            Set<DiscoveryNode> nodes = Sets.union(newNodes, new HashSet<>(randomSubsetOf(previousNodes)));
            ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
                .metadata(metadata)
                .nodes(discoveryNodesBuilder(nodes, true))
                .build();
            Set<DiscoveryNode> missingNodes = Sets.difference(nodes, previousSucceededNodes);
            Set<DiscoveryNode> failingNodes = new HashSet<>(randomSubsetOf(missingNodes));
            Set<DiscoveryNode> succeedingNodes = Sets.difference(missingNodes, failingNodes);
            List<FailedNodeException> failures = failingNodes.stream()
                .map(node -> new FailedNodeException(node.getId(), randomAlphaOfLength(10), new Exception()))
                .collect(Collectors.toList());
            NodesStatsResponse response = new NodesStatsResponse(
                ClusterName.DEFAULT,
                succeedingNodes.stream()
                    .map(n -> statsForNode(n, randomLongBetween(0, Long.MAX_VALUE / 1000)))
                    .collect(Collectors.toList()),
                failures
            );
            client.respond(response, () -> {
                Sets.union(missingNodes, Sets.difference(previousNodes, nodes))
                    .forEach(n -> { assertThat(service.snapshot().get(n), nullValue()); });
                Sets.intersection(previousSucceededNodes, nodes).forEach(n -> assertThat(service.snapshot().get(n), notNullValue()));
            });

            service.onClusterChanged(new ClusterChangedEvent("test", state, previousState));
            client.assertNoResponder();

            assertMatchesResponse(succeedingNodes, response);
            failingNodes.forEach(n -> { assertThat(service.snapshot().get(n), nullValue()); });

            previousNodes.clear();
            previousNodes.addAll(nodes);
            previousSucceededNodes.retainAll(nodes);
            previousSucceededNodes.addAll(succeedingNodes);
            previousState = state;
        }
    }

    public void testNotMaster() {
        Set<DiscoveryNode> nodes = IntStream.range(0, between(1, 10)).mapToObj(n -> newNode("test_" + n)).collect(Collectors.toSet());
        DiscoveryNodes.Builder nodesBuilder = discoveryNodesBuilder(nodes, false);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).nodes(nodesBuilder).metadata(metadata).build();

        // client throws if called.
        service.onClusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));

        nodes.forEach(n -> assertThat(service.snapshot().get(n), nullValue()));
    }

    public void testNoLongerMaster() {
        Set<DiscoveryNode> nodes = IntStream.range(0, between(1, 10)).mapToObj(n -> newNode("test_" + n)).collect(Collectors.toSet());
        ClusterState masterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodesBuilder(nodes, true))
            .metadata(metadata)
            .build();
        NodesStatsResponse response = new NodesStatsResponse(
            ClusterName.DEFAULT,
            nodes.stream().map(n -> statsForNode(n, randomLongBetween(0, Long.MAX_VALUE / 1000))).collect(Collectors.toList()),
            List.of()
        );

        client.respond(response, () -> {});
        service.onClusterChanged(new ClusterChangedEvent("test", masterState, ClusterState.EMPTY_STATE));
        client.assertNoResponder();

        assertMatchesResponse(nodes, response);

        ClusterState notMasterState = ClusterState.builder(masterState)
            .nodes(DiscoveryNodes.builder(masterState.nodes()).masterNodeId(null))
            .build();

        // client throws if called.
        service.onClusterChanged(new ClusterChangedEvent("test", notMasterState, masterState));

        nodes.forEach(n -> assertThat(service.snapshot().get(n), nullValue()));
    }

    public void testFails() {
        Set<DiscoveryNode> nodes = IntStream.range(0, between(1, 10)).mapToObj(n -> newNode("test_" + n)).collect(Collectors.toSet());
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).nodes(discoveryNodesBuilder(nodes, true)).metadata(metadata).build();

        client.respond((r, listener) -> listener.onFailure(randomFrom(new IllegalStateException(), new RejectedExecutionException())));
        service.onClusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));

        nodes.forEach(n -> assertThat(service.snapshot().get(n), nullValue()));

        NodesStatsResponse response = new NodesStatsResponse(
            ClusterName.DEFAULT,
            nodes.stream().map(n -> statsForNode(n, randomLongBetween(0, Long.MAX_VALUE / 1000))).collect(Collectors.toList()),
            List.of()
        );

        // implicit retry on cluster state update.
        client.respond(response, () -> {});
        service.onClusterChanged(new ClusterChangedEvent("test", state, state));
        client.assertNoResponder();
    }

    public void testRestartNode() {
        Set<DiscoveryNode> nodes = IntStream.range(0, between(1, 10)).mapToObj(n -> newNode("test_" + n)).collect(Collectors.toSet());
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).nodes(discoveryNodesBuilder(nodes, true)).metadata(metadata).build();

        NodesStatsResponse response = new NodesStatsResponse(
            ClusterName.DEFAULT,
            nodes.stream().map(n -> statsForNode(n, randomLongBetween(0, Long.MAX_VALUE / 1000))).collect(Collectors.toList()),
            List.of()
        );

        client.respond(response, () -> {});
        service.onClusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        client.assertNoResponder();

        assertMatchesResponse(nodes, response);

        Set<DiscoveryNode> restartedNodes = randomValueOtherThan(
            nodes,
            () -> nodes.stream().map(n -> randomBoolean() ? restartNode(n) : n).collect(Collectors.toSet())
        );

        ClusterState restartedState = ClusterState.builder(state).nodes(discoveryNodesBuilder(restartedNodes, true)).build();

        NodesStatsResponse restartedStatsResponse = new NodesStatsResponse(
            ClusterName.DEFAULT,
            Sets.difference(restartedNodes, nodes)
                .stream()
                .map(n -> statsForNode(n, randomLongBetween(0, Long.MAX_VALUE / 1000)))
                .collect(Collectors.toList()),
            List.of()
        );

        client.respond(restartedStatsResponse, () -> {});
        service.onClusterChanged(new ClusterChangedEvent("test", restartedState, state));
        client.assertNoResponder();

        assertMatchesResponse(Sets.intersection(restartedNodes, nodes), response);
        assertMatchesResponse(Sets.difference(restartedNodes, nodes), restartedStatsResponse);

        Sets.difference(nodes, restartedNodes).forEach(n -> assertThat(service.snapshot().get(n), nullValue()));
    }

    public void testConcurrentStateUpdate() throws Exception {
        Set<DiscoveryNode> nodes = IntStream.range(0, between(1, 10)).mapToObj(n -> newNode("test_" + n)).collect(Collectors.toSet());
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).nodes(discoveryNodesBuilder(nodes, true)).metadata(metadata).build();

        NodesStatsResponse response = new NodesStatsResponse(
            ClusterName.DEFAULT,
            nodes.stream().map(n -> statsForNode(n, randomLongBetween(0, Long.MAX_VALUE / 1000))).collect(Collectors.toList()),
            List.of()
        );

        List<Thread> threads = new ArrayList<>();
        client.respond((request, listener) -> {
            CountDownLatch latch = new CountDownLatch(1);
            threads.add(startThread(() -> {
                try {
                    assertThat(latch.await(10, TimeUnit.SECONDS), is(true));
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                listener.onResponse(response);
            }));
            threads.add(startThread(() -> {
                // we do not register a new responder, so this will fail if it calls anything on client.
                service.onClusterChanged(new ClusterChangedEvent("test_concurrent", state, state));
                latch.countDown();
            }));
        });
        service.onClusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        client.assertNoResponder();
        for (Thread thread : threads) {
            thread.join(10000);
        }

        threads.forEach(t -> assertThat(t.isAlive(), is(false)));
    }

    public void testRelevantNodes() {
        Set<DiscoveryNode> nodes = IntStream.range(0, between(1, 10)).mapToObj(n -> newNode("test_" + n)).collect(Collectors.toSet());
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).nodes(discoveryNodesBuilder(nodes, true)).metadata(metadata).build();
        Set<DiscoveryNode> relevantNodes = service.relevantNodes(state);
        assertThat(relevantNodes, equalTo(nodes));
    }

    private DiscoveryNodes.Builder discoveryNodesBuilder(Set<DiscoveryNode> nodes, boolean master) {
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        String masterNodeId = randomAlphaOfLength(10);
        nodesBuilder.localNodeId(masterNodeId);
        nodesBuilder.masterNodeId(master ? masterNodeId : null);
        nodes.forEach(nodesBuilder::add);
        addIrrelevantNodes(nodesBuilder);
        return nodesBuilder;
    }

    /**
     * Add irrelevant nodes. NodeStatsClient will validate that they are not asked for.
     */
    private void addIrrelevantNodes(DiscoveryNodes.Builder nodesBuilder) {
        Set<Set<String>> relevantRoleSets = autoscalingMetadata.policies()
            .values()
            .stream()
            .map(AutoscalingPolicyMetadata::policy)
            .map(AutoscalingPolicy::roles)
            .collect(Collectors.toSet());

        IntStream.range(0, 5).mapToObj(i -> newNode("irrelevant_" + i, randomIrrelevantRoles(relevantRoleSets))).forEach(nodesBuilder::add);
    }

    private Set<DiscoveryNodeRole> randomIrrelevantRoles(Set<Set<String>> relevantRoleSets) {
        return randomValueOtherThanMany(relevantRoleSets::contains, AutoscalingTestCase::randomRoles).stream()
            .map(DiscoveryNodeRole::getRoleFromRoleName)
            .collect(Collectors.toSet());
    }

    public void assertMatchesResponse(Set<DiscoveryNode> nodes, NodesStatsResponse response) {
        nodes.forEach(
            n -> {
                assertThat(
                    service.snapshot().get(n),
                    equalTo(response.getNodesMap().get(n.getId()).getOs().getMem().getTotal().getBytes())
                );
            }
        );
    }

    private Thread startThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.start();
        return thread;
    }

    private static NodeStats statsForNode(DiscoveryNode node, long memory) {
        OsStats osStats = new OsStats(
            randomNonNegativeLong(),
            new OsStats.Cpu(randomShort(), null),
            new OsStats.Mem(memory, randomLongBetween(0, memory)),
            new OsStats.Swap(randomNonNegativeLong(), randomNonNegativeLong()),
            null
        );
        return new NodeStats(
            node,
            randomNonNegativeLong(),
            null,
            osStats,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }

    private class NodeStatsClient extends NoOpClient {
        private BiConsumer<NodesStatsRequest, ActionListener<NodesStatsResponse>> responder;

        private NodeStatsClient() {
            super(getTestName());
        }

        public void respond(NodesStatsResponse response, Runnable whileFetching) {
            respond((request, listener) -> {
                assertThat(
                    Set.of(request.nodesIds()),
                    Matchers.equalTo(
                        Stream.concat(
                            response.getNodesMap().keySet().stream(),
                            response.failures().stream().map(FailedNodeException::nodeId)
                        ).collect(Collectors.toSet())
                    )
                );
                whileFetching.run();
                listener.onResponse(response);
            });
        }

        public void respond(BiConsumer<NodesStatsRequest, ActionListener<NodesStatsResponse>> responder) {
            assertThat(responder, notNullValue());
            this.responder = responder;
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertThat(action, Matchers.sameInstance(NodesStatsAction.INSTANCE));
            NodesStatsRequest nodesStatsRequest = (NodesStatsRequest) request;
            assertThat(nodesStatsRequest.timeout(), equalTo(fetchTimeout));
            assertThat(responder, notNullValue());
            BiConsumer<NodesStatsRequest, ActionListener<NodesStatsResponse>> responder = this.responder;
            this.responder = null;
            @SuppressWarnings("unchecked")
            ActionListener<NodesStatsResponse> statsListener = (ActionListener<NodesStatsResponse>) listener;
            responder.accept(nodesStatsRequest, statsListener);
        }

        public void assertNoResponder() {
            assertThat(responder, nullValue());
        }
    }

    private DiscoveryNode newNode(String nodeName) {
        return newNode(
            nodeName,
            randomFrom(autoscalingMetadata.policies().values()).policy()
                .roles()
                .stream()
                .map(DiscoveryNodeRole::getRoleFromRoleName)
                .collect(Collectors.toSet())
        );
    }

    private DiscoveryNode newNode(String nodeName, Set<DiscoveryNodeRole> roles) {
        return new DiscoveryNode(nodeName, UUIDs.randomBase64UUID(), buildNewFakeTransportAddress(), Map.of(), roles, Version.CURRENT);
    }

    private DiscoveryNode restartNode(DiscoveryNode node) {
        return new DiscoveryNode(node.getName(), node.getId(), node.getAddress(), node.getAttributes(), node.getRoles(), node.getVersion());
    }
}
