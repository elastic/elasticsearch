/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.capacity.nodeinfo;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
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
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.os.OsInfo;
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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.autoscaling.capacity.nodeinfo.AutoscalingNodeInfoService.FETCH_TIMEOUT;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AutoscalingNodesInfoServiceTests extends AutoscalingTestCase {

    private NodeStatsClient client;
    private AutoscalingNodeInfoService service;
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
            settings = Settings.builder().put(FETCH_TIMEOUT.getKey(), fetchTimeout).build();
        }
        when(clusterService.getSettings()).thenReturn(settings);
        Set<Setting<?>> settingsSet = Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(FETCH_TIMEOUT));
        ClusterSettings clusterSettings = new ClusterSettings(settings, settingsSet);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        service = new AutoscalingNodeInfoService(clusterService, client);
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
            NodesInfoResponse responseInfo = new NodesInfoResponse(
                ClusterName.DEFAULT,
                succeedingNodes.stream().map(n -> infoForNode(n, randomIntBetween(1, 64))).collect(Collectors.toList()),
                List.of()
            );
            client.respondStats(response, () -> {
                Sets.union(missingNodes, Sets.difference(previousNodes, nodes))
                    .forEach(n -> assertThat(service.snapshot().get(n).isEmpty(), is(true)));
                Sets.intersection(previousSucceededNodes, nodes).forEach(n -> assertThat(service.snapshot().get(n).isPresent(), is(true)));
            });
            client.respondInfo(responseInfo, () -> {

            });

            service.onClusterChanged(new ClusterChangedEvent("test", state, previousState));
            client.assertNoResponder();

            assertMatchesResponse(succeedingNodes, response, responseInfo);
            failingNodes.forEach(n -> assertThat(service.snapshot().get(n).isEmpty(), is(true)));

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

        nodes.forEach(n -> assertThat(service.snapshot().get(n).isEmpty(), is(true)));
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
        NodesInfoResponse responseInfo = new NodesInfoResponse(
            ClusterName.DEFAULT,
            nodes.stream().map(n -> infoForNode(n, randomIntBetween(1, 64))).collect(Collectors.toList()),
            List.of()
        );

        client.respondStats(response, () -> {});
        client.respondInfo(responseInfo, () -> {});
        service.onClusterChanged(new ClusterChangedEvent("test", masterState, ClusterState.EMPTY_STATE));
        client.assertNoResponder();
        assertMatchesResponse(nodes, response, responseInfo);

        ClusterState notMasterState = ClusterState.builder(masterState)
            .nodes(DiscoveryNodes.builder(masterState.nodes()).masterNodeId(null))
            .build();

        // client throws if called.
        service.onClusterChanged(new ClusterChangedEvent("test", notMasterState, masterState));

        nodes.forEach(n -> assertThat(service.snapshot().get(n).isEmpty(), is(true)));
    }

    public void testStatsFails() {
        Set<DiscoveryNode> nodes = IntStream.range(0, between(1, 10)).mapToObj(n -> newNode("test_" + n)).collect(Collectors.toSet());
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).nodes(discoveryNodesBuilder(nodes, true)).metadata(metadata).build();

        client.respondStats((r, listener) -> listener.onFailure(randomFrom(new IllegalStateException(), new RejectedExecutionException())));
        service.onClusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));

        nodes.forEach(n -> assertThat(service.snapshot().get(n).isEmpty(), is(true)));

        NodesStatsResponse response = new NodesStatsResponse(
            ClusterName.DEFAULT,
            nodes.stream().map(n -> statsForNode(n, randomLongBetween(0, Long.MAX_VALUE / 1000))).collect(Collectors.toList()),
            List.of()
        );
        NodesInfoResponse responseInfo = new NodesInfoResponse(
            ClusterName.DEFAULT,
            nodes.stream().map(n -> infoForNode(n, randomIntBetween(1, 64))).collect(Collectors.toList()),
            List.of()
        );

        // implicit retry on cluster state update.
        client.respondStats(response, () -> {});
        client.respondInfo(responseInfo, () -> {});
        service.onClusterChanged(new ClusterChangedEvent("test", state, state));
        client.assertNoResponder();
    }

    public void testInfoFails() {
        Set<DiscoveryNode> nodes = IntStream.range(0, between(1, 10)).mapToObj(n -> newNode("test_" + n)).collect(Collectors.toSet());
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).nodes(discoveryNodesBuilder(nodes, true)).metadata(metadata).build();
        NodesStatsResponse response = new NodesStatsResponse(
            ClusterName.DEFAULT,
            nodes.stream().map(n -> statsForNode(n, randomLongBetween(0, Long.MAX_VALUE / 1000))).collect(Collectors.toList()),
            List.of()
        );
        client.respondStats(response, () -> {});
        client.respondInfo((r, listener) -> listener.onFailure(randomFrom(new IllegalStateException(), new RejectedExecutionException())));
        service.onClusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        nodes.forEach(n -> assertThat(service.snapshot().get(n).isEmpty(), is(true)));
        NodesInfoResponse responseInfo = new NodesInfoResponse(
            ClusterName.DEFAULT,
            nodes.stream().map(n -> infoForNode(n, randomIntBetween(1, 64))).collect(Collectors.toList()),
            List.of()
        );

        // implicit retry on cluster state update.
        client.respondStats(response, () -> {});
        client.respondInfo(responseInfo, () -> {});
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

        NodesInfoResponse responseInfo = new NodesInfoResponse(
            ClusterName.DEFAULT,
            nodes.stream().map(n -> infoForNode(n, randomIntBetween(1, 64))).collect(Collectors.toList()),
            List.of()
        );

        client.respondStats(response, () -> {});
        client.respondInfo(responseInfo, () -> {});
        service.onClusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        client.assertNoResponder();

        assertMatchesResponse(nodes, response, responseInfo);

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

        NodesInfoResponse restartedInfoResponse = new NodesInfoResponse(
            ClusterName.DEFAULT,
            Sets.difference(restartedNodes, nodes).stream().map(n -> infoForNode(n, randomIntBetween(1, 64))).collect(Collectors.toList()),
            List.of()
        );

        client.respondStats(restartedStatsResponse, () -> {});
        client.respondInfo(restartedInfoResponse, () -> {});
        service.onClusterChanged(new ClusterChangedEvent("test", restartedState, state));
        client.assertNoResponder();

        assertMatchesResponse(Sets.intersection(restartedNodes, nodes), response, responseInfo);
        assertMatchesResponse(Sets.difference(restartedNodes, nodes), restartedStatsResponse, restartedInfoResponse);

        Sets.difference(nodes, restartedNodes).forEach(n -> assertThat(service.snapshot().get(n).isEmpty(), is(true)));
    }

    public void testConcurrentStateUpdate() throws Exception {
        Set<DiscoveryNode> nodes = IntStream.range(0, between(1, 10)).mapToObj(n -> newNode("test_" + n)).collect(Collectors.toSet());
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).nodes(discoveryNodesBuilder(nodes, true)).metadata(metadata).build();

        NodesStatsResponse response = new NodesStatsResponse(
            ClusterName.DEFAULT,
            nodes.stream().map(n -> statsForNode(n, randomLongBetween(0, Long.MAX_VALUE / 1000))).collect(Collectors.toList()),
            List.of()
        );
        NodesInfoResponse nodesInfoResponse = new NodesInfoResponse(
            ClusterName.DEFAULT,
            nodes.stream().map(n -> infoForNode(n, randomIntBetween(1, 64))).collect(Collectors.toList()),
            List.of()
        );

        List<Thread> threads = new ArrayList<>();
        client.respondStats((request, listener) -> {
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
        client.respondInfo((r, l) -> l.onResponse(nodesInfoResponse));
        service.onClusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));
        for (Thread thread : threads) {
            thread.join(10000);
        }
        client.assertNoResponder();

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
        final String localNodeId = nodes.isEmpty() ? null : randomFrom(nodes).getId();
        nodesBuilder.localNodeId(localNodeId);
        nodesBuilder.masterNodeId(master ? localNodeId : null);
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

    public void assertMatchesResponse(Set<DiscoveryNode> nodes, NodesStatsResponse response, NodesInfoResponse infoResponse) {
        nodes.forEach(n -> {
            assertThat(service.snapshot().get(n).isPresent(), is(true));
            assertThat(
                service.snapshot().get(n).get(),
                equalTo(
                    new AutoscalingNodeInfo(
                        response.getNodesMap().get(n.getId()).getOs().getMem().getAdjustedTotal().getBytes(),
                        Processors.of(infoResponse.getNodesMap().get(n.getId()).getInfo(OsInfo.class).getFractionalAllocatedProcessors())
                    )
                )
            );
        });
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
            new OsStats.Mem(memory, randomLongBetween(0, memory), randomLongBetween(0, memory)),
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
            null,
            null
        );
    }

    private static org.elasticsearch.action.admin.cluster.node.info.NodeInfo infoForNode(DiscoveryNode node, int processors) {
        OsInfo osInfo = new OsInfo(randomLong(), processors, Processors.of((double) processors), null, null, null, null);
        return new org.elasticsearch.action.admin.cluster.node.info.NodeInfo(
            Version.CURRENT,
            TransportVersion.current(),
            Build.CURRENT,
            node,
            null,
            osInfo,
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
        private BiConsumer<NodesStatsRequest, ActionListener<NodesStatsResponse>> responderStats;
        private BiConsumer<NodesInfoRequest, ActionListener<NodesInfoResponse>> responderInfo;

        private NodeStatsClient() {
            super(getTestName());
        }

        public void respondInfo(NodesInfoResponse response, Runnable whileFetching) {
            respondInfo((request, listener) -> {
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

        public void respondStats(NodesStatsResponse response, Runnable whileFetching) {
            respondStats((request, listener) -> {
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

        public void respondStats(BiConsumer<NodesStatsRequest, ActionListener<NodesStatsResponse>> responderValue) {
            assertThat(responderValue, notNullValue());
            this.responderStats = responderValue;
        }

        public void respondInfo(BiConsumer<NodesInfoRequest, ActionListener<NodesInfoResponse>> responderValue) {
            assertThat(responderValue, notNullValue());
            this.responderInfo = responderValue;
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertThat(action, anyOf(Matchers.sameInstance(NodesStatsAction.INSTANCE), Matchers.sameInstance(NodesInfoAction.INSTANCE)));
            if (action == NodesStatsAction.INSTANCE) {
                NodesStatsRequest nodesStatsRequest = (NodesStatsRequest) request;
                assertThat(nodesStatsRequest.timeout(), equalTo(fetchTimeout));
                assertThat(responderStats, notNullValue());
                BiConsumer<NodesStatsRequest, ActionListener<NodesStatsResponse>> responderValue = this.responderStats;
                this.responderStats = null;
                @SuppressWarnings("unchecked")
                ActionListener<NodesStatsResponse> statsListener = (ActionListener<NodesStatsResponse>) listener;
                responderValue.accept(nodesStatsRequest, statsListener);
            } else {
                NodesInfoRequest nodesInfoRequest = (NodesInfoRequest) request;
                assertThat(nodesInfoRequest.timeout(), equalTo(fetchTimeout));
                assertThat(responderInfo, notNullValue());
                BiConsumer<NodesInfoRequest, ActionListener<NodesInfoResponse>> responderValue = this.responderInfo;
                this.responderInfo = null;
                @SuppressWarnings("unchecked")
                ActionListener<NodesInfoResponse> infoListener = (ActionListener<NodesInfoResponse>) listener;
                responderValue.accept(nodesInfoRequest, infoListener);
            }
        }

        public void assertNoResponder() {
            assertThat(responderInfo, nullValue());
            assertThat(responderStats, nullValue());
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
        return DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).name(nodeName).roles(roles).build();
    }

    private DiscoveryNode restartNode(DiscoveryNode node) {
        return new DiscoveryNode(node.getName(), node.getId(), node.getAddress(), node.getAttributes(), node.getRoles(), node.getVersion());
    }
}
