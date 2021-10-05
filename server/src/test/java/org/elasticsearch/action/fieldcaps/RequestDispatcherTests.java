/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.nio.MockNioTransport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponseTests.randomIndexResponse;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// TODO: Add more tests
public class RequestDispatcherTests extends ESAllocationTestCase {
    static final Logger logger = LogManager.getLogger(RequestDispatcherTests.class);

    /**
     * Verify that in a happy case, each node in the new cluster receives at most one request and
     * - Without query filter each index is requested once in a single node request
     * - With query filter every shard of each index is requested once
     */
    public void testHappyNewCluster() throws Exception {
        final List<String> allIndices = IntStream.rangeClosed(1, 5).mapToObj(n -> "index_" + n).collect(Collectors.toList());
        final ClusterState clusterState;
        {
            DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
            int numNodes = randomIntBetween(1, 10);
            for (int i = 0; i < numNodes; i++) {
                discoNodes.add(newNode("node_" + i, randomNewVersion()));
            }
            Metadata.Builder metadata = Metadata.builder();
            for (String index : allIndices) {
                final Settings.Builder settings = Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 10))
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(0, 2))
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.minimumIndexCompatibilityVersion());
                metadata.put(IndexMetadata.builder(index).settings(settings));
            }
            clusterState = newClusterState(metadata.build(), discoNodes.build());
        }

        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final List<String> indices = randomSubsetOf(between(1, allIndices.size()), allIndices);
            logger.debug("--> test with indices {}", indices);
            final ResponseCollector collector = new ResponseCollector(indices);
            final QueryBuilder filter = randomBoolean() ? new RangeQueryBuilder("timestamp").from(randomNonNegativeLong()) : null;
            final RequestDispatcher dispatcher = new RequestDispatcher(
                mockClusterService(clusterState),
                transportService,
                newRandomParentTask(),
                new FieldCapabilitiesRequest().fields("*").indexFilter(filter),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                indices.toArray(new String[0]),
                collector::addIndexResponse,
                collector::addFailure);
            dispatcher.execute();
            collector.awaitCompletion();
            assertThat("no shard request sent on the new cluster", transportService.sentShardRequests, empty());
            Map<String, List<NodeRequest>> requestsPerNode = transportService.sentNodeRequests.stream()
                .collect(Collectors.groupingBy(r -> r.node.getId()));
            for (Map.Entry<String, List<NodeRequest>> e : requestsPerNode.entrySet()) {
                assertThat("node " + e.getKey() + " received more than 1 node request", e.getValue(), hasSize(1));
            }
            Map<String, Set<NodeRequest>> requestsPerIndex = new HashMap<>();
            for (NodeRequest request : transportService.sentNodeRequests) {
                for (ShardId shardId : request.request.shardIds()) {
                    requestsPerIndex.computeIfAbsent(shardId.getIndexName(), index -> new HashSet<>()).add(request);
                }
            }
            for (String index : indices) {
                final Set<NodeRequest> nodeRequests = requestsPerIndex.get(index);
                assertNotNull("index " + index + " was not requested", nodeRequests);
                if (filter == null) {
                    assertThat("index was request more than once [" + nodeRequests + "]", nodeRequests, hasSize(1));
                } else {
                    Map<ShardId, List<ShardId>> groupShardIds = nodeRequests.stream().flatMap(r -> r.request.shardIds().stream())
                        .collect(Collectors.groupingBy(s -> s));
                    for (IndexShardRoutingTable shardRoutingTable : clusterState.routingTable().index(index)) {
                        final ShardId shardId = shardRoutingTable.shardId();
                        assertNotNull(groupShardIds.get(shardId));
                        assertThat("ShardId [" + shardId + "] was request more than once", groupShardIds.get(shardId), hasSize(1));
                    }
                }
            }
        }
    }

    /**
     * Verify that in a happy case in a mixed cluster, each new node receives at most one node request and
     * - Without filter each index is requested once in a node/shard request
     * - With filter every shard of each index is requested once in both node and shard requests
     */
    public void testHappyMixedCluster() throws Exception {
        final List<String> allIndices = IntStream.rangeClosed(1, 5).mapToObj(n -> "index_" + n).collect(Collectors.toList());
        final ClusterState clusterState;
        {
            DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
            int numNodes = randomIntBetween(1, 10);
            for (int i = 0; i < numNodes; i++) {
                discoNodes.add(newNode("node_" + i, randomBoolean() ? randomNewVersion(): randomOldVersion()));
            }
            Metadata.Builder metadata = Metadata.builder();
            for (String index : allIndices) {
                final Settings.Builder settings = Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 10))
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(0, 3))
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.minimumIndexCompatibilityVersion());
                metadata.put(IndexMetadata.builder(index).settings(settings));
            }
            clusterState = newClusterState(metadata.build(), discoNodes.build());
        }

        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final List<String> indices = randomSubsetOf(between(1, allIndices.size()), allIndices);
            final ResponseCollector collector = new ResponseCollector(indices);
            final QueryBuilder filter = randomBoolean() ? new RangeQueryBuilder("timestamp").from(randomNonNegativeLong()) : null;
            logger.debug("--> test with indices={}, filter={}", indices, filter != null);
            final RequestDispatcher dispatcher = new RequestDispatcher(
                mockClusterService(clusterState),
                transportService,
                newRandomParentTask(),
                new FieldCapabilitiesRequest().indexFilter(filter).fields("*"),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                indices.toArray(new String[0]),
                collector::addIndexResponse,
                collector::addFailure);
            dispatcher.execute();
            collector.awaitCompletion();

            // Node-level requests must be sent at most once for each new node
            Map<String, List<NodeRequest>> nodeRequestsPerNode = transportService.sentNodeRequests.stream()
                .collect(Collectors.groupingBy(r -> r.node.getId()));
            for (Map.Entry<String, List<NodeRequest>> e : nodeRequestsPerNode.entrySet()) {
                assertThat("node " + e.getKey() + " received more than 1 node request", e.getValue(), hasSize(1));
            }
            if (filter == null) {
                Map<String, Object> requestsPerIndex = new HashMap<>();
                for (NodeRequest nodeRequest : transportService.sentNodeRequests) {
                    for (ShardId shardId : nodeRequest.request.shardIds()) {
                        final Object existing = requestsPerIndex.put(shardId.getIndexName(), nodeRequest);
                        if (existing != null) {
                            assertThat("Index was requested in " + existing, existing, sameInstance(nodeRequest));
                        }
                    }
                }
                for (ShardRequest shardRequest : transportService.sentShardRequests) {
                    final Object existing = requestsPerIndex.put(shardRequest.request.index(), shardRequest);
                    assertNull("Index was requested in " + existing, existing);
                }
            } else {
                Map<ShardId, List<ShardId>> groupedShards = new HashMap<>();
                for (NodeRequest request : transportService.sentNodeRequests) {
                    for (ShardId shardId : request.request.shardIds()) {
                        groupedShards.computeIfAbsent(shardId, index -> new ArrayList<>()).add(shardId);
                    }
                }
                for (ShardRequest request : transportService.sentShardRequests) {
                    groupedShards.computeIfAbsent(request.request.shardId(), index -> new ArrayList<>()).add(request.request.shardId());
                }
                for (String index : indices) {
                    for (IndexShardRoutingTable shardRoutingTable : clusterState.routingTable().index(index)) {
                        final ShardId shardId = shardRoutingTable.shardId();
                        assertNotNull(groupedShards.get(shardId));
                        assertThat("ShardId [" + shardId + "] was request more than once", groupedShards.get(shardId), hasSize(1));
                    }
                }
            }
        }
    }

    public void testRetryWithoutFilterNewCluster() throws Exception {
        final List<String> allIndices = IntStream.rangeClosed(1, 5).mapToObj(n -> "index_" + n).collect(Collectors.toList());
        final ClusterState clusterState;
        {
            DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
            int numNodes = randomIntBetween(2, 10);
            for (int i = 0; i < numNodes; i++) {
                discoNodes.add(newNode("node_" + i, randomNewVersion()));
            }
            Metadata.Builder metadata = Metadata.builder();
            for (String index : allIndices) {
                final Settings.Builder settings = Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 10))
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(1, 3))
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.minimumIndexCompatibilityVersion());
                metadata.put(IndexMetadata.builder(index).settings(settings));
            }
            clusterState = newClusterState(metadata.build(), discoNodes.build());
        }
        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final List<String> indices = randomSubsetOf(between(1, allIndices.size()), allIndices);
            logger.debug("--> test with indices {}", indices);
            final ResponseCollector collector = new ResponseCollector(indices);
            final RequestDispatcher dispatcher = new RequestDispatcher(
                mockClusterService(clusterState),
                transportService,
                newRandomParentTask(),
                new FieldCapabilitiesRequest().fields("*"),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                indices.toArray(new String[0]),
                collector::addIndexResponse,
                collector::addFailure);

            String failedIndex = randomFrom(indices);
            Set<String> assignedNodes = clusterState.routingTable().index(failedIndex).randomAllActiveShardsIt().getShardRoutings()
                .stream().map(ShardRouting::currentNodeId)
                .collect(Collectors.toSet());
            int numFailures = randomIntBetween(1, assignedNodes.size() - 1);
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                final Set<DiscoveryNode> failedNodes = new HashSet<>();

                @Override
                public <T extends TransportResponse> void sendRequest(Transport.Connection connection, String action,
                                                                      TransportRequest request, TransportRequestOptions options,
                                                                      TransportResponseHandler<T> handler) {
                    final String nodeId = connection.getNode().getId();
                    assertThat(request, instanceOf(FieldCapabilitiesNodeRequest.class));
                    FieldCapabilitiesNodeRequest nodeRequest = (FieldCapabilitiesNodeRequest) request;
                    final Map<String, List<ShardId>> shardsPerIndex = nodeRequest.shardIds().stream()
                        .collect(Collectors.groupingBy(ShardId::getIndexName));
                    for (Map.Entry<String, List<ShardId>> e : shardsPerIndex.entrySet()) {
                        final String index = e.getKey();
                        List<ShardId> shardsPerNode = clusterState.routingTable().index(index)
                            .randomAllActiveShardsIt().getShardRoutings()
                            .stream().filter(shr -> shr.currentNodeId().equals(nodeId))
                            .map(ShardRouting::shardId)
                            .collect(Collectors.toList());
                        assertThat(shardsPerNode, containsInAnyOrder(shardsPerIndex.get(index).toArray(new ShardId[0])));
                    }
                    if (shardsPerIndex.containsKey(failedIndex)) {
                        assertTrue("Node " + connection.getNode() + " was tried already", failedNodes.add(connection.getNode()));
                    }
                    if (failedNodes.size() <= numFailures) {
                        List<FieldCapabilitiesIndexResponse> indexResponses = new ArrayList<>();
                        List<FieldCapabilitiesFailure> indexFailures = new ArrayList<>();
                        for (String index : shardsPerIndex.keySet()) {
                            if (index.equals(failedIndex)) {
                                indexFailures.add(
                                    new FieldCapabilitiesFailure(new String[]{failedIndex}, new IllegalStateException("shard is closed")));
                            } else {
                                indexResponses.add(randomIndexResponse(index, true));
                            }
                        }
                        FieldCapabilitiesNodeResponse resp = new FieldCapabilitiesNodeResponse(indexResponses, indexFailures);
                        sendResponse(transportService.threadPool, handler, resp);
                    } else {
                        sendResponse(transportService.threadPool, handler, randomSuccessResponse(request));
                    }
                }
            });
            dispatcher.execute();
            collector.awaitCompletion();
            assertThat("no shard request sent on the new cluster", transportService.sentShardRequests, empty());
            Map<String, Set<NodeRequest>> requestsPerIndex = new HashMap<>();
            for (NodeRequest request : transportService.sentNodeRequests) {
                for (ShardId shardId : request.request.shardIds()) {
                    requestsPerIndex.computeIfAbsent(shardId.getIndexName(), index -> new HashSet<>()).add(request);
                }
            }
            for (String index : indices) {
                final Set<NodeRequest> nodeRequests = requestsPerIndex.get(index);
                assertNotNull("index " + index + " was not requested", nodeRequests);
                if (index.equals(failedIndex)) {
                    assertThat(nodeRequests, hasSize(numFailures + 1));
                } else {
                    assertThat("index was request more than once [" + nodeRequests + "]", nodeRequests, hasSize(1));
                }
            }
        }
    }

    public void testRetryWithoutFilterMixedCluster() throws Exception {

    }

    public void testRetryWithFilterNewCluster() throws Exception {
        final List<String> allIndices = IntStream.rangeClosed(1, 5).mapToObj(n -> "index_" + n).collect(Collectors.toList());
        final ClusterState clusterState;
        {
            DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
            int numNodes = randomIntBetween(2, 10);
            for (int i = 0; i < numNodes; i++) {
                discoNodes.add(newNode("node_" + i, randomNewVersion()));
            }
            Metadata.Builder metadata = Metadata.builder();
            for (String index : allIndices) {
                final Settings.Builder settings = Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 10))
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(1, 3))
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.minimumIndexCompatibilityVersion());
                metadata.put(IndexMetadata.builder(index).settings(settings));
            }
            clusterState = newClusterState(metadata.build(), discoNodes.build());
        }
        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final List<String> indices = randomSubsetOf(between(1, allIndices.size()), allIndices);
            logger.debug("--> test with indices {}", indices);
            final QueryBuilder filter = new RangeQueryBuilder("timestamp").from(randomNonNegativeLong());
            final ResponseCollector collector = new ResponseCollector(indices);
            final RequestDispatcher dispatcher = new RequestDispatcher(
                mockClusterService(clusterState),
                transportService,
                newRandomParentTask(),
                new FieldCapabilitiesRequest().indexFilter(filter).fields("*"),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                indices.toArray(new String[0]),
                collector::addIndexResponse,
                collector::addFailure);

            String failedIndex = randomFrom(indices);
            int totalShards = clusterState.routingTable().index(failedIndex).randomAllActiveShardsIt().size();
            int allowFailures = randomIntBetween(1, totalShards - 1);
            final Map<String, Set<ShardId>> failedShards = new HashMap<>();
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(Transport.Connection connection, String action,
                                                                      TransportRequest request, TransportRequestOptions options,
                                                                      TransportResponseHandler<T> handler) {
                    final DiscoveryNode node = connection.getNode();
                    assertThat(request, instanceOf(FieldCapabilitiesNodeRequest.class));
                    FieldCapabilitiesNodeRequest nodeRequest = (FieldCapabilitiesNodeRequest) request;
                    List<ShardId> suspectShardIds = nodeRequest.shardIds().stream()
                        .filter(shr -> shr.getIndexName().equals(failedIndex))
                        .collect(Collectors.toList());
                    if (suspectShardIds.isEmpty()) {
                        sendResponse(transportService.threadPool, handler, randomSuccessResponse(request));
                    } else {
                        final boolean toFail =
                            failedShards.values().stream().mapToInt(Set::size).sum() + suspectShardIds.size() <= allowFailures;
                        failedShards.compute(node.getId(), (n, curr) -> {
                            if (curr == null) {
                                return new HashSet<>(suspectShardIds);
                            } else {
                                for (ShardId shardId : suspectShardIds) {
                                    assertTrue("shard " + shardId + " was request on node " + node.getId(), curr.add(shardId));
                                }
                                return curr;
                            }
                        });
                        List<FieldCapabilitiesIndexResponse> indexResponses = new ArrayList<>();
                        List<FieldCapabilitiesFailure> indexFailures = new ArrayList<>();
                        Set<String> indices = nodeRequest.shardIds().stream().map(ShardId::getIndexName).collect(Collectors.toSet());
                        for (String index : indices) {
                            if (toFail && index.equals(failedIndex)) {
                                indexFailures.add(
                                    new FieldCapabilitiesFailure(new String[]{failedIndex}, new IllegalStateException("shard is closed")));
                            } else {
                                indexResponses.add(randomIndexResponse(index, true));
                            }
                        }
                        FieldCapabilitiesNodeResponse resp = new FieldCapabilitiesNodeResponse(indexResponses, indexFailures);
                        sendResponse(transportService.threadPool, handler, resp);
                    }
                }
            });
            // TODO: more assertions
            dispatcher.execute();
            collector.awaitCompletion();
            assertThat("no shard request sent on the new cluster", transportService.sentShardRequests, empty());
        }
    }

    public void testRetryWithFilterMixedCluster() throws Exception {

    }

    public void testUnassignedShards() throws Exception {

    }

    public void testFailsAfterTryAllShards() throws Exception {

    }

    public void testWithFilterSuccessSingleMatch() throws Exception {

    }

    public void testWithFilterStopAfterAllShardsDoNotMatch() throws Exception {

    }

    static Version randomNewVersion() {
        return VersionUtils.randomVersionBetween(random(), Version.V_7_16_0, Version.CURRENT);
    }

    static Version randomOldVersion() {
        final Version previousVersion = VersionUtils.getPreviousVersion(Version.V_7_16_0);
        return VersionUtils.randomVersionBetween(random(), previousVersion.minimumCompatibilityVersion(), previousVersion);
    }

    static ClusterService mockClusterService(ClusterState clusterState) {
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final OperationRouting operationRouting = new OperationRouting(Settings.EMPTY, clusterSettings);
        when(clusterService.operationRouting()).thenReturn(operationRouting);
        return clusterService;
    }

    static final class NodeRequest {
        final DiscoveryNode node;
        final FieldCapabilitiesNodeRequest request;

        NodeRequest(DiscoveryNode node, FieldCapabilitiesNodeRequest request) {
            this.node = node;
            this.request = request;
        }
    }

    static final class ShardRequest {
        final DiscoveryNode node;
        final FieldCapabilitiesIndexRequest request;

        ShardRequest(DiscoveryNode node, FieldCapabilitiesIndexRequest request) {
            this.node = node;
            this.request = request;
        }
    }

    static TransportResponse randomSuccessResponse(TransportRequest request) {
        if (request instanceof FieldCapabilitiesIndexRequest) {
            FieldCapabilitiesIndexRequest indexRequest = (FieldCapabilitiesIndexRequest) request;
            return randomIndexResponse(indexRequest.index(), true);
        } else if (request instanceof FieldCapabilitiesNodeRequest) {
            FieldCapabilitiesNodeRequest nodeRequest = (FieldCapabilitiesNodeRequest) request;
            Set<String> indices = nodeRequest.shardIds().stream().map(ShardId::getIndexName).collect(Collectors.toSet());
            List<FieldCapabilitiesIndexResponse> indexResponses = indices.stream()
                .map(index -> randomIndexResponse(index, true)).collect(Collectors.toList());
            return new FieldCapabilitiesNodeResponse(indexResponses, Collections.emptyList());
        } else {
            throw new AssertionError("unknown request " + request);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends TransportResponse> void sendResponse(ThreadPool threadPool,
                                                                   TransportResponseHandler<T> handler,
                                                                   TransportResponse r) {
        threadPool.executor(ThreadPool.Names.MANAGEMENT).submit(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                handler.handleException(new TransportException(e));
            }

            @Override
            protected void doRun() {
                handler.handleResponse((T) r);
            }
        });
    }

    static final class TestTransportService extends TransportService {
        final List<NodeRequest> sentNodeRequests = new CopyOnWriteArrayList<>();
        final List<ShardRequest> sentShardRequests = new CopyOnWriteArrayList<>();
        final ThreadPool threadPool;
        private TransportInterceptor.AsyncSender interceptor = null;

        private TestTransportService(Transport transport, TransportInterceptor.AsyncSender asyncSender, ThreadPool threadPool) {
            super(Settings.EMPTY, transport, threadPool, new TransportInterceptor() {
                @Override
                public AsyncSender interceptSender(AsyncSender sender) {
                    return asyncSender;
                }
            }, addr -> newNode("local"), null, Collections.emptySet());
            this.threadPool = threadPool;
        }

        @Override
        public Transport.Connection getConnection(DiscoveryNode node) {
            final Transport.Connection conn = mock(Transport.Connection.class);
            when(conn.getNode()).thenReturn(node);
            return conn;
        }

        static TestTransportService newTestTransportService() {
            final TestThreadPool threadPool = new TestThreadPool("test");
            MockNioTransport mockTransport = new MockNioTransport(Settings.EMPTY, Version.CURRENT, threadPool,
                new NetworkService(Collections.emptyList()), PageCacheRecycler.NON_RECYCLING_INSTANCE,
                new NamedWriteableRegistry(Collections.emptyList()), new NoneCircuitBreakerService());
            SetOnce<TransportInterceptor.AsyncSender> asyncSenderHolder = new SetOnce<>();
            TestTransportService transportService = new TestTransportService(mockTransport, new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(Transport.Connection connection, String action,
                                                                      TransportRequest request, TransportRequestOptions options,
                                                                      TransportResponseHandler<T> handler) {
                    final TransportInterceptor.AsyncSender asyncSender = asyncSenderHolder.get();
                    assertNotNull(asyncSender);
                    asyncSender.sendRequest(connection, action, request, options, handler);
                }
            }, threadPool);
            asyncSenderHolder.set(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(Transport.Connection connection, String action,
                                                                      TransportRequest request, TransportRequestOptions options,
                                                                      TransportResponseHandler<T> handler) {
                    transportService.verifyAndTrackRequest(connection, action, request);
                    if (transportService.interceptor != null) {
                        transportService.interceptor.sendRequest(connection, action, request, options, handler);
                    } else {
                        sendResponse(threadPool, handler, randomSuccessResponse(request));
                    }
                }
            });
            transportService.start();
            return transportService;
        }

        private void verifyAndTrackRequest(Transport.Connection connection, String action, TransportRequest request) {
            final DiscoveryNode node = connection.getNode();
            if (action.equals(TransportFieldCapabilitiesAction.ACTION_NODE_NAME)) {
                assertTrue(node.getVersion().onOrAfter(Version.V_7_16_0));
                sentNodeRequests.add(new NodeRequest(connection.getNode(), (FieldCapabilitiesNodeRequest) request));
                logger.debug("--> received node-level request: node {}, shards {}",
                    node.getId(), ((FieldCapabilitiesNodeRequest) request).shardIds());
            } else if (action.equals(TransportFieldCapabilitiesAction.ACTION_SHARD_NAME)) {
                assertTrue(node.getVersion().before(Version.V_7_16_0));
                sentShardRequests.add(new ShardRequest(connection.getNode(), (FieldCapabilitiesIndexRequest) request));
                logger.debug("--> received shard-level request: node {}, shard {}",
                    node.getId(), ((FieldCapabilitiesIndexRequest) request).shardId());
            } else {
                throw new AssertionError("unexpected action [" + action + "]");
            }
        }

        void setTransportInterceptor(TransportInterceptor.AsyncSender interceptor) {
            this.interceptor = interceptor;
        }

        @Override
        protected void doClose() throws IOException {
            super.doClose();
            threadPool.shutdown();
        }
    }

    static final class ResponseCollector {
        final Map<String, FieldCapabilitiesIndexResponse> responses = new HashMap<>();
        final Map<String, Exception> failures = new HashMap<>();
        final List<String> indices;
        final CountDownLatch latch;

        ResponseCollector(List<String> indices) {
            this.indices = indices;
            this.latch = new CountDownLatch(indices.size());
        }

        private synchronized void assertIndex(String index) {
            assertThat(index, in(indices));
            assertThat(failures, not(hasKey(index)));
            assertThat(responses, not(hasKey(index)));
        }

        synchronized void addIndexResponse(FieldCapabilitiesIndexResponse resp) {
            logger.debug("--> receive response for index {} can_match {}", resp.getIndexName(), resp.canMatch());
            assertIndex(resp.getIndexName());
            responses.put(resp.getIndexName(), resp);
            latch.countDown();
        }

        synchronized void addFailure(String index, Exception e) {
            logger.debug("--> receive failure for index {}", index);
            assertIndex(index);
            failures.put(index, e);
            latch.countDown();
        }

        void awaitCompletion() throws InterruptedException {
            assertTrue("expected " + indices.size() + ", got: " + responses.size() + " responses; " + failures.size() + " failures",
                latch.await(120, TimeUnit.SECONDS));
        }
    }

    static Task newRandomParentTask() {
        return new Task(0, "type", "action", randomAlphaOfLength(10), TaskId.EMPTY_TASK_ID, Collections.emptyMap());
    }

    private ClusterState newClusterState(Metadata metadata, DiscoveryNodes discoveryNodes) {
        final RoutingTable.Builder routingTable = RoutingTable.builder();
        for (IndexMetadata imd : metadata) {
            routingTable.addAsNew(metadata.index(imd.getIndex()));
        }
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(metadata)
            .routingTable(routingTable.build())
            .build();
        return applyStartedShardsUntilNoChange(clusterState, createAllocationService());
    }
}
