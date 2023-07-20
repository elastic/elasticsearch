/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStatePublicationEvent;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.IncompatibleClusterStateVersionException;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.BatchSummary;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.cluster.service.MasterService.STATE_UPDATE_ACTION_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PublicationTransportHandlerTests extends ESTestCase {

    public void testDiffSerializationFailure() {
        final DiscoveryNode localNode = DiscoveryNodeUtils.create("localNode");

        final TransportService transportService = mock(TransportService.class);
        final BytesRefRecycler recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
        when(transportService.newNetworkBytesStream()).then(invocation -> new RecyclerBytesStreamOutput(recycler));
        Transport.Connection connection = mock(Transport.Connection.class);
        when(connection.getTransportVersion()).thenReturn(TransportVersion.current());
        when(transportService.getConnection(any())).thenReturn(connection);

        final PublicationTransportHandler handler = new PublicationTransportHandler(transportService, writableRegistry(), pu -> null);

        final DiscoveryNode otherNode = DiscoveryNodeUtils.create("otherNode");
        final ClusterState clusterState = CoordinationStateTests.clusterState(
            2L,
            1L,
            DiscoveryNodes.builder().add(localNode).add(otherNode).localNodeId(localNode.getId()).build(),
            VotingConfiguration.EMPTY_CONFIG,
            VotingConfiguration.EMPTY_CONFIG,
            0L
        );

        final ClusterState unserializableClusterState = new ClusterState(clusterState.version(), clusterState.stateUUID(), clusterState) {
            @Override
            public Diff<ClusterState> diff(ClusterState previousState) {
                return new Diff<ClusterState>() {
                    @Override
                    public ClusterState apply(ClusterState part) {
                        fail("this diff shouldn't be applied");
                        return part;
                    }

                    @Override
                    public void writeTo(StreamOutput out) throws IOException {
                        out.writeString("allocate something to detect leaks");
                        throw new IOException("Simulated failure of diff serialization");
                    }
                };
            }
        };

        final ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> handler.newPublicationContext(
                new ClusterStatePublicationEvent(
                    new BatchSummary(() -> "test"),
                    clusterState,
                    unserializableClusterState,
                    new Task(randomNonNegativeLong(), "test", STATE_UPDATE_ACTION_NAME, "", TaskId.EMPTY_TASK_ID, emptyMap()),
                    0L,
                    0L
                )
            )
        );
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IOException.class));
        assertThat(e.getCause().getMessage(), containsString("Simulated failure of diff serialization"));
    }

    private static boolean isDiff(BytesTransportRequest request, TransportVersion version) {
        try {
            StreamInput in = null;
            try {
                in = request.bytes().streamInput();
                final Compressor compressor = CompressorFactory.compressor(request.bytes());
                if (compressor != null) {
                    in = new InputStreamStreamInput(compressor.threadLocalInputStream(in));
                }
                in.setTransportVersion(version);
                return in.readBoolean() == false;
            } finally {
                IOUtils.close(in);
            }
        } catch (IOException e) {
            throw new AssertionError("unexpected", e);
        }
    }

    public void testSerializationFailuresDoNotLeak() throws InterruptedException {
        final ThreadPool threadPool = new TestThreadPool("test");
        try {
            threadPool.getThreadContext().markAsSystemContext();

            final boolean simulateFailures = randomBoolean();
            final Map<DiscoveryNode, TransportVersion> nodeTransports = new HashMap<>();
            final DiscoveryNode localNode = DiscoveryNodeUtils.builder("localNode").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();
            final BytesRefRecycler recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
            final MockTransport mockTransport = new MockTransport() {

                @Nullable
                private Exception simulateException(String action, BytesTransportRequest request, DiscoveryNode node) {
                    if (action.equals(PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME) && rarely()) {
                        if (isDiff(request, nodeTransports.get(node)) && randomBoolean()) {
                            return new IncompatibleClusterStateVersionException(
                                randomNonNegativeLong(),
                                UUIDs.randomBase64UUID(random()),
                                randomNonNegativeLong(),
                                UUIDs.randomBase64UUID(random())
                            );
                        }

                        if (simulateFailures && randomBoolean()) {
                            return new IOException("simulated failure");
                        }
                    }

                    return null;
                }

                @Override
                protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                    final Exception exception = simulateException(action, (BytesTransportRequest) request, node);
                    if (exception == null) {
                        handleResponse(
                            requestId,
                            new PublishWithJoinResponse(
                                new PublishResponse(randomNonNegativeLong(), randomNonNegativeLong()),
                                Optional.empty()
                            )
                        );
                    } else {
                        handleError(requestId, new RemoteTransportException(node.getName(), node.getAddress(), action, exception));
                    }
                }

                @Override
                public RecyclerBytesStreamOutput newNetworkBytesStream() {
                    return new RecyclerBytesStreamOutput(recycler);
                }
            };

            final TransportService transportService = mockTransport.createTransportService(
                Settings.EMPTY,
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                x -> localNode,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                Collections.emptySet()
            );
            final PublicationTransportHandler handler = new PublicationTransportHandler(transportService, writableRegistry(), pu -> null);
            transportService.start();
            transportService.acceptIncomingRequests();

            final List<DiscoveryNode> allNodes = new ArrayList<>();
            while (allNodes.size() < 10) {
                var node = DiscoveryNodeUtils.builder("node-" + allNodes.size())
                    .version(VersionUtils.randomCompatibleVersion(random(), Version.CURRENT))
                    .build();
                allNodes.add(node);
                nodeTransports.put(
                    node,
                    TransportVersionUtils.randomVersionBetween(random(), TransportVersion.MINIMUM_COMPATIBLE, TransportVersion.current())
                );
            }

            final DiscoveryNodes.Builder prevNodes = DiscoveryNodes.builder();
            prevNodes.add(localNode);
            prevNodes.localNodeId(localNode.getId());
            randomSubsetOf(allNodes).forEach(prevNodes::add);

            final DiscoveryNodes.Builder nextNodes = DiscoveryNodes.builder();
            nextNodes.add(localNode);
            nextNodes.localNodeId(localNode.getId());
            randomSubsetOf(allNodes).forEach(nextNodes::add);

            final ClusterState prevClusterState = CoordinationStateTests.clusterState(
                randomLongBetween(1L, Long.MAX_VALUE - 1),
                randomNonNegativeLong(),
                prevNodes.build(),
                VotingConfiguration.EMPTY_CONFIG,
                VotingConfiguration.EMPTY_CONFIG,
                0L
            );

            final ClusterState nextClusterState = new ClusterState(
                randomNonNegativeLong(),
                UUIDs.randomBase64UUID(random()),
                CoordinationStateTests.clusterState(
                    randomLongBetween(prevClusterState.term() + 1, Long.MAX_VALUE),
                    randomNonNegativeLong(),
                    nextNodes.build(),
                    VotingConfiguration.EMPTY_CONFIG,
                    VotingConfiguration.EMPTY_CONFIG,
                    0L
                )
            ) {

                @Override
                public void writeTo(StreamOutput out) throws IOException {
                    if (simulateFailures && rarely()) {
                        out.writeString("allocate something to detect leaks");
                        throw new IOException("simulated failure");
                    } else {
                        super.writeTo(out);
                    }
                }

                @Override
                public Diff<ClusterState> diff(ClusterState previousState) {
                    if (simulateFailures && rarely()) {
                        return new Diff<ClusterState>() {
                            @Override
                            public ClusterState apply(ClusterState part) {
                                fail("this diff shouldn't be applied");
                                return part;
                            }

                            @Override
                            public void writeTo(StreamOutput out) throws IOException {
                                out.writeString("allocate something to detect leaks");
                                throw new IOException("simulated failure");
                            }
                        };
                    } else {
                        return super.diff(previousState);
                    }
                }
            };

            final PublicationTransportHandler.PublicationContext context;
            try {
                context = handler.newPublicationContext(
                    new ClusterStatePublicationEvent(
                        new BatchSummary(() -> "test"),
                        prevClusterState,
                        nextClusterState,
                        new Task(randomNonNegativeLong(), "test", STATE_UPDATE_ACTION_NAME, "", TaskId.EMPTY_TASK_ID, emptyMap()),
                        0L,
                        0L
                    )
                );
            } catch (ElasticsearchException e) {
                assertTrue(simulateFailures);
                assertThat(e.getCause(), instanceOf(IOException.class));
                assertThat(e.getCause().getMessage(), equalTo("simulated failure"));
                return;
            }

            final CountDownLatch requestsLatch = new CountDownLatch(nextClusterState.nodes().getSize());
            final CountDownLatch responsesLatch = new CountDownLatch(nextClusterState.nodes().getSize());

            for (DiscoveryNode discoveryNode : nextClusterState.nodes()) {
                threadPool.generic().execute(() -> {
                    context.sendPublishRequest(
                        discoveryNode,
                        new PublishRequest(nextClusterState),
                        ActionListener.runAfter(ActionListener.wrap(r -> {}, e -> {
                            assert simulateFailures : e;
                            final Throwable inner = ExceptionsHelper.unwrap(e, IOException.class);
                            assert inner instanceof IOException : e;
                            assertThat(inner.getMessage(), equalTo("simulated failure"));
                        }), responsesLatch::countDown)
                    );
                    requestsLatch.countDown();
                });
            }

            assertTrue(requestsLatch.await(10, TimeUnit.SECONDS));
            context.decRef();
            assertTrue(responsesLatch.await(10, TimeUnit.SECONDS));
        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
        }
    }

    public void testIncludesLastCommittedFieldsInDiffSerialization() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();

        final var transportsByNode = new HashMap<DiscoveryNode, MockTransport>();
        final var transportHandlersByNode = new HashMap<DiscoveryNode, PublicationTransportHandler>();
        final var transportServicesByNode = new HashMap<DiscoveryNode, TransportService>();
        final var receivedStateRef = new AtomicReference<ClusterState>();
        final var completed = new AtomicBoolean();

        final var localNode = DiscoveryNodeUtils.create("localNode");
        final var otherNode = DiscoveryNodeUtils.builder("otherNode")
            .version(VersionUtils.randomCompatibleVersion(random(), Version.CURRENT))
            .build();
        for (final var discoveryNode : List.of(localNode, otherNode)) {
            final var transport = new MockTransport() {
                @Override
                protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                    @SuppressWarnings("unchecked")
                    final var context = (ResponseContext<TransportResponse>) getResponseHandlers().remove(requestId);
                    try {
                        transportsByNode.get(node)
                            .getRequestHandlers()
                            .getHandler(action)
                            .getHandler()
                            .messageReceived(request, new TestTransportChannel(new ActionListener<>() {
                                @Override
                                public void onResponse(TransportResponse transportResponse) {
                                    context.handler().handleResponse(transportResponse);
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    throw new AssertionError("unexpected", e);
                                }
                            }), new Task(randomNonNegativeLong(), "test", "test", "", TaskId.EMPTY_TASK_ID, Map.of()));
                    } catch (IncompatibleClusterStateVersionException e) {
                        context.handler().handleException(new RemoteTransportException("wrapped", e));
                    } catch (Exception e) {
                        throw new AssertionError("unexpected", e);
                    }
                }
            };
            transportsByNode.put(discoveryNode, transport);

            final var transportService = transport.createTransportService(
                Settings.EMPTY,
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                ignored -> discoveryNode,
                null,
                Set.of()
            );
            transportServicesByNode.put(discoveryNode, transportService);

            final var publicationTransportHandler = new PublicationTransportHandler(
                transportService,
                writableRegistry(),
                publishRequest -> {
                    assertTrue(receivedStateRef.compareAndSet(null, publishRequest.getAcceptedState()));
                    return new PublishWithJoinResponse(
                        new PublishResponse(publishRequest.getAcceptedState().term(), publishRequest.getAcceptedState().version()),
                        Optional.empty()
                    );
                }
            );
            transportHandlersByNode.put(discoveryNode, publicationTransportHandler);
        }

        for (final var transportService : transportServicesByNode.values()) {
            transportService.start();
            transportService.acceptIncomingRequests();
        }

        threadPool.getThreadContext().markAsSystemContext();

        final var clusterState0 = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(DiscoveryNodes.builder().add(localNode).add(otherNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder().lastAcceptedConfiguration(VotingConfiguration.of(localNode)).build()
                    )
                    .generateClusterUuidIfNeeded()
            )
            .build();

        final ClusterState receivedState0;
        var context0 = transportHandlersByNode.get(localNode)
            .newPublicationContext(
                new ClusterStatePublicationEvent(
                    new BatchSummary(() -> "test"),
                    clusterState0,
                    clusterState0,
                    new Task(randomNonNegativeLong(), "test", "test", "", TaskId.EMPTY_TASK_ID, Map.of()),
                    0L,
                    0L
                )
            );
        try {
            context0.sendPublishRequest(
                otherNode,
                new PublishRequest(clusterState0),
                ActionListener.running(() -> assertTrue(completed.compareAndSet(false, true)))
            );
            assertTrue(completed.getAndSet(false));
            receivedState0 = receivedStateRef.getAndSet(null);
            assertEquals(clusterState0.stateUUID(), receivedState0.stateUUID());
            assertEquals(otherNode, receivedState0.nodes().getLocalNode());
            assertFalse(receivedState0.metadata().clusterUUIDCommitted());
            assertEquals(VotingConfiguration.of(), receivedState0.getLastCommittedConfiguration());
            final var receivedStateStats = transportHandlersByNode.get(otherNode).stats();
            assertEquals(0, receivedStateStats.getCompatibleClusterStateDiffReceivedCount());
            assertEquals(1, receivedStateStats.getIncompatibleClusterStateDiffReceivedCount());
            assertEquals(1, receivedStateStats.getFullClusterStateReceivedCount());
        } finally {
            context0.decRef();
        }

        final var committedClusterState0 = ClusterState.builder(clusterState0)
            .metadata(clusterState0.metadata().withLastCommittedValues(true, clusterState0.getLastAcceptedConfiguration()))
            .build();
        assertEquals(clusterState0.stateUUID(), committedClusterState0.stateUUID());
        assertEquals(clusterState0.term(), committedClusterState0.term());
        assertEquals(clusterState0.version(), committedClusterState0.version());

        final var clusterState1 = ClusterState.builder(committedClusterState0).incrementVersion().build();
        assertSame(committedClusterState0.metadata(), clusterState1.metadata());

        var context1 = transportHandlersByNode.get(localNode)
            .newPublicationContext(
                new ClusterStatePublicationEvent(
                    new BatchSummary(() -> "test"),
                    committedClusterState0,
                    clusterState1,
                    new Task(randomNonNegativeLong(), "test", "test", "", TaskId.EMPTY_TASK_ID, Map.of()),
                    0L,
                    0L
                )
            );
        try {
            context1.sendPublishRequest(
                otherNode,
                new PublishRequest(clusterState1),
                ActionListener.running(() -> assertTrue(completed.compareAndSet(false, true)))
            );
            assertTrue(completed.getAndSet(false));
            var receivedState1 = receivedStateRef.getAndSet(null);
            assertEquals(clusterState1.stateUUID(), receivedState1.stateUUID());
            assertEquals(otherNode, receivedState1.nodes().getLocalNode());
            assertSame(receivedState0.nodes(), receivedState1.nodes()); // it was a diff
            assertTrue(receivedState1.metadata().clusterUUIDCommitted());
            assertEquals(VotingConfiguration.of(localNode), receivedState1.getLastCommittedConfiguration());
            final var receivedStateStats = transportHandlersByNode.get(otherNode).stats();
            assertEquals(1, receivedStateStats.getCompatibleClusterStateDiffReceivedCount());
            assertEquals(1, receivedStateStats.getIncompatibleClusterStateDiffReceivedCount());
            assertEquals(1, receivedStateStats.getFullClusterStateReceivedCount());
        } finally {
            context1.decRef();
        }

        assertFalse(deterministicTaskQueue.hasRunnableTasks());
        assertFalse(deterministicTaskQueue.hasDeferredTasks());
    }
}
