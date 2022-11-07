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
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStatePublicationEvent;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.IncompatibleClusterStateVersionException;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class PublicationTransportHandlerTests extends ESTestCase {

    public void testDiffSerializationFailure() {
        final DiscoveryNode localNode = new DiscoveryNode("localNode", buildNewFakeTransportAddress(), Version.CURRENT);
        final PublicationTransportHandler handler = new PublicationTransportHandler(
            new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService()),
            mock(TransportService.class),
            writableRegistry(),
            pu -> null,
            (pu, l) -> {}
        );

        final DiscoveryNode otherNode = new DiscoveryNode("otherNode", buildNewFakeTransportAddress(), Version.CURRENT);
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
            () -> handler.newPublicationContext(new ClusterStatePublicationEvent("test", clusterState, unserializableClusterState, 0L, 0L))
        );
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IOException.class));
        assertThat(e.getCause().getMessage(), containsString("Simulated failure of diff serialization"));
    }

    private static boolean isDiff(BytesTransportRequest request, DiscoveryNode node) {
        try {
            StreamInput in = null;
            try {
                in = request.bytes().streamInput();
                final Compressor compressor = CompressorFactory.compressor(request.bytes());
                if (compressor != null) {
                    in = new InputStreamStreamInput(compressor.threadLocalInputStream(in));
                }
                in.setVersion(node.getVersion());
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
            final DiscoveryNode localNode = new DiscoveryNode("localNode", buildNewFakeTransportAddress(), Version.CURRENT);
            final MockTransport mockTransport = new MockTransport() {

                @Nullable
                private Exception simulateException(String action, BytesTransportRequest request, DiscoveryNode node) {
                    if (action.equals(PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME) && rarely()) {
                        if (isDiff(request, node) && randomBoolean()) {
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
            };
            final TransportService transportService = mockTransport.createTransportService(
                Settings.EMPTY,
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                x -> localNode,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                Collections.emptySet()
            );
            final PublicationTransportHandler handler = new PublicationTransportHandler(
                new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService()),
                transportService,
                writableRegistry(),
                pu -> new PublishWithJoinResponse(new PublishResponse(randomNonNegativeLong(), randomNonNegativeLong()), Optional.empty()),
                (pu, l) -> {}
            );
            transportService.start();
            transportService.acceptIncomingRequests();

            final List<DiscoveryNode> allNodes = new ArrayList<>();
            while (allNodes.size() < 10) {
                allNodes.add(
                    new DiscoveryNode(
                        "node-" + allNodes.size(),
                        buildNewFakeTransportAddress(),
                        VersionUtils.randomCompatibleVersion(random(), Version.CURRENT)
                    )
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
                    new ClusterStatePublicationEvent("test", prevClusterState, nextClusterState, 0L, 0L)
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
                            assert simulateFailures;
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

}
