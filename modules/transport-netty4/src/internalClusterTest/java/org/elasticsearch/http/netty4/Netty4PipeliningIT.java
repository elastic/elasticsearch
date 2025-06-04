/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCounted;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ESNetty4IntegTestCase;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.bytes.ZeroBytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.EmptyResponseListener;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.netty4.NettyAllocator;
import org.elasticsearch.xcontent.ToXContentObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_PIPELINING_MAX_EVENTS;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.oneOf;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class Netty4PipeliningIT extends ESNetty4IntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(
            List.of(CountDown3Plugin.class, ChunkAndFailPlugin.class, KeepPipeliningPlugin.class),
            super.nodePlugins()
        );
    }

    private static final int MAX_PIPELINE_EVENTS = 10;

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SETTING_PIPELINING_MAX_EVENTS.getKey(), MAX_PIPELINE_EVENTS)
            .build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testThatNettyHttpServerSupportsPipelining() throws Exception {
        runPipeliningTest(
            CountDown3Plugin.ROUTE,
            "/_nodes",
            "/_nodes/stats",
            CountDown3Plugin.ROUTE,
            "/_cluster/health",
            "/_cluster/state",
            CountDown3Plugin.ROUTE,
            "/_cat/shards"
        );
    }

    public void testChunkingFailures() throws Exception {
        runPipeliningTest(0, ChunkAndFailPlugin.randomRequestUri());
        runPipeliningTest(0, ChunkAndFailPlugin.randomRequestUri(), "/_cluster/state");
        runPipeliningTest(
            -1, // typically get the first 2 responses, but we can hit the failing chunk and close the channel soon enough to lose them too
            CountDown3Plugin.ROUTE,
            CountDown3Plugin.ROUTE,
            ChunkAndFailPlugin.randomRequestUri(),
            "/_cluster/health",
            CountDown3Plugin.ROUTE
        );
    }

    public void testPipelineOverflow() throws Exception {
        final var routes = new String[1 // the first request which never returns a response so doesn't consume a spot in the queue
            + MAX_PIPELINE_EVENTS // the responses which fill up the queue
            + 1 // to cause the overflow
            + between(0, 5) // for good measure, to e.g. make sure we don't leak these responses
        ];
        Arrays.fill(routes, "/_cluster/health");
        routes[0] = CountDown3Plugin.ROUTE; // never returns
        runPipeliningTest(0, routes);
    }

    private void runPipeliningTest(String... routes) throws InterruptedException {
        runPipeliningTest(routes.length, routes);
    }

    private void runPipeliningTest(int expectedResponseCount, String... routes) throws InterruptedException {
        try (var client = new Netty4HttpClient()) {
            final var responses = client.get(
                randomFrom(internalCluster().getInstance(HttpServerTransport.class).boundAddress().boundAddresses()).address(),
                routes
            );
            try {
                logger.info("response codes: {}", responses.stream().mapToInt(r -> r.status().code()).toArray());
                if (expectedResponseCount >= 0) {
                    assertThat(responses, hasSize(expectedResponseCount));
                }
                assertThat(responses.size(), lessThanOrEqualTo(routes.length));
                assertTrue(responses.stream().allMatch(r -> r.status().code() == 200));
                assertOpaqueIdsInOrder(Netty4HttpClient.returnOpaqueIds(responses));
            } finally {
                responses.forEach(ReferenceCounted::release);
            }
        }
    }

    public void testSetCloseConnectionHeaderWhenShuttingDown() throws IOException {

        // This test works using KeepPipeliningPlugin to keep a HTTP connection from becoming idle with a sequence of requests while the
        // node shuts down and ensures that these requests start to receive responses with `Connection: close` and that the node does not
        // shut down until all requests have received a response.

        final var victimNode = internalCluster().startNode();

        final var releasables = new ArrayList<Releasable>(3);
        try {
            final var keepPipeliningRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, KeepPipeliningPlugin.ROUTE);
            releasables.add(keepPipeliningRequest::release);

            final var enoughResponsesToCloseLatch = new CountDownLatch(between(1, 5));
            final var outstandingRequestsCounter = new AtomicInteger();
            final var nodeShuttingDown = new AtomicBoolean();
            final var stoppedPipelining = new AtomicBoolean();

            final EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
            releasables.add(() -> eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS).awaitUninterruptibly());
            final var clientBootstrap = new Bootstrap().channel(NettyAllocator.getChannelType())
                .option(ChannelOption.ALLOCATOR, NettyAllocator.getAllocator())
                .group(eventLoopGroup)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new HttpClientCodec());
                        ch.pipeline().addLast(new SimpleChannelInboundHandler<HttpResponse>() {

                            private int closeHeadersToIgnore = between(0, 5);

                            private boolean ignoreCloseHeader() {
                                if (closeHeadersToIgnore == 0) {
                                    return false;
                                } else {
                                    closeHeadersToIgnore -= 1;
                                    return true;
                                }
                            }

                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, HttpResponse msg) {
                                enoughResponsesToCloseLatch.countDown();
                                assertThat(
                                    outstandingRequestsCounter.decrementAndGet(),
                                    stoppedPipelining.get() ? oneOf(0, 1) : equalTo(1)
                                );

                                if ("close".equals(msg.headers().get("connection")) && ignoreCloseHeader() == false) {
                                    assertTrue(nodeShuttingDown.get());
                                    // send one more request with `?respond_immediately` to stop the pipelining
                                    if (stoppedPipelining.compareAndSet(false, true)) {
                                        assertThat(outstandingRequestsCounter.incrementAndGet(), equalTo(2));
                                        ctx.writeAndFlush(
                                            new DefaultFullHttpRequest(
                                                HttpVersion.HTTP_1_1,
                                                HttpMethod.GET,
                                                KeepPipeliningPlugin.ROUTE + "?" + KeepPipeliningPlugin.RESPOND_IMMEDIATELY
                                            )
                                        );
                                    }
                                } else {
                                    // still pipelining, send another request to trigger the next response
                                    assertThat(outstandingRequestsCounter.incrementAndGet(), equalTo(2));
                                    ctx.writeAndFlush(keepPipeliningRequest.retain());
                                }
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ExceptionsHelper.maybeDieOnAnotherThread(new AssertionError(cause));
                            }
                        });
                    }
                });

            final var httpServerTransport = internalCluster().getInstance(HttpServerTransport.class, victimNode);
            final var httpServerAddress = randomFrom(httpServerTransport.boundAddress().boundAddresses()).address();

            // Open a channel on which we will pipeline the requests to KeepPipeliningPlugin.ROUTE
            final var pipeliningChannel = clientBootstrap.connect(httpServerAddress).syncUninterruptibly().channel();
            releasables.add(() -> pipeliningChannel.close().syncUninterruptibly());

            // Send two pipelined requests so that we start to receive responses
            assertTrue(outstandingRequestsCounter.compareAndSet(0, 2));
            pipeliningChannel.writeAndFlush(keepPipeliningRequest.retain());
            pipeliningChannel.writeAndFlush(keepPipeliningRequest.retain());

            // wait until we've started to receive responses
            safeAwait(enoughResponsesToCloseLatch);

            // Shut down the node
            assertTrue(nodeShuttingDown.compareAndSet(false, true));
            internalCluster().stopNode(victimNode);

            // Wait for the pipelining channel to be closed, indicating that it stopped pipelining (because it received a response with
            // `Connection: close`) and allowed the node to shut down
            pipeliningChannel.closeFuture().syncUninterruptibly();

            // The shutdown did not happen until all requests had had a response.
            assertTrue(stoppedPipelining.get());
            assertEquals(0, outstandingRequestsCounter.get());

        } finally {
            Collections.reverse(releasables);
            Releasables.close(releasables);
        }
    }

    private void assertOpaqueIdsInOrder(Collection<String> opaqueIds) {
        // check if opaque ids are monotonically increasing
        int i = 0;
        String msg = Strings.format("Expected list of opaque ids to be monotonically increasing, got [%s]", opaqueIds);
        for (String opaqueId : opaqueIds) {
            assertThat(msg, opaqueId, is(String.valueOf(i++)));
        }
    }

    private static final ToXContentObject EMPTY_RESPONSE = (builder, params) -> builder.startObject().endObject();

    /**
     * Adds an HTTP route that waits for 3 concurrent executions before returning any of them
     */
    public static class CountDown3Plugin extends Plugin implements ActionPlugin {

        static final String ROUTE = "/_test/countdown_3";

        @Override
        public Collection<RestHandler> getRestHandlers(
            Settings settings,
            NamedWriteableRegistry namedWriteableRegistry,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster,
            Predicate<NodeFeature> clusterSupportsFeature
        ) {
            return List.of(new BaseRestHandler() {
                private final SubscribableListener<ToXContentObject> subscribableListener = new SubscribableListener<>();
                private final CountDownActionListener countDownActionListener = new CountDownActionListener(
                    3,
                    subscribableListener.map(v -> EMPTY_RESPONSE)
                );

                private void addListener(ActionListener<ToXContentObject> listener) {
                    subscribableListener.addListener(listener);
                    countDownActionListener.onResponse(null);
                }

                @Override
                public String getName() {
                    return ROUTE;
                }

                @Override
                public List<Route> routes() {
                    return List.of(new Route(GET, ROUTE));
                }

                @Override
                protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
                    return channel -> addListener(new RestToXContentListener<>(channel));
                }
            });
        }
    }

    /**
     * Adds an HTTP route that starts to emit a chunked response and then fails before its completion.
     */
    public static class ChunkAndFailPlugin extends Plugin implements ActionPlugin {

        static final String ROUTE = "/_test/chunk_and_fail";
        static final String FAIL_AFTER_BYTES_PARAM = "fail_after_bytes";

        static String randomRequestUri() {
            return ROUTE + '?' + FAIL_AFTER_BYTES_PARAM + '=' + between(0, ByteSizeUnit.MB.toIntBytes(2));
        }

        @Override
        public Collection<RestHandler> getRestHandlers(
            Settings settings,
            NamedWriteableRegistry namedWriteableRegistry,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster,
            Predicate<NodeFeature> clusterSupportsFeature
        ) {
            return List.of(new BaseRestHandler() {
                @Override
                public String getName() {
                    return ROUTE;
                }

                @Override
                public List<Route> routes() {
                    return List.of(new Route(GET, ROUTE));
                }

                @Override
                protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
                    final var failAfterBytes = request.paramAsInt(FAIL_AFTER_BYTES_PARAM, -1);
                    if (failAfterBytes < 0) {
                        throw new IllegalArgumentException("[" + FAIL_AFTER_BYTES_PARAM + "] must be present and non-negative");
                    }
                    return channel -> randomExecutor(client.threadPool()).execute(
                        () -> channel.sendResponse(RestResponse.chunked(RestStatus.OK, new ChunkedRestResponseBodyPart() {
                            int bytesRemaining = failAfterBytes;

                            @Override
                            public boolean isPartComplete() {
                                return false;
                            }

                            @Override
                            public boolean isLastPart() {
                                return true;
                            }

                            @Override
                            public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
                                fail("no continuations here");
                            }

                            @Override
                            public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
                                assert bytesRemaining >= 0 : "already failed";
                                if (bytesRemaining == 0) {
                                    bytesRemaining = -1;
                                    throw new IOException("simulated failure");
                                } else {
                                    final var bytesToSend = between(1, bytesRemaining);
                                    bytesRemaining -= bytesToSend;
                                    return ReleasableBytesReference.wrap(new ZeroBytesReference(bytesToSend));
                                }
                            }

                            @Override
                            public String getResponseContentTypeString() {
                                return RestResponse.TEXT_CONTENT_TYPE;
                            }
                        }, null))
                    );
                }
            });
        }
    }

    /**
     * Adds an HTTP route that only responds when starting to process a second request, ensuring that there is always at least one in-flight
     * request in the pipeline which keeps a connection from becoming idle.
     */
    public static class KeepPipeliningPlugin extends Plugin implements ActionPlugin {

        static final String ROUTE = "/_test/keep_pipelining";
        static final String RESPOND_IMMEDIATELY = "respond_immediately";

        @Override
        public Collection<RestHandler> getRestHandlers(
            Settings settings,
            NamedWriteableRegistry namedWriteableRegistry,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster,
            Predicate<NodeFeature> clusterSupportsFeature
        ) {
            return List.of(new BaseRestHandler() {

                private SubscribableListener<Void> lastRequestTrigger = new SubscribableListener<>();

                @Override
                public String getName() {
                    return ROUTE;
                }

                @Override
                public List<Route> routes() {
                    return List.of(new Route(GET, ROUTE));
                }

                @Override
                protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
                    final var respondImmediately = request.paramAsBoolean(RESPOND_IMMEDIATELY, false);
                    return channel -> {
                        // all happens on a single thread in these tests, no need for concurrency protection
                        final var previousRequestTrigger = lastRequestTrigger;
                        lastRequestTrigger = respondImmediately ? SubscribableListener.nullSuccess() : new SubscribableListener<>();
                        lastRequestTrigger.addListener(new EmptyResponseListener(channel).map(ignored -> ActionResponse.Empty.INSTANCE));
                        previousRequestTrigger.onResponse(null);
                    };
                }
            });
        }
    }
}
