/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ESNetty4IntegTestCase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ChunkedLoggingStreamTestUtils;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.http.HttpRouteStats;
import org.elasticsearch.http.HttpRouteStatsTracker;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.ChunkedRestResponseBody;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4Utils;
import org.elasticsearch.xcontent.ToXContentObject;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestResponse.TEXT_CONTENT_TYPE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class Netty4ChunkedContinuationsIT extends ESNetty4IntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(
            List.of(YieldsContinuationsPlugin.class, InfiniteContinuationsPlugin.class, CountDown3Plugin.class),
            super.nodePlugins()
        );
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    private static final String expectedBody = """
        batch-0-chunk-0
        batch-0-chunk-1
        batch-0-chunk-2
        batch-1-chunk-0
        batch-1-chunk-1
        batch-1-chunk-2
        batch-2-chunk-0
        batch-2-chunk-1
        batch-2-chunk-2
        """;

    public void testBasic() throws IOException {
        try (var ignored = withRequestTracker()) {
            final var response = getRestClient().performRequest(new Request("GET", YieldsContinuationsPlugin.ROUTE));
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertThat(response.getEntity().getContentType().toString(), containsString(TEXT_CONTENT_TYPE));
            assertTrue(response.getEntity().isChunked());
            final String body;
            try (var reader = new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8)) {
                body = Streams.copyToString(reader);
            }
            assertEquals(expectedBody, body);
        }
    }

    @TestLogging(
        reason = "testing TRACE logging",
        value = "org.elasticsearch.http.HttpTracer:TRACE,org.elasticsearch.http.HttpBodyTracer:TRACE"
    )
    public void testTraceLogging() {

        // slightly awkward test, we can't use ChunkedLoggingStreamTestUtils.getDecodedLoggedBody directly because it asserts that we _only_
        // log one thing and we can't easily separate the request body from the response body logging, so instead we capture the body log
        // message and then log it again with a different logger.

        var loggingFinishedLatch = new CountDownLatch(1);
        var mockLogAppender = new MockLogAppender();
        mockLogAppender.addExpectation(new MockLogAppender.LoggingExpectation() {
            final Pattern messagePattern = Pattern.compile("^\\[[1-9][0-9]*] (response body.*)");

            @Override
            public void match(LogEvent event) {
                final var formattedMessage = event.getMessage().getFormattedMessage();
                final var matcher = messagePattern.matcher(formattedMessage);
                if (matcher.matches()) {
                    logger.info("{}", matcher.group(1));
                    if (formattedMessage.contains(ReferenceDocs.HTTP_TRACER.toString())) {
                        loggingFinishedLatch.countDown();
                    }
                }
            }

            @Override
            public void assertMatched() {}
        });

        try (var ignored = withRequestTracker(); var ignored2 = mockLogAppender.capturing("org.elasticsearch.http.HttpBodyTracer")) {
            assertEquals(
                expectedBody,
                ChunkedLoggingStreamTestUtils.getDecodedLoggedBody(logger, Level.INFO, "response body", ReferenceDocs.HTTP_TRACER, () -> {
                    getRestClient().performRequest(new Request("GET", YieldsContinuationsPlugin.ROUTE));
                    safeAwait(loggingFinishedLatch);
                }).utf8ToString()
            );
            mockLogAppender.assertAllExpectationsMatched();
        }
    }

    public void testResponseBodySizeStats() throws IOException {
        try (var ignored = withRequestTracker()) {
            final var totalResponseSizeBefore = getTotalResponseSize();
            getRestClient().performRequest(new Request("GET", YieldsContinuationsPlugin.ROUTE));
            final var totalResponseSizeAfter = getTotalResponseSize();
            assertEquals(expectedBody.length(), totalResponseSizeAfter - totalResponseSizeBefore);
        }
    }

    private static final HttpRouteStats EMPTY_ROUTE_STATS = new HttpRouteStatsTracker().getStats();

    private long getTotalResponseSize() {
        return client().admin()
            .cluster()
            .prepareNodesStats()
            .clear()
            .setHttp(true)
            .get()
            .getNodes()
            .stream()
            .mapToLong(
                ns -> ns.getHttp().httpRouteStats().getOrDefault(YieldsContinuationsPlugin.ROUTE, EMPTY_ROUTE_STATS).totalResponseSize()
            )
            .sum();
    }

    public void testPipelining() throws Exception {
        try (var ignored = withRequestTracker(); var nettyClient = new Netty4HttpClient()) {
            final var responses = nettyClient.get(
                randomFrom(internalCluster().getInstance(HttpServerTransport.class).boundAddress().boundAddresses()).address(),
                CountDown3Plugin.ROUTE,
                YieldsContinuationsPlugin.ROUTE,
                CountDown3Plugin.ROUTE,
                YieldsContinuationsPlugin.ROUTE,
                CountDown3Plugin.ROUTE
            );

            assertEquals("{}", Netty4Utils.toBytesReference(responses.get(0).content()).utf8ToString());
            assertEquals(expectedBody, Netty4Utils.toBytesReference(responses.get(1).content()).utf8ToString());
            assertEquals("{}", Netty4Utils.toBytesReference(responses.get(2).content()).utf8ToString());
            assertEquals(expectedBody, Netty4Utils.toBytesReference(responses.get(3).content()).utf8ToString());
            assertEquals("{}", Netty4Utils.toBytesReference(responses.get(4).content()).utf8ToString());
        } finally {
            internalCluster().fullRestart(); // reset countdown listener
        }
    }

    public void testContinuationFailure() throws Exception {
        // TODO when https://github.com/netty/netty/issues/13816 addressed, verify that we see the failure properly and no later responses
        try (var ignored = withRequestTracker(); var nettyClient = new Netty4HttpClient()) {
            final var responses = nettyClient.get(
                randomFrom(internalCluster().getInstance(HttpServerTransport.class).boundAddress().boundAddresses()).address(),
                YieldsContinuationsPlugin.ROUTE,
                YieldsContinuationsPlugin.ROUTE + "?" + YieldsContinuationsPlugin.FAIL_INDEX_PARAM + "=1"
            );

            assertEquals(expectedBody, Netty4Utils.toBytesReference(responses.get(0).content()).utf8ToString());
            assertEquals("""
                batch-0-chunk-0
                batch-0-chunk-1
                batch-0-chunk-2
                """, Netty4Utils.toBytesReference(responses.get(1).content()).utf8ToString());
        }
    }

    public void testClientCancellation() {
        try (var ignored = withRequestTracker()) {
            final var cancellable = getRestClient().performRequestAsync(
                new Request("GET", InfiniteContinuationsPlugin.ROUTE),
                new ResponseListener() {
                    @Override
                    public void onSuccess(Response response) {
                        fail("should not succeed");
                    }

                    @Override
                    public void onFailure(Exception exception) {
                        assertThat(exception, instanceOf(CancellationException.class));
                    }
                }
            );
            if (randomBoolean()) {
                safeSleep(10); // make it more likely the request started executing
            }
            cancellable.cancel();
        } // closing the request tracker ensures that everything is released, including all response chunks and the overall response
    }

    private static Releasable withRequestTracker() {
        final var latch = new CountDownLatch(1);
        final var refCounted = AbstractRefCounted.of(latch::countDown);
        setPluginRequestRefs(refCounted);
        return () -> {
            setPluginRequestRefs(RefCounted.ALWAYS_REFERENCED);
            refCounted.decRef();
            safeAwait(latch);
        };
    }

    private static void setPluginRequestRefs(RefCounted refCounted) {
        Iterators.flatMap(
            internalCluster().getInstances(PluginsService.class).iterator(),
            pluginsService -> pluginsService.filterPlugins(HasRequestRefs.class).iterator()
        ).forEachRemaining(p -> p.setRequestRefs(refCounted));
    }

    interface HasRequestRefs {
        void setRequestRefs(RefCounted requestRefs);
    }

    /**
     * Adds a REST route which yields a sequence of continuations which are computed asynchronously, effectively pausing after each one..
     */
    public static class YieldsContinuationsPlugin extends Plugin implements ActionPlugin, HasRequestRefs {
        static final String ROUTE = "/_test/yields_continuations";
        static final String FAIL_INDEX_PARAM = "fail_index";

        private static final ActionType<YieldsContinuationsPlugin.Response> TYPE = new ActionType<>("test:yields_continuations");

        private RefCounted requestRefs = RefCounted.ALWAYS_REFERENCED;

        @Override
        public Collection<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(TYPE, TransportYieldsContinuationsAction.class));
        }

        @Override
        public void setRequestRefs(RefCounted requestRefs) {
            this.requestRefs = requestRefs;
        }

        public static class Request extends ActionRequest {
            final RefCounted requestRefs;
            final int failIndex;

            public Request(RefCounted requestRefs, int failIndex) {
                this.requestRefs = requestRefs;
                this.failIndex = failIndex;
            }

            @Override
            public ActionRequestValidationException validate() {
                return null;
            }
        }

        public static class Response extends ActionResponse {
            private final int failIndex;
            private final Executor executor;
            private final RefCounted requestRefs;

            public Response(int failIndex, Executor executor, RefCounted requestRefs) {
                this.failIndex = failIndex;
                this.executor = executor;
                this.requestRefs = requestRefs;
            }

            @Override
            public void writeTo(StreamOutput out) {
                TransportAction.localOnly();
            }

            public ChunkedRestResponseBody getChunkedBody() {
                return getChunkBatch(0);
            }

            private ChunkedRestResponseBody getChunkBatch(int batchIndex) {
                if (batchIndex == failIndex) {
                    throw new ElasticsearchException("simulated failure");
                }
                return new ChunkedRestResponseBody() {

                    private final Iterator<String> lines = Iterators.forRange(0, 3, i -> "batch-" + batchIndex + "-chunk-" + i + "\n");

                    @Override
                    public boolean isDone() {
                        return lines.hasNext() == false;
                    }

                    @Override
                    public boolean isEndOfResponse() {
                        return batchIndex == 2;
                    }

                    @Override
                    public void getContinuation(ActionListener<ChunkedRestResponseBody> listener) {
                        executor.execute(ActionRunnable.supply(listener, () -> getChunkBatch(batchIndex + 1)));
                    }

                    @Override
                    public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
                        assertTrue(lines.hasNext());
                        requestRefs.mustIncRef();
                        final var output = new RecyclerBytesStreamOutput(recycler);
                        boolean success = false;
                        try {
                            try (var writer = new OutputStreamWriter(Streams.flushOnCloseStream(output), StandardCharsets.UTF_8)) {
                                writer.write(lines.next());
                            }
                            final var result = new ReleasableBytesReference(output.bytes(), Releasables.wrap(output, requestRefs::decRef));
                            success = true;
                            return result;
                        } finally {
                            if (success == false) {
                                requestRefs.decRef();
                                output.close();
                            }
                        }
                    }

                    @Override
                    public String getResponseContentTypeString() {
                        assertEquals(0, batchIndex);
                        return TEXT_CONTENT_TYPE;
                    }
                };
            }
        }

        public static class TransportYieldsContinuationsAction extends TransportAction<Request, Response> {
            private final ExecutorService executor;

            @Inject
            public TransportYieldsContinuationsAction(ActionFilters actionFilters, TransportService transportService) {
                super(TYPE.name(), actionFilters, transportService.getTaskManager());
                executor = transportService.getThreadPool().executor(ThreadPool.Names.GENERIC);
            }

            @Override
            protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
                executor.execute(ActionRunnable.supply(listener, () -> new Response(request.failIndex, executor, request.requestRefs)));
            }
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
                    final var failIndex = request.paramAsInt(FAIL_INDEX_PARAM, Integer.MAX_VALUE);
                    final var localRequestRefs = requestRefs;
                    localRequestRefs.mustIncRef();
                    return new RestChannelConsumer() {

                        @Override
                        public void close() {
                            localRequestRefs.decRef();
                        }

                        @Override
                        public void accept(RestChannel channel) {
                            localRequestRefs.mustIncRef();
                            client.execute(TYPE, new Request(localRequestRefs, failIndex), new RestActionListener<>(channel) {
                                @Override
                                protected void processResponse(Response response) {
                                    channel.sendResponse(
                                        RestResponse.chunked(RestStatus.OK, response.getChunkedBody(), localRequestRefs::decRef)
                                    );
                                }
                            });
                        }
                    };
                }
            });
        }
    }

    /**
     * Adds a REST route which yields an infinite sequence of continuations which can only be stopped by the client closing the connection.
     */
    public static class InfiniteContinuationsPlugin extends Plugin implements ActionPlugin, HasRequestRefs {
        static final String ROUTE = "/_test/infinite_continuations";

        private static final ActionType<Response> TYPE = new ActionType<>("test:infinite_continuations");

        private RefCounted requestRefs = RefCounted.ALWAYS_REFERENCED;

        @Override
        public Collection<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(TYPE, TransportInfiniteContinuationsAction.class));
        }

        @Override
        public void setRequestRefs(RefCounted requestRefs) {
            this.requestRefs = requestRefs;
        }

        public static class Request extends ActionRequest {
            final RefCounted requestRefs;

            public Request(RefCounted requestRefs) {
                this.requestRefs = requestRefs;
            }

            @Override
            public ActionRequestValidationException validate() {
                return null;
            }
        }

        public static class Response extends ActionResponse {
            private final Executor executor;
            private final RefCounted requestRefs;

            public Response(Executor executor, RefCounted requestRefs) {
                this.executor = executor;
                this.requestRefs = requestRefs;
            }

            @Override
            public void writeTo(StreamOutput out) {
                TransportAction.localOnly();
            }

            public ChunkedRestResponseBody getChunkedBody() {
                return new ChunkedRestResponseBody() {
                    private final Iterator<String> lines = Iterators.single("infinite response\n");

                    @Override
                    public boolean isDone() {
                        return lines.hasNext() == false;
                    }

                    @Override
                    public boolean isEndOfResponse() {
                        return false;
                    }

                    @Override
                    public void getContinuation(ActionListener<ChunkedRestResponseBody> listener) {
                        executor.execute(ActionRunnable.supply(listener, () -> getChunkedBody()));
                    }

                    @Override
                    public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) {
                        assertTrue(lines.hasNext());
                        requestRefs.mustIncRef();
                        return new ReleasableBytesReference(new BytesArray(lines.next()), requestRefs::decRef);
                    }

                    @Override
                    public String getResponseContentTypeString() {
                        return TEXT_CONTENT_TYPE;
                    }
                };
            }
        }

        public static class TransportInfiniteContinuationsAction extends TransportAction<Request, Response> {
            private final ExecutorService executor;

            @Inject
            public TransportInfiniteContinuationsAction(ActionFilters actionFilters, TransportService transportService) {
                super(TYPE.name(), actionFilters, transportService.getTaskManager());
                this.executor = transportService.getThreadPool().executor(ThreadPool.Names.GENERIC);
            }

            @Override
            protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
                executor.execute(
                    ActionRunnable.supply(
                        ActionTestUtils.assertNoFailureListener(listener::onResponse),
                        () -> new Response(executor, request.requestRefs)
                    )
                );
            }
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
                    final var localRequestRefs = requestRefs;
                    localRequestRefs.mustIncRef();
                    return new RestChannelConsumer() {
                        @Override
                        public void close() {
                            localRequestRefs.decRef();
                        }

                        @Override
                        public void accept(RestChannel channel) {
                            localRequestRefs.mustIncRef();
                            client.execute(TYPE, new Request(localRequestRefs), new RestActionListener<>(channel) {
                                @Override
                                protected void processResponse(Response response) {
                                    channel.sendResponse(
                                        RestResponse.chunked(RestStatus.OK, response.getChunkedBody(), localRequestRefs::decRef)
                                    );
                                }
                            });
                        }
                    };
                }
            });
        }
    }

    /**
     * Adds an HTTP route that waits for 3 concurrent executions before returning any of them
     */
    public static class CountDown3Plugin extends Plugin implements ActionPlugin, HasRequestRefs {

        static final String ROUTE = "/_test/countdown_3";

        private RefCounted requestRefs = RefCounted.ALWAYS_REFERENCED;

        @Override
        public void setRequestRefs(RefCounted requestRefs) {
            this.requestRefs = requestRefs;
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
                    requestRefs.mustIncRef();
                    return new RestChannelConsumer() {

                        @Override
                        public void close() {
                            requestRefs.decRef();
                        }

                        @Override
                        public void accept(RestChannel channel) {
                            requestRefs.mustIncRef();
                            addListener(ActionListener.releaseAfter(new RestToXContentListener<>(channel), requestRefs::decRef));
                        }
                    };
                }
            });
        }
    }

    private static final ToXContentObject EMPTY_RESPONSE = (builder, params) -> builder.startObject().endObject();
}
