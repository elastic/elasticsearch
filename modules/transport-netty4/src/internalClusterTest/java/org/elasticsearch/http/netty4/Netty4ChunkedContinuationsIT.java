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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.http.HttpBodyTracer;
import org.elasticsearch.http.HttpRouteStats;
import org.elasticsearch.http.HttpRouteStatsTracker;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4Utils;
import org.elasticsearch.xcontent.ToXContentObject;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestResponse.TEXT_CONTENT_TYPE;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
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
        try (var ignored = withResourceTracker()) {
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
        final var resources = new ArrayList<Releasable>();
        try (var ignored = Releasables.wrap(resources)) {
            resources.add(withResourceTracker());
            final var executor = EsExecutors.newFixed(
                "test",
                1,
                -1,
                EsExecutors.daemonThreadFactory(Settings.EMPTY, "test"),
                new ThreadContext(Settings.EMPTY),
                EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
            );
            resources.add(() -> assertTrue(ThreadPool.terminate(executor, 10, TimeUnit.SECONDS)));
            var loggingFinishedLatch = new CountDownLatch(1);
            MockLog.assertThatLogger(
                () -> assertEquals(
                    expectedBody,
                    ChunkedLoggingStreamTestUtils.getDecodedLoggedBody(
                        logger,
                        Level.INFO,
                        "response body",
                        ReferenceDocs.HTTP_TRACER,
                        () -> {
                            final var request = new Request("GET", YieldsContinuationsPlugin.ROUTE);
                            request.addParameter("error_trace", "true");
                            getRestClient().performRequest(request);
                            safeAwait(loggingFinishedLatch);
                        }
                    ).utf8ToString()
                ),
                HttpBodyTracer.class,
                new MockLog.LoggingExpectation() {
                    final Pattern messagePattern = Pattern.compile("^\\[[1-9][0-9]*] (response body.*)");

                    @Override
                    public void match(LogEvent event) {
                        final var formattedMessage = event.getMessage().getFormattedMessage();
                        final var matcher = messagePattern.matcher(formattedMessage);
                        if (matcher.matches()) {
                            executor.execute(() -> {
                                logger.info("{}", matcher.group(1));
                                if (formattedMessage.contains(ReferenceDocs.HTTP_TRACER.toString())) {
                                    loggingFinishedLatch.countDown();
                                }
                            });
                        }
                    }

                    @Override
                    public void assertMatched() {}
                }
            );
        }
    }

    public void testResponseBodySizeStats() throws IOException {
        try (var ignored = withResourceTracker()) {
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
        try (var ignored = withResourceTracker(); var nettyClient = new Netty4HttpClient()) {
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
        try (var ignored = withResourceTracker(); var nettyClient = new Netty4HttpClient()) {
            final var failIndex = between(0, 2);
            final var responses = nettyClient.get(
                randomFrom(internalCluster().getInstance(HttpServerTransport.class).boundAddress().boundAddresses()).address(),
                YieldsContinuationsPlugin.ROUTE,
                YieldsContinuationsPlugin.ROUTE + "?" + YieldsContinuationsPlugin.FAIL_INDEX_PARAM + "=" + failIndex
            );

            if (failIndex == 0) {
                assertThat(
                    responses,
                    anyOf(
                        // might get a 500 response if the failure is early enough
                        hasSize(2),
                        // might get no response before channel closed
                        hasSize(1),
                        // might even close the channel before flushing the previous response
                        hasSize(0)
                    )
                );

                if (responses.size() == 2) {
                    assertEquals(expectedBody, Netty4Utils.toBytesReference(responses.get(0).content()).utf8ToString());
                    assertEquals(500, responses.get(1).status().code());
                }
            } else {
                assertThat(responses, hasSize(1));
            }

            if (responses.size() > 0) {
                assertEquals(expectedBody, Netty4Utils.toBytesReference(responses.get(0).content()).utf8ToString());
                assertEquals(200, responses.get(0).status().code());
            }
        }
    }

    public void testClientCancellation() {
        try (var ignored = withResourceTracker()) {
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
                safeSleep(scaledRandomIntBetween(10, 500)); // make it more likely the request started executing
            }
            cancellable.cancel();
        } // closing the resource tracker ensures that everything is released, including all response chunks and the overall response
    }

    private static Releasable withResourceTracker() {
        assertNull(refs);
        final var latch = new CountDownLatch(1);
        refs = AbstractRefCounted.of(latch::countDown);
        return () -> {
            refs.decRef();
            try {
                safeAwait(latch);
            } finally {
                refs = null;
            }
        };
    }

    private static volatile RefCounted refs = null;

    /**
     * Adds a REST route which yields a sequence of continuations which are computed asynchronously, effectively pausing after each one..
     */
    public static class YieldsContinuationsPlugin extends Plugin implements ActionPlugin {
        static final String ROUTE = "/_test/yields_continuations";
        static final String FAIL_INDEX_PARAM = "fail_index";

        private static final ActionType<YieldsContinuationsPlugin.Response> TYPE = new ActionType<>("test:yields_continuations");

        @Override
        public Collection<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(TYPE, TransportYieldsContinuationsAction.class));
        }

        public static class Request extends ActionRequest {
            final int failIndex;

            public Request(int failIndex) {
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

            public Response(int failIndex, Executor executor) {
                this.failIndex = failIndex;
                this.executor = executor;
            }

            @Override
            public void writeTo(StreamOutput out) {
                TransportAction.localOnly();
            }

            public ChunkedRestResponseBodyPart getFirstResponseBodyPart() {
                return getResponseBodyPart(0);
            }

            private ChunkedRestResponseBodyPart getResponseBodyPart(int batchIndex) {
                if (batchIndex == failIndex && randomBoolean()) {
                    throw new ElasticsearchException("simulated failure creating next batch");
                }
                return new ChunkedRestResponseBodyPart() {

                    private final Iterator<String> lines = Iterators.forRange(0, 3, i -> "batch-" + batchIndex + "-chunk-" + i + "\n");

                    @Override
                    public boolean isPartComplete() {
                        return lines.hasNext() == false;
                    }

                    @Override
                    public boolean isLastPart() {
                        return batchIndex == 2;
                    }

                    @Override
                    public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
                        executor.execute(ActionRunnable.supply(listener, () -> getResponseBodyPart(batchIndex + 1)));
                    }

                    @Override
                    public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
                        assertTrue(lines.hasNext());
                        refs.mustIncRef();
                        final var output = new RecyclerBytesStreamOutput(recycler);
                        boolean success = false;
                        try {
                            try (var writer = new OutputStreamWriter(Streams.flushOnCloseStream(output), StandardCharsets.UTF_8)) {
                                writer.write(lines.next());
                            }
                            final var result = new ReleasableBytesReference(output.bytes(), Releasables.wrap(output, refs::decRef));
                            if (batchIndex == failIndex) {
                                throw new ElasticsearchException("simulated failure encoding chunk");
                            }
                            success = true;
                            return result;
                        } finally {
                            if (success == false) {
                                refs.decRef();
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
                this(actionFilters, transportService, transportService.getThreadPool().executor(ThreadPool.Names.GENERIC));
            }

            TransportYieldsContinuationsAction(ActionFilters actionFilters, TransportService transportService, ExecutorService executor) {
                super(TYPE.name(), actionFilters, transportService.getTaskManager(), executor);
                this.executor = executor;
            }

            @Override
            protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
                var response = new Response(request.failIndex, executor);
                try {
                    listener.onResponse(response);
                } catch (Exception e) {
                    ESTestCase.fail(e);
                }
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
                    refs.mustIncRef();
                    return new RestChannelConsumer() {

                        @Override
                        public void close() {
                            refs.decRef();
                        }

                        @Override
                        public void accept(RestChannel channel) {
                            refs.mustIncRef();
                            client.execute(TYPE, new Request(failIndex), new RestActionListener<>(channel) {
                                @Override
                                protected void processResponse(Response response) {
                                    try {
                                        final var responseBody = response.getFirstResponseBodyPart();
                                        // preceding line might fail, so needs to be done before acquiring the sendResponse ref
                                        refs.mustIncRef();
                                        channel.sendResponse(RestResponse.chunked(RestStatus.OK, responseBody, refs::decRef));
                                    } finally {
                                        refs.decRef(); // release the ref acquired at the top of accept()
                                    }
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
    public static class InfiniteContinuationsPlugin extends Plugin implements ActionPlugin {
        static final String ROUTE = "/_test/infinite_continuations";

        private static final ActionType<Response> TYPE = new ActionType<>("test:infinite_continuations");

        @Override
        public Collection<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(TYPE, TransportInfiniteContinuationsAction.class));
        }

        public static class Request extends ActionRequest {
            @Override
            public ActionRequestValidationException validate() {
                return null;
            }
        }

        public static class Response extends ActionResponse {
            private final Executor executor;
            volatile boolean computingContinuation;
            boolean recursive = false;

            public Response(Executor executor) {
                this.executor = executor;
            }

            @Override
            public void writeTo(StreamOutput out) {
                TransportAction.localOnly();
            }

            public ChunkedRestResponseBodyPart getResponseBodyPart() {
                return new ChunkedRestResponseBodyPart() {
                    private final Iterator<String> lines = Iterators.single("infinite response\n");

                    @Override
                    public boolean isPartComplete() {
                        return lines.hasNext() == false;
                    }

                    @Override
                    public boolean isLastPart() {
                        return false;
                    }

                    @Override
                    public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
                        assertFalse(recursive);
                        recursive = true;
                        try {
                            computingContinuation = true;
                            executor.execute(ActionRunnable.supply(listener, () -> {
                                computingContinuation = false;
                                return getResponseBodyPart();
                            }));
                        } finally {
                            recursive = false;
                        }
                    }

                    @Override
                    public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) {
                        assertTrue(lines.hasNext());
                        refs.mustIncRef();
                        return new ReleasableBytesReference(new BytesArray(lines.next()), refs::decRef);
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
                this(actionFilters, transportService, transportService.getThreadPool().executor(ThreadPool.Names.GENERIC));
            }

            TransportInfiniteContinuationsAction(ActionFilters actionFilters, TransportService transportService, ExecutorService executor) {
                super(TYPE.name(), actionFilters, transportService.getTaskManager(), executor);
                this.executor = executor;
            }

            @Override
            protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
                var response = new Response(randomFrom(executor, EsExecutors.DIRECT_EXECUTOR_SERVICE));
                try {
                    listener.onResponse(response);
                } catch (Exception e) {
                    ESTestCase.fail(e);
                }
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
                    final var localRefs = refs; // single volatile read
                    if (localRefs != null && localRefs.tryIncRef()) {
                        return new RestChannelConsumer() {
                            @Override
                            public void close() {
                                localRefs.decRef();
                            }

                            @Override
                            public void accept(RestChannel channel) {
                                localRefs.mustIncRef();
                                client.execute(TYPE, new Request(), ActionListener.releaseAfter(new RestActionListener<>(channel) {
                                    @Override
                                    protected void processResponse(Response response) {
                                        localRefs.mustIncRef();
                                        channel.sendResponse(RestResponse.chunked(RestStatus.OK, response.getResponseBodyPart(), () -> {
                                            // cancellation notification only happens while processing a continuation, not while computing
                                            // the next one; prompt cancellation requires use of something like RestCancellableNodeClient
                                            assertFalse(response.computingContinuation);
                                            assertSame(localRefs, refs);
                                            localRefs.decRef();
                                        }));
                                    }
                                }, () -> {
                                    assertSame(localRefs, refs);
                                    localRefs.decRef();
                                }));
                            }
                        };
                    } else {
                        throw new TaskCancelledException("request cancelled");
                    }
                }
            });
        }
    }

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
                    refs.mustIncRef();
                    return new RestChannelConsumer() {

                        @Override
                        public void close() {
                            refs.decRef();
                        }

                        @Override
                        public void accept(RestChannel channel) {
                            refs.mustIncRef();
                            addListener(ActionListener.releaseAfter(new RestToXContentListener<>(channel), refs::decRef));
                        }
                    };
                }
            });
        }
    }

    private static final ToXContentObject EMPTY_RESPONSE = (builder, params) -> builder.startObject().endObject();
}
