/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.bulk.IncrementalBulkService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.internal.RestExtension;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestControllerTests;
import org.elasticsearch.rest.RestHeaderDefinition;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.net.InetAddress.getByName;
import static java.util.Arrays.asList;
import static org.elasticsearch.http.AbstractHttpServerTransport.resolvePublishPort;
import static org.elasticsearch.http.DefaultRestChannel.CLOSE;
import static org.elasticsearch.http.DefaultRestChannel.CONNECTION;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_CLIENT_STATS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_SERVER_SHUTDOWN_GRACE_PERIOD;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class AbstractHttpServerTransportTests extends ESTestCase {

    private NetworkService networkService;
    private ThreadPool threadPool;
    private Recycler<BytesRef> recycler;

    private static final int LONG_GRACE_PERIOD_MS = 20_000;
    private static final int SHORT_GRACE_PERIOD_MS = 1;

    @Before
    public void setup() throws Exception {
        networkService = new NetworkService(Collections.emptyList());
        threadPool = new TestThreadPool("test");
        recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
    }

    @After
    public void shutdown() throws Exception {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        threadPool = null;
        networkService = null;
        recycler = null;
    }

    public void testHttpPublishPort() throws Exception {
        int boundPort = randomIntBetween(9000, 9100);
        int otherBoundPort = randomIntBetween(9200, 9300);

        int publishPort = resolvePublishPort(
            Settings.builder().put(HttpTransportSettings.SETTING_HTTP_PUBLISH_PORT.getKey(), 9080).build(),
            randomAddresses(),
            getByName("127.0.0.2")
        );
        assertThat("Publish port should be explicitly set to 9080", publishPort, equalTo(9080));

        publishPort = resolvePublishPort(
            Settings.EMPTY,
            asList(address("127.0.0.1", boundPort), address("127.0.0.2", otherBoundPort)),
            getByName("127.0.0.1")
        );
        assertThat("Publish port should be derived from matched address", publishPort, equalTo(boundPort));

        publishPort = resolvePublishPort(
            Settings.EMPTY,
            asList(address("127.0.0.1", boundPort), address("127.0.0.2", boundPort)),
            getByName("127.0.0.3")
        );
        assertThat("Publish port should be derived from unique port of bound addresses", publishPort, equalTo(boundPort));

        final BindHttpException e = expectThrows(
            BindHttpException.class,
            () -> resolvePublishPort(
                Settings.EMPTY,
                asList(address("127.0.0.1", boundPort), address("127.0.0.2", otherBoundPort)),
                getByName("127.0.0.3")
            )
        );
        assertThat(e.getMessage(), containsString("Failed to auto-resolve http publish port"));

        publishPort = resolvePublishPort(
            Settings.EMPTY,
            asList(address("0.0.0.0", boundPort), address("127.0.0.2", otherBoundPort)),
            getByName("127.0.0.1")
        );
        assertThat("Publish port should be derived from matching wildcard address", publishPort, equalTo(boundPort));

        if (NetworkUtils.SUPPORTS_V6) {
            publishPort = resolvePublishPort(
                Settings.EMPTY,
                asList(address("0.0.0.0", boundPort), address("127.0.0.2", otherBoundPort)),
                getByName("::1")
            );
            assertThat("Publish port should be derived from matching wildcard address", publishPort, equalTo(boundPort));
        }
    }

    public void testDispatchDoesNotModifyThreadContext() {
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {

            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                threadContext.putHeader("foo", "bar");
                threadContext.putTransient("bar", "baz");
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                threadContext.putHeader("foo_bad", "bar");
                threadContext.putTransient("bar_bad", "baz");
            }

        };

        try (
            AbstractHttpServerTransport transport = new AbstractHttpServerTransport(
                Settings.EMPTY,
                networkService,
                recycler,
                threadPool,
                xContentRegistry(),
                dispatcher,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                Tracer.NOOP
            ) {

                @Override
                protected HttpServerChannel bind(InetSocketAddress hostAddress) {
                    return null;
                }

                @Override
                protected void doStart() {

                }

                @Override
                protected void stopInternal() {

                }

                @Override
                public HttpStats stats() {
                    return null;
                }
            }
        ) {

            transport.dispatchRequest(null, null, null);
            assertNull(threadPool.getThreadContext().getHeader("foo"));
            assertNull(threadPool.getThreadContext().getTransient("bar"));

            transport.dispatchRequest(null, null, new Exception());
            assertNull(threadPool.getThreadContext().getHeader("foo_bad"));
            assertNull(threadPool.getThreadContext().getTransient("bar_bad"));
        }
    }

    public void testRequestHeadersPopulateThreadContext() {
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {

            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                // specified request headers value are copied into the thread context
                assertEquals("true", threadContext.getHeader("header.1"));
                assertEquals("true", threadContext.getHeader("header.2"));
                // trace start time is also set
                assertThat(threadContext.getTransient(Task.TRACE_START_TIME), notNullValue());
                // but unknown headers are not copied at all
                assertNull(threadContext.getHeader("header.3"));
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                // no request headers are copied in to the context of malformed requests
                assertNull(threadContext.getHeader("header.1"));
                assertNull(threadContext.getHeader("header.2"));
                assertNull(threadContext.getHeader("header.3"));
                assertNull(threadContext.getTransient(Task.TRACE_START_TIME));
            }

        };
        // the set of headers to copy
        final Set<RestHeaderDefinition> headers = new HashSet<>(
            Arrays.asList(new RestHeaderDefinition("header.1", true), new RestHeaderDefinition("header.2", true))
        );
        // sample request headers to test with
        final Map<String, List<String>> restHeaders = new HashMap<>();
        restHeaders.put("header.1", Collections.singletonList("true"));
        restHeaders.put("header.2", Collections.singletonList("true"));
        restHeaders.put("header.3", Collections.singletonList("true"));
        final RestRequest fakeRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(restHeaders).build();
        final RestControllerTests.AssertingChannel channel = new RestControllerTests.AssertingChannel(
            fakeRequest,
            false,
            RestStatus.BAD_REQUEST
        );

        try (
            AbstractHttpServerTransport transport = new AbstractHttpServerTransport(
                Settings.EMPTY,
                networkService,
                recycler,
                threadPool,
                xContentRegistry(),
                dispatcher,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                Tracer.NOOP
            ) {

                @Override
                protected HttpServerChannel bind(InetSocketAddress hostAddress) {
                    return null;
                }

                @Override
                protected void doStart() {

                }

                @Override
                protected void stopInternal() {

                }

                @Override
                public HttpStats stats() {
                    return null;
                }

                @Override
                protected void populatePerRequestThreadContext(RestRequest restRequest, ThreadContext threadContext) {
                    getFakeActionModule(headers).copyRequestHeadersToThreadContext(restRequest.getHttpRequest(), threadContext);
                }
            }
        ) {
            transport.dispatchRequest(fakeRequest, channel, null);
            // headers are "null" here, aka not present, because the thread context changes containing them is to be confined to the request
            assertNull(threadPool.getThreadContext().getHeader("header.1"));
            assertNull(threadPool.getThreadContext().getHeader("header.2"));
            assertNull(threadPool.getThreadContext().getHeader("header.3"));
            transport.dispatchRequest(null, null, new Exception());
            // headers are "null" here, aka not present, because the thread context changes containing them is to be confined to the request
            assertNull(threadPool.getThreadContext().getHeader("header.1"));
            assertNull(threadPool.getThreadContext().getHeader("header.2"));
            assertNull(threadPool.getThreadContext().getHeader("header.3"));
        }
    }

    /**
     * Check that the REST controller picks up and propagates W3C trace context headers via the {@link ThreadContext}.
     * @see <a href="https://www.w3.org/TR/trace-context/">Trace Context - W3C Recommendation</a>
     */
    public void testTraceParentAndTraceId() {
        final String traceParentValue = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        final AtomicReference<Instant> traceStartTimeRef = new AtomicReference<>();
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {

            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                assertThat(threadContext.getHeader(Task.TRACE_ID), equalTo("0af7651916cd43dd8448eb211c80319c"));
                assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), nullValue());
                assertThat(threadContext.getTransient("parent_" + Task.TRACE_PARENT_HTTP_HEADER), equalTo(traceParentValue));
                // request trace start time is also set
                assertTrue(traceStartTimeRef.compareAndSet(null, threadContext.getTransient(Task.TRACE_START_TIME)));
                assertNotNull(traceStartTimeRef.get());
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                // but they're not copied in for bad requests
                assertThat(threadContext.getHeader(Task.TRACE_ID), nullValue());
                assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), nullValue());
                assertThat(threadContext.getTransient("parent_" + Task.TRACE_PARENT_HTTP_HEADER), nullValue());
                assertThat(threadContext.getTransient(Task.TRACE_START_TIME), nullValue());
            }

        };
        // the set of headers to copy
        Set<RestHeaderDefinition> headers = Set.of(new RestHeaderDefinition(Task.TRACE_PARENT_HTTP_HEADER, false));
        // sample request headers to test with
        Map<String, List<String>> restHeaders = new HashMap<>();
        restHeaders.put(Task.TRACE_PARENT_HTTP_HEADER, Collections.singletonList(traceParentValue));
        RestRequest fakeRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(restHeaders).build();
        RestControllerTests.AssertingChannel channel = new RestControllerTests.AssertingChannel(fakeRequest, false, RestStatus.BAD_REQUEST);

        try (
            AbstractHttpServerTransport transport = new AbstractHttpServerTransport(
                Settings.EMPTY,
                networkService,
                recycler,
                threadPool,
                xContentRegistry(),
                dispatcher,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                Tracer.NOOP
            ) {

                @Override
                protected HttpServerChannel bind(InetSocketAddress hostAddress) {
                    return null;
                }

                @Override
                protected void doStart() {}

                @Override
                protected void stopInternal() {}

                @Override
                public HttpStats stats() {
                    return null;
                }

                @Override
                protected void populatePerRequestThreadContext(RestRequest restRequest, ThreadContext threadContext) {
                    getFakeActionModule(headers).copyRequestHeadersToThreadContext(restRequest.getHttpRequest(), threadContext);
                }
            }
        ) {
            final var systemTimeBeforeRequest = System.currentTimeMillis();
            transport.dispatchRequest(fakeRequest, channel, null);
            final var systemTimeAfterRequest = System.currentTimeMillis();
            // headers are "null" here, aka not present, because the thread context changes containing them is to be confined to the request
            assertThat(threadPool.getThreadContext().getHeader(Task.TRACE_ID), nullValue());
            assertThat(threadPool.getThreadContext().getHeader(Task.TRACE_PARENT_HTTP_HEADER), nullValue());
            assertThat(threadPool.getThreadContext().getTransient("parent_" + Task.TRACE_PARENT_HTTP_HEADER), nullValue());

            // system clock is not _technically_ monotonic but in practice it's very unlikely to see a discontinuity here
            assertThat(
                traceStartTimeRef.get().toEpochMilli(),
                allOf(greaterThanOrEqualTo(systemTimeBeforeRequest), lessThanOrEqualTo(systemTimeAfterRequest))
            );

            transport.dispatchRequest(null, null, new Exception());
            // headers are "null" here, aka not present, because the thread context changes containing them is to be confined to the request
            assertThat(threadPool.getThreadContext().getHeader(Task.TRACE_ID), nullValue());
            assertThat(threadPool.getThreadContext().getHeader(Task.TRACE_PARENT_HTTP_HEADER), nullValue());
            assertThat(threadPool.getThreadContext().getTransient("parent_" + Task.TRACE_PARENT_HTTP_HEADER), nullValue());
        }
    }

    public void testHandlingCompatibleVersionParsingErrors() {
        // a compatible version exception (v8 on accept and v9 on content-type) should be handled gracefully
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        try (
            AbstractHttpServerTransport transport = failureAssertingtHttpServerTransport(clusterSettings, Set.of("Accept", "Content-Type"))
        ) {
            Map<String, List<String>> headers = new HashMap<>();
            headers.put("Accept", Collections.singletonList("aaa/bbb;compatible-with=8"));
            headers.put("Content-Type", Collections.singletonList("aaa/bbb;compatible-with=9"));

            FakeRestRequest.FakeHttpRequest fakeHttpRequest = new FakeRestRequest.FakeHttpRequest(
                RestRequest.Method.GET,
                "/",
                new BytesArray(randomByteArrayOfLength(between(1, 20))),
                headers
            );

            transport.incomingRequest(fakeHttpRequest, new TestHttpChannel());
        }
    }

    public void testIncorrectHeaderHandling() {

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        try (AbstractHttpServerTransport transport = failureAssertingtHttpServerTransport(clusterSettings, Set.of("Accept"))) {

            Map<String, List<String>> headers = new HashMap<>();
            headers.put("Accept", List.of("incorrectHeader"));
            if (randomBoolean()) {
                headers.put("Content-Type", List.of("alsoIncorrectHeader"));
            }

            FakeRestRequest.FakeHttpRequest fakeHttpRequest = new FakeRestRequest.FakeHttpRequest(
                RestRequest.Method.GET,
                "/",
                null,
                headers
            );

            transport.incomingRequest(fakeHttpRequest, new TestHttpChannel());
        }
        try (AbstractHttpServerTransport transport = failureAssertingtHttpServerTransport(clusterSettings, Set.of("Content-Type"))) {
            Map<String, List<String>> headers = new HashMap<>();
            if (randomBoolean()) {
                headers.put("Accept", List.of("application/json"));
            }
            headers.put("Content-Type", List.of("incorrectHeader"));

            FakeRestRequest.FakeHttpRequest fakeHttpRequest = new FakeRestRequest.FakeHttpRequest(
                RestRequest.Method.GET,
                "/",
                null,
                headers
            );

            transport.incomingRequest(fakeHttpRequest, new TestHttpChannel());
        }
    }

    private AbstractHttpServerTransport failureAssertingtHttpServerTransport(
        ClusterSettings clusterSettings,
        final Set<String> failedHeaderNames
    ) {
        return new AbstractHttpServerTransport(
            Settings.EMPTY,
            networkService,
            recycler,
            threadPool,
            xContentRegistry(),
            new HttpServerTransport.Dispatcher() {
                @Override
                public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
                    Assert.fail();
                }

                @Override
                public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
                    assertThat(cause, instanceOf(RestRequest.MediaTypeHeaderException.class));
                    RestRequest.MediaTypeHeaderException mediaTypeHeaderException = (RestRequest.MediaTypeHeaderException) cause;
                    assertThat(mediaTypeHeaderException.getFailedHeaderNames(), equalTo(failedHeaderNames));
                    assertThat(mediaTypeHeaderException.getMessage(), equalTo("Invalid media-type value on headers " + failedHeaderNames));
                }
            },
            clusterSettings,
            Tracer.NOOP
        ) {
            @Override
            protected HttpServerChannel bind(InetSocketAddress hostAddress) {
                return null;
            }

            @Override
            protected void doStart() {}

            @Override
            protected void stopInternal() {}

            @Override
            public HttpStats stats() {
                return null;
            }
        };
    }

    @TestLogging(value = "org.elasticsearch.http.HttpTracer:trace", reason = "to ensure we log REST requests on TRACE level")
    public void testTracerLog() throws Exception {
        final String includeSettings;
        final String excludeSettings;
        if (randomBoolean()) {
            includeSettings = randomBoolean() ? "*" : "";
        } else {
            includeSettings = "/internal/test";
        }
        excludeSettings = "/internal/testNotSeen";

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        try (
            AbstractHttpServerTransport transport = new AbstractHttpServerTransport(
                Settings.EMPTY,
                networkService,
                recycler,
                threadPool,
                xContentRegistry(),
                new HttpServerTransport.Dispatcher() {
                    @Override
                    public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
                        channel.sendResponse(emptyResponse(RestStatus.OK));
                    }

                    @Override
                    public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
                        channel.sendResponse(emptyResponse(RestStatus.BAD_REQUEST));
                    }
                },
                clusterSettings,
                Tracer.NOOP
            ) {
                @Override
                protected HttpServerChannel bind(InetSocketAddress hostAddress) {
                    return null;
                }

                @Override
                protected void doStart() {

                }

                @Override
                protected void stopInternal() {

                }

                @Override
                public HttpStats stats() {
                    return null;
                }
            }
        ) {
            clusterSettings.applySettings(
                Settings.builder()
                    .put(HttpTransportSettings.SETTING_HTTP_TRACE_LOG_INCLUDE.getKey(), includeSettings)
                    .put(HttpTransportSettings.SETTING_HTTP_TRACE_LOG_EXCLUDE.getKey(), excludeSettings)
                    .build()
            );
            try (var mockLog = MockLog.capture(HttpTracer.class)) {

                final String opaqueId = UUIDs.randomBase64UUID(random());
                mockLog.addExpectation(
                    new MockLog.PatternSeenEventExpectation(
                        "received request",
                        HttpTracerTests.HTTP_TRACER_LOGGER,
                        Level.TRACE,
                        "\\[\\d+\\]\\[" + opaqueId + "\\]\\[OPTIONS\\]\\[/internal/test\\] received request from \\[.*"
                    )
                );

                final boolean badRequest = randomBoolean();

                mockLog.addExpectation(
                    new MockLog.PatternSeenEventExpectation(
                        "sent response",
                        HttpTracerTests.HTTP_TRACER_LOGGER,
                        Level.TRACE,
                        "\\[\\d+\\]\\["
                            + opaqueId
                            + "\\]\\["
                            + (badRequest ? "BAD_REQUEST" : "OK")
                            + "\\]\\["
                            + RestResponse.TEXT_CONTENT_TYPE
                            + "\\]\\[0\\] sent response to \\[.*"
                    )
                );

                mockLog.addExpectation(
                    new MockLog.UnseenEventExpectation(
                        "received other request",
                        HttpTracerTests.HTTP_TRACER_LOGGER,
                        Level.TRACE,
                        "\\[\\d+\\]\\[" + opaqueId + "\\]\\[OPTIONS\\]\\[/internal/testNotSeen\\] received request from \\[.*"
                    )
                );

                final Exception inboundException;
                if (badRequest) {
                    inboundException = new RuntimeException();
                } else {
                    inboundException = null;
                }

                final FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(
                    RestRequest.Method.OPTIONS
                )
                    .withPath("/internal/test")
                    .withHeaders(Collections.singletonMap(Task.X_OPAQUE_ID_HTTP_HEADER, Collections.singletonList(opaqueId)))
                    .withInboundException(inboundException)
                    .build();

                try (var httpChannel = fakeRestRequest.getHttpChannel()) {
                    transport.serverAcceptedChannel(httpChannel);
                    transport.incomingRequest(fakeRestRequest.getHttpRequest(), httpChannel);
                }

                final Exception inboundExceptionExcludedPath;
                if (randomBoolean()) {
                    inboundExceptionExcludedPath = new RuntimeException();
                } else {
                    inboundExceptionExcludedPath = null;
                }

                final FakeRestRequest fakeRestRequestExcludedPath = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(
                    RestRequest.Method.OPTIONS
                )
                    .withPath("/internal/testNotSeen")
                    .withHeaders(Collections.singletonMap(Task.X_OPAQUE_ID_HTTP_HEADER, Collections.singletonList(opaqueId)))
                    .withInboundException(inboundExceptionExcludedPath)
                    .build();

                try (var httpChannel = fakeRestRequestExcludedPath.getHttpChannel()) {
                    transport.incomingRequest(fakeRestRequestExcludedPath.getHttpRequest(), httpChannel);
                }
                mockLog.assertAllExpectationsMatched();
            }
        }
    }

    public void testLogsSlowInboundProcessing() throws Exception {
        final String opaqueId = UUIDs.randomBase64UUID(random());
        final String path = "/internal/test";
        final RestRequest.Method method = randomFrom(RestRequest.Method.values());
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final Settings settings = Settings.builder()
            .put(TransportSettings.SLOW_OPERATION_THRESHOLD_SETTING.getKey(), TimeValue.timeValueMillis(5))
            .build();
        try (
            var mockLog = MockLog.capture(AbstractHttpServerTransport.class);
            AbstractHttpServerTransport transport = new AbstractHttpServerTransport(
                settings,
                networkService,
                recycler,
                threadPool,
                xContentRegistry(),
                new HttpServerTransport.Dispatcher() {
                    @Override
                    public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
                        try {
                            TimeUnit.SECONDS.sleep(1L);
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                        channel.sendResponse(emptyResponse(RestStatus.OK));
                    }

                    @Override
                    public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
                        channel.sendResponse(emptyResponse(RestStatus.BAD_REQUEST));
                    }
                },
                clusterSettings,
                Tracer.NOOP
            ) {
                @Override
                protected HttpServerChannel bind(InetSocketAddress hostAddress) {
                    return null;
                }

                @Override
                protected void doStart() {

                }

                @Override
                protected void stopInternal() {

                }

                @Override
                public HttpStats stats() {
                    return null;
                }
            }
        ) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "expected message",
                    AbstractHttpServerTransport.class.getCanonicalName(),
                    Level.WARN,
                    "handling request [" + opaqueId + "][" + method + "][" + path + "]"
                )
            );

            final FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(method)
                .withPath(path)
                .withHeaders(Collections.singletonMap(Task.X_OPAQUE_ID_HTTP_HEADER, Collections.singletonList(opaqueId)))
                .build();
            transport.serverAcceptedChannel(fakeRestRequest.getHttpChannel());
            transport.incomingRequest(fakeRestRequest.getHttpRequest(), fakeRestRequest.getHttpChannel());
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testHttpClientStats() {
        try (
            AbstractHttpServerTransport transport = new AbstractHttpServerTransport(
                Settings.EMPTY,
                networkService,
                recycler,
                threadPool,
                xContentRegistry(),
                new HttpServerTransport.Dispatcher() {
                    @Override
                    public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {

                        channel.sendResponse(emptyResponse(RestStatus.OK));
                    }

                    @Override
                    public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
                        channel.sendResponse(emptyResponse(RestStatus.BAD_REQUEST));
                    }
                },
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                Tracer.NOOP
            ) {

                @Override
                protected HttpServerChannel bind(InetSocketAddress hostAddress) {
                    return null;
                }

                @Override
                protected void doStart() {}

                @Override
                protected void stopInternal() {}
            }
        ) {

            InetSocketAddress remoteAddress = new InetSocketAddress(randomIp(randomBoolean()), randomIntBetween(1, 65535));
            String opaqueId = UUIDs.randomBase64UUID(random());
            FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withRemoteAddress(remoteAddress)
                .withMethod(RestRequest.Method.GET)
                .withPath("/internal/stats_test")
                .withHeaders(Map.of(Task.X_OPAQUE_ID_HTTP_HEADER, Collections.singletonList(opaqueId)))
                .build();
            transport.serverAcceptedChannel(fakeRestRequest.getHttpChannel());
            transport.incomingRequest(fakeRestRequest.getHttpRequest(), fakeRestRequest.getHttpChannel());

            HttpStats httpStats = transport.stats();
            assertThat(
                httpStats.getClientStats(),
                contains(
                    allOf(
                        transformedMatch(HttpStats.ClientStats::remoteAddress, equalTo(NetworkAddress.format(remoteAddress))),
                        transformedMatch(HttpStats.ClientStats::opaqueId, equalTo(opaqueId)),
                        transformedMatch(HttpStats.ClientStats::lastUri, equalTo("/internal/stats_test"))
                    )
                )
            );

            remoteAddress = new InetSocketAddress(randomIp(randomBoolean()), randomIntBetween(1, 65535));
            opaqueId = UUIDs.randomBase64UUID(random());
            fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withRemoteAddress(remoteAddress)
                .withMethod(RestRequest.Method.GET)
                .withPath("/internal/stats_test2")
                .withHeaders(Map.of(Task.X_OPAQUE_ID_HTTP_HEADER.toUpperCase(Locale.ROOT), Collections.singletonList(opaqueId)))
                .build();
            transport.serverAcceptedChannel(fakeRestRequest.getHttpChannel());
            transport.incomingRequest(fakeRestRequest.getHttpRequest(), fakeRestRequest.getHttpChannel());
            httpStats = transport.stats();
            assertThat(httpStats.getClientStats().size(), equalTo(2));

            // due to non-deterministic ordering in map iteration, the second client may not be the second entry in the list
            HttpStats.ClientStats secondClientStats = httpStats.getClientStats().get(0).opaqueId().equals(opaqueId)
                ? httpStats.getClientStats().get(0)
                : httpStats.getClientStats().get(1);

            assertThat(secondClientStats.remoteAddress(), equalTo(NetworkAddress.format(remoteAddress)));
            assertThat(secondClientStats.opaqueId(), equalTo(opaqueId));
            assertThat(secondClientStats.lastUri(), equalTo("/internal/stats_test2"));
        }
    }

    public void testDisablingHttpClientStats() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        try (
            AbstractHttpServerTransport transport = new AbstractHttpServerTransport(
                Settings.EMPTY,
                networkService,
                recycler,
                threadPool,
                xContentRegistry(),
                new HttpServerTransport.Dispatcher() {
                    @Override
                    public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
                        channel.sendResponse(emptyResponse(RestStatus.OK));
                    }

                    @Override
                    public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
                        channel.sendResponse(emptyResponse(RestStatus.BAD_REQUEST));
                    }
                },
                clusterSettings,
                Tracer.NOOP
            ) {

                @Override
                protected HttpServerChannel bind(InetSocketAddress hostAddress) {
                    return null;
                }

                @Override
                protected void doStart() {}

                @Override
                protected void stopInternal() {}
            }
        ) {

            InetSocketAddress remoteAddress = new InetSocketAddress(randomIp(randomBoolean()), randomIntBetween(1, 65535));
            String opaqueId = UUIDs.randomBase64UUID(random());
            FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withRemoteAddress(remoteAddress)
                .withMethod(RestRequest.Method.GET)
                .withPath("/internal/stats_test")
                .withHeaders(Map.of(Task.X_OPAQUE_ID_HTTP_HEADER, Collections.singletonList(opaqueId)))
                .build();
            transport.serverAcceptedChannel(fakeRestRequest.getHttpChannel());
            transport.incomingRequest(fakeRestRequest.getHttpRequest(), fakeRestRequest.getHttpChannel());

            // HTTP client stats should default to enabled
            HttpStats httpStats = transport.stats();
            assertThat(httpStats.getClientStats().size(), equalTo(1));
            assertThat(httpStats.getClientStats().get(0).opaqueId(), equalTo(opaqueId));

            clusterSettings.applySettings(
                Settings.builder().put(HttpTransportSettings.SETTING_HTTP_CLIENT_STATS_ENABLED.getKey(), false).build()
            );

            // After disabling, HTTP client stats should be cleared immediately
            httpStats = transport.stats();
            assertThat(httpStats.getClientStats().size(), equalTo(0));

            // After disabling, HTTP client stats should not track new clients
            remoteAddress = new InetSocketAddress(randomIp(randomBoolean()), randomIntBetween(1, 65535));
            opaqueId = UUIDs.randomBase64UUID(random());
            fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withRemoteAddress(remoteAddress)
                .withMethod(RestRequest.Method.GET)
                .withPath("/internal/stats_test")
                .withHeaders(Map.of(Task.X_OPAQUE_ID_HTTP_HEADER, Collections.singletonList(opaqueId)))
                .build();
            transport.serverAcceptedChannel(fakeRestRequest.getHttpChannel());
            transport.incomingRequest(fakeRestRequest.getHttpRequest(), fakeRestRequest.getHttpChannel());
            httpStats = transport.stats();
            assertThat(httpStats.getClientStats().size(), equalTo(0));

            clusterSettings.applySettings(
                Settings.builder().put(HttpTransportSettings.SETTING_HTTP_CLIENT_STATS_ENABLED.getKey(), true).build()
            );

            // After re-enabling, HTTP client stats should now track new clients
            remoteAddress = new InetSocketAddress(randomIp(randomBoolean()), randomIntBetween(1, 65535));
            opaqueId = UUIDs.randomBase64UUID(random());
            fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withRemoteAddress(remoteAddress)
                .withMethod(RestRequest.Method.GET)
                .withPath("/internal/stats_test")
                .withHeaders(Map.of(Task.X_OPAQUE_ID_HTTP_HEADER, Collections.singletonList(opaqueId)))
                .build();
            transport.serverAcceptedChannel(fakeRestRequest.getHttpChannel());
            transport.incomingRequest(fakeRestRequest.getHttpRequest(), fakeRestRequest.getHttpChannel());
            httpStats = transport.stats();
            assertThat(httpStats.getClientStats().size(), equalTo(1));
        }
    }

    public void testStopWaitsIndefinitelyIfGraceIsZero() {
        try (var wait = LogExpectation.expectWait(); var transport = new TestHttpServerTransport(Settings.EMPTY)) {
            TestHttpChannel httpChannel = new TestHttpChannel();
            transport.serverAcceptedChannel(httpChannel);
            transport.incomingRequest(testHttpRequest(), httpChannel);

            transport.doStop();
            assertFalse(transport.testHttpServerChannel.isOpen());
            assertFalse(httpChannel.isOpen());
            wait.assertExpectationsMatched();
        }
    }

    public void testStopLogsProgress() throws Exception {
        TestHttpChannel httpChannel = new TestHttpChannel();
        var doneWithRequest = new CountDownLatch(1);
        try (var wait = LogExpectation.expectUpdate(1); var transport = new TestHttpServerTransport(gracePeriod(SHORT_GRACE_PERIOD_MS))) {

            httpChannel.blockSendResponse();
            var inResponse = httpChannel.notifyInSendResponse();

            transport.serverAcceptedChannel(httpChannel);
            new Thread(() -> {
                transport.incomingRequest(testHttpRequest(), httpChannel);
                doneWithRequest.countDown();
            }, "testStopLogsProgress -> incomingRequest").start();

            inResponse.await();

            transport.doStop();
            assertFalse(transport.testHttpServerChannel.isOpen());
            assertFalse(httpChannel.isOpen());
            wait.assertExpectationsMatched();
        } finally {
            httpChannel.allowSendResponse();
            doneWithRequest.await();
        }
    }

    public void testStopWorksWithNoOpenRequests() {
        var grace = SHORT_GRACE_PERIOD_MS;
        try (var noWait = LogExpectation.unexpectedTimeout(grace); var transport = new TestHttpServerTransport(gracePeriod(grace))) {
            final TestHttpRequest httpRequest = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/") {
                @Override
                public Map<String, List<String>> getHeaders() {
                    // close connection before shutting down
                    return Map.of(CONNECTION, List.of(CLOSE));
                }
            };
            TestHttpChannel httpChannel = new TestHttpChannel();
            transport.serverAcceptedChannel(httpChannel);
            transport.incomingRequest(httpRequest, httpChannel);
            assertFalse(httpChannel.isOpen());

            // TestHttpChannel will throw if closed twice, so this ensures close is not called.
            transport.doStop();
            assertFalse(transport.testHttpServerChannel.isOpen());

            noWait.assertExpectationsMatched();
        }
    }

    public void testStopClosesIdleConnectionImmediately() {
        var grace = SHORT_GRACE_PERIOD_MS;
        try (
            var noTimeout = LogExpectation.unexpectedTimeout(grace);
            TestHttpServerTransport transport = new TestHttpServerTransport(gracePeriod(grace))
        ) {

            TestHttpChannel httpChannel = new TestHttpChannel();
            transport.serverAcceptedChannel(httpChannel);

            transport.incomingRequest(testHttpRequest(), httpChannel);
            // channel now idle

            assertTrue(httpChannel.isOpen());
            transport.doStop();
            assertFalse(httpChannel.isOpen());
            assertFalse(transport.testHttpServerChannel.isOpen());

            // ensure we timed out waiting for connections to close naturally
            noTimeout.assertExpectationsMatched();
        }
    }

    public void testStopForceClosesConnectionDuringRequest() throws Exception {
        var grace = SHORT_GRACE_PERIOD_MS;
        TestHttpChannel httpChannel = new TestHttpChannel();
        var doneWithRequest = new CountDownLatch(1);
        try (
            var timeout = LogExpectation.expectTimeout(grace);
            TestHttpServerTransport transport = new TestHttpServerTransport(gracePeriod(grace))
        ) {

            httpChannel.blockSendResponse();
            var inResponse = httpChannel.notifyInSendResponse();

            transport.serverAcceptedChannel(httpChannel);
            new Thread(() -> {
                transport.incomingRequest(testHttpRequest(), httpChannel);
                doneWithRequest.countDown();
            }, "testStopForceClosesConnectionDuringRequest -> incomingRequest").start();

            inResponse.await();

            assertTrue(httpChannel.isOpen());
            transport.doStop();

            assertFalse(httpChannel.isOpen());
            assertFalse(transport.testHttpServerChannel.isOpen());
            assertTrue(httpChannel.noResponses());

            // ensure we timed out waiting for connections to close naturally
            timeout.assertExpectationsMatched();
        } finally {
            // unblock request thread
            httpChannel.allowSendResponse();
            doneWithRequest.countDown();
        }
    }

    public void testStopClosesChannelAfterRequest() throws Exception {
        var grace = LONG_GRACE_PERIOD_MS;
        try (var noTimeout = LogExpectation.unexpectedTimeout(grace); var transport = new TestHttpServerTransport(gracePeriod(grace))) {

            TestHttpChannel httpChannel = new TestHttpChannel();
            transport.serverAcceptedChannel(httpChannel);
            transport.incomingRequest(testHttpRequest(), httpChannel);

            TestHttpChannel idleChannel = new TestHttpChannel();
            transport.serverAcceptedChannel(idleChannel);
            transport.incomingRequest(testHttpRequest(), idleChannel);

            CountDownLatch stopped = new CountDownLatch(1);

            var inSendResponse = httpChannel.notifyInSendResponse();
            httpChannel.blockSendResponse();

            // one last request, should cause httpChannel to close after the request once we start shutting down.
            new Thread(() -> transport.incomingRequest(testHttpRequest(), httpChannel), "testStopClosesChannelAfterRequest last request")
                .start();

            inSendResponse.await();

            new Thread(() -> {
                transport.doStop();
                stopped.countDown();
            }, "testStopClosesChannelAfterRequest stopping transport").start();

            // wait until we are shutting down
            assertBusy(() -> assertFalse(transport.isAcceptingConnections()));
            httpChannel.allowSendResponse();

            // wait for channel to close
            assertBusy(() -> assertFalse(httpChannel.isOpen()));

            try {
                assertTrue(stopped.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                fail("server never stopped");
            }

            assertFalse(transport.testHttpServerChannel.isOpen());
            assertFalse(idleChannel.isOpen());

            assertThat(httpChannel.responses, hasSize(2));
            // should have closed naturally without having to wait
            noTimeout.assertExpectationsMatched();
        }
    }

    public void testForceClosesOpenChannels() throws Exception {
        var grace = 100; // this test waits for the entire grace, so try to keep it short
        TestHttpChannel httpChannel = new TestHttpChannel();
        var doneWithRequest = new CountDownLatch(1);
        try (var timeout = LogExpectation.expectTimeout(grace); var transport = new TestHttpServerTransport(gracePeriod(grace))) {

            transport.serverAcceptedChannel(httpChannel);
            transport.incomingRequest(testHttpRequest(), httpChannel);

            CountDownLatch stopped = new CountDownLatch(1);

            var inResponse = httpChannel.notifyInSendResponse();
            httpChannel.blockSendResponse();

            new Thread(() -> {
                transport.incomingRequest(testHttpRequest(), httpChannel);
                doneWithRequest.countDown();
            }).start();

            inResponse.await();

            new Thread(() -> {
                transport.doStop();
                stopped.countDown();
            }).start();

            try {
                assertTrue(stopped.await(2 * LONG_GRACE_PERIOD_MS, TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                fail("server never stopped");
            }

            assertFalse(transport.testHttpServerChannel.isOpen());
            assertFalse(httpChannel.isOpen());

            HttpResponse first = httpChannel.getResponse();
            assertTrue(httpChannel.noResponses()); // never sent the second response
            assertThat(first, instanceOf(TestHttpResponse.class));

            timeout.assertExpectationsMatched();
        } finally {
            // cleanup thread
            httpChannel.allowSendResponse();
            doneWithRequest.await();
        }

    }

    private static RestResponse emptyResponse(RestStatus status) {
        return new RestResponse(status, RestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY);
    }

    private TransportAddress address(String host, int port) throws UnknownHostException {
        return new TransportAddress(getByName(host), port);
    }

    private TransportAddress randomAddress() throws UnknownHostException {
        return address("127.0.0." + randomIntBetween(1, 100), randomIntBetween(9200, 9300));
    }

    private List<TransportAddress> randomAddresses() throws UnknownHostException {
        List<TransportAddress> addresses = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            addresses.add(randomAddress());
        }
        return addresses;
    }

    private ActionModule getFakeActionModule(Set<RestHeaderDefinition> headersToCopy) {
        SettingsModule settings = new SettingsModule(Settings.EMPTY);
        ActionPlugin copyHeadersPlugin = new ActionPlugin() {
            @Override
            public Collection<RestHeaderDefinition> getRestHeaders() {
                return headersToCopy;
            }
        };
        return new ActionModule(
            settings.getSettings(),
            TestIndexNameExpressionResolver.newInstance(threadPool.getThreadContext()),
            null,
            settings.getIndexScopedSettings(),
            settings.getClusterSettings(),
            settings.getSettingsFilter(),
            threadPool,
            List.of(copyHeadersPlugin),
            null,
            null,
            new UsageService(),
            null,
            TelemetryProvider.NOOP,
            mock(ClusterService.class),
            null,
            List.of(),
            RestExtension.allowAll(),
            new IncrementalBulkService(null, null, new ThreadContext(Settings.EMPTY))
        );
    }

    private class TestHttpServerTransport extends AbstractHttpServerTransport {
        public TestHttpChannel testHttpServerChannel = new TestHttpChannel();

        TestHttpServerTransport(Settings settings, HttpServerTransport.Dispatcher dispatcher) {
            super(
                Settings.builder().put(settings).put(SETTING_HTTP_CLIENT_STATS_ENABLED.getKey(), false).build(),
                AbstractHttpServerTransportTests.this.networkService,
                AbstractHttpServerTransportTests.this.recycler,
                AbstractHttpServerTransportTests.this.threadPool,
                xContentRegistry(),
                dispatcher,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                Tracer.NOOP
            );
            bindServer();
        }

        TestHttpServerTransport(Settings settings) {
            this(settings, new HttpServerTransport.Dispatcher() {
                @Override
                public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
                    channel.sendResponse(emptyResponse(RestStatus.OK));
                }

                @Override
                public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
                    channel.sendResponse(emptyResponse(RestStatus.BAD_REQUEST));
                }
            });
        }

        @Override
        protected HttpServerChannel bind(InetSocketAddress hostAddress) {
            testHttpServerChannel.setLocalAddress(hostAddress);
            return testHttpServerChannel;
        }

        @Override
        protected void doStart() {
            bindServer();
        }

        @Override
        protected void stopInternal() {}
    }

    private Settings gracePeriod(int ms) {
        return Settings.builder().put(SETTING_HTTP_SERVER_SHUTDOWN_GRACE_PERIOD.getKey(), new TimeValue(ms)).build();
    }

    private static class TestHttpChannel implements HttpChannel, HttpServerChannel {
        private boolean open = true;
        private ActionListener<Void> closeListener;
        private InetSocketAddress localAddress;

        private final BlockingDeque<HttpResponse> responses = new LinkedBlockingDeque<>();

        private CountDownLatch notifySendResponse = null;
        private CountDownLatch blockSendResponse = null;

        public CountDownLatch notifyInSendResponse() {
            synchronized (this) {
                assert notifySendResponse == null : "already notifying";
                notifySendResponse = new CountDownLatch(1);
                return notifySendResponse;
            }
        }

        public synchronized void blockSendResponse() {
            synchronized (this) {
                assert blockSendResponse == null : "blockSendResponse already set";
                blockSendResponse = new CountDownLatch(1);
            }
        }

        public synchronized void allowSendResponse() {
            synchronized (this) {
                assert blockSendResponse != null : "blockSendResponse null, no need to allow";
                blockSendResponse.countDown();
            }
        }

        public boolean noResponses() {
            return responses.peek() == null;
        }

        public HttpResponse getResponse() {
            try {
                return responses.takeFirst();
            } catch (InterruptedException e) {
                fail("interrupted");
            }
            // unreachable
            return null;
        }

        @Override
        public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
            CountDownLatch notify;
            CountDownLatch blockSend;
            synchronized (this) {
                notify = notifySendResponse;
                blockSend = blockSendResponse;
            }
            if (notify != null) {
                notify.countDown();
                synchronized (this) {
                    notifySendResponse = null;
                }
            }
            if (blockSend != null) {
                try {
                    blockSend.await();
                    synchronized (this) {
                        blockSendResponse = null;
                    }
                } catch (InterruptedException e) {
                    fail("interrupted");
                }
            }
            responses.add(response);
            listener.onResponse(null);
        }

        public void setLocalAddress(InetSocketAddress localAddress) {
            this.localAddress = localAddress;
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return localAddress;
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public void close() {
            synchronized (this) {
                if (open == false) {
                    throw new IllegalStateException("channel already closed!");
                }
                open = false;
            }
            if (closeListener != null) {
                closeListener.onResponse(null);
            }
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
            if (open == false) {
                listener.onResponse(null);
            } else {
                if (closeListener != null) {
                    throw new IllegalStateException("close listener already set");
                }
                closeListener = listener;
            }
        }
    }

    private static class LogExpectation implements AutoCloseable {
        private final Logger mockLogger;
        private final MockLog mockLog;
        private final int grace;

        private LogExpectation(int grace) {
            mockLogger = LogManager.getLogger(AbstractHttpServerTransport.class);
            Loggers.setLevel(mockLogger, Level.DEBUG);
            mockLog = MockLog.capture(AbstractHttpServerTransport.class);
            this.grace = grace;
        }

        public static LogExpectation expectTimeout(int grace) {
            return new LogExpectation(grace).timedOut(true).wait(false);
        }

        public static LogExpectation unexpectedTimeout(int grace) {
            return new LogExpectation(grace).timedOut(false).wait(false);
        }

        public static LogExpectation expectWait() {
            return new LogExpectation(0).wait(true);
        }

        public static LogExpectation expectUpdate(int connections) {
            return new LogExpectation(0).update(connections);
        }

        private LogExpectation timedOut(boolean expected) {
            var message = "timed out while waiting [" + grace + "]ms for clients to close connections";
            var name = "timed out message";
            var logger = AbstractHttpServerTransport.class.getName();
            var level = Level.WARN;
            if (expected) {
                mockLog.addExpectation(new MockLog.SeenEventExpectation(name, logger, level, message));
            } else {
                mockLog.addExpectation(new MockLog.UnseenEventExpectation(name, logger, level, message));
            }
            return this;
        }

        private LogExpectation wait(boolean expected) {
            var message = "waiting indefinitely for clients to close connections";
            var name = "wait message";
            var logger = AbstractHttpServerTransport.class.getName();
            var level = Level.DEBUG;
            if (expected) {
                mockLog.addExpectation(new MockLog.SeenEventExpectation(name, logger, level, message));
            } else {
                mockLog.addExpectation(new MockLog.UnseenEventExpectation(name, logger, level, message));
            }
            return this;
        }

        private LogExpectation update(int connections) {
            var message = "still waiting on " + connections + " client connections to close";
            var name = "update message";
            var logger = AbstractHttpServerTransport.class.getName();
            var level = Level.INFO;
            mockLog.addExpectation(new MockLog.SeenEventExpectation(name, logger, level, message));
            return this;
        }

        public void assertExpectationsMatched() {
            mockLog.assertAllExpectationsMatched();
        }

        @Override
        public void close() {
            mockLog.close();
        }
    }

    private TestHttpRequest testHttpRequest() {
        return new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
    }
}
