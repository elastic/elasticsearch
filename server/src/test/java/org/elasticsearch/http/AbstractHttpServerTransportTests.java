/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionModule;
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
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestControllerTests;
import org.elasticsearch.rest.RestHeaderDefinition;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class AbstractHttpServerTransportTests extends ESTestCase {

    private NetworkService networkService;
    private ThreadPool threadPool;
    private Recycler<BytesRef> recycler;

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
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {

            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                assertThat(threadContext.getHeader(Task.TRACE_ID), equalTo("0af7651916cd43dd8448eb211c80319c"));
                assertThat(threadContext.getHeader(Task.TRACE_PARENT_HTTP_HEADER), nullValue());
                assertThat(threadContext.getTransient("parent_" + Task.TRACE_PARENT_HTTP_HEADER), equalTo(traceParentValue));
                // request trace start time is also set
                assertThat(threadContext.getTransient(Task.TRACE_START_TIME), notNullValue());
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
            assertThat(threadPool.getThreadContext().getHeader(Task.TRACE_ID), nullValue());
            assertThat(threadPool.getThreadContext().getHeader(Task.TRACE_PARENT_HTTP_HEADER), nullValue());
            assertThat(threadPool.getThreadContext().getTransient("parent_" + Task.TRACE_PARENT_HTTP_HEADER), nullValue());
            transport.dispatchRequest(null, null, new Exception());
            // headers are "null" here, aka not present, because the thread context changes containing them is to be confined to the request
            assertThat(threadPool.getThreadContext().getHeader(Task.TRACE_ID), nullValue());
            assertThat(threadPool.getThreadContext().getHeader(Task.TRACE_PARENT_HTTP_HEADER), nullValue());
            assertThat(threadPool.getThreadContext().getTransient("parent_" + Task.TRACE_PARENT_HTTP_HEADER), nullValue());
        }
    }

    public void testHandlingCompatibleVersionParsingErrors() {
        // a compatible version exception (v7 on accept and v8 on content-type) should be handled gracefully
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        try (
            AbstractHttpServerTransport transport = failureAssertingtHttpServerTransport(clusterSettings, Set.of("Accept", "Content-Type"))
        ) {
            Map<String, List<String>> headers = new HashMap<>();
            headers.put("Accept", Collections.singletonList("aaa/bbb;compatible-with=7"));
            headers.put("Content-Type", Collections.singletonList("aaa/bbb;compatible-with=8"));

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
            MockLogAppender appender = new MockLogAppender();
            try (var ignored = appender.capturing(HttpTracer.class)) {

                final String opaqueId = UUIDs.randomBase64UUID(random());
                appender.addExpectation(
                    new MockLogAppender.PatternSeenEventExpectation(
                        "received request",
                        HttpTracerTests.HTTP_TRACER_LOGGER,
                        Level.TRACE,
                        "\\[\\d+\\]\\[" + opaqueId + "\\]\\[OPTIONS\\]\\[/internal/test\\] received request from \\[.*"
                    )
                );

                final boolean badRequest = randomBoolean();

                appender.addExpectation(
                    new MockLogAppender.PatternSeenEventExpectation(
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

                appender.addExpectation(
                    new MockLogAppender.UnseenEventExpectation(
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
                appender.assertAllExpectationsMatched();
            }
        }
    }

    public void testLogsSlowInboundProcessing() throws Exception {
        final MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        final String opaqueId = UUIDs.randomBase64UUID(random());
        final String path = "/internal/test";
        final RestRequest.Method method = randomFrom(RestRequest.Method.values());
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "expected message",
                AbstractHttpServerTransport.class.getCanonicalName(),
                Level.WARN,
                "handling request [" + opaqueId + "][" + method + "][" + path + "]"
            )
        );
        final Logger inboundHandlerLogger = LogManager.getLogger(AbstractHttpServerTransport.class);
        Loggers.addAppender(inboundHandlerLogger, mockAppender);
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final Settings settings = Settings.builder()
            .put(TransportSettings.SLOW_OPERATION_THRESHOLD_SETTING.getKey(), TimeValue.timeValueMillis(5))
            .build();
        try (
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

            final FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(method)
                .withPath(path)
                .withHeaders(Collections.singletonMap(Task.X_OPAQUE_ID_HTTP_HEADER, Collections.singletonList(opaqueId)))
                .build();
            transport.incomingRequest(fakeRestRequest.getHttpRequest(), fakeRestRequest.getHttpChannel());
            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(inboundHandlerLogger, mockAppender);
            mockAppender.stop();
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

    public void testStopDoesntWaitIfGraceIsZero() {
        try (var noWait = LogExpectation.unexpectWait(); var transport = new TestHttpServerTransport(Settings.EMPTY)) {
            TestHttpChannel httpChannel = new TestHttpChannel();
            transport.serverAcceptedChannel(httpChannel);
            transport.incomingRequest(testHttpRequest(), httpChannel);

            transport.doStop();
            assertFalse(transport.testHttpServerChannel.isOpen());
            assertFalse(httpChannel.isOpen());
            noWait.assertExpectationsMatched();
        }
    }

    public void testStopWorksWithNoOpenRequests() {
        var grace = 1;
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
        var grace = 10;
        try (
            var noTimeout = LogExpectation.unexpectedTimeout(grace);
            TestHttpServerTransport transport = new TestHttpServerTransport(gracePeriod(grace))
        ) {

            final TestHttpRequest httpRequest = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
            TestHttpChannel httpChannel = new TestHttpChannel();
            transport.serverAcceptedChannel(httpChannel);

            transport.incomingRequest(httpRequest, httpChannel);
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
        var grace = 10;
        TestHttpChannel httpChannel = new TestHttpChannel();
        try (
            var timeout = LogExpectation.expectTimeout(grace);
            TestHttpServerTransport transport = new TestHttpServerTransport(gracePeriod(grace))
        ) {

            httpChannel.blockSendResponse();
            var inResponse = httpChannel.notifyInSendResponse();

            transport.serverAcceptedChannel(httpChannel);
            new Thread(
                () -> transport.incomingRequest(
                    new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/"),
                    httpChannel
                ),
                "testStopForceClosesConnectionDuringRequest -> incomingRequest"
            ).start();

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
        }
    }

    public void testStopClosesChannelAfterRequest() {
        try (TestHttpServerTransport transport = new TestHttpServerTransport(gracePeriod(100))) {

            TestHttpChannel httpChannel = new TestHttpChannel();
            transport.serverAcceptedChannel(httpChannel);
            transport.incomingRequest(new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/"), httpChannel);

            TestHttpChannel idleChannel = new TestHttpChannel();
            transport.serverAcceptedChannel(idleChannel);
            transport.incomingRequest(new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/"), idleChannel);

            // one last request, should cause httpChannel to close naturally now that we've set grace period
            transport.incomingRequest(new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/"), httpChannel);

            CountDownLatch stopped = new CountDownLatch(1);

            new Thread(() -> {
                transport.doStop();
                stopped.countDown();
            }).start();

            // transport.waitForDispatch();
            assertFalse(httpChannel.isOpen());

            try {
                assertTrue(stopped.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                fail("server never stopped");
            }

            assertFalse(transport.testHttpServerChannel.isOpen());
            assertFalse(idleChannel.isOpen());

            assertThat(httpChannel.responses, hasSize(2));
        }
    }

    public void testForceClosesOpenChannels() {
        try (TestHttpServerTransport transport = new TestHttpServerTransport(gracePeriod(100))) {

            TestHttpChannel httpChannel = new TestHttpChannel(true);
            transport.serverAcceptedChannel(httpChannel);
            // transport.continueDispatch();
            transport.incomingRequest(new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/"), httpChannel);

            CountDownLatch stopped = new CountDownLatch(1);

            new Thread(() -> {
                // one last request, will attempt to close naturally, but we are blocking it
                transport.incomingRequest(new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/"), httpChannel);
            }).start();

            new Thread(() -> {
                transport.doStop();
                stopped.countDown();
            }).start();

            try {
                assertTrue(stopped.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                fail("server never stopped");
            }

            assertFalse(transport.testHttpServerChannel.isOpen());
            assertFalse(httpChannel.isOpen());

            HttpResponse first = httpChannel.getResponse();
            HttpResponse last = httpChannel.getResponse();
            assertTrue(httpChannel.noResponses());
            assertFalse(first.containsHeader(CONNECTION));
            assertThat(last, instanceOf(TestHttpResponse.class));
            // transport.continueDispatch();
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
            settings.getIndexScopedSettings(),
            settings.getClusterSettings(),
            settings.getSettingsFilter(),
            threadPool,
            List.of(copyHeadersPlugin),
            null,
            null,
            new UsageService(),
            null,
            null,
            mock(ClusterService.class),
            List.of()
        );
    }

    private class TestHttpServerTransport extends AbstractHttpServerTransport {
        public TestHttpChannel testHttpServerChannel = new TestHttpChannel(false);

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
        private int numCloses = 0;
        private final CountDownLatch closeLatch;
        private ActionListener<Void> closeListener;
        private InetSocketAddress localAddress;

        private BlockingDeque<HttpResponse> responses = new LinkedBlockingDeque<>();

        private final AtomicReference<CountDownLatch> notifySendResponse = new AtomicReference<>();
        private final AtomicReference<CountDownLatch> blockSendResponse = new AtomicReference<>();

        public synchronized CountDownLatch notifyInSendResponse() {
            var latch = new CountDownLatch(1);
            notifySendResponse.set(latch);
            return latch;
        }

        public synchronized void blockSendResponse() {
            assert blockSendResponse.get() == null : "blockSendResponse already set";
            blockSendResponse.set(new CountDownLatch(1));
        }

        public synchronized void allowSendResponse() {
            assert blockSendResponse.get() != null : "blockSendResponse null, no need to allow";
            blockSendResponse.get().countDown();
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

        TestHttpChannel() {
            this(false);
        }

        TestHttpChannel(boolean blockFirstClose) {
            closeLatch = blockFirstClose ? new CountDownLatch(1) : null;
        }

        @Override
        public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
            var latch = notifySendResponse.get();
            if (latch != null) {
                latch.countDown();
            }
            latch = blockSendResponse.get();
            if (latch != null) {
                try {
                    latch.await();
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
            if (closeLatch != null) {
                boolean waitForever;
                synchronized (this) {
                    waitForever = numCloses == 0;
                    numCloses++;
                }
                if (waitForever) {
                    try {
                        if (closeLatch.await(1, TimeUnit.SECONDS) == false) {
                            return;
                        }
                    } catch (InterruptedException ie) {
                        throw new RuntimeException(ie);
                    }
                }
            }
            if (open == false) {
                throw new IllegalStateException("channel already closed!");
            }
            open = false;
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
        private final MockLogAppender appender;
        private boolean checked = false;
        private final int grace;

        private LogExpectation(int grace) {
            mockLogger = LogManager.getLogger(AbstractHttpServerTransport.class);
            Loggers.setLevel(mockLogger, Level.DEBUG);
            appender = new MockLogAppender();
            Loggers.addAppender(mockLogger, appender);
            appender.start();
            this.grace = grace;
        }

        public static LogExpectation expectTimeout(int grace) {
            return new LogExpectation(grace).timedOut(true).wait(true);
        }

        public static LogExpectation unexpectedTimeout(int grace) {
            return new LogExpectation(grace).timedOut(false).wait(true);
        }

        public static LogExpectation unexpectWait() {
            return new LogExpectation(0).wait(false);
        }

        private LogExpectation timedOut(boolean expected) {
            var message = "timed out while waiting [" + grace + "]ms for clients to close connections";
            var name = "message";
            var logger = AbstractHttpServerTransport.class.getName();
            var level = Level.WARN;
            if (expected) {
                appender.addExpectation(new MockLogAppender.SeenEventExpectation(name, logger, level, message));
            } else {
                appender.addExpectation(new MockLogAppender.UnseenEventExpectation(name, logger, level, message));
            }
            return this;
        }

        private LogExpectation wait(boolean expected) {
            var message = "closing all client connections immediately";
            var name = "message";
            var logger = AbstractHttpServerTransport.class.getName();
            var level = Level.DEBUG;
            if (expected) {
                appender.addExpectation(new MockLogAppender.UnseenEventExpectation(name, logger, level, message));
            } else {
                appender.addExpectation(new MockLogAppender.SeenEventExpectation(name, logger, level, message));
            }
            return this;
        }

        private void expectation(boolean expected, Level level, String message) {
            var name = "message";
            var logger = AbstractHttpServerTransport.class.getName();
            if (expected) {
                appender.addExpectation(new MockLogAppender.SeenEventExpectation(name, logger, level, message));
            } else {
                appender.addExpectation(new MockLogAppender.UnseenEventExpectation(name, logger, level, message));
            }
        }

        public void assertExpectationsMatched() {
            appender.assertAllExpectationsMatched();
            checked = true;
        }

        @Override
        public void close() {
            appender.stop();
            Loggers.removeAppender(mockLogger, appender);
            if (checked == false) {
                fail("did not check expectations matched in TimedOutLogExpectation");
            }
        }
    }

    private TestHttpRequest testHttpRequest() {
        return new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
    }
}
