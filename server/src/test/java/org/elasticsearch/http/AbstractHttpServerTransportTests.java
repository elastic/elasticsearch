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
import org.mockito.ArgumentCaptor;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.net.InetAddress.getByName;
import static java.util.Arrays.asList;
import static org.elasticsearch.http.AbstractHttpServerTransport.resolvePublishPort;
import static org.elasticsearch.http.DefaultRestChannel.CLOSE;
import static org.elasticsearch.http.DefaultRestChannel.CONNECTION;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_CLIENT_STATS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_SERVER_SHUTDOWN_GRACE_PERIOD;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

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

            transport.incomingRequest(fakeHttpRequest, null);
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

            transport.incomingRequest(fakeHttpRequest, null);
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

            transport.incomingRequest(fakeHttpRequest, null);
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
            assertThat(httpStats.getClientStats().size(), equalTo(1));
            assertThat(httpStats.getClientStats().get(0).remoteAddress(), equalTo(NetworkAddress.format(remoteAddress)));
            assertThat(httpStats.getClientStats().get(0).opaqueId(), equalTo(opaqueId));
            assertThat(httpStats.getClientStats().get(0).lastUri(), equalTo("/internal/stats_test"));

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

    @SuppressWarnings("unchecked")
    public void testSetGracefulClose() {
        try (AbstractHttpServerTransport transport = new TestHttpServerTransport(Settings.EMPTY)) {
            final TestHttpRequest httpRequest = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");

            HttpChannel httpChannel = mock(HttpChannel.class);
            transport.incomingRequest(httpRequest, httpChannel);

            var response = ArgumentCaptor.forClass(TestHttpResponse.class);
            var listener = ArgumentCaptor.forClass(ActionListener.class);
            verify(httpChannel).sendResponse(response.capture(), listener.capture());

            listener.getValue().onResponse(null);
            assertThat(response.getValue().containsHeader(CONNECTION), is(false));
            verify(httpChannel, never()).close();

            httpChannel = mock(HttpChannel.class);
            transport.gracefullyCloseConnections();
            transport.incomingRequest(httpRequest, httpChannel);
            verify(httpChannel).sendResponse(response.capture(), listener.capture());

            listener.getValue().onResponse(null);
            assertThat(response.getValue().headers().get(CONNECTION), containsInAnyOrder(DefaultRestChannel.CLOSE));
            verify(httpChannel).close();
        }
    }

    public void testStopDoesntWaitIfGraceIsZero() {
        try (TestHttpServerTransport transport = new TestHttpServerTransport(Settings.EMPTY)) {
            transport.bindServer();
            final TestHttpRequest httpRequest = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");

            TestHttpChannel httpChannel = new TestHttpChannel(false);
            transport.serverAcceptedChannel(httpChannel);
            transport.incomingRequest(httpRequest, httpChannel);

            transport.doStop();
            assertThat("stop listening first", transport.stopListeningForNewConnectionsOrder(), is(1));
            assertThat("graceful close second", transport.gracefullyCloseConnectionsOrder(), is(2));
            assertThat("no wait", transport.waitForClientConnectionsToCloseOrder(), is(-1));
            assertThat("close immediately third", transport.closeClientConnectionsImmediatelyOrder(), is(3));
            assertFalse(transport.testHttpServerChannel.isOpen());
            assertFalse(httpChannel.isOpen());
        }
    }

    public void testStopEarlyWhenNoRequests() {
        try (TestHttpServerTransport transport = new TestHttpServerTransport(gracePeriod(1))) {
            transport.bindServer();

            transport.doStop();
            assertThat("stop listening first", transport.stopListeningForNewConnectionsOrder(), is(1));
            assertThat("graceful close second", transport.gracefullyCloseConnectionsOrder(), is(2));
            assertThat("wait third", transport.waitForClientConnectionsToCloseOrder(), is(3));
            assertThat("no need to close immediately", transport.closeClientConnectionsImmediatelyOrder(), is(-1));
            assertTrue(transport.waitClosedBeforeTimeout);
            assertFalse(transport.testHttpServerChannel.isOpen());
        }
    }

    public void testStopWaitsForTimeoutWhenRequests() {
        try (TestHttpServerTransport transport = new TestHttpServerTransport(gracePeriod(1))) {
            transport.bindServer();

            final TestHttpRequest httpRequest = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
            TestHttpChannel httpChannel = new TestHttpChannel(true);
            transport.serverAcceptedChannel(httpChannel);
            transport.incomingRequest(httpRequest, httpChannel);
            transport.addForceCloser(httpChannel.delayClose::countDown);

            transport.doStop();
            assertThat("stop listening first", transport.stopListeningForNewConnectionsOrder(), is(1));
            assertThat("graceful close second", transport.gracefullyCloseConnectionsOrder(), is(2));
            assertThat("wait third", transport.waitForClientConnectionsToCloseOrder(), is(3));
            assertThat("close immediately fourth", transport.closeClientConnectionsImmediatelyOrder(), is(4));
            assertFalse(transport.waitClosedBeforeTimeout);
            assertFalse(transport.testHttpServerChannel.isOpen());
        }
    }

    public void testStopClosesChannelAfterRequest() {
        try (TestHttpServerTransport transport = new TestHttpServerTransport(gracePeriod(100))) {
            transport.bindServer();

            TestHttpChannel httpChannel = new TestHttpChannel(true);
            transport.serverAcceptedChannel(httpChannel);
            transport.incomingRequest(new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/"), httpChannel);
            CountDownLatch lastRequestSent = new CountDownLatch(1);

            TestHttpChannel idleChannel = new TestHttpChannel(true);
            transport.serverAcceptedChannel(idleChannel);
            transport.incomingRequest(new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/"), idleChannel);

            // going to force close the idle channel
            transport.addForceCloser(idleChannel.delayClose::countDown);

            // After graceful close, send one last request
            transport.listeners.put(TestHttpServerTransport.State.GRACEFUL_CLOSE, () -> {
                httpChannel.delayClose.countDown();
                transport.incomingRequest(new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/"), httpChannel);
                lastRequestSent.countDown();
            });

            // Latch for all reactive logic to trigger
            CountDownLatch stopped = new CountDownLatch(1);
            new Thread(() -> {
                transport.doStop();
                stopped.countDown();
            }).start();

            try {
                assertTrue(stopped.await(100, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                fail("server never stopped");
            }
            assertThat("stop listening first", transport.stopListeningForNewConnectionsOrder(), is(1));
            assertThat("graceful close second", transport.gracefullyCloseConnectionsOrder(), is(2));
            assertThat("wait third", transport.waitForClientConnectionsToCloseOrder(), is(3));
            assertThat("close immediately fourth", transport.closeClientConnectionsImmediatelyOrder(), is(4));
            assertFalse(transport.waitClosedBeforeTimeout);
            assertFalse(transport.testHttpServerChannel.isOpen());
            assertFalse(httpChannel.open);
            assertThat(httpChannel.responses, hasSize(2));
            HttpResponse first = httpChannel.responses.get(0);
            HttpResponse last = httpChannel.responses.get(1);
            assertFalse(first.containsHeader(CONNECTION));
            assertTrue(last.containsHeader(CONNECTION));
            assertThat(last, instanceOf(TestHttpResponse.class));
            assertThat(((TestHttpResponse) last).headers().get(CONNECTION).get(0), equalTo(CLOSE));
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
        List<Runnable> forceClosers = new ArrayList<>();

        public TestHttpServerTransport(Settings settings) {
            super(
                Settings.builder().put(settings).put(SETTING_HTTP_CLIENT_STATS_ENABLED.getKey(), false).build(),
                AbstractHttpServerTransportTests.this.networkService,
                AbstractHttpServerTransportTests.this.recycler,
                AbstractHttpServerTransportTests.this.threadPool,
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
            );
        }

        private enum State {
            STOP_LISTENING,
            GRACEFUL_CLOSE,
            WAIT_FOR_CLIENTS,
            FORCE_CLIENTS
        }

        private int step = 1;

        public Map<State, Integer> order = new HashMap<>();
        public Map<State, Runnable> listeners = new HashMap<>();

        boolean waitClosedBeforeTimeout = false;

        public int stopListeningForNewConnectionsOrder() {
            return order.getOrDefault(State.STOP_LISTENING, -1);
        }

        public int gracefullyCloseConnectionsOrder() {
            return order.getOrDefault(State.GRACEFUL_CLOSE, -1);
        }

        public int waitForClientConnectionsToCloseOrder() {
            return order.getOrDefault(State.WAIT_FOR_CLIENTS, -1);
        }

        public int closeClientConnectionsImmediatelyOrder() {
            return order.getOrDefault(State.FORCE_CLIENTS, -1);
        }

        public void addForceCloser(Runnable runnable) {
            forceClosers.add(runnable);
        }

        @Override
        protected void stopListeningForNewConnections() {
            State state = State.STOP_LISTENING;
            assertThat("already stopped listening", order, not(hasKey(state)));
            order.put(state, step++);
            super.stopListeningForNewConnections();
            listeners.getOrDefault(state, () -> {}).run();
        }

        @Override
        public void gracefullyCloseConnections() {
            State state = State.GRACEFUL_CLOSE;
            super.gracefullyCloseConnections();
            assertThat("already gracefully closed", order, not(hasKey(state)));
            order.put(state, step++);
            super.gracefullyCloseConnections();
            listeners.getOrDefault(state, () -> {}).run();
        }

        @Override
        protected boolean waitForClientConnectionsToClose() {
            State state = State.WAIT_FOR_CLIENTS;
            assertThat("already waited", order, not(hasKey(state)));
            order.put(state, step++);
            waitClosedBeforeTimeout = super.waitForClientConnectionsToClose();
            listeners.getOrDefault(state, () -> {}).run();
            return waitClosedBeforeTimeout;
        }

        @Override
        protected void closeClientConnectionsImmediately() {
            State state = State.FORCE_CLIENTS;
            assertThat("already closed", order, not(hasKey(state)));
            order.put(state, step++);
            for (Runnable fc : forceClosers) {
                fc.run();
            }
            super.closeClientConnectionsImmediately();
            listeners.getOrDefault(state, () -> {}).run();
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

    private static class TestHttpChannel implements HttpChannel, HttpServerChannel {
        private boolean open = true;
        public final CountDownLatch delayClose = new CountDownLatch(1);
        private ActionListener<Void> closeListener;
        private InetSocketAddress localAddress;

        public List<HttpResponse> responses = new ArrayList<>();

        public TestHttpChannel(boolean waitToClose) {
            if (waitToClose == false) {
                delayClose.countDown();
            }
        }

        @Override
        public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
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
            if (open == false) {
                throw new IllegalStateException("channel already closed!");
            }
            open = false;
            try {
                delayClose.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
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

    private Settings gracePeriod(int ms) {
        return Settings.builder().put(SETTING_HTTP_SERVER_SHUTDOWN_GRACE_PERIOD.getKey(), new TimeValue(ms)).build();
    }
}
