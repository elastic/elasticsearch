/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.path.PathTrie;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.usage.UsageService;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RestControllerTests extends ESTestCase {

    private static final ByteSizeValue BREAKER_LIMIT = new ByteSizeValue(20);
    private CircuitBreaker inFlightRequestsBreaker;
    private RestController restController;
    private HierarchyCircuitBreakerService circuitBreakerService;
    private UsageService usageService;

    @Before
    public void setup() {
        Settings settings = Settings.EMPTY;
        circuitBreakerService = new HierarchyCircuitBreakerService(
            Settings.builder()
                .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), BREAKER_LIMIT)
                .build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        usageService = new UsageService(settings);
        // we can do this here only because we know that we don't adjust breaker settings dynamically in the test
        inFlightRequestsBreaker = circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);

        HttpServerTransport httpServerTransport = new TestHttpServerTransport();
        restController = new RestController(settings, Collections.emptySet(), null, null, circuitBreakerService, usageService);
        restController.registerHandler(RestRequest.Method.GET, "/",
            (request, channel, client) -> channel.sendResponse(
                new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY)));
        restController.registerHandler(RestRequest.Method.GET, "/error", (request, channel, client) -> {
            throw new IllegalArgumentException("test error");
        });

        httpServerTransport.start();
    }

    public void testApplyRelevantHeaders() throws Exception {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        Set<String> headers = new HashSet<>(Arrays.asList("header.1", "header.2"));
        final RestController restController = new RestController(Settings.EMPTY, headers, null, null, circuitBreakerService, usageService);
        Map<String, List<String>> restHeaders = new HashMap<>();
        restHeaders.put("header.1", Collections.singletonList("true"));
        restHeaders.put("header.2", Collections.singletonList("true"));
        restHeaders.put("header.3", Collections.singletonList("false"));
        RestRequest fakeRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(restHeaders).build();
        final RestController spyRestController = spy(restController);
        when(spyRestController.getAllHandlers(fakeRequest))
                .thenReturn(new Iterator<MethodHandlers>() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public MethodHandlers next() {
                        return new MethodHandlers("/", (RestRequest request, RestChannel channel, NodeClient client) -> {
                            assertEquals("true", threadContext.getHeader("header.1"));
                            assertEquals("true", threadContext.getHeader("header.2"));
                            assertNull(threadContext.getHeader("header.3"));
                        }, RestRequest.Method.GET);
                    }
                });
        AssertingChannel channel = new AssertingChannel(fakeRequest, false, RestStatus.BAD_REQUEST);
        restController.dispatchRequest(fakeRequest, channel, threadContext);
        // the rest controller relies on the caller to stash the context, so we should expect these values here as we didn't stash the
        // context in this test
        assertEquals("true", threadContext.getHeader("header.1"));
        assertEquals("true", threadContext.getHeader("header.2"));
        assertNull(threadContext.getHeader("header.3"));
    }

    public void testCanTripCircuitBreaker() throws Exception {
        RestController controller = new RestController(Settings.EMPTY, Collections.emptySet(), null, null, circuitBreakerService,
                usageService);
        // trip circuit breaker by default
        controller.registerHandler(RestRequest.Method.GET, "/trip", new FakeRestHandler(true));
        controller.registerHandler(RestRequest.Method.GET, "/do-not-trip", new FakeRestHandler(false));

        RestRequest fakeRequest = new FakeRestRequest.Builder(xContentRegistry()).withPath("/trip").build();
        for (Iterator<MethodHandlers> it = controller.getAllHandlers(fakeRequest); it.hasNext(); ) {
            Optional<MethodHandlers> mHandler = Optional.ofNullable(it.next());
            assertTrue(mHandler.map(mh -> controller.canTripCircuitBreaker(mh.getHandler(RestRequest.Method.GET))).orElse(true));
        }
        // assume trip even on unknown paths
        fakeRequest = new FakeRestRequest.Builder(xContentRegistry()).withPath("/unknown-path").build();
        for (Iterator<MethodHandlers> it = controller.getAllHandlers(fakeRequest); it.hasNext(); ) {
            Optional<MethodHandlers> mHandler = Optional.ofNullable(it.next());
            assertTrue(mHandler.map(mh -> controller.canTripCircuitBreaker(mh.getHandler(RestRequest.Method.GET))).orElse(true));
        }
        fakeRequest = new FakeRestRequest.Builder(xContentRegistry()).withPath("/do-not-trip").build();
        for (Iterator<MethodHandlers> it = controller.getAllHandlers(fakeRequest); it.hasNext(); ) {
            Optional<MethodHandlers> mHandler = Optional.ofNullable(it.next());
            assertFalse(mHandler.map(mh -> controller.canTripCircuitBreaker(mh.getHandler(RestRequest.Method.GET))).orElse(false));
        }
    }

    public void testRegisterAsDeprecatedHandler() {
        RestController controller = mock(RestController.class);

        RestRequest.Method method = randomFrom(RestRequest.Method.values());
        String path = "/_" + randomAlphaOfLengthBetween(1, 6);
        RestHandler handler = mock(RestHandler.class);
        String deprecationMessage = randomAlphaOfLengthBetween(1, 10);
        DeprecationLogger logger = mock(DeprecationLogger.class);

        // don't want to test everything -- just that it actually wraps the handler
        doCallRealMethod().when(controller).registerAsDeprecatedHandler(method, path, handler, deprecationMessage, logger);

        controller.registerAsDeprecatedHandler(method, path, handler, deprecationMessage, logger);

        verify(controller).registerHandler(eq(method), eq(path), any(DeprecationRestHandler.class));
    }

    public void testRegisterWithDeprecatedHandler() {
        final RestController controller = mock(RestController.class);

        final RestRequest.Method method = randomFrom(RestRequest.Method.values());
        final String path = "/_" + randomAlphaOfLengthBetween(1, 6);
        final RestHandler handler = mock(RestHandler.class);
        final RestRequest.Method deprecatedMethod = randomFrom(RestRequest.Method.values());
        final String deprecatedPath = "/_" + randomAlphaOfLengthBetween(1, 6);
        final DeprecationLogger logger = mock(DeprecationLogger.class);

        final String deprecationMessage = "[" + deprecatedMethod.name() + " " + deprecatedPath + "] is deprecated! Use [" +
            method.name() + " " + path + "] instead.";

        // don't want to test everything -- just that it actually wraps the handlers
        doCallRealMethod().when(controller).registerWithDeprecatedHandler(method, path, handler, deprecatedMethod, deprecatedPath, logger);

        controller.registerWithDeprecatedHandler(method, path, handler, deprecatedMethod, deprecatedPath, logger);

        verify(controller).registerHandler(method, path, handler);
        verify(controller).registerAsDeprecatedHandler(deprecatedMethod, deprecatedPath, handler, deprecationMessage, logger);
    }

    public void testRestHandlerWrapper() throws Exception {
        AtomicBoolean handlerCalled = new AtomicBoolean(false);
        AtomicBoolean wrapperCalled = new AtomicBoolean(false);
        RestHandler handler = (RestRequest request, RestChannel channel, NodeClient client) -> {
            handlerCalled.set(true);
        };
        UnaryOperator<RestHandler> wrapper = h -> {
            assertSame(handler, h);
            return (RestRequest request, RestChannel channel, NodeClient client) -> wrapperCalled.set(true);
        };
        final RestController restController = new RestController(Settings.EMPTY, Collections.emptySet(), wrapper, null,
                circuitBreakerService, usageService);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        restController.dispatchRequest(new FakeRestRequest.Builder(xContentRegistry()).build(), null, null, Optional.of(handler));
        assertTrue(wrapperCalled.get());
        assertFalse(handlerCalled.get());
    }

    /**
     * Useful for testing with deprecation handler.
     */
    private static class FakeRestHandler implements RestHandler {
        private final boolean canTripCircuitBreaker;

        private FakeRestHandler(boolean canTripCircuitBreaker) {
            this.canTripCircuitBreaker = canTripCircuitBreaker;
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
            //no op
        }

        @Override
        public boolean canTripCircuitBreaker() {
            return canTripCircuitBreaker;
        }
    }

    public void testDispatchRequestAddsAndFreesBytesOnSuccess() {
        int contentLength = BREAKER_LIMIT.bytesAsInt();
        String content = randomAlphaOfLength(contentLength);
        TestRestRequest request = new TestRestRequest("/", content, XContentType.JSON);
        AssertingChannel channel = new AssertingChannel(request, true, RestStatus.OK);

        restController.dispatchRequest(request, channel, new ThreadContext(Settings.EMPTY));

        assertEquals(0, inFlightRequestsBreaker.getTrippedCount());
        assertEquals(0, inFlightRequestsBreaker.getUsed());
    }

    public void testDispatchRequestAddsAndFreesBytesOnError() {
        int contentLength = BREAKER_LIMIT.bytesAsInt();
        String content = randomAlphaOfLength(contentLength);
        TestRestRequest request = new TestRestRequest("/error", content, XContentType.JSON);
        AssertingChannel channel = new AssertingChannel(request, true, RestStatus.BAD_REQUEST);

        restController.dispatchRequest(request, channel, new ThreadContext(Settings.EMPTY));

        assertEquals(0, inFlightRequestsBreaker.getTrippedCount());
        assertEquals(0, inFlightRequestsBreaker.getUsed());
    }

    public void testDispatchRequestAddsAndFreesBytesOnlyOnceOnError() {
        int contentLength = BREAKER_LIMIT.bytesAsInt();
        String content = randomAlphaOfLength(contentLength);
        // we will produce an error in the rest handler and one more when sending the error response
        TestRestRequest request = new TestRestRequest("/error", content, XContentType.JSON);
        ExceptionThrowingChannel channel = new ExceptionThrowingChannel(request, true);

        restController.dispatchRequest(request, channel, new ThreadContext(Settings.EMPTY));

        assertEquals(0, inFlightRequestsBreaker.getTrippedCount());
        assertEquals(0, inFlightRequestsBreaker.getUsed());
    }

    public void testDispatchRequestLimitsBytes() {
        int contentLength = BREAKER_LIMIT.bytesAsInt() + 1;
        String content = randomAlphaOfLength(contentLength);
        TestRestRequest request = new TestRestRequest("/", content, XContentType.JSON);
        AssertingChannel channel = new AssertingChannel(request, true, RestStatus.SERVICE_UNAVAILABLE);

        restController.dispatchRequest(request, channel, new ThreadContext(Settings.EMPTY));

        assertEquals(1, inFlightRequestsBreaker.getTrippedCount());
        assertEquals(0, inFlightRequestsBreaker.getUsed());
    }

    public void testDispatchRequiresContentTypeForRequestsWithContent() {
        String content = randomAlphaOfLengthBetween(1, BREAKER_LIMIT.bytesAsInt());
        TestRestRequest request = new TestRestRequest("/", content, null);
        AssertingChannel channel = new AssertingChannel(request, true, RestStatus.NOT_ACCEPTABLE);
        restController = new RestController(
            Settings.builder().put(HttpTransportSettings.SETTING_HTTP_CONTENT_TYPE_REQUIRED.getKey(), true).build(),
                Collections.emptySet(), null, null, circuitBreakerService, usageService);
        restController.registerHandler(RestRequest.Method.GET, "/",
            (r, c, client) -> c.sendResponse(
                new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY)));

        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(request, channel, new ThreadContext(Settings.EMPTY));
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchDoesNotRequireContentTypeForRequestsWithoutContent() {
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.OK);

        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(fakeRestRequest, channel, new ThreadContext(Settings.EMPTY));
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchFailsWithPlainText() {
        String content = randomAlphaOfLengthBetween(1, BREAKER_LIMIT.bytesAsInt());
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withContent(new BytesArray(content), null).withPath("/foo")
            .withHeaders(Collections.singletonMap("Content-Type", Collections.singletonList("text/plain"))).build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.NOT_ACCEPTABLE);
        restController.registerHandler(RestRequest.Method.GET, "/foo", new RestHandler() {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
            }
        });

        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(fakeRestRequest, channel, new ThreadContext(Settings.EMPTY));
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchUnsupportedContentType() {
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withContent(new BytesArray("{}"), null).withPath("/")
            .withHeaders(Collections.singletonMap("Content-Type", Collections.singletonList("application/x-www-form-urlencoded"))).build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.NOT_ACCEPTABLE);

        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(fakeRestRequest, channel, new ThreadContext(Settings.EMPTY));
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchWorksWithNewlineDelimitedJson() {
        final String mimeType = "application/x-ndjson";
        String content = randomAlphaOfLengthBetween(1, BREAKER_LIMIT.bytesAsInt());
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withContent(new BytesArray(content), null).withPath("/foo")
            .withHeaders(Collections.singletonMap("Content-Type", Collections.singletonList(mimeType))).build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.OK);
        restController.registerHandler(RestRequest.Method.GET, "/foo", new RestHandler() {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
            }

            @Override
            public boolean supportsContentStream() {
                return true;
            }
        });

        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(fakeRestRequest, channel, new ThreadContext(Settings.EMPTY));
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchWithContentStream() {
        final String mimeType = randomFrom("application/json", "application/smile");
        String content = randomAlphaOfLengthBetween(1, BREAKER_LIMIT.bytesAsInt());
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withContent(new BytesArray(content), null).withPath("/foo")
            .withHeaders(Collections.singletonMap("Content-Type", Collections.singletonList(mimeType))).build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.OK);
        restController.registerHandler(RestRequest.Method.GET, "/foo", new RestHandler() {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
            }

            @Override
            public boolean supportsContentStream() {
                return true;
            }
        });

        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(fakeRestRequest, channel, new ThreadContext(Settings.EMPTY));
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchWithContentStreamNoContentType() {
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withContent(new BytesArray("{}"), null).withPath("/foo").build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.NOT_ACCEPTABLE);
        restController.registerHandler(RestRequest.Method.GET, "/foo", new RestHandler() {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
            }

            @Override
            public boolean supportsContentStream() {
                return true;
            }
        });

        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(fakeRestRequest, channel, new ThreadContext(Settings.EMPTY));
        assertTrue(channel.getSendResponseCalled());
    }

    public void testNonStreamingXContentCausesErrorResponse() throws IOException {
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withContent(YamlXContent.contentBuilder().startObject().endObject().bytes(), XContentType.YAML).withPath("/foo").build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.NOT_ACCEPTABLE);
        restController.registerHandler(RestRequest.Method.GET, "/foo", new RestHandler() {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
            }

            @Override
            public boolean supportsContentStream() {
                return true;
            }
        });
        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(fakeRestRequest, channel, new ThreadContext(Settings.EMPTY));
        assertTrue(channel.getSendResponseCalled());
    }

    public void testUnknownContentWithContentStream() {
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withContent(new BytesArray("aaaabbbbb"), null).withPath("/foo")
            .withHeaders(Collections.singletonMap("Content-Type", Collections.singletonList("foo/bar")))
            .build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.NOT_ACCEPTABLE);
        restController.registerHandler(RestRequest.Method.GET, "/foo", new RestHandler() {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
            }

            @Override
            public boolean supportsContentStream() {
                return true;
            }
        });
        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(fakeRestRequest, channel, new ThreadContext(Settings.EMPTY));
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchBadRequest() {
        final FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).build();
        final AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.BAD_REQUEST);
        restController.dispatchBadRequest(
                fakeRestRequest,
                channel,
                new ThreadContext(Settings.EMPTY),
                randomBoolean() ? new IllegalStateException("bad request") : new Throwable("bad request"));
        assertTrue(channel.getSendResponseCalled());
        assertThat(channel.getRestResponse().content().utf8ToString(), containsString("bad request"));
    }

    public void testDispatchBadRequestUnknownCause() {
        final FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).build();
        final AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.BAD_REQUEST);
        restController.dispatchBadRequest(fakeRestRequest, channel, new ThreadContext(Settings.EMPTY), null);
        assertTrue(channel.getSendResponseCalled());
        assertThat(channel.getRestResponse().content().utf8ToString(), containsString("unknown cause"));
    }

    private static final class TestHttpServerTransport extends AbstractLifecycleComponent implements
        HttpServerTransport {

        TestHttpServerTransport() {
            super(Settings.EMPTY);
        }

        @Override
        protected void doStart() {
        }

        @Override
        protected void doStop() {
        }

        @Override
        protected void doClose() {
        }

        @Override
        public BoundTransportAddress boundAddress() {
            TransportAddress transportAddress = buildNewFakeTransportAddress();
            return new BoundTransportAddress(new TransportAddress[] {transportAddress} ,transportAddress);
        }

        @Override
        public HttpInfo info() {
            return null;
        }

        @Override
        public HttpStats stats() {
            return null;
        }
    }

    private static final class AssertingChannel extends AbstractRestChannel {

        private final RestStatus expectedStatus;
        private final AtomicReference<RestResponse> responseReference = new AtomicReference<>();

        protected AssertingChannel(RestRequest request, boolean detailedErrorsEnabled, RestStatus expectedStatus) {
            super(request, detailedErrorsEnabled);
            this.expectedStatus = expectedStatus;
        }

        @Override
        public void sendResponse(RestResponse response) {
            assertEquals(expectedStatus, response.status());
            responseReference.set(response);
        }

        RestResponse getRestResponse() {
            return responseReference.get();
        }

        boolean getSendResponseCalled() {
            return getRestResponse() != null;
        }

    }

    private static final class ExceptionThrowingChannel extends AbstractRestChannel {

        protected ExceptionThrowingChannel(RestRequest request, boolean detailedErrorsEnabled) {
            super(request, detailedErrorsEnabled);
        }

        @Override
        public void sendResponse(RestResponse response) {
            throw new IllegalStateException("always throwing an exception for testing");
        }
    }

    private static final class TestRestRequest extends RestRequest {

        private final BytesReference content;

        private TestRestRequest(String path, String content, XContentType xContentType) {
            super(NamedXContentRegistry.EMPTY, Collections.emptyMap(), path, xContentType == null ?
                Collections.emptyMap() : Collections.singletonMap("Content-Type", Collections.singletonList(xContentType.mediaType())));
            this.content = new BytesArray(content);
        }

        @Override
        public Method method() {
            return Method.GET;
        }

        @Override
        public String uri() {
            return null;
        }

        @Override
        public boolean hasContent() {
            return true;
        }

        @Override
        public BytesReference content() {
            return content;
        }

    }
}
