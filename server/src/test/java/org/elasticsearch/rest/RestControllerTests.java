/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.http.HttpHeadersValidationException;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.RestHandler.Route;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.yaml.YamlXContent;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestController.ELASTIC_PRODUCT_HTTP_HEADER;
import static org.elasticsearch.rest.RestController.ELASTIC_PRODUCT_HTTP_HEADER_VALUE;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.OPTIONS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RestControllerTests extends ESTestCase {

    private static final ByteSizeValue BREAKER_LIMIT = new ByteSizeValue(20);
    private CircuitBreaker inFlightRequestsBreaker;
    private RestController restController;
    private HierarchyCircuitBreakerService circuitBreakerService;
    private UsageService usageService;
    private NodeClient client;
    private List<RestRequest.Method> methodList;

    @Before
    public void setup() {
        circuitBreakerService = new HierarchyCircuitBreakerService(
            Settings.builder()
                .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), BREAKER_LIMIT)
                // We want to have reproducible results in this test, hence we disable real memory usage accounting
                .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
                .build(),
            Collections.emptyList(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        usageService = new UsageService();
        // we can do this here only because we know that we don't adjust breaker settings dynamically in the test
        inFlightRequestsBreaker = circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);

        HttpServerTransport httpServerTransport = new TestHttpServerTransport();
        client = new NoOpNodeClient(this.getTestName());
        restController = new RestController(null, client, circuitBreakerService, usageService);
        restController.registerHandler(
            new Route(GET, "/"),
            (request, channel, client) -> channel.sendResponse(
                new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY)
            )
        );
        restController.registerHandler(
            new Route(GET, "/error"),
            (request, channel, client) -> { throw new IllegalArgumentException("test error"); }
        );
        httpServerTransport.start();
        methodList = Arrays.stream(RestRequest.Method.values()).filter(m -> m != OPTIONS).collect(Collectors.toList());
    }

    @After
    public void teardown() throws IOException {
        IOUtils.close(client);
    }

    public void testApplyProductSpecificResponseHeaders() {
        final ThreadContext threadContext = client.threadPool().getThreadContext();
        final RestController restController = new RestController(null, null, circuitBreakerService, usageService);
        RestRequest fakeRequest = new FakeRestRequest.Builder(xContentRegistry()).build();
        AssertingChannel channel = new AssertingChannel(fakeRequest, false, RestStatus.BAD_REQUEST);
        restController.dispatchRequest(fakeRequest, channel, threadContext);
        // the rest controller relies on the caller to stash the context, so we should expect these values here as we didn't stash the
        // context in this test
        List<String> expectedProductResponseHeader = new ArrayList<>();
        expectedProductResponseHeader.add(ELASTIC_PRODUCT_HTTP_HEADER_VALUE);
        assertEquals(expectedProductResponseHeader, threadContext.getResponseHeaders().getOrDefault(ELASTIC_PRODUCT_HTTP_HEADER, null));
    }

    public void testRequestWithDisallowedMultiValuedHeader() {
        final ThreadContext threadContext = client.threadPool().getThreadContext();
        Set<RestHeaderDefinition> headers = new HashSet<>(
            Arrays.asList(new RestHeaderDefinition("header.1", true), new RestHeaderDefinition("header.2", false))
        );
        final RestController restController = new RestController(null, null, circuitBreakerService, usageService);
        Map<String, List<String>> restHeaders = new HashMap<>();
        restHeaders.put("header.1", Collections.singletonList("boo"));
        restHeaders.put("header.2", Arrays.asList("foo", "bar"));
        RestRequest fakeRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(restHeaders).build();
        AssertingChannel channel = new AssertingChannel(fakeRequest, false, RestStatus.BAD_REQUEST);
        restController.dispatchRequest(fakeRequest, channel, threadContext);
        assertTrue(channel.getSendResponseCalled());
    }

    public void testRequestWithDisallowedMultiValuedHeaderButSameValues() {
        final ThreadContext threadContext = client.threadPool().getThreadContext();
        Set<RestHeaderDefinition> headers = new HashSet<>(
            Arrays.asList(new RestHeaderDefinition("header.1", true), new RestHeaderDefinition("header.2", false))
        );
        final RestController restController = new RestController(null, client, circuitBreakerService, usageService);
        Map<String, List<String>> restHeaders = new HashMap<>();
        restHeaders.put("header.1", Collections.singletonList("boo"));
        restHeaders.put("header.2", Arrays.asList("foo", "foo"));
        RestRequest fakeRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(restHeaders).withPath("/bar").build();
        restController.registerHandler(
            new Route(GET, "/bar"),
            (request, channel, client) -> channel.sendResponse(
                new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY)
            )
        );
        AssertingChannel channel = new AssertingChannel(fakeRequest, false, RestStatus.OK);
        restController.dispatchRequest(fakeRequest, channel, threadContext);
        assertTrue(channel.getSendResponseCalled());
    }

    public void testRegisterAsDeprecatedHandler() {
        RestController controller = mock(RestController.class);

        RestRequest.Method method = randomFrom(methodList);
        String path = "/_" + randomAlphaOfLengthBetween(1, 6);
        RestHandler handler = (request, channel, client) -> {};
        String deprecationMessage = randomAlphaOfLengthBetween(1, 10);

        Route route = Route.builder(method, path).deprecated(deprecationMessage, RestApiVersion.V_7).build();

        // don't want to test everything -- just that it actually wraps the handler
        doCallRealMethod().when(controller).registerHandler(route, handler);
        doCallRealMethod().when(controller).registerAsDeprecatedHandler(method, path, handler, deprecationMessage, null);

        controller.registerHandler(route, handler);

        verify(controller).registerHandler(eq(method), eq(path), any(DeprecationRestHandler.class));
    }

    public void testRegisterAsReplacedHandler() {
        final RestController controller = mock(RestController.class);

        final RestRequest.Method method = randomFrom(methodList);
        final String path = "/_" + randomAlphaOfLengthBetween(1, 6);
        final RestHandler handler = (request, channel, client) -> {};
        final RestRequest.Method replacedMethod = randomFrom(methodList);
        final String replacedPath = "/_" + randomAlphaOfLengthBetween(1, 6);
        final String deprecationMessage = "["
            + replacedMethod.name()
            + " "
            + replacedPath
            + "] is deprecated! Use ["
            + method.name()
            + " "
            + path
            + "] instead.";

        final Route route = Route.builder(method, path).replaces(replacedMethod, replacedPath, RestApiVersion.V_7).build();

        // don't want to test everything -- just that it actually wraps the handlers
        doCallRealMethod().when(controller).registerHandler(route, handler);
        doCallRealMethod().when(controller).registerAsReplacedHandler(method, path, handler, replacedMethod, replacedPath);

        controller.registerHandler(route, handler);

        verify(controller).registerHandler(method, path, handler);
        verify(controller).registerAsDeprecatedHandler(replacedMethod, replacedPath, handler, deprecationMessage);
    }

    public void testRegisterSecondMethodWithDifferentNamedWildcard() {
        final RestController restController = new RestController(null, null, circuitBreakerService, usageService);

        RestRequest.Method firstMethod = randomFrom(methodList);
        RestRequest.Method secondMethod = randomFrom(methodList.stream().filter(m -> m != firstMethod).collect(Collectors.toList()));

        final String path = "/_" + randomAlphaOfLengthBetween(1, 6);

        RestHandler handler = (request, channel, client) -> {};

        restController.registerHandler(new Route(firstMethod, path + "/{wildcard1}"), handler);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> restController.registerHandler(new Route(secondMethod, path + "/{wildcard2}"), handler)
        );

        assertThat(exception.getMessage(), equalTo("Trying to use conflicting wildcard names for same path: wildcard1 and wildcard2"));
    }

    public void testRestHandlerWrapper() throws Exception {
        AtomicBoolean handlerCalled = new AtomicBoolean(false);
        AtomicBoolean wrapperCalled = new AtomicBoolean(false);
        final RestHandler handler = (RestRequest request, RestChannel channel, NodeClient client) -> handlerCalled.set(true);
        final HttpServerTransport httpServerTransport = new TestHttpServerTransport();
        final RestController restController = new RestController(h -> {
            assertSame(handler, h);
            return (RestRequest request, RestChannel channel, NodeClient client) -> wrapperCalled.set(true);
        }, client, circuitBreakerService, usageService);
        restController.registerHandler(new Route(GET, "/wrapped"), handler);
        RestRequest request = testRestRequest("/wrapped", "{}", XContentType.JSON);
        AssertingChannel channel = new AssertingChannel(request, true, RestStatus.BAD_REQUEST);
        restController.dispatchRequest(request, channel, client.threadPool().getThreadContext());
        httpServerTransport.start();
        assertTrue(wrapperCalled.get());
        assertFalse(handlerCalled.get());
    }

    public void testDispatchRequestAddsAndFreesBytesOnSuccess() {
        int contentLength = BREAKER_LIMIT.bytesAsInt();
        String content = randomAlphaOfLength((int) Math.round(contentLength / inFlightRequestsBreaker.getOverhead()));
        RestRequest request = testRestRequest("/", content, XContentType.JSON);
        AssertingChannel channel = new AssertingChannel(request, true, RestStatus.OK);

        restController.dispatchRequest(request, channel, client.threadPool().getThreadContext());

        assertEquals(0, inFlightRequestsBreaker.getTrippedCount());
        assertEquals(0, inFlightRequestsBreaker.getUsed());
    }

    public void testDispatchRequestAddsAndFreesBytesOnError() {
        int contentLength = BREAKER_LIMIT.bytesAsInt();
        String content = randomAlphaOfLength((int) Math.round(contentLength / inFlightRequestsBreaker.getOverhead()));
        RestRequest request = testRestRequest("/error", content, XContentType.JSON);
        AssertingChannel channel = new AssertingChannel(request, true, RestStatus.BAD_REQUEST);

        restController.dispatchRequest(request, channel, client.threadPool().getThreadContext());

        assertEquals(0, inFlightRequestsBreaker.getTrippedCount());
        assertEquals(0, inFlightRequestsBreaker.getUsed());
    }

    public void testDispatchRequestAddsAndFreesBytesOnlyOnceOnError() {
        int contentLength = BREAKER_LIMIT.bytesAsInt();
        String content = randomAlphaOfLength((int) Math.round(contentLength / inFlightRequestsBreaker.getOverhead()));
        // we will produce an error in the rest handler and one more when sending the error response
        RestRequest request = testRestRequest("/error", content, XContentType.JSON);
        ExceptionThrowingChannel channel = new ExceptionThrowingChannel(request, true);

        restController.dispatchRequest(request, channel, client.threadPool().getThreadContext());

        assertEquals(0, inFlightRequestsBreaker.getTrippedCount());
        assertEquals(0, inFlightRequestsBreaker.getUsed());
    }

    public void testDispatchRequestAddsAndFreesBytesOnlyOnceOnErrorDuringSend() {
        int contentLength = Math.toIntExact(BREAKER_LIMIT.getBytes());
        String content = randomAlphaOfLength((int) Math.round(contentLength / inFlightRequestsBreaker.getOverhead()));
        // use a real recycler that tracks leaks and create some content bytes in the test handler to check for leaks
        final BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        restController.registerHandler(
            new Route(GET, "/foo"),
            (request, c, client) -> new RestToXContentListener<>(c).onResponse((b, p) -> b.startObject().endObject())
        );
        // we will produce an error in the rest handler and one more when sending the error response
        RestRequest request = testRestRequest("/foo", content, XContentType.JSON);
        ExceptionThrowingChannel channel = new ExceptionThrowingChannel(request, true) {
            @Override
            protected BytesStreamOutput newBytesOutput() {
                return new ReleasableBytesStreamOutput(bigArrays);
            }
        };

        restController.dispatchRequest(request, channel, client.threadPool().getThreadContext());

        assertEquals(0, inFlightRequestsBreaker.getTrippedCount());
        assertEquals(0, inFlightRequestsBreaker.getUsed());
    }

    public void testDispatchRequestLimitsBytes() {
        int contentLength = BREAKER_LIMIT.bytesAsInt() + 1;
        String content = randomAlphaOfLength((int) Math.round(contentLength / inFlightRequestsBreaker.getOverhead()));
        RestRequest request = testRestRequest("/", content, XContentType.JSON);
        AssertingChannel channel = new AssertingChannel(request, true, RestStatus.TOO_MANY_REQUESTS);

        restController.dispatchRequest(request, channel, client.threadPool().getThreadContext());

        assertEquals(1, inFlightRequestsBreaker.getTrippedCount());
        assertEquals(0, inFlightRequestsBreaker.getUsed());
    }

    public void testDispatchRequiresContentTypeForRequestsWithContent() {
        String content = randomAlphaOfLength((int) Math.round(BREAKER_LIMIT.getBytes() / inFlightRequestsBreaker.getOverhead()));
        RestRequest request = testRestRequest("/", content, null);
        AssertingChannel channel = new AssertingChannel(request, true, RestStatus.NOT_ACCEPTABLE);
        restController = new RestController(null, null, circuitBreakerService, usageService);
        restController.registerHandler(
            GET,
            "/",
            (r, c, client) -> c.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY))
        );

        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(request, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchDoesNotRequireContentTypeForRequestsWithoutContent() {
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.OK);

        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchFailsWithPlainText() {
        String content = randomAlphaOfLength((int) Math.round(BREAKER_LIMIT.getBytes() / inFlightRequestsBreaker.getOverhead()));
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
            new BytesArray(content),
            null
        ).withPath("/foo").withHeaders(Collections.singletonMap("Content-Type", Collections.singletonList("text/plain"))).build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.NOT_ACCEPTABLE);
        restController.registerHandler(
            GET,
            "/foo",
            (request, channel1, client) -> channel1.sendResponse(
                new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY)
            )
        );

        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchUnsupportedContentType() {
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(new BytesArray("{}"), null)
            .withPath("/")
            .withHeaders(Collections.singletonMap("Content-Type", Collections.singletonList("application/x-www-form-urlencoded")))
            .build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.NOT_ACCEPTABLE);

        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchWorksWithNewlineDelimitedJson() {
        final String mimeType = "application/x-ndjson";
        String content = randomAlphaOfLength((int) Math.round(BREAKER_LIMIT.getBytes() / inFlightRequestsBreaker.getOverhead()));
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
            new BytesArray(content),
            null
        ).withPath("/foo").withHeaders(Collections.singletonMap("Content-Type", Collections.singletonList(mimeType))).build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.OK);
        restController.registerHandler(new Route(GET, "/foo"), new RestHandler() {
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
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchWithContentStream() {
        final String mimeType = randomFrom("application/json", "application/smile");
        String content = randomAlphaOfLength((int) Math.round(BREAKER_LIMIT.getBytes() / inFlightRequestsBreaker.getOverhead()));
        final List<String> contentTypeHeader = Collections.singletonList(mimeType);
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
            new BytesArray(content),
            RestRequest.parseContentType(contentTypeHeader)
        ).withPath("/foo").withHeaders(Collections.singletonMap("Content-Type", contentTypeHeader)).build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.OK);
        restController.registerHandler(new Route(GET, "/foo"), new RestHandler() {
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
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchWithContentStreamNoContentType() {
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(new BytesArray("{}"), null)
            .withPath("/foo")
            .build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.NOT_ACCEPTABLE);
        restController.registerHandler(new Route(GET, "/foo"), new RestHandler() {
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
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
    }

    public void testNonStreamingXContentCausesErrorResponse() throws IOException {
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
            BytesReference.bytes(YamlXContent.contentBuilder().startObject().endObject()),
            XContentType.YAML
        ).withPath("/foo").build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.NOT_ACCEPTABLE);
        restController.registerHandler(new Route(GET, "/foo"), new RestHandler() {
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
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
    }

    public void testUnknownContentWithContentStream() {
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
            new BytesArray("aaaabbbbb"),
            null
        ).withPath("/foo").withHeaders(Collections.singletonMap("Content-Type", Collections.singletonList("foo/bar"))).build();
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.NOT_ACCEPTABLE);
        restController.registerHandler(new Route(GET, "/foo"), new RestHandler() {
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
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
    }

    public void testDispatchBadRequest() {
        final FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).build();
        final AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.BAD_REQUEST);
        restController.dispatchBadRequest(
            channel,
            client.threadPool().getThreadContext(),
            randomBoolean() ? new IllegalStateException("bad request") : new Throwable("bad request")
        );
        assertTrue(channel.getSendResponseCalled());
        assertThat(channel.getRestResponse().content().utf8ToString(), containsString("bad request"));
    }

    public void testDoesNotConsumeContent() throws Exception {
        final RestRequest.Method method = randomFrom(methodList);
        restController.registerHandler(new Route(method, "/notconsumed"), new RestHandler() {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
            }

            @Override
            public boolean canTripCircuitBreaker() {
                return false;
            }
        });

        final XContentBuilder content = XContentBuilder.builder(randomFrom(XContentType.values()).xContent())
            .startObject()
            .field("field", "value")
            .endObject();
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withPath("/notconsumed")
            .withMethod(method)
            .withContent(BytesReference.bytes(content), content.contentType())
            .build();

        final AssertingChannel channel = new AssertingChannel(restRequest, true, RestStatus.OK);
        assertFalse(channel.getSendResponseCalled());
        assertFalse(restRequest.isContentConsumed());

        restController.dispatchRequest(restRequest, channel, new ThreadContext(Settings.EMPTY));

        assertTrue(channel.getSendResponseCalled());
        assertFalse("RestController must not consume request content", restRequest.isContentConsumed());
    }

    public void testDispatchBadRequestUnknownCause() {
        final FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).build();
        final AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.BAD_REQUEST);
        restController.dispatchBadRequest(channel, client.threadPool().getThreadContext(), null);
        assertTrue(channel.getSendResponseCalled());
        assertThat(channel.getRestResponse().content().utf8ToString(), containsString("unknown cause"));
    }

    public void testDispatchBadRequestWithValidationException() {
        final RestStatus status = randomFrom(RestStatus.values());
        final Exception exception = new ElasticsearchStatusException("bad bad exception", status);
        final FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).build();

        // it's always a 400 bad request when dispatching "regular" {@code ElasticsearchException}
        AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.BAD_REQUEST);
        assertFalse(channel.getSendResponseCalled());
        restController.dispatchBadRequest(channel, client.threadPool().getThreadContext(), exception);
        assertTrue(channel.getSendResponseCalled());
        assertThat(channel.getRestResponse().content().utf8ToString(), containsString("bad bad exception"));

        // but {@code HttpHeadersValidationException} do carry over the rest response code
        channel = new AssertingChannel(fakeRestRequest, true, status);
        assertFalse(channel.getSendResponseCalled());
        restController.dispatchBadRequest(channel, client.threadPool().getThreadContext(), new HttpHeadersValidationException(exception));
        assertTrue(channel.getSendResponseCalled());
        assertThat(channel.getRestResponse().content().utf8ToString(), containsString("bad bad exception"));
    }

    public void testFavicon() {
        final FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(GET)
            .withPath("/favicon.ico")
            .build();
        final AssertingChannel channel = new AssertingChannel(fakeRestRequest, false, RestStatus.OK);
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
        assertThat(channel.getRestResponse().contentType(), containsString("image/x-icon"));
    }

    public void testFaviconWithWrongHttpMethod() {
        final FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(
            randomValueOtherThanMany(m -> m == GET || m == OPTIONS, () -> randomFrom(RestRequest.Method.values()))
        ).withPath("/favicon.ico").build();
        final AssertingChannel channel = new AssertingChannel(fakeRestRequest, true, RestStatus.METHOD_NOT_ALLOWED);
        restController.dispatchRequest(fakeRestRequest, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
        assertThat(channel.getRestResponse().getHeaders().containsKey("Allow"), equalTo(true));
        assertThat(channel.getRestResponse().getHeaders().get("Allow"), hasItem(equalTo(GET.toString())));
    }

    public void testDispatchUnsupportedHttpMethod() {
        final boolean hasContent = randomBoolean();
        final RestRequest request = RestRequest.request(xContentRegistry(), new HttpRequest() {
            @Override
            public RestRequest.Method method() {
                throw new IllegalArgumentException("test");
            }

            @Override
            public String uri() {
                return "/";
            }

            @Override
            public BytesReference content() {
                if (hasContent) {
                    return new BytesArray("test");
                }
                return BytesArray.EMPTY;
            }

            @Override
            public Map<String, List<String>> getHeaders() {
                Map<String, List<String>> headers = new HashMap<>();
                if (hasContent) {
                    headers.put("Content-Type", Collections.singletonList("text/plain"));
                }
                return headers;
            }

            @Override
            public List<String> strictCookies() {
                return null;
            }

            @Override
            public HttpVersion protocolVersion() {
                return randomFrom(HttpVersion.values());
            }

            @Override
            public HttpRequest removeHeader(String header) {
                return this;
            }

            @Override
            public HttpResponse createResponse(RestStatus status, BytesReference content) {
                return null;
            }

            @Override
            public void release() {}

            @Override
            public HttpRequest releaseAndCopy() {
                return this;
            }

            @Override
            public Exception getInboundException() {
                return null;
            }
        }, null);

        final AssertingChannel channel = new AssertingChannel(request, true, RestStatus.METHOD_NOT_ALLOWED);
        assertFalse(channel.getSendResponseCalled());
        restController.dispatchRequest(request, channel, client.threadPool().getThreadContext());
        assertTrue(channel.getSendResponseCalled());
        assertThat(channel.getRestResponse().getHeaders().containsKey("Allow"), equalTo(true));
        assertThat(channel.getRestResponse().getHeaders().get("Allow"), hasItem(equalTo(GET.toString())));
    }

    public void testRegisterWithReservedPath() {
        final RestController restController = new RestController(null, client, circuitBreakerService, usageService);
        for (String path : RestController.RESERVED_PATHS) {
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> {
                restController.registerHandler(new Route(randomFrom(methodList), path), new RestHandler() {
                    @Override
                    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) {
                        channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
                    }

                    @Override
                    public boolean supportsContentStream() {
                        return true;
                    }
                });
            });
            assertThat(iae.getMessage(), containsString("path [" + path + "] is a reserved path and may not be registered"));
        }
    }

    private static final class TestHttpServerTransport extends AbstractLifecycleComponent implements HttpServerTransport {

        TestHttpServerTransport() {}

        @Override
        protected void doStart() {}

        @Override
        protected void doStop() {}

        @Override
        protected void doClose() {}

        @Override
        public BoundTransportAddress boundAddress() {
            TransportAddress transportAddress = buildNewFakeTransportAddress();
            return new BoundTransportAddress(new TransportAddress[] { transportAddress }, transportAddress);
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

    public static final class AssertingChannel extends AbstractRestChannel {

        private final RestStatus expectedStatus;
        private final AtomicReference<RestResponse> responseReference = new AtomicReference<>();

        public AssertingChannel(RestRequest request, boolean detailedErrorsEnabled, RestStatus expectedStatus) {
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

    private static class ExceptionThrowingChannel extends AbstractRestChannel {

        protected ExceptionThrowingChannel(RestRequest request, boolean detailedErrorsEnabled) {
            super(request, detailedErrorsEnabled);
        }

        @Override
        public void sendResponse(RestResponse response) {
            try {
                throw new IllegalStateException("always throwing an exception for testing");
            } finally {
                // the production implementation in DefaultRestChannel always releases the output buffer, so we must too
                releaseOutputBuffer();
            }
        }
    }

    private static RestRequest testRestRequest(String path, String content, XContentType xContentType) {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY);
        builder.withPath(path);
        builder.withContent(new BytesArray(content), xContentType);
        return builder.build();
    }
}
