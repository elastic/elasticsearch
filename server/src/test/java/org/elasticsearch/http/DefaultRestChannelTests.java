/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DefaultRestChannelTests extends ESTestCase {

    private ThreadPool threadPool;
    private Recycler<BytesRef> bigArrays;
    private HttpChannel httpChannel;
    private HttpTracer httpTracer;
    private Tracer tracer;

    @Before
    public void setup() {
        httpChannel = mock(HttpChannel.class);
        threadPool = new TestThreadPool("test");
        bigArrays = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
        httpTracer = mock(HttpTracer.class);
        tracer = mock(Tracer.class);
    }

    @After
    public void shutdown() {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
    }

    public void testResponse() {
        final TestHttpResponse response = executeRequest(Settings.EMPTY, "request-host");
        assertThat(response.content(), equalTo(testRestResponse().content()));
    }

    public void testCorsEnabledWithoutAllowOrigins() {
        // Set up an HTTP transport with only the CORS enabled setting
        Settings settings = Settings.builder().put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true).build();
        TestHttpResponse response = executeRequest(settings, "request-host");
        assertThat(response.headers().get(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN), nullValue());
    }

    public void testCorsEnabledWithAllowOrigins() {
        final String originValue = "remote-host";
        final String pattern;
        if (randomBoolean()) {
            pattern = originValue;
        } else {
            pattern = "/remote-hos.+/";
        }
        // create an HTTP transport with CORS enabled and allow origin configured
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .put(HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN.getKey(), pattern)
            .build();
        TestHttpResponse response = executeRequest(settings, originValue, "https://127.0.0.1");
        assertEquals(originValue, response.headers().get(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN).get(0));
        assertThat(response.headers().get(CorsHandler.VARY), containsInAnyOrder(CorsHandler.ORIGIN));
    }

    public void testCorsEnabledWithAllowOriginsAndAllowCredentials() {
        final String originValue = "remote-host";
        // create an HTTP transport with CORS enabled and allow origin configured
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .put(HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN.getKey(), CorsHandler.ANY_ORIGIN)
            .put(HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS.getKey(), true)
            .build();
        TestHttpResponse response = executeRequest(settings, originValue, "https://127.0.0.1");
        assertEquals(originValue, response.headers().get(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN).get(0));
        assertEquals(CorsHandler.ORIGIN, response.headers().get(CorsHandler.VARY).get(0));
        assertEquals("true", response.headers().get(CorsHandler.ACCESS_CONTROL_ALLOW_CREDENTIALS).get(0));
    }

    public void testThatAnyOriginWorks() {
        final String originValue = CorsHandler.ANY_ORIGIN;
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .put(HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN.getKey(), originValue)
            .build();
        TestHttpResponse response = executeRequest(settings, originValue, "https://127.0.0.1");
        assertEquals(originValue, response.headers().get(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN).get(0));
        assertNull(response.headers().get(CorsHandler.VARY));
    }

    public void testHeadersSet() {
        Settings settings = Settings.builder().build();
        final TestHttpRequest httpRequest = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
        httpRequest.getHeaders().put(Task.X_OPAQUE_ID_HTTP_HEADER, Collections.singletonList("abc"));
        final RestRequest request = RestRequest.request(parserConfig(), httpRequest, httpChannel);
        HttpHandlingSettings handlingSettings = HttpHandlingSettings.fromSettings(settings);

        // send a response
        DefaultRestChannel channel = new DefaultRestChannel(
            httpChannel,
            httpRequest,
            request,
            bigArrays,
            handlingSettings,
            threadPool.getThreadContext(),
            CorsHandler.fromSettings(settings),
            httpTracer,
            tracer
        );
        RestResponse resp = testRestResponse();
        final String customHeader = "custom-header";
        final String customHeaderValue = "xyz";
        resp.addHeader(customHeader, customHeaderValue);
        channel.sendResponse(resp);

        // inspect what was written
        ArgumentCaptor<TestHttpResponse> responseCaptor = ArgumentCaptor.forClass(TestHttpResponse.class);
        verify(httpChannel).sendResponse(responseCaptor.capture(), any());
        TestHttpResponse httpResponse = responseCaptor.getValue();
        Map<String, List<String>> headers = httpResponse.headers();
        assertNull(headers.get("non-existent-header"));
        assertEquals(customHeaderValue, headers.get(customHeader).get(0));
        assertEquals("abc", headers.get(Task.X_OPAQUE_ID_HTTP_HEADER).get(0));
        assertEquals(Integer.toString(resp.content().length()), headers.get(DefaultRestChannel.CONTENT_LENGTH).get(0));
        assertEquals(resp.contentType(), headers.get(DefaultRestChannel.CONTENT_TYPE).get(0));
    }

    public void testCookiesSet() {
        Settings settings = Settings.builder().put(HttpTransportSettings.SETTING_HTTP_RESET_COOKIES.getKey(), true).build();
        final TestHttpRequest httpRequest = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
        httpRequest.getHeaders().put(Task.X_OPAQUE_ID_HTTP_HEADER, Collections.singletonList("abc"));
        final RestRequest request = RestRequest.request(parserConfig(), httpRequest, httpChannel);
        HttpHandlingSettings handlingSettings = HttpHandlingSettings.fromSettings(settings);

        // send a response
        DefaultRestChannel channel = new DefaultRestChannel(
            httpChannel,
            httpRequest,
            request,
            bigArrays,
            handlingSettings,
            threadPool.getThreadContext(),
            CorsHandler.fromSettings(settings),
            httpTracer,
            tracer
        );
        channel.sendResponse(testRestResponse());

        // inspect what was written
        ArgumentCaptor<TestHttpResponse> responseCaptor = ArgumentCaptor.forClass(TestHttpResponse.class);
        verify(httpChannel).sendResponse(responseCaptor.capture(), any());
        TestHttpResponse nioResponse = responseCaptor.getValue();
        Map<String, List<String>> headers = nioResponse.headers();
        assertThat(headers.get(DefaultRestChannel.SET_COOKIE), hasItem("cookie"));
        assertThat(headers.get(DefaultRestChannel.SET_COOKIE), hasItem("cookie2"));
    }

    @SuppressWarnings("unchecked")
    public void testReleaseInListener() throws IOException {
        final Settings settings = Settings.builder().build();
        final TestHttpRequest httpRequest = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
        final RestRequest request = RestRequest.request(parserConfig(), httpRequest, httpChannel);
        HttpHandlingSettings handlingSettings = HttpHandlingSettings.fromSettings(settings);

        DefaultRestChannel channel = new DefaultRestChannel(
            httpChannel,
            httpRequest,
            request,
            bigArrays,
            handlingSettings,
            threadPool.getThreadContext(),
            CorsHandler.fromSettings(settings),
            httpTracer,
            tracer
        );
        final RestResponse response = new RestResponse(
            RestStatus.INTERNAL_SERVER_ERROR,
            JsonXContent.contentBuilder().startObject().endObject()
        );
        assertThat(response.content(), not(instanceOf(Releasable.class)));

        // ensure we have reserved bytes
        if (randomBoolean()) {
            BytesStream out = channel.bytesOutput();
            assertThat(out, instanceOf(RecyclerBytesStreamOutput.class));
        } else {
            try (XContentBuilder builder = channel.newBuilder()) {
                // do something builder
                builder.startObject().endObject();
            }
        }

        channel.sendResponse(response);
        Class<ActionListener<Void>> listenerClass = (Class<ActionListener<Void>>) (Class) ActionListener.class;
        ArgumentCaptor<ActionListener<Void>> listenerCaptor = ArgumentCaptor.forClass(listenerClass);
        verify(httpChannel).sendResponse(any(), listenerCaptor.capture());
        ActionListener<Void> listener = listenerCaptor.getValue();
        if (randomBoolean()) {
            listener.onResponse(null);
        } else {
            listener.onFailure(new ClosedChannelException());
        }
        // ESTestCase#after will invoke ensureAllArraysAreReleased which will fail if the response content was not released
    }

    @SuppressWarnings("unchecked")
    public void testConnectionClose() throws Exception {
        final Settings settings = Settings.builder().build();
        final HttpRequest httpRequest;
        final boolean brokenRequest = randomBoolean();
        final boolean close = brokenRequest || randomBoolean();
        if (brokenRequest) {
            httpRequest = new TestHttpRequest(
                () -> { throw new IllegalArgumentException("Can't parse HTTP version"); },
                RestRequest.Method.GET,
                "/"
            );
        } else if (randomBoolean()) {
            httpRequest = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
            if (close) {
                httpRequest.getHeaders().put(DefaultRestChannel.CONNECTION, Collections.singletonList(DefaultRestChannel.CLOSE));
            }
        } else {
            httpRequest = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_0, RestRequest.Method.GET, "/");
            if (close == false) {
                httpRequest.getHeaders().put(DefaultRestChannel.CONNECTION, Collections.singletonList(DefaultRestChannel.KEEP_ALIVE));
            }
        }
        final RestRequest request = RestRequest.request(parserConfig(), httpRequest, httpChannel);

        HttpHandlingSettings handlingSettings = HttpHandlingSettings.fromSettings(settings);

        DefaultRestChannel channel = new DefaultRestChannel(
            httpChannel,
            httpRequest,
            request,
            bigArrays,
            handlingSettings,
            threadPool.getThreadContext(),
            CorsHandler.fromSettings(settings),
            httpTracer,
            tracer
        );
        channel.sendResponse(testRestResponse());
        Class<ActionListener<Void>> listenerClass = (Class<ActionListener<Void>>) (Class) ActionListener.class;
        ArgumentCaptor<ActionListener<Void>> listenerCaptor = ArgumentCaptor.forClass(listenerClass);
        verify(httpChannel).sendResponse(any(), listenerCaptor.capture());
        ActionListener<Void> listener = listenerCaptor.getValue();
        if (randomBoolean()) {
            listener.onResponse(null);
        } else {
            listener.onFailure(new ClosedChannelException());
        }
        if (close) {
            verify(httpChannel, times(1)).close();
        } else {
            verify(httpChannel, times(0)).close();
        }
    }

    public void testUnsupportedHttpMethod() {
        final boolean close = randomBoolean();
        final HttpRequest.HttpVersion httpVersion = close ? HttpRequest.HttpVersion.HTTP_1_0 : HttpRequest.HttpVersion.HTTP_1_1;
        final String httpConnectionHeaderValue = close ? DefaultRestChannel.CLOSE : DefaultRestChannel.KEEP_ALIVE;
        final RestRequest request = RestRequest.request(parserConfig(), new TestHttpRequest(httpVersion, null, "/") {
            @Override
            public RestRequest.Method method() {
                throw new IllegalArgumentException("test");
            }
        }, httpChannel);
        request.getHttpRequest().getHeaders().put(DefaultRestChannel.CONNECTION, Collections.singletonList(httpConnectionHeaderValue));

        DefaultRestChannel channel = new DefaultRestChannel(
            httpChannel,
            request.getHttpRequest(),
            request,
            bigArrays,
            HttpHandlingSettings.fromSettings(Settings.EMPTY),
            threadPool.getThreadContext(),
            CorsHandler.fromSettings(Settings.EMPTY),
            httpTracer,
            tracer
        );

        // ESTestCase#after will invoke ensureAllArraysAreReleased which will fail if the response content was not released
        final BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        final ByteArray byteArray = bigArrays.newByteArray(0, false);
        final BytesReference content = new ReleasableBytesReference(BytesReference.fromByteArray(byteArray, 0), byteArray);
        channel.sendResponse(new RestResponse(RestStatus.METHOD_NOT_ALLOWED, RestResponse.TEXT_CONTENT_TYPE, content));

        @SuppressWarnings("unchecked")
        Class<ActionListener<Void>> listenerClass = (Class<ActionListener<Void>>) (Class<?>) ActionListener.class;
        ArgumentCaptor<ActionListener<Void>> listenerCaptor = ArgumentCaptor.forClass(listenerClass);
        verify(httpChannel).sendResponse(any(), listenerCaptor.capture());
        ActionListener<Void> listener = listenerCaptor.getValue();
        if (randomBoolean()) {
            listener.onResponse(null);
        } else {
            listener.onFailure(new ClosedChannelException());
        }
        if (close) {
            verify(httpChannel, times(1)).close();
        } else {
            verify(httpChannel, times(0)).close();
        }
    }

    public void testCloseOnException() {
        final boolean close = randomBoolean();
        final HttpRequest.HttpVersion httpVersion = close ? HttpRequest.HttpVersion.HTTP_1_0 : HttpRequest.HttpVersion.HTTP_1_1;
        final String httpConnectionHeaderValue = close ? DefaultRestChannel.CLOSE : DefaultRestChannel.KEEP_ALIVE;
        final RestRequest request = RestRequest.request(parserConfig(), new TestHttpRequest(httpVersion, null, "/") {
            @Override
            public HttpResponse createResponse(RestStatus status, BytesReference content) {
                throw new IllegalArgumentException("test");
            }
        }, httpChannel);
        request.getHttpRequest().getHeaders().put(DefaultRestChannel.CONNECTION, Collections.singletonList(httpConnectionHeaderValue));

        DefaultRestChannel channel = new DefaultRestChannel(
            httpChannel,
            request.getHttpRequest(),
            request,
            bigArrays,
            HttpHandlingSettings.fromSettings(Settings.EMPTY),
            threadPool.getThreadContext(),
            CorsHandler.fromSettings(Settings.EMPTY),
            httpTracer,
            tracer
        );

        // ESTestCase#after will invoke ensureAllArraysAreReleased which will fail if the response content was not released
        final BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        final ByteArray byteArray = bigArrays.newByteArray(0, false);
        final BytesReference content = new ReleasableBytesReference(BytesReference.fromByteArray(byteArray, 0), byteArray);

        expectThrows(
            IllegalArgumentException.class,
            () -> channel.sendResponse(new RestResponse(RestStatus.OK, RestResponse.TEXT_CONTENT_TYPE, content))
        );

        if (close) {
            verify(httpChannel, times(1)).close();
        } else {
            verify(httpChannel, times(0)).close();
        }
    }

    /**
     * Check that when a REST channel sends a response, then it records the HTTP response code and stops the active trace.
     */
    public void testTraceStopped() {
        // Configure the httpChannel mock to call the action listener passed to it when sending a response
        doAnswer(invocationOnMock -> {
            ActionListener<?> listener = invocationOnMock.getArgument(1);
            listener.onResponse(null);
            return null;
        }).when(httpChannel).sendResponse(any(HttpResponse.class), anyActionListener());

        executeRequest(Settings.EMPTY, "request-host");

        verify(tracer).setAttribute(argThat(id -> id.startsWith("rest-")), eq("http.status_code"), eq(200L));
        verify(tracer).stopTrace(argThat(id -> id.startsWith("rest-")));
    }

    private TestHttpResponse executeRequest(final Settings settings, final String host) {
        return executeRequest(settings, null, host);
    }

    private TestHttpResponse executeRequest(final Settings settings, final String originValue, final String host) {
        HttpRequest httpRequest = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
        if (originValue != null) {
            httpRequest.getHeaders().put(CorsHandler.ORIGIN, Collections.singletonList(originValue));
        }
        httpRequest.getHeaders().put(CorsHandler.HOST, Collections.singletonList(host));
        final RestRequest request = RestRequest.request(parserConfig(), httpRequest, httpChannel);

        HttpHandlingSettings httpHandlingSettings = HttpHandlingSettings.fromSettings(settings);
        RestChannel channel = new DefaultRestChannel(
            httpChannel,
            httpRequest,
            request,
            bigArrays,
            httpHandlingSettings,
            threadPool.getThreadContext(),
            new CorsHandler(CorsHandler.buildConfig(settings)),
            httpTracer,
            tracer
        );
        channel.sendResponse(testRestResponse());

        // get the response
        ArgumentCaptor<TestHttpResponse> responseCaptor = ArgumentCaptor.forClass(TestHttpResponse.class);
        verify(httpChannel, atLeastOnce()).sendResponse(responseCaptor.capture(), any());
        return responseCaptor.getValue();
    }

    private static RestResponse testRestResponse() {
        return new RestResponse(RestStatus.OK, "content");
    }
}
