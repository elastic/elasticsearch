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

package org.elasticsearch.http;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.PagedBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DefaultRestChannelTests extends ESTestCase {

    private ThreadPool threadPool;
    private MockBigArrays bigArrays;
    private HttpChannel httpChannel;

    @Before
    public void setup() {
        httpChannel = mock(HttpChannel.class);
        threadPool = new TestThreadPool("test");
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    @After
    public void shutdown() {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
    }

    public void testResponse() {
        final TestResponse response = executeRequest(Settings.EMPTY, "request-host");
        assertThat(response.content(), equalTo(new TestRestResponse().content()));
    }

    // TODO: Enable these Cors tests when the Cors logic lives in :server

//    public void testCorsEnabledWithoutAllowOrigins() {
//        // Set up an HTTP transport with only the CORS enabled setting
//        Settings settings = Settings.builder()
//            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
//            .build();
//        HttpResponse response = executeRequest(settings, "remote-host", "request-host");
//        // inspect response and validate
//        assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), nullValue());
//    }
//
//    public void testCorsEnabledWithAllowOrigins() {
//        final String originValue = "remote-host";
//        // create an HTTP transport with CORS enabled and allow origin configured
//        Settings settings = Settings.builder()
//            .put(SETTING_CORS_ENABLED.getKey(), true)
//            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), originValue)
//            .build();
//        HttpResponse response = executeRequest(settings, originValue, "request-host");
//        // inspect response and validate
//        assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
//        String allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
//        assertThat(allowedOrigins, is(originValue));
//    }
//
//    public void testCorsAllowOriginWithSameHost() {
//        String originValue = "remote-host";
//        String host = "remote-host";
//        // create an HTTP transport with CORS enabled
//        Settings settings = Settings.builder()
//            .put(SETTING_CORS_ENABLED.getKey(), true)
//            .build();
//        HttpResponse response = executeRequest(settings, originValue, host);
//        // inspect response and validate
//        assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
//        String allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
//        assertThat(allowedOrigins, is(originValue));
//
//        originValue = "http://" + originValue;
//        response = executeRequest(settings, originValue, host);
//        assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
//        allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
//        assertThat(allowedOrigins, is(originValue));
//
//        originValue = originValue + ":5555";
//        host = host + ":5555";
//        response = executeRequest(settings, originValue, host);
//        assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
//        allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
//        assertThat(allowedOrigins, is(originValue));
//
//        originValue = originValue.replace("http", "https");
//        response = executeRequest(settings, originValue, host);
//        assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
//        allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
//        assertThat(allowedOrigins, is(originValue));
//    }
//
//    public void testThatStringLiteralWorksOnMatch() {
//        final String originValue = "remote-host";
//        Settings settings = Settings.builder()
//            .put(SETTING_CORS_ENABLED.getKey(), true)
//            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), originValue)
//            .put(SETTING_CORS_ALLOW_METHODS.getKey(), "get, options, post")
//            .put(SETTING_CORS_ALLOW_CREDENTIALS.getKey(), true)
//            .build();
//        HttpResponse response = executeRequest(settings, originValue, "request-host");
//        // inspect response and validate
//        assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
//        String allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
//        assertThat(allowedOrigins, is(originValue));
//        assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_CREDENTIALS), equalTo("true"));
//    }
//
//    public void testThatAnyOriginWorks() {
//        final String originValue = NioCorsHandler.ANY_ORIGIN;
//        Settings settings = Settings.builder()
//            .put(SETTING_CORS_ENABLED.getKey(), true)
//            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), originValue)
//            .build();
//        HttpResponse response = executeRequest(settings, originValue, "request-host");
//        // inspect response and validate
//        assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
//        String allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
//        assertThat(allowedOrigins, is(originValue));
//        assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_CREDENTIALS), nullValue());
//    }

    public void testHeadersSet() {
        Settings settings = Settings.builder().build();
        final TestRequest httpRequest = new TestRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
        httpRequest.getHeaders().put(Task.X_OPAQUE_ID, Collections.singletonList("abc"));
        final RestRequest request = RestRequest.request(xContentRegistry(), httpRequest, httpChannel);
        HttpHandlingSettings handlingSettings = HttpHandlingSettings.fromSettings(settings);

        // send a response
        DefaultRestChannel channel = new DefaultRestChannel(httpChannel, httpRequest, request, bigArrays, handlingSettings,
            threadPool.getThreadContext(), null);
        TestRestResponse resp = new TestRestResponse();
        final String customHeader = "custom-header";
        final String customHeaderValue = "xyz";
        resp.addHeader(customHeader, customHeaderValue);
        channel.sendResponse(resp);

        // inspect what was written
        ArgumentCaptor<TestResponse> responseCaptor = ArgumentCaptor.forClass(TestResponse.class);
        verify(httpChannel).sendResponse(responseCaptor.capture(), any());
        TestResponse httpResponse = responseCaptor.getValue();
        Map<String, List<String>> headers = httpResponse.headers;
        assertNull(headers.get("non-existent-header"));
        assertEquals(customHeaderValue, headers.get(customHeader).get(0));
        assertEquals("abc", headers.get(Task.X_OPAQUE_ID).get(0));
        assertEquals(Integer.toString(resp.content().length()), headers.get(DefaultRestChannel.CONTENT_LENGTH).get(0));
        assertEquals(resp.contentType(), headers.get(DefaultRestChannel.CONTENT_TYPE).get(0));
    }

    public void testCookiesSet() {
        Settings settings = Settings.builder().put(HttpTransportSettings.SETTING_HTTP_RESET_COOKIES.getKey(), true).build();
        final TestRequest httpRequest = new TestRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
        httpRequest.getHeaders().put(Task.X_OPAQUE_ID, Collections.singletonList("abc"));
        final RestRequest request = RestRequest.request(xContentRegistry(), httpRequest, httpChannel);
        HttpHandlingSettings handlingSettings = HttpHandlingSettings.fromSettings(settings);

        // send a response
        DefaultRestChannel channel = new DefaultRestChannel(httpChannel, httpRequest, request, bigArrays, handlingSettings,
            threadPool.getThreadContext(), null);
        channel.sendResponse(new TestRestResponse());

        // inspect what was written
        ArgumentCaptor<TestResponse> responseCaptor = ArgumentCaptor.forClass(TestResponse.class);
        verify(httpChannel).sendResponse(responseCaptor.capture(), any());
        TestResponse nioResponse = responseCaptor.getValue();
        Map<String, List<String>> headers = nioResponse.headers;
        assertThat(headers.get(DefaultRestChannel.SET_COOKIE), hasItem("cookie"));
        assertThat(headers.get(DefaultRestChannel.SET_COOKIE), hasItem("cookie2"));
    }

    @SuppressWarnings("unchecked")
    public void testReleaseInListener() throws IOException {
        final Settings settings = Settings.builder().build();
        final TestRequest httpRequest = new TestRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
        final RestRequest request = RestRequest.request(xContentRegistry(), httpRequest, httpChannel);
        HttpHandlingSettings handlingSettings = HttpHandlingSettings.fromSettings(settings);

        DefaultRestChannel channel = new DefaultRestChannel(httpChannel, httpRequest, request, bigArrays, handlingSettings,
            threadPool.getThreadContext(), null);
        final BytesRestResponse response = new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR,
            JsonXContent.contentBuilder().startObject().endObject());
        assertThat(response.content(), not(instanceOf(Releasable.class)));

        // ensure we have reserved bytes
        if (randomBoolean()) {
            BytesStreamOutput out = channel.bytesOutput();
            assertThat(out, instanceOf(ReleasableBytesStreamOutput.class));
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
            httpRequest = new TestRequest(() -> {
                throw new IllegalArgumentException("Can't parse HTTP version");
            }, RestRequest.Method.GET, "/");
        } else if (randomBoolean()) {
            httpRequest = new TestRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
            if (close) {
                httpRequest.getHeaders().put(DefaultRestChannel.CONNECTION, Collections.singletonList(DefaultRestChannel.CLOSE));
            }
        } else {
            httpRequest = new TestRequest(HttpRequest.HttpVersion.HTTP_1_0, RestRequest.Method.GET, "/");
            if (!close) {
                httpRequest.getHeaders().put(DefaultRestChannel.CONNECTION, Collections.singletonList(DefaultRestChannel.KEEP_ALIVE));
            }
        }
        final RestRequest request = RestRequest.request(xContentRegistry(), httpRequest, httpChannel);

        HttpHandlingSettings handlingSettings = HttpHandlingSettings.fromSettings(settings);

        DefaultRestChannel channel = new DefaultRestChannel(httpChannel, httpRequest, request, bigArrays, handlingSettings,
            threadPool.getThreadContext(), null);
        channel.sendResponse(new TestRestResponse());
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
        final RestRequest request = RestRequest.request(xContentRegistry(), new TestRequest(httpVersion, null, "/") {
            @Override
            public RestRequest.Method method() {
                throw new IllegalArgumentException("test");
            }
        }, httpChannel);
        request.getHttpRequest().getHeaders().put(DefaultRestChannel.CONNECTION, Collections.singletonList(httpConnectionHeaderValue));

        DefaultRestChannel channel = new DefaultRestChannel(httpChannel, request.getHttpRequest(), request, bigArrays,
            HttpHandlingSettings.fromSettings(Settings.EMPTY), threadPool.getThreadContext(), null);

        // ESTestCase#after will invoke ensureAllArraysAreReleased which will fail if the response content was not released
        final BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        final ByteArray byteArray = bigArrays.newByteArray(0, false);
        final BytesReference content = new ReleasableBytesReference(new PagedBytesReference(byteArray, 0) , byteArray);
        channel.sendResponse(new TestRestResponse(RestStatus.METHOD_NOT_ALLOWED, content));

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

    public void testCloseOnException() {
        final boolean close = randomBoolean();
        final HttpRequest.HttpVersion httpVersion = close ? HttpRequest.HttpVersion.HTTP_1_0 : HttpRequest.HttpVersion.HTTP_1_1;
        final String httpConnectionHeaderValue = close ? DefaultRestChannel.CLOSE : DefaultRestChannel.KEEP_ALIVE;
        final RestRequest request = RestRequest.request(xContentRegistry(), new TestRequest(httpVersion, null, "/") {
            @Override
            public HttpResponse createResponse(RestStatus status, BytesReference content) {
                throw new IllegalArgumentException("test");
            }
        }, httpChannel);
        request.getHttpRequest().getHeaders().put(DefaultRestChannel.CONNECTION, Collections.singletonList(httpConnectionHeaderValue));

        DefaultRestChannel channel = new DefaultRestChannel(httpChannel, request.getHttpRequest(), request, bigArrays,
            HttpHandlingSettings.fromSettings(Settings.EMPTY), threadPool.getThreadContext(), null);

        // ESTestCase#after will invoke ensureAllArraysAreReleased which will fail if the response content was not released
        final BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        final ByteArray byteArray = bigArrays.newByteArray(0, false);
        final BytesReference content = new ReleasableBytesReference(new PagedBytesReference(byteArray, 0) , byteArray);

        expectThrows(IllegalArgumentException.class, () -> channel.sendResponse(new TestRestResponse(RestStatus.OK, content)));

        if (close) {
            verify(httpChannel, times(1)).close();
        } else {
            verify(httpChannel, times(0)).close();
        }
    }

    private TestResponse executeRequest(final Settings settings, final String host) {
        return executeRequest(settings, null, host);
    }

    private TestResponse executeRequest(final Settings settings, final String originValue, final String host) {
        HttpRequest httpRequest = new TestRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
        // TODO: These exist for the Cors tests
//        if (originValue != null) {
//            httpRequest.headers().add(HttpHeaderNames.ORIGIN, originValue);
//        }
//        httpRequest.headers().add(HttpHeaderNames.HOST, host);
        final RestRequest request = RestRequest.request(xContentRegistry(), httpRequest, httpChannel);

        HttpHandlingSettings httpHandlingSettings = HttpHandlingSettings.fromSettings(settings);
        RestChannel channel = new DefaultRestChannel(httpChannel, httpRequest, request, bigArrays, httpHandlingSettings,
            threadPool.getThreadContext(), null);
        channel.sendResponse(new TestRestResponse());

        // get the response
        ArgumentCaptor<TestResponse> responseCaptor = ArgumentCaptor.forClass(TestResponse.class);
        verify(httpChannel, atLeastOnce()).sendResponse(responseCaptor.capture(), any());
        return responseCaptor.getValue();
    }

    private static class TestRequest implements HttpRequest {

        private final Supplier<HttpVersion> version;
        private final RestRequest.Method method;
        private final String uri;
        private HashMap<String, List<String>> headers = new HashMap<>();

        private TestRequest(Supplier<HttpVersion> versionSupplier, RestRequest.Method method, String uri) {
            this.version = versionSupplier;
            this.method = method;
            this.uri = uri;
        }

        private TestRequest(HttpVersion version, RestRequest.Method method, String uri) {
            this(() -> version, method, uri);
        }

        @Override
        public RestRequest.Method method() {
            return method;
        }

        @Override
        public String uri() {
            return uri;
        }

        @Override
        public BytesReference content() {
            return BytesArray.EMPTY;
        }

        @Override
        public Map<String, List<String>> getHeaders() {
            return headers;
        }

        @Override
        public List<String> strictCookies() {
            return Arrays.asList("cookie", "cookie2");
        }

        @Override
        public HttpVersion protocolVersion() {
            return version.get();
        }

        @Override
        public HttpRequest removeHeader(String header) {
            throw new UnsupportedOperationException("Do not support removing header on test request.");
        }

        @Override
        public HttpResponse createResponse(RestStatus status, BytesReference content) {
            return new TestResponse(status, content);
        }

        @Override
        public void release() {
        }

        @Override
        public HttpRequest releaseAndCopy() {
            return this;
        }

        @Override
        public Exception getInboundException() {
            return null;
        }
    }

    private static class TestResponse implements HttpResponse {

        private final RestStatus status;
        private final BytesReference content;
        private final Map<String, List<String>> headers = new HashMap<>();

        TestResponse(RestStatus status, BytesReference content) {
            this.status = status;
            this.content = content;
        }

        public String contentType() {
            return "text";
        }

        public BytesReference content() {
            return content;
        }

        public RestStatus status() {
            return status;
        }

        @Override
        public void addHeader(String name, String value) {
            if (headers.containsKey(name) == false) {
                ArrayList<String> values = new ArrayList<>();
                values.add(value);
                headers.put(name, values);
            } else {
                headers.get(name).add(value);
            }
        }

        @Override
        public boolean containsHeader(String name) {
            return headers.containsKey(name);
        }
    }

    private static class TestRestResponse extends RestResponse {

        private final RestStatus status;
        private final BytesReference content;

        TestRestResponse(final RestStatus status, final BytesReference content) {
            this.status = Objects.requireNonNull(status);
            this.content = Objects.requireNonNull(content);
        }

        TestRestResponse() {
            this(RestStatus.OK, new BytesArray("content".getBytes(StandardCharsets.UTF_8)));
        }

        public String contentType() {
            return "text";
        }

        public BytesReference content() {
            return content;
        }

        public RestStatus status() {
            return status;
        }
    }
}
