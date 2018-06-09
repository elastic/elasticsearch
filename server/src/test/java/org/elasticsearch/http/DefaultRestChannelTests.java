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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.NewRestRequest;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
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
    private LLHttpChannel httpChannel;

    @Before
    public void setup() {
        httpChannel = mock(LLHttpChannel.class);
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
        final RestResponse response = executeRequest(Settings.EMPTY, "request-host");
        assertThat(response.content(), equalTo(new TestResponse().content()));
    }

    // TODO: Enable these Cors tests when the Cors logic lives in :server

//    public void testCorsEnabledWithoutAllowOrigins() {
//        // Set up a HTTP transport with only the CORS enabled setting
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
//        // create a http transport with CORS enabled and allow origin configured
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
//        // create a http transport with CORS enabled
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
        final TestRequest httpRequest = new TestRequest(LLHttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
        httpRequest.getHeaders().put(DefaultRestChannel.X_OPAQUE_ID, Collections.singletonList("abc"));
        final RestRequest request = NewRestRequest.request(xContentRegistry(), httpRequest);
        HttpHandlingSettings handlingSettings = HttpHandlingSettings.fromSettings(settings);

        // send a response
        DefaultRestChannel channel = new DefaultRestChannel(httpChannel, request, bigArrays, handlingSettings,
            threadPool.getThreadContext());
        TestResponse resp = new TestResponse();
        final String customHeader = "custom-header";
        final String customHeaderValue = "xyz";
        resp.addHeader(customHeader, customHeaderValue);
        channel.sendResponse(resp);

        // inspect what was written
        ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
        verify(httpChannel).sendResponse(responseCaptor.capture(), any());
        RestResponse nioResponse = responseCaptor.getValue();
        Map<String, List<String>> headers = nioResponse.getHeaders();
        assertNull(headers.get("non-existent-header"));
        assertEquals(customHeaderValue, headers.get(customHeader).get(0));
        assertEquals("abc", headers.get(DefaultRestChannel.X_OPAQUE_ID).get(0));
        assertEquals(Integer.toString(resp.content().length()), headers.get("content-length").get(0));
        assertEquals(resp.contentType(), headers.get("content-type").get(0));
    }

    @SuppressWarnings("unchecked")
    public void testReleaseInListener() throws IOException {
        final Settings settings = Settings.builder().build();
        final TestRequest httpRequest = new TestRequest(LLHttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
        final RestRequest request = NewRestRequest.request(xContentRegistry(), httpRequest);
        HttpHandlingSettings handlingSettings = HttpHandlingSettings.fromSettings(settings);

        DefaultRestChannel channel = new DefaultRestChannel(httpChannel, request, bigArrays, handlingSettings,
            threadPool.getThreadContext());
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
        final LLHttpRequest httpRequest;
        final boolean close = randomBoolean();
        if (randomBoolean()) {
            httpRequest = new TestRequest(LLHttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
            if (close) {
                httpRequest.getHeaders().put(DefaultRestChannel.CONNECTION, Collections.singletonList(DefaultRestChannel.CLOSE));
            }
        } else {
            httpRequest = new TestRequest(LLHttpRequest.HttpVersion.HTTP_1_0, RestRequest.Method.GET, "/");
            if (!close) {
                httpRequest.getHeaders().put(DefaultRestChannel.CONNECTION, Collections.singletonList(DefaultRestChannel.KEEP_ALIVE));
            }
        }
        final RestRequest request = NewRestRequest.request(xContentRegistry(), httpRequest);

        HttpHandlingSettings handlingSettings = HttpHandlingSettings.fromSettings(settings);

        DefaultRestChannel channel = new DefaultRestChannel(httpChannel, request, bigArrays, handlingSettings,
            threadPool.getThreadContext());
        final TestResponse resp = new TestResponse();
        channel.sendResponse(resp);
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

    private RestResponse executeRequest(final Settings settings, final String host) {
        return executeRequest(settings, null, host);
    }

    private RestResponse executeRequest(final Settings settings, final String originValue, final String host) {
        LLHttpRequest httpRequest = new TestRequest(LLHttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
        // TODO: These exist for the Cors tests
//        if (originValue != null) {
//            httpRequest.headers().add(HttpHeaderNames.ORIGIN, originValue);
//        }
//        httpRequest.headers().add(HttpHeaderNames.HOST, host);
        final RestRequest request = NewRestRequest.request(xContentRegistry(), httpRequest);

        HttpHandlingSettings httpHandlingSettings = HttpHandlingSettings.fromSettings(settings);
        RestChannel channel = new DefaultRestChannel(httpChannel, request, bigArrays, httpHandlingSettings, threadPool.getThreadContext());
        channel.sendResponse(new TestResponse());

        // get the response
        ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
        verify(httpChannel, atLeastOnce()).sendResponse(responseCaptor.capture(), any());
        return responseCaptor.getValue();
    }

    private static class TestRequest implements LLHttpRequest {

        private final HttpVersion version;
        private final RestRequest.Method method;
        private final String uri;
        private HashMap<String, List<String>> headers = new HashMap<>();

        public TestRequest(HttpVersion version, RestRequest.Method method, String uri) {

            this.version = version;
            this.method = method;
            this.uri = uri;
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
            return Collections.emptyList();
        }

        @Override
        public HttpVersion protocolVersion() {
            return version;
        }

        @Override
        public LLHttpRequest removeHeader(String header) {
            throw new UnsupportedOperationException("Do not support removing header on test request.");
        }
    }

    private static class TestResponse extends RestResponse {

        private final BytesReference reference;

        TestResponse() {
            reference = new BytesArray("content".getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public String contentType() {
            return "text";
        }

        @Override
        public BytesReference content() {
            return reference;
        }

        @Override
        public RestStatus status() {
            return RestStatus.OK;
        }

    }
}
