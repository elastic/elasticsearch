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

package org.elasticsearch.http.nio;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.http.HttpHandlingSettings;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.http.nio.cors.NioCorsConfig;
import org.elasticsearch.http.nio.cors.NioCorsHandler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.rest.BytesRestResponse;
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
import java.util.function.BiConsumer;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_METHODS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NioHttpChannelTests extends ESTestCase {

    private ThreadPool threadPool;
    private MockBigArrays bigArrays;
    private NioSocketChannel nioChannel;
    private SocketChannelContext channelContext;

    @Before
    public void setup() throws Exception {
        nioChannel = mock(NioSocketChannel.class);
        channelContext = mock(SocketChannelContext.class);
        when(nioChannel.getContext()).thenReturn(channelContext);
        threadPool = new TestThreadPool("test");
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    @After
    public void shutdown() throws Exception {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
    }

    public void testResponse() {
        final FullHttpResponse response = executeRequest(Settings.EMPTY, "request-host");
        assertThat(response.content(), equalTo(ByteBufUtils.toByteBuf(new TestResponse().content())));
    }

    public void testCorsEnabledWithoutAllowOrigins() {
        // Set up a HTTP transport with only the CORS enabled setting
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .build();
        HttpResponse response = executeRequest(settings, "remote-host", "request-host");
        // inspect response and validate
        assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), nullValue());
    }

    public void testCorsEnabledWithAllowOrigins() {
        final String originValue = "remote-host";
        // create a http transport with CORS enabled and allow origin configured
        Settings settings = Settings.builder()
            .put(SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), originValue)
            .build();
        HttpResponse response = executeRequest(settings, originValue, "request-host");
        // inspect response and validate
        assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
        String allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
        assertThat(allowedOrigins, is(originValue));
    }

    public void testCorsAllowOriginWithSameHost() {
        String originValue = "remote-host";
        String host = "remote-host";
        // create a http transport with CORS enabled
        Settings settings = Settings.builder()
            .put(SETTING_CORS_ENABLED.getKey(), true)
            .build();
        HttpResponse response = executeRequest(settings, originValue, host);
        // inspect response and validate
        assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
        String allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
        assertThat(allowedOrigins, is(originValue));

        originValue = "http://" + originValue;
        response = executeRequest(settings, originValue, host);
        assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
        allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
        assertThat(allowedOrigins, is(originValue));

        originValue = originValue + ":5555";
        host = host + ":5555";
        response = executeRequest(settings, originValue, host);
        assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
        allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
        assertThat(allowedOrigins, is(originValue));

        originValue = originValue.replace("http", "https");
        response = executeRequest(settings, originValue, host);
        assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
        allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
        assertThat(allowedOrigins, is(originValue));
    }

    public void testThatStringLiteralWorksOnMatch() {
        final String originValue = "remote-host";
        Settings settings = Settings.builder()
            .put(SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), originValue)
            .put(SETTING_CORS_ALLOW_METHODS.getKey(), "get, options, post")
            .put(SETTING_CORS_ALLOW_CREDENTIALS.getKey(), true)
            .build();
        HttpResponse response = executeRequest(settings, originValue, "request-host");
        // inspect response and validate
        assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
        String allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
        assertThat(allowedOrigins, is(originValue));
        assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_CREDENTIALS), equalTo("true"));
    }

    public void testThatAnyOriginWorks() {
        final String originValue = NioCorsHandler.ANY_ORIGIN;
        Settings settings = Settings.builder()
            .put(SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), originValue)
            .build();
        HttpResponse response = executeRequest(settings, originValue, "request-host");
        // inspect response and validate
        assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
        String allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
        assertThat(allowedOrigins, is(originValue));
        assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_CREDENTIALS), nullValue());
    }

    public void testHeadersSet() {
        Settings settings = Settings.builder().build();
        final FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        httpRequest.headers().add(HttpHeaderNames.ORIGIN, "remote");
        final NioHttpRequest request = new NioHttpRequest(xContentRegistry(), httpRequest);
        HttpHandlingSettings handlingSettings = HttpHandlingSettings.fromSettings(settings);
        NioCorsConfig corsConfig = NioHttpServerTransport.buildCorsConfig(settings);

        // send a response
        NioHttpChannel channel = new NioHttpChannel(nioChannel, bigArrays, request, 1, handlingSettings, corsConfig,
            threadPool.getThreadContext());
        TestResponse resp = new TestResponse();
        final String customHeader = "custom-header";
        final String customHeaderValue = "xyz";
        resp.addHeader(customHeader, customHeaderValue);
        channel.sendResponse(resp);

        // inspect what was written
        ArgumentCaptor<Object> responseCaptor = ArgumentCaptor.forClass(Object.class);
        verify(channelContext).sendMessage(responseCaptor.capture(), any());
        Object nioResponse = responseCaptor.getValue();
        HttpResponse response = ((NioHttpResponse) nioResponse).getResponse();
        assertThat(response.headers().get("non-existent-header"), nullValue());
        assertThat(response.headers().get(customHeader), equalTo(customHeaderValue));
        assertThat(response.headers().get(HttpHeaderNames.CONTENT_LENGTH), equalTo(Integer.toString(resp.content().length())));
        assertThat(response.headers().get(HttpHeaderNames.CONTENT_TYPE), equalTo(resp.contentType()));
    }

    @SuppressWarnings("unchecked")
    public void testReleaseInListener() throws IOException {
        final Settings settings = Settings.builder().build();
        final NamedXContentRegistry registry = xContentRegistry();
        final FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        final NioHttpRequest request = new NioHttpRequest(registry, httpRequest);
        HttpHandlingSettings handlingSettings = HttpHandlingSettings.fromSettings(settings);
        NioCorsConfig corsConfig = NioHttpServerTransport.buildCorsConfig(settings);

        NioHttpChannel channel = new NioHttpChannel(nioChannel, bigArrays, request, 1, handlingSettings,
            corsConfig, threadPool.getThreadContext());
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
        Class<BiConsumer<Void, Exception>> listenerClass = (Class<BiConsumer<Void, Exception>>) (Class) BiConsumer.class;
        ArgumentCaptor<BiConsumer<Void, Exception>> listenerCaptor = ArgumentCaptor.forClass(listenerClass);
        verify(channelContext).sendMessage(any(), listenerCaptor.capture());
        BiConsumer<Void, Exception> listener = listenerCaptor.getValue();
        if (randomBoolean()) {
            listener.accept(null, null);
        } else {
            listener.accept(null, new ClosedChannelException());
        }
        // ESTestCase#after will invoke ensureAllArraysAreReleased which will fail if the response content was not released
    }


    @SuppressWarnings("unchecked")
    public void testConnectionClose() throws Exception {
        final Settings settings = Settings.builder().build();
        final FullHttpRequest httpRequest;
        final boolean close = randomBoolean();
        if (randomBoolean()) {
            httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
            if (close) {
                httpRequest.headers().add(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
            }
        } else {
            httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "/");
            if (!close) {
                httpRequest.headers().add(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }
        }
        final NioHttpRequest request = new NioHttpRequest(xContentRegistry(), httpRequest);

        HttpHandlingSettings handlingSettings = HttpHandlingSettings.fromSettings(settings);
        NioCorsConfig corsConfig = NioHttpServerTransport.buildCorsConfig(settings);

        NioHttpChannel channel = new NioHttpChannel(nioChannel, bigArrays, request, 1, handlingSettings,
            corsConfig, threadPool.getThreadContext());
        final TestResponse resp = new TestResponse();
        channel.sendResponse(resp);
        Class<BiConsumer<Void, Exception>> listenerClass = (Class<BiConsumer<Void, Exception>>) (Class) BiConsumer.class;
        ArgumentCaptor<BiConsumer<Void, Exception>> listenerCaptor = ArgumentCaptor.forClass(listenerClass);
        verify(channelContext).sendMessage(any(), listenerCaptor.capture());
        BiConsumer<Void, Exception> listener = listenerCaptor.getValue();
        listener.accept(null, null);
        if (close) {
            verify(nioChannel, times(1)).close();
        } else {
            verify(nioChannel, times(0)).close();
        }
    }

    private FullHttpResponse executeRequest(final Settings settings, final String host) {
        return executeRequest(settings, null, host);
    }

    private FullHttpResponse executeRequest(final Settings settings, final String originValue, final String host) {
        // construct request and send it over the transport layer
        final FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        if (originValue != null) {
            httpRequest.headers().add(HttpHeaderNames.ORIGIN, originValue);
        }
        httpRequest.headers().add(HttpHeaderNames.HOST, host);
        final NioHttpRequest request = new NioHttpRequest(xContentRegistry(), httpRequest);

        HttpHandlingSettings httpHandlingSettings = HttpHandlingSettings.fromSettings(settings);
        NioCorsConfig corsConfig = NioHttpServerTransport.buildCorsConfig(settings);
        NioHttpChannel channel = new NioHttpChannel(nioChannel, bigArrays, request, 1, httpHandlingSettings, corsConfig,
            threadPool.getThreadContext());
        channel.sendResponse(new TestResponse());

        // get the response
        ArgumentCaptor<Object> responseCaptor = ArgumentCaptor.forClass(Object.class);
        verify(channelContext, atLeastOnce()).sendMessage(responseCaptor.capture(), any());
        return ((NioHttpResponse) responseCaptor.getValue()).getResponse();
    }

    private static class TestResponse extends RestResponse {

        private final BytesReference reference;

        TestResponse() {
            reference = ByteBufUtils.toBytesReference(Unpooled.copiedBuffer("content", StandardCharsets.UTF_8));
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
