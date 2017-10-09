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

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasablePagedBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.http.NullDispatcher;
import org.elasticsearch.http.netty4.cors.Netty4CorsHandler;
import org.elasticsearch.http.netty4.pipelining.HttpPipelinedRequest;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty4.Netty4Utils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

public class Netty4HttpChannelTests extends ESTestCase {

    private NetworkService networkService;
    private ThreadPool threadPool;
    private MockBigArrays bigArrays;

    @Before
    public void setup() throws Exception {
        networkService = new NetworkService(Collections.emptyList());
        threadPool = new TestThreadPool("test");
        bigArrays = new MockBigArrays(Settings.EMPTY, new NoneCircuitBreakerService());
    }

    @After
    public void shutdown() throws Exception {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
    }

    public void testResponse() {
        final FullHttpResponse response = executeRequest(Settings.EMPTY, "request-host");
        assertThat(response.content(), equalTo(Netty4Utils.toByteBuf(new TestResponse().content())));
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
        final String originValue = Netty4CorsHandler.ANY_ORIGIN;
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
        try (Netty4HttpServerTransport httpServerTransport =
                     new Netty4HttpServerTransport(settings, networkService, bigArrays, threadPool, xContentRegistry(),
                         new NullDispatcher())) {
            httpServerTransport.start();
            final FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
            httpRequest.headers().add(HttpHeaderNames.ORIGIN, "remote");
            final WriteCapturingChannel writeCapturingChannel = new WriteCapturingChannel();
            Netty4HttpRequest request = new Netty4HttpRequest(xContentRegistry(), httpRequest, writeCapturingChannel);

            // send a response
            Netty4HttpChannel channel =
                    new Netty4HttpChannel(httpServerTransport, request, null, randomBoolean(), threadPool.getThreadContext());
            TestResponse resp = new TestResponse();
            final String customHeader = "custom-header";
            final String customHeaderValue = "xyz";
            resp.addHeader(customHeader, customHeaderValue);
            channel.sendResponse(resp);

            // inspect what was written
            List<Object> writtenObjects = writeCapturingChannel.getWrittenObjects();
            assertThat(writtenObjects.size(), is(1));
            HttpResponse response = (HttpResponse) writtenObjects.get(0);
            assertThat(response.headers().get("non-existent-header"), nullValue());
            assertThat(response.headers().get(customHeader), equalTo(customHeaderValue));
            assertThat(response.headers().get(HttpHeaderNames.CONTENT_LENGTH), equalTo(Integer.toString(resp.content().length())));
            assertThat(response.headers().get(HttpHeaderNames.CONTENT_TYPE), equalTo(resp.contentType()));
        }
    }

    public void testReleaseOnSendToClosedChannel() {
        final Settings settings = Settings.builder().build();
        final NamedXContentRegistry registry = xContentRegistry();
        try (Netty4HttpServerTransport httpServerTransport =
                     new Netty4HttpServerTransport(settings, networkService, bigArrays, threadPool, registry, new NullDispatcher())) {
            final FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
            final EmbeddedChannel embeddedChannel = new EmbeddedChannel();
            final Netty4HttpRequest request = new Netty4HttpRequest(registry, httpRequest, embeddedChannel);
            final HttpPipelinedRequest pipelinedRequest = randomBoolean() ? new HttpPipelinedRequest(request.request(), 1) : null;
            final Netty4HttpChannel channel =
                    new Netty4HttpChannel(httpServerTransport, request, pipelinedRequest, randomBoolean(), threadPool.getThreadContext());
            final TestResponse response = new TestResponse(bigArrays);
            assertThat(response.content(), instanceOf(Releasable.class));
            embeddedChannel.close();
            channel.sendResponse(response);
            // ESTestCase#after will invoke ensureAllArraysAreReleased which will fail if the response content was not released
        }
    }

    public void testReleaseOnSendToChannelAfterException() throws IOException {
        final Settings settings = Settings.builder().build();
        final NamedXContentRegistry registry = xContentRegistry();
        try (Netty4HttpServerTransport httpServerTransport =
                 new Netty4HttpServerTransport(settings, networkService, bigArrays, threadPool, registry, new NullDispatcher())) {
            final FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
            final EmbeddedChannel embeddedChannel = new EmbeddedChannel();
            final Netty4HttpRequest request = new Netty4HttpRequest(registry, httpRequest, embeddedChannel);
            final HttpPipelinedRequest pipelinedRequest = randomBoolean() ? new HttpPipelinedRequest(request.request(), 1) : null;
            final Netty4HttpChannel channel =
                new Netty4HttpChannel(httpServerTransport, request, pipelinedRequest, randomBoolean(), threadPool.getThreadContext());
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
            // ESTestCase#after will invoke ensureAllArraysAreReleased which will fail if the response content was not released
        }
    }

    public void testConnectionClose() throws Exception {
        final Settings settings = Settings.builder().build();
        try (Netty4HttpServerTransport httpServerTransport =
                 new Netty4HttpServerTransport(settings, networkService, bigArrays, threadPool, xContentRegistry(), new NullDispatcher())) {
            httpServerTransport.start();
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
            final EmbeddedChannel embeddedChannel = new EmbeddedChannel();
            final Netty4HttpRequest request = new Netty4HttpRequest(xContentRegistry(), httpRequest, embeddedChannel);

            // send a response, the channel close status should match
            assertTrue(embeddedChannel.isOpen());
            final Netty4HttpChannel channel =
                new Netty4HttpChannel(httpServerTransport, request, null, randomBoolean(), threadPool.getThreadContext());
            final TestResponse resp = new TestResponse();
            channel.sendResponse(resp);
            assertThat(embeddedChannel.isOpen(), equalTo(!close));
        }
    }

    private FullHttpResponse executeRequest(final Settings settings, final String host) {
        return executeRequest(settings, null, host);
    }

    private FullHttpResponse executeRequest(final Settings settings, final String originValue, final String host) {
        // construct request and send it over the transport layer
        try (Netty4HttpServerTransport httpServerTransport =
                     new Netty4HttpServerTransport(settings, networkService, bigArrays, threadPool, xContentRegistry(),
                             new NullDispatcher())) {
            httpServerTransport.start();
            final FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
            if (originValue != null) {
                httpRequest.headers().add(HttpHeaderNames.ORIGIN, originValue);
            }
            httpRequest.headers().add(HttpHeaderNames.HOST, host);
            final WriteCapturingChannel writeCapturingChannel = new WriteCapturingChannel();
            final Netty4HttpRequest request = new Netty4HttpRequest(xContentRegistry(), httpRequest, writeCapturingChannel);

            Netty4HttpChannel channel =
                    new Netty4HttpChannel(httpServerTransport, request, null, randomBoolean(), threadPool.getThreadContext());
            channel.sendResponse(new TestResponse());

            // get the response
            List<Object> writtenObjects = writeCapturingChannel.getWrittenObjects();
            assertThat(writtenObjects.size(), is(1));
            return (FullHttpResponse) writtenObjects.get(0);
        }
    }

    private static class WriteCapturingChannel implements Channel {

        private List<Object> writtenObjects = new ArrayList<>();

        @Override
        public ChannelId id() {
            return null;
        }

        @Override
        public EventLoop eventLoop() {
            return null;
        }

        @Override
        public Channel parent() {
            return null;
        }

        @Override
        public ChannelConfig config() {
            return null;
        }

        @Override
        public boolean isOpen() {
            return false;
        }

        @Override
        public boolean isRegistered() {
            return false;
        }

        @Override
        public boolean isActive() {
            return false;
        }

        @Override
        public ChannelMetadata metadata() {
            return null;
        }

        @Override
        public SocketAddress localAddress() {
            return null;
        }

        @Override
        public SocketAddress remoteAddress() {
            return null;
        }

        @Override
        public ChannelFuture closeFuture() {
            return null;
        }

        @Override
        public boolean isWritable() {
            return false;
        }

        @Override
        public long bytesBeforeUnwritable() {
            return 0;
        }

        @Override
        public long bytesBeforeWritable() {
            return 0;
        }

        @Override
        public Unsafe unsafe() {
            return null;
        }

        @Override
        public ChannelPipeline pipeline() {
            return null;
        }

        @Override
        public ByteBufAllocator alloc() {
            return null;
        }

        @Override
        public Channel read() {
            return null;
        }

        @Override
        public Channel flush() {
            return null;
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
            return null;
        }

        @Override
        public ChannelFuture disconnect() {
            return null;
        }

        @Override
        public ChannelFuture close() {
            return null;
        }

        @Override
        public ChannelFuture deregister() {
            return null;
        }

        @Override
        public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture disconnect(ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture close(ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture deregister(ChannelPromise promise) {
            return null;
        }

        @Override
        public ChannelFuture write(Object msg) {
            writtenObjects.add(msg);
            return null;
        }

        @Override
        public ChannelFuture write(Object msg, ChannelPromise promise) {
            writtenObjects.add(msg);
            return null;
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
            writtenObjects.add(msg);
            return null;
        }

        @Override
        public ChannelFuture writeAndFlush(Object msg) {
            writtenObjects.add(msg);
            return null;
        }

        @Override
        public ChannelPromise newPromise() {
            return null;
        }

        @Override
        public ChannelProgressivePromise newProgressivePromise() {
            return null;
        }

        @Override
        public ChannelFuture newSucceededFuture() {
            return null;
        }

        @Override
        public ChannelFuture newFailedFuture(Throwable cause) {
            return null;
        }

        @Override
        public ChannelPromise voidPromise() {
            return null;
        }

        @Override
        public <T> Attribute<T> attr(AttributeKey<T> key) {
            return null;
        }

        @Override
        public <T> boolean hasAttr(AttributeKey<T> key) {
            return false;
        }

        @Override
        public int compareTo(Channel o) {
            return 0;
        }

        List<Object> getWrittenObjects() {
            return writtenObjects;
        }

    }

    private static class TestResponse extends RestResponse {

        private final BytesReference reference;

        TestResponse() {
            reference = Netty4Utils.toBytesReference(Unpooled.copiedBuffer("content", StandardCharsets.UTF_8));
        }

        TestResponse(final BigArrays bigArrays) {
            final byte[] bytes;
            try {
                bytes = "content".getBytes("UTF-8");
            } catch (final UnsupportedEncodingException e) {
                throw new AssertionError(e);
            }
            final ByteArray bigArray = bigArrays.newByteArray(bytes.length);
            bigArray.set(0, bytes, 0, bytes.length);
            reference = new ReleasablePagedBytesReference(bigArrays, bigArray, bytes.length, Releasables.releaseOnce(bigArray));
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
