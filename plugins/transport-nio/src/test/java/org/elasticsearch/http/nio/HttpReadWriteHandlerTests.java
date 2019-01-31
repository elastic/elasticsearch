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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.HttpHandlingSettings;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.http.nio.cors.NioCorsConfig;
import org.elasticsearch.http.nio.cors.NioCorsConfigBuilder;
import org.elasticsearch.http.nio.cors.NioCorsHandler;
import org.elasticsearch.nio.FlushOperation;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.BiConsumer;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_METHODS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION_LEVEL;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_DETAILED_ERRORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CHUNK_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_RESET_COOKIES;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_PIPELINING_MAX_EVENTS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class HttpReadWriteHandlerTests extends ESTestCase {

    private HttpReadWriteHandler handler;
    private NioHttpChannel nioHttpChannel;
    private NioHttpServerTransport transport;

    private final RequestEncoder requestEncoder = new RequestEncoder();
    private final ResponseDecoder responseDecoder = new ResponseDecoder();

    @Before
    public void setMocks() {
        transport = mock(NioHttpServerTransport.class);
        Settings settings = Settings.EMPTY;
        ByteSizeValue maxChunkSize = SETTING_HTTP_MAX_CHUNK_SIZE.getDefault(settings);
        ByteSizeValue maxHeaderSize = SETTING_HTTP_MAX_HEADER_SIZE.getDefault(settings);
        ByteSizeValue maxInitialLineLength = SETTING_HTTP_MAX_INITIAL_LINE_LENGTH.getDefault(settings);
        HttpHandlingSettings httpHandlingSettings = new HttpHandlingSettings(1024,
            Math.toIntExact(maxChunkSize.getBytes()),
            Math.toIntExact(maxHeaderSize.getBytes()),
            Math.toIntExact(maxInitialLineLength.getBytes()),
            SETTING_HTTP_RESET_COOKIES.getDefault(settings),
            SETTING_HTTP_COMPRESSION.getDefault(settings),
            SETTING_HTTP_COMPRESSION_LEVEL.getDefault(settings),
            SETTING_HTTP_DETAILED_ERRORS_ENABLED.getDefault(settings),
            SETTING_PIPELINING_MAX_EVENTS.getDefault(settings),
            SETTING_CORS_ENABLED.getDefault(settings));
        nioHttpChannel = mock(NioHttpChannel.class);
        handler = new HttpReadWriteHandler(nioHttpChannel, transport, httpHandlingSettings, NioCorsConfigBuilder.forAnyOrigin().build());
    }

    public void testSuccessfulDecodeHttpRequest() throws IOException {
        String uri = "localhost:9090/" + randomAlphaOfLength(8);
        io.netty.handler.codec.http.HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);

        ByteBuf buf = requestEncoder.encode(httpRequest);
        int slicePoint = randomInt(buf.writerIndex() - 1);
        ByteBuf slicedBuf = buf.retainedSlice(0, slicePoint);
        ByteBuf slicedBuf2 = buf.retainedSlice(slicePoint, buf.writerIndex());
        try {
            handler.consumeReads(toChannelBuffer(slicedBuf));

            verify(transport, times(0)).incomingRequest(any(HttpRequest.class), any(NioHttpChannel.class));

            handler.consumeReads(toChannelBuffer(slicedBuf2));

            ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
            verify(transport).incomingRequest(requestCaptor.capture(), any(NioHttpChannel.class));

            HttpRequest nioHttpRequest = requestCaptor.getValue();
            assertEquals(HttpRequest.HttpVersion.HTTP_1_1, nioHttpRequest.protocolVersion());
            assertEquals(RestRequest.Method.GET, nioHttpRequest.method());
        } finally {
            handler.close();
            buf.release();
            slicedBuf.release();
            slicedBuf2.release();
        }
    }

    public void testDecodeHttpRequestError() throws IOException {
        String uri = "localhost:9090/" + randomAlphaOfLength(8);
        io.netty.handler.codec.http.HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);

        ByteBuf buf = requestEncoder.encode(httpRequest);
        try {
            buf.setByte(0, ' ');
            buf.setByte(1, ' ');
            buf.setByte(2, ' ');

            handler.consumeReads(toChannelBuffer(buf));

            ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
            verify(transport).incomingRequestError(any(HttpRequest.class), any(NioHttpChannel.class), exceptionCaptor.capture());

            assertTrue(exceptionCaptor.getValue() instanceof IllegalArgumentException);
        } finally {
            buf.release();
        }
    }

    public void testDecodeHttpRequestContentLengthToLongGeneratesOutboundMessage() throws IOException {
        String uri = "localhost:9090/" + randomAlphaOfLength(8);
        io.netty.handler.codec.http.HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri, false);
        HttpUtil.setContentLength(httpRequest, 1025);
        HttpUtil.setKeepAlive(httpRequest, false);

        ByteBuf buf = requestEncoder.encode(httpRequest);
        try {
            handler.consumeReads(toChannelBuffer(buf));
        } finally {
            buf.release();
        }
        verify(transport, times(0)).incomingRequestError(any(), any(), any());
        verify(transport, times(0)).incomingRequest(any(), any());

        List<FlushOperation> flushOperations = handler.pollFlushOperations();
        assertFalse(flushOperations.isEmpty());

        FlushOperation flushOperation = flushOperations.get(0);
        FullHttpResponse response = responseDecoder.decode(Unpooled.wrappedBuffer(flushOperation.getBuffersToWrite()));
        try {
            assertEquals(HttpVersion.HTTP_1_1, response.protocolVersion());
            assertEquals(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, response.status());

            flushOperation.getListener().accept(null, null);
            // Since we have keep-alive set to false, we should close the channel after the response has been
            // flushed
            verify(nioHttpChannel).close();
        } finally {
            response.release();
        }
    }

    @SuppressWarnings("unchecked")
    public void testEncodeHttpResponse() throws IOException {
        prepareHandlerForResponse(handler);

        DefaultFullHttpRequest nettyRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        NioHttpRequest nioHttpRequest = new NioHttpRequest(nettyRequest, 0);
        NioHttpResponse httpResponse = nioHttpRequest.createResponse(RestStatus.OK, BytesArray.EMPTY);
        httpResponse.addHeader(HttpHeaderNames.CONTENT_LENGTH.toString(), "0");

        SocketChannelContext context = mock(SocketChannelContext.class);
        HttpWriteOperation writeOperation = new HttpWriteOperation(context, httpResponse, mock(BiConsumer.class));
        List<FlushOperation> flushOperations = handler.writeToBytes(writeOperation);
        FlushOperation operation = flushOperations.get(0);
        FullHttpResponse response = responseDecoder.decode(Unpooled.wrappedBuffer(operation.getBuffersToWrite()));
        ((ChannelPromise) operation.getListener()).setSuccess();
        try {
            assertEquals(HttpResponseStatus.OK, response.status());
            assertEquals(HttpVersion.HTTP_1_1, response.protocolVersion());
        } finally {
            response.release();
        }
    }

    public void testCorsEnabledWithoutAllowOrigins() throws IOException {
        // Set up an HTTP transport with only the CORS enabled setting
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .build();
        FullHttpResponse response = executeCorsRequest(settings, "remote-host", "request-host");
        try {
            // inspect response and validate
            assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), nullValue());
        } finally {
            response.release();
        }
    }

    public void testCorsEnabledWithAllowOrigins() throws IOException {
        final String originValue = "remote-host";
        // create an HTTP transport with CORS enabled and allow origin configured
        Settings settings = Settings.builder()
            .put(SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), originValue)
            .build();
        FullHttpResponse response = executeCorsRequest(settings, originValue, "request-host");
        try {
            // inspect response and validate
            assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
            String allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
            assertThat(allowedOrigins, is(originValue));
        } finally {
            response.release();
        }
    }

    public void testCorsAllowOriginWithSameHost() throws IOException {
        String originValue = "remote-host";
        String host = "remote-host";
        // create an HTTP transport with CORS enabled
        Settings settings = Settings.builder()
            .put(SETTING_CORS_ENABLED.getKey(), true)
            .build();
        FullHttpResponse response = executeCorsRequest(settings, originValue, host);
        String allowedOrigins;
        try {
            // inspect response and validate
            assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
            allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
            assertThat(allowedOrigins, is(originValue));
        } finally {
            response.release();
        }
        originValue = "http://" + originValue;
        response = executeCorsRequest(settings, originValue, host);
        try {
            assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
            allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
            assertThat(allowedOrigins, is(originValue));
        } finally {
            response.release();
        }

        originValue = originValue + ":5555";
        host = host + ":5555";
        response = executeCorsRequest(settings, originValue, host);
        try {
            assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
            allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
            assertThat(allowedOrigins, is(originValue));
        } finally {
            response.release();
        }
        originValue = originValue.replace("http", "https");
        response = executeCorsRequest(settings, originValue, host);
        try {
            assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
            allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
            assertThat(allowedOrigins, is(originValue));
        } finally {
            response.release();
        }
    }

    public void testThatStringLiteralWorksOnMatch() throws IOException {
        final String originValue = "remote-host";
        Settings settings = Settings.builder()
            .put(SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), originValue)
            .put(SETTING_CORS_ALLOW_METHODS.getKey(), "get, options, post")
            .put(SETTING_CORS_ALLOW_CREDENTIALS.getKey(), true)
            .build();
        FullHttpResponse response = executeCorsRequest(settings, originValue, "request-host");
        try {
            // inspect response and validate
            assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
            String allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
            assertThat(allowedOrigins, is(originValue));
            assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_CREDENTIALS), equalTo("true"));
        } finally {
            response.release();
        }
    }

    public void testThatAnyOriginWorks() throws IOException {
        final String originValue = NioCorsHandler.ANY_ORIGIN;
        Settings settings = Settings.builder()
            .put(SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), originValue)
            .build();
        FullHttpResponse response = executeCorsRequest(settings, originValue, "request-host");
        try {
            // inspect response and validate
            assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), notNullValue());
            String allowedOrigins = response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN);
            assertThat(allowedOrigins, is(originValue));
            assertThat(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_CREDENTIALS), nullValue());
        } finally {
            response.release();
        }
    }

    private FullHttpResponse executeCorsRequest(final Settings settings, final String originValue, final String host) throws IOException {
        HttpHandlingSettings httpHandlingSettings = HttpHandlingSettings.fromSettings(settings);
        NioCorsConfig nioCorsConfig = NioHttpServerTransport.buildCorsConfig(settings);
        HttpReadWriteHandler handler = new HttpReadWriteHandler(nioHttpChannel, transport, httpHandlingSettings, nioCorsConfig);
        prepareHandlerForResponse(handler);
        DefaultFullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
        if (originValue != null) {
            httpRequest.headers().add(HttpHeaderNames.ORIGIN, originValue);
        }
        httpRequest.headers().add(HttpHeaderNames.HOST, host);
        NioHttpRequest nioHttpRequest = new NioHttpRequest(httpRequest, 0);
        BytesArray content = new BytesArray("content");
        HttpResponse response = nioHttpRequest.createResponse(RestStatus.OK, content);
        response.addHeader("Content-Length", Integer.toString(content.length()));

        SocketChannelContext context = mock(SocketChannelContext.class);
        List<FlushOperation> flushOperations = handler.writeToBytes(handler.createWriteOperation(context, response, (v, e) -> {}));
        handler.close();
        FlushOperation flushOperation = flushOperations.get(0);
        ((ChannelPromise) flushOperation.getListener()).setSuccess();
        return responseDecoder.decode(Unpooled.wrappedBuffer(flushOperation.getBuffersToWrite()));
    }



    private NioHttpRequest prepareHandlerForResponse(HttpReadWriteHandler handler) throws IOException {
        HttpMethod method = randomBoolean() ? HttpMethod.GET : HttpMethod.HEAD;
        HttpVersion version = randomBoolean() ? HttpVersion.HTTP_1_0 : HttpVersion.HTTP_1_1;
        String uri = "http://localhost:9090/" + randomAlphaOfLength(8);

        io.netty.handler.codec.http.HttpRequest request = new DefaultFullHttpRequest(version, method, uri);
        ByteBuf buf = requestEncoder.encode(request);
        try {
            handler.consumeReads(toChannelBuffer(buf));
        } finally {
            buf.release();
        }

        ArgumentCaptor<NioHttpRequest> requestCaptor = ArgumentCaptor.forClass(NioHttpRequest.class);
        verify(transport, atLeastOnce()).incomingRequest(requestCaptor.capture(), any(HttpChannel.class));

        NioHttpRequest nioHttpRequest = requestCaptor.getValue();
        assertNotNull(nioHttpRequest);
        assertEquals(method.name(), nioHttpRequest.method().name());
        if (version == HttpVersion.HTTP_1_1) {
            assertEquals(HttpRequest.HttpVersion.HTTP_1_1, nioHttpRequest.protocolVersion());
        } else {
            assertEquals(HttpRequest.HttpVersion.HTTP_1_0, nioHttpRequest.protocolVersion());
        }
        assertEquals(nioHttpRequest.uri(), uri);
        return nioHttpRequest;
    }

    private InboundChannelBuffer toChannelBuffer(ByteBuf buf) {
        InboundChannelBuffer buffer = InboundChannelBuffer.allocatingInstance();
        int readableBytes = buf.readableBytes();
        buffer.ensureCapacity(readableBytes);
        int bytesWritten = 0;
        ByteBuffer[] byteBuffers = buffer.sliceBuffersTo(readableBytes);
        int i = 0;
        while (bytesWritten != readableBytes) {
            ByteBuffer byteBuffer = byteBuffers[i++];
            int initialRemaining = byteBuffer.remaining();
            buf.readBytes(byteBuffer);
            bytesWritten += initialRemaining - byteBuffer.remaining();
        }
        buffer.incrementIndex(bytesWritten);
        return buffer;
    }

    private static final int MAX = 16 * 1024 * 1024;

    private static class RequestEncoder {

        private final EmbeddedChannel requestEncoder = new EmbeddedChannel(new HttpRequestEncoder(), new HttpObjectAggregator(MAX));

        private ByteBuf encode(io.netty.handler.codec.http.HttpRequest httpRequest) {
            requestEncoder.writeOutbound(httpRequest);
            return requestEncoder.readOutbound();
        }
    }

    private static class ResponseDecoder {

        private final EmbeddedChannel responseDecoder = new EmbeddedChannel(new HttpResponseDecoder(), new HttpObjectAggregator(MAX));

        private FullHttpResponse decode(ByteBuf response) {
            responseDecoder.writeInbound(response);
            return responseDecoder.readInbound();
        }
    }
}
