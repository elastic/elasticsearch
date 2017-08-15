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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.http.HttpStatus;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.http.netty4.cors.Netty4CorsConfigBuilder;
import org.elasticsearch.http.netty4.pipelining.HttpPipelinedRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.netty4.Netty4Utils;
import org.elasticsearch.transport.nio.ByteWriteOperation;
import org.elasticsearch.transport.nio.channel.CloseFuture;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.elasticsearch.transport.nio.channel.WriteContext;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.function.BiConsumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NioHttpNettyAdaptorTests extends ESTestCase {

    private BiConsumer<NioSocketChannel, Throwable> exceptionHandler;
    private NioHttpNettyAdaptor adaptor;
    private NioSocketChannel nioSocketChannel;

    private final RequestEncoder requestEncoder = new RequestEncoder();
    private final ResponseDecoder responseDecoder = new ResponseDecoder();

    @Before
    @SuppressWarnings("unchecked")
    public void setMocks() {
        exceptionHandler = mock(BiConsumer.class);

        adaptor = new NioHttpNettyAdaptor(Settings.EMPTY, exceptionHandler, Netty4CorsConfigBuilder.forAnyOrigin().build(), 1024);
        nioSocketChannel = mock(NioSocketChannel.class);
    }

    @SuppressWarnings("unchecked")
    public void testCloseAdaptorSchedulesRealChannelForClose() {
        NioSocketChannel channel = mock(NioSocketChannel.class);
        ESEmbeddedChannel channelAdaptor = adaptor.getAdaptor(channel);
        ArgumentCaptor<ActionListener> captor = ArgumentCaptor.forClass(ActionListener.class);
        CloseFuture closeFuture = mock(CloseFuture.class);
        when(channel.closeAsync()).thenReturn(closeFuture);

        ChannelFuture nettyFuture = channelAdaptor.close();
        verify(channel).closeAsync();
        verify(closeFuture).addListener(captor.capture());

        ActionListener<NioChannel> listener = captor.getValue();
        assertFalse(nettyFuture.isDone());
        if (randomBoolean()) {
            listener.onResponse(channel);
            assertTrue(nettyFuture.isSuccess());
        } else {
            IOException e = new IOException();
            listener.onFailure(e);
            assertFalse(nettyFuture.isSuccess());
            assertSame(e, nettyFuture.cause());
        }

        assertTrue(nettyFuture.isDone());
    }

    public void testSuccessfulDecodeHttpRequest() {
        ESEmbeddedChannel channelAdaptor = adaptor.getAdaptor(nioSocketChannel);

        String uri = "localhost:9090/" + randomAlphaOfLength(8);
        HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);

        ByteBuf buf = requestEncoder.encode(httpRequest);
        int slicePoint = randomInt(buf.writerIndex() - 1);

        ByteBuf slicedBuf = buf.retainedSlice(0, slicePoint);
        channelAdaptor.writeInbound(slicedBuf);

        assertNull(channelAdaptor.readInbound());

        channelAdaptor.writeInbound(buf.retainedSlice(slicePoint, buf.writerIndex() - slicePoint));
        HttpPipelinedRequest decodedRequest = channelAdaptor.readInbound();

        FullHttpRequest fullHttpRequest = (FullHttpRequest) decodedRequest.last();
        assertEquals(httpRequest.protocolVersion(), fullHttpRequest.protocolVersion());
        assertEquals(httpRequest.method(), fullHttpRequest.method());
    }

    public void testDecodeHttpRequestError() {
        ESEmbeddedChannel channelAdaptor = adaptor.getAdaptor(nioSocketChannel);

        String uri = "localhost:9090/" + randomAlphaOfLength(8);
        HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);

        ByteBuf buf = requestEncoder.encode(httpRequest);
        buf.setByte(0, ' ');
        buf.setByte(1, ' ');
        buf.setByte(2, ' ');

        channelAdaptor.writeInbound(buf.retainedDuplicate());
        HttpPipelinedRequest decodedRequest = channelAdaptor.readInbound();

        FullHttpRequest fullHttpRequest = (FullHttpRequest) decodedRequest.last();
        DecoderResult decoderResult = fullHttpRequest.decoderResult();
        assertTrue(decoderResult.isFailure());
        assertTrue(decoderResult.cause() instanceof IllegalArgumentException);
    }

    public void testDecodeHttpRequestContentLengthToLongGeneratesOutboundMessage() {
        ESEmbeddedChannel channelAdaptor = adaptor.getAdaptor(nioSocketChannel);

        String uri = "localhost:9090/" + randomAlphaOfLength(8);
        HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri, false);
        HttpUtil.setContentLength(httpRequest, 1025);

        ByteBuf buf = requestEncoder.encode(httpRequest);

        channelAdaptor.writeInbound(buf.retainedDuplicate());

        HttpPipelinedRequest decodedRequest = channelAdaptor.readInbound();

        assertNull(decodedRequest);

        Tuple<BytesReference, ChannelPromise> message = channelAdaptor.getMessage();

        assertFalse(message.v2().isDone());

        HttpResponse response = responseDecoder.decode(Netty4Utils.toByteBuf(message.v1()));
        assertEquals(HttpVersion.HTTP_1_1, response.protocolVersion());
        assertEquals(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, response.status());
    }

    public void testEncodeHttpResponse() {
        ESEmbeddedChannel channelAdaptor = adaptor.getAdaptor(nioSocketChannel);

        prepareAdaptorForResponse(channelAdaptor);

        HttpResponse defaultFullHttpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

        channelAdaptor.writeOutbound(defaultFullHttpResponse);
        Tuple<BytesReference, ChannelPromise> encodedMessage = channelAdaptor.getMessage();

        HttpResponse response = responseDecoder.decode(Netty4Utils.toByteBuf(encodedMessage.v1()));

        assertEquals(HttpResponseStatus.OK, response.status());
        assertEquals(HttpVersion.HTTP_1_1, response.protocolVersion());
    }

//    public void testEncodeHttpResponseAfterClose() {
//        ESEmbeddedChannel channelAdaptor = adaptor.getAdaptor(nioSocketChannel);
//
//        prepareAdaptorForResponse(channelAdaptor);
//
//        HttpResponse defaultFullHttpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
//
//        ChannelFuture close = channelAdaptor.close();
//
//        channelAdaptor.writeOutbound(defaultFullHttpResponse);
//        Tuple<BytesReference, ChannelPromise> encodedMessage = channelAdaptor.getMessage();
//
//        HttpResponse response = responseDecoder.decode(Netty4Utils.toByteBuf(encodedMessage.v1()));
//
//        assertEquals(HttpResponseStatus.OK, response.status());
//        assertEquals(HttpVersion.HTTP_1_1, response.protocolVersion());
//    }

    private void prepareAdaptorForResponse(ESEmbeddedChannel adaptor) {
        HttpMethod method = HttpMethod.GET;
        HttpVersion version = HttpVersion.HTTP_1_1;
        String uri = "http://localhost:9090/" + randomAlphaOfLength(8);

        HttpRequest request = new DefaultFullHttpRequest(version, method, uri);
        ByteBuf buf = requestEncoder.encode(request);

        adaptor.writeInbound(buf);
        HttpPipelinedRequest pipelinedRequest = adaptor.readInbound();
        FullHttpRequest requestParsed = (FullHttpRequest) pipelinedRequest.last();
        assertNotNull(requestParsed);
        assertEquals(requestParsed.method(), method);
        assertEquals(requestParsed.protocolVersion(), version);
        assertEquals(requestParsed.uri(), uri);
    }

    private static class RequestEncoder {

        private final EmbeddedChannel requestEncoder = new EmbeddedChannel(new HttpRequestEncoder());

        private ByteBuf encode(HttpRequest httpRequest) {
            requestEncoder.writeOutbound(httpRequest);
            return requestEncoder.readOutbound();
        }
    }

    private static class ResponseDecoder {

        private final EmbeddedChannel responseDecoder = new EmbeddedChannel(new HttpResponseDecoder());

        private HttpResponse decode(ByteBuf response) {
            responseDecoder.writeInbound(response);
            return responseDecoder.readInbound();
        }
    }
}
