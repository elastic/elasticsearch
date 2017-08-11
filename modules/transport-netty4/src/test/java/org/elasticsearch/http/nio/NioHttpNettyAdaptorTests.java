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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.netty4.cors.Netty4CorsConfigBuilder;
import org.elasticsearch.http.netty4.pipelining.HttpPipelinedRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.ByteWriteOperation;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.elasticsearch.transport.nio.channel.WriteContext;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.function.BiConsumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NioHttpNettyAdaptorTests extends ESTestCase {

    private BiConsumer<NioSocketChannel, Throwable> exceptionHandler;
    private NioHttpNettyAdaptor adaptor;
    private NioSocketChannel nioSocketChannel;
    private WriteContext writeContext;
    private ArgumentCaptor<ByteWriteOperation> writeOperation;

    @Before
    @SuppressWarnings("unchecked")
    public void setMocks() {
        exceptionHandler = mock(BiConsumer.class);
        adaptor = new NioHttpNettyAdaptor(Settings.EMPTY, exceptionHandler, Netty4CorsConfigBuilder.forAnyOrigin().build(), 1024);
        nioSocketChannel = mock(NioSocketChannel.class);
        writeContext = mock(WriteContext.class);
        writeOperation = ArgumentCaptor.forClass(ByteWriteOperation.class);

        when(nioSocketChannel.getWriteContext()).thenReturn(writeContext);
    }

    public void testCloseEmbeddedChannelSchedulesRealChannelForClose() {
        NioSocketChannel channel = mock(NioSocketChannel.class);
        EmbeddedChannel channelAdaptor = adaptor.getAdaptor(channel);

        channelAdaptor.close();
        verify(channel).closeAsync();
    }

    public void testDecodeHttpRequest() {
        EmbeddedChannel channelAdaptor = adaptor.getAdaptor(mock(NioSocketChannel.class));

        HttpRequest defaultFullHttpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "localhost:9090/got/got");

        EmbeddedChannel ch = new EmbeddedChannel(new HttpRequestEncoder());
        ch.writeOutbound(defaultFullHttpRequest);
        ByteBuf buf = ch.readOutbound();

        channelAdaptor.writeInbound(buf.slice(0, 5).retainedDuplicate());

        assertNull(channelAdaptor.readInbound());

        channelAdaptor.writeInbound(buf.slice(5, buf.writerIndex() - 5).retainedDuplicate());
        HttpPipelinedRequest decodedRequest = channelAdaptor.readInbound();

        FullHttpRequest fullHttpRequest = (FullHttpRequest) decodedRequest.last();
        assertEquals(defaultFullHttpRequest.protocolVersion(), fullHttpRequest.protocolVersion());
        assertEquals(defaultFullHttpRequest.method(), fullHttpRequest.method());
    }

    public void testDecodeHttpRequestError() {
        EmbeddedChannel channelAdaptor = adaptor.getAdaptor(mock(NioSocketChannel.class));

        HttpRequest defaultFullHttpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "localhost:9090/got/got");

        EmbeddedChannel ch = new EmbeddedChannel(new HttpRequestEncoder());
        ch.writeOutbound(defaultFullHttpRequest);
        ByteBuf buf = ch.readOutbound();
        buf.setByte(0, ' ');
        buf.setByte(1, ' ');
        buf.setByte(2, ' ');

        channelAdaptor.writeInbound(buf);

        HttpPipelinedRequest decodedRequest = channelAdaptor.readInbound();

        FullHttpRequest fullHttpRequest = (FullHttpRequest) decodedRequest.last();
        DecoderResult decoderResult = fullHttpRequest.decoderResult();
        assertTrue(decoderResult.isFailure());
        assertTrue(decoderResult.cause() instanceof IllegalArgumentException);
    }

    public void testDecodeHttpRequestContentLengthToLongGeneratesOutboundMessage() {
        EmbeddedChannel channelAdaptor = adaptor.getAdaptor(nioSocketChannel);

        HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "localhost:9090/got/got", false);
        HttpUtil.setContentLength(httpRequest, 1025);

        EmbeddedChannel ch = new EmbeddedChannel(new HttpRequestEncoder());
        ch.writeOutbound(httpRequest);
        ByteBuf buf = ch.readOutbound();

        channelAdaptor.writeInbound(buf);

        HttpPipelinedRequest decodedRequest = channelAdaptor.readInbound();

        assertNull(decodedRequest);

        ByteBuf buffer = channelAdaptor.readOutbound();
    }

    public void testEncodeHttpResponse() {
        NioHttpNettyAdaptor nioHttpNettyAdaptor = new NioHttpNettyAdaptor(Settings.EMPTY, exceptionHandler,
            Netty4CorsConfigBuilder.forAnyOrigin().build(), 1024);
        NioSocketChannel nioSocketChannel = mock(NioSocketChannel.class);
        when(nioSocketChannel.getWriteContext()).thenReturn(mock(WriteContext.class));
        ESEmbeddedChannel adaptor = nioHttpNettyAdaptor.getAdaptor(nioSocketChannel);

        // Must send a request through pipeline inorder to handle response
        HttpRequest defaultFullHttpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "localhost:9090/got/got");
        EmbeddedChannel encodingChannel = new EmbeddedChannel(new HttpRequestEncoder());
        encodingChannel.writeOutbound(defaultFullHttpRequest);
        ByteBuf buf = encodingChannel.readOutbound();
        adaptor.writeInbound(buf);


        HttpResponse defaultFullHttpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

        adaptor.writeOutbound(defaultFullHttpResponse);
        ByteBuf encodedResponse = adaptor.readOutbound();

        EmbeddedChannel decodingChannel = new EmbeddedChannel(new HttpResponseDecoder());
        decodingChannel.writeInbound(encodedResponse);
        HttpResponse response = decodingChannel.readInbound();

        assertEquals(HttpResponseStatus.OK, response.status());
        assertEquals(HttpVersion.HTTP_1_1, response.protocolVersion());
    }
}
