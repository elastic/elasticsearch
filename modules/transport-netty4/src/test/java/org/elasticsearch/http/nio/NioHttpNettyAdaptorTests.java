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
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.netty4.cors.Netty4CorsConfigBuilder;
import org.elasticsearch.http.netty4.pipelining.HttpPipelinedRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.elasticsearch.transport.nio.channel.WriteContext;

import java.util.function.BiConsumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NioHttpNettyAdaptorTests extends ESTestCase {

    public void testDecodeHttpRequest() {
        NioHttpNettyAdaptor nioHttpNettyAdaptor = new NioHttpNettyAdaptor(Settings.EMPTY, mock(BiConsumer.class),
            Netty4CorsConfigBuilder.forAnyOrigin().build(), 1024);
        EmbeddedChannel adaptor = nioHttpNettyAdaptor.getAdaptor(mock(NioSocketChannel.class));

        HttpRequest defaultFullHttpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "localhost:9090/got/got");

        EmbeddedChannel ch = new EmbeddedChannel(new HttpRequestEncoder());
        ch.writeOutbound(defaultFullHttpRequest);
        ByteBuf buf = ch.readOutbound();

        adaptor.writeInbound(buf.slice(0, 5).retainedDuplicate());

        assertNull(adaptor.readInbound());

        adaptor.writeInbound(buf.slice(5, buf.writerIndex() - 5).retainedDuplicate());
        HttpPipelinedRequest decodedRequest = adaptor.readInbound();

        FullHttpRequest fullHttpRequest = (FullHttpRequest) decodedRequest.last();
        assertEquals(defaultFullHttpRequest.protocolVersion(), fullHttpRequest.protocolVersion());
        assertEquals(defaultFullHttpRequest.method(), fullHttpRequest.method());
    }

    public void testEncodeHttpResponse() {
        NioHttpNettyAdaptor nioHttpNettyAdaptor = new NioHttpNettyAdaptor(Settings.EMPTY, mock(BiConsumer.class),
            Netty4CorsConfigBuilder.forAnyOrigin().build(), 1024);
        NioSocketChannel nioSocketChannel = mock(NioSocketChannel.class);
        when(nioSocketChannel.getWriteContext()).thenReturn(mock(WriteContext.class));
        EmbeddedChannel adaptor = nioHttpNettyAdaptor.getAdaptor(nioSocketChannel);

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
