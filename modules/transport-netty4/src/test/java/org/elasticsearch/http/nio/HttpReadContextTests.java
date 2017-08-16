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
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.UnpooledHeapByteBuf;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.NetworkBytesReference;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HttpReadContextTests extends ESTestCase {

    private NioHttpRequestHandler handler;
    private NioSocketChannel channel;
    private ESEmbeddedChannel adaptor;
    private int messageLength;

    private HttpReadContext readContext;

    @Before
    public void init() throws IOException {
        handler = mock(NioHttpRequestHandler.class);
        channel = mock(NioSocketChannel.class);
        adaptor = mock(ESEmbeddedChannel.class);
        messageLength = randomInt(96) + 10;


        readContext = new HttpReadContext(channel, adaptor, handler);
    }

    public void testSuccessfulRequest() throws IOException {
        byte[] bytes = createMessage(messageLength);

        final AtomicInteger bufferCapacity = new AtomicInteger();
        when(channel.read(any(NetworkBytesReference.class))).thenAnswer(invocationOnMock -> {
            NetworkBytesReference reference = (NetworkBytesReference) invocationOnMock.getArguments()[0];
            ByteBuffer buffer = reference.getWriteByteBuffer();
            bufferCapacity.set(reference.getWriteRemaining());
            buffer.put(bytes);
            reference.incrementWrite(bytes.length);
            return bytes.length;
        });

        String uri = "localhost:9090/" + randomAlphaOfLength(8);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
        when(adaptor.decode(Unpooled.wrappedBuffer(bytes))).thenReturn(new LinkedList<>(Collections.singletonList(request)));

        readContext.read();

        verify(handler).handleMessage(channel, adaptor, request);
    }

    public void testMultipleRequests() throws IOException {
        byte[] bytes = createMessage(messageLength);

        final AtomicInteger bufferCapacity = new AtomicInteger();
        when(channel.read(any(NetworkBytesReference.class))).thenAnswer(invocationOnMock -> {
            NetworkBytesReference reference = (NetworkBytesReference) invocationOnMock.getArguments()[0];
            ByteBuffer buffer = reference.getWriteByteBuffer();
            bufferCapacity.set(reference.getWriteRemaining());
            buffer.put(bytes);
            reference.incrementWrite(bytes.length);
            return bytes.length;
        });

        String uri1 = "localhost:9090/" + randomAlphaOfLength(8);
        String uri2 = "localhost:9090/" + randomAlphaOfLength(8);
        DefaultFullHttpRequest request1 = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri1);
        DefaultFullHttpRequest request2 = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri2);
        when(adaptor.decode(Unpooled.wrappedBuffer(bytes))).thenReturn(new LinkedList<>(Arrays.asList(request1, request2)));

        readContext.read();

        verify(handler).handleMessage(channel, adaptor, request1);
        verify(handler).handleMessage(channel, adaptor, request2);
    }

    public void testZeroToOneOutboundMessageMeansWriteInterested() throws IOException {
        byte[] bytes = createMessage(messageLength);

        final AtomicInteger bufferCapacity = new AtomicInteger();
        when(channel.read(any(NetworkBytesReference.class))).thenAnswer(invocationOnMock -> {
            NetworkBytesReference reference = (NetworkBytesReference) invocationOnMock.getArguments()[0];
            ByteBuffer buffer = reference.getWriteByteBuffer();
            bufferCapacity.set(reference.getWriteRemaining());
            buffer.put(bytes);
            reference.incrementWrite(bytes.length);
            return bytes.length;
        });

        when(adaptor.decode(Unpooled.wrappedBuffer(bytes))).thenReturn(new LinkedList<>(Arrays.asList()));

        readContext.read();

        // TODO: Implement
    }

    private static byte[] createMessage(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = randomByte();
        }
        return bytes;
    }
}
