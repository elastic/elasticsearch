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

package org.elasticsearch.transport.nio.http;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.NetworkBytesReference;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.elasticsearch.transport.nio.utils.TestSelectionKey;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HttpReadContextTests extends ESTestCase {

    private NioHttpRequestHandler handler;
    private NioSocketChannel channel;
    private NettyChannelAdaptor adaptor;
    private int messageLength;

    private HttpReadContext readContext;

    @Before
    public void init() throws IOException {
        handler = mock(NioHttpRequestHandler.class);
        channel = mock(NioSocketChannel.class);
        adaptor = mock(NettyChannelAdaptor.class);
        messageLength = randomInt(96) + 10;


        readContext = new HttpReadContext(channel, adaptor, handler);

        when(channel.getSelectionKey()).thenReturn(new TestSelectionKey(SelectionKey.OP_READ));
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

        when(adaptor.decode(Unpooled.wrappedBuffer(bytes))).thenReturn(new LinkedList<>(Collections.emptyList()));
        when(adaptor.hasMessages()).thenReturn(false, true);

        assertFalse((channel.getSelectionKey().interestOps() & SelectionKey.OP_WRITE) != 0);

        readContext.read();

        assertTrue((channel.getSelectionKey().interestOps() & SelectionKey.OP_WRITE) != 0);
    }

    public void testMultipleReadsForRequest() throws IOException {
        byte[] bytes = createMessage(messageLength);
        byte[] bytes2 = createMessage(messageLength + randomInt(100));

        final AtomicInteger bufferCapacity = new AtomicInteger();
        final AtomicBoolean isFirst = new AtomicBoolean(true);
        when(channel.read(any(NetworkBytesReference.class))).thenAnswer(invocationOnMock -> {
            NetworkBytesReference reference = (NetworkBytesReference) invocationOnMock.getArguments()[0];
            ByteBuffer buffer = reference.getWriteByteBuffer();
            bufferCapacity.set(reference.getWriteRemaining());
            int length;
            if (isFirst.compareAndSet(true, false)) {
                length = bytes.length;
                buffer.put(bytes);
            } else {
                length = bytes2.length;
                buffer.put(bytes2);
            }
            reference.incrementWrite(length);
            return length;
        });

        String uri = "localhost:9090/" + randomAlphaOfLength(8);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
        when(adaptor.decode(Unpooled.wrappedBuffer(bytes))).thenReturn(new LinkedList<>());
        when(adaptor.decode(Unpooled.wrappedBuffer(bytes2))).thenReturn(new LinkedList<>(Collections.singletonList(request)));

        readContext.read();
        readContext.read();

        verify(handler, times(1)).handleMessage(channel, adaptor, request);
    }

    private static byte[] createMessage(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = randomByte();
        }
        return bytes;
    }
}
