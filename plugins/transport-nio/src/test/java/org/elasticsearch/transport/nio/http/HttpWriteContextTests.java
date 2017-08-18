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

import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.NetworkBytesReference;
import org.elasticsearch.transport.nio.SocketSelector;
import org.elasticsearch.transport.nio.WriteOperation;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.elasticsearch.transport.nio.http.ESChannelPromise;
import org.elasticsearch.transport.nio.http.ESEmbeddedChannel;
import org.elasticsearch.transport.nio.http.HttpWriteContext;
import org.elasticsearch.transport.nio.http.HttpWriteOperation;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class HttpWriteContextTests extends ESTestCase {

    private ESEmbeddedChannel adaptor;
    private NioSocketChannel channel;
    private SocketSelector selector;
    private ESChannelPromise listener;

    private HttpWriteContext writeContext;

    @Before
    public void initMocks() {
        adaptor = mock(ESEmbeddedChannel.class);
        channel = mock(NioSocketChannel.class);
        selector = mock(SocketSelector.class);
        listener = mock(ESChannelPromise.class);

        writeContext = new HttpWriteContext(channel, adaptor);

        when(channel.isWritable()).thenReturn(true);
        when(channel.getSelector()).thenReturn(selector);
        when(selector.isOnCurrentThread()).thenReturn(true);
    }

    public void testCannotSendIfChannelNotWritable() {
        DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        when(channel.isWritable()).thenReturn(false);

        writeContext.sendMessage(response, listener);

        verify(listener).onFailure(any(ClosedChannelException.class));
    }

    public void testSendFromDifferentThread() {
        DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        ArgumentCaptor<WriteOperation> captor = ArgumentCaptor.forClass(WriteOperation.class);

        when(selector.isOnCurrentThread()).thenReturn(false);
        writeContext.sendMessage(response, listener);

        verify(selector).queueWrite(captor.capture());

        HttpWriteOperation httpWriteOperation = (HttpWriteOperation) captor.getValue();
        assertSame(response, httpWriteOperation.getHttpResponse());
        assertSame(listener, httpWriteOperation.getListener());
    }

    public void testSendFromSelectorThread() {
        DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        ArgumentCaptor<WriteOperation> captor = ArgumentCaptor.forClass(WriteOperation.class);

        writeContext.sendMessage(response, listener);

        verify(selector).queueWriteInChannelBuffer(captor.capture());

        HttpWriteOperation httpWriteOperation = (HttpWriteOperation) captor.getValue();
        assertSame(response, httpWriteOperation.getHttpResponse());
        assertSame(listener, httpWriteOperation.getListener());
    }

    public void testQueueWriteOperationIsDelegatedToNettyAdaptor() {
        DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

        writeContext.queueWriteOperations(new HttpWriteOperation(channel, response, listener));

        verify(adaptor).write(response, listener);
    }

    public void testSingleFlush() throws IOException {
        when(adaptor.popMessage()).thenReturn(new Tuple<>(new BytesArray("message"), listener),
            (Tuple<BytesReference, ChannelPromise>) null);

        List<String> messages = new ArrayList<>();
        when(channel.write(any())).thenAnswer(invocationOnMock -> {
            NetworkBytesReference[] references = (NetworkBytesReference[]) invocationOnMock.getArguments()[0];
            if (references.length > 1) {
                throw new IOException("Only expected 1 reference");
            }
            int readRemaining = references[0].getReadRemaining();
            messages.add(references[0].utf8ToString());
            references[0].incrementRead(readRemaining);
            return readRemaining;
        });

        writeContext.flushChannel();

        verify(listener).onResponse(channel);
        assertEquals("message", messages.get(0));
    }

    public void testMultiFlush() throws IOException {
        ESChannelPromise listener2 = mock(ESChannelPromise.class);
        when(adaptor.popMessage()).thenReturn(new Tuple<>(new BytesArray("message1"), listener),
            new Tuple<>(new BytesArray("message2"), listener2), null);

        List<String> messages = new ArrayList<>();
        when(channel.write(any())).thenAnswer(invocationOnMock -> {
            NetworkBytesReference[] references = (NetworkBytesReference[]) invocationOnMock.getArguments()[0];
            if (references.length > 1) {
                throw new IOException("Only expected 1 reference");
            }
            int readRemaining = references[0].getReadRemaining();
            messages.add(references[0].utf8ToString());
            references[0].incrementRead(readRemaining);
            return readRemaining;
        });

        writeContext.flushChannel();

        verify(listener).onResponse(channel);
        verify(listener2).onResponse(channel);
        assertEquals("message1", messages.get(0));
        assertEquals("message2", messages.get(1));
    }

    public void testPartialFlush() throws IOException {
        when(adaptor.popMessage()).thenReturn(new Tuple<>(new BytesArray("message"), listener),
            (Tuple<BytesReference, ChannelPromise>) null);

        List<BytesReference> messages = new ArrayList<>();
        AtomicBoolean firstCall = new AtomicBoolean(true);
        when(channel.write(any())).thenAnswer(invocationOnMock -> {
            NetworkBytesReference[] references = (NetworkBytesReference[]) invocationOnMock.getArguments()[0];
            if (references.length > 1) {
                throw new IOException("Only expected 1 reference");
            }
            int read;
            if (firstCall.compareAndSet(true, false)) {
                messages.add(references[0].slice(0, 1));
                references[0].incrementRead(1);
                read = 1;
            } else {
                int readRemaining = references[0].getReadRemaining();
                messages.add(references[0].slice(1, readRemaining));
                references[0].incrementRead(readRemaining);
                read = readRemaining;
            }
            return read;
        });

        writeContext.flushChannel();
        verify(listener, times(0)).onResponse(channel);

        writeContext.flushChannel();
        verify(listener, times(1)).onResponse(channel);
        assertEquals("message", new CompositeBytesReference(messages.toArray(new BytesReference[0])).utf8ToString());
    }

    public void testHasQueuedReferencesAdaptor() throws IOException {
        when(adaptor.hasMessages()).thenReturn(true);

        assertTrue(writeContext.hasQueuedWriteOps());
    }

    public void testHasQueuedWillConsiderPartialFlush() throws IOException {
        when(adaptor.popMessage()).thenReturn(new Tuple<>(new BytesArray("message"), listener),
            (Tuple<BytesReference, ChannelPromise>) null);
        when(channel.write(any())).thenAnswer(invocationOnMock -> {
            NetworkBytesReference[] references = (NetworkBytesReference[]) invocationOnMock.getArguments()[0];
            if (references.length > 1) {
                throw new IOException("Only expected 1 reference");
            }
            references[0].incrementRead(1);
            return 1;
        });

        writeContext.flushChannel();

        when(adaptor.hasMessages()).thenReturn(false);

        assertTrue(writeContext.hasQueuedWriteOps());
    }

    public void testAdaptorMessagesAndPartialMessagesAreClosed() throws IOException {
        ESChannelPromise listener2 = mock(ESChannelPromise.class);
        when(adaptor.popMessage()).thenReturn(new Tuple<>(new BytesArray("message1"), listener),
            new Tuple<>(new BytesArray("message2"), listener2), null);
        when(channel.write(any())).thenAnswer(invocationOnMock -> {
            NetworkBytesReference[] references = (NetworkBytesReference[]) invocationOnMock.getArguments()[0];
            references[0].incrementRead(1);
            return 1;
        });

        writeContext.flushChannel();

        ClosedChannelException closedChannelException = new ClosedChannelException();

        writeContext.clearQueuedWriteOps(closedChannelException);

        verify(listener).onFailure(closedChannelException);
        verify(listener2).setFailure(closedChannelException);
        verify(adaptor).closeNettyChannel();
    }
}
