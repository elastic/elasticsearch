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

package org.elasticsearch.nio;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SocketEventHandlerTests extends ESTestCase {

    private BiConsumer<NioSocketChannel, Exception> exceptionHandler;

    private SocketEventHandler handler;
    private NioSocketChannel channel;
    private SocketChannel rawChannel;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpHandler() throws IOException {
        exceptionHandler = mock(BiConsumer.class);
        SocketSelector socketSelector = mock(SocketSelector.class);
        handler = new SocketEventHandler(logger);
        rawChannel = mock(SocketChannel.class);
        channel = new DoNotRegisterChannel(rawChannel, socketSelector);
        when(rawChannel.finishConnect()).thenReturn(true);

        Supplier<InboundChannelBuffer.Page> pageSupplier = () -> new InboundChannelBuffer.Page(ByteBuffer.allocate(1 << 14), () -> {});
        InboundChannelBuffer buffer = new InboundChannelBuffer(pageSupplier);
        channel.setContexts(new BytesChannelContext(channel, mock(ChannelContext.ReadConsumer.class), buffer), exceptionHandler);
        channel.register();
        channel.finishConnect();

        when(socketSelector.isOnCurrentThread()).thenReturn(true);
    }

    public void testRegisterCallsContext() throws IOException {
        NioSocketChannel channel = mock(NioSocketChannel.class);
        ChannelContext channelContext = mock(ChannelContext.class);
        when(channel.getContext()).thenReturn(channelContext);
        when(channel.getSelectionKey()).thenReturn(new TestSelectionKey(0));
        handler.handleRegistration(channel);
        verify(channelContext).channelRegistered();
    }

    public void testRegisterAddsOP_CONNECTAndOP_READInterest() throws IOException {
        handler.handleRegistration(channel);
        assertEquals(SelectionKey.OP_READ | SelectionKey.OP_CONNECT, channel.getSelectionKey().interestOps());
    }

    public void testRegisterWithPendingWritesAddsOP_CONNECTAndOP_READAndOP_WRITEInterest() throws IOException {
        channel.getContext().queueWriteOperation(mock(BytesWriteOperation.class));
        handler.handleRegistration(channel);
        assertEquals(SelectionKey.OP_READ | SelectionKey.OP_CONNECT | SelectionKey.OP_WRITE, channel.getSelectionKey().interestOps());
    }

    public void testRegistrationExceptionCallsExceptionHandler() throws IOException {
        CancelledKeyException exception = new CancelledKeyException();
        handler.registrationException(channel, exception);
        verify(exceptionHandler).accept(channel, exception);
    }

    public void testConnectRemovesOP_CONNECTInterest() throws IOException {
        SelectionKeyUtils.setConnectAndReadInterested(channel);
        handler.handleConnect(channel);
        assertEquals(SelectionKey.OP_READ, channel.getSelectionKey().interestOps());
    }

    public void testConnectExceptionCallsExceptionHandler() throws IOException {
        IOException exception = new IOException();
        handler.connectException(channel, exception);
        verify(exceptionHandler).accept(channel, exception);
    }

    public void testHandleReadDelegatesToContext() throws IOException {
        NioSocketChannel channel = new DoNotRegisterChannel(rawChannel, mock(SocketSelector.class));
        ChannelContext context = mock(ChannelContext.class);
        channel.setContexts(context, exceptionHandler);

        when(context.read()).thenReturn(1);
        handler.handleRead(channel);
        verify(context).read();
    }

    public void testReadExceptionCallsExceptionHandler() {
        IOException exception = new IOException();
        handler.readException(channel, exception);
        verify(exceptionHandler).accept(channel, exception);
    }

    public void testWriteExceptionCallsExceptionHandler() {
        IOException exception = new IOException();
        handler.writeException(channel, exception);
        verify(exceptionHandler).accept(channel, exception);
    }

    public void testPostHandlingCallWillCloseTheChannelIfReady() throws IOException {
        NioSocketChannel channel = mock(NioSocketChannel.class);
        ChannelContext context = mock(ChannelContext.class);
        when(channel.getSelectionKey()).thenReturn(new TestSelectionKey(0));

        when(channel.getContext()).thenReturn(context);
        when(context.selectorShouldClose()).thenReturn(true);
        handler.postHandling(channel);

        verify(channel).closeFromSelector();
    }

    public void testPostHandlingCallWillNotCloseTheChannelIfNotReady() throws IOException {
        NioSocketChannel channel = mock(NioSocketChannel.class);
        ChannelContext context = mock(ChannelContext.class);
        when(channel.getSelectionKey()).thenReturn(new TestSelectionKey(0));

        when(channel.getContext()).thenReturn(context);
        when(context.selectorShouldClose()).thenReturn(false);
        handler.postHandling(channel);

        verify(channel, times(0)).closeFromSelector();
    }

    public void testPostHandlingWillAddWriteIfNecessary() throws IOException {
        NioSocketChannel channel = new DoNotRegisterChannel(rawChannel, mock(SocketSelector.class));
        channel.setSelectionKey(new TestSelectionKey(SelectionKey.OP_READ));
        ChannelContext context = mock(ChannelContext.class);
        channel.setContexts(context, null);

        when(context.hasQueuedWriteOps()).thenReturn(true);

        assertEquals(SelectionKey.OP_READ, channel.getSelectionKey().interestOps());
        handler.postHandling(channel);
        assertEquals(SelectionKey.OP_READ | SelectionKey.OP_WRITE, channel.getSelectionKey().interestOps());
    }

    public void testPostHandlingWillRemoveWriteIfNecessary() throws IOException {
        NioSocketChannel channel = new DoNotRegisterChannel(rawChannel, mock(SocketSelector.class));
        channel.setSelectionKey(new TestSelectionKey(SelectionKey.OP_READ | SelectionKey.OP_WRITE));
        ChannelContext context = mock(ChannelContext.class);
        channel.setContexts(context, null);

        when(context.hasQueuedWriteOps()).thenReturn(false);

        assertEquals(SelectionKey.OP_READ | SelectionKey.OP_WRITE, channel.getSelectionKey().interestOps());
        handler.postHandling(channel);
        assertEquals(SelectionKey.OP_READ, channel.getSelectionKey().interestOps());
    }

    private void setWriteAndRead(NioChannel channel) {
        SelectionKeyUtils.setConnectAndReadInterested(channel);
        SelectionKeyUtils.removeConnectInterested(channel);
        SelectionKeyUtils.setWriteInterested(channel);
    }
}
