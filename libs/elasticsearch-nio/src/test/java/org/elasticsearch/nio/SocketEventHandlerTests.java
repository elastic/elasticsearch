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
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SocketEventHandlerTests extends ESTestCase {

    private Consumer<Exception> channelExceptionHandler;
    private Consumer<Exception> genericExceptionHandler;

    private ReadWriteHandler readWriteHandler;
    private SocketEventHandler handler;
    private NioSocketChannel channel;
    private SocketChannel rawChannel;
    private DoNotRegisterContext context;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpHandler() throws IOException {
        channelExceptionHandler = mock(Consumer.class);
        genericExceptionHandler = mock(Consumer.class);
        readWriteHandler = mock(ReadWriteHandler.class);
        SocketSelector selector = mock(SocketSelector.class);
        handler = new SocketEventHandler(genericExceptionHandler);
        rawChannel = mock(SocketChannel.class);
        channel = new NioSocketChannel(rawChannel);
        when(rawChannel.finishConnect()).thenReturn(true);

        context = new DoNotRegisterContext(channel, selector, channelExceptionHandler, new TestSelectionKey(0), readWriteHandler);
        channel.setContext(context);
        handler.handleRegistration(context);

        when(selector.isOnCurrentThread()).thenReturn(true);
    }

    public void testRegisterCallsContext() throws IOException {
        NioSocketChannel channel = mock(NioSocketChannel.class);
        SocketChannelContext channelContext = mock(SocketChannelContext.class);
        when(channel.getContext()).thenReturn(channelContext);
        when(channelContext.getSelectionKey()).thenReturn(new TestSelectionKey(0));
        handler.handleRegistration(channelContext);
        verify(channelContext).register();
    }

    public void testRegisterAddsOP_CONNECTAndOP_READInterest() throws IOException {
        SocketChannelContext context = mock(SocketChannelContext.class);
        when(context.getSelectionKey()).thenReturn(new TestSelectionKey(0));
        handler.handleRegistration(context);
        assertEquals(SelectionKey.OP_READ | SelectionKey.OP_CONNECT, context.getSelectionKey().interestOps());
    }

    public void testRegisterAddsAttachment() throws IOException {
        SocketChannelContext context = mock(SocketChannelContext.class);
        when(context.getSelectionKey()).thenReturn(new TestSelectionKey(0));
        handler.handleRegistration(context);
        assertEquals(context, context.getSelectionKey().attachment());
    }

    public void testRegisterWithPendingWritesAddsOP_CONNECTAndOP_READAndOP_WRITEInterest() throws IOException {
        FlushReadyWrite flushReadyWrite = mock(FlushReadyWrite.class);
        when(readWriteHandler.writeToBytes(flushReadyWrite)).thenReturn(Collections.singletonList(flushReadyWrite));
        channel.getContext().queueWriteOperation(flushReadyWrite);
        handler.handleRegistration(context);
        assertEquals(SelectionKey.OP_READ | SelectionKey.OP_CONNECT | SelectionKey.OP_WRITE, context.getSelectionKey().interestOps());
    }

    public void testRegistrationExceptionCallsExceptionHandler() throws IOException {
        CancelledKeyException exception = new CancelledKeyException();
        handler.registrationException(context, exception);
        verify(channelExceptionHandler).accept(exception);
    }

    public void testConnectDoesNotRemoveOP_CONNECTInterestIfIncomplete() throws IOException {
        SelectionKeyUtils.setConnectAndReadInterested(context.getSelectionKey());
        handler.handleConnect(context);
        assertEquals(SelectionKey.OP_READ, context.getSelectionKey().interestOps());
    }

    public void testConnectRemovesOP_CONNECTInterestIfComplete() throws IOException {
        SelectionKeyUtils.setConnectAndReadInterested(context.getSelectionKey());
        handler.handleConnect(context);
        assertEquals(SelectionKey.OP_READ, context.getSelectionKey().interestOps());
    }

    public void testConnectExceptionCallsExceptionHandler() throws IOException {
        IOException exception = new IOException();
        handler.connectException(context, exception);
        verify(channelExceptionHandler).accept(exception);
    }

    public void testHandleReadDelegatesToContext() throws IOException {
        NioSocketChannel channel = new NioSocketChannel(rawChannel);
        SocketChannelContext context = mock(SocketChannelContext.class);
        channel.setContext(context);

        when(context.read()).thenReturn(1);
        handler.handleRead(context);
        verify(context).read();
    }

    public void testReadExceptionCallsExceptionHandler() {
        IOException exception = new IOException();
        handler.readException(context, exception);
        verify(channelExceptionHandler).accept(exception);
    }

    public void testWriteExceptionCallsExceptionHandler() {
        IOException exception = new IOException();
        handler.writeException(context, exception);
        verify(channelExceptionHandler).accept(exception);
    }

    public void testPostHandlingCallWillCloseTheChannelIfReady() throws IOException {
        NioSocketChannel channel = mock(NioSocketChannel.class);
        SocketChannelContext context = mock(SocketChannelContext.class);

        when(channel.getContext()).thenReturn(context);
        when(context.selectorShouldClose()).thenReturn(true);
        handler.postHandling(context);

        verify(context).closeFromSelector();
    }

    public void testPostHandlingCallWillNotCloseTheChannelIfNotReady() throws IOException {
        SocketChannelContext context = mock(SocketChannelContext.class);
        when(context.getSelectionKey()).thenReturn(new TestSelectionKey(SelectionKey.OP_READ | SelectionKey.OP_WRITE));
        when(context.selectorShouldClose()).thenReturn(false);

        NioSocketChannel channel = mock(NioSocketChannel.class);
        when(channel.getContext()).thenReturn(context);

        handler.postHandling(context);

        verify(context, times(0)).closeFromSelector();
    }

    public void testPostHandlingWillAddWriteIfNecessary() throws IOException {
        TestSelectionKey selectionKey = new TestSelectionKey(SelectionKey.OP_READ);
        SocketChannelContext context = mock(SocketChannelContext.class);
        when(context.getSelectionKey()).thenReturn(selectionKey);
        when(context.readyForFlush()).thenReturn(true);

        NioSocketChannel channel = mock(NioSocketChannel.class);
        when(channel.getContext()).thenReturn(context);

        assertEquals(SelectionKey.OP_READ, selectionKey.interestOps());
        handler.postHandling(context);
        assertEquals(SelectionKey.OP_READ | SelectionKey.OP_WRITE, selectionKey.interestOps());
    }

    public void testPostHandlingWillRemoveWriteIfNecessary() throws IOException {
        TestSelectionKey key = new TestSelectionKey(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        SocketChannelContext context = mock(SocketChannelContext.class);
        when(context.getSelectionKey()).thenReturn(key);
        when(context.readyForFlush()).thenReturn(false);

        NioSocketChannel channel = mock(NioSocketChannel.class);
        when(channel.getContext()).thenReturn(context);


        assertEquals(SelectionKey.OP_READ | SelectionKey.OP_WRITE, key.interestOps());
        handler.postHandling(context);
        assertEquals(SelectionKey.OP_READ, key.interestOps());
    }

    public void testListenerExceptionCallsGenericExceptionHandler() throws IOException {
        RuntimeException listenerException = new RuntimeException();
        handler.listenerException(listenerException);
        verify(genericExceptionHandler).accept(listenerException);
    }

    private class DoNotRegisterContext extends BytesChannelContext {

        private final TestSelectionKey selectionKey;

        DoNotRegisterContext(NioSocketChannel channel, SocketSelector selector, Consumer<Exception> exceptionHandler,
                             TestSelectionKey selectionKey, ReadWriteHandler handler) {
            super(channel, selector, exceptionHandler, handler, InboundChannelBuffer.allocatingInstance());
            this.selectionKey = selectionKey;
        }

        @Override
        public void register() {
            setSelectionKey(selectionKey);
        }
    }
}
