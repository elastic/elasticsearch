/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EventHandlerTests extends ESTestCase {

    private Consumer<Exception> channelExceptionHandler;
    private Consumer<Exception> genericExceptionHandler;

    private NioChannelHandler readWriteHandler;
    private EventHandler handler;
    private DoNotRegisterSocketContext context;
    private DoNotRegisterServerContext serverContext;
    private ChannelFactory<NioServerSocketChannel, NioSocketChannel> channelFactory;
    private RoundRobinSupplier<NioSelector> selectorSupplier;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpHandler() throws IOException {
        channelExceptionHandler = mock(Consumer.class);
        genericExceptionHandler = mock(Consumer.class);
        readWriteHandler = mock(NioChannelHandler.class);
        channelFactory = mock(ChannelFactory.class);
        NioSelector selector = mock(NioSelector.class);
        ArrayList<NioSelector> selectors = new ArrayList<>();
        selectors.add(selector);
        selectorSupplier = new RoundRobinSupplier<>(selectors.toArray(new NioSelector[0]));
        handler = new EventHandler(genericExceptionHandler, selectorSupplier);

        SocketChannel rawChannel = mock(SocketChannel.class);
        when(rawChannel.finishConnect()).thenReturn(true);
        NioSocketChannel channel = new NioSocketChannel(rawChannel);
        Socket socket = mock(Socket.class);
        when(rawChannel.socket()).thenReturn(socket);
        when(socket.getChannel()).thenReturn(rawChannel);
        context = new DoNotRegisterSocketContext(channel, selector, channelExceptionHandler, readWriteHandler);
        channel.setContext(context);
        handler.handleRegistration(context);

        ServerSocketChannel serverSocketChannel = mock(ServerSocketChannel.class);
        when(serverSocketChannel.socket()).thenReturn(mock(ServerSocket.class));
        NioServerSocketChannel serverChannel = new NioServerSocketChannel(serverSocketChannel);
        serverContext = new DoNotRegisterServerContext(serverChannel, mock(NioSelector.class), mock(Consumer.class));
        serverChannel.setContext(serverContext);

        when(selector.isOnCurrentThread()).thenReturn(true);
    }

    public void testRegisterCallsContext() throws IOException {
        ChannelContext<?> channelContext = randomBoolean() ? mock(SocketChannelContext.class) : mock(ServerChannelContext.class);
        TestSelectionKey attachment = new TestSelectionKey(0);
        when(channelContext.getSelectionKey()).thenReturn(attachment);
        attachment.attach(channelContext);
        handler.handleRegistration(channelContext);
        verify(channelContext).register();
    }

    public void testActiveNonServerAddsOP_CONNECTAndOP_READInterest() throws IOException {
        SocketChannelContext mockContext = mock(SocketChannelContext.class);
        when(mockContext.getSelectionKey()).thenReturn(new TestSelectionKey(0));
        handler.handleActive(mockContext);
        assertEquals(SelectionKey.OP_READ | SelectionKey.OP_CONNECT, mockContext.getSelectionKey().interestOps());
    }

    public void testHandleServerActiveSetsOP_ACCEPTInterest() throws IOException {
        ServerChannelContext mockServerContext = mock(ServerChannelContext.class);
        when(mockServerContext.getSelectionKey()).thenReturn(new TestSelectionKey(0));
        handler.handleActive(mockServerContext);

        assertEquals(SelectionKey.OP_ACCEPT, mockServerContext.getSelectionKey().interestOps());
    }

    public void testHandleAcceptAccept() throws IOException {
        ServerChannelContext serverChannelContext = mock(ServerChannelContext.class);

        handler.acceptChannel(serverChannelContext);

        verify(serverChannelContext).acceptChannels(selectorSupplier);
    }

    public void testAcceptExceptionCallsExceptionHandler() throws IOException {
        ServerChannelContext serverChannelContext = mock(ServerChannelContext.class);
        IOException exception = new IOException();
        handler.acceptException(serverChannelContext, exception);

        verify(serverChannelContext).handleException(exception);
    }

    public void testActiveWithPendingWritesAddsOP_CONNECTAndOP_READAndOP_WRITEInterest() throws IOException {
        FlushReadyWrite flushReadyWrite = mock(FlushReadyWrite.class);
        when(readWriteHandler.writeToBytes(flushReadyWrite)).thenReturn(Collections.singletonList(flushReadyWrite));
        context.queueWriteOperation(flushReadyWrite);
        handler.handleActive(context);
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
        SocketChannelContext mockContext = mock(SocketChannelContext.class);
        handler.handleRead(mockContext);
        verify(mockContext).read();
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
        SocketChannelContext mockContext = mock(SocketChannelContext.class);

        when(channel.getContext()).thenReturn(mockContext);
        when(mockContext.selectorShouldClose()).thenReturn(true);
        handler.postHandling(mockContext);

        verify(mockContext).closeFromSelector();
    }

    public void testPostHandlingCallWillNotCloseTheChannelIfNotReady() throws IOException {
        SocketChannelContext mockContext = mock(SocketChannelContext.class);
        when(mockContext.getSelectionKey()).thenReturn(new TestSelectionKey(SelectionKey.OP_READ | SelectionKey.OP_WRITE));
        when(mockContext.selectorShouldClose()).thenReturn(false);

        NioSocketChannel channel = mock(NioSocketChannel.class);
        when(channel.getContext()).thenReturn(mockContext);

        handler.postHandling(mockContext);

        verify(mockContext, times(0)).closeFromSelector();
    }

    public void testPostHandlingWillAddWriteIfNecessary() throws IOException {
        TestSelectionKey selectionKey = new TestSelectionKey(SelectionKey.OP_READ);
        SocketChannelContext mockContext = mock(SocketChannelContext.class);
        when(mockContext.getSelectionKey()).thenReturn(selectionKey);
        when(mockContext.readyForFlush()).thenReturn(true);

        NioSocketChannel channel = mock(NioSocketChannel.class);
        when(channel.getContext()).thenReturn(mockContext);

        assertEquals(SelectionKey.OP_READ, selectionKey.interestOps());
        handler.postHandling(mockContext);
        assertEquals(SelectionKey.OP_READ | SelectionKey.OP_WRITE, selectionKey.interestOps());
    }

    public void testPostHandlingWillRemoveWriteIfNecessary() throws IOException {
        TestSelectionKey key = new TestSelectionKey(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        SocketChannelContext mockContext = mock(SocketChannelContext.class);
        when(mockContext.getSelectionKey()).thenReturn(key);
        when(mockContext.readyForFlush()).thenReturn(false);

        NioSocketChannel channel = mock(NioSocketChannel.class);
        when(channel.getContext()).thenReturn(mockContext);

        assertEquals(SelectionKey.OP_READ | SelectionKey.OP_WRITE, key.interestOps());
        handler.postHandling(mockContext);
        assertEquals(SelectionKey.OP_READ, key.interestOps());
    }

    public void testHandleTaskWillRunTask() throws Exception {
        AtomicBoolean isRun = new AtomicBoolean(false);
        handler.handleTask(() -> isRun.set(true));
        assertTrue(isRun.get());
    }

    public void testTaskExceptionWillCallExceptionHandler() throws Exception {
        RuntimeException exception = new RuntimeException();
        handler.taskException(exception);
        verify(genericExceptionHandler).accept(exception);
    }

    private class DoNotRegisterSocketContext extends BytesChannelContext {

        DoNotRegisterSocketContext(
            NioSocketChannel channel,
            NioSelector selector,
            Consumer<Exception> exceptionHandler,
            NioChannelHandler handler
        ) {
            super(channel, selector, getSocketConfig(), exceptionHandler, handler, InboundChannelBuffer.allocatingInstance());
        }

        @Override
        public void register() {
            TestSelectionKey selectionKey = new TestSelectionKey(0);
            setSelectionKey(selectionKey);
            selectionKey.attach(this);
        }
    }

    private class DoNotRegisterServerContext extends ServerChannelContext {

        @SuppressWarnings("unchecked")
        DoNotRegisterServerContext(NioServerSocketChannel channel, NioSelector selector, Consumer<NioSocketChannel> acceptor) {
            super(channel, channelFactory, selector, getServerSocketConfig(), acceptor, mock(Consumer.class));
        }

        @Override
        public void register() {
            TestSelectionKey selectionKey = new TestSelectionKey(0);
            setSelectionKey(new TestSelectionKey(0));
            selectionKey.attach(this);
        }
    }

    private static Config.ServerSocket getServerSocketConfig() {
        return new Config.ServerSocket(randomBoolean(), mock(InetSocketAddress.class));
    }

    private static Config.Socket getSocketConfig() {
        return new Config.Socket(
            randomBoolean(),
            randomBoolean(),
            -1,
            -1,
            -1,
            randomBoolean(),
            -1,
            -1,
            mock(InetSocketAddress.class),
            randomBoolean()
        );
    }
}
