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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Collections;
import java.util.HashSet;
import java.util.function.BiConsumer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.isNull;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SocketSelectorTests extends ESTestCase {

    private SocketSelector socketSelector;
    private SocketEventHandler eventHandler;
    private NioSocketChannel channel;
    private TestSelectionKey selectionKey;
    private SocketChannelContext channelContext;
    private BiConsumer<Void, Exception> listener;
    private ByteBuffer[] buffers = {ByteBuffer.allocate(1)};
    private Selector rawSelector;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        super.setUp();
        rawSelector = mock(Selector.class);
        eventHandler = mock(SocketEventHandler.class);
        channel = mock(NioSocketChannel.class);
        channelContext = mock(SocketChannelContext.class);
        listener = mock(BiConsumer.class);
        selectionKey = new TestSelectionKey(0);
        selectionKey.attach(channelContext);

        this.socketSelector = new SocketSelector(eventHandler, rawSelector);
        this.socketSelector.setThread();

        when(channel.getContext()).thenReturn(channelContext);
        when(channelContext.isOpen()).thenReturn(true);
        when(channelContext.getSelector()).thenReturn(socketSelector);
        when(channelContext.getSelectionKey()).thenReturn(selectionKey);
        when(channelContext.isConnectComplete()).thenReturn(true);
    }

    public void testRegisterChannel() throws Exception {
        socketSelector.scheduleForRegistration(channel);

        socketSelector.preSelect();

        verify(eventHandler).handleRegistration(channelContext);
    }

    public void testClosedChannelWillNotBeRegistered() throws Exception {
        when(channelContext.isOpen()).thenReturn(false);
        socketSelector.scheduleForRegistration(channel);

        socketSelector.preSelect();

        verify(eventHandler).registrationException(same(channelContext), any(ClosedChannelException.class));
        verify(eventHandler, times(0)).handleConnect(channelContext);
    }

    public void testRegisterChannelFailsDueToException() throws Exception {
        socketSelector.scheduleForRegistration(channel);

        ClosedChannelException closedChannelException = new ClosedChannelException();
        doThrow(closedChannelException).when(eventHandler).handleRegistration(channelContext);

        socketSelector.preSelect();

        verify(eventHandler).registrationException(channelContext, closedChannelException);
        verify(eventHandler, times(0)).handleConnect(channelContext);
    }

    public void testSuccessfullyRegisterChannelWillAttemptConnect() throws Exception {
        socketSelector.scheduleForRegistration(channel);

        socketSelector.preSelect();

        verify(eventHandler).handleConnect(channelContext);
    }

    public void testQueueWriteWhenNotRunning() throws Exception {
        socketSelector.close();

        socketSelector.queueWrite(new FlushReadyWrite(channelContext, buffers, listener));

        verify(listener).accept(isNull(Void.class), any(ClosedSelectorException.class));
    }

    public void testQueueWriteChannelIsClosed() throws Exception {
        WriteOperation writeOperation = new FlushReadyWrite(channelContext, buffers, listener);
        socketSelector.queueWrite(writeOperation);

        when(channelContext.isOpen()).thenReturn(false);
        socketSelector.preSelect();

        verify(channelContext, times(0)).queueWriteOperation(writeOperation);
        verify(listener).accept(isNull(Void.class), any(ClosedChannelException.class));
    }

    public void testQueueWriteSelectionKeyThrowsException() throws Exception {
        SelectionKey selectionKey = mock(SelectionKey.class);

        WriteOperation writeOperation = new FlushReadyWrite(channelContext, buffers, listener);
        CancelledKeyException cancelledKeyException = new CancelledKeyException();
        socketSelector.queueWrite(writeOperation);

        when(channelContext.getSelectionKey()).thenReturn(selectionKey);
        when(selectionKey.interestOps(anyInt())).thenThrow(cancelledKeyException);
        socketSelector.preSelect();

        verify(channelContext, times(0)).queueWriteOperation(writeOperation);
        verify(listener).accept(null, cancelledKeyException);
    }

    public void testQueueWriteSuccessful() throws Exception {
        WriteOperation writeOperation = new FlushReadyWrite(channelContext, buffers, listener);
        socketSelector.queueWrite(writeOperation);

        assertTrue((selectionKey.interestOps() & SelectionKey.OP_WRITE) == 0);

        socketSelector.preSelect();

        verify(channelContext).queueWriteOperation(writeOperation);
        assertTrue((selectionKey.interestOps() & SelectionKey.OP_WRITE) != 0);
    }

    public void testQueueDirectlyInChannelBufferSuccessful() throws Exception {
        WriteOperation writeOperation = new FlushReadyWrite(channelContext, buffers, listener);

        assertTrue((selectionKey.interestOps() & SelectionKey.OP_WRITE) == 0);

        socketSelector.queueWriteInChannelBuffer(writeOperation);

        verify(channelContext).queueWriteOperation(writeOperation);
        assertTrue((selectionKey.interestOps() & SelectionKey.OP_WRITE) != 0);
    }

    public void testQueueDirectlyInChannelBufferSelectionKeyThrowsException() throws Exception {
        SelectionKey selectionKey = mock(SelectionKey.class);

        WriteOperation writeOperation = new FlushReadyWrite(channelContext, buffers, listener);
        CancelledKeyException cancelledKeyException = new CancelledKeyException();

        when(channelContext.getSelectionKey()).thenReturn(selectionKey);
        when(selectionKey.interestOps(anyInt())).thenThrow(cancelledKeyException);
        socketSelector.queueWriteInChannelBuffer(writeOperation);

        verify(channelContext, times(0)).queueWriteOperation(writeOperation);
        verify(listener).accept(null, cancelledKeyException);
    }

    public void testConnectEvent() throws Exception {
        selectionKey.setReadyOps(SelectionKey.OP_CONNECT);

        socketSelector.processKey(selectionKey);

        verify(eventHandler).handleConnect(channelContext);
    }

    public void testConnectEventFinishThrowException() throws Exception {
        IOException ioException = new IOException();

        selectionKey.setReadyOps(SelectionKey.OP_CONNECT);

        doThrow(ioException).when(eventHandler).handleConnect(channelContext);
        socketSelector.processKey(selectionKey);

        verify(eventHandler).connectException(channelContext, ioException);
    }

    public void testWillNotConsiderWriteOrReadUntilConnectionComplete() throws Exception {
        IOException ioException = new IOException();

        selectionKey.setReadyOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);

        doThrow(ioException).when(eventHandler).handleWrite(channelContext);

        when(channelContext.isConnectComplete()).thenReturn(false);
        socketSelector.processKey(selectionKey);

        verify(eventHandler, times(0)).handleWrite(channelContext);
        verify(eventHandler, times(0)).handleRead(channelContext);
    }

    public void testSuccessfulWriteEvent() throws Exception {
        selectionKey.setReadyOps(SelectionKey.OP_WRITE);

        socketSelector.processKey(selectionKey);

        verify(eventHandler).handleWrite(channelContext);
    }

    public void testWriteEventWithException() throws Exception {
        IOException ioException = new IOException();

        selectionKey.setReadyOps(SelectionKey.OP_WRITE);

        doThrow(ioException).when(eventHandler).handleWrite(channelContext);

        socketSelector.processKey(selectionKey);

        verify(eventHandler).writeException(channelContext, ioException);
    }

    public void testSuccessfulReadEvent() throws Exception {
        selectionKey.setReadyOps(SelectionKey.OP_READ);

        socketSelector.processKey(selectionKey);

        verify(eventHandler).handleRead(channelContext);
    }

    public void testReadEventWithException() throws Exception {
        IOException ioException = new IOException();

        selectionKey.setReadyOps(SelectionKey.OP_READ);

        doThrow(ioException).when(eventHandler).handleRead(channelContext);

        socketSelector.processKey(selectionKey);

        verify(eventHandler).readException(channelContext, ioException);
    }

    public void testWillCallPostHandleAfterChannelHandling() throws Exception {
        selectionKey.setReadyOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);

        socketSelector.processKey(selectionKey);

        verify(eventHandler).handleWrite(channelContext);
        verify(eventHandler).handleRead(channelContext);
        verify(eventHandler).postHandling(channelContext);
    }

    public void testCleanup() throws Exception {
        NioSocketChannel unregisteredChannel = mock(NioSocketChannel.class);
        SocketChannelContext unregisteredContext = mock(SocketChannelContext.class);
        when(unregisteredChannel.getContext()).thenReturn(unregisteredContext);

        socketSelector.scheduleForRegistration(channel);

        socketSelector.preSelect();

        socketSelector.queueWrite(new FlushReadyWrite(channelContext, buffers, listener));
        socketSelector.scheduleForRegistration(unregisteredChannel);

        TestSelectionKey testSelectionKey = new TestSelectionKey(0);
        testSelectionKey.attach(channelContext);
        when(rawSelector.keys()).thenReturn(new HashSet<>(Collections.singletonList(testSelectionKey)));

        socketSelector.cleanupAndCloseChannels();

        verify(listener).accept(isNull(Void.class), any(ClosedSelectorException.class));
        verify(eventHandler).handleClose(channelContext);
        verify(eventHandler).handleClose(unregisteredContext);
    }

    public void testExecuteListenerWillHandleException() throws Exception {
        RuntimeException exception = new RuntimeException();
        doThrow(exception).when(listener).accept(null, null);

        socketSelector.executeListener(listener, null);

        verify(eventHandler).listenerException(exception);
    }

    public void testExecuteFailedListenerWillHandleException() throws Exception {
        IOException ioException = new IOException();
        RuntimeException exception = new RuntimeException();
        doThrow(exception).when(listener).accept(null, ioException);

        socketSelector.executeFailedListener(listener, ioException);

        verify(eventHandler).listenerException(exception);
    }
}
