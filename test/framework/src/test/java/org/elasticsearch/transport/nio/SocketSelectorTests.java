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

package org.elasticsearch.transport.nio;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.elasticsearch.transport.nio.channel.WriteContext;
import org.elasticsearch.transport.nio.utils.TestSelectionKey;
import org.junit.Before;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Collections;
import java.util.HashSet;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
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
    private WriteContext writeContext;
    private ActionListener<Void> listener;
    private NetworkBytesReference bufferReference = NetworkBytesReference.wrap(new BytesArray(new byte[1]));
    private Selector rawSelector;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        super.setUp();
        eventHandler = mock(SocketEventHandler.class);
        channel = mock(NioSocketChannel.class);
        writeContext = mock(WriteContext.class);
        listener = mock(ActionListener.class);
        selectionKey = new TestSelectionKey(0);
        selectionKey.attach(channel);
        rawSelector = mock(Selector.class);

        this.socketSelector = new SocketSelector(eventHandler, rawSelector);
        this.socketSelector.setThread();

        when(channel.isOpen()).thenReturn(true);
        when(channel.getSelectionKey()).thenReturn(selectionKey);
        when(channel.getWriteContext()).thenReturn(writeContext);
        when(channel.isConnectComplete()).thenReturn(true);
        when(channel.getSelector()).thenReturn(socketSelector);
    }

    public void testRegisterChannel() throws Exception {
        socketSelector.scheduleForRegistration(channel);

        socketSelector.preSelect();

        verify(eventHandler).handleRegistration(channel);
    }

    public void testClosedChannelWillNotBeRegistered() throws Exception {
        when(channel.isOpen()).thenReturn(false);
        socketSelector.scheduleForRegistration(channel);

        socketSelector.preSelect();

        verify(eventHandler).registrationException(same(channel), any(ClosedChannelException.class));
        verify(channel, times(0)).finishConnect();
    }

    public void testRegisterChannelFailsDueToException() throws Exception {
        socketSelector.scheduleForRegistration(channel);

        ClosedChannelException closedChannelException = new ClosedChannelException();
        doThrow(closedChannelException).when(channel).register();

        socketSelector.preSelect();

        verify(eventHandler).registrationException(channel, closedChannelException);
        verify(channel, times(0)).finishConnect();
    }

    public void testSuccessfullyRegisterChannelWillConnect() throws Exception {
        socketSelector.scheduleForRegistration(channel);

        when(channel.finishConnect()).thenReturn(true);

        socketSelector.preSelect();

        verify(eventHandler).handleConnect(channel);
    }

    public void testConnectIncompleteWillNotNotify() throws Exception {
        socketSelector.scheduleForRegistration(channel);

        when(channel.finishConnect()).thenReturn(false);

        socketSelector.preSelect();

        verify(eventHandler, times(0)).handleConnect(channel);
    }

    public void testQueueWriteWhenNotRunning() throws Exception {
        socketSelector.close();

        socketSelector.queueWrite(new WriteOperation(channel, bufferReference, listener));

        verify(listener).onFailure(any(ClosedSelectorException.class));
    }

    public void testQueueWriteChannelIsNoLongerWritable() throws Exception {
        WriteOperation writeOperation = new WriteOperation(channel, bufferReference, listener);
        socketSelector.queueWrite(writeOperation);

        when(channel.isWritable()).thenReturn(false);
        socketSelector.preSelect();

        verify(writeContext, times(0)).queueWriteOperations(writeOperation);
        verify(listener).onFailure(any(ClosedChannelException.class));
    }

    public void testQueueWriteSelectionKeyThrowsException() throws Exception {
        SelectionKey selectionKey = mock(SelectionKey.class);

        WriteOperation writeOperation = new WriteOperation(channel, bufferReference, listener);
        CancelledKeyException cancelledKeyException = new CancelledKeyException();
        socketSelector.queueWrite(writeOperation);

        when(channel.isWritable()).thenReturn(true);
        when(channel.getSelectionKey()).thenReturn(selectionKey);
        when(selectionKey.interestOps(anyInt())).thenThrow(cancelledKeyException);
        socketSelector.preSelect();

        verify(writeContext, times(0)).queueWriteOperations(writeOperation);
        verify(listener).onFailure(cancelledKeyException);
    }

    public void testQueueWriteSuccessful() throws Exception {
        WriteOperation writeOperation = new WriteOperation(channel, bufferReference, listener);
        socketSelector.queueWrite(writeOperation);

        assertTrue((selectionKey.interestOps() & SelectionKey.OP_WRITE) == 0);

        when(channel.isWritable()).thenReturn(true);
        socketSelector.preSelect();

        verify(writeContext).queueWriteOperations(writeOperation);
        assertTrue((selectionKey.interestOps() & SelectionKey.OP_WRITE) != 0);
    }

    public void testQueueDirectlyInChannelBufferSuccessful() throws Exception {
        WriteOperation writeOperation = new WriteOperation(channel, bufferReference, listener);

        assertTrue((selectionKey.interestOps() & SelectionKey.OP_WRITE) == 0);

        when(channel.isWritable()).thenReturn(true);
        socketSelector.queueWriteInChannelBuffer(writeOperation);

        verify(writeContext).queueWriteOperations(writeOperation);
        assertTrue((selectionKey.interestOps() & SelectionKey.OP_WRITE) != 0);
    }

    public void testQueueDirectlyInChannelBufferSelectionKeyThrowsException() throws Exception {
        SelectionKey selectionKey = mock(SelectionKey.class);

        WriteOperation writeOperation = new WriteOperation(channel, bufferReference, listener);
        CancelledKeyException cancelledKeyException = new CancelledKeyException();

        when(channel.isWritable()).thenReturn(true);
        when(channel.getSelectionKey()).thenReturn(selectionKey);
        when(selectionKey.interestOps(anyInt())).thenThrow(cancelledKeyException);
        socketSelector.queueWriteInChannelBuffer(writeOperation);

        verify(writeContext, times(0)).queueWriteOperations(writeOperation);
        verify(listener).onFailure(cancelledKeyException);
    }

    public void testConnectEvent() throws Exception {
        selectionKey.setReadyOps(SelectionKey.OP_CONNECT);

        when(channel.finishConnect()).thenReturn(true);
        socketSelector.processKey(selectionKey);

        verify(eventHandler).handleConnect(channel);
    }

    public void testConnectEventFinishUnsuccessful() throws Exception {
        selectionKey.setReadyOps(SelectionKey.OP_CONNECT);

        when(channel.finishConnect()).thenReturn(false);
        socketSelector.processKey(selectionKey);

        verify(eventHandler, times(0)).handleConnect(channel);
    }

    public void testConnectEventFinishThrowException() throws Exception {
        IOException ioException = new IOException();

        selectionKey.setReadyOps(SelectionKey.OP_CONNECT);

        when(channel.finishConnect()).thenThrow(ioException);
        socketSelector.processKey(selectionKey);

        verify(eventHandler, times(0)).handleConnect(channel);
        verify(eventHandler).connectException(channel, ioException);
    }

    public void testWillNotConsiderWriteOrReadUntilConnectionComplete() throws Exception {
        IOException ioException = new IOException();

        selectionKey.setReadyOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);

        doThrow(ioException).when(eventHandler).handleWrite(channel);

        when(channel.isConnectComplete()).thenReturn(false);
        socketSelector.processKey(selectionKey);

        verify(eventHandler, times(0)).handleWrite(channel);
        verify(eventHandler, times(0)).handleRead(channel);
    }

    public void testSuccessfulWriteEvent() throws Exception {
        selectionKey.setReadyOps(SelectionKey.OP_WRITE);

        socketSelector.processKey(selectionKey);

        verify(eventHandler).handleWrite(channel);
    }

    public void testWriteEventWithException() throws Exception {
        IOException ioException = new IOException();

        selectionKey.setReadyOps(SelectionKey.OP_WRITE);

        doThrow(ioException).when(eventHandler).handleWrite(channel);

        socketSelector.processKey(selectionKey);

        verify(eventHandler).writeException(channel, ioException);
    }

    public void testSuccessfulReadEvent() throws Exception {
        selectionKey.setReadyOps(SelectionKey.OP_READ);

        socketSelector.processKey(selectionKey);

        verify(eventHandler).handleRead(channel);
    }

    public void testReadEventWithException() throws Exception {
        IOException ioException = new IOException();

        selectionKey.setReadyOps(SelectionKey.OP_READ);

        doThrow(ioException).when(eventHandler).handleRead(channel);

        socketSelector.processKey(selectionKey);

        verify(eventHandler).readException(channel, ioException);
    }

    public void testCleanup() throws Exception {
        NioSocketChannel unRegisteredChannel = mock(NioSocketChannel.class);

        socketSelector.scheduleForRegistration(channel);

        socketSelector.preSelect();

        NetworkBytesReference networkBuffer = NetworkBytesReference.wrap(new BytesArray(new byte[1]));
        socketSelector.queueWrite(new WriteOperation(mock(NioSocketChannel.class), networkBuffer, listener));
        socketSelector.scheduleForRegistration(unRegisteredChannel);

        TestSelectionKey testSelectionKey = new TestSelectionKey(0);
        testSelectionKey.attach(channel);
        when(rawSelector.keys()).thenReturn(new HashSet<>(Collections.singletonList(testSelectionKey)));

        socketSelector.cleanupAndCloseChannels();

        verify(listener).onFailure(any(ClosedSelectorException.class));
        verify(eventHandler).handleClose(channel);
        verify(eventHandler).handleClose(unRegisteredChannel);
    }
}
