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
import org.elasticsearch.transport.nio.channel.DoNotRegisterChannel;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.elasticsearch.transport.nio.channel.ReadContext;
import org.elasticsearch.transport.nio.channel.SelectionKeyUtils;
import org.elasticsearch.transport.nio.channel.TcpWriteContext;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.function.BiConsumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SocketEventHandlerTests extends ESTestCase {

    private BiConsumer<NioSocketChannel, Exception> exceptionHandler;

    private SocketEventHandler handler;
    private NioSocketChannel channel;
    private ReadContext readContext;
    private SocketChannel rawChannel;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpHandler() throws IOException {
        exceptionHandler = mock(BiConsumer.class);
        SocketSelector socketSelector = mock(SocketSelector.class);
        handler = new SocketEventHandler(logger, exceptionHandler, mock(OpenChannels.class));
        rawChannel = mock(SocketChannel.class);
        channel = new DoNotRegisterChannel(rawChannel, socketSelector);
        readContext = mock(ReadContext.class);
        when(rawChannel.finishConnect()).thenReturn(true);

        channel.setContexts(readContext, new TcpWriteContext(channel));
        channel.register();
        channel.finishConnect();

        when(socketSelector.isOnCurrentThread()).thenReturn(true);
    }

    public void testRegisterAddsOP_CONNECTAndOP_READInterest() throws IOException {
        handler.handleRegistration(channel);
        assertEquals(SelectionKey.OP_READ | SelectionKey.OP_CONNECT, channel.getSelectionKey().interestOps());
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

    public void testHandleReadDelegatesToReadContext() throws IOException {
        when(readContext.read()).thenReturn(1);

        handler.handleRead(channel);

        verify(readContext).read();
    }

    public void testHandleReadMarksChannelForCloseIfPeerClosed() throws IOException {
        NioSocketChannel nioSocketChannel = mock(NioSocketChannel.class);
        when(nioSocketChannel.getReadContext()).thenReturn(readContext);
        when(readContext.read()).thenReturn(-1);

        handler.handleRead(nioSocketChannel);

        verify(nioSocketChannel).closeFromSelector();
    }

    public void testReadExceptionCallsExceptionHandler() throws IOException {
        IOException exception = new IOException();
        handler.readException(channel, exception);
        verify(exceptionHandler).accept(channel, exception);
    }

    @SuppressWarnings("unchecked")
    public void testHandleWriteWithCompleteFlushRemovesOP_WRITEInterest() throws IOException {
        SelectionKey selectionKey = channel.getSelectionKey();
        setWriteAndRead(channel);
        assertEquals(SelectionKey.OP_READ | SelectionKey.OP_WRITE, selectionKey.interestOps());

        BytesArray bytesArray = new BytesArray(new byte[1]);
        NetworkBytesReference networkBuffer = NetworkBytesReference.wrap(bytesArray);
        channel.getWriteContext().queueWriteOperations(new WriteOperation(channel, networkBuffer, mock(ActionListener.class)));

        when(rawChannel.write(ByteBuffer.wrap(bytesArray.array()))).thenReturn(1);
        handler.handleWrite(channel);

        assertEquals(SelectionKey.OP_READ, selectionKey.interestOps());
    }

    @SuppressWarnings("unchecked")
    public void testHandleWriteWithInCompleteFlushLeavesOP_WRITEInterest() throws IOException {
        SelectionKey selectionKey = channel.getSelectionKey();
        setWriteAndRead(channel);
        assertEquals(SelectionKey.OP_READ | SelectionKey.OP_WRITE, selectionKey.interestOps());

        BytesArray bytesArray = new BytesArray(new byte[1]);
        NetworkBytesReference networkBuffer = NetworkBytesReference.wrap(bytesArray, 1, 0);
        channel.getWriteContext().queueWriteOperations(new WriteOperation(channel, networkBuffer, mock(ActionListener.class)));

        when(rawChannel.write(ByteBuffer.wrap(bytesArray.array()))).thenReturn(0);
        handler.handleWrite(channel);

        assertEquals(SelectionKey.OP_READ | SelectionKey.OP_WRITE, selectionKey.interestOps());
    }

    public void testHandleWriteWithNoOpsRemovesOP_WRITEInterest() throws IOException {
        SelectionKey selectionKey = channel.getSelectionKey();
        setWriteAndRead(channel);
        assertEquals(SelectionKey.OP_READ | SelectionKey.OP_WRITE, channel.getSelectionKey().interestOps());

        handler.handleWrite(channel);

        assertEquals(SelectionKey.OP_READ, selectionKey.interestOps());
    }

    private void setWriteAndRead(NioChannel channel) {
        SelectionKeyUtils.setConnectAndReadInterested(channel);
        SelectionKeyUtils.removeConnectInterested(channel);
        SelectionKeyUtils.setWriteInterested(channel);
    }

    public void testWriteExceptionCallsExceptionHandler() throws IOException {
        IOException exception = new IOException();
        handler.writeException(channel, exception);
        verify(exceptionHandler).accept(channel, exception);
    }
}
