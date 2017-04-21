package org.elasticsearch.transport.nio;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.channel.DoNotRegisterChannel;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.elasticsearch.transport.nio.channel.SKUtils;
import org.elasticsearch.transport.nio.channel.TcpReadContext;
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

    private BiConsumer<NioSocketChannel, Throwable> exceptionHandler;

    private SocketEventHandler handler;
    private NioSocketChannel channel;
    private TcpReadContext readContext;
    private SocketChannel rawChannel;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpHandler() throws IOException {
        exceptionHandler = mock(BiConsumer.class);
        SocketSelector socketSelector = mock(SocketSelector.class);
        handler = new SocketEventHandler(logger, exceptionHandler);
        rawChannel = mock(SocketChannel.class);
        channel = new DoNotRegisterChannel("", rawChannel);
        readContext = mock(TcpReadContext.class);
        when(rawChannel.finishConnect()).thenReturn(true);

        channel.setReadContext(readContext);
        channel.setWriteContext(new TcpWriteContext(channel));
        channel.register(socketSelector);
        channel.finishConnect();

        when(socketSelector.onThread()).thenReturn(true);
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
        SKUtils.setConnectAndReadInterested(channel);
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
        when(readContext.read()).thenReturn(-1);

        handler.handleRead(channel);

        assertTrue(channel.isOpen());
        assertFalse(channel.isWritable());
        assertFalse(channel.isReadable());
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
        channel.getWriteContext().queueWriteOperations(new WriteOperation(channel, bytesArray, mock(ActionListener.class)));

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
        channel.getWriteContext().queueWriteOperations(new WriteOperation(channel, bytesArray, mock(ActionListener.class)));

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
        SKUtils.setConnectAndReadInterested(channel);
        SKUtils.removeConnectInterested(channel);
        SKUtils.setWriteInterested(channel);
    }

    public void testWriteExceptionCallsExceptionHandler() throws IOException {
        IOException exception = new IOException();
        handler.writeException(channel, exception);
        verify(exceptionHandler).accept(channel, exception);
    }
}
