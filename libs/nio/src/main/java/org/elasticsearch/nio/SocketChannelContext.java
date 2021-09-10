/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import org.elasticsearch.core.CompletableContext;
import org.elasticsearch.core.internal.net.NetUtils;
import org.elasticsearch.nio.utils.ByteBufferUtils;
import org.elasticsearch.nio.utils.ExceptionsHelper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * This context should implement the specific logic for a channel. When a channel receives a notification
 * that it is ready to perform certain operations (read, write, etc) the {@link SocketChannelContext} will
 * be called. This context will need to implement all protocol related logic. Additionally, if any special
 * close behavior is required, it should be implemented in this context.
 *
 * The only methods of the context that should ever be called from a non-selector thread are
 * {@link #closeChannel()} and {@link #sendMessage(Object, BiConsumer)}.
 */
public abstract class SocketChannelContext extends ChannelContext<SocketChannel> {

    protected final NioSocketChannel channel;
    protected final InboundChannelBuffer channelBuffer;
    protected final AtomicBoolean isClosing = new AtomicBoolean(false);
    private final NioChannelHandler channelHandler;
    private final NioSelector selector;
    private final Config.Socket socketConfig;
    private final CompletableContext<Void> connectContext = new CompletableContext<>();
    private final LinkedList<FlushOperation> pendingFlushes = new LinkedList<>();
    private boolean closeNow;
    private boolean socketOptionsSet;
    private Exception connectException;

    protected SocketChannelContext(NioSocketChannel channel, NioSelector selector, Config.Socket socketConfig,
                                   Consumer<Exception> exceptionHandler, NioChannelHandler channelHandler,
                                   InboundChannelBuffer channelBuffer) {
        super(channel.getRawChannel(), exceptionHandler);
        this.selector = selector;
        this.channel = channel;
        this.socketConfig = socketConfig;
        this.channelHandler = channelHandler;
        this.channelBuffer = channelBuffer;
    }

    @Override
    public NioSelector getSelector() {
        return selector;
    }

    @Override
    public NioSocketChannel getChannel() {
        return channel;
    }

    @Override
    protected void register() throws IOException {
        super.register();

        configureSocket(rawChannel.socket(), false);

        if (socketConfig.isAccepted() == false) {
            InetSocketAddress remoteAddress = socketConfig.getRemoteAddress();
            try {
                connect(rawChannel, remoteAddress);
            } catch (IOException e) {
                throw new IOException("Failed to initiate socket channel connection {remoteAddress=" + remoteAddress + "}.", e);
            }
        }
    }

    public void addConnectListener(BiConsumer<Void, Exception> listener) {
        connectContext.addListener(listener);
    }

    public boolean isConnectComplete() {
        return connectContext.isDone() && connectContext.isCompletedExceptionally() == false;
    }

    /**
     * This method will attempt to complete the connection process for this channel. It should be called for
     * new channels or for a channel that has produced a OP_CONNECT event. If this method returns true then
     * the connection is complete and the channel is ready for reads and writes. If it returns false, the
     * channel is not yet connected and this method should be called again when a OP_CONNECT event is
     * received.
     *
     * @return true if the connection process is complete
     * @throws IOException if an I/O error occurs
     */
    public boolean connect() throws IOException {
        if (isConnectComplete()) {
            return true;
        } else if (connectContext.isCompletedExceptionally()) {
            Exception exception = connectException;
            if (exception == null) {
                throw new AssertionError("Should have received connection exception");
            } else if (exception instanceof IOException) {
                throw (IOException) exception;
            } else {
                throw (RuntimeException) exception;
            }
        }

        boolean isConnected = rawChannel.isConnected();
        if (isConnected == false) {
            try {
                isConnected = rawChannel.finishConnect();
            } catch (IOException | RuntimeException e) {
                connectException = e;
                connectContext.completeExceptionally(e);
                throw e;
            }
        }
        if (isConnected) {
            connectContext.complete(null);
            configureSocket(rawChannel.socket(), true);
        }
        return isConnected;
    }

    public void sendMessage(Object message, BiConsumer<Void, Exception> listener) {
        if (isClosing.get()) {
            listener.accept(null, new ClosedChannelException());
            return;
        }

        WriteOperation writeOperation = channelHandler.createWriteOperation(this, message, listener);

        getSelector().queueWrite(writeOperation);
    }

    public void queueWriteOperation(WriteOperation writeOperation) {
        getSelector().assertOnSelectorThread();
        pendingFlushes.addAll(channelHandler.writeToBytes(writeOperation));
    }

    public abstract int read() throws IOException;

    public abstract void flushChannel() throws IOException;

    protected void currentFlushOperationFailed(IOException e) {
        FlushOperation flushOperation = pendingFlushes.pollFirst();
        getSelector().executeFailedListener(flushOperation.getListener(), e);
    }

    protected void currentFlushOperationComplete() {
        FlushOperation flushOperation = pendingFlushes.pollFirst();
        getSelector().executeListener(flushOperation.getListener(), null);
    }

    protected FlushOperation getPendingFlush() {
        return pendingFlushes.peekFirst();
    }

    @Override
    protected void channelActive() throws IOException {
        channelHandler.channelActive();
    }

    @Override
    public void closeFromSelector() throws IOException {
        getSelector().assertOnSelectorThread();
        if (isOpen()) {
            ArrayList<IOException> closingExceptions = new ArrayList<>(3);
            try {
                super.closeFromSelector();
            } catch (IOException e) {
                closingExceptions.add(e);
            }
            // Set to true in order to reject new writes before queuing with selector
            isClosing.set(true);

            // Poll for new flush operations to close
            pendingFlushes.addAll(channelHandler.pollFlushOperations());
            FlushOperation flushOperation;
            while ((flushOperation = pendingFlushes.pollFirst()) != null) {
                selector.executeFailedListener(flushOperation.getListener(), new ClosedChannelException());
            }

            try {
                channelHandler.close();
            } catch (IOException e) {
                closingExceptions.add(e);
            }
            channelBuffer.close();

            if (closingExceptions.isEmpty() == false) {
                ExceptionsHelper.rethrowAndSuppress(closingExceptions);
            }
        }
    }

    protected void handleReadBytes() throws IOException {
        int bytesConsumed = Integer.MAX_VALUE;
        while (isOpen() && bytesConsumed > 0 && channelBuffer.getIndex() > 0) {
            bytesConsumed = channelHandler.consumeReads(channelBuffer);
            channelBuffer.release(bytesConsumed);
        }

        // Some protocols might produce messages to flush during a read operation.
        pendingFlushes.addAll(channelHandler.pollFlushOperations());
    }

    public boolean readyForFlush() {
        getSelector().assertOnSelectorThread();
        return pendingFlushes.isEmpty() == false;
    }

    /**
     * This method indicates if a selector should close this channel.
     *
     * @return a boolean indicating if the selector should close
     */
    public abstract boolean selectorShouldClose();

    protected boolean closeNow() {
        return closeNow || channelHandler.closeNow();
    }

    protected void setCloseNow() {
        closeNow = true;
    }

    // When you read or write to a nio socket in java, the heap memory passed down must be copied to/from
    // direct memory. The JVM internally does some buffering of the direct memory, however we can save space
    // by reusing a thread-local direct buffer (provided by the NioSelector).
    //
    // Each network event loop is given a 64kb DirectByteBuffer. When we read we use this buffer and copy the
    // data after the read. When we go to write, we copy the data to the direct memory before calling write.
    // The choice of 64KB is rather arbitrary. We can explore different sizes in the future. However, any
    // data that is copied to the buffer for a write, but not successfully flushed immediately, must be
    // copied again on the next call.

    protected int readFromChannel(InboundChannelBuffer channelBuffer) throws IOException {
        ByteBuffer ioBuffer = getSelector().getIoBuffer();
        int bytesRead;
        try {
            bytesRead = rawChannel.read(ioBuffer);
        } catch (IOException e) {
            closeNow = true;
            throw e;
        }
        if (bytesRead < 0) {
            closeNow = true;
            return 0;
        } else {
            ioBuffer.flip();
            channelBuffer.ensureCapacity(channelBuffer.getIndex() + ioBuffer.remaining());
            ByteBuffer[] buffers = channelBuffer.sliceBuffersFrom(channelBuffer.getIndex());
            int j = 0;
            while (j < buffers.length && ioBuffer.remaining() > 0) {
                ByteBuffer buffer = buffers[j++];
                ByteBufferUtils.copyBytes(ioBuffer, buffer);
            }
            channelBuffer.incrementIndex(bytesRead);
            return bytesRead;
        }
    }

    // Currently we limit to 64KB. This is a trade-off which means more syscalls, in exchange for less
    // copying.
    private static final int WRITE_LIMIT = 1 << 16;

    protected int flushToChannel(FlushOperation flushOperation) throws IOException {
        ByteBuffer ioBuffer = getSelector().getIoBuffer();

        boolean continueFlush = flushOperation.isFullyFlushed() == false;
        int totalBytesFlushed = 0;
        while (continueFlush) {
            ioBuffer.clear();
            ioBuffer.limit(Math.min(WRITE_LIMIT, ioBuffer.limit()));
            ByteBuffer[] buffers = flushOperation.getBuffersToWrite(WRITE_LIMIT);
            ByteBufferUtils.copyBytes(buffers, ioBuffer);
            ioBuffer.flip();
            int bytesFlushed;
            try {
                bytesFlushed = rawChannel.write(ioBuffer);
            } catch (IOException e) {
                closeNow = true;
                throw e;
            }
            flushOperation.incrementIndex(bytesFlushed);
            totalBytesFlushed += bytesFlushed;
            continueFlush = ioBuffer.hasRemaining() == false && flushOperation.isFullyFlushed() == false;
        }
        return totalBytesFlushed;
    }

    private void configureSocket(Socket socket, boolean isConnectComplete) throws IOException {
        if (socketOptionsSet) {
            return;
        }

        try {
            // Set reuse address first as it must be set before a bind call. Some implementations throw
            // exceptions on other socket options if the channel is not connected. But setting reuse first,
            // we ensure that it is properly set before any bind attempt.
            socket.setReuseAddress(socketConfig.tcpReuseAddress());
            socket.setKeepAlive(socketConfig.tcpKeepAlive());
            if (socketConfig.tcpKeepAlive()) {
                final Set<SocketOption<?>> supportedOptions = socket.supportedOptions();
                if (socketConfig.tcpKeepIdle() >= 0) {
                    final SocketOption<Integer> keepIdleOption = NetUtils.getTcpKeepIdleSocketOptionOrNull();
                    if (keepIdleOption != null && supportedOptions.contains(keepIdleOption)) {
                        socket.setOption(keepIdleOption, socketConfig.tcpKeepIdle());
                    }
                }
                if (socketConfig.tcpKeepInterval() >= 0) {
                    final SocketOption<Integer> keepIntervalOption = NetUtils.getTcpKeepIntervalSocketOptionOrNull();
                    if (keepIntervalOption != null && supportedOptions.contains(keepIntervalOption)) {
                        socket.setOption(keepIntervalOption, socketConfig.tcpKeepInterval());
                    }
                }
                if (socketConfig.tcpKeepCount() >= 0) {
                    final SocketOption<Integer> keepCountOption = NetUtils.getTcpKeepCountSocketOptionOrNull();
                    if (keepCountOption != null && supportedOptions.contains(keepCountOption)) {
                        socket.setOption(keepCountOption, socketConfig.tcpKeepCount());
                    }
                }
            }
            NetUtils.tryEnsureReasonableKeepAliveConfig(socket.getChannel());
            socket.setTcpNoDelay(socketConfig.tcpNoDelay());
            int tcpSendBufferSize = socketConfig.tcpSendBufferSize();
            if (tcpSendBufferSize > 0) {
                socket.setSendBufferSize(tcpSendBufferSize);
            }
            int tcpReceiveBufferSize = socketConfig.tcpReceiveBufferSize();
            if (tcpReceiveBufferSize > 0) {
                socket.setReceiveBufferSize(tcpReceiveBufferSize);
            }
            socketOptionsSet = true;
        } catch (IOException e) {
            if (isConnectComplete) {
                throw e;
            }
            // Ignore if not connect complete. Some implementations fail on setting socket options if the
            // socket is not connected. We will try again after connection.
        }
    }

    private static void connect(SocketChannel socketChannel, InetSocketAddress remoteAddress) throws IOException {
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Boolean>) () -> socketChannel.connect(remoteAddress));
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }
}
