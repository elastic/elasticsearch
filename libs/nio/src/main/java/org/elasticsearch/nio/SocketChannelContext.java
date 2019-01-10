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

import org.elasticsearch.common.concurrent.CompletableContext;
import org.elasticsearch.nio.utils.ExceptionsHelper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

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

    protected static final Predicate<NioSocketChannel> ALWAYS_ALLOW_CHANNEL = (c) -> true;

    protected final NioSocketChannel channel;
    protected final InboundChannelBuffer channelBuffer;
    protected final AtomicBoolean isClosing = new AtomicBoolean(false);
    private final ReadWriteHandler readWriteHandler;
    private final Predicate<NioSocketChannel> allowChannelPredicate;
    private final NioSelector selector;
    private final CompletableContext<Void> connectContext = new CompletableContext<>();
    private final LinkedList<FlushOperation> pendingFlushes = new LinkedList<>();
    private boolean closeNow;
    private Exception connectException;

    protected SocketChannelContext(NioSocketChannel channel, NioSelector selector, Consumer<Exception> exceptionHandler,
                                   ReadWriteHandler readWriteHandler, InboundChannelBuffer channelBuffer,
                                   Predicate<NioSocketChannel> allowChannelPredicate) {
        super(channel.getRawChannel(), exceptionHandler);
        this.selector = selector;
        this.channel = channel;
        this.readWriteHandler = readWriteHandler;
        this.channelBuffer = channelBuffer;
        this.allowChannelPredicate = allowChannelPredicate;
    }

    @Override
    public NioSelector getSelector() {
        return selector;
    }

    @Override
    public NioSocketChannel getChannel() {
        return channel;
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
        }
        return isConnected;
    }

    public void sendMessage(Object message, BiConsumer<Void, Exception> listener) {
        if (isClosing.get()) {
            listener.accept(null, new ClosedChannelException());
            return;
        }

        WriteOperation writeOperation = readWriteHandler.createWriteOperation(this, message, listener);

        NioSelector selector = getSelector();
        if (selector.isOnCurrentThread() == false) {
            selector.queueWrite(writeOperation);
            return;
        }

        selector.writeToChannel(writeOperation);
    }

    public void queueWriteOperation(WriteOperation writeOperation) {
        getSelector().assertOnSelectorThread();
        pendingFlushes.addAll(readWriteHandler.writeToBytes(writeOperation));
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
    protected void register() throws IOException {
        super.register();
        if (allowChannelPredicate.test(channel) == false) {
            closeNow = true;
        }
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
            pendingFlushes.addAll(readWriteHandler.pollFlushOperations());
            FlushOperation flushOperation;
            while ((flushOperation = pendingFlushes.pollFirst()) != null) {
                selector.executeFailedListener(flushOperation.getListener(), new ClosedChannelException());
            }

            try {
                readWriteHandler.close();
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
        while (bytesConsumed > 0 && channelBuffer.getIndex() > 0) {
            bytesConsumed = readWriteHandler.consumeReads(channelBuffer);
            channelBuffer.release(bytesConsumed);
        }

        // Some protocols might produce messages to flush during a read operation.
        pendingFlushes.addAll(readWriteHandler.pollFlushOperations());
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
        return closeNow;
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

    protected int readFromChannel(ByteBuffer buffer) throws IOException {
        ByteBuffer ioBuffer = getSelector().getIoBuffer();
        ioBuffer.limit(Math.min(buffer.remaining(), ioBuffer.limit()));
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
            buffer.put(ioBuffer);
            return bytesRead;
        }
    }

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
                copyBytes(ioBuffer, buffer);
            }
            channelBuffer.incrementIndex(bytesRead);
            return bytesRead;
        }
    }

    protected int flushToChannel(ByteBuffer buffer) throws IOException {
        int initialPosition = buffer.position();
        ByteBuffer ioBuffer = getSelector().getIoBuffer();
        copyBytes(buffer, ioBuffer);
        ioBuffer.flip();
        int bytesWritten;
        try {
            bytesWritten = rawChannel.write(ioBuffer);
        } catch (IOException e) {
            closeNow = true;
            buffer.position(initialPosition);
            throw e;
        }
        buffer.position(initialPosition + bytesWritten);
        return bytesWritten;
    }

    protected int flushToChannel(FlushOperation flushOperation) throws IOException {
        ByteBuffer ioBuffer = getSelector().getIoBuffer();

        boolean continueFlush = flushOperation.isFullyFlushed() == false;
        int totalBytesFlushed = 0;
        while (continueFlush) {
            ioBuffer.clear();
            int j = 0;
            ByteBuffer[] buffers = flushOperation.getBuffersToWrite();
            while (j < buffers.length && ioBuffer.remaining() > 0) {
                ByteBuffer buffer = buffers[j++];
                copyBytes(buffer, ioBuffer);
            }
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

    private void copyBytes(ByteBuffer from, ByteBuffer to) {
        int nBytesToCopy = Math.min(to.remaining(), from.remaining());
        int initialLimit = from.limit();
        from.limit(from.position() + nBytesToCopy);
        to.put(from);
        from.limit(initialLimit);
    }
}
