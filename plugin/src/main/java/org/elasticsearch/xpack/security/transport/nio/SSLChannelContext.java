/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.nio.BytesWriteOperation;
import org.elasticsearch.nio.ChannelContext;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.SocketSelector;
import org.elasticsearch.nio.WriteOperation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * Provides a TLS/SSL read/write layer over a channel. This context will use a {@link SSLDriver} to handshake
 * with the peer channel. Once the handshake is complete, any data from the peer channel will be decrypted
 * before being passed to the {@link org.elasticsearch.nio.ChannelContext.ReadConsumer}. Outbound data will
 * be encrypted before being flushed to the channel.
 */
public final class SSLChannelContext implements ChannelContext {

    private final NioSocketChannel channel;
    private final LinkedList<BytesWriteOperation> queued = new LinkedList<>();
    private final SSLDriver sslDriver;
    private final ReadConsumer readConsumer;
    private final InboundChannelBuffer buffer;
    private final AtomicBoolean isClosing = new AtomicBoolean(false);
    private boolean peerClosed = false;
    private boolean ioException = false;

    SSLChannelContext(NioSocketChannel channel, SSLDriver sslDriver, ReadConsumer readConsumer, InboundChannelBuffer buffer) {
        this.channel = channel;
        this.sslDriver = sslDriver;
        this.readConsumer = readConsumer;
        this.buffer = buffer;
    }

    @Override
    public void channelRegistered() throws IOException {
        sslDriver.init();
    }

    @Override
    public void sendMessage(ByteBuffer[] buffers, BiConsumer<Void, Throwable> listener) {
        if (isClosing.get()) {
            listener.accept(null, new ClosedChannelException());
            return;
        }

        BytesWriteOperation writeOperation = new BytesWriteOperation(channel, buffers, listener);
        SocketSelector selector = channel.getSelector();
        if (selector.isOnCurrentThread() == false) {
            // If this message is being sent from another thread, we queue the write to be handled by the
            // network thread
            selector.queueWrite(writeOperation);
            return;
        }

        // TODO: Eval if we will allow writes from sendMessage
        selector.queueWriteInChannelBuffer(writeOperation);
    }

    @Override
    public void queueWriteOperation(WriteOperation writeOperation) {
        channel.getSelector().assertOnSelectorThread();
        if (writeOperation instanceof CloseNotifyOperation) {
            sslDriver.initiateClose();
        } else {
            queued.add((BytesWriteOperation) writeOperation);
        }
    }

    @Override
    public void flushChannel() throws IOException {
        if (ioException) {
            return;
        }
        // If there is currently data in the outbound write buffer, flush the buffer.
        if (sslDriver.hasFlushPending()) {
            internalFlush();
            // If the data is not completely flushed, exit. We cannot produce new write data until the
            // existing data has been fully flushed.
            if (sslDriver.hasFlushPending()) {
                return;
            }
        }

        // If the driver is ready for application writes, we can attempt to proceed with any queued writes.
        if (sslDriver.readyForApplicationWrites()) {
            BytesWriteOperation currentOperation = queued.peekFirst();
            while (sslDriver.hasFlushPending() == false && currentOperation != null) {
                // If the current operation has been fully consumed (encrypted) we now know that it has been
                // sent (as we only get to this point if the write buffer has been fully flushed).
                if (currentOperation.isFullyFlushed()) {
                    queued.removeFirst();
                    channel.getSelector().executeListener(currentOperation.getListener(), null);
                    currentOperation = queued.peekFirst();
                } else {
                    try {
                        // Attempt to encrypt application write data. The encrypted data ends up in the
                        // outbound write buffer.
                        int bytesEncrypted = sslDriver.applicationWrite(currentOperation.getBuffersToWrite());
                        if (bytesEncrypted == 0) {
                            break;
                        }
                        currentOperation.incrementIndex(bytesEncrypted);
                        // Flush the write buffer to the channel
                        internalFlush();
                    } catch (IOException e) {
                        queued.removeFirst();
                        channel.getSelector().executeFailedListener(currentOperation.getListener(), e);
                        throw e;
                    }
                }
            }
        } else {
            // We are not ready for application writes, check if the driver has non-application writes. We
            // only want to continue producing new writes if the outbound write buffer is fully flushed.
            while (sslDriver.hasFlushPending() == false && sslDriver.needsNonApplicationWrite()) {
                sslDriver.nonApplicationWrite();
                // If non-application writes were produced, flush the outbound write buffer.
                if (sslDriver.hasFlushPending()) {
                    internalFlush();
                }
            }
        }
    }

    private int internalFlush() throws IOException {
        try {
            return channel.write(sslDriver.getNetworkWriteBuffer());
        } catch (IOException e) {
            ioException = true;
            throw e;
        }
    }

    @Override
    public boolean hasQueuedWriteOps() {
        channel.getSelector().assertOnSelectorThread();
        if (sslDriver.readyForApplicationWrites()) {
            return sslDriver.hasFlushPending() || queued.isEmpty() == false;
        } else {
            return sslDriver.hasFlushPending() || sslDriver.needsNonApplicationWrite();
        }
    }

    @Override
    public int read() throws IOException {
        int bytesRead = 0;
        if (ioException) {
            return bytesRead;
        }
        try {
            bytesRead = channel.read(sslDriver.getNetworkReadBuffer());
        } catch (IOException e) {
            ioException = true;
            throw e;
        }
        if (bytesRead < 0) {
            peerClosed = true;
            return 0;
        }

        sslDriver.read(buffer);

        int bytesConsumed = Integer.MAX_VALUE;
        while (bytesConsumed > 0 && buffer.getIndex() > 0) {
            bytesConsumed = readConsumer.consumeReads(buffer);
            buffer.release(bytesConsumed);
        }

        return bytesRead;
    }

    @Override
    public boolean selectorShouldClose() {
        return peerClosed || ioException || sslDriver.isClosed();
    }

    @Override
    public void closeChannel() {
        if (isClosing.compareAndSet(false, true)) {
            WriteOperation writeOperation = new CloseNotifyOperation(channel);
            SocketSelector selector = channel.getSelector();
            if (selector.isOnCurrentThread() == false) {
                selector.queueWrite(writeOperation);
                return;
            }
            selector.queueWriteInChannelBuffer(writeOperation);
        }
    }

    @Override
    public void closeFromSelector() throws IOException {
        channel.getSelector().assertOnSelectorThread();
        // Set to true in order to reject new writes before queuing with selector
        isClosing.set(true);
        buffer.close();
        for (BytesWriteOperation op : queued) {
            channel.getSelector().executeFailedListener(op.getListener(), new ClosedChannelException());
        }
        queued.clear();
        sslDriver.close();
    }

    private static class CloseNotifyOperation implements WriteOperation {

        private static final BiConsumer<Void, Throwable> LISTENER = (v, t) -> {};
        private final NioSocketChannel channel;

        private CloseNotifyOperation(NioSocketChannel channel) {
            this.channel = channel;
        }

        @Override
        public BiConsumer<Void, Throwable> getListener() {
            return LISTENER;
        }

        @Override
        public NioSocketChannel getChannel() {
            return channel;
        }
    }
}
