/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.nio.Config;
import org.elasticsearch.nio.FlushOperation;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioChannelHandler;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.nio.WriteOperation;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

/**
 * Provides a TLS/SSL read/write layer over a channel. This context will use a {@link SSLDriver} to handshake
 * with the peer channel. Once the handshake is complete, any data from the peer channel will be decrypted
 * before being passed to the {@link NioChannelHandler}. Outbound data will be encrypted before being flushed
 * to the channel.
 */
public final class SSLChannelContext extends SocketChannelContext {

    private static final long CLOSE_TIMEOUT_NANOS = new TimeValue(10, TimeUnit.SECONDS).nanos();
    private static final Runnable DEFAULT_TIMEOUT_CANCELLER = () -> {};

    private final SSLDriver sslDriver;
    private final InboundChannelBuffer networkReadBuffer;
    private final LinkedList<FlushOperation> encryptedFlushes = new LinkedList<>();
    private Runnable closeTimeoutCanceller = DEFAULT_TIMEOUT_CANCELLER;

    SSLChannelContext(
        NioSocketChannel channel,
        NioSelector selector,
        Config.Socket socketConfig,
        Consumer<Exception> exceptionHandler,
        SSLDriver sslDriver,
        NioChannelHandler readWriteHandler,
        InboundChannelBuffer applicationBuffer
    ) {
        this(
            channel,
            selector,
            socketConfig,
            exceptionHandler,
            sslDriver,
            readWriteHandler,
            InboundChannelBuffer.allocatingInstance(),
            applicationBuffer
        );
    }

    SSLChannelContext(
        NioSocketChannel channel,
        NioSelector selector,
        Config.Socket socketConfig,
        Consumer<Exception> exceptionHandler,
        SSLDriver sslDriver,
        NioChannelHandler readWriteHandler,
        InboundChannelBuffer networkReadBuffer,
        InboundChannelBuffer channelBuffer
    ) {
        super(channel, selector, socketConfig, exceptionHandler, readWriteHandler, channelBuffer);
        this.sslDriver = sslDriver;
        this.networkReadBuffer = networkReadBuffer;
    }

    @Override
    protected void channelActive() throws IOException {
        super.channelActive();
        sslDriver.init();
        SSLOutboundBuffer outboundBuffer = sslDriver.getOutboundBuffer();
        if (outboundBuffer.hasEncryptedBytesToFlush()) {
            encryptedFlushes.addLast(outboundBuffer.buildNetworkFlushOperation());
        }
    }

    @Override
    public void queueWriteOperation(WriteOperation writeOperation) {
        getSelector().assertOnSelectorThread();
        if (writeOperation instanceof CloseNotifyOperation) {
            try {
                sslDriver.initiateClose();
                SSLOutboundBuffer outboundBuffer = sslDriver.getOutboundBuffer();
                if (outboundBuffer.hasEncryptedBytesToFlush()) {
                    encryptedFlushes.addLast(outboundBuffer.buildNetworkFlushOperation());
                }
                long relativeNanos = CLOSE_TIMEOUT_NANOS + System.nanoTime();
                closeTimeoutCanceller = getSelector().getTaskScheduler().scheduleAtRelativeTime(this::channelCloseTimeout, relativeNanos);
            } catch (SSLException e) {
                handleException(e);
            }
        } else {
            super.queueWriteOperation(writeOperation);
        }
    }

    @Override
    public void flushChannel() throws IOException {
        if (closeNow()) {
            return;
        }
        // If there is currently data in the outbound write buffer, flush the buffer.
        if (pendingChannelFlush()) {
            // If the data is not completely flushed, exit. We cannot produce new write data until the
            // existing data has been fully flushed.
            flushEncryptedOperation();
            if (pendingChannelFlush()) {
                return;
            }
        }

        // If the driver is ready for application writes, we can attempt to proceed with any queued writes.
        FlushOperation unencryptedFlush;
        while (pendingChannelFlush() == false && (unencryptedFlush = getPendingFlush()) != null) {
            if (unencryptedFlush.isFullyFlushed()) {
                currentFlushOperationComplete();
            } else {
                try {
                    // Attempt to encrypt application write data. The encrypted data ends up in the
                    // outbound write buffer.
                    sslDriver.write(unencryptedFlush);
                    SSLOutboundBuffer outboundBuffer = sslDriver.getOutboundBuffer();
                    if (outboundBuffer.hasEncryptedBytesToFlush() == false) {
                        break;
                    }
                    encryptedFlushes.addLast(outboundBuffer.buildNetworkFlushOperation());
                    // Flush the write buffer to the channel
                    flushEncryptedOperation();
                } catch (IOException e) {
                    currentFlushOperationFailed(e);
                    throw e;
                }
            }
        }
    }

    private void flushEncryptedOperation() throws IOException {
        try {
            FlushOperation encryptedFlush = encryptedFlushes.getFirst();
            flushToChannel(encryptedFlush);
            if (encryptedFlush.isFullyFlushed()) {
                getSelector().executeListener(encryptedFlush.getListener(), null);
                encryptedFlushes.removeFirst();
            }
        } catch (IOException e) {
            getSelector().executeFailedListener(encryptedFlushes.removeFirst().getListener(), e);
            throw e;
        }
    }

    @Override
    public boolean readyForFlush() {
        getSelector().assertOnSelectorThread();
        if (sslDriver.readyForApplicationData()) {
            return pendingChannelFlush() || super.readyForFlush();
        } else {
            return pendingChannelFlush();
        }
    }

    @Override
    public int read() throws IOException {
        int bytesRead = 0;
        if (closeNow()) {
            return bytesRead;
        }
        bytesRead = readFromChannel(networkReadBuffer);
        if (bytesRead == 0) {
            return bytesRead;
        }

        sslDriver.read(networkReadBuffer, channelBuffer);

        handleReadBytes();
        // It is possible that a read call produced non-application bytes to flush
        SSLOutboundBuffer outboundBuffer = sslDriver.getOutboundBuffer();
        if (outboundBuffer.hasEncryptedBytesToFlush()) {
            encryptedFlushes.addLast(outboundBuffer.buildNetworkFlushOperation());
        }

        return bytesRead;
    }

    @Override
    public boolean selectorShouldClose() {
        return closeNow() || (sslDriver.isClosed() && pendingChannelFlush() == false);
    }

    @Override
    public void closeChannel() {
        if (isClosing.compareAndSet(false, true)) {
            // The model for closing channels will change at some point, removing the need for this "schedule
            // a write" signal. But for now, we need to handle the edge case where the channel is not
            // registered.
            if (getSelectionKey() == null) {
                getSelector().queueChannelClose(channel);
            } else {
                WriteOperation writeOperation = new CloseNotifyOperation(this);
                getSelector().queueWrite(writeOperation);
            }
        }
    }

    @Override
    public void closeFromSelector() throws IOException {
        getSelector().assertOnSelectorThread();
        if (channel.isOpen()) {
            closeTimeoutCanceller.run();
            for (FlushOperation encryptedFlush : encryptedFlushes) {
                getSelector().executeFailedListener(encryptedFlush.getListener(), new ClosedChannelException());
            }
            encryptedFlushes.clear();
            IOUtils.close(super::closeFromSelector, networkReadBuffer::close, sslDriver::close);
        }
    }

    public SSLEngine getSSLEngine() {
        return sslDriver.getSSLEngine();
    }

    private void channelCloseTimeout() {
        closeTimeoutCanceller = DEFAULT_TIMEOUT_CANCELLER;
        setCloseNow();
        getSelector().queueChannelClose(channel);
    }

    private boolean pendingChannelFlush() {
        return encryptedFlushes.isEmpty() == false;
    }

    private static class CloseNotifyOperation implements WriteOperation {

        private static final BiConsumer<Void, Exception> LISTENER = (v, t) -> {};
        private static final Object WRITE_OBJECT = new Object();
        private final SocketChannelContext channelContext;

        private CloseNotifyOperation(SocketChannelContext channelContext) {
            this.channelContext = channelContext;
        }

        @Override
        public BiConsumer<Void, Exception> getListener() {
            return LISTENER;
        }

        @Override
        public SocketChannelContext getChannel() {
            return channelContext;
        }

        @Override
        public Object getObject() {
            return WRITE_OBJECT;
        }
    }
}
