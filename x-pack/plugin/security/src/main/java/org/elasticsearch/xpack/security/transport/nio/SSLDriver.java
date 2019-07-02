/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.nio.FlushOperation;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.Page;
import org.elasticsearch.nio.utils.ByteBufferUtils;
import org.elasticsearch.nio.utils.ExceptionsHelper;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.function.IntFunction;

/**
 * SSLDriver is a class that wraps the {@link SSLEngine} and attempts to simplify the API. The basic usage is
 * to create an SSLDriver class and call {@link #init()}. This initiates the SSL/TLS handshaking process.
 *
 * When the SSLDriver is handshaking or closing, reads and writes will be consumed/produced internally to
 * advance the handshake or close process. Alternatively, when the SSLDriver is in application mode, it will
 * decrypt data off the wire to be consumed by the application and will encrypt data provided by the
 * application to be written to the wire.
 *
 * Handling reads from a channel with this class is very simple. When data has been read, call
 * {@link #read(InboundChannelBuffer, InboundChannelBuffer)}. If the data is application data, it will be
 * decrypted and placed into the application buffer passed as an argument. Otherwise, it will be consumed
 * internally and advance the SSL/TLS close or handshake process.
 *
 * When the handshake begins, handshake data is read from the wire, or the channel close initiated, internal
 * bytes that need to be written will be produced. The bytes will be placed in the outbound buffer for
 * flushing to a channel.
 *
 * The method {@link #readyForApplicationData()} can be called to determine if the driver is ready to consume
 * application data. If the driver indicates it is ready for application writes,
 * {@link #write(FlushOperation)} can be called. This method will encrypt flush operation application data
 * and place it in the outbound buffer for flushing to a channel.
 *
 * If you are ready to close the channel {@link #initiateClose()} should be called. After that is called, the
 * driver will start producing non-application writes related to notifying the peer connection that this
 * connection is closing. When {@link #isClosed()} returns true, this SSL connection is closed and the
 * channel should be closed.
 */
public class SSLDriver implements AutoCloseable {

    private static final ByteBuffer[] EMPTY_BUFFERS = {ByteBuffer.allocate(0)};
    private static final FlushOperation EMPTY_FLUSH_OPERATION = new FlushOperation(EMPTY_BUFFERS, (r, t) -> {});

    private final SSLEngine engine;
    private final IntFunction<Page> pageAllocator;
    private final SSLOutboundBuffer outboundBuffer;
    private Page networkReadPage;
    private final boolean isClientMode;
    // This should only be accessed by the network thread associated with this channel, so nothing needs to
    // be volatile.
    private Mode currentMode = new RegularMode();
    private int packetSize;

    public SSLDriver(SSLEngine engine, IntFunction<Page> pageAllocator, boolean isClientMode) {
        this.engine = engine;
        this.pageAllocator = pageAllocator;
        this.outboundBuffer = new SSLOutboundBuffer(pageAllocator);
        this.isClientMode = isClientMode;
        SSLSession session = engine.getSession();
        packetSize = session.getPacketBufferSize();
    }

    public void init() throws SSLException {
        engine.setUseClientMode(isClientMode);
        if (currentMode.isClose()) {
            throw new AssertionError("Attempted to init outside from non-handshaking mode: " + currentMode.modeName());
        } else {
            engine.beginHandshake();
            ((RegularMode) currentMode).startHandshake();
            try {
                ((RegularMode) currentMode).handshake();
            } catch (SSLException e) {
                currentMode = new CloseMode(((RegularMode) currentMode).isHandshaking, false);
                throw e;
            }
        }
    }

    /**
     * Requests a TLS renegotiation. This means the we will request that the peer performs another handshake
     * prior to the continued exchange of application data. This can only be requested if we are currently
     * not closing.
     *
     * @throws SSLException if the handshake cannot be initiated
     */
    public void renegotiate() throws SSLException {
        if (currentMode.isClose() == false) {
            engine.beginHandshake();
            ((RegularMode) currentMode).startHandshake();
        } else {
            throw new IllegalStateException("Attempted to renegotiate while in invalid mode: " + currentMode.modeName());
        }
    }

    public SSLEngine getSSLEngine() {
        return engine;
    }

    public SSLOutboundBuffer getOutboundBuffer() {
        return outboundBuffer;
    }

    public void read(InboundChannelBuffer encryptedBuffer, InboundChannelBuffer applicationBuffer) throws SSLException {
        networkReadPage = pageAllocator.apply(packetSize);
        try {
            boolean continueUnwrap = true;
            while (continueUnwrap && encryptedBuffer.getIndex() > 0) {
                int bytesConsumed = currentMode.read(encryptedBuffer, applicationBuffer);
                continueUnwrap = bytesConsumed > 0;
            }
        } finally {
            networkReadPage.close();
            networkReadPage = null;
        }
    }

    public boolean readyForApplicationData() {
        return currentMode.readyForApplicationData();
    }

    public int write(FlushOperation applicationBytes) throws SSLException {
        int totalBytesProduced = 0;
        boolean continueWrap = true;
        while (continueWrap && applicationBytes.isFullyFlushed() == false) {
            int bytesProduced = currentMode.write(applicationBytes);
            totalBytesProduced += bytesProduced;
            continueWrap = bytesProduced > 0;
        }
        return totalBytesProduced;
    }

    public void initiateClose() throws SSLException {
        internalClose();
    }

    public boolean isClosed() {
        return currentMode.isClose() && ((CloseMode) currentMode).isCloseDone();
    }

    @Override
    public void close() throws SSLException {
        outboundBuffer.close();
        ArrayList<SSLException> closingExceptions = new ArrayList<>(2);
        if (currentMode.isClose() == false) {
            currentMode = new CloseMode(((RegularMode) currentMode).isHandshaking, false);
        }
        CloseMode closeMode = (CloseMode) this.currentMode;
        if (closeMode.needToSendClose) {
            closingExceptions.add(new SSLException("Closed engine without completely sending the close alert message."));
            engine.closeOutbound();
        }

        if (closeMode.needToReceiveClose) {
            closingExceptions.add(new SSLException("Closed engine without receiving the close alert message."));
        }
        closeMode.closeInboundAndSwallowPeerDidNotCloseException();
        ExceptionsHelper.rethrowAndSuppress(closingExceptions);
    }

    private SSLEngineResult unwrap(InboundChannelBuffer networkBuffer, InboundChannelBuffer applicationBuffer) throws SSLException {
        while (true) {
            ensureApplicationBufferSize(applicationBuffer);
            ByteBuffer networkReadBuffer = networkReadPage.byteBuffer();
            networkReadBuffer.clear();
            ByteBufferUtils.copyBytes(networkBuffer.sliceBuffersTo(Math.min(networkBuffer.getIndex(), packetSize)), networkReadBuffer);
            networkReadBuffer.flip();
            SSLEngineResult result = engine.unwrap(networkReadBuffer, applicationBuffer.sliceBuffersFrom(applicationBuffer.getIndex()));
            networkBuffer.release(result.bytesConsumed());
            applicationBuffer.incrementIndex(result.bytesProduced());
            switch (result.getStatus()) {
                case OK:
                    return result;
                case BUFFER_UNDERFLOW:
                    // There is not enough space in the network buffer for an entire SSL packet. Compact the
                    // current data and expand the buffer if necessary.
                    packetSize = engine.getSession().getPacketBufferSize();
                    if (networkReadPage.byteBuffer().capacity() < packetSize) {
                        networkReadPage.close();
                        networkReadPage = pageAllocator.apply(packetSize);
                    } else {
                        return result;
                    }
                    break;
                case BUFFER_OVERFLOW:
                    // There is not enough space in the application buffer for the decrypted message. Expand
                    // the application buffer to ensure that it has enough space.
                    ensureApplicationBufferSize(applicationBuffer);
                    break;
                case CLOSED:
                    assert engine.isInboundDone() : "We received close_notify so read should be done";
                    internalClose();
                    return result;
                default:
                    throw new IllegalStateException("Unexpected UNWRAP result: " + result.getStatus());
            }
        }
    }

    private SSLEngineResult wrap(SSLOutboundBuffer outboundBuffer) throws SSLException {
        return wrap(outboundBuffer, EMPTY_FLUSH_OPERATION);
    }

    private SSLEngineResult wrap(SSLOutboundBuffer outboundBuffer, FlushOperation applicationBytes) throws SSLException {
        ByteBuffer[] buffers = applicationBytes.getBuffersToWrite(engine.getSession().getApplicationBufferSize());
        while (true) {
            SSLEngineResult result;
            ByteBuffer networkBuffer = outboundBuffer.nextWriteBuffer(packetSize);
            try {
                result = engine.wrap(buffers, networkBuffer);
            } catch (SSLException e) {
                outboundBuffer.incrementEncryptedBytes(0);
                throw e;
            }

            outboundBuffer.incrementEncryptedBytes(result.bytesProduced());
            applicationBytes.incrementIndex(result.bytesConsumed());
            switch (result.getStatus()) {
                case OK:
                case CLOSED:
                    return result;
                case BUFFER_UNDERFLOW:
                    throw new IllegalStateException("Should not receive BUFFER_UNDERFLOW on WRAP");
                case BUFFER_OVERFLOW:
                    packetSize = engine.getSession().getPacketBufferSize();
                    // There is not enough space in the network buffer for an entire SSL packet. We will
                    // allocate a buffer with the correct packet size the next time through the loop.
                    break;
                default:
                    throw new IllegalStateException("Unexpected WRAP result: " + result.getStatus());
            }
        }
    }

    private void internalClose() throws SSLException {
        // This check prevents us from attempting to send close_notify twice
        if (currentMode.isClose() == false) {
            currentMode = new CloseMode(((RegularMode) currentMode).isHandshaking);
        }
    }

    private void ensureApplicationBufferSize(InboundChannelBuffer applicationBuffer) {
        int applicationBufferSize = engine.getSession().getApplicationBufferSize();
        if (applicationBuffer.getRemaining() < applicationBufferSize) {
            applicationBuffer.ensureCapacity(applicationBuffer.getIndex() + engine.getSession().getApplicationBufferSize());
        }
    }

    // There are two potential modes for the driver to be in - REGULAR or CLOSE. REGULAR is the initial mode.
    // During this mode the initial data that is read and written will be related to the TLS handshake
    // process. Application related data cannot be encrypted until the handshake is complete. Once the
    // handshake is complete data read from the channel will be decrypted and placed into the buffer passed
    // as an argument to the read call. Additionally, application writes will be accepted and encrypted into
    // the outbound write buffer. REGULAR mode will proceed until CLOSE mode begins. CLOSE mode can begin if
    // we receive a CLOSE_NOTIFY message from the peer or if initiateClose is called. In CLOSE mode we attempt
    // to both send and receive an SSL CLOSE_NOTIFY message. The exception to this is when we enter CLOSE mode
    // during a handshake. In this scenario we only need to send the alert to the peer and then close the
    // channel. Some SSL/TLS implementations do not properly adhere to the full two-direction close_notify
    // process. Additionally, in newer TLS specifications it is not required to wait to receive close_notify.
    // However, we will make our best attempt to both send and receive as it is expected by the java SSLEngine
    // (it throws an exception if close_notify has not been received when inbound is closed).

    private interface Mode {

        int read(InboundChannelBuffer encryptedBuffer, InboundChannelBuffer applicationBuffer) throws SSLException;

        int write(FlushOperation applicationBytes) throws SSLException;

        boolean isClose();

        boolean readyForApplicationData();

        String modeName();

    }

    private class RegularMode implements Mode {

        private SSLEngineResult.HandshakeStatus handshakeStatus;
        private boolean isHandshaking = false;

        private void startHandshake() {
            handshakeStatus = engine.getHandshakeStatus();
            isHandshaking = true;
        }

        private void handshake() throws SSLException {
            boolean continueHandshaking = true;
            while (continueHandshaking) {
                switch (handshakeStatus) {
                    case NEED_UNWRAP:
                        isHandshaking = true;
                        // We UNWRAP as much as possible immediately after a read. Do not need to do it here.
                        continueHandshaking = false;
                        break;
                    case NEED_WRAP:
                        isHandshaking = true;
                        handshakeStatus = wrap(outboundBuffer).getHandshakeStatus();
                        break;
                    case NEED_TASK:
                        runTasks();
                        isHandshaking = true;
                        handshakeStatus = engine.getHandshakeStatus();
                        break;
                    case NOT_HANDSHAKING:
                    case FINISHED:
                        isHandshaking = false;
                        continueHandshaking = false;
                        break;
                }
            }
        }

        @Override
        public int read(InboundChannelBuffer encryptedBuffer, InboundChannelBuffer applicationBuffer) throws SSLException {
            try {
                SSLEngineResult result = unwrap(encryptedBuffer, applicationBuffer);
                handshakeStatus = result.getHandshakeStatus();
                if (result.getStatus() != SSLEngineResult.Status.CLOSED) {
                    handshake();
                }
                return result.bytesConsumed();
            } catch (SSLException e) {
                handshakeStatus = engine.getHandshakeStatus();
                try {
                    internalClose();
                } catch (SSLException closeException) {
                    e.addSuppressed(closeException);
                }
                throw e;
            }
        }

        @Override
        public int write(FlushOperation applicationBytes) throws SSLException {
            SSLEngineResult result = wrap(outboundBuffer, applicationBytes);
            handshakeStatus = result.getHandshakeStatus();
            return result.bytesProduced();
        }

        @Override
        public boolean isClose() {
            return false;
        }

        @Override
        public boolean readyForApplicationData() {
            return isHandshaking == false;
        }

        @Override
        public String modeName() {
            return "REGULAR";
        }

        private void runTasks() {
            Runnable delegatedTask;
            while ((delegatedTask = engine.getDelegatedTask()) != null) {
                delegatedTask.run();
            }
        }
    }

    private class CloseMode implements Mode {

        private boolean needToSendClose = true;
        private boolean needToReceiveClose = true;

        private CloseMode(boolean isHandshaking) throws SSLException {
            this(isHandshaking, true);
        }

        private CloseMode(boolean isHandshaking, boolean tryToWrap) throws SSLException {
            if (isHandshaking && engine.isInboundDone() == false) {
                // If we attempt to close during a handshake either we are sending an alert and inbound
                // should already be closed or we are sending a close_notify. If we send a close_notify
                // the peer might send an handshake error alert. If we attempt to receive the handshake alert,
                // the engine will throw an IllegalStateException as it is not in a proper state to receive
                // handshake message. Closing inbound immediately after close_notify is the cleanest option.
                needToReceiveClose = false;
            } else if (engine.isInboundDone()) {
                needToReceiveClose = false;
            }
            if (engine.isOutboundDone()) {
                needToSendClose = false;
            } else {
                engine.closeOutbound();
                if (tryToWrap) {
                    try {
                        boolean continueWrap = true;
                        while (continueWrap) {
                            continueWrap = wrap(outboundBuffer).getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP;
                        }
                    } finally {
                        needToSendClose = false;
                    }
                }
            }
        }

        @Override
        public int read(InboundChannelBuffer encryptedBuffer, InboundChannelBuffer applicationBuffer) throws SSLException {
            if (needToReceiveClose == false) {
                // There is an issue where receiving handshake messages after initiating the close process
                // can place the SSLEngine back into handshaking mode. In order to handle this, if we
                // initiate close during a handshake we do not wait to receive close. As we do not need to
                // receive close, we will not handle reads.
                return 0;
            }

            SSLEngineResult result = unwrap(encryptedBuffer, applicationBuffer);
            if (engine.isInboundDone()) {
                needToReceiveClose = false;
            }

            return result.bytesConsumed();
        }

        @Override
        public int write(FlushOperation applicationBytes) {
            return 0;
        }

        @Override
        public boolean isClose() {
            return true;
        }

        @Override
        public boolean readyForApplicationData() {
            return false;
        }

        @Override
        public String modeName() {
            return "CLOSE";
        }

        private boolean isCloseDone() {
            // We do as much as possible to generate the outbound messages in the ctor. At this point, we are
            // only interested in interrogating if we need to wait to receive the close message.
            return needToReceiveClose == false;
        }

        private void closeInboundAndSwallowPeerDidNotCloseException() throws SSLException {
            try {
                engine.closeInbound();
            } catch (SSLException e) {
                if (e.getMessage().contains("before receiving peer's close_notify") == false) {
                    throw e;
                }
            }
        }
    }
}
