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
 * Producing writes for a channel is more complicated. The method {@link #needsNonApplicationWrite()} can be
 * called to determine if this driver needs to produce more data to advance the handshake or close process.
 * If that method returns true, {@link #nonApplicationWrite()} should be called (and the
 * data produced then flushed to the channel) until no further non-application writes are needed.
 *
 * If no non-application writes are needed, {@link #readyForApplicationWrites()} can be called to determine
 * if the driver is ready to consume application data. (Note: It is possible that
 * {@link #readyForApplicationWrites()} and {@link #needsNonApplicationWrite()} can both return false if the
 * driver is waiting on non-application data from the peer.) If the driver indicates it is ready for
 * application writes, {@link #write(FlushOperation)} can be called. This method will
 * encrypt flush operation application data and place it in the outbound buffer for flushing to a channel.
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
    private Mode currentMode = new HandshakeMode();
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
        if (currentMode.isHandshake()) {
            engine.beginHandshake();
            ((HandshakeMode) currentMode).startHandshake();
        } else {
            throw new AssertionError("Attempted to init outside from non-handshaking mode: " + currentMode.modeName());
        }
    }

    /**
     * Requests a TLS renegotiation. This means the we will request that the peer performs another handshake
     * prior to the continued exchange of application data. This can only be requested if we are currently in
     * APPLICATION mode.
     *
     * @throws SSLException if the handshake cannot be initiated
     */
    public void renegotiate() throws SSLException {
        if (currentMode.isApplication()) {
            currentMode = new HandshakeMode();
            engine.beginHandshake();
            ((HandshakeMode) currentMode).startHandshake();
        } else {
            throw new IllegalStateException("Attempted to renegotiate while in invalid mode: " + currentMode.modeName());
        }
    }

    public SSLEngine getSSLEngine() {
        return engine;
    }

    public boolean isHandshaking() {
        return currentMode.isHandshake();
    }

    public SSLOutboundBuffer getOutboundBuffer() {
        return outboundBuffer;
    }

    public void read(InboundChannelBuffer encryptedBuffer, InboundChannelBuffer applicationBuffer) throws SSLException {
        networkReadPage = pageAllocator.apply(packetSize);
        try {
            Mode modePriorToRead;
            do {
                modePriorToRead = currentMode;
                currentMode.read(encryptedBuffer, applicationBuffer);
                // It is possible that we received multiple SSL packets from the network since the last read.
                // If one of those packets causes us to change modes (such as finished handshaking), we need
                // to call read in the new mode to handle the remaining packets.
            } while (modePriorToRead != currentMode);
        } finally {
            networkReadPage.close();
            networkReadPage = null;
        }
    }

    public boolean readyForApplicationWrites() {
        return currentMode.isApplication();
    }

    public boolean needsNonApplicationWrite() {
        return currentMode.needsNonApplicationWrite();
    }

    public int write(FlushOperation applicationBytes) throws SSLException {
        return currentMode.write(applicationBytes);
    }

    public void nonApplicationWrite() throws SSLException {
        assert currentMode.isApplication() == false : "Should not be called if driver is in application mode";
        if (currentMode.isApplication() == false) {
            currentMode.write(EMPTY_FLUSH_OPERATION);
        } else {
            throw new AssertionError("Attempted to non-application write from invalid mode: " + currentMode.modeName());
        }
    }

    public void initiateClose() {
        closingInternal();
    }

    public boolean isClosed() {
        return currentMode.isClose() && ((CloseMode) currentMode).isCloseDone();
    }

    @Override
    public void close() throws SSLException {
        outboundBuffer.close();
        ArrayList<SSLException> closingExceptions = new ArrayList<>(2);
        closingInternal();
        CloseMode closeMode = (CloseMode) this.currentMode;
        if (closeMode.needToSendClose) {
            closingExceptions.add(new SSLException("Closed engine without completely sending the close alert message."));
            engine.closeOutbound();
        }

        if (closeMode.needToReceiveClose) {
            closingExceptions.add(new SSLException("Closed engine without receiving the close alert message."));
            closeMode.closeInboundAndSwallowPeerDidNotCloseException();
        }
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
                    closingInternal();
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
                    return result;
                case BUFFER_UNDERFLOW:
                    throw new IllegalStateException("Should not receive BUFFER_UNDERFLOW on WRAP");
                case BUFFER_OVERFLOW:
                    packetSize = engine.getSession().getPacketBufferSize();
                    // There is not enough space in the network buffer for an entire SSL packet. We will
                    // allocate a buffer with the correct packet size the next time through the loop.
                    break;
                case CLOSED:
                    assert result.bytesProduced() > 0 : "WRAP during close processing should produce close message.";
                    return result;
                default:
                    throw new IllegalStateException("Unexpected WRAP result: " + result.getStatus());
            }
        }
    }

    private void closingInternal() {
        // This check prevents us from attempting to send close_notify twice
        if (currentMode.isClose() == false) {
            currentMode = new CloseMode(currentMode.isHandshake());
        }
    }

    private void ensureApplicationBufferSize(InboundChannelBuffer applicationBuffer) {
        int applicationBufferSize = engine.getSession().getApplicationBufferSize();
        if (applicationBuffer.getRemaining() < applicationBufferSize) {
            applicationBuffer.ensureCapacity(applicationBuffer.getIndex() + engine.getSession().getApplicationBufferSize());
        }
    }

    // There are three potential modes for the driver to be in - HANDSHAKE, APPLICATION, or CLOSE. HANDSHAKE
    // is the initial mode. During this mode data that is read and written will be related to the TLS
    // handshake process. Application related data cannot be encrypted until the handshake is complete. From
    // HANDSHAKE mode the driver can transition to APPLICATION (if the handshake is successful) or CLOSE (if
    // an error occurs or we initiate a close). In APPLICATION mode data read from the channel will be
    // decrypted and placed into the buffer passed as an argument to the read call. Additionally, application
    // writes will be accepted and encrypted into the outbound write buffer. APPLICATION mode will proceed
    // until we receive a request for renegotiation (currently unsupported) or the CLOSE mode begins. CLOSE
    // mode can begin if we receive a CLOSE_NOTIFY message from the peer or if initiateClose is called. In
    // CLOSE mode we attempt to both send and receive an SSL CLOSE_NOTIFY message. The exception to this is
    // when we enter CLOSE mode from HANDSHAKE mode. In this scenario we only need to send the alert to the
    // peer and then close the channel. Some SSL/TLS implementations do not properly adhere to the full
    // two-direction close_notify process. Additionally, in newer TLS specifications it is not required to
    // wait to receive close_notify. However, we will make our best attempt to both send and receive as it is
    // expected by the java SSLEngine (it throws an exception if close_notify has not been received when
    // inbound is closed).

    private interface Mode {

        void read(InboundChannelBuffer encryptedBuffer, InboundChannelBuffer applicationBuffer) throws SSLException;

        int write(FlushOperation applicationBytes) throws SSLException;

        boolean needsNonApplicationWrite();

        boolean isHandshake();

        boolean isApplication();

        boolean isClose();

        String modeName();

    }

    private class HandshakeMode implements Mode {

        private SSLEngineResult.HandshakeStatus handshakeStatus;

        private void startHandshake() throws SSLException {
            handshakeStatus = engine.getHandshakeStatus();
            if (handshakeStatus != SSLEngineResult.HandshakeStatus.NEED_UNWRAP) {
                try {
                    handshake();
                } catch (SSLException e) {
                    closingInternal();
                    throw e;
                }
            }
        }

        private void handshake() throws SSLException {
            boolean continueHandshaking = true;
            while (continueHandshaking) {
                switch (handshakeStatus) {
                    case NEED_UNWRAP:
                        // We UNWRAP as much as possible immediately after a read. Do not need to do it here.
                        continueHandshaking = false;
                        break;
                    case NEED_WRAP:
                        handshakeStatus = wrap(outboundBuffer).getHandshakeStatus();
                        break;
                    case NEED_TASK:
                        runTasks();
                        handshakeStatus = engine.getHandshakeStatus();
                        break;
                    case NOT_HANDSHAKING:
                        maybeFinishHandshake();
                        continueHandshaking = false;
                        break;
                    case FINISHED:
                        maybeFinishHandshake();
                        continueHandshaking = false;
                        break;
                }
            }
        }

        @Override
        public void read(InboundChannelBuffer encryptedBuffer, InboundChannelBuffer applicationBuffer) throws SSLException {
            boolean continueUnwrap = true;
            while (continueUnwrap && encryptedBuffer.getIndex() > 0) {
                try {
                    SSLEngineResult result = unwrap(encryptedBuffer, applicationBuffer);
                    handshakeStatus = result.getHandshakeStatus();
                    handshake();
                    // If we are done handshaking we should exit the handshake read
                    continueUnwrap = result.bytesConsumed() > 0 && currentMode.isHandshake();
                } catch (SSLException e) {
                    closingInternal();
                    throw e;
                }
            }
        }

        @Override
        public int write(FlushOperation applicationBytes) throws SSLException {
            try {
                handshake();
            } catch (SSLException e) {
                closingInternal();
                throw e;
            }
            return 0;
        }

        @Override
        public boolean needsNonApplicationWrite() {
            return handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_WRAP
                || handshakeStatus == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING
                || handshakeStatus == SSLEngineResult.HandshakeStatus.FINISHED;
        }

        @Override
        public boolean isHandshake() {
            return true;
        }

        @Override
        public boolean isApplication() {
            return false;
        }

        @Override
        public boolean isClose() {
            return false;
        }

        @Override
        public String modeName() {
            return "HANDSHAKE";
        }

        private void runTasks() {
            Runnable delegatedTask;
            while ((delegatedTask = engine.getDelegatedTask()) != null) {
                delegatedTask.run();
            }
        }

        private void maybeFinishHandshake() {
            if (engine.isOutboundDone() || engine.isInboundDone()) {
                // If the engine is partially closed, immediate transition to close mode.
                if (currentMode.isHandshake()) {
                    currentMode = new CloseMode(true);
                } else if (currentMode.isApplication()) {
                    // It is possible to be in CLOSED mode if the prior UNWRAP call returned CLOSE_NOTIFY.
                    // However we should not be in application mode at this point.
                    String message = "Expected to be in handshaking/closed mode. Instead in application mode.";
                    throw new AssertionError(message);
                }
            } else {
                if (currentMode.isHandshake()) {
                    currentMode = new ApplicationMode();
                } else {
                    String message = "Attempted to transition to application mode from non-handshaking mode: " + currentMode;
                    throw new AssertionError(message);
                }
            }
        }
    }

    private class ApplicationMode implements Mode {

        @Override
        public void read(InboundChannelBuffer encryptedBuffer, InboundChannelBuffer applicationBuffer) throws SSLException {
            boolean continueUnwrap = true;
            while (continueUnwrap && encryptedBuffer.getIndex() > 0) {
                SSLEngineResult result = unwrap(encryptedBuffer, applicationBuffer);
                boolean renegotiationRequested = result.getStatus() != SSLEngineResult.Status.CLOSED
                    && maybeRenegotiation(result.getHandshakeStatus());
                continueUnwrap = result.bytesProduced() > 0 && renegotiationRequested == false;
            }
        }

        @Override
        public int write(FlushOperation applicationBytes) throws SSLException {
            boolean continueWrap = true;
            int totalBytesProduced = 0;
            while (continueWrap && applicationBytes.isFullyFlushed() == false) {
                SSLEngineResult result = wrap(outboundBuffer, applicationBytes);
                int bytesProduced = result.bytesProduced();
                totalBytesProduced += bytesProduced;
                boolean renegotiationRequested = maybeRenegotiation(result.getHandshakeStatus());
                continueWrap = bytesProduced > 0 && renegotiationRequested == false;
            }
            return totalBytesProduced;
        }

        private boolean maybeRenegotiation(SSLEngineResult.HandshakeStatus newStatus) throws SSLException {
            if (newStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING && newStatus != SSLEngineResult.HandshakeStatus.FINISHED) {
                renegotiate();
                return true;
            } else {
                return false;
            }
        }

        @Override
        public boolean needsNonApplicationWrite() {
            return false;
        }

        @Override
        public boolean isHandshake() {
            return false;
        }

        @Override
        public boolean isApplication() {
            return true;
        }

        @Override
        public boolean isClose() {
            return false;
        }

        @Override
        public String modeName() {
            return "APPLICATION";
        }
    }

    private class CloseMode implements Mode {

        private boolean needToSendClose = true;
        private boolean needToReceiveClose = true;

        private CloseMode(boolean isHandshaking) {
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
            }
        }

        @Override
        public void read(InboundChannelBuffer encryptedBuffer, InboundChannelBuffer applicationBuffer) throws SSLException {
            if (needToReceiveClose == false) {
                // There is an issue where receiving handshake messages after initiating the close process
                // can place the SSLEngine back into handshaking mode. In order to handle this, if we
                // initiate close during a handshake we do not wait to receive close. As we do not need to
                // receive close, we will not handle reads.
                return;
            }

            boolean continueUnwrap = true;
            while (continueUnwrap && encryptedBuffer.getIndex() > 0) {
                SSLEngineResult result = unwrap(encryptedBuffer, applicationBuffer);
                continueUnwrap = result.bytesProduced() > 0 || result.bytesConsumed() > 0;
            }
            if (engine.isInboundDone()) {
                needToReceiveClose = false;
            }
        }

        @Override
        public int write(FlushOperation applicationBytes) throws SSLException {
            int bytesProduced = 0;
            if (engine.isOutboundDone() == false) {
                bytesProduced += wrap(outboundBuffer).bytesProduced();
                if (engine.isOutboundDone()) {
                    needToSendClose = false;
                    // Close inbound if it is still open and we have decided not to wait for response.
                    if (needToReceiveClose == false && engine.isInboundDone() == false) {
                        closeInboundAndSwallowPeerDidNotCloseException();
                    }
                }
            } else {
                needToSendClose = false;
            }
            return bytesProduced;
        }

        @Override
        public boolean needsNonApplicationWrite() {
            return needToSendClose;
        }

        @Override
        public boolean isHandshake() {
            return false;
        }

        @Override
        public boolean isApplication() {
            return false;
        }

        @Override
        public boolean isClose() {
            return true;
        }

        @Override
        public String modeName() {
            return "CLOSE";
        }

        private boolean isCloseDone() {
            return needToSendClose == false && needToReceiveClose == false;
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
