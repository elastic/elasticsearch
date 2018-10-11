/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.utils.ExceptionsHelper;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import java.nio.ByteBuffer;
import java.util.ArrayList;

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
 * {@link #read(InboundChannelBuffer)}. If the data is application data, it will be decrypted and placed into
 * the buffer passed as an argument. Otherwise, it will be consumed internally and advance the SSL/TLS close
 * or handshake process.
 *
 * Producing writes for a channel is more complicated. If there is existing data in the outbound write buffer
 * as indicated by {@link #hasFlushPending()}, that data must be written to the channel before more outbound
 * data can be produced. If no flushes are pending, {@link #needsNonApplicationWrite()} can be called to
 * determine if this driver needs to produce more data to advance the handshake or close process. If that
 * method returns true, {@link #nonApplicationWrite()} should be called (and the data produced then flushed
 * to the channel) until no further non-application writes are needed.
 *
 * If no non-application writes are needed, {@link #readyForApplicationWrites()} can be called to determine
 * if the driver is ready to consume application data. (Note: It is possible that
 * {@link #readyForApplicationWrites()} and {@link #needsNonApplicationWrite()} can both return false if the
 * driver is waiting on non-application data from the peer.) If the driver indicates it is ready for
 * application writes, {@link #applicationWrite(ByteBuffer[])} can be called. This method will encrypt
 * application data and place it in the write buffer for flushing to a channel.
 *
 * If you are ready to close the channel {@link #initiateClose()} should be called. After that is called, the
 * driver will start producing non-application writes related to notifying the peer connection that this
 * connection is closing. When {@link #isClosed()} returns true, this SSL connection is closed and the
 * channel should be closed.
 */
public class SSLDriver implements AutoCloseable {

    private static final ByteBuffer[] EMPTY_BUFFER_ARRAY = new ByteBuffer[0];

    private final SSLEngine engine;
    private final boolean isClientMode;
    // This should only be accessed by the network thread associated with this channel, so nothing needs to
    // be volatile.
    private Mode currentMode = new HandshakeMode();
    private ByteBuffer networkWriteBuffer;
    private ByteBuffer networkReadBuffer;

    public SSLDriver(SSLEngine engine, boolean isClientMode) {
        this.engine = engine;
        this.isClientMode = isClientMode;
        SSLSession session = engine.getSession();
        this.networkReadBuffer = ByteBuffer.allocate(session.getPacketBufferSize());
        this.networkWriteBuffer = ByteBuffer.allocate(session.getPacketBufferSize());
        this.networkWriteBuffer.position(this.networkWriteBuffer.limit());
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

    public boolean hasFlushPending() {
        return networkWriteBuffer.hasRemaining();
    }

    public boolean isHandshaking() {
        return currentMode.isHandshake();
    }

    public ByteBuffer getNetworkWriteBuffer() {
        return networkWriteBuffer;
    }

    public ByteBuffer getNetworkReadBuffer() {
        return networkReadBuffer;
    }

    public void read(InboundChannelBuffer buffer) throws SSLException {
        Mode modePriorToRead;
        do {
            modePriorToRead = currentMode;
            currentMode.read(buffer);
            // If we switched modes we want to read again as there might be unhandled bytes that need to be
            // handled by the new mode.
        } while (modePriorToRead != currentMode);
    }

    public boolean readyForApplicationWrites() {
        return currentMode.isApplication();
    }

    public boolean needsNonApplicationWrite() {
        return currentMode.needsNonApplicationWrite();
    }

    public int applicationWrite(ByteBuffer[] buffers) throws SSLException {
        assert readyForApplicationWrites() : "Should not be called if driver is not ready for application writes";
        return currentMode.write(buffers);
    }

    public void nonApplicationWrite() throws SSLException {
        assert currentMode.isApplication() == false : "Should not be called if driver is in application mode";
        if (currentMode.isApplication() == false) {
            currentMode.write(EMPTY_BUFFER_ARRAY);
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

    private SSLEngineResult unwrap(InboundChannelBuffer buffer) throws SSLException {
        while (true) {
            SSLEngineResult result = engine.unwrap(networkReadBuffer, buffer.sliceBuffersFrom(buffer.getIndex()));
            buffer.incrementIndex(result.bytesProduced());
            switch (result.getStatus()) {
                case OK:
                    networkReadBuffer.compact();
                    return result;
                case BUFFER_UNDERFLOW:
                    // There is not enough space in the network buffer for an entire SSL packet. Compact the
                    // current data and expand the buffer if necessary.
                    int currentCapacity = networkReadBuffer.capacity();
                    ensureNetworkReadBufferSize();
                    if (currentCapacity == networkReadBuffer.capacity()) {
                        networkReadBuffer.compact();
                    }
                    return result;
                case BUFFER_OVERFLOW:
                    // There is not enough space in the application buffer for the decrypted message. Expand
                    // the application buffer to ensure that it has enough space.
                    ensureApplicationBufferSize(buffer);
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

    private SSLEngineResult wrap(ByteBuffer[] buffers) throws SSLException {
        assert hasFlushPending() == false : "Should never called with pending writes";

        networkWriteBuffer.clear();
        while (true) {
            SSLEngineResult result;
            try {
                if (buffers.length == 1) {
                    result = engine.wrap(buffers[0], networkWriteBuffer);
                } else {
                    result = engine.wrap(buffers, networkWriteBuffer);
                }
            } catch (SSLException e) {
                networkWriteBuffer.position(networkWriteBuffer.limit());
                throw e;
            }

            switch (result.getStatus()) {
                case OK:
                    networkWriteBuffer.flip();
                    return result;
                case BUFFER_UNDERFLOW:
                    throw new IllegalStateException("Should not receive BUFFER_UNDERFLOW on WRAP");
                case BUFFER_OVERFLOW:
                    // There is not enough space in the network buffer for an entire SSL packet. Expand the
                    // buffer if it's smaller than the current session packet size. Otherwise return and wait
                    // for existing data to be flushed.
                    int currentCapacity = networkWriteBuffer.capacity();
                    ensureNetworkWriteBufferSize();
                    if (currentCapacity == networkWriteBuffer.capacity()) {
                        return result;
                    }
                    break;
                case CLOSED:
                    if (result.bytesProduced() > 0) {
                        networkWriteBuffer.flip();
                    } else {
                        assert false : "WRAP during close processing should produce close message.";
                    }
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

    private void ensureNetworkWriteBufferSize() {
        networkWriteBuffer = ensureNetBufferSize(networkWriteBuffer);
    }

    private void ensureNetworkReadBufferSize() {
        networkReadBuffer = ensureNetBufferSize(networkReadBuffer);
    }

    private ByteBuffer ensureNetBufferSize(ByteBuffer current) {
        int networkPacketSize = engine.getSession().getPacketBufferSize();
        if (current.capacity() < networkPacketSize) {
            ByteBuffer newBuffer = ByteBuffer.allocate(networkPacketSize);
            current.flip();
            newBuffer.put(current);
            return newBuffer;
        } else {
            return current;
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

        void read(InboundChannelBuffer buffer) throws SSLException;

        int write(ByteBuffer[] buffers) throws SSLException;

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
            if (handshakeStatus != SSLEngineResult.HandshakeStatus.NEED_UNWRAP &&
                handshakeStatus != SSLEngineResult.HandshakeStatus.NEED_WRAP) {
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
                        if (hasFlushPending() == false) {
                            handshakeStatus = wrap(EMPTY_BUFFER_ARRAY).getHandshakeStatus();
                        }
                        // If we need NEED_TASK we should run the tasks immediately
                        if (handshakeStatus != SSLEngineResult.HandshakeStatus.NEED_TASK) {
                            continueHandshaking = false;
                        }
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
        public void read(InboundChannelBuffer buffer) throws SSLException {
            ensureApplicationBufferSize(buffer);
            boolean continueUnwrap = true;
            while (continueUnwrap && networkReadBuffer.position() > 0) {
                networkReadBuffer.flip();
                try {
                    SSLEngineResult result = unwrap(buffer);
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
        public int write(ByteBuffer[] buffers) throws SSLException {
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
                } else {
                    String message = "Expected to be in handshaking mode. Instead in non-handshaking mode: " + currentMode;
                    throw new AssertionError(message);
                }
            } else if (hasFlushPending() == false) {
                // We only acknowledge that we are done handshaking if there are no bytes that need to be written
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
        public void read(InboundChannelBuffer buffer) throws SSLException {
            ensureApplicationBufferSize(buffer);
            boolean continueUnwrap = true;
            while (continueUnwrap && networkReadBuffer.position() > 0) {
                networkReadBuffer.flip();
                SSLEngineResult result = unwrap(buffer);
                boolean renegotiationRequested = result.getStatus() != SSLEngineResult.Status.CLOSED
                    && maybeRenegotiation(result.getHandshakeStatus());
                continueUnwrap = result.bytesProduced() > 0 && renegotiationRequested == false;
            }
        }

        @Override
        public int write(ByteBuffer[] buffers) throws SSLException {
            SSLEngineResult result = wrap(buffers);
            maybeRenegotiation(result.getHandshakeStatus());
            return result.bytesConsumed();
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
        public void read(InboundChannelBuffer buffer) throws SSLException {
            if (needToReceiveClose == false) {
                // There is an issue where receiving handshake messages after initiating the close process
                // can place the SSLEngine back into handshaking mode. In order to handle this, if we
                // initiate close during a handshake we do not wait to receive close. As we do not need to
                // receive close, we will not handle reads.
                return;
            }

            ensureApplicationBufferSize(buffer);
            boolean continueUnwrap = true;
            while (continueUnwrap && networkReadBuffer.position() > 0) {
                networkReadBuffer.flip();
                SSLEngineResult result = unwrap(buffer);
                continueUnwrap = result.bytesProduced() > 0 || result.bytesConsumed() > 0;
            }
            if (engine.isInboundDone()) {
                needToReceiveClose = false;
            }
        }

        @Override
        public int write(ByteBuffer[] buffers) throws SSLException {
            if (hasFlushPending() == false && engine.isOutboundDone()) {
                needToSendClose = false;
                // Close inbound if it is still open and we have decided not to wait for response.
                if (needToReceiveClose == false && engine.isInboundDone() == false) {
                    closeInboundAndSwallowPeerDidNotCloseException();
                }
            } else {
                wrap(EMPTY_BUFFER_ARRAY);
                assert hasFlushPending() : "Should have produced close message";
            }
            return 0;
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
