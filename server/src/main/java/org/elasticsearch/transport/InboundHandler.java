/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.HandlingTimeTracker;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Handles inbound messages by first deserializing a {@link TransportRequest} or {@link TransportResponse} from an {@link InboundMessage}
 * and then passing it to the appropriate handler.
 */
public class InboundHandler {

    private static final Logger logger = LogManager.getLogger(InboundHandler.class);

    private final ThreadPool threadPool;
    private final OutboundHandler outboundHandler;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final TransportHandshaker handshaker;
    private final TransportKeepAlive keepAlive;
    private final Transport.ResponseHandlers responseHandlers;
    private final Transport.RequestHandlers requestHandlers;
    private final HandlingTimeTracker handlingTimeTracker;
    private final boolean ignoreDeserializationErrors;

    private volatile TransportMessageListener messageListener = TransportMessageListener.NOOP_LISTENER;

    private volatile long slowLogThresholdMs = Long.MAX_VALUE;

    InboundHandler(
        ThreadPool threadPool,
        OutboundHandler outboundHandler,
        NamedWriteableRegistry namedWriteableRegistry,
        TransportHandshaker handshaker,
        TransportKeepAlive keepAlive,
        Transport.RequestHandlers requestHandlers,
        Transport.ResponseHandlers responseHandlers,
        HandlingTimeTracker handlingTimeTracker,
        boolean ignoreDeserializationErrors
    ) {
        this.threadPool = threadPool;
        this.outboundHandler = outboundHandler;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.handshaker = handshaker;
        this.keepAlive = keepAlive;
        this.requestHandlers = requestHandlers;
        this.responseHandlers = responseHandlers;
        this.handlingTimeTracker = handlingTimeTracker;
        this.ignoreDeserializationErrors = ignoreDeserializationErrors;
    }

    void setMessageListener(TransportMessageListener listener) {
        if (messageListener == TransportMessageListener.NOOP_LISTENER) {
            messageListener = listener;
        } else {
            throw new IllegalStateException("Cannot set message listener twice");
        }
    }

    void setSlowLogThreshold(TimeValue slowLogThreshold) {
        this.slowLogThresholdMs = slowLogThreshold.getMillis();
    }

    /**
     * @param message the transport message received, guaranteed to be closed by this method if it returns without exception.
     *                Callers must ensure that {@code message} is closed if this method throws an exception but must not release
     *                the message themselves otherwise
     */
    void inboundMessage(TcpChannel channel, InboundMessage message) throws Exception {
        final long startTime = threadPool.rawRelativeTimeInMillis();
        channel.getChannelStats().markAccessed(startTime);
        TransportLogger.logInboundMessage(channel, message);

        if (message.isPing()) {
            keepAlive.receiveKeepAlive(channel); // pings hold no resources, no need to close
        } else {
            messageReceived(channel, /* autocloses absent exception */ message, startTime);
        }
    }

    // Empty stream constant to avoid instantiating a new stream for empty messages.
    private static final StreamInput EMPTY_STREAM_INPUT = new ByteBufferStreamInput(ByteBuffer.wrap(BytesRef.EMPTY_BYTES));

    /**
     * @param message the transport message received, guaranteed to be closed by this method if it returns without exception.
     *                Callers must ensure that {@code message} is closed if this method throws an exception but must not release
     *                the message themselves otherwise
     */
    private void messageReceived(TcpChannel channel, InboundMessage message, long startTime) throws IOException {
        final InetSocketAddress remoteAddress = channel.getRemoteAddress();
        final Header header = message.getHeader();
        assert header.needsToReadVariableHeader() == false;

        TransportResponseHandler<?> responseHandler = null;
        ThreadContext threadContext = threadPool.getThreadContext();
        assert threadContext.isDefaultContext();
        try (var ignored = threadContext.newStoredContext()) {
            // Place the context with the headers from the message
            threadContext.setHeaders(header.getHeaders());
            threadContext.putTransient("_remote_address", remoteAddress);
            if (header.isRequest()) {
                handleRequest(channel, /* autocloses absent exception */ message);
            } else {
                // Responses do not support short circuiting currently
                assert message.isShortCircuit() == false;
                responseHandler = findResponseHandler(header);
                // ignore if its null, the service logs it
                if (responseHandler != null) {
                    executeResponseHandler( /* autocloses absent exception */ message, responseHandler, remoteAddress);
                } else {
                    message.close();
                }
            }
        } finally {
            final long took = threadPool.rawRelativeTimeInMillis() - startTime;
            handlingTimeTracker.addObservation(took);
            final long logThreshold = slowLogThresholdMs;
            if (logThreshold > 0 && took > logThreshold) {
                logSlowMessage(message, took, logThreshold, responseHandler);
            }
        }
    }

    /**
     * @param message the transport message received, guaranteed to be closed by this method if it returns without exception.
     *                Callers must ensure that {@code message} is closed if this method throws an exception but must not release
     *                the message themselves otherwise
     */
    private void executeResponseHandler(
        InboundMessage message,
        TransportResponseHandler<?> responseHandler,
        InetSocketAddress remoteAddress
    ) throws IOException {
        final var header = message.getHeader();
        if (message.getContentLength() > 0 || header.getVersion().equals(TransportVersion.current()) == false) {
            final StreamInput streamInput = namedWriteableStream(message.openOrGetStreamInput());
            assert assertRemoteVersion(streamInput, header.getVersion());
            if (header.isError()) {
                handlerResponseError(streamInput, /* autocloses */ message, responseHandler);
            } else {
                handleResponse(remoteAddress, streamInput, responseHandler, /* autocloses */ message);
            }
        } else {
            assert header.isError() == false;
            handleResponse(remoteAddress, EMPTY_STREAM_INPUT, responseHandler, /* autocloses */ message);
        }
    }

    private TransportResponseHandler<?> findResponseHandler(Header header) {
        if (header.isHandshake()) {
            return handshaker.removeHandlerForHandshake(header.getRequestId());
        }
        final TransportResponseHandler<? extends TransportResponse> theHandler = responseHandlers.onResponseReceived(
            header.getRequestId(),
            messageListener
        );
        if (theHandler == null && header.isError()) {
            return handshaker.removeHandlerForHandshake(header.getRequestId());
        } else {
            return theHandler;
        }
    }

    private static void logSlowMessage(InboundMessage message, long took, long logThreshold, TransportResponseHandler<?> responseHandler) {
        if (message.getHeader().isRequest()) {
            logger.warn("""
                handling request [{}] took [{}ms] which is above the warn threshold of [{}ms]; \
                for more information, see {}""", message, took, logThreshold, ReferenceDocs.NETWORK_THREADING_MODEL);
        } else {
            logger.warn("""
                handling response [{}] on handler [{}] took [{}ms] which is above the warn threshold of [{}ms]; \
                for more information, see {}""", message, responseHandler, took, logThreshold, ReferenceDocs.NETWORK_THREADING_MODEL);
        }
    }

    private void verifyRequestReadFully(StreamInput stream, long requestId, String action) throws IOException {
        final int nextByte = stream.read();
        // calling read() is useful to make sure the message is fully read, even if there some kind of EOS marker
        if (nextByte != -1) {
            final IllegalStateException exception = new IllegalStateException(
                "Message not fully read (request) for requestId ["
                    + requestId
                    + "], action ["
                    + action
                    + "], available ["
                    + stream.available()
                    + "]; resetting"
            );
            assert ignoreDeserializationErrors : exception;
            throw exception;
        }
    }

    private void verifyResponseReadFully(Header header, TransportResponseHandler<?> responseHandler, StreamInput streamInput)
        throws IOException {
        // Check the entire message has been read
        final int nextByte = streamInput.read();
        // calling read() is useful to make sure the message is fully read, even if there is an EOS marker
        if (nextByte != -1) {
            final IllegalStateException exception = new IllegalStateException(
                "Message not fully read (response) for requestId ["
                    + header.getRequestId()
                    + "], handler ["
                    + responseHandler
                    + "], error ["
                    + header.isError()
                    + "]; resetting"
            );
            assert ignoreDeserializationErrors : exception;
            throw exception;
        }
    }

    /**
     * @param message the transport message received, guaranteed to be closed by this method if it returns without exception.
     *                Callers must ensure that {@code message} is closed if this method throws an exception but must not release
     *                the message themselves otherwise
     */
    private <T extends TransportRequest> void handleRequest(TcpChannel channel, InboundMessage message) throws IOException {
        final Header header = message.getHeader();
        if (header.isHandshake()) {
            handleHandshakeRequest(channel, /* autocloses */ message);
            return;
        }

        final String action = header.getActionName();
        final long requestId = header.getRequestId();
        final RequestHandlerRegistry<T> reg = requestHandlers.getHandler(action);
        assert message.isShortCircuit() || reg != null : action;
        final TransportChannel transportChannel = new TcpTransportChannel(
            outboundHandler,
            channel,
            action,
            requestId,
            header.getVersion(),
            header.getCompressionScheme(),
            reg == null ? ResponseStatsConsumer.NONE : reg,
            false,
            Releasables.assertOnce(message.takeBreakerReleaseControl())
        );

        try {
            messageListener.onRequestReceived(requestId, action);
            if (reg != null) {
                reg.addRequestStats(header.getNetworkMessageSize() + TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE);
            }

            if (message.isShortCircuit()) {
                sendErrorResponse(action, transportChannel, message.getException());
                return;
            }

            assert reg != null;
            final StreamInput stream = namedWriteableStream(message.openOrGetStreamInput());
            assert assertRemoteVersion(stream, header.getVersion());
            T request;
            try {
                request = reg.newRequest(stream);
                message.close();
                message = null;
            } catch (Exception e) {
                assert ignoreDeserializationErrors : e;
                throw e;
            }
            try {
                request.remoteAddress(channel.getRemoteAddress());
                assert requestId > 0;
                request.setRequestId(requestId);
                verifyRequestReadFully(stream, requestId, action);
                if (reg.getExecutor() == EsExecutors.DIRECT_EXECUTOR_SERVICE) {
                    try (var ignored = threadPool.getThreadContext().newTraceContext()) {
                        doHandleRequest(reg, request, transportChannel);
                    }
                } else {
                    handleRequestForking(/* autocloses */ request, reg, transportChannel);
                    request = null; // now owned by the thread we forked to
                }
            } finally {
                if (request != null) {
                    request.decRef();
                }
            }
        } catch (Exception e) {
            sendErrorResponse(action, transportChannel, e);
        } finally {
            if (message != null) {
                message.close();
            }
        }
    }

    private static <T extends TransportRequest> void doHandleRequest(RequestHandlerRegistry<T> reg, T request, TransportChannel channel) {
        try {
            reg.processMessageReceived(request, channel);
        } catch (Exception e) {
            sendErrorResponse(reg.getAction(), channel, e);
        }
    }

    private <T extends TransportRequest> void handleRequestForking(T request, RequestHandlerRegistry<T> reg, TransportChannel channel) {
        boolean success = false;
        try {
            reg.getExecutor().execute(threadPool.getThreadContext().preserveContextWithTracing(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    doHandleRequest(reg, request, channel);
                }

                @Override
                public boolean isForceExecution() {
                    return reg.isForceExecution();
                }

                @Override
                public void onRejection(Exception e) {
                    sendErrorResponse(reg.getAction(), channel, e);
                }

                @Override
                public void onFailure(Exception e) {
                    assert false : e; // shouldn't get here ever, no failures other than rejection by the thread-pool expected
                    sendErrorResponse(reg.getAction(), channel, e);
                }

                @Override
                public void onAfter() {
                    request.decRef();
                }
            }));
            success = true;
        } finally {
            if (success == false) {
                request.decRef();
            }
        }
    }

    /**
     * @param message guaranteed to get closed by this method
     */
    private void handleHandshakeRequest(TcpChannel channel, InboundMessage message) throws IOException {
        var header = message.getHeader();
        assert header.actionName.equals(TransportHandshaker.HANDSHAKE_ACTION_NAME);
        final long requestId = header.getRequestId();
        messageListener.onRequestReceived(requestId, TransportHandshaker.HANDSHAKE_ACTION_NAME);
        // Cannot short circuit handshakes
        assert message.isShortCircuit() == false;
        final StreamInput stream = namedWriteableStream(message.openOrGetStreamInput());
        assert assertRemoteVersion(stream, header.getVersion());
        final TransportChannel transportChannel = new TcpTransportChannel(
            outboundHandler,
            channel,
            TransportHandshaker.HANDSHAKE_ACTION_NAME,
            requestId,
            header.getVersion(),
            header.getCompressionScheme(),
            ResponseStatsConsumer.NONE,
            true,
            Releasables.assertOnce(message.takeBreakerReleaseControl())
        );
        try (message) {
            handshaker.handleHandshake(transportChannel, requestId, stream);
        } catch (Exception e) {
            logger.warn(
                () -> "error processing handshake version [" + header.getVersion() + "] received on [" + channel + "], closing channel",
                e
            );
            channel.close();
        }
    }

    private static void sendErrorResponse(String actionName, TransportChannel transportChannel, Exception e) {
        try {
            transportChannel.sendResponse(e);
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.warn(() -> "Failed to send error message back to client for action [" + actionName + "]", inner);
        }
    }

    /**
     * @param message guaranteed to get closed by this method
     */
    private <T extends TransportResponse> void handleResponse(
        InetSocketAddress remoteAddress,
        final StreamInput stream,
        final TransportResponseHandler<T> handler,
        final InboundMessage message
    ) {
        final var executor = handler.executor();
        if (executor == EsExecutors.DIRECT_EXECUTOR_SERVICE) {
            // no need to provide a buffer release here, we never escape the buffer when handling directly
            doHandleResponse(handler, remoteAddress, stream, /* autocloses */ message);
        } else {
            // release buffer once we deserialize the message, but have a fail-safe in #onAfter below in case that didn't work out
            executor.execute(new ForkingResponseHandlerRunnable(handler, null) {
                @Override
                protected void doRun() {
                    doHandleResponse(handler, remoteAddress, stream, /* autocloses */ message);
                }

                @Override
                public void onAfter() {
                    message.close();
                }
            });
        }
    }

    /**
     *
     * @param handler response handler
     * @param remoteAddress remote address that the message was sent from
     * @param stream bytes stream for reading the message
     * @param inboundMessage inbound message, guaranteed to get closed by this method
     * @param <T> response message type
     */
    private <T extends TransportResponse> void doHandleResponse(
        TransportResponseHandler<T> handler,
        InetSocketAddress remoteAddress,
        final StreamInput stream,
        InboundMessage inboundMessage
    ) {
        final T response;
        try (inboundMessage) {
            response = handler.read(stream);
            verifyResponseReadFully(inboundMessage.getHeader(), handler, stream);
        } catch (Exception e) {
            final TransportException serializationException = new TransportSerializationException(
                "Failed to deserialize response from handler [" + handler + "]",
                e
            );
            logger.warn(() -> "Failed to deserialize response from [" + remoteAddress + "]", serializationException);
            assert ignoreDeserializationErrors : e;
            doHandleException(handler, serializationException);
            return;
        }
        try {
            handler.handleResponse(response);
        } catch (Exception e) {
            doHandleException(handler, new ResponseHandlerFailureTransportException(e));
        } finally {
            response.decRef();
        }
    }

    /**
     * @param message guaranteed to get closed by this method
     */
    private void handlerResponseError(StreamInput stream, InboundMessage message, final TransportResponseHandler<?> handler) {
        Exception error;
        try (message) {
            error = stream.readException();
            verifyResponseReadFully(message.getHeader(), handler, stream);
        } catch (Exception e) {
            error = new TransportSerializationException(
                "Failed to deserialize exception response from stream for handler [" + handler + "]",
                e
            );
            assert ignoreDeserializationErrors : error;
        }
        handleException(
            handler,
            error instanceof RemoteTransportException rtx ? rtx : new RemoteTransportException(error.getMessage(), error)
        );
    }

    private void handleException(final TransportResponseHandler<?> handler, TransportException transportException) {
        final var executor = handler.executor();
        if (executor == EsExecutors.DIRECT_EXECUTOR_SERVICE) {
            doHandleException(handler, transportException);
        } else {
            executor.execute(new ForkingResponseHandlerRunnable(handler, transportException) {
                @Override
                protected void doRun() {
                    doHandleException(handler, transportException);
                }
            });
        }
    }

    private static void doHandleException(final TransportResponseHandler<?> handler, TransportException transportException) {
        try {
            handler.handleException(transportException);
        } catch (Exception e) {
            transportException.addSuppressed(e);
            logger.error(() -> "failed to handle exception response [" + handler + "]", transportException);
        }
    }

    private StreamInput namedWriteableStream(StreamInput delegate) {
        return new NamedWriteableAwareStreamInput(delegate, namedWriteableRegistry);
    }

    static boolean assertRemoteVersion(StreamInput in, TransportVersion version) {
        assert version.equals(in.getTransportVersion())
            : "Stream version [" + in.getTransportVersion() + "] does not match version [" + version + "]";
        return true;
    }
}
