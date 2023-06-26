/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.HandlingTimeTracker;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Handles inbound messages by first deserializing a {@link TransportMessage} from an {@link InboundMessage} and then passing
 * it to the appropriate handler.
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

    void inboundMessage(TcpChannel channel, InboundMessage message) throws Exception {
        final long startTime = threadPool.rawRelativeTimeInMillis();
        channel.getChannelStats().markAccessed(startTime);
        TransportLogger.logInboundMessage(channel, message);

        if (message.isPing()) {
            keepAlive.receiveKeepAlive(channel);
        } else {
            messageReceived(channel, message, startTime);
        }
    }

    // Empty stream constant to avoid instantiating a new stream for empty messages.
    private static final StreamInput EMPTY_STREAM_INPUT = new ByteBufferStreamInput(ByteBuffer.wrap(BytesRef.EMPTY_BYTES));

    private void messageReceived(TcpChannel channel, InboundMessage message, long startTime) throws IOException {
        final InetSocketAddress remoteAddress = channel.getRemoteAddress();
        final Header header = message.getHeader();
        assert header.needsToReadVariableHeader() == false;

        TransportResponseHandler<?> responseHandler = null;
        final boolean isRequest = message.getHeader().isRequest();

        ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext existing = threadContext.stashContext()) {
            // Place the context with the headers from the message
            threadContext.setHeaders(header.getHeaders());
            threadContext.putTransient("_remote_address", remoteAddress);
            if (isRequest) {
                handleRequest(channel, header, message);
            } else {
                // Responses do not support short circuiting currently
                assert message.isShortCircuit() == false;
                long requestId = header.getRequestId();
                if (header.isHandshake()) {
                    responseHandler = handshaker.removeHandlerForHandshake(requestId);
                } else {
                    final TransportResponseHandler<? extends TransportResponse> theHandler = responseHandlers.onResponseReceived(
                        requestId,
                        messageListener
                    );
                    if (theHandler == null && header.isError()) {
                        responseHandler = handshaker.removeHandlerForHandshake(requestId);
                    } else {
                        responseHandler = theHandler;
                    }
                }
                // ignore if its null, the service logs it
                if (responseHandler != null) {
                    if (message.getContentLength() > 0 || header.getVersion().equals(TransportVersion.current()) == false) {
                        final StreamInput streamInput = namedWriteableStream(message.openOrGetStreamInput());
                        assertRemoteVersion(streamInput, header.getVersion());
                        if (header.isError()) {
                            handlerResponseError(streamInput, message, responseHandler);
                        } else {
                            handleResponse(remoteAddress, streamInput, responseHandler, message);
                        }
                    } else {
                        assert header.isError() == false;
                        handleResponse(remoteAddress, EMPTY_STREAM_INPUT, responseHandler, message);
                    }
                }
            }
        } finally {
            final long took = threadPool.rawRelativeTimeInMillis() - startTime;
            handlingTimeTracker.addHandlingTime(took);
            final long logThreshold = slowLogThresholdMs;
            if (logThreshold > 0 && took > logThreshold) {
                if (isRequest) {
                    logger.warn(
                        "handling request [{}] took [{}ms] which is above the warn threshold of [{}ms]",
                        message,
                        took,
                        logThreshold
                    );
                } else {
                    logger.warn(
                        "handling response [{}] on handler [{}] took [{}ms] which is above the warn threshold of [{}ms]",
                        message,
                        responseHandler,
                        took,
                        logThreshold
                    );
                }
            }
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

    private <T extends TransportRequest> void handleRequest(TcpChannel channel, Header header, InboundMessage message) throws IOException {
        final String action = header.getActionName();
        final long requestId = header.getRequestId();
        final TransportVersion version = header.getVersion();
        if (header.isHandshake()) {
            messageListener.onRequestReceived(requestId, action);
            // Cannot short circuit handshakes
            assert message.isShortCircuit() == false;
            final StreamInput stream = namedWriteableStream(message.openOrGetStreamInput());
            assertRemoteVersion(stream, header.getVersion());
            final TransportChannel transportChannel = new TcpTransportChannel(
                outboundHandler,
                channel,
                action,
                requestId,
                version,
                header.getCompressionScheme(),
                ResponseStatsConsumer.NONE,
                header.isHandshake(),
                message.takeBreakerReleaseControl()
            );
            try {
                handshaker.handleHandshake(transportChannel, requestId, stream);
            } catch (Exception e) {
                logger.warn(() -> "error processing handshake version [" + version + "] received on [" + channel + "], closing channel", e);
                channel.close();
            }
        } else {
            final TransportChannel transportChannel;
            final RequestHandlerRegistry<T> reg;
            try {
                reg = requestHandlers.getHandler(action);
                assert message.isShortCircuit() || reg != null : action;
                transportChannel = new TcpTransportChannel(
                    outboundHandler,
                    channel,
                    action,
                    requestId,
                    version,
                    header.getCompressionScheme(),
                    reg == null ? ResponseStatsConsumer.NONE : reg,
                    header.isHandshake(),
                    message.takeBreakerReleaseControl()
                );
            } catch (Exception e) {
                assert false : e;
                sendErrorResponse(
                    action,
                    new TcpTransportChannel(
                        outboundHandler,
                        channel,
                        action,
                        requestId,
                        version,
                        header.getCompressionScheme(),
                        ResponseStatsConsumer.NONE,
                        header.isHandshake(),
                        message.takeBreakerReleaseControl()
                    ),
                    e
                );
                return;
            }

            try {
                messageListener.onRequestReceived(requestId, action);
                if (reg != null) {
                    reg.addRequestStats(header.getNetworkMessageSize() + TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE);
                }

                if (message.isShortCircuit()) {
                    sendErrorResponse(action, transportChannel, message.getException());
                } else {
                    assert reg != null;
                    final StreamInput stream = namedWriteableStream(message.openOrGetStreamInput());
                    assertRemoteVersion(stream, header.getVersion());
                    final T request;
                    try {
                        request = reg.newRequest(stream);
                    } catch (Exception e) {
                        assert ignoreDeserializationErrors : e;
                        throw e;
                    }
                    try {
                        request.remoteAddress(channel.getRemoteAddress());
                        assert requestId > 0;
                        request.setRequestId(requestId);
                        // in case we throw an exception, i.e. when the limit is hit, we don't want to verify
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
                        final String executor = reg.getExecutor();
                        if (ThreadPool.Names.SAME.equals(executor)) {
                            try (var ignored = threadPool.getThreadContext().newTraceContext()) {
                                try {
                                    reg.processMessageReceived(request, transportChannel);
                                } catch (Exception e) {
                                    sendErrorResponse(reg.getAction(), transportChannel, e);
                                }
                            }
                        } else {
                            boolean success = false;
                            request.incRef();
                            try {
                                threadPool.executor(executor)
                                    .execute(threadPool.getThreadContext().preserveContextWithTracing(new AbstractRunnable() {
                                        @Override
                                        protected void doRun() throws Exception {
                                            reg.processMessageReceived(request, transportChannel);
                                        }

                                        @Override
                                        public boolean isForceExecution() {
                                            return reg.isForceExecution();
                                        }

                                        @Override
                                        public void onFailure(Exception e) {
                                            sendErrorResponse(reg.getAction(), transportChannel, e);
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
                    } finally {
                        request.decRef();
                    }
                }
            } catch (Exception e) {
                sendErrorResponse(action, transportChannel, e);
            }
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

    private <T extends TransportResponse> void handleResponse(
        InetSocketAddress remoteAddress,
        final StreamInput stream,
        final TransportResponseHandler<T> handler,
        final InboundMessage inboundMessage
    ) {
        final String executor = handler.executor();
        if (ThreadPool.Names.SAME.equals(executor)) {
            // no need to provide a buffer release here, we never escape the buffer when handling directly
            doHandleResponse(handler, remoteAddress, stream, inboundMessage.getHeader(), () -> {});
        } else {
            inboundMessage.incRef();
            // release buffer once we deserialize the message, but have a fail-safe in #onAfter below in case that didn't work out
            final Releasable releaseBuffer = Releasables.releaseOnce(inboundMessage::decRef);
            threadPool.executor(executor).execute(new ForkingResponseHandlerRunnable(handler, null) {
                @Override
                protected void doRun() {
                    doHandleResponse(handler, remoteAddress, stream, inboundMessage.getHeader(), releaseBuffer);
                }

                @Override
                public void onAfter() {
                    Releasables.closeExpectNoException(releaseBuffer);
                }
            });
        }
    }

    /**
     *
     * @param handler response handler
     * @param remoteAddress remote address that the message was sent from
     * @param stream bytes stream for reading the message
     * @param header message header
     * @param releaseResponseBuffer releasable that will be released once the message has been read from the {@code stream}
     * @param <T> response message type
     */
    private <T extends TransportResponse> void doHandleResponse(
        TransportResponseHandler<T> handler,
        InetSocketAddress remoteAddress,
        final StreamInput stream,
        final Header header,
        Releasable releaseResponseBuffer
    ) {
        final T response;
        try (releaseResponseBuffer) {
            response = handler.read(stream);
            response.remoteAddress(remoteAddress);
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
            verifyResponseReadFully(header, handler, stream);
            handler.handleResponse(response);
        } catch (Exception e) {
            doHandleException(handler, new ResponseHandlerFailureTransportException(e));
        } finally {
            response.decRef();
        }
    }

    private void handlerResponseError(StreamInput stream, InboundMessage message, final TransportResponseHandler<?> handler) {
        Exception error;
        try {
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
        if (ThreadPool.Names.SAME.equals(executor)) {
            doHandleException(handler, transportException);
        } else {
            threadPool.executor(executor).execute(new ForkingResponseHandlerRunnable(handler, transportException) {
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

    static void assertRemoteVersion(StreamInput in, TransportVersion version) {
        assert version.equals(in.getTransportVersion())
            : "Stream version [" + in.getTransportVersion() + "] does not match version [" + version + "]";
    }
}
