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

package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;

public class InboundHandler {

    private static final Logger logger = LogManager.getLogger(InboundHandler.class);

    private final MeanMetric readBytesMetric = new MeanMetric();
    private final ThreadPool threadPool;
    private final OutboundHandler outboundHandler;
    private final CircuitBreakerService circuitBreakerService;
    private final InboundMessage.Reader reader;
    private final TransportHandshaker handshaker;
    private final TransportKeepAlive keepAlive;

    private final Transport.ResponseHandlers responseHandlers = new Transport.ResponseHandlers();
    private volatile Map<String, RequestHandlerRegistry<? extends TransportRequest>> requestHandlers = Collections.emptyMap();
    private volatile TransportMessageListener messageListener = TransportMessageListener.NOOP_LISTENER;

    InboundHandler(ThreadPool threadPool, OutboundHandler outboundHandler, InboundMessage.Reader reader,
                   CircuitBreakerService circuitBreakerService, TransportHandshaker handshaker, TransportKeepAlive keepAlive) {
        this.threadPool = threadPool;
        this.outboundHandler = outboundHandler;
        this.circuitBreakerService = circuitBreakerService;
        this.reader = reader;
        this.handshaker = handshaker;
        this.keepAlive = keepAlive;
    }

    synchronized <Request extends TransportRequest> void registerRequestHandler(RequestHandlerRegistry<Request> reg) {
        if (requestHandlers.containsKey(reg.getAction())) {
            throw new IllegalArgumentException("transport handlers for action " + reg.getAction() + " is already registered");
        }
        requestHandlers = Maps.copyMapWithAddedEntry(requestHandlers, reg.getAction(), reg);
    }

    final RequestHandlerRegistry<? extends TransportRequest> getRequestHandler(String action) {
        return requestHandlers.get(action);
    }

    final Transport.ResponseHandlers getResponseHandlers() {
        return responseHandlers;
    }

    MeanMetric getReadBytes() {
        return readBytesMetric;
    }

    void setMessageListener(TransportMessageListener listener) {
        if (messageListener == TransportMessageListener.NOOP_LISTENER) {
            messageListener = listener;
        } else {
            throw new IllegalStateException("Cannot set message listener twice");
        }
    }

    void inboundMessage(TcpChannel channel, BytesReference message) throws Exception {
        channel.getChannelStats().markAccessed(threadPool.relativeTimeInMillis());
        TransportLogger.logInboundMessage(channel, message);
        readBytesMetric.inc(message.length() + TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE);
        // Message length of 0 is a ping
        if (message.length() != 0) {
            messageReceived(message, channel);
        } else {
            keepAlive.receiveKeepAlive(channel);
        }
    }

    private void messageReceived(BytesReference reference, TcpChannel channel) throws IOException {
        InetSocketAddress remoteAddress = channel.getRemoteAddress();

        ThreadContext threadContext = threadPool.getThreadContext();
        final InboundMessage message;
        try {
            message = reader.deserialize(reference);
        } catch (Exception e) {
            if (reference instanceof Releasable) {
                ((Releasable) reference).close();
            }
            throw e;
        }
        final Releasable releasable = () -> {
            try {
                IOUtils.close(() -> {
                    if (reference instanceof Releasable) {
                        ((Releasable) reference).close();
                    }
                }, message);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
        try (ThreadContext.StoredContext existing = threadContext.stashContext()) {
            // Place the context with the headers from the message
            message.getStoredContext().restore();
            threadContext.putTransient("_remote_address", remoteAddress);
            if (message.isRequest()) {
                handleRequest(channel, (InboundMessage.Request) message, releasable, reference.length());
            } else {
                boolean release = true;
                try {
                    final TransportResponseHandler<?> handler;
                    long requestId = message.getRequestId();
                    if (message.isHandshake()) {
                        handler = handshaker.removeHandlerForHandshake(requestId);
                    } else {
                        TransportResponseHandler<? extends TransportResponse> theHandler =
                            responseHandlers.onResponseReceived(requestId, messageListener);
                        if (theHandler == null && message.isError()) {
                            handler = handshaker.removeHandlerForHandshake(requestId);
                        } else {
                            handler = theHandler;
                        }
                    }
                    // ignore if its null, the service logs it
                    if (handler != null) {
                        if (message.isError()) {
                            handlerResponseError(message.getStreamInput(), handler);
                        } else {
                            release = false;
                            handleResponse(remoteAddress, message.getStreamInput(), releasable, handler);
                        }
                    }
                } finally {
                    if (release) {
                        releasable.close();
                    }
                }
            }
        }
    }

    private void handleRequest(TcpChannel channel, InboundMessage.Request message, Releasable releasable, int messageLengthBytes) {
        final String action = message.getActionName();
        final long requestId = message.getRequestId();
        final Version version = message.getVersion();
        TransportChannel transportChannel = null;
        try {
            messageListener.onRequestReceived(requestId, action);
            if (message.isHandshake()) {
                handshaker.handleHandshake(version, channel, requestId, message.getStreamInput());
                releasable.close();
            } else {
                final RequestHandlerRegistry reg = getRequestHandler(action);
                if (reg == null) {
                    throw new ActionNotFoundTransportException(action);
                }
                CircuitBreaker breaker = circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
                if (reg.canTripCircuitBreaker()) {
                    breaker.addEstimateBytesAndMaybeBreak(messageLengthBytes, "<transport_request>");
                } else {
                    breaker.addWithoutBreaking(messageLengthBytes);
                }
                transportChannel = new TcpTransportChannel(outboundHandler, channel, action, requestId, version,
                    circuitBreakerService, messageLengthBytes, message.isCompress());
                final TransportChannel requestChannel = transportChannel;
                threadPool.executor(reg.getExecutor()).execute(new AbstractRunnable() {

                    private boolean released = false;

                    @SuppressWarnings({"unchecked"})
                    @Override
                    protected void doRun() throws Exception {
                        final TransportRequest request = reg.newRequest(message.getStreamInput());
                        releasable.close();
                        released = true;
                        request.remoteAddress(new TransportAddress(channel.getRemoteAddress()));
                        reg.processMessageReceived(request, requestChannel);
                    }

                    @Override
                    public boolean isForceExecution() {
                        return reg.isForceExecution();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try {
                            requestChannel.sendResponse(e);
                        } catch (Exception inner) {
                            inner.addSuppressed(e);
                            logger.warn(() -> new ParameterizedMessage(
                                "Failed to send error message back to client for action [{}]", reg.getAction()), inner);
                        }
                    }

                    @Override
                    public void onAfter() {
                        if (released == false) {
                            releasable.close();
                        }
                    }
                });
            }
        } catch (Exception e) {
            releasable.close();
            // the circuit breaker tripped
            if (transportChannel == null) {
                transportChannel = new TcpTransportChannel(outboundHandler, channel, action, requestId, version,
                    circuitBreakerService, 0, message.isCompress());
            }
            try {
                transportChannel.sendResponse(e);
            } catch (IOException inner) {
                inner.addSuppressed(e);
                logger.warn(() -> new ParameterizedMessage("Failed to send error message back to client for action [{}]", action), inner);
            }
        }
    }

    private <T extends TransportResponse> void handleResponse(InetSocketAddress remoteAddress, final StreamInput stream,
                                                              Releasable releasable, TransportResponseHandler<T> handler) {
        threadPool.executor(handler.executor()).execute(new AbstractRunnable() {

            private boolean released = false;

            @Override
            public void onFailure(Exception e) {
                handleException(handler, new ResponseHandlerFailureTransportException(e));
            }

            @Override
            protected void doRun() {
                final T response;
                try {
                    response = handler.read(stream);
                    releasable.close();
                    released = true;
                    response.remoteAddress(new TransportAddress(remoteAddress));
                } catch (Exception e) {
                    handleException(handler, new TransportSerializationException(
                        "Failed to deserialize response from handler [" + handler.getClass().getName() + "]", e));
                    return;
                }
                handler.handleResponse(response);
            }

            @Override
            public void onAfter() {
                if (released == false) {
                    releasable.close();
                }
            }
        });
    }

    private void handlerResponseError(StreamInput stream, final TransportResponseHandler<?> handler) {
        Exception error;
        try {
            error = stream.readException();
        } catch (Exception e) {
            error = new TransportSerializationException("Failed to deserialize exception response from stream", e);
        }
        handleException(handler, error);
    }

    private void handleException(final TransportResponseHandler<?> handler, Throwable error) {
        if (!(error instanceof RemoteTransportException)) {
            error = new RemoteTransportException(error.getMessage(), error);
        }
        final RemoteTransportException rtx = (RemoteTransportException) error;
        threadPool.executor(handler.executor()).execute(() -> {
            try {
                handler.handleException(rtx);
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage("failed to handle exception response [{}]", handler), e);
            }
        });
    }

}
