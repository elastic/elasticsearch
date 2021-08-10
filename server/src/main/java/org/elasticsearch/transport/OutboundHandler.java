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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.transport.NetworkExceptionHelper;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

final class OutboundHandler {

    private static final Logger logger = LogManager.getLogger(OutboundHandler.class);

    private final String nodeName;
    private final Version version;
    private final StatsTracker statsTracker;
    private final ThreadPool threadPool;
    private final BigArrays bigArrays;
    private final Compression.Scheme configuredCompressionScheme;

    private volatile long slowLogThresholdMs = Long.MAX_VALUE;

    private volatile TransportMessageListener messageListener = TransportMessageListener.NOOP_LISTENER;

    OutboundHandler(String nodeName, Version version, StatsTracker statsTracker, ThreadPool threadPool, BigArrays bigArrays,
                    Compression.Scheme compressionScheme) {
        this.nodeName = nodeName;
        this.version = version;
        this.statsTracker = statsTracker;
        this.threadPool = threadPool;
        this.bigArrays = bigArrays;
        this.configuredCompressionScheme = compressionScheme;
    }

    void setSlowLogThreshold(TimeValue slowLogThreshold) {
        this.slowLogThresholdMs = slowLogThreshold.getMillis();
    }

    void sendBytes(TcpChannel channel, BytesReference bytes, ActionListener<Void> listener) {
        internalSend(channel, bytes, null, listener);
    }

    /**
     * Sends the request to the given channel. This method should be used to send {@link TransportRequest}
     * objects back to the caller.
     */
    void sendRequest(final DiscoveryNode node, final TcpChannel channel, final long requestId, final String action,
                     final TransportRequest request, final TransportRequestOptions options, final Version channelVersion,
                     final Compression.Scheme compressionScheme, final boolean isHandshake) throws IOException, TransportException {
        Version version = Version.min(this.version, channelVersion);
        OutboundMessage.Request message =
            new OutboundMessage.Request(threadPool.getThreadContext(), request, version, action, requestId, isHandshake, compressionScheme);
        if (request.tryIncRef() == false) {
            assert false : "request [" + request + "] has been released already";
            throw new AlreadyClosedException("request [" + request + "] has been released already");
        }
        ActionListener<Void> listener = ActionListener.wrap(() -> {
            try {
                messageListener.onRequestSent(node, requestId, action, request, options);
            } finally {
                request.decRef();
            }
        });
        sendMessage(channel, message, listener);
    }

    /**
     * Sends the response to the given channel. This method should be used to send {@link TransportResponse}
     * objects back to the caller.
     *
     * @see #sendErrorResponse(Version, TcpChannel, long, String, Exception) for sending error responses
     */
    void sendResponse(final Version nodeVersion, final TcpChannel channel, final long requestId, final String action,
                      final TransportResponse response, Compression.Scheme compressionScheme, final boolean isHandshake)
        throws IOException {
        Version version = Version.min(this.version, nodeVersion);
        OutboundMessage.Response message = new OutboundMessage.Response(threadPool.getThreadContext(), response, version,
            requestId, isHandshake, compressionScheme);
        ActionListener<Void> listener = ActionListener.wrap(() -> messageListener.onResponseSent(requestId, action, response));
        sendMessage(channel, message, listener);
    }

    /**
     * Sends back an error response to the caller via the given channel
     */
    void sendErrorResponse(final Version nodeVersion, final TcpChannel channel, final long requestId, final String action,
                           final Exception error) throws IOException {
        Version version = Version.min(this.version, nodeVersion);
        TransportAddress address = new TransportAddress(channel.getLocalAddress());
        RemoteTransportException tx = new RemoteTransportException(nodeName, address, action, error);
        OutboundMessage.Response message = new OutboundMessage.Response(threadPool.getThreadContext(), tx, version, requestId,
            false, null);
        ActionListener<Void> listener = ActionListener.wrap(() -> messageListener.onResponseSent(requestId, action, error));
        sendMessage(channel, message, listener);
    }

    private void sendMessage(TcpChannel channel, OutboundMessage networkMessage, ActionListener<Void> listener) throws IOException {
        final BytesStreamOutput bytesStreamOutput = new ReleasableBytesStreamOutput(bigArrays);
        final ActionListener<Void> wrappedListener = ActionListener.runBefore(listener, bytesStreamOutput::close);
        final BytesReference message;
        try {
            message = networkMessage.serialize(bytesStreamOutput);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("failed to serialize outbound message [{}]", networkMessage), e);
            wrappedListener.onFailure(e);
            throw e;
        }
        internalSend(channel, message, networkMessage, wrappedListener);
    }

    private void internalSend(TcpChannel channel, BytesReference reference, @Nullable OutboundMessage message,
                              ActionListener<Void> listener) {
        final long startTime = threadPool.relativeTimeInMillis();
        channel.getChannelStats().markAccessed(startTime);
        final long messageSize = reference.length();
        TransportLogger.logOutboundMessage(channel, reference);
        // stash thread context so that channel event loop is not polluted by thread context
        try (ThreadContext.StoredContext existing = threadPool.getThreadContext().stashContext()) {
            channel.sendMessage(reference, new ActionListener<>() {
                @Override
                public void onResponse(Void v) {
                    statsTracker.markBytesWritten(messageSize);
                    listener.onResponse(v);
                    maybeLogSlowMessage(true);
                }

                @Override
                public void onFailure(Exception e) {
                    if (NetworkExceptionHelper.isCloseConnectionException(e)) {
                        logger.debug(() -> new ParameterizedMessage("send message failed [channel: {}]", channel), e);
                    } else {
                        logger.warn(() -> new ParameterizedMessage("send message failed [channel: {}]", channel), e);
                    }
                    listener.onFailure(e);
                    maybeLogSlowMessage(false);
                }

                private void maybeLogSlowMessage(boolean success) {
                    final long logThreshold = slowLogThresholdMs;
                    if (logThreshold > 0) {
                        final long took = threadPool.relativeTimeInMillis() - startTime;
                        if (took > logThreshold) {
                            logger.warn("sending transport message [{}] of size [{}] on [{}] took [{}ms] which is above the warn " +
                                    "threshold of [{}ms] with success [{}]", message, messageSize, channel, took, logThreshold, success);
                        }
                    }
                }
            });
        } catch (RuntimeException ex) {
            listener.onFailure(ex);
            CloseableChannel.closeChannel(channel);
            throw ex;
        }
    }

    void setMessageListener(TransportMessageListener listener) {
        if (messageListener == TransportMessageListener.NOOP_LISTENER) {
            messageListener = listener;
        } else {
            throw new IllegalStateException("Cannot set message listener twice");
        }
    }

}
