/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.network.HandlingTimeTracker;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.transport.NetworkExceptionHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

import static org.elasticsearch.core.Strings.format;

final class OutboundHandler {

    private static final Logger logger = LogManager.getLogger(OutboundHandler.class);

    private final String nodeName;
    private final TransportVersion version;
    private final StatsTracker statsTracker;
    private final ThreadPool threadPool;
    private final Recycler<BytesRef> recycler;
    private final HandlingTimeTracker handlingTimeTracker;
    private final boolean rstOnClose;

    private volatile long slowLogThresholdMs = Long.MAX_VALUE;

    private volatile TransportMessageListener messageListener = TransportMessageListener.NOOP_LISTENER;

    OutboundHandler(
        String nodeName,
        TransportVersion version,
        StatsTracker statsTracker,
        ThreadPool threadPool,
        Recycler<BytesRef> recycler,
        HandlingTimeTracker handlingTimeTracker,
        boolean rstOnClose
    ) {
        this.nodeName = nodeName;
        this.version = version;
        this.statsTracker = statsTracker;
        this.threadPool = threadPool;
        this.recycler = recycler;
        this.handlingTimeTracker = handlingTimeTracker;
        this.rstOnClose = rstOnClose;
    }

    void setSlowLogThreshold(TimeValue slowLogThreshold) {
        this.slowLogThresholdMs = slowLogThreshold.getMillis();
    }

    /**
     * Send a raw message over the given channel.
     *
     * @param listener completed when the message has been sent, on the network thread (unless the network thread has shut down). Take care
     *                 if calling back into the network layer from this listener without dispatching to a new thread since if we do that
     *                 too many times in a row it can cause a stack overflow. When in doubt, dispatch any follow-up work onto a separate
     *                 thread.
     */
    void sendBytes(TcpChannel channel, BytesReference bytes, ActionListener<Void> listener) {
        internalSend(channel, bytes, null, listener);
    }

    /**
     * Sends the request to the given channel. This method should be used to send {@link TransportRequest}
     * objects back to the caller.
     */
    void sendRequest(
        final DiscoveryNode node,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final TransportRequest request,
        final TransportRequestOptions options,
        final TransportVersion transportVersion,
        final Compression.Scheme compressionScheme,
        final boolean isHandshake
    ) throws IOException, TransportException {
        TransportVersion version = TransportVersion.min(this.version, transportVersion);
        OutboundMessage.Request message = new OutboundMessage.Request(
            threadPool.getThreadContext(),
            request,
            version,
            action,
            requestId,
            isHandshake,
            compressionScheme
        );
        if (request.tryIncRef() == false) {
            assert false : "request [" + request + "] has been released already";
            throw new AlreadyClosedException("request [" + request + "] has been released already");
        }
        sendMessage(channel, message, ResponseStatsConsumer.NONE, () -> {
            try {
                messageListener.onRequestSent(node, requestId, action, request, options);
            } finally {
                request.decRef();
            }
        });
    }

    /**
     * Sends the response to the given channel. This method should be used to send {@link TransportResponse}
     * objects back to the caller.
     *
     * @see #sendErrorResponse for sending error responses
     */
    void sendResponse(
        final TransportVersion transportVersion,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final TransportResponse response,
        final Compression.Scheme compressionScheme,
        final boolean isHandshake,
        final ResponseStatsConsumer responseStatsConsumer
    ) {
        TransportVersion version = TransportVersion.min(this.version, transportVersion);
        OutboundMessage.Response message = new OutboundMessage.Response(
            threadPool.getThreadContext(),
            response,
            version,
            requestId,
            isHandshake,
            compressionScheme
        );
        response.mustIncRef();
        try {
            sendMessage(channel, message, responseStatsConsumer, () -> {
                try {
                    messageListener.onResponseSent(requestId, action, response);
                } finally {
                    response.decRef();
                }
            });
        } catch (Exception ex) {
            if (isHandshake) {
                logger.error(
                    () -> format("Failed to send handshake response version [%s] received on [%s], closing channel", version, channel),
                    ex
                );
                channel.close();
            } else {
                sendErrorResponse(transportVersion, channel, requestId, action, responseStatsConsumer, ex);
            }
        }
    }

    /**
     * Sends back an error response to the caller via the given channel
     */
    void sendErrorResponse(
        final TransportVersion transportVersion,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final ResponseStatsConsumer responseStatsConsumer,
        final Exception error
    ) {
        TransportVersion version = TransportVersion.min(this.version, transportVersion);
        RemoteTransportException tx = new RemoteTransportException(nodeName, channel.getLocalAddress(), action, error);
        OutboundMessage.Response message = new OutboundMessage.Response(threadPool.getThreadContext(), tx, version, requestId, false, null);
        try {
            sendMessage(channel, message, responseStatsConsumer, () -> messageListener.onResponseSent(requestId, action, error));
        } catch (Exception sendException) {
            sendException.addSuppressed(error);
            logger.error(() -> format("Failed to send error response on channel [%s], closing channel", channel), sendException);
            channel.close();
        }
    }

    private void sendMessage(
        TcpChannel channel,
        OutboundMessage networkMessage,
        ResponseStatsConsumer responseStatsConsumer,
        Releasable onAfter
    ) throws IOException {
        final RecyclerBytesStreamOutput byteStreamOutput;
        boolean bufferSuccess = false;
        try {
            byteStreamOutput = new RecyclerBytesStreamOutput(recycler);
            bufferSuccess = true;
        } finally {
            if (bufferSuccess == false) {
                Releasables.closeExpectNoException(onAfter);
            }
        }
        final Releasable release = Releasables.wrap(byteStreamOutput, onAfter);
        final BytesReference message;
        boolean serializeSuccess = false;
        try {
            message = networkMessage.serialize(byteStreamOutput);
            serializeSuccess = true;
        } catch (Exception e) {
            logger.warn(() -> "failed to serialize outbound message [" + networkMessage + "]", e);
            throw e;
        } finally {
            if (serializeSuccess == false) {
                release.close();
            }
        }
        responseStatsConsumer.addResponseStats(message.length());
        internalSend(channel, message, networkMessage, ActionListener.running(release::close));
    }

    private void internalSend(
        TcpChannel channel,
        BytesReference reference,
        @Nullable OutboundMessage message,
        ActionListener<Void> listener
    ) {
        final long startTime = threadPool.rawRelativeTimeInMillis();
        channel.getChannelStats().markAccessed(startTime);
        final long messageSize = reference.length();
        TransportLogger.logOutboundMessage(channel, reference);
        // stash thread context so that channel event loop is not polluted by thread context
        try (var ignored = threadPool.getThreadContext().newEmptyContext()) {
            channel.sendMessage(reference, new ActionListener<>() {
                @Override
                public void onResponse(Void v) {
                    statsTracker.markBytesWritten(messageSize);
                    listener.onResponse(v);
                    maybeLogSlowMessage(true);
                }

                @Override
                public void onFailure(Exception e) {
                    final Level closeConnectionExceptionLevel = NetworkExceptionHelper.getCloseConnectionExceptionLevel(e, rstOnClose);
                    if (closeConnectionExceptionLevel == Level.OFF) {
                        logger.warn(() -> "send message failed [channel: " + channel + "]", e);
                    } else if (closeConnectionExceptionLevel == Level.INFO && logger.isDebugEnabled() == false) {
                        logger.info("send message failed [channel: {}]: {}", channel, e.getMessage());
                    } else {
                        logger.log(closeConnectionExceptionLevel, () -> "send message failed [channel: " + channel + "]", e);
                    }
                    listener.onFailure(e);
                    maybeLogSlowMessage(false);
                }

                private void maybeLogSlowMessage(boolean success) {
                    final long logThreshold = slowLogThresholdMs;
                    if (logThreshold > 0) {
                        final long took = threadPool.rawRelativeTimeInMillis() - startTime;
                        handlingTimeTracker.addHandlingTime(took);
                        if (took > logThreshold) {
                            logger.warn(
                                "sending transport message [{}] of size [{}] on [{}] took [{}ms] which is above the warn "
                                    + "threshold of [{}ms] with success [{}]",
                                message,
                                messageSize,
                                channel,
                                took,
                                logThreshold,
                                success
                            );
                        }
                    }
                }
            });
        } catch (RuntimeException ex) {
            Releasables.closeExpectNoException(() -> listener.onFailure(ex), () -> CloseableChannel.closeChannel(channel));
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

    public boolean rstOnClose() {
        return rstOnClose;
    }

}
