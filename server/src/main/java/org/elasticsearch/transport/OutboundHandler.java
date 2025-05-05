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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.network.HandlingTimeTracker;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.transport.NetworkExceptionHelper;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Streams;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;

public final class OutboundHandler {

    private static final Logger logger = LogManager.getLogger(OutboundHandler.class);

    private final String nodeName;

    @UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED_COORDINATION) // only used in assertions, can be dropped in future
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
        internalSend(channel, bytes, () -> "raw bytes", listener);
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
        assert assertValidTransportVersion(transportVersion);
        sendMessage(
            channel,
            MessageDirection.REQUEST,
            action,
            request,
            requestId,
            isHandshake,
            compressionScheme,
            transportVersion,
            ResponseStatsConsumer.NONE,
            () -> messageListener.onRequestSent(node, requestId, action, request, options)
        );
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
        assert assertValidTransportVersion(transportVersion);
        assert response.hasReferences();
        try {
            sendMessage(
                channel,
                MessageDirection.RESPONSE,
                action,
                response,
                requestId,
                isHandshake,
                compressionScheme,
                transportVersion,
                responseStatsConsumer,
                () -> messageListener.onResponseSent(requestId, action)
            );
        } catch (Exception ex) {
            if (isHandshake) {
                logger.error(
                    () -> format(
                        "Failed to send handshake response version [%s] received on [%s], closing channel",
                        transportVersion,
                        channel
                    ),
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
        assert assertValidTransportVersion(transportVersion);
        var msg = new RemoteTransportException(nodeName, channel.getLocalAddress(), action, error);
        try {
            sendMessage(
                channel,
                MessageDirection.RESPONSE_ERROR,
                action,
                msg,
                requestId,
                false,
                null,
                transportVersion,
                responseStatsConsumer,
                () -> messageListener.onResponseSent(requestId, action, error)
            );
        } catch (Exception sendException) {
            sendException.addSuppressed(error);
            logger.error(() -> format("Failed to send error response on channel [%s], closing channel", channel), sendException);
            channel.close();
        }
    }

    public enum MessageDirection {
        REQUEST,
        RESPONSE,
        RESPONSE_ERROR
    }

    private void sendMessage(
        TcpChannel channel,
        MessageDirection messageDirection,
        String action,
        Writeable writeable,
        long requestId,
        boolean isHandshake,
        Compression.Scheme possibleCompressionScheme,
        TransportVersion version,
        ResponseStatsConsumer responseStatsConsumer,
        Releasable onAfter
    ) throws IOException {
        assert action != null;
        final var compressionScheme = writeable instanceof BytesTransportMessage ? null : possibleCompressionScheme;
        final BytesReference message;
        boolean serializeSuccess = false;
        final RecyclerBytesStreamOutput byteStreamOutput = new RecyclerBytesStreamOutput(recycler);
        try {
            message = serialize(
                messageDirection,
                action,
                requestId,
                isHandshake,
                version,
                compressionScheme,
                writeable,
                threadPool.getThreadContext(),
                byteStreamOutput
            );
            serializeSuccess = true;
        } catch (Exception e) {
            logger.warn(() -> "failed to serialize outbound message [" + writeable + "]", e);
            throw e;
        } finally {
            if (serializeSuccess == false) {
                Releasables.close(byteStreamOutput, onAfter);
            }
        }
        responseStatsConsumer.addResponseStats(message.length());
        final var messageType = writeable.getClass();
        internalSend(
            channel,
            message,
            () -> (messageDirection == MessageDirection.REQUEST ? "Request{" : "Response{")
                + action
                + "}{id="
                + requestId
                + "}{err="
                + (messageDirection == MessageDirection.RESPONSE_ERROR)
                + "}{cs="
                + compressionScheme
                + "}{hs="
                + isHandshake
                + "}{t="
                + messageType
                + "}",
            ActionListener.releasing(
                message instanceof ReleasableBytesReference r
                    ? Releasables.wrap(byteStreamOutput, onAfter, r)
                    : Releasables.wrap(byteStreamOutput, onAfter)
            )
        );
    }

    // public for tests
    public static BytesReference serialize(
        MessageDirection messageDirection,
        String action,
        long requestId,
        boolean isHandshake,
        TransportVersion version,
        Compression.Scheme compressionScheme,
        Writeable writeable,
        ThreadContext threadContext,
        RecyclerBytesStreamOutput byteStreamOutput
    ) throws IOException {
        assert action != null;
        assert byteStreamOutput.position() == 0;
        byteStreamOutput.setTransportVersion(version);
        byteStreamOutput.skip(TcpHeader.HEADER_SIZE);
        threadContext.writeTo(byteStreamOutput);
        if (messageDirection == MessageDirection.REQUEST) {
            if (version.before(TransportVersions.V_8_0_0)) {
                // empty features array
                byteStreamOutput.writeStringArray(Strings.EMPTY_ARRAY);
            }
            byteStreamOutput.writeString(action);
        }

        final int variableHeaderLength = Math.toIntExact(byteStreamOutput.position() - TcpHeader.HEADER_SIZE);
        BytesReference message = serializeMessageBody(writeable, compressionScheme, version, byteStreamOutput);
        byte status = 0;
        if (messageDirection != MessageDirection.REQUEST) {
            status = TransportStatus.setResponse(status);
        }
        if (isHandshake) {
            status = TransportStatus.setHandshake(status);
        }
        if (messageDirection == MessageDirection.RESPONSE_ERROR) {
            status = TransportStatus.setError(status);
        }
        if (compressionScheme != null) {
            status = TransportStatus.setCompress(status);
        }
        byteStreamOutput.seek(0);
        TcpHeader.writeHeader(byteStreamOutput, requestId, status, version, message.length() - TcpHeader.HEADER_SIZE, variableHeaderLength);
        return message;
    }

    private static BytesReference serializeMessageBody(
        Writeable writeable,
        Compression.Scheme compressionScheme,
        TransportVersion version,
        RecyclerBytesStreamOutput byteStreamOutput
    ) throws IOException {
        // The compressible bytes stream will not close the underlying bytes stream
        final StreamOutput stream = compressionScheme != null ? wrapCompressed(compressionScheme, byteStreamOutput) : byteStreamOutput;
        final ReleasableBytesReference zeroCopyBuffer;
        try {
            stream.setTransportVersion(version);
            if (writeable instanceof BytesTransportMessage bRequest) {
                assert stream == byteStreamOutput;
                assert compressionScheme == null;
                bRequest.writeThin(stream);
                zeroCopyBuffer = bRequest.bytes();
            } else if (writeable instanceof RemoteTransportException remoteTransportException) {
                stream.writeException(remoteTransportException);
                zeroCopyBuffer = ReleasableBytesReference.empty();
            } else {
                writeable.writeTo(stream);
                zeroCopyBuffer = ReleasableBytesReference.empty();
            }
        } finally {
            // We have to close here before accessing the bytes when using compression to ensure that some marker bytes (EOS marker)
            // are written.
            if (compressionScheme != null) {
                stream.close();
            }
        }
        final BytesReference msg = byteStreamOutput.bytes();
        if (zeroCopyBuffer.length() == 0) {
            return msg;
        }
        zeroCopyBuffer.mustIncRef();
        return new ReleasableBytesReference(CompositeBytesReference.of(msg, zeroCopyBuffer), (RefCounted) zeroCopyBuffer);
    }

    // compressed stream wrapped bytes must be no-close wrapped since we need to close the compressed wrapper below to release
    // resources and write EOS marker bytes but must not yet release the bytes themselves
    private static StreamOutput wrapCompressed(Compression.Scheme compressionScheme, RecyclerBytesStreamOutput bytesStream)
        throws IOException {
        if (compressionScheme == Compression.Scheme.DEFLATE) {
            return new OutputStreamStreamOutput(
                CompressorFactory.COMPRESSOR.threadLocalOutputStream(org.elasticsearch.core.Streams.noCloseStream(bytesStream))
            );
        } else if (compressionScheme == Compression.Scheme.LZ4) {
            return new OutputStreamStreamOutput(Compression.Scheme.lz4OutputStream(Streams.noCloseStream(bytesStream)));
        } else {
            throw new IllegalArgumentException("Invalid compression scheme: " + compressionScheme);
        }
    }

    private void internalSend(
        TcpChannel channel,
        BytesReference reference,
        Supplier<String> messageDescription,
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
                        handlingTimeTracker.addObservation(took);
                        if (took > logThreshold) {
                            logger.warn(
                                "sending transport message [{}] of size [{}] on [{}] took [{}ms] which is above the warn "
                                    + "threshold of [{}ms] with success [{}]",
                                messageDescription.get(),
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

    private boolean assertValidTransportVersion(TransportVersion transportVersion) {
        assert this.version.before(TransportVersions.MINIMUM_COMPATIBLE) // running an incompatible-version test
            || this.version.onOrAfter(transportVersion) : this.version + " vs " + transportVersion;
        return true;
    }

}
