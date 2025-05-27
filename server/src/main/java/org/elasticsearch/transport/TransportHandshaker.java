/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Sends and receives transport-level connection handshakes. This class will send the initial handshake, manage state/timeouts while the
 * handshake is in transit, and handle the eventual response.
 */
final class TransportHandshaker {

    /*
     * The transport-level handshake allows the node that opened the connection to determine the newest protocol version with which it can
     * communicate with the remote node. Each node sends its maximum acceptable protocol version to the other, but the responding node
     * ignores the body of the request. After the handshake, the OutboundHandler uses the min(local,remote) protocol version for all later
     * messages.
     *
     * This version supports two handshake protocols, v7170099 and v8800000, which respectively have the same message structure as the
     * transport protocols of v7.17.0, and v8.18.0. This node only sends v8800000 requests, but it can send a valid response to any v7170099
     * requests that it receives.
     *
     * Note that these are not really TransportVersion constants as used elsewhere in ES, they're independent things that just happen to be
     * stored in the same location in the message header and which roughly match the same ID numbering scheme. Older versions of ES did rely
     * on them matching the real transport protocol (which itself matched the release version numbers), but these days that's no longer
     * true.
     *
     * Here are some example messages, broken down to show their structure. See TransportHandshakerRawMessageTests for supporting tests.
     *
     * ## v7170099 Requests:
     *
     * 45 53                            -- 'ES' marker
     * 00 00 00 31                      -- total message length
     *    00 00 00 00 00 00 00 01       -- request ID
     *    08                            -- status flags (0b1000 == handshake request)
     *    00 6d 68 33                   -- handshake protocol version (0x6d6833 == 7170099)
     *    00 00 00 1a                   -- length of variable portion of header
     *       00                         -- no request headers [1]
     *       00                         -- no response headers [1]
     *       00                         -- no features [2]
     *       16                         -- action string size
     *       69 6e 74 65 72 6e 61 6c    }
     *       3a 74 63 70 2f 68 61 6e    }- ASCII representation of HANDSHAKE_ACTION_NAME
     *       64 73 68 61 6b 65          }
     *    00                            -- no parent task ID [3]
     *    04                            -- payload length
     *       c3 f9 eb 03                -- max acceptable protocol version (vInt: 00000011 11101011 11111001 11000011 == 8060099)
     *
     * ## v7170099 Responses:
     *
     * 45 53                            -- 'ES' marker
     * 00 00 00 17                      -- total message length
     *    00 00 00 00 00 00 00 01       -- request ID (copied from request)
     *    09                            -- status flags (0b1001 == handshake response)
     *    00 6d 68 33                   -- handshake protocol version (0x6d6833 == 7170099, copied from request)
     *    00 00 00 02                   -- length of following variable portion of header
     *       00                         -- no request headers [1]
     *       00                         -- no response headers [1]
     *    c3 f9 eb 03                   -- max acceptable protocol version (vInt: 00000011 11101011 11111001 11000011 == 8060099)
     *
     * ## v8800000 Requests:
     *
     * 45 53                            -- 'ES' marker
     * 00 00 00 36                      -- total message length
     *    00 00 00 00 00 00 00 01       -- request ID
     *    08                            -- status flags (0b1000 == handshake request)
     *    00 86 47 00                   -- handshake protocol version (0x6d6833 == 7170099)
     *    00 00 00 19                   -- length of variable portion of header
     *       00                         -- no request headers [1]
     *       00                         -- no response headers [1]
     *       16                         -- action string size
     *       69 6e 74 65 72 6e 61 6c    }
     *       3a 74 63 70 2f 68 61 6e    }- ASCII representation of HANDSHAKE_ACTION_NAME
     *       64 73 68 61 6b 65          }
     *    00                            -- no parent task ID [3]
     *    0a                            -- payload length
     *       e8 8f 9b 04                -- requesting node transport version (vInt: 00000100 10011011 10001111 11101000 == 8833000)
     *       05                         -- requesting node release version string length
     *          39 2e 30 2e 30          -- requesting node release version string "9.0.0"
     *
     * ## v8800000 Responses:
     *
     * 45 53                            -- 'ES' marker
     * 00 00 00 1d                      -- total message length
     *    00 00 00 00 00 00 00 01       -- request ID (copied from request)
     *    09                            -- status flags (0b1001 == handshake response)
     *    00 86 47 00                   -- handshake protocol version (0x864700 == 8800000, copied from request)
     *    00 00 00 02                   -- length of following variable portion of header
     *       00                         -- no request headers [1]
     *       00                         -- no response headers [1]
     *    e8 8f 9b 04                   -- responding node transport version (vInt: 00000100 10011011 10001111 11101000 == 8833000)
     *    05                            -- responding node release version string length
     *       39 2e 30 2e 30             -- responding node release version string "9.0.0"
     *
     * [1] Thread context headers should be empty; see org.elasticsearch.common.util.concurrent.ThreadContext.ThreadContextStruct.writeTo
     *     for their structure.
     * [2] A list of strings, which can safely be ignored
     * [3] Parent task ID should be empty; see org.elasticsearch.tasks.TaskId.writeTo for its structure.
     */

    private static final Logger logger = LogManager.getLogger(TransportHandshaker.class);

    static final TransportVersion V8_HANDSHAKE_VERSION = TransportVersion.fromId(7_17_00_99);
    static final TransportVersion V9_HANDSHAKE_VERSION = TransportVersion.fromId(8_800_00_0);
    static final Set<TransportVersion> ALLOWED_HANDSHAKE_VERSIONS = Set.of(V8_HANDSHAKE_VERSION, V9_HANDSHAKE_VERSION);

    static final String HANDSHAKE_ACTION_NAME = "internal:tcp/handshake";
    private final ConcurrentMap<Long, HandshakeResponseHandler> pendingHandshakes = new ConcurrentHashMap<>();
    private final CounterMetric numHandshakes = new CounterMetric();

    private final TransportVersion version;
    private final ThreadPool threadPool;
    private final HandshakeRequestSender handshakeRequestSender;
    private final boolean ignoreDeserializationErrors;

    TransportHandshaker(
        TransportVersion version,
        ThreadPool threadPool,
        HandshakeRequestSender handshakeRequestSender,
        boolean ignoreDeserializationErrors
    ) {
        this.version = version;
        this.threadPool = threadPool;
        this.handshakeRequestSender = handshakeRequestSender;
        this.ignoreDeserializationErrors = ignoreDeserializationErrors;
    }

    void sendHandshake(
        long requestId,
        DiscoveryNode node,
        TcpChannel channel,
        TimeValue timeout,
        ActionListener<TransportVersion> listener
    ) {
        numHandshakes.inc();
        final HandshakeResponseHandler handler = new HandshakeResponseHandler(requestId, channel, listener);
        pendingHandshakes.put(requestId, handler);
        channel.addCloseListener(
            ActionListener.running(() -> handler.handleLocalException(new TransportException("handshake failed because connection reset")))
        );
        boolean success = false;
        try {
            handshakeRequestSender.sendRequest(node, channel, requestId, V9_HANDSHAKE_VERSION);

            threadPool.schedule(
                () -> handler.handleLocalException(new ConnectTransportException(node, "handshake_timeout[" + timeout + "]")),
                timeout,
                threadPool.generic()
            );
            success = true;
        } catch (Exception e) {
            handler.handleLocalException(new ConnectTransportException(node, "failure to send " + HANDSHAKE_ACTION_NAME, e));
        } finally {
            if (success == false) {
                TransportResponseHandler<?> removed = pendingHandshakes.remove(requestId);
                assert removed == null : "Handshake should not be pending if exception was thrown";
            }
        }
    }

    void handleHandshake(TransportChannel channel, long requestId, StreamInput stream) throws IOException {
        final HandshakeRequest handshakeRequest;
        try {
            handshakeRequest = new HandshakeRequest(stream);
        } catch (Exception e) {
            assert ignoreDeserializationErrors : e;
            throw e;
        }
        final int nextByte = stream.read();
        if (nextByte != -1) {
            final IllegalStateException exception = new IllegalStateException(
                "Handshake request not fully read for requestId ["
                    + requestId
                    + "], action ["
                    + TransportHandshaker.HANDSHAKE_ACTION_NAME
                    + "], available ["
                    + stream.available()
                    + "]; resetting"
            );
            assert ignoreDeserializationErrors : exception;
            throw exception;
        }
        channel.sendResponse(
            new HandshakeResponse(
                ensureCompatibleVersion(version, handshakeRequest.transportVersion, handshakeRequest.releaseVersion, channel),
                Build.current().version()
            )
        );
    }

    private static TransportVersion ensureCompatibleVersion(
        TransportVersion localTransportVersion,
        TransportVersion remoteTransportVersion,
        String releaseVersion,
        Object channel
    ) {
        if (TransportVersion.isCompatible(remoteTransportVersion)) {
            if (remoteTransportVersion.onOrAfter(localTransportVersion)) {
                // Remote is semantically newer than us (i.e. has a greater transport protocol version), so we propose using our current
                // transport protocol version. If we're initiating the connection then that's the version we'll use; if the other end is
                // initiating the connection then it's up to the other end to decide whether to use this version (if it knows it) or
                // an earlier one.
                return localTransportVersion;
            }
            final var bestKnownVersion = remoteTransportVersion.bestKnownVersion();
            if (bestKnownVersion.equals(TransportVersions.ZERO) == false) {
                if (bestKnownVersion.equals(remoteTransportVersion) == false) {
                    // Remote is semantically older than us (i.e. has a lower transport protocol version), but we do not know its exact
                    // transport protocol version so it must be chronologically newer. We recommend not doing this, it implies an upgrade
                    // that goes backwards in time and therefore may regress in some way, so we emit a warning. But we carry on with the
                    // best known version anyway since both ends will know it.
                    logger.warn(
                        """
                            Negotiating transport handshake with remote node with version [{}/{}] received on [{}] which appears to be \
                            from a chronologically-older release with a numerically-newer version compared to this node's version [{}/{}]. \
                            Upgrading to a chronologically-older release may not work reliably and is not recommended. \
                            Falling back to transport protocol version [{}].""",
                        releaseVersion,
                        remoteTransportVersion,
                        channel,
                        Build.current().version(),
                        localTransportVersion,
                        bestKnownVersion
                    );
                } // else remote is semantically older and we _do_ know its version, so we just use that without further fuss.
                return bestKnownVersion;
            }
        }

        final var message = Strings.format(
            """
                Rejecting unreadable transport handshake from remote node with version [%s/%s] received on [%s] since this node has \
                version [%s/%s] which has an incompatible wire format.""",
            releaseVersion,
            remoteTransportVersion,
            channel,
            Build.current().version(),
            localTransportVersion
        );
        logger.warn(message);
        throw new IllegalStateException(message);

    }

    TransportResponseHandler<HandshakeResponse> removeHandlerForHandshake(long requestId) {
        return pendingHandshakes.remove(requestId);
    }

    int getNumPendingHandshakes() {
        return pendingHandshakes.size();
    }

    long getNumHandshakes() {
        return numHandshakes.count();
    }

    private class HandshakeResponseHandler implements TransportResponseHandler<HandshakeResponse> {

        private final long requestId;
        private final TcpChannel channel;
        private final ActionListener<TransportVersion> listener;
        private final AtomicBoolean isDone = new AtomicBoolean(false);

        private HandshakeResponseHandler(long requestId, TcpChannel channel, ActionListener<TransportVersion> listener) {
            this.requestId = requestId;
            this.channel = channel;
            this.listener = listener;
        }

        @Override
        public HandshakeResponse read(StreamInput in) throws IOException {
            return new HandshakeResponse(in);
        }

        @Override
        public Executor executor() {
            return TransportResponseHandler.TRANSPORT_WORKER;
        }

        @Override
        public void handleResponse(HandshakeResponse response) {
            if (isDone.compareAndSet(false, true)) {
                ActionListener.completeWith(listener, () -> {
                    final var resultVersion = ensureCompatibleVersion(
                        version,
                        response.getTransportVersion(),
                        response.getReleaseVersion(),
                        channel
                    );
                    assert TransportVersion.current().before(version) // simulating a newer-version transport service for test purposes
                        || resultVersion.isKnown() : "negotiated unknown version " + resultVersion;
                    return resultVersion;
                });
            }
        }

        @Override
        public void handleException(TransportException e) {
            if (isDone.compareAndSet(false, true)) {
                listener.onFailure(new IllegalStateException("handshake failed", e));
            }
        }

        void handleLocalException(TransportException e) {
            if (removeHandlerForHandshake(requestId) != null && isDone.compareAndSet(false, true)) {
                listener.onFailure(e);
            }
        }
    }

    static final class HandshakeRequest extends AbstractTransportRequest {

        /**
         * The {@link TransportVersion#current()} of the requesting node.
         */
        final TransportVersion transportVersion;

        /**
         * The {@link Build#version()} of the requesting node, as a {@link String}, for better reporting of handshake failures due to
         * an incompatible version.
         */
        final String releaseVersion;

        HandshakeRequest(TransportVersion transportVersion, String releaseVersion) {
            this.transportVersion = Objects.requireNonNull(transportVersion);
            this.releaseVersion = Objects.requireNonNull(releaseVersion);
        }

        HandshakeRequest(StreamInput streamInput) throws IOException {
            super(streamInput);

            try (StreamInput messageStreamInput = streamInput.readSlicedBytesReference().streamInput()) {
                this.transportVersion = TransportVersion.readVersion(messageStreamInput);
                if (streamInput.getTransportVersion().onOrAfter(V9_HANDSHAKE_VERSION)) {
                    this.releaseVersion = messageStreamInput.readString();
                } else {
                    this.releaseVersion = this.transportVersion.toReleaseVersion();
                }
            }

        }

        @Override
        public void writeTo(StreamOutput streamOutput) throws IOException {
            super.writeTo(streamOutput);
            assert transportVersion != null;
            try (BytesStreamOutput messageStreamOutput = new BytesStreamOutput(1024)) {
                TransportVersion.writeVersion(transportVersion, messageStreamOutput);
                if (streamOutput.getTransportVersion().onOrAfter(V9_HANDSHAKE_VERSION)) {
                    messageStreamOutput.writeString(releaseVersion);
                } // else we just send the transport version and rely on a best-effort mapping to release versions
                BytesReference reference = messageStreamOutput.bytes();
                streamOutput.writeBytesReference(reference);
            }
        }
    }

    /**
     * A response to a low-level transport handshake, carrying information about the version of the responding node.
     */
    static final class HandshakeResponse extends TransportResponse {

        /**
         * The {@link TransportVersion#current()} of the responding node.
         */
        private final TransportVersion transportVersion;

        /**
         * The {@link Build#version()} of the responding node, as a {@link String}, for better reporting of handshake failures due to
         * an incompatible version.
         */
        private final String releaseVersion;

        HandshakeResponse(TransportVersion transportVersion, String releaseVersion) {
            this.transportVersion = Objects.requireNonNull(transportVersion);
            this.releaseVersion = Objects.requireNonNull(releaseVersion);
        }

        HandshakeResponse(StreamInput in) throws IOException {
            transportVersion = TransportVersion.readVersion(in);
            if (in.getTransportVersion().onOrAfter(V9_HANDSHAKE_VERSION)) {
                releaseVersion = in.readString();
            } else {
                releaseVersion = transportVersion.toReleaseVersion();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportVersion.writeVersion(transportVersion, out);
            if (out.getTransportVersion().onOrAfter(V9_HANDSHAKE_VERSION)) {
                out.writeString(releaseVersion);
            } // else we just send the transport version and rely on a best-effort mapping to release versions
        }

        /**
         * @return the {@link TransportVersion#current()} of the responding node.
         */
        TransportVersion getTransportVersion() {
            return transportVersion;
        }

        /**
         * @return the {@link Build#version()} of the responding node, as a {@link String}, for better reporting of handshake failures due
         * to an incompatible version.
         */
        String getReleaseVersion() {
            return releaseVersion;
        }
    }

    @FunctionalInterface
    interface HandshakeRequestSender {
        /**
         * @param node                      The (expected) remote node, for error reporting and passing to
         *                                  {@link TransportMessageListener#onRequestSent}.
         * @param channel                   The TCP channel to use to send the handshake request.
         * @param requestId                 The transport request ID, for matching up the response.
         * @param handshakeTransportVersion The {@link TransportVersion} to use for the handshake request, which will be
         *                                  {@link TransportHandshaker#V8_HANDSHAKE_VERSION} in production.
         */
        void sendRequest(DiscoveryNode node, TcpChannel channel, long requestId, TransportVersion handshakeTransportVersion)
            throws IOException;
    }
}
