/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
     * This version supports two handshake protocols, v6080099 and v7170099, which respectively have the same message structure as the
     * transport protocols of v6.8.0 and v7.17.0. This node only sends v7170099 requests, but it can send a valid response to any v6080099
     * requests that it receives.
     *
     * Here are some example messages, broken down to show their structure:
     *
     * ## v6080099 Request:
     *
     * 45 53                            -- 'ES' marker
     * 00 00 00 34                      -- total message length
     *    00 00 00 00 00 00 00 01       -- request ID
     *    08                            -- status flags (0b1000 == handshake request)
     *    00 5c c6 63                   -- handshake protocol version (0x5cc663 == 6080099)
     *    00                            -- no request headers [1]
     *    00                            -- no response headers [1]
     *    01                            -- one feature [2]
     *       06                         -- feature name length
     *          78 2d 70 61 63 6b       -- feature name 'x-pack'
     *    16                            -- action string size
     *       69 6e 74 65 72 6e 61 6c    }
     *       3a 74 63 70 2f 68 61 6e    }- ASCII representation of HANDSHAKE_ACTION_NAME
     *       64 73 68 61 6b 65          }
     *    00                            -- no parent task ID [3]
     *    04                            -- payload length
     *       8b d5 b5 03                -- max acceptable protocol version (vInt: 00000011 10110101 11010101 10001011 == 7170699)
     *
     * ## v6080099 Response:
     *
     * 45 53                            -- 'ES' marker
     * 00 00 00 13                      -- total message length
     *    00 00 00 00 00 00 00 01       -- request ID (copied from request)
     *    09                            -- status flags (0b1001 == handshake response)
     *    00 5c c6 63                   -- handshake protocol version (0x5cc663 == 6080099, copied from request)
     *    00                            -- no request headers [1]
     *    00                            -- no response headers [1]
     *    c3 f9 eb 03                   -- max acceptable protocol version (vInt: 00000011 11101011 11111001 11000011 == 8060099)
     *
     *
     * ## v7170099 Request:
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
     * ## v7170099 Response:
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
     * [1] Thread context headers should be empty; see org.elasticsearch.common.util.concurrent.ThreadContext.ThreadContextStruct.writeTo
     *     for their structure.
     * [2] A list of strings, which can safely be ignored
     * [3] Parent task ID should be empty; see org.elasticsearch.tasks.TaskId.writeTo for its structure.
     */

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
        final HandshakeResponseHandler handler = new HandshakeResponseHandler(requestId, version, listener);
        pendingHandshakes.put(requestId, handler);
        channel.addCloseListener(
            ActionListener.wrap(() -> handler.handleLocalException(new TransportException("handshake failed because connection reset")))
        );
        boolean success = false;
        try {
            // for the request we use the minCompatVersion since we don't know what's the version of the node we talk to
            // we also have no payload on the request but the response will contain the actual version of the node we talk
            // to as the payload.
            TransportVersion minCompatVersion = version.calculateMinimumCompatVersion();
            handshakeRequestSender.sendRequest(node, channel, requestId, minCompatVersion);

            threadPool.schedule(
                () -> handler.handleLocalException(new ConnectTransportException(node, "handshake_timeout[" + timeout + "]")),
                timeout,
                ThreadPool.Names.GENERIC
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
        try {
            // Must read the handshake request to exhaust the stream
            new HandshakeRequest(stream);
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
        channel.sendResponse(new HandshakeResponse(this.version));
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
        private final TransportVersion currentVersion;
        private final ActionListener<TransportVersion> listener;
        private final AtomicBoolean isDone = new AtomicBoolean(false);

        private HandshakeResponseHandler(long requestId, TransportVersion currentVersion, ActionListener<TransportVersion> listener) {
            this.requestId = requestId;
            this.currentVersion = currentVersion;
            this.listener = listener;
        }

        @Override
        public HandshakeResponse read(StreamInput in) throws IOException {
            return new HandshakeResponse(in);
        }

        @Override
        public void handleResponse(HandshakeResponse response) {
            if (isDone.compareAndSet(false, true)) {
                TransportVersion responseVersion = response.responseVersion;
                if (currentVersion.isCompatible(responseVersion) == false) {
                    listener.onFailure(
                        new IllegalStateException(
                            "Received message from unsupported version: ["
                                + responseVersion
                                + "] minimal compatible version is: ["
                                + currentVersion.calculateMinimumCompatVersion()
                                + "]"
                        )
                    );
                } else {
                    listener.onResponse(responseVersion);
                }
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

    static final class HandshakeRequest extends TransportRequest {

        private final TransportVersion version;

        HandshakeRequest(TransportVersion version) {
            this.version = version;
        }

        HandshakeRequest(StreamInput streamInput) throws IOException {
            super(streamInput);
            BytesReference remainingMessage;
            try {
                remainingMessage = streamInput.readBytesReference();
            } catch (EOFException e) {
                remainingMessage = null;
            }
            if (remainingMessage == null) {
                version = null;
            } else {
                try (StreamInput messageStreamInput = remainingMessage.streamInput()) {
                    this.version = TransportVersion.readVersion(messageStreamInput);
                }
            }
        }

        @Override
        public void writeTo(StreamOutput streamOutput) throws IOException {
            super.writeTo(streamOutput);
            assert version != null;
            try (BytesStreamOutput messageStreamOutput = new BytesStreamOutput(4)) {
                TransportVersion.writeVersion(version, messageStreamOutput);
                BytesReference reference = messageStreamOutput.bytes();
                streamOutput.writeBytesReference(reference);
            }
        }
    }

    static final class HandshakeResponse extends TransportResponse {

        private final TransportVersion responseVersion;

        HandshakeResponse(TransportVersion responseVersion) {
            this.responseVersion = responseVersion;
        }

        private HandshakeResponse(StreamInput in) throws IOException {
            super(in);
            responseVersion = TransportVersion.readVersion(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert responseVersion != null;
            TransportVersion.writeVersion(responseVersion, out);
        }

        TransportVersion getResponseVersion() {
            return responseVersion;
        }
    }

    @FunctionalInterface
    interface HandshakeRequestSender {

        void sendRequest(DiscoveryNode node, TcpChannel channel, long requestId, TransportVersion version) throws IOException;
    }
}
