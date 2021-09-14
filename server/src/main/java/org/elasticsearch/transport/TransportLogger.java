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
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.IOException;

public final class TransportLogger {

    private static final Logger logger = LogManager.getLogger(TransportLogger.class);
    private static final int HEADER_SIZE = TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE;

    static void logInboundMessage(TcpChannel channel, BytesReference message) {
        if (logger.isTraceEnabled()) {
            try {
                String logMessage = format(channel, message, "READ");
                logger.trace(logMessage);
            } catch (IOException e) {
                logger.warn("an exception occurred formatting a READ trace message", e);
            }
        }
    }

    static void logInboundMessage(TcpChannel channel, InboundMessage message) {
        if (logger.isTraceEnabled()) {
            try {
                String logMessage = format(channel, message, "READ");
                logger.trace(logMessage);
            } catch (IOException e) {
                logger.warn("an exception occurred formatting a READ trace message", e);
            }
        }
    }

    static void logOutboundMessage(TcpChannel channel, BytesReference message) {
        if (logger.isTraceEnabled()) {
            try {
                if (message.get(0) != 'E') {
                    // This is not an Elasticsearch transport message.
                    return;
                }
                BytesReference withoutHeader = message.slice(HEADER_SIZE, message.length() - HEADER_SIZE);
                String logMessage = format(channel, withoutHeader, "WRITE");
                logger.trace(logMessage);
            } catch (IOException e) {
                logger.warn("an exception occurred formatting a WRITE trace message", e);
            }
        }
    }

    private static String format(TcpChannel channel, BytesReference message, String event) throws IOException {
        final StringBuilder sb = new StringBuilder();
        sb.append(channel);
        int messageLengthWithHeader = HEADER_SIZE + message.length();
        // This is a ping
        if (message.length() == 0) {
            sb.append(" [ping]").append(' ').append(event).append(": ").append(messageLengthWithHeader).append('B');
        } else {
            boolean success = false;
            StreamInput streamInput = message.streamInput();
            try {
                final long requestId = streamInput.readLong();
                final byte status = streamInput.readByte();
                final boolean isRequest = TransportStatus.isRequest(status);
                final String type = isRequest ? "request" : "response";
                final Version version = Version.fromId(streamInput.readInt());
                streamInput.setVersion(version);
                sb.append(" [length: ").append(messageLengthWithHeader);
                sb.append(", request id: ").append(requestId);
                sb.append(", type: ").append(type);
                sb.append(", version: ").append(version);

                if (version.onOrAfter(TcpHeader.VERSION_WITH_HEADER_SIZE)) {
                    sb.append(", header size: ").append(streamInput.readInt()).append('B');
                } else {
                    streamInput = decompressingStream(status, streamInput);
                    InboundHandler.assertRemoteVersion(streamInput, version);
                }

                // read and discard headers
                ThreadContext.readHeadersFromStream(streamInput);

                if (isRequest) {
                    if (version.before(Version.V_8_0_0)) {
                        // discard features
                        streamInput.readStringArray();
                    }
                    sb.append(", action: ").append(streamInput.readString());
                }
                sb.append(']');
                sb.append(' ').append(event).append(": ").append(messageLengthWithHeader).append('B');
                success = true;
            } finally {
                if (success) {
                    IOUtils.close(streamInput);
                } else {
                    IOUtils.closeWhileHandlingException(streamInput);
                }
            }
        }
        return sb.toString();
    }

    private static String format(TcpChannel channel, InboundMessage message, String event) throws IOException {
        final StringBuilder sb = new StringBuilder();
        sb.append(channel);

        if (message.isPing()) {
            sb.append(" [ping]").append(' ').append(event).append(": ").append(6).append('B');
        } else {
            boolean success = false;
            Header header = message.getHeader();
            int networkMessageSize = header.getNetworkMessageSize();
            int messageLengthWithHeader = HEADER_SIZE + networkMessageSize;
            StreamInput streamInput = message.openOrGetStreamInput();
            try {
                final long requestId = header.getRequestId();
                final boolean isRequest = header.isRequest();
                final String type = isRequest ? "request" : "response";
                final String version = header.getVersion().toString();
                sb.append(" [length: ").append(messageLengthWithHeader);
                sb.append(", request id: ").append(requestId);
                sb.append(", type: ").append(type);
                sb.append(", version: ").append(version);

                // TODO: Maybe Fix for BWC
                if (header.needsToReadVariableHeader() == false && isRequest) {
                    sb.append(", action: ").append(header.getActionName());
                }
                sb.append(']');
                sb.append(' ').append(event).append(": ").append(messageLengthWithHeader).append('B');
                success = true;
            } finally {
                if (success) {
                    IOUtils.close(streamInput);
                } else {
                    IOUtils.closeWhileHandlingException(streamInput);
                }
            }
        }
        return sb.toString();
    }

    private static StreamInput decompressingStream(byte status, StreamInput streamInput) throws IOException {
        if (TransportStatus.isCompress(status) && streamInput.available() > 0) {
            try {
                return new InputStreamStreamInput(CompressorFactory.COMPRESSOR.threadLocalInputStream(streamInput));
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("stream marked as compressed, but is missing deflate header");
            }
        } else {
            return streamInput;
        }
    }
}
