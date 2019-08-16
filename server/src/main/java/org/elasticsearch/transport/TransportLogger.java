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
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.NotCompressedException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
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
                logger.trace("an exception occurred formatting a READ trace message", e);
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
                logger.trace("an exception occurred formatting a WRITE trace message", e);
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
                final String version = Version.fromId(streamInput.readInt()).toString();
                sb.append(" [length: ").append(messageLengthWithHeader);
                sb.append(", request id: ").append(requestId);
                sb.append(", type: ").append(type);
                sb.append(", version: ").append(version);

                if (isRequest) {
                    if (TransportStatus.isCompress(status)) {
                        Compressor compressor;
                        compressor = InboundMessage.getCompressor(message);
                        if (compressor == null) {
                            throw new IllegalStateException(new NotCompressedException());
                        }
                        streamInput = compressor.streamInput(streamInput);
                    }

                    try (ThreadContext context = new ThreadContext(Settings.EMPTY)) {
                        context.readHeaders(streamInput);
                    }
                    if (streamInput.getVersion().before(Version.V_8_0_0)) {
                        // discard the features
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
}
