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

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;

public abstract class InboundMessage extends NetworkMessage implements Closeable {

    private final StreamInput streamInput;

    InboundMessage(ThreadContext threadContext, Version version, byte status, long requestId, StreamInput streamInput) {
        super(threadContext, version, status, requestId);
        this.streamInput = streamInput;
    }

    StreamInput getStreamInput() {
        return streamInput;
    }

    static class Reader {

        private final Version version;
        private final NamedWriteableRegistry namedWriteableRegistry;
        private final ThreadContext threadContext;

        Reader(Version version, NamedWriteableRegistry namedWriteableRegistry, ThreadContext threadContext) {
            this.version = version;
            this.namedWriteableRegistry = namedWriteableRegistry;
            this.threadContext = threadContext;
        }

        InboundMessage deserialize(BytesReference reference) throws IOException {
            int messageLengthBytes = reference.length();
            final int totalMessageSize = messageLengthBytes + TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE;
            // we have additional bytes to read, outside of the header
            boolean hasMessageBytesToRead = (totalMessageSize - TcpHeader.HEADER_SIZE) > 0;
            StreamInput streamInput = reference.streamInput();
            boolean success = false;
            try (ThreadContext.StoredContext existing = threadContext.stashContext()) {
                long requestId = streamInput.readLong();
                byte status = streamInput.readByte();
                Version remoteVersion = Version.fromId(streamInput.readInt());
                final boolean isHandshake = TransportStatus.isHandshake(status);
                ensureVersionCompatibility(remoteVersion, version, isHandshake);
                if (TransportStatus.isCompress(status) && hasMessageBytesToRead && streamInput.available() > 0) {
                    Compressor compressor = getCompressor(reference);
                    if (compressor == null) {
                        int maxToRead = Math.min(reference.length(), 10);
                        StringBuilder sb = new StringBuilder("stream marked as compressed, but no compressor found, first [")
                            .append(maxToRead).append("] content bytes out of [").append(reference.length())
                            .append("] readable bytes with message size [").append(messageLengthBytes).append("] ").append("] are [");
                        for (int i = 0; i < maxToRead; i++) {
                            sb.append(reference.get(i)).append(",");
                        }
                        sb.append("]");
                        throw new IllegalStateException(sb.toString());
                    }
                    streamInput = compressor.streamInput(streamInput);
                }
                streamInput = new NamedWriteableAwareStreamInput(streamInput, namedWriteableRegistry);
                streamInput.setVersion(remoteVersion);

                threadContext.readHeaders(streamInput);

                InboundMessage message;
                if (TransportStatus.isRequest(status)) {
                    if (remoteVersion.before(Version.V_8_0_0)) {
                        // discard features
                        streamInput.readStringArray();
                    }
                    final String action = streamInput.readString();
                    message = new Request(threadContext, remoteVersion, status, requestId, action, streamInput);
                } else {
                    message = new Response(threadContext, remoteVersion, status, requestId, streamInput);
                }
                success = true;
                return message;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(streamInput);
                }
            }
        }
    }

    @Nullable
    static Compressor getCompressor(BytesReference message) {
        final int offset = TcpHeader.REQUEST_ID_SIZE + TcpHeader.STATUS_SIZE + TcpHeader.VERSION_ID_SIZE;
        return CompressorFactory.COMPRESSOR.isCompressed(message.slice(offset, message.length() - offset))
            ? CompressorFactory.COMPRESSOR : null;
    }

    @Override
    public void close() throws IOException {
        streamInput.close();
    }

    private static void ensureVersionCompatibility(Version version, Version currentVersion, boolean isHandshake) {
        // for handshakes we are compatible with N-2 since otherwise we can't figure out our initial version
        // since we are compatible with N-1 and N+1 so we always send our minCompatVersion as the initial version in the
        // handshake. This looks odd but it's required to establish the connection correctly we check for real compatibility
        // once the connection is established
        final Version compatibilityVersion = isHandshake ? currentVersion.minimumCompatibilityVersion() : currentVersion;
        if (version.isCompatible(compatibilityVersion) == false) {
            final Version minCompatibilityVersion = isHandshake ? compatibilityVersion : compatibilityVersion.minimumCompatibilityVersion();
            String msg = "Received " + (isHandshake ? "handshake " : "") + "message from unsupported version: [";
            throw new IllegalStateException(msg + version + "] minimal compatible version is: [" + minCompatibilityVersion + "]");
        }
    }

    public static class Request extends InboundMessage {

        private final String actionName;

        Request(ThreadContext threadContext, Version version, byte status, long requestId, String actionName,
                StreamInput streamInput) {
            super(threadContext, version, status, requestId, streamInput);
            this.actionName = actionName;
        }

        String getActionName() {
            return actionName;
        }

    }

    public static class Response extends InboundMessage {

        Response(ThreadContext threadContext, Version version, byte status, long requestId, StreamInput streamInput) {
            super(threadContext, version, status, requestId, streamInput);
        }
    }
}
