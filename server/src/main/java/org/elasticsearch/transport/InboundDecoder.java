/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.function.Consumer;

public class InboundDecoder implements Releasable {

    static final Object PING = new Object();
    static final Object END_CONTENT = new Object();

    private final TransportVersion version;
    private final Recycler<BytesRef> recycler;
    private TransportDecompressor decompressor;
    private int totalNetworkSize = -1;
    private int bytesConsumed = 0;
    private boolean isCompressed = false;
    private boolean isClosed = false;

    public InboundDecoder(TransportVersion version, Recycler<BytesRef> recycler) {
        this.version = version;
        this.recycler = recycler;
    }

    public int decode(ReleasableBytesReference reference, Consumer<Object> fragmentConsumer) throws IOException {
        ensureOpen();
        try {
            return internalDecode(reference, fragmentConsumer);
        } catch (Exception e) {
            cleanDecodeState();
            throw e;
        }
    }

    public int internalDecode(ReleasableBytesReference reference, Consumer<Object> fragmentConsumer) throws IOException {
        if (isOnHeader()) {
            int messageLength = TcpTransport.readMessageLength(reference);
            if (messageLength == -1) {
                return 0;
            } else if (messageLength == 0) {
                fragmentConsumer.accept(PING);
                return 6;
            } else {
                int headerBytesToRead = headerBytesToRead(reference);
                if (headerBytesToRead == 0) {
                    return 0;
                } else {
                    totalNetworkSize = messageLength + TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE;

                    Header header = readHeader(messageLength, reference);
                    bytesConsumed += headerBytesToRead;
                    if (header.isCompressed()) {
                        isCompressed = true;
                    }
                    fragmentConsumer.accept(header);

                    if (isDone()) {
                        finishMessage(fragmentConsumer);
                    }
                    return headerBytesToRead;
                }
            }
        } else {
            if (isCompressed && decompressor == null) {
                // Attempt to initialize decompressor
                TransportDecompressor decompressor = TransportDecompressor.getDecompressor(recycler, reference);
                if (decompressor == null) {
                    return 0;
                } else {
                    this.decompressor = decompressor;
                    fragmentConsumer.accept(this.decompressor.getScheme());
                }
            }
            int remainingToConsume = totalNetworkSize - bytesConsumed;
            int maxBytesToConsume = Math.min(reference.length(), remainingToConsume);
            ReleasableBytesReference retainedContent;
            if (maxBytesToConsume == remainingToConsume) {
                retainedContent = reference.retainedSlice(0, maxBytesToConsume);
            } else {
                retainedContent = reference.retain();
            }

            int bytesConsumedThisDecode = 0;
            if (decompressor != null) {
                bytesConsumedThisDecode += decompress(retainedContent);
                bytesConsumed += bytesConsumedThisDecode;
                ReleasableBytesReference decompressed;
                while ((decompressed = decompressor.pollDecompressedPage(isDone())) != null) {
                    fragmentConsumer.accept(decompressed);
                }
            } else {
                bytesConsumedThisDecode += maxBytesToConsume;
                bytesConsumed += maxBytesToConsume;
                fragmentConsumer.accept(retainedContent);
            }
            if (isDone()) {
                finishMessage(fragmentConsumer);
            }

            return bytesConsumedThisDecode;
        }
    }

    @Override
    public void close() {
        isClosed = true;
        cleanDecodeState();
    }

    private void finishMessage(Consumer<Object> fragmentConsumer) {
        cleanDecodeState();
        fragmentConsumer.accept(END_CONTENT);
    }

    private void cleanDecodeState() {
        try {
            Releasables.closeExpectNoException(decompressor);
        } finally {
            isCompressed = false;
            decompressor = null;
            totalNetworkSize = -1;
            bytesConsumed = 0;
        }
    }

    private int decompress(ReleasableBytesReference content) throws IOException {
        try (content) {
            return decompressor.decompress(content);
        }
    }

    private boolean isDone() {
        return bytesConsumed == totalNetworkSize;
    }

    private static int headerBytesToRead(BytesReference reference) {
        if (reference.length() < TcpHeader.BYTES_REQUIRED_FOR_VERSION) {
            return 0;
        }

        TransportVersion remoteVersion = TransportVersion.fromId(reference.getInt(TcpHeader.VERSION_POSITION));
        int fixedHeaderSize = TcpHeader.headerSize(remoteVersion);
        if (fixedHeaderSize > reference.length()) {
            return 0;
        } else if (remoteVersion.before(TcpHeader.VERSION_WITH_HEADER_SIZE)) {
            return fixedHeaderSize;
        } else {
            int variableHeaderSize = reference.getInt(TcpHeader.VARIABLE_HEADER_SIZE_POSITION);
            int totalHeaderSize = fixedHeaderSize + variableHeaderSize;
            if (totalHeaderSize > reference.length()) {
                return 0;
            } else {
                return totalHeaderSize;
            }
        }
    }

    private static Header readHeader(int networkMessageSize, BytesReference bytesReference) throws IOException {
        try (StreamInput streamInput = bytesReference.streamInput()) {
            streamInput.skip(TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE);
            long requestId = streamInput.readLong();
            byte status = streamInput.readByte();
            int remoteVersion = streamInput.readInt();

            Header header = new Header(networkMessageSize, requestId, status, TransportVersion.fromId(remoteVersion));
            if (header.isHandshake()) {
                checkHandshakeVersionCompatibility(header.getVersion());
            } else {
                checkVersionCompatibility(header.getVersion());
            }

            if (header.getVersion().onOrAfter(TcpHeader.VERSION_WITH_HEADER_SIZE)) {
                // Skip since we already have ensured enough data available
                streamInput.readInt();
                header.finishParsingHeader(streamInput);
            }
            return header;
        }
    }

    private boolean isOnHeader() {
        return totalNetworkSize == -1;
    }

    private void ensureOpen() {
        if (isClosed) {
            throw new IllegalStateException("Decoder is already closed");
        }
    }

    static void checkHandshakeVersionCompatibility(TransportVersion handshakeVersion) {
        if (TransportHandshaker.ALLOWED_HANDSHAKE_VERSIONS.contains(handshakeVersion) == false) {
            throw new IllegalStateException(
                "Received message from unsupported version: ["
                    + handshakeVersion
                    + "] allowed versions are: "
                    + TransportHandshaker.ALLOWED_HANDSHAKE_VERSIONS
            );
        }
    }

    static void checkVersionCompatibility(TransportVersion remoteVersion) {
        if (TransportVersion.isCompatible(remoteVersion) == false) {
            throw new IllegalStateException(
                "Received message from unsupported version: ["
                    + remoteVersion
                    + "] minimal compatible version is: ["
                    + TransportVersion.MINIMUM_COMPATIBLE
                    + "]"
            );
        }
    }
}
