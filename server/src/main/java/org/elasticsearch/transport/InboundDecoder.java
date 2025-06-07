/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.io.StreamCorruptedException;

public class InboundDecoder implements Releasable {

    static final Object PING = new Object();
    static final Object END_CONTENT = new Object();

    private final Recycler<BytesRef> recycler;
    private TransportDecompressor decompressor;
    private int totalNetworkSize = -1;
    private int bytesConsumed = 0;
    private boolean isCompressed = false;
    private boolean isClosed = false;
    private final int maxHeaderSize;
    private final boolean isServerChannel;

    public InboundDecoder(Recycler<BytesRef> recycler) {
        this(recycler, Integer.MAX_VALUE, false);
    }

    public InboundDecoder(Recycler<BytesRef> recycler, int maxHeaderSize, boolean isServerChannel) {
        this.recycler = recycler;
        this.maxHeaderSize = maxHeaderSize;
        this.isServerChannel = isServerChannel;
    }

    public int decode(ReleasableBytesReference reference, CheckedConsumer<Object, IOException> fragmentConsumer) throws IOException {
        ensureOpen();
        try {
            return internalDecode(reference, fragmentConsumer);
        } catch (Exception e) {
            cleanDecodeState();
            throw e;
        }
    }

    public int internalDecode(ReleasableBytesReference reference, CheckedConsumer<Object, IOException> fragmentConsumer)
        throws IOException {
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
            int bytesConsumedThisDecode = 0;
            if (decompressor != null) {
                bytesConsumedThisDecode += decompressor.decompress(
                    maxBytesToConsume == remainingToConsume ? reference.slice(0, maxBytesToConsume) : reference
                );
                bytesConsumed += bytesConsumedThisDecode;
                ReleasableBytesReference decompressed;
                while ((decompressed = decompressor.pollDecompressedPage(isDone())) != null) {
                    try (var buf = decompressed) {
                        fragmentConsumer.accept(buf);
                    }
                }
            } else {
                bytesConsumedThisDecode += maxBytesToConsume;
                bytesConsumed += maxBytesToConsume;
                if (maxBytesToConsume == remainingToConsume) {
                    fragmentConsumer.accept(reference.slice(0, maxBytesToConsume));
                } else {
                    fragmentConsumer.accept(reference);
                }
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

    private void finishMessage(CheckedConsumer<Object, IOException> fragmentConsumer) throws IOException {
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

    private boolean isDone() {
        return bytesConsumed == totalNetworkSize;
    }

    private int headerBytesToRead(BytesReference reference) throws StreamCorruptedException {
        if (reference.length() <= TcpHeader.HEADER_SIZE) {
            return 0;
        } else {
            int variableHeaderSize = reference.getInt(TcpHeader.VARIABLE_HEADER_SIZE_POSITION);
            if (variableHeaderSize < 0) {
                throw new StreamCorruptedException("invalid negative variable header size: " + variableHeaderSize);
            }
            int totalHeaderSize = TcpHeader.HEADER_SIZE + variableHeaderSize;
            if (totalHeaderSize > maxHeaderSize) {
                throw new StreamCorruptedException("header size [" + totalHeaderSize + "] exceeds limit of [" + maxHeaderSize + "]");
            }
            if (totalHeaderSize > reference.length()) {
                return 0;
            } else {
                return totalHeaderSize;
            }
        }
    }

    private Header readHeader(int networkMessageSize, BytesReference bytesReference) throws IOException {
        try (StreamInput streamInput = bytesReference.streamInput()) {
            streamInput.skip(TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE);
            long requestId = streamInput.readLong();
            byte status = streamInput.readByte();
            int remoteVersion = streamInput.readInt();

            Header header = new Header(networkMessageSize, requestId, status, TransportVersion.fromId(remoteVersion));
            if (isServerChannel && header.isResponse()) {
                throw new IllegalArgumentException("server channels do not accept inbound responses, only requests, closing channel");
            }
            if (header.isHandshake()) {
                checkHandshakeVersionCompatibility(header.getVersion());
            } else {
                checkVersionCompatibility(header.getVersion());
            }

            // Skip since we already have ensured enough data available
            streamInput.readInt();
            header.finishParsingHeader(streamInput);
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
                    + remoteVersion.toReleaseVersion()
                    + "] minimal compatible version is: ["
                    + TransportVersions.MINIMUM_COMPATIBLE.toReleaseVersion()
                    + "]"
            );
        }
    }

}
