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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.EOFException;
import java.io.IOException;
import java.io.StreamCorruptedException;

public class InboundDecoder implements Releasable {

    static final Object PING = new Object();
    static final Object END_CONTENT = new Object();

    /**
     * Emitted when a header exceeds {@code maxHeaderSize} but not {@code maxAuthorizedHeaderSize} and its first request header is the
     * leading request header. The decoder consumes nothing more until {@link #authorizeOversizedHeader()} is called.
     */
    public record OversizedHeaderCredentials(String value) {}

    private enum OversizedHeaderState {
        NONE,
        AWAITING_AUTHORIZATION,
        AUTHORIZED
    }

    private final Recycler<BytesRef> recycler;
    private TransportDecompressor decompressor;
    private int totalNetworkSize = -1;
    private int bytesConsumed = 0;
    private boolean isCompressed = false;
    private boolean isClosed = false;
    private OversizedHeaderState oversizedHeaderState = OversizedHeaderState.NONE;
    private final ByteSizeValue maxHeaderSize;
    private final ByteSizeValue maxAuthorizedHeaderSize;
    @Nullable
    private final String leadingRequestHeader;
    private final ChannelType channelType;

    public InboundDecoder(Recycler<BytesRef> recycler) {
        this(recycler, ByteSizeValue.of(2, ByteSizeUnit.GB), ChannelType.MIX);
    }

    public InboundDecoder(Recycler<BytesRef> recycler, ChannelType channelType) {
        this(recycler, ByteSizeValue.of(2, ByteSizeUnit.GB), channelType);
    }

    public InboundDecoder(Recycler<BytesRef> recycler, ByteSizeValue maxHeaderSize, ChannelType channelType) {
        this(recycler, maxHeaderSize, maxHeaderSize, null, channelType);
    }

    /**
     * @param maxAuthorizedHeaderSize the largest header accepted once the value of {@code leadingRequestHeader}, which must be the
     *                                first request header of an oversized header, has been authorized
     */
    public InboundDecoder(
        Recycler<BytesRef> recycler,
        ByteSizeValue maxHeaderSize,
        ByteSizeValue maxAuthorizedHeaderSize,
        @Nullable String leadingRequestHeader,
        ChannelType channelType
    ) {
        this.recycler = recycler;
        this.maxHeaderSize = maxHeaderSize;
        this.maxAuthorizedHeaderSize = maxAuthorizedHeaderSize;
        this.leadingRequestHeader = leadingRequestHeader;
        this.channelType = channelType;
    }

    /**
     * Lets decoding continue past the oversized header announced by the last {@link OversizedHeaderCredentials} fragment.
     */
    public void authorizeOversizedHeader() {
        assert oversizedHeaderState == OversizedHeaderState.AWAITING_AUTHORIZATION : oversizedHeaderState;
        oversizedHeaderState = OversizedHeaderState.AUTHORIZED;
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
                int headerBytesToRead = headerBytesToRead(reference, fragmentConsumer);
                if (headerBytesToRead == 0) {
                    return 0;
                } else {
                    totalNetworkSize = messageLength + TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE;
                    oversizedHeaderState = OversizedHeaderState.NONE;

                    Header header = readHeader(messageLength, reference, channelType);
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
            oversizedHeaderState = OversizedHeaderState.NONE;
        }
    }

    private boolean isDone() {
        return bytesConsumed == totalNetworkSize;
    }

    private int headerBytesToRead(BytesReference reference, CheckedConsumer<Object, IOException> fragmentConsumer) throws IOException {
        if (reference.length() < TcpHeader.BYTES_REQUIRED_FOR_VERSION) {
            return 0;
        }

        if (reference.length() <= TcpHeader.HEADER_SIZE) {
            return 0;
        } else {
            int variableHeaderSize = reference.getInt(TcpHeader.VARIABLE_HEADER_SIZE_POSITION);
            if (variableHeaderSize < 0) {
                throw new StreamCorruptedException("invalid negative variable header size: " + variableHeaderSize);
            }
            int totalHeaderSize = TcpHeader.HEADER_SIZE + variableHeaderSize;
            if (totalHeaderSize > maxHeaderSize.getBytes()
                && oversizedHeaderMayProceed(reference, totalHeaderSize, fragmentConsumer) == false) {
                return 0;
            }
            if (totalHeaderSize > reference.length()) {
                return 0;
            } else {
                return totalHeaderSize;
            }
        }
    }

    private boolean oversizedHeaderMayProceed(
        BytesReference reference,
        int totalHeaderSize,
        CheckedConsumer<Object, IOException> fragmentConsumer
    ) throws IOException {
        return switch (oversizedHeaderState) {
            case AUTHORIZED -> true;
            case AWAITING_AUTHORIZATION -> false;
            case NONE -> {
                if (leadingRequestHeader == null) {
                    throw headerTooLarge(totalHeaderSize, maxHeaderSize);
                }
                if (totalHeaderSize > maxAuthorizedHeaderSize.getBytes()) {
                    throw headerTooLarge(totalHeaderSize, maxAuthorizedHeaderSize);
                }
                final String credentials = readLeadingRequestHeader(reference, totalHeaderSize);
                if (credentials == null) {
                    // the credentials must be found within the limit that applies before authentication
                    if (reference.length() >= maxHeaderSize.getBytes()) {
                        throw headerTooLarge(totalHeaderSize, maxHeaderSize);
                    }
                    yield false;
                }
                oversizedHeaderState = OversizedHeaderState.AWAITING_AUTHORIZATION;
                fragmentConsumer.accept(new OversizedHeaderCredentials(credentials));
                yield false;
            }
        };
    }

    /**
     * Reads the value of the leading request header from the start of the variable header, which begins with the request headers map.
     * Returns {@code null} if more bytes are needed, and fails if the first request header is any other header.
     */
    @Nullable
    private String readLeadingRequestHeader(BytesReference reference, int totalHeaderSize) throws IOException {
        final int available = Math.min(reference.length(), totalHeaderSize) - TcpHeader.HEADER_SIZE;
        try (StreamInput in = reference.slice(TcpHeader.HEADER_SIZE, available).streamInput()) {
            if (in.readVInt() == 0 || leadingRequestHeader.equals(in.readString()) == false) {
                throw headerTooLarge(totalHeaderSize, maxHeaderSize);
            }
            return in.readString();
        } catch (EOFException e) {
            return null;
        }
    }

    private static StreamCorruptedException headerTooLarge(int totalHeaderSize, ByteSizeValue limit) {
        return new StreamCorruptedException("header size [" + totalHeaderSize + "] exceeds limit of [" + limit + "]");
    }

    private static Header readHeader(int networkMessageSize, BytesReference bytesReference, ChannelType channelType) throws IOException {
        try (StreamInput streamInput = bytesReference.streamInput()) {
            streamInput.skip(TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE);
            long requestId = streamInput.readLong();
            byte status = streamInput.readByte();
            int remoteVersion = streamInput.readInt();

            Header header = new Header(networkMessageSize, requestId, status, TransportVersion.fromId(remoteVersion));
            if (channelType == ChannelType.SERVER && header.isResponse()) {
                throw new IllegalArgumentException("server channels do not accept inbound responses, only requests, closing channel");
            } else if (channelType == ChannelType.CLIENT && header.isRequest()) {
                throw new IllegalArgumentException("client channels do not accept inbound requests, only responses, closing channel");
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
                    + TransportVersion.minimumCompatible().toReleaseVersion()
                    + "]"
            );
        }
    }

    public enum ChannelType {
        SERVER,
        CLIENT,
        MIX
    }
}
