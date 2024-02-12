/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.Compression;
import org.elasticsearch.transport.Header;
import org.elasticsearch.transport.InboundDecoder;
import org.elasticsearch.transport.InboundMessage;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TcpHeader;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportDecompressor;
import org.elasticsearch.transport.TransportHandshaker;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * A handler (must be the last one!) that does size based frame decoding and forwards the actual message
 * to the relevant action.
 */
public class Netty4MessageInboundHandler extends ByteToMessageDecoder {

    private static final InboundMessage PING_MESSAGE = new InboundMessage(null, true);

    private Exception uncaughtException;

    private int totalNetworkSize = -1;
    private int bytesConsumed = 0;

    private boolean isCompressed = false;

    private TransportDecompressor decompressor;

    private final Netty4Transport transport;

    private final InboundDecoder.ChannelType channelType = InboundDecoder.ChannelType.MIX;

    private static final ByteSizeValue maxHeaderSize = new ByteSizeValue(2, ByteSizeUnit.GB);

    private final Supplier<CircuitBreaker> circuitBreaker;
    private final Predicate<String> requestCanTripBreaker;

    private Header currentHeader;
    private Exception aggregationException;
    private boolean canTripBreaker = true;

    private boolean isOnHeader() {
        return totalNetworkSize == -1;
    }

    public Netty4MessageInboundHandler(Netty4Transport transport) {
        this.transport = transport;
        this.circuitBreaker = transport.getInflightBreaker();
        this.requestCanTripBreaker = actionName -> {
            final RequestHandlerRegistry<TransportRequest> reg = transport.getRequestHandlers().getHandler(actionName);
            if (reg == null) {
                assert transport.ignoreDeserializationErrors() : actionName;
                throw new ActionNotFoundTransportException(actionName);
            } else {
                return reg.canTripCircuitBreaker();
            }
        };
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        assert Transports.assertDefaultThreadContext(transport.getThreadPool().getThreadContext());
        ExceptionsHelper.maybeDieOnAnotherThread(cause);
        final Throwable unwrapped = ExceptionsHelper.unwrap(cause, ElasticsearchException.class);
        final Throwable newCause = unwrapped != null ? unwrapped : cause;
        Netty4TcpChannel tcpChannel = ctx.channel().attr(Netty4Transport.CHANNEL_KEY).get();
        if (newCause instanceof Error) {
            transport.onException(tcpChannel, new Exception(newCause));
        } else {
            transport.onException(tcpChannel, (Exception) newCause);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        Releasables.closeExpectNoException(this::closeCurrentAggregation);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        Netty4TcpChannel channel = ctx.channel().attr(Netty4Transport.CHANNEL_KEY).get();
        if (uncaughtException != null) {
            throw new IllegalStateException("Pipeline state corrupted by uncaught exception", uncaughtException);
        }
        try {
            doHandleBytes(channel, in);
        } catch (Exception e) {
            uncaughtException = e;
            throw e;
        }
    }

    public void doHandleBytes(TcpChannel channel, ByteBuf reference) throws IOException {
        channel.getChannelStats().markAccessed(transport.getThreadPool().relativeTimeInMillis());
        int bytesDecoded = 0;
        try {
            int decoded;
            while ((decoded = internalDecode(reference, channel)) != 0) {
                bytesDecoded += decoded;
            }
        } catch (Exception e) {
            cleanDecodeState();
            throw e;
        }
        transport.getStatsTracker().markBytesRead(bytesDecoded);
    }

    private void endContent(TcpChannel channel, ByteBuf cumulation) throws IOException {
        cleanDecodeState();
        assert isAggregating();
        InboundMessage aggregated = finishAggregation(cumulation);
        try {
            transport.getStatsTracker().markMessageReceived();
            transport.inboundMessage(channel, aggregated);
        } finally {
            aggregated.decRef();
        }
    }

    public void headerReceived(Header header) {
        assert isAggregating() == false;
        currentHeader = header;
        if (currentHeader.isRequest() && currentHeader.needsToReadVariableHeader() == false) {
            initializeRequestState();
        }
    }

    private void initializeRequestState() {
        assert currentHeader.needsToReadVariableHeader() == false;
        assert currentHeader.isRequest();
        if (currentHeader.isHandshake()) {
            canTripBreaker = false;
            return;
        }

        final String actionName = currentHeader.getActionName();
        try {
            canTripBreaker = requestCanTripBreaker.test(actionName);
        } catch (ActionNotFoundTransportException e) {
            shortCircuit(e);
        }
    }

    public void updateCompressionScheme(Compression.Scheme compressionScheme) {
        assert isAggregating();
        currentHeader.setCompressionScheme(compressionScheme);
    }

    private InboundMessage finishAggregation(ByteBuf cummulation) throws IOException {
        final ReleasableBytesReference releasableContent = cummulation == null
            ? ReleasableBytesReference.empty()
            : new ReleasableBytesReference(Netty4Utils.toBytesReference(cummulation), new ByteBufRefCounted(cummulation));

        final BreakerControl breakerControl = new BreakerControl(circuitBreaker);
        final InboundMessage aggregated = new InboundMessage(currentHeader, releasableContent, breakerControl);
        boolean success = false;
        try {
            if (aggregated.getHeader().needsToReadVariableHeader()) {
                aggregated.getHeader().finishParsingHeader(aggregated.openOrGetStreamInput());
                if (aggregated.getHeader().isRequest()) {
                    initializeRequestState();
                }
            }
            if (isShortCircuited() == false) {
                checkBreaker(aggregated.getHeader(), aggregated.getContentLength(), breakerControl);
            }
            if (isShortCircuited()) {
                aggregated.decRef();
                success = true;
                return new InboundMessage(aggregated.getHeader(), aggregationException);
            } else {
                assert uncompressedOrSchemeDefined(aggregated.getHeader());
                success = true;
                return aggregated;
            }
        } finally {
            resetCurrentAggregation();
            if (success == false) {
                aggregated.decRef();
            }
        }
    }

    private static boolean uncompressedOrSchemeDefined(Header header) {
        return header.isCompressed() == (header.getCompressionScheme() != null);
    }

    private void checkBreaker(final Header header, final int contentLength, final BreakerControl breakerControl) {
        if (header.isRequest() == false) {
            return;
        }
        assert header.needsToReadVariableHeader() == false;

        if (canTripBreaker) {
            try {
                circuitBreaker.get().addEstimateBytesAndMaybeBreak(contentLength, header.getActionName());
                breakerControl.setReservedBytes(contentLength);
            } catch (CircuitBreakingException e) {
                shortCircuit(e);
            }
        } else {
            circuitBreaker.get().addWithoutBreaking(contentLength);
            breakerControl.setReservedBytes(contentLength);
        }
    }

    public boolean isAggregating() {
        return currentHeader != null;
    }

    private void shortCircuit(Exception exception) {
        this.aggregationException = exception;
    }

    private boolean isShortCircuited() {
        return aggregationException != null;
    }

    private void closeCurrentAggregation() {
        resetCurrentAggregation();
    }

    private void resetCurrentAggregation() {
        currentHeader = null;
        aggregationException = null;
        canTripBreaker = true;
    }

    public int internalDecode(ByteBuf byteBuf, TcpChannel channel) throws IOException {
        if (isOnHeader()) {
            int messageLength = TcpTransport.readMessageLength(Netty4Utils.toBytesReference(byteBuf));
            if (messageLength == -1) {
                return 0;
            } else if (messageLength == 0) {
                assert isAggregating() == false;
                transport.inboundMessage(channel, PING_MESSAGE);
                byteBuf.skipBytes(6);
                return 6;
            } else {
                int headerBytesToRead = headerBytesToRead(byteBuf, maxHeaderSize);
                if (headerBytesToRead == 0) {
                    return 0;
                } else {
                    totalNetworkSize = messageLength + TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE;

                    Header header = readHeader(messageLength, byteBuf.readSlice(headerBytesToRead), channelType);
                    bytesConsumed = headerBytesToRead;
                    if (header.isCompressed()) {
                        isCompressed = true;
                    }
                    headerReceived(header);

                    if (headerBytesToRead == messageLength + TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE) {
                        endContent(channel, null);
                        return headerBytesToRead;
                    } else {
                        return headerBytesToRead + readBody(byteBuf, channel);
                    }
                }
            }
        } else {
            return readBody(byteBuf, channel);
        }
    }

    private int readBody(ByteBuf byteBuf, TcpChannel channel) throws IOException {
        if (isCompressed && decompressor == null) {
            // Attempt to initialize decompressor
            TransportDecompressor decompressor = TransportDecompressor.getDecompressor(
                transport.recycler(),
                Netty4Utils.toBytesReference(byteBuf)
            );
            if (decompressor == null) {
                return 0;
            } else {
                this.decompressor = decompressor;
                assert isAggregating();
                updateCompressionScheme(this.decompressor.getScheme());
            }
        }
        int remainingToConsume = totalNetworkSize - bytesConsumed;
        if (byteBuf.readableBytes() < remainingToConsume) {
            return 0;
        }
        final int bytesConsumedThisDecode;
        if (decompressor != null) {
            ByteBuf retainedContent = byteBuf.readSlice(remainingToConsume);
            bytesConsumedThisDecode = decompressor.decompress(Netty4Utils.toBytesReference(retainedContent));
            retainedContent.skipBytes(bytesConsumedThisDecode);
            bytesConsumed += bytesConsumedThisDecode;
            ReleasableBytesReference decompressed;
            int pagesDecompressed = decompressor.pages();
            if (pagesDecompressed == 0) {
                endContent(channel, null);
            } else {
                ByteBuf dec = NettyAllocator.getAllocator().heapBuffer(pagesDecompressed * PageCacheRecycler.BYTE_PAGE_SIZE);
                while ((decompressed = decompressor.pollDecompressedPage(true)) != null) {
                    dec.writeBytes(decompressed.array(), decompressed.arrayOffset(), decompressed.length());
                    decompressed.decRef();
                }
                endContent(channel, dec);
            }
        } else {
            bytesConsumedThisDecode = remainingToConsume;
            bytesConsumed += remainingToConsume;
            endContent(channel, byteBuf.readRetainedSlice(bytesConsumedThisDecode));
        }

        return bytesConsumedThisDecode;
    }

    private static int headerBytesToRead(ByteBuf byteBuf, ByteSizeValue maxHeaderSize) throws StreamCorruptedException {
        if (byteBuf.readableBytes() < TcpHeader.BYTES_REQUIRED_FOR_VERSION) {
            return 0;
        }
        final int readerIndex = byteBuf.readerIndex();
        TransportVersion remoteVersion = TransportVersion.fromId(byteBuf.getInt(readerIndex + TcpHeader.VERSION_POSITION));
        int fixedHeaderSize = TcpHeader.headerSize(remoteVersion);
        if (fixedHeaderSize > byteBuf.readableBytes()) {
            return 0;
        } else if (remoteVersion.before(TcpHeader.VERSION_WITH_HEADER_SIZE)) {
            return fixedHeaderSize;
        } else {
            int variableHeaderSize = byteBuf.getInt(readerIndex + TcpHeader.VARIABLE_HEADER_SIZE_POSITION);
            if (variableHeaderSize < 0) {
                throw new StreamCorruptedException("invalid negative variable header size: " + variableHeaderSize);
            }
            if (variableHeaderSize > maxHeaderSize.getBytes() - fixedHeaderSize) {
                throw new StreamCorruptedException(
                    "header size [" + (fixedHeaderSize + variableHeaderSize) + "] exceeds limit of [" + maxHeaderSize + "]"
                );
            }
            int totalHeaderSize = fixedHeaderSize + variableHeaderSize;
            if (totalHeaderSize > byteBuf.readableBytes()) {
                return 0;
            } else {
                return totalHeaderSize;
            }
        }
    }

    private static Header readHeader(int networkMessageSize, ByteBuf byteBuf, InboundDecoder.ChannelType channelType) throws IOException {
        byteBuf.skipBytes(TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE);
        long requestId = byteBuf.readLong();
        byte status = byteBuf.readByte();
        int remoteVersion = byteBuf.readInt();

        Header header = new Header(networkMessageSize, requestId, status, TransportVersion.fromId(remoteVersion));
        if (channelType == InboundDecoder.ChannelType.SERVER && header.isResponse()) {
            throw new IllegalArgumentException("server channels do not accept inbound responses, only requests, closing channel");
        } else if (channelType == InboundDecoder.ChannelType.CLIENT && header.isRequest()) {
            throw new IllegalArgumentException("client channels do not accept inbound requests, only responses, closing channel");
        }
        if (header.isHandshake()) {
            checkHandshakeVersionCompatibility(header.getVersion());
        } else {
            checkVersionCompatibility(header.getVersion());
        }

        if (header.getVersion().onOrAfter(TcpHeader.VERSION_WITH_HEADER_SIZE)) {
            // Skip since we already have ensured enough data available
            byteBuf.readInt();
            header.finishParsingHeader(Netty4Utils.toBytesReference(byteBuf).streamInput());
        }
        return header;
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
                    + TransportVersions.MINIMUM_COMPATIBLE
                    + "]"
            );
        }
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

    private record ByteBufRefCounted(ByteBuf buffer) implements RefCounted {

        @Override
        public void incRef() {
            buffer.retain();
        }

        @Override
        public boolean tryIncRef() {
            if (hasReferences() == false) {
                return false;
            }
            try {
                buffer.retain();
            } catch (RuntimeException e) {
                assert hasReferences() == false;
                return false;
            }
            return true;
        }

        @Override
        public boolean decRef() {
            return buffer.release();
        }

        @Override
        public boolean hasReferences() {
            return buffer.refCnt() > 0;
        }
    }

    private static class BreakerControl implements Releasable {

        private static final int CLOSED = -1;

        private final Supplier<CircuitBreaker> circuitBreaker;
        private final AtomicInteger bytesToRelease = new AtomicInteger(0);

        private BreakerControl(Supplier<CircuitBreaker> circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
        }

        private void setReservedBytes(int reservedBytes) {
            final boolean set = bytesToRelease.compareAndSet(0, reservedBytes);
            assert set : "Expected bytesToRelease to be 0, found " + bytesToRelease.get();
        }

        @Override
        public void close() {
            final int toRelease = bytesToRelease.getAndSet(CLOSED);
            assert toRelease != CLOSED;
            if (toRelease > 0) {
                circuitBreaker.get().addWithoutBreaking(-toRelease);
            }
        }
    }
}
