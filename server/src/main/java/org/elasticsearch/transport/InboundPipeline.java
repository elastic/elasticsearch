/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class InboundPipeline implements Releasable {

    private static final InboundMessage PING_MESSAGE = new InboundMessage(null, true);

    private final LongSupplier relativeTimeInMillis;
    private final StatsTracker statsTracker;
    private final InboundDecoder decoder;
    private final InboundAggregator aggregator;
    private final BiConsumer<TcpChannel, InboundMessage> messageHandler;
    private Exception uncaughtException;
    private final ArrayDeque<ReleasableBytesReference> pending = new ArrayDeque<>(2);
    private boolean isClosed = false;

    public InboundPipeline(
        Version version,
        StatsTracker statsTracker,
        Recycler<BytesRef> recycler,
        LongSupplier relativeTimeInMillis,
        Supplier<CircuitBreaker> circuitBreaker,
        Function<String, RequestHandlerRegistry<TransportRequest>> registryFunction,
        BiConsumer<TcpChannel, InboundMessage> messageHandler
    ) {
        this(
            statsTracker,
            relativeTimeInMillis,
            new InboundDecoder(version, recycler),
            new InboundAggregator(circuitBreaker, registryFunction),
            messageHandler
        );
    }

    public InboundPipeline(
        StatsTracker statsTracker,
        LongSupplier relativeTimeInMillis,
        InboundDecoder decoder,
        InboundAggregator aggregator,
        BiConsumer<TcpChannel, InboundMessage> messageHandler
    ) {
        this.relativeTimeInMillis = relativeTimeInMillis;
        this.statsTracker = statsTracker;
        this.decoder = decoder;
        this.aggregator = aggregator;
        this.messageHandler = messageHandler;
    }

    @Override
    public void close() {
        isClosed = true;
        Releasables.closeExpectNoException(decoder, aggregator, () -> Releasables.close(pending), pending::clear);
    }

    public void handleBytes(TcpChannel channel, ReleasableBytesReference reference) throws IOException {
        if (uncaughtException != null) {
            throw new IllegalStateException("Pipeline state corrupted by uncaught exception", uncaughtException);
        }
        try {
            doHandleBytes(channel, reference);
        } catch (Exception e) {
            uncaughtException = e;
            throw e;
        }
    }

    public void doHandleBytes(TcpChannel channel, ReleasableBytesReference reference) throws IOException {
        channel.getChannelStats().markAccessed(relativeTimeInMillis.getAsLong());
        statsTracker.markBytesRead(reference.length());

        if (pending.isEmpty() == false) {
            if (isClosed == false) {
                pending.add(reference.retain());
                doHandleBytesWithPending(channel);
            }
            return;
        }

        boolean continueDecoding = true;
        while (continueDecoding && isClosed == false && reference.length() > 0) {
            final int bytesDecoded = decoder.decode(reference, fragment -> this.forwardFragment(channel, fragment));
            if (bytesDecoded != 0) {
                reference = reference.releasableSlice(bytesDecoded);
            } else {
                continueDecoding = false;
            }
        }
        if (isClosed == false && reference.length() > 0) {
            pending.add(reference.retain());
        }
    }

    private void doHandleBytesWithPending(TcpChannel channel) throws IOException {
        while (isClosed == false && pending.isEmpty() == false) {
            final int bytesDecoded;
            if (pending.size() == 1) {
                bytesDecoded = decoder.decode(pending.peekFirst(), fragment -> this.forwardFragment(channel, fragment));
            } else {
                try (ReleasableBytesReference toDecode = getPendingBytes()) {
                    bytesDecoded = decoder.decode(toDecode, fragment -> this.forwardFragment(channel, fragment));
                }
            }
            if (bytesDecoded != 0 && isClosed == false) {
                releasePendingBytes(bytesDecoded);
            } else {
                return;
            }
        }
    }

    private void forwardFragment(TcpChannel channel, Object fragment) throws IOException {
        if (fragment instanceof Header) {
            assert aggregator.isAggregating() == false;
            aggregator.headerReceived((Header) fragment);
        } else if (fragment instanceof Compression.Scheme) {
            assert aggregator.isAggregating();
            aggregator.updateCompressionScheme((Compression.Scheme) fragment);
        } else if (fragment == InboundDecoder.PING) {
            assert aggregator.isAggregating() == false;
            messageHandler.accept(channel, PING_MESSAGE);
        } else if (fragment == InboundDecoder.END_CONTENT) {
            assert aggregator.isAggregating();
            try (InboundMessage aggregated = aggregator.finishAggregation()) {
                statsTracker.markMessageReceived();
                messageHandler.accept(channel, aggregated);
            }
        } else {
            assert aggregator.isAggregating();
            assert fragment instanceof ReleasableBytesReference;
            aggregator.aggregate((ReleasableBytesReference) fragment);
        }
    }

    private ReleasableBytesReference getPendingBytes() {
        assert pending.size() > 1 : "must use this method with multiple pending references but used with " + pending;
        final ReleasableBytesReference[] bytesReferences = new ReleasableBytesReference[pending.size()];
        int index = 0;
        for (ReleasableBytesReference pendingReference : pending) {
            bytesReferences[index] = pendingReference.retain();
            ++index;
        }
        final Releasable releasable = () -> Releasables.closeExpectNoException(bytesReferences);
        return new ReleasableBytesReference(CompositeBytesReference.of(bytesReferences), releasable);
    }

    private void releasePendingBytes(int bytesConsumed) {
        int bytesToRelease = bytesConsumed;
        while (bytesToRelease != 0) {
            ReleasableBytesReference reference = pending.pollFirst();
            assert reference != null;
            if (bytesToRelease < reference.length()) {
                pending.addFirst(reference.releasableSlice(bytesToRelease));
                return;
            } else {
                bytesToRelease -= reference.length();
                reference.decRef();
            }
        }
    }
}
