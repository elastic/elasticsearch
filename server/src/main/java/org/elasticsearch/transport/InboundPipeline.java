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
import org.elasticsearch.common.CheckedBiConsumer;
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
    private Exception uncaughtException;
    private final ArrayDeque<ReleasableBytesReference> pending = new ArrayDeque<>(2);
    private boolean isClosed = false;

    private final CheckedBiConsumer<TcpChannel, Object, IOException> fragmentConsumer;

    public InboundPipeline(
        Version version,
        StatsTracker statsTracker,
        Recycler<BytesRef> recycler,
        LongSupplier relativeTimeInMillis,
        Supplier<CircuitBreaker> circuitBreaker,
        Function<String, RequestHandlerRegistry<TransportRequest>> registryFunction,
        BiConsumer<TcpChannel, InboundMessage> messageHandler,
        boolean ignoreDeserializationErrors
    ) {
        this(
            statsTracker,
            relativeTimeInMillis,
            new InboundDecoder(version, recycler),
            new InboundAggregator(circuitBreaker, registryFunction, ignoreDeserializationErrors),
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
        this.fragmentConsumer = (c, fragment) -> {
            if (fragment instanceof Header) {
                assert aggregator.isAggregating() == false;
                aggregator.headerReceived((Header) fragment);
            } else if (fragment instanceof Compression.Scheme) {
                assert aggregator.isAggregating();
                aggregator.updateCompressionScheme((Compression.Scheme) fragment);
            } else if (fragment == InboundDecoder.PING) {
                assert aggregator.isAggregating() == false;
                messageHandler.accept(c, PING_MESSAGE);
            } else if (fragment == InboundDecoder.END_CONTENT) {
                assert aggregator.isAggregating();
                InboundMessage aggregated = aggregator.finishAggregation();
                try {
                    statsTracker.markMessageReceived();
                    messageHandler.accept(c, aggregated);
                } finally {
                    aggregated.decRef();
                }
            } else {
                assert aggregator.isAggregating();
                assert fragment instanceof ReleasableBytesReference;
                // fragment will be released by the aggregator
                final var bytes = (ReleasableBytesReference) fragment;
                try {
                    aggregator.aggregate(bytes);
                } finally {
                    bytes.decRef();
                }
            }
        };
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
        pending.add(reference.retain());
        while (pending.isEmpty() == false && isClosed == false) {
            try (ReleasableBytesReference toDecode = getPendingBytes()) {
                final int bytesDecoded = decoder.decode(toDecode, channel, fragmentConsumer);
                if (bytesDecoded == 0) {
                    break;
                }
                releasePendingBytes(bytesDecoded);
            }
        }
    }

    private ReleasableBytesReference getPendingBytes() {
        if (pending.size() == 1) {
            return pending.peekFirst().retain();
        } else {
            final ReleasableBytesReference[] bytesReferences = new ReleasableBytesReference[pending.size()];
            int index = 0;
            for (ReleasableBytesReference pendingReference : pending) {
                bytesReferences[index] = pendingReference.retain();
                ++index;
            }
            final Releasable releasable = () -> Releasables.closeExpectNoException(bytesReferences);
            return new ReleasableBytesReference(CompositeBytesReference.of(bytesReferences), releasable);
        }
    }

    private void releasePendingBytes(int bytesConsumed) {
        int bytesToRelease = bytesConsumed;
        while (bytesToRelease != 0) {
            try (ReleasableBytesReference reference = pending.pollFirst()) {
                assert reference != null;
                if (bytesToRelease < reference.length()) {
                    pending.addFirst(reference.retainedSlice(bytesToRelease, reference.length() - bytesToRelease));
                    bytesToRelease -= bytesToRelease;
                } else {
                    bytesToRelease -= reference.length();
                }
            }
        }
    }
}
