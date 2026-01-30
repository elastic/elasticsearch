/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

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
            reference.close();
            throw new IllegalStateException("Pipeline state corrupted by uncaught exception", uncaughtException);
        }
        try {
            channel.getChannelStats().markAccessed(relativeTimeInMillis.getAsLong());
            statsTracker.markBytesRead(reference.length());
            if (isClosed) {
                reference.close();
                return;
            }
            pending.add(reference);
            doHandleBytes(channel);
        } catch (Exception e) {
            uncaughtException = e;
            throw e;
        }
    }

    private void doHandleBytes(TcpChannel channel) throws IOException {
        do {
            CheckedConsumer<Object, IOException> decodeConsumer = f -> forwardFragment(channel, f);
            int bytesDecoded = decoder.decode(pending.peekFirst(), decodeConsumer);
            if (bytesDecoded == 0 && pending.size() > 1) {
                final ReleasableBytesReference[] bytesReferences = new ReleasableBytesReference[pending.size()];
                int index = 0;
                for (ReleasableBytesReference pendingReference : pending) {
                    bytesReferences[index] = pendingReference.retain();
                    ++index;
                }
                try (
                    ReleasableBytesReference toDecode = new ReleasableBytesReference(
                        CompositeBytesReference.of(bytesReferences),
                        () -> Releasables.closeExpectNoException(bytesReferences)
                    )
                ) {
                    bytesDecoded = decoder.decode(toDecode, decodeConsumer);
                }
            }
            if (bytesDecoded != 0) {
                releasePendingBytes(bytesDecoded);
            } else {
                break;
            }
        } while (pending.isEmpty() == false);
    }

    private void forwardFragment(TcpChannel channel, Object fragment) throws IOException {
        if (fragment instanceof Header) {
            headerReceived((Header) fragment);
        } else if (fragment instanceof Compression.Scheme) {
            assert aggregator.isAggregating();
            aggregator.updateCompressionScheme((Compression.Scheme) fragment);
        } else if (fragment == InboundDecoder.PING) {
            assert aggregator.isAggregating() == false;
            messageHandler.accept(channel, PING_MESSAGE);
        } else if (fragment == InboundDecoder.END_CONTENT) {
            assert aggregator.isAggregating();
            statsTracker.markMessageReceived();
            messageHandler.accept(channel, /* autocloses */ aggregator.finishAggregation());
        } else {
            assert aggregator.isAggregating();
            assert fragment instanceof ReleasableBytesReference;
            aggregator.aggregate((ReleasableBytesReference) fragment);
        }
    }

    protected void headerReceived(Header header) {
        assert aggregator.isAggregating() == false;
        aggregator.headerReceived(header);
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
