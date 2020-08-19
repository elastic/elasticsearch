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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.PageCacheRecycler;

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
    private final PipelineFragmentHandler fragmentHandler;
    private Exception uncaughtException;
    private final ArrayDeque<ReleasableBytesReference> pending = new ArrayDeque<>(2);
    private boolean isClosed = false;
    private TcpChannel channel;

    public InboundPipeline(Version version, StatsTracker statsTracker, PageCacheRecycler recycler, LongSupplier relativeTimeInMillis,
                           Supplier<CircuitBreaker> circuitBreaker,
                           Function<String, RequestHandlerRegistry<TransportRequest>> registryFunction,
                           BiConsumer<TcpChannel, InboundMessage> messageHandler) {
        this(statsTracker, relativeTimeInMillis, new InboundDecoder(version, recycler),
            new InboundAggregator(circuitBreaker, registryFunction), messageHandler);
    }

    public InboundPipeline(StatsTracker statsTracker, LongSupplier relativeTimeInMillis, InboundDecoder decoder,
                           InboundAggregator aggregator, BiConsumer<TcpChannel, InboundMessage> messageHandler) {
        this.relativeTimeInMillis = relativeTimeInMillis;
        this.statsTracker = statsTracker;
        this.decoder = decoder;
        this.aggregator = aggregator;
        this.messageHandler = messageHandler;
        this.fragmentHandler = new PipelineFragmentHandler();
    }

    @Override
    public void close() {
        isClosed = true;
        Releasables.closeWhileHandlingException(decoder, aggregator);
        Releasables.closeWhileHandlingException(pending);
        pending.clear();
    }

    public void handleBytes(ReleasableBytesReference reference) throws IOException {
        assert channel != null;
        if (uncaughtException != null) {
            throw new IllegalStateException("Pipeline state corrupted by uncaught exception", uncaughtException);
        }
        try {
            doHandleBytes(reference);
        } catch (Exception e) {
            uncaughtException = e;
            throw e;
        }
    }

    private void doHandleBytes(ReleasableBytesReference reference) throws IOException {
        assert isClosed == false : "received [" + reference.length() + "] bytes after close";

        channel.getChannelStats().markAccessed(relativeTimeInMillis.getAsLong());
        statsTracker.markBytesRead(reference.length());
        pending.add(reference.retain());

        while (pending.isEmpty() == false) {
            try (ReleasableBytesReference toDecode = getPendingBytes()) {
                fragmentHandler.madeProgress = false;
                final int bytesConsumed = decoder.decode(toDecode, fragmentHandler);
                if (isClosed) {
                    assert pending.isEmpty() : "closed without releasing [" + pending.size() + "] chunks";
                    break;
                } else if (fragmentHandler.madeProgress) {
                    assert bytesConsumed > 0 : "made progress without consuming bytes";
                    releasePendingBytes(bytesConsumed);
                } else {
                    assert bytesConsumed == 0 : "consumed [" + bytesConsumed + "] bytes without making progress";
                    break;
                }
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
            final Releasable releasable = () -> Releasables.closeWhileHandlingException(bytesReferences);
            return new ReleasableBytesReference(CompositeBytesReference.of(bytesReferences), releasable);
        }
    }

    private void releasePendingBytes(int bytesConsumed) {
        int bytesToRelease = bytesConsumed;
        while (bytesToRelease != 0) {
            try (ReleasableBytesReference reference = pending.pollFirst()) {
                assert reference != null : channel;
                if (bytesToRelease < reference.length()) {
                    pending.addFirst(reference.retainedSlice(bytesToRelease, reference.length() - bytesToRelease));
                    bytesToRelease -= bytesToRelease;
                } else {
                    bytesToRelease -= reference.length();
                }
            }
        }
    }

    public void setChannel(TcpChannel channel) {
        assert this.channel == null || this.channel == channel : "changing channel from " + this.channel + " to " + channel;
        assert channel != null;
        this.channel = channel;
    }

    private class PipelineFragmentHandler implements InboundFragmentHandler {

        boolean madeProgress;

        @Override
        public void handleHeader(Header header) {
            assert aggregator.isAggregating() == false;
            madeProgress = true;
            aggregator.headerReceived(header);
        }

        @Override
        public void handlePing() {
            assert aggregator.isAggregating() == false;
            madeProgress = true;
            messageHandler.accept(channel, PING_MESSAGE);
        }

        @Override
        public void handleEndContent() throws IOException {
            assert aggregator.isAggregating();
            madeProgress = true;
            try (InboundMessage aggregated = aggregator.finishAggregation()) {
                statsTracker.markMessageReceived();
                messageHandler.accept(channel, aggregated);
            }
        }

        @Override
        public void handleFragment(ReleasableBytesReference fragment) {
            assert aggregator.isAggregating();
            madeProgress = true;
            aggregator.aggregate(fragment);
            fragment.close(); // the aggregator took ownership by incrementing the refcount on the fragment itself
        }
    }

}
