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
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.PageCacheRecycler;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.function.BiConsumer;

public class InboundPipeline implements Releasable {

    private static final ThreadLocal<ArrayList<Object>> fragmentList = ThreadLocal.withInitial(ArrayList::new);
    private static final InboundMessage PING_MESSAGE = new InboundMessage(null, true);

    private final InboundDecoder decoder;
    private final InboundAggregator aggregator;
    private final BiConsumer<TcpChannel, InboundMessage> messageHandler;
    private final BiConsumer<TcpChannel, Tuple<Header, Exception>> errorHandler;
    private ArrayDeque<ReleasableBytesReference> pending = new ArrayDeque<>(2);
    private boolean isClosed = false;

    public InboundPipeline(Version version, PageCacheRecycler recycler, BiConsumer<TcpChannel, InboundMessage> messageHandler,
                           BiConsumer<TcpChannel, Tuple<Header, Exception>> errorHandler) {
        this(new InboundDecoder(version, recycler), new InboundAggregator(), messageHandler, errorHandler);
    }

    private InboundPipeline(InboundDecoder decoder, InboundAggregator aggregator,
                            BiConsumer<TcpChannel, InboundMessage> messageHandler,
                            BiConsumer<TcpChannel, Tuple<Header, Exception>> errorHandler) {
        this.decoder = decoder;
        this.aggregator = aggregator;
        this.messageHandler = messageHandler;
        this.errorHandler = errorHandler;
    }

    @Override
    public void close() {
        isClosed = true;
        Releasables.closeWhileHandlingException(decoder, aggregator);
        Releasables.closeWhileHandlingException(pending);
        pending.clear();
    }

    public void handleBytes(TcpChannel channel, ReleasableBytesReference reference) throws IOException {
        pending.add(reference.retain());

        final ReleasableBytesReference composite;
        if (pending.size() == 1) {
            composite = pending.peekFirst();
        } else {
            final ReleasableBytesReference[] bytesReferences = pending.toArray(new ReleasableBytesReference[0]);
            final Releasable releasable = () -> Releasables.closeWhileHandlingException(bytesReferences);
            composite = new ReleasableBytesReference(new CompositeBytesReference(bytesReferences), releasable);
        }

        final ArrayList<Object> fragments = fragmentList.get();
        int bytesConsumed = 0;
        boolean continueHandling = true;

        while (continueHandling && isClosed == false) {
            boolean continueDecoding = true;
            while (continueDecoding) {
                final int remaining = composite.length() - bytesConsumed;
                if (remaining != 0) {
                    try (ReleasableBytesReference slice = composite.retainedSlice(bytesConsumed, remaining)) {
                        final int bytesDecoded = decoder.decode(slice, fragments::add);
                        if (bytesDecoded != 0) {
                            bytesConsumed += bytesDecoded;
                            if (fragments.isEmpty() == false && endOfMessage(fragments.get(fragments.size() - 1))) {
                                continueDecoding = false;
                            }
                        } else {
                            continueDecoding = false;
                        }
                    }
                } else {
                    continueDecoding = false;
                }
            }

            if (fragments.isEmpty()) {
                continueHandling = false;
            } else {
                try {
                    forwardFragments(channel, fragments);
                } finally {
                    for (Object fragment : fragments) {
                        if (fragment instanceof ReleasableBytesReference) {
                            ((ReleasableBytesReference) fragment).close();
                        }
                    }
                    fragments.clear();
                }
            }
        }

        releasePendingBytes(bytesConsumed);
    }

    private void forwardFragments(TcpChannel channel, ArrayList<Object> fragments) throws IOException {
        for (Object fragment : fragments) {
            if (fragment instanceof Header) {
                assert aggregator.isAggregating() == false;
                aggregator.headerReceived((Header) fragment);
            } else if (fragment == InboundDecoder.PING) {
                assert aggregator.isAggregating() == false;
                messageHandler.accept(channel, PING_MESSAGE);
            } else if (fragment == InboundDecoder.END_CONTENT) {
                assert aggregator.isAggregating();
                try (InboundMessage aggregated = aggregator.finishAggregation()) {
                    messageHandler.accept(channel, aggregated);
                }
            } else if (fragment instanceof Exception) {
                final Header header;
                if (aggregator.isAggregating()) {
                    header = aggregator.cancelAggregation();
                } else {
                    header = null;
                }
                errorHandler.accept(channel, new Tuple<>(header, (Exception) fragment));
            } else {
                assert aggregator.isAggregating();
                assert fragment instanceof ReleasableBytesReference;
                aggregator.aggregate((ReleasableBytesReference) fragment);
            }
        }
    }

    private boolean endOfMessage(Object fragment) {
        return fragment == InboundDecoder.PING || fragment == InboundDecoder.END_CONTENT || fragment instanceof Exception;
    }

    private void releasePendingBytes(int bytesConsumed) {
        if (isClosed) {
            // Are released by the close method
            return;
        }
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
