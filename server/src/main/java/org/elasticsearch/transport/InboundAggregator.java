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

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class InboundAggregator implements Releasable {

    private final MemoryController memoryController;
    private final Predicate<String> requestCanTripBreaker;

    private ReleasableBytesReference firstContent;
    private ArrayList<ReleasableBytesReference> contentAggregation;
    private Header currentHeader;
    private Exception aggregationException;
    private boolean canTripBreaker = true;
    private boolean isClosed = false;

    public InboundAggregator(Supplier<CircuitBreaker> circuitBreaker,
                             Function<String, RequestHandlerRegistry<TransportRequest>> registryFunction) {
        this(new MemoryController(circuitBreaker), (Predicate<String>) actionName -> {
            final RequestHandlerRegistry<TransportRequest> reg = registryFunction.apply(actionName);
            if (reg == null) {
                throw new ActionNotFoundTransportException(actionName);
            } else {
                return reg.canTripCircuitBreaker();
            }
        });
    }

    // Visible for testing
    InboundAggregator(Supplier<CircuitBreaker> circuitBreaker, Predicate<String> requestCanTripBreaker) {
        this(new MemoryController(circuitBreaker), requestCanTripBreaker);
    }

    // Visible for testing
    InboundAggregator(MemoryController memoryController, Predicate<String> requestCanTripBreaker) {
        this.memoryController = memoryController;
        this.requestCanTripBreaker = requestCanTripBreaker;
    }

    public void headerReceived(Header header) {
        ensureOpen();
        assert isAggregating() == false;
        assert firstContent == null && contentAggregation == null;
        currentHeader = header;
        if (currentHeader.needsToReadVariableHeader() == false) {
            initializeState();
        }
    }

    public void aggregate(ReleasableBytesReference content) {
        ensureOpen();
        assert isAggregating();
        if (isShortCircuited() == false) {
            memoryController.inboundBytesBuffered(content.length());
            if (isFirstContent()) {
                firstContent = content.retain();
            } else {
                if (contentAggregation == null) {
                    contentAggregation = new ArrayList<>(4);
                    assert firstContent != null;
                    contentAggregation.add(firstContent);
                    firstContent = null;
                }
                contentAggregation.add(content.retain());
            }
        }
    }

    public InboundMessage finishAggregation() throws IOException {
        ensureOpen();
        final ReleasableBytesReference releasableContent;
        if (isFirstContent()) {
            releasableContent = ReleasableBytesReference.wrap(BytesArray.EMPTY);
        } else if (contentAggregation == null) {
            releasableContent = firstContent;
        } else {
            final ReleasableBytesReference[] references = contentAggregation.toArray(new ReleasableBytesReference[0]);
            final BytesReference content = CompositeBytesReference.of(references);
            releasableContent = new ReleasableBytesReference(content, () -> Releasables.close(references));
        }

        final BreakerControl breakerControl = new BreakerControl(memoryController);
        final InboundMessage aggregated = new InboundMessage(currentHeader, releasableContent, breakerControl);
        boolean success = false;
        try {
            if (aggregated.getHeader().needsToReadVariableHeader()) {
                aggregated.getHeader().finishParsingHeader(aggregated.openOrGetStreamInput());
                initializeState();
            }
            if (isShortCircuited() == false) {
                checkBreaker(aggregated.getHeader(), aggregated.getContentLength(), breakerControl);
            }
            if (isShortCircuited()) {
                aggregated.close();
                success = true;
                return new InboundMessage(aggregated.getHeader(), aggregationException);
            } else {
                success = true;
                return aggregated;
            }
        } finally {
            resetCurrentAggregation();
            if (success == false) {
                aggregated.close();
            }
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

    private boolean isFirstContent() {
        return firstContent == null && contentAggregation == null;
    }

    @Override
    public void close() {
        isClosed = true;
        closeCurrentAggregation();
    }

    private void closeCurrentAggregation() {
        releaseContent();
        resetCurrentAggregation();
    }

    private void releaseContent() {
        if (contentAggregation == null) {
            Releasables.close(firstContent);
        } else {
            Releasables.close(contentAggregation);
        }
    }

    private void resetCurrentAggregation() {
        firstContent = null;
        contentAggregation = null;
        currentHeader = null;
        aggregationException = null;
        canTripBreaker = true;
    }

    private void ensureOpen() {
        if (isClosed) {
            throw new IllegalStateException("Aggregator is already closed");
        }
    }

    private void initializeState() {
        assert currentHeader.needsToReadVariableHeader() == false;
        if (currentHeader.isHandshake()) {
            canTripBreaker = false;
            return;
        }
        if (currentHeader.isResponse()) {
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

    private void checkBreaker(final Header header, final int contentLength, final BreakerControl breakerControl) {
        assert header.needsToReadVariableHeader() == false;
        try {
            memoryController.transferBytesToApplicationLayer(contentLength, header.getActionName(), canTripBreaker);
            breakerControl.setReservedBytes(contentLength);
        } catch (CircuitBreakingException e) {
            shortCircuit(e);
        }
    }

    private static class BreakerControl implements Releasable {

        private static final int CLOSED = -1;

        private final MemoryController memoryController;
        private final AtomicInteger bytesToRelease = new AtomicInteger(0);

        private BreakerControl(MemoryController memoryController) {
            this.memoryController = memoryController;
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
                memoryController.releaseApplicationLayerBytes(toRelease);
            }
        }
    }
}
