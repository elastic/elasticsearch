/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class InboundAggregator implements Releasable {

    private final Supplier<CircuitBreaker> circuitBreaker;
    private final Predicate<String> requestCanTripBreaker;

    private ReleasableBytesReference firstContent;
    private ArrayList<ReleasableBytesReference> contentAggregation;
    private Header currentHeader;
    private Exception aggregationException;
    private boolean canTripBreaker = true;
    private boolean isClosed = false;
    // Bytes charged to the breaker for the current message before it finished aggregating; handed to the BreakerControl on finish
    private int chargedBytes = 0;

    public InboundAggregator(
        Supplier<CircuitBreaker> circuitBreaker,
        Function<String, RequestHandlerRegistry<TransportRequest>> registryFunction,
        boolean ignoreDeserializationErrors
    ) {
        this(circuitBreaker, actionName -> {
            final RequestHandlerRegistry<TransportRequest> reg = registryFunction.apply(actionName);
            if (reg == null) {
                assert ignoreDeserializationErrors : actionName;
                throw new ActionNotFoundTransportException(actionName);
            } else {
                return reg.canTripCircuitBreaker();
            }
        });
    }

    // Visible for testing
    InboundAggregator(Supplier<CircuitBreaker> circuitBreaker, Predicate<String> requestCanTripBreaker) {
        this.circuitBreaker = circuitBreaker;
        this.requestCanTripBreaker = requestCanTripBreaker;
    }

    public void headerReceived(Header header) {
        ensureOpen();
        assert isAggregating() == false;
        assert firstContent == null && contentAggregation == null;
        currentHeader = header;
        if (currentHeader.isRequest() && currentHeader.needsToReadVariableHeader() == false) {
            initializeRequestState();
        }
    }

    public void updateCompressionScheme(Compression.Scheme compressionScheme) {
        ensureOpen();
        assert isAggregating();
        assert firstContent == null && contentAggregation == null;
        currentHeader.setCompressionScheme(compressionScheme);
    }

    public void aggregate(ReleasableBytesReference content) {
        ensureOpen();
        assert isAggregating();
        if (isShortCircuited() == false) {
            // Charging each fragment as it arrives, rather than reserving the declared size once up front, means every fragment is checked
            // against the memory in use at that moment, so a message that arrives alongside others trips as the heap fills. Only requests
            // whose action name is already known can be charged, anything else is charged in chargeContent once it has all been read.
            if (currentHeader.isRequest() && currentHeader.needsToReadVariableHeader() == false && content.length() > 0) {
                if (reserveBreakerBytes(content.length(), currentHeader.getActionName())) {
                    chargedBytes += content.length();
                } else {
                    // The breaker tripped: stop holding on to what has been read so far, the rest of the message is discarded
                    releaseContent();
                    firstContent = null;
                    contentAggregation = null;
                    releaseChargedBytes();
                    return;
                }
            }
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
            releasableContent = ReleasableBytesReference.empty();
        } else if (contentAggregation == null) {
            releasableContent = firstContent;
        } else {
            final ReleasableBytesReference[] references = contentAggregation.toArray(new ReleasableBytesReference[0]);
            final BytesReference content = CompositeBytesReference.of(references);
            releasableContent = new ReleasableBytesReference(content, () -> Releasables.close(references));
        }

        final BreakerControl breakerControl = new BreakerControl(circuitBreaker);
        breakerControl.addReservedBytes(chargedBytes);
        chargedBytes = 0;
        final InboundMessage aggregated = new InboundMessage(currentHeader, releasableContent, breakerControl);
        boolean success = false;
        try {
            final boolean headerParsedWithContent = aggregated.getHeader().needsToReadVariableHeader();
            if (headerParsedWithContent) {
                aggregated.getHeader().finishParsingHeader(aggregated.openOrGetStreamInput());
                if (aggregated.getHeader().isRequest()) {
                    initializeRequestState();
                }
            }
            if (headerParsedWithContent && isShortCircuited() == false) {
                chargeContent(aggregated.getHeader(), aggregated.getContentLength(), breakerControl);
            }
            if (isShortCircuited()) {
                aggregated.close();
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
        releaseChargedBytes();
        resetCurrentAggregation();
    }

    private void releaseChargedBytes() {
        if (chargedBytes > 0) {
            circuitBreaker.get().addWithoutBreaking(-chargedBytes);
        }
        chargedBytes = 0;
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

    private static boolean uncompressedOrSchemeDefined(Header header) {
        return header.isCompressed() == (header.getCompressionScheme() != null);
    }

    /**
     * @return whether the bytes were reserved; {@code false} if the breaker tripped, in which case the aggregation is short-circuited
     */
    private boolean reserveBreakerBytes(int bytes, String label) {
        if (canTripBreaker) {
            try {
                circuitBreaker.get().addEstimateBytesAndMaybeBreak(bytes, label);
            } catch (CircuitBreakingException e) {
                shortCircuit(e);
                return false;
            }
        } else {
            circuitBreaker.get().addWithoutBreaking(bytes);
        }
        return true;
    }

    /**
     * Charges the whole content of a request whose action name was only parsed along with the content, so nothing could be charged
     * while it was being read. Everything else has already been charged fragment by fragment in {@link #aggregate}.
     */
    private void chargeContent(final Header header, final int contentLength, final BreakerControl breakerControl) {
        if (header.isRequest() && reserveBreakerBytes(contentLength, header.getActionName())) {
            breakerControl.addReservedBytes(contentLength);
        }
    }

    private static class BreakerControl implements Releasable {

        private static final int CLOSED = -1;

        private final Supplier<CircuitBreaker> circuitBreaker;
        private final AtomicInteger bytesToRelease = new AtomicInteger(0);

        private BreakerControl(Supplier<CircuitBreaker> circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
        }

        private void addReservedBytes(int reservedBytes) {
            final int updated = bytesToRelease.addAndGet(reservedBytes);
            assert updated >= 0 : "Expected bytesToRelease to be non-negative, found " + updated;
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
