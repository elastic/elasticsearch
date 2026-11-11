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
import java.util.function.LongFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class InboundAggregator implements Releasable {

    private final Supplier<CircuitBreaker> circuitBreaker;
    private final Predicate<String> requestCanTripBreaker;
    private final LongFunction<TransportResponseHandler<?>> responseHandlerFunction;

    private ReleasableBytesReference firstContent;
    private ArrayList<ReleasableBytesReference> contentAggregation;
    private Header currentHeader;
    private Exception aggregationException;
    private boolean canTripBreaker = true;
    private boolean isClosed = false;
    private BreakerControl breakerControl;
    // Handler of the response being aggregated, while it reserves memory for it. Null for a request, a handshake or error response, a
    // response with no handler, and once the handler returned NO_RESERVATION.
    private TransportResponseHandler<?> reservingResponseHandler;

    public InboundAggregator(
        Supplier<CircuitBreaker> circuitBreaker,
        Function<String, RequestHandlerRegistry<TransportRequest>> registryFunction,
        LongFunction<TransportResponseHandler<?>> responseHandlerFunction,
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
        }, responseHandlerFunction);
    }

    // Visible for testing
    InboundAggregator(Supplier<CircuitBreaker> circuitBreaker, Predicate<String> requestCanTripBreaker) {
        this(circuitBreaker, requestCanTripBreaker, requestId -> null);
    }

    // Visible for testing
    InboundAggregator(
        Supplier<CircuitBreaker> circuitBreaker,
        Predicate<String> requestCanTripBreaker,
        LongFunction<TransportResponseHandler<?>> responseHandlerFunction
    ) {
        this.circuitBreaker = circuitBreaker;
        this.requestCanTripBreaker = requestCanTripBreaker;
        this.responseHandlerFunction = responseHandlerFunction;
    }

    public void headerReceived(Header header) {
        ensureOpen();
        assert isAggregating() == false;
        assert firstContent == null && contentAggregation == null;
        currentHeader = header;
        breakerControl = new BreakerControl(circuitBreaker);
        if (currentHeader.isRequest() && currentHeader.needsToReadVariableHeader() == false) {
            initializeRequestState();
        } else if (currentHeader.isResponse()) {
            initializeResponseState();
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
            if (content.length() > 0 && reserveArrivingBytes(content.length()) == false) {
                releaseContent();
                firstContent = null;
                contentAggregation = null;
                // Release what was reserved now, the control that goes with the short-circuited message starts empty
                breakerControl.close();
                breakerControl = new BreakerControl(circuitBreaker);
                return;
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
                reserveContentBytes(aggregated.getHeader(), aggregated.getContentLength());
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
        Releasables.close(breakerControl);
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
        breakerControl = null;
        reservingResponseHandler = null;
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

    // A handshake response has no handler in the response handlers. An error response is not reserved, so the failure always reaches its
    // handler.
    private void initializeResponseState() {
        assert currentHeader.isResponse();
        if (currentHeader.isHandshake() == false && currentHeader.isError() == false) {
            reservingResponseHandler = responseHandlerFunction.apply(currentHeader.getRequestId());
        }
    }

    private static boolean uncompressedOrSchemeDefined(Header header) {
        return header.isCompressed() == (header.getCompressionScheme() != null);
    }

    // Reserves each piece as it arrives so it is checked against the memory in use at that moment. Requests whose action name isn't known
    // yet are reserved in reserveContentBytes.
    private boolean reserveArrivingBytes(int bytes) {
        if (currentHeader.isRequest()) {
            return currentHeader.needsToReadVariableHeader() || reserveBreakerBytes(bytes, currentHeader.getActionName());
        } else {
            return reserveResponseBytes(bytes);
        }
    }

    // Reserves the bytes on the breaker and records them in the breaker control, so they are released with it. Returns false if the breaker
    // tripped, which short-circuits the aggregation.
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
        breakerControl.addReservedBytes(bytes);
        return true;
    }

    // Asks the handler of the response to reserve the bytes and records its reservation in the breaker control, so it is released with it.
    // A handler that reserves nothing isn't asked again for the rest of the response. Returns false if the handler rejected the bytes,
    // which short-circuits the aggregation.
    private boolean reserveResponseBytes(int bytes) {
        if (reservingResponseHandler == null) {
            return true;
        }
        final Releasable reservation;
        try {
            reservation = reservingResponseHandler.reserveResponseBytes(bytes);
        } catch (CircuitBreakingException e) {
            shortCircuit(e);
            return false;
        }
        if (reservation == TransportResponseHandler.NO_RESERVATION) {
            reservingResponseHandler = null;
        } else {
            breakerControl.addReservation(reservation);
        }
        return true;
    }

    /**
     * Reserves the content of a request whose action name was only parsed along with the content, so it couldn't be reserved while read.
     */
    private void reserveContentBytes(final Header header, final int contentLength) {
        if (header.isRequest()) {
            reserveBreakerBytes(contentLength, header.getActionName());
        }
    }

    private static class BreakerControl implements Releasable {

        private static final int CLOSED = -1;

        private final Supplier<CircuitBreaker> circuitBreaker;
        private final AtomicInteger bytesToRelease = new AtomicInteger(0);
        // Filled on the transport worker before the message is handed over, so it needs no synchronization
        private ArrayList<Releasable> reservations;

        private BreakerControl(Supplier<CircuitBreaker> circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
        }

        private void addReservedBytes(int reservedBytes) {
            final int updated = bytesToRelease.addAndGet(reservedBytes);
            assert updated >= 0 : "Expected bytesToRelease to be non-negative, found " + updated;
        }

        private void addReservation(Releasable reservation) {
            if (reservations == null) {
                reservations = new ArrayList<>();
            }
            reservations.add(reservation);
        }

        @Override
        public void close() {
            final int toRelease = bytesToRelease.getAndSet(CLOSED);
            assert toRelease != CLOSED;
            if (toRelease > 0) {
                circuitBreaker.get().addWithoutBreaking(-toRelease);
            }
            if (reservations != null) {
                Releasables.close(reservations);
            }
        }
    }
}
