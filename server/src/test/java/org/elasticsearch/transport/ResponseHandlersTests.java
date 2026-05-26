/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

public class ResponseHandlersTests extends ESTestCase {

    /** A circuit breaker that tracks its current byte usage and trips when it exceeds a configurable limit. */
    private static class TrackingCircuitBreaker extends NoopCircuitBreaker {
        private final AtomicLong used = new AtomicLong();
        private final long limit;

        TrackingCircuitBreaker(long limit) {
            super("test");
            this.limit = limit;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            long newUsed = used.addAndGet(bytes);
            if (newUsed > limit) {
                used.addAndGet(-bytes);
                throw new CircuitBreakingException("test breaker tripped: " + newUsed + " > " + limit, getDurability());
            }
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used.addAndGet(bytes);
        }

        @Override
        public long getUsed() {
            return used.get();
        }
    }

    private static TransportResponseHandler<ActionResponse.Empty> newHandler() {
        return new TransportResponseHandler<>() {
            @Override
            public ActionResponse.Empty read(StreamInput in) throws IOException {
                return ActionResponse.Empty.INSTANCE;
            }

            @Override
            public Executor executor() {
                return TransportResponseHandler.TRANSPORT_WORKER;
            }

            @Override
            public void handleResponse(ActionResponse.Empty response) {}

            @Override
            public void handleException(TransportException exp) {}
        };
    }

    public void testAddReservesBytesInBreaker() {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(Long.MAX_VALUE);
        Transport.ResponseHandlers handlers = new Transport.ResponseHandlers(() -> breaker);

        TransportResponseHandler<ActionResponse.Empty> handler = newHandler();
        long expectedBytes = handler.ramBytesUsed();
        assertTrue("handler should report positive bytes", expectedBytes > 0);

        handlers.add(handler, null, "test-action");
        assertEquals(expectedBytes, breaker.getUsed());
    }

    public void testRemoveReleasesBytesInBreaker() {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(Long.MAX_VALUE);
        Transport.ResponseHandlers handlers = new Transport.ResponseHandlers(() -> breaker);

        long requestId = handlers.add(newHandler(), null, "test-action").requestId();
        assertTrue("breaker should have reserved bytes after add", breaker.getUsed() > 0);

        handlers.remove(requestId);
        assertEquals("breaker should be back to zero after remove", 0, breaker.getUsed());
    }

    public void testOnResponseReceivedReleasesBytesInBreaker() {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(Long.MAX_VALUE);
        Transport.ResponseHandlers handlers = new Transport.ResponseHandlers(() -> breaker);

        long requestId = handlers.add(newHandler(), null, "test-action").requestId();
        assertTrue(breaker.getUsed() > 0);

        handlers.onResponseReceived(requestId, TransportMessageListener.NOOP_LISTENER);
        assertEquals("breaker should be back to zero after onResponseReceived", 0, breaker.getUsed());
    }

    public void testPruneReleasesBytesInBreaker() {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(Long.MAX_VALUE);
        Transport.ResponseHandlers handlers = new Transport.ResponseHandlers(() -> breaker);

        handlers.add(newHandler(), null, "action-a");
        handlers.add(newHandler(), null, "action-b");
        assertTrue(breaker.getUsed() > 0);

        handlers.prune(ctx -> true);
        assertEquals("breaker should be back to zero after prune of all handlers", 0, breaker.getUsed());
    }

    public void testAddThrowsCircuitBreakingExceptionWhenBreakerIsTripped() {
        // Set a very small limit so the first add trips the breaker.
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(0);
        Transport.ResponseHandlers handlers = new Transport.ResponseHandlers(() -> breaker);

        expectThrows(CircuitBreakingException.class, () -> handlers.add(newHandler(), null, "test-action"));
        assertEquals("bytes should not be reserved after a tripped add", 0, breaker.getUsed());
    }

    public void testNoBreakerIsNoop() {
        // Constructing without a breaker should not throw and should behave as before.
        Transport.ResponseHandlers handlers = new Transport.ResponseHandlers();
        long requestId = handlers.add(newHandler(), null, "test-action").requestId();
        assertNotNull(handlers.remove(requestId));
    }

    public void testMultipleHandlersBytesAccumulate() {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker(Long.MAX_VALUE);
        Transport.ResponseHandlers handlers = new Transport.ResponseHandlers(() -> breaker);

        TransportResponseHandler<ActionResponse.Empty> handler = newHandler();
        long perHandler = handler.ramBytesUsed();
        int count = randomIntBetween(2, 10);
        for (int i = 0; i < count; i++) {
            handlers.add(newHandler(), null, "test-action");
        }
        assertEquals(perHandler * count, breaker.getUsed());
    }
}
