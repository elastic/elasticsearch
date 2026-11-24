/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * Drives {@link AbstractTransportQlAsyncGetResultsAction.RelayedResponseHandler} as the transport does for a relayed response: the
 * pieces are reserved while they arrive, the reservations are released once the response is read, then the handler gets the response.
 */
public class RelayedResponseHandlerTests extends ESTestCase {

    private static final String LABEL = "<relayed_response>";

    private static final int ARRIVED_BYTES = 1024;

    /** The size the response arrived with is charged while the listener runs, and released once it returns. */
    public void testChargesTheArrivedSizeWhileTheListenerRuns() {
        CircuitBreaker breaker = requestBreaker("1mb");
        AtomicLong usedInListener = new AtomicLong(-1);
        AtomicReference<Exception> failure = new AtomicReference<>();
        var handler = handler(
            ActionListener.wrap(response -> usedInListener.set(breaker.getUsed()), e -> assertTrue(failure.compareAndSet(null, e))),
            breaker
        );

        arrive(handler);
        assertThat(breaker.getUsed(), equalTo(0L));
        handler.handleResponse(ActionResponse.Empty.INSTANCE);

        assertThat(failure.get(), nullValue());
        assertThat(usedInListener.get(), equalTo((long) ARRIVED_BYTES));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** A listener that throws still releases the charge. */
    public void testReleasesTheChargeWhenTheListenerThrows() {
        CircuitBreaker breaker = requestBreaker("1mb");
        AtomicLong usedInListener = new AtomicLong(-1);
        var handler = handler(new ActionListener<>() {
            @Override
            public void onResponse(ActionResponse response) {
                usedInListener.set(breaker.getUsed());
                throw new IllegalStateException("boom");
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        }, breaker);

        arrive(handler);
        expectThrows(IllegalStateException.class, () -> handler.handleResponse(ActionResponse.Empty.INSTANCE));

        assertThat(usedInListener.get(), equalTo((long) ARRIVED_BYTES));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** A response that doesn't fit while it arrives fails the request with the trip itself, which is a 429. */
    public void testFailsWhenTheResponseDoesNotFitWhileArriving() {
        CircuitBreaker breaker = requestBreaker("1kb");
        AtomicReference<ActionResponse> received = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        var handler = handler(keeping(received, failure), breaker);

        Releasable firstPiece = handler.reserveResponseBytes(ARRIVED_BYTES / 2);
        CircuitBreakingException trip = expectThrows(CircuitBreakingException.class, () -> handler.reserveResponseBytes(ARRIVED_BYTES));
        firstPiece.close();
        // The transport fails the response with the trip wrapped.
        handler.handleException(new RemoteTransportException("rejected", trip));

        assertThat(received.get(), nullValue());
        assertSame(trip, failure.get());
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** A response that fit while it arrived, but no longer does once read, fails the request with the trip and isn't passed on. */
    public void testFailsWhenTheReadResponseDoesNotFit() {
        CircuitBreaker breaker = requestBreaker("1kb");
        AtomicReference<ActionResponse> received = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        var handler = handler(keeping(received, failure), breaker);

        int arrived = ARRIVED_BYTES / 2;
        handler.reserveResponseBytes(arrived).close();
        // Something else took the room in between.
        long other = ARRIVED_BYTES - arrived + 1;
        breaker.addEstimateBytesAndMaybeBreak(other, "other");
        handler.handleResponse(ActionResponse.Empty.INSTANCE);

        assertThat(received.get(), nullValue());
        assertThat(failure.get(), instanceOf(CircuitBreakingException.class));
        assertThat(failure.get().getMessage(), containsString("[" + LABEL + "]"));
        assertThat(breaker.getUsed(), equalTo(other));
    }

    private static AbstractTransportQlAsyncGetResultsAction.RelayedResponseHandler<ActionResponse> handler(
        ActionListener<ActionResponse> listener,
        CircuitBreaker breaker
    ) {
        return new AbstractTransportQlAsyncGetResultsAction.RelayedResponseHandler<>(
            listener,
            in -> ActionResponse.Empty.INSTANCE,
            breaker,
            LABEL
        );
    }

    private static void arrive(AbstractTransportQlAsyncGetResultsAction.RelayedResponseHandler<ActionResponse> handler) {
        int first = between(1, ARRIVED_BYTES - 1);
        Releasable firstPiece = handler.reserveResponseBytes(first);
        Releasable secondPiece = handler.reserveResponseBytes(ARRIVED_BYTES - first);
        firstPiece.close();
        secondPiece.close();
    }

    private static ActionListener<ActionResponse> keeping(AtomicReference<ActionResponse> received, AtomicReference<Exception> failure) {
        return ActionListener.wrap(
            response -> assertTrue(received.compareAndSet(null, response)),
            e -> assertTrue(failure.compareAndSet(null, e))
        );
    }

    private static CircuitBreaker requestBreaker(String limit) {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limit)
            .build();
        return new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            settings,
            List.of(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ).getBreaker(CircuitBreaker.REQUEST);
    }
}
