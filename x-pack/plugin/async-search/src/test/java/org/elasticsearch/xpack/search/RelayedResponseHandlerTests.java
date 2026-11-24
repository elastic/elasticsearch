/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * Drives {@link TransportGetAsyncSearchAction.RelayedResponseHandler} as the transport does for a relayed response: the pieces are
 * reserved while they arrive, the reservations are released once the response is read, then the handler gets the response, which the
 * transport releases once the handler returns.
 */
public class RelayedResponseHandlerTests extends ESTestCase {

    private static final int ARRIVED_BYTES = 1024;

    /** The size the response arrived with, plus the heap of its hits, stays charged until the response is released. */
    public void testChargesTheResponseItKeepsUntilReleased() {
        CircuitBreaker breaker = requestBreaker("1mb");
        AtomicReference<AsyncSearchResponse> received = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        var handler = new TransportGetAsyncSearchAction.RelayedResponseHandler(keeping(received, failure), breaker);

        arrive(handler);
        assertThat(breaker.getUsed(), equalTo(0L));
        handOver(handler, responseWithHits());

        assertThat(failure.get(), nullValue());
        long hitsBytes = 0;
        for (SearchHit hit : received.get().getSearchResponse().getHits().getHits()) {
            hitsBytes += hit.ramBytesUsed() - hit.rawSourceLength();
        }
        assertThat(breaker.getUsed(), equalTo(ARRIVED_BYTES + hitsBytes));
        received.get().decRef();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** A response that doesn't fit while it arrives fails the request with the trip itself, which is a 429. */
    public void testFailsWhenTheResponseDoesNotFitWhileArriving() {
        CircuitBreaker breaker = requestBreaker("1kb");
        AtomicReference<AsyncSearchResponse> received = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        var handler = new TransportGetAsyncSearchAction.RelayedResponseHandler(keeping(received, failure), breaker);

        Releasable firstPiece = handler.reserveResponseBytes(ARRIVED_BYTES / 2);
        CircuitBreakingException trip = expectThrows(CircuitBreakingException.class, () -> handler.reserveResponseBytes(ARRIVED_BYTES));
        firstPiece.close();
        // The transport fails the response with the trip wrapped.
        handler.handleException(new RemoteTransportException("rejected", trip));

        assertThat(received.get(), nullValue());
        assertSame(trip, failure.get());
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** A response that fits while it arrives, but not with the heap of its hits, fails the request once read. */
    public void testFailsWhenTheReadResponseDoesNotFit() {
        CircuitBreaker breaker = requestBreaker("2kb");
        AtomicReference<AsyncSearchResponse> received = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        var handler = new TransportGetAsyncSearchAction.RelayedResponseHandler(keeping(received, failure), breaker);

        arrive(handler);
        handOver(handler, responseWithHits());

        assertThat(received.get(), nullValue());
        // The trip itself, so that it fails the request with a 429 rather than as a failure of the node that runs the search.
        assertThat(failure.get(), instanceOf(CircuitBreakingException.class));
        assertThat(failure.get().getMessage(), containsString("[" + TransportGetAsyncSearchAction.RELAYED_RESPONSE_BREAKER_LABEL + "]"));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** A response without search results, such as one for a search that failed, carries no hits and is passed on uncharged. */
    public void testPassesOnResponseWithoutSearchResults() {
        CircuitBreaker breaker = requestBreaker("1mb");
        AtomicReference<AsyncSearchResponse> received = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        var handler = new TransportGetAsyncSearchAction.RelayedResponseHandler(keeping(received, failure), breaker);

        arrive(handler);
        handOver(handler, new AsyncSearchResponse("id", null, new ElasticsearchException("boom"), true, false, 0L, 0L));

        assertThat(failure.get(), nullValue());
        assertThat(breaker.getUsed(), equalTo(0L));
        received.get().decRef();
    }

    private static void arrive(TransportGetAsyncSearchAction.RelayedResponseHandler handler) {
        int first = between(1, ARRIVED_BYTES - 1);
        Releasable firstPiece = handler.reserveResponseBytes(first);
        Releasable secondPiece = handler.reserveResponseBytes(ARRIVED_BYTES - first);
        firstPiece.close();
        secondPiece.close();
    }

    private static void handOver(TransportGetAsyncSearchAction.RelayedResponseHandler handler, AsyncSearchResponse response) {
        try {
            handler.handleResponse(response);
        } finally {
            response.decRef();
        }
    }

    /** Keeps a reference to the response, as the listener that writes it does until it is written. */
    private static ActionListener<AsyncSearchResponse> keeping(
        AtomicReference<AsyncSearchResponse> received,
        AtomicReference<Exception> failure
    ) {
        return ActionListener.wrap(response -> {
            response.mustIncRef();
            assertTrue(received.compareAndSet(null, response));
        }, e -> assertTrue(failure.compareAndSet(null, e)));
    }

    /** Enough hits and fields that their heap doesn't fit in the room a 2kb breaker leaves after {@link #ARRIVED_BYTES}. */
    private static AsyncSearchResponse responseWithHits() {
        SearchHit[] hits = new SearchHit[3];
        for (int i = 0; i < hits.length; i++) {
            hits[i] = new SearchHit(i, "id-" + i);
            hits[i].sourceRef(new BytesArray("{}"));
            Map<String, DocumentField> fields = new HashMap<>();
            for (int f = 0; f < 10; f++) {
                fields.put("field-" + f, new DocumentField("field-" + f, List.of("value")));
            }
            hits[i].addDocumentFields(fields, Map.of());
        }
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1f);
        SearchResponse searchResponse = SearchResponseUtils.response(searchHits).build();
        searchHits.decRef();
        try {
            return new AsyncSearchResponse("id", searchResponse, null, false, false, 0L, 0L);
        } finally {
            searchResponse.decRef();
        }
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
