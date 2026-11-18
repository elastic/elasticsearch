/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.rank.feature.RankFeatureShardRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TestProxyConnections;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.search.fetch.chunk.TransportFetchPhaseCoordinationAction.CHUNKED_FETCH_DOC_ID_ORDER;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchTransportServiceTests extends ESTestCase {

    /** A shard request route whose response handler reserves and reports trips. */
    private interface TripRoute {
        /**
         * Sends a request over {@code service} with a reservation against {@code breaker}, and returns what the search got told of a trip.
         */
        Supplier<Throwable> send(
            SearchTransportService service,
            Transport.Connection connection,
            CircuitBreaker breaker,
            Consumer<Exception> onFailure
        );
    }

    /**
     * The handler of every shard request route except scroll charges the response's pieces to the reservation it is given while they
     * arrive, and reports a trip to the search before failing the listener.
     */
    public void testShardResponseHandlersReserveAndReportTripWhileArriving() {
        for (Map.Entry<String, TripRoute> route : shardResponseRoutes().entrySet()) {
            CircuitBreaker breaker = newLimitedBreaker(ShardResponseReservationTests.TRIPPING_LIMIT);
            AtomicReference<TransportResponseHandler<?>> handler = new AtomicReference<>();
            SearchTransportService service = newHandlerCapturingService(handler);
            List<Exception> failures = new ArrayList<>();
            AtomicReference<Supplier<Throwable>> reportedTrip = new AtomicReference<>();
            // The listener guards on the trip, so the search must have it by the time the listener fails
            List<Throwable> reportedWhenFailed = new ArrayList<>();

            reportedTrip.set(
                route.getValue().send(service, new SearchAsyncActionTests.MockConnection(DiscoveryNodeUtils.create("node")), breaker, e -> {
                    reportedWhenFailed.add(reportedTrip.get().get());
                    failures.add(e);
                })
            );
            assertNotNull(route.getKey(), handler.get());

            Releasable piece = handler.get().reserveResponseBytes(40);
            assertThat(route.getKey(), breaker.getUsed(), equalTo(40L));
            piece.close();
            assertThat(route.getKey(), breaker.getUsed(), equalTo(0L));

            CircuitBreakingException trip = ShardResponseReservationTests.tripWhileArriving(handler.get());
            assertThat(route.getKey(), reportedTrip.get().get(), sameInstance(trip));
            assertThat(route.getKey(), reportedWhenFailed, contains(sameInstance(trip)));
            assertThat(route.getKey(), failures, hasSize(1));
            assertThat(route.getKey(), ExceptionsHelper.unwrap(failures.getFirst(), CircuitBreakingException.class), sameInstance(trip));
        }
    }

    /**
     * Through a gateway node, the routes whose response names a reader context the coordinator doesn't know yet charge nothing while it
     * arrives, since a trip would leave that context to the reaper. The other routes still charge.
     * {@link #testShardResponseHandlersReserveAndReportTripWhileArriving} is the control, over a direct connection.
     */
    public void testOnlyResponsesNamingKnownContextsChargeThroughGateway() {
        Set<String> uncharged = Set.of("dfs", "query");
        Transport.Connection gateway = new SearchAsyncActionTests.MockConnection(DiscoveryNodeUtils.create("gateway"));
        Transport.Connection proxied = TestProxyConnections.newProxyConnection(gateway, DiscoveryNodeUtils.create("node"));
        for (Map.Entry<String, TripRoute> route : shardResponseRoutes().entrySet()) {
            CircuitBreaker breaker = newLimitedBreaker(ShardResponseReservationTests.TRIPPING_LIMIT);
            AtomicReference<TransportResponseHandler<?>> handler = new AtomicReference<>();
            SearchTransportService service = newHandlerCapturingService(handler);

            route.getValue().send(service, proxied, breaker, e -> fail(route.getKey() + " must not fail"));
            assertNotNull(route.getKey(), handler.get());

            Releasable piece = handler.get().reserveResponseBytes(40);
            assertThat(route.getKey(), breaker.getUsed(), equalTo(uncharged.contains(route.getKey()) ? 0L : 40L));
            piece.close();
        }
    }

    private static Map<String, TripRoute> shardResponseRoutes() {
        // The transport is captured rather than run, so the requests are never serialized and can be mocks
        Map<String, TripRoute> routes = new LinkedHashMap<>();
        routes.put(
            "dfs",
            reserving((s, c, r, f) -> s.sendExecuteDfs(c, mock(ShardSearchRequest.class), null, r, failing(f), l -> {}, l -> {}))
        );
        routes.put(
            "query",
            reserving((s, c, r, f) -> s.sendExecuteQuery(c, mock(ShardSearchRequest.class), null, r, failing(f), l -> {}, l -> {}))
        );
        routes.put(
            "query by id",
            reserving((s, c, r, f) -> s.sendExecuteQuery(c, mock(QuerySearchRequest.class), null, r, failing(f), l -> {}, l -> {}))
        );
        routes.put(
            "rank feature",
            reserving(
                (s, c, r, f) -> s.sendExecuteRankFeature(c, mock(RankFeatureShardRequest.class), null, r, failing(f), l -> {}, l -> {})
            )
        );
        // The fetch phase builds its reservation from the search, so a trip fails the search through it
        routes.put("fetch", (service, connection, breaker, onFailure) -> {
            MockSearchPhaseContext context = new MockSearchPhaseContext(1, breaker);
            context.results.close();
            service.sendExecuteFetch(
                connection,
                mock(ShardFetchSearchRequest.class),
                context,
                new SearchShardTarget(connection.getNode().getId(), new ShardId("idx", "_na_", 0), null),
                failing(onFailure),
                l -> {},
                l -> {}
            );
            return context.phaseFailure::get;
        });

        return routes;
    }

    /** A service whose requests are captured rather than sent, so that a test can drive the handler of the last one. */
    private static SearchTransportService newHandlerCapturingService(AtomicReference<TransportResponseHandler<?>> handler) {
        var transportService = mock(TransportService.class);
        doAnswer(invocation -> {
            handler.set(invocation.getArgument(4));
            return null;
        }).when(transportService).sendChildRequest(any(Transport.Connection.class), any(), any(), any(), any());
        SearchTransportService service = new SearchTransportService(transportService, null, (connection, listener) -> listener);
        // Only asked whether fetch is chunked, and the classic fetch route is the one under test
        SearchService searchService = mock(SearchService.class);
        when(searchService.fetchPhaseChunked()).thenReturn(false);
        service.setSearchService(searchService);
        return service;
    }

    /** A route that is handed its reservation, which reports trips to a list. */
    private interface ReservingRoute {
        void send(
            SearchTransportService service,
            Transport.Connection connection,
            ShardResponseReservation reservation,
            Consumer<Exception> onFailure
        );
    }

    private static TripRoute reserving(ReservingRoute route) {
        return (service, connection, breaker, onFailure) -> {
            List<Exception> trips = new ArrayList<>();
            route.send(
                service,
                connection,
                new ShardResponseReservation(breaker, ShardResponseReservation.BREAKER_LABEL, trips::add),
                onFailure
            );
            return () -> trips.isEmpty() ? null : trips.getFirst();
        };
    }

    private static <T> ActionListener<T> failing(Consumer<Exception> onFailure) {
        return ActionListener.wrap(r -> fail("the response must be rejected"), onFailure::accept);
    }

    /** A held last chunk stays charged while the coordinator keeps slices of it, and the charge goes with its pages. */
    public void testChargeHeldChunkLastsUntilThePagesAreFreed() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(10));
        AtomicBoolean pagesFreed = new AtomicBoolean();
        int length = randomIntBetween(1, 100_000);
        ReleasableBytesReference pages = new ReleasableBytesReference(new BytesArray(randomByteArrayOfLength(length)), () -> {
            pagesFreed.set(true);
        });

        ReleasableBytesReference held = SearchTransportService.chargeHeldChunk(pages, breaker);
        assertThat(breaker.getUsed(), equalTo((long) length));

        ReleasableBytesReference coordinatorSlice = held.retainedSlice(0, length);
        held.close();
        assertThat("the coordinator still holds the pages", breaker.getUsed(), equalTo((long) length));
        assertFalse(pagesFreed.get());

        coordinatorSlice.close();
        assertTrue(pagesFreed.get());
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /** A held last chunk that does not fit trips the breaker and leaves its pages to the caller, which closes them with the chunk. */
    public void testChargeHeldChunkTrips() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofKb(1));
        AtomicBoolean pagesFreed = new AtomicBoolean();
        ReleasableBytesReference pages = new ReleasableBytesReference(
            new BytesArray(randomByteArrayOfLength(4096)),
            () -> { pagesFreed.set(true); }
        );

        expectThrows(CircuitBreakingException.class, () -> SearchTransportService.chargeHeldChunk(pages, breaker));
        assertThat(breaker.getUsed(), equalTo(0L));
        assertFalse(pagesFreed.get());

        pages.close();
        assertTrue(pagesFreed.get());
    }

    /**
     * Verifies that {@link SearchTransportService#countingRequest} and
     * {@link SearchTransportService#countingReader} report the same byte count for an identical payload,
     * ensuring that the send-side and receive-side accounting are consistent.
     */
    public void testRequestAndReaderBytesAgree() throws IOException {
        int intVal = randomInt();
        String strVal = randomAlphaOfLengthBetween(1, 64);
        long longVal = randomLong();
        boolean boolVal = randomBoolean();
        String optStr = randomBoolean() ? null : randomAlphaOfLength(10);

        AbstractTransportRequest inner = new AbstractTransportRequest() {
            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeInt(intVal);
                out.writeString(strVal);
                out.writeLong(longVal);
                out.writeBoolean(boolVal);
                out.writeOptionalString(optStr);
            }
        };

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersion.current());

        long[] written = new long[1];
        SearchTransportService.countingRequest(inner, b -> written[0] = b).writeTo(out);

        long[] read = new long[1];
        Writeable.Reader<String> countingReader = SearchTransportService.countingReader(in -> {
            in.readInt();
            in.readString();
            in.readLong();
            in.readBoolean();
            in.readOptionalString();
            return "ok";
        }, b -> read[0] = b);

        var streamInput = out.bytes().streamInput();
        streamInput.setTransportVersion(TransportVersion.current());
        countingReader.read(streamInput);

        assertThat(written[0], equalTo(read[0]));
        assertThat(written[0], greaterThan(0L));
    }

    /**
     * Verifies that {@link SearchTransportService#countingRequest} counts the exact number of bytes
     * written by the inner request's {@code writeTo}.
     */
    public void testCountingRequestCountsBytes() throws IOException {
        AbstractTransportRequest request = new AbstractTransportRequest() {
            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeInt(1);
                out.writeInt(2);
            }
        };

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersion.current());

        long[] counted = new long[1];
        SearchTransportService.countingRequest(request, b -> counted[0] = b).writeTo(out);

        // Two ints = 8 bytes
        assertThat(counted[0], equalTo(8L));
        assertThat(out.size(), equalTo(8));
    }

    public void testPopulateBwcLastChunkBytesWritesPositionsForDocIdOrderCoordinators() throws IOException {
        assertBwcLastChunkBytes(TransportVersionUtils.randomVersionSupporting(CHUNKED_FETCH_DOC_ID_ORDER), true);
    }

    public void testPopulateBwcLastChunkBytesOmitsPositionsForOlderCoordinators() throws IOException {
        assertBwcLastChunkBytes(TransportVersionUtils.randomVersionNotSupporting(CHUNKED_FETCH_DOC_ID_ORDER), false);
    }

    private static void assertBwcLastChunkBytes(TransportVersion coordinatorVersion, boolean withPositions) throws IOException {
        FetchSearchResult result = new FetchSearchResult(
            new ShardSearchContextId("test", 1L),
            new SearchShardTarget("node", new ShardId(new Index("idx", "uuid"), 0), null)
        );
        try {
            SearchHit[] hits = new SearchHit[] { new SearchHit(0, "doc-0"), new SearchHit(1, "doc-1"), new SearchHit(2, "doc-2") };
            result.shardResult(new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), Float.NaN), null);

            SearchTransportService.populateBwcLastChunkBytes(result, coordinatorVersion);

            assertThat(result.getLastChunkHitCount(), equalTo(hits.length));
            try (StreamInput in = result.getLastChunkBytes().streamInput()) {
                in.setTransportVersion(coordinatorVersion);
                for (int i = 0; i < hits.length; i++) {
                    if (withPositions) {
                        assertThat(in.readVInt(), equalTo(i));
                    }
                    SearchHit hit = SearchHit.readFrom(in);
                    try {
                        assertThat(hit.getId(), equalTo("doc-" + i));
                    } finally {
                        hit.decRef();
                    }
                }
                assertThat(in.available(), equalTo(0));
            }
        } finally {
            result.decRef();
        }
    }
}
