/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;
import org.junit.After;
import org.junit.Before;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.hamcrest.Matchers.containsString;

/**
 * What the connector does when a query is cancelled, a call times out or the satellite answers wrongly: it must return promptly and
 * leave no client, stream or allocator open. The cancel tests use a satellite that is stalled before its first batch, as a satellite
 * is while it sorts for a {@code topn}.
 */
public class FlightCancellationTests extends ESTestCase {

    private static final List<Attribute> ATTRIBUTES = List.of(referenceAttribute("id", DataType.INTEGER));

    private BlockFactory blockFactory;
    private BlockingSatelliteServer server;

    @Before
    public void startServer() throws Exception {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(NoopCircuitBreaker.INSTANCE).build();
        server = new BlockingSatelliteServer();
    }

    @After
    public void stopServer() throws Exception {
        server.close();
        assertEquals("a client or stream was left open", 0, blockFactory.arrowAllocator().getChildAllocators().size());
        assertEquals("a page was left unreleased", 0L, blockFactory.arrowAllocator().getAllocatedMemory());
    }

    private static Map<String, Object> scanFragment() {
        Map<String, Object> scan = new LinkedHashMap<>();
        scan.put("op", "scan");
        scan.put("target", "t");
        scan.put("columns", List.of(Map.of("name", "id", "type", "integer")));
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("specVersion", 0);
        request.put("format", "json");
        request.put("settings", Map.of("locale", "en-US", "timeZone", "UTC"));
        request.put("output", List.of(Map.of("name", "id", "type", "integer")));
        request.put("plan", scan);
        return request;
    }

    private FlightConnector connector(TimeValue timeout) {
        return new FlightConnector("flight://localhost:" + server.port(), timeout);
    }

    private QueryRequest request() {
        return new QueryRequest("t", List.of("id"), ATTRIBUTES, Map.of(), 1000, 0, blockFactory, scanFragment());
    }

    public void testAFragmentReadReturnsTheRows() throws Exception {
        try (
            FlightConnector connector = connector(TimeValue.timeValueSeconds(30));
            ResultCursor cursor = connector.execute(request(), Split.SINGLE)
        ) {
            assertTrue(cursor.hasNext());
            Page page = cursor.next();
            assertEquals(3, page.getPositionCount());
            page.releaseBlocks();
            assertFalse(cursor.hasNext());
        }
        assertThat(server.fragments.get(0), containsString("\"op\":\"scan\""));
    }

    public void testCancelWhileTheSatelliteIsBlockedBeforeTheFirstBatch() throws Exception {
        server.blockInDoGet = true;
        try (
            FlightConnector connector = connector(TimeValue.timeValueSeconds(30));
            ResultCursor cursor = connector.execute(request(), Split.SINGLE)
        ) {
            CompletableFuture<Boolean> read = CompletableFuture.supplyAsync(cursor::hasNext);
            assertTrue("the satellite was never reached", server.inDoGet.await(10, TimeUnit.SECONDS));
            assertFalse("the read should be blocked", read.isDone());

            long start = System.nanoTime();
            cursor.cancel();

            assertFalse("a cancelled cursor has no rows", read.get(10, TimeUnit.SECONDS));
            assertTrue("the satellite must see its call cancelled", server.doGetCancelled.await(10, TimeUnit.SECONDS));
            assertFalse(server.servedRows.get());
            cursor.close();
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            logger.info("cancel to closed client: {} ms", millis);
            assertThat("closing the client must not wait for a channel that is gone", millis, org.hamcrest.Matchers.lessThan(3000L));
        }
    }

    public void testCancelBeforeTheFirstReadMakesNoCallAtAll() throws Exception {
        try (
            FlightConnector connector = connector(TimeValue.timeValueSeconds(30));
            ResultCursor cursor = connector.execute(request(), Split.SINGLE)
        ) {
            cursor.cancel();
            assertFalse(cursor.hasNext());
        }
        assertEquals(0, server.fragments.size());
    }

    /** A pending planning call is not interrupted (it is bounded by the call deadline): the cancel takes effect when it returns. */
    public void testCancelWhileTheFlightInfoIsPendingStopsBeforeTheStream() throws Exception {
        server.blockInFlightInfo = true;
        try (
            FlightConnector connector = connector(TimeValue.timeValueSeconds(30));
            ResultCursor cursor = connector.execute(request(), Split.SINGLE)
        ) {
            CompletableFuture<Boolean> read = CompletableFuture.supplyAsync(cursor::hasNext);
            assertTrue(server.inFlightInfo.await(10, TimeUnit.SECONDS));
            cursor.cancel();
            server.release.countDown(); // the satellite answers late; the connector must not go on to read
            assertFalse(read.get(10, TimeUnit.SECONDS));
            assertFalse(server.servedRows.get());
        }
    }

    public void testFlightInfoDeadline() throws Exception {
        server.blockInFlightInfo = true;
        try (
            FlightConnector connector = connector(TimeValue.timeValueMillis(300));
            ResultCursor cursor = connector.execute(request(), Split.SINGLE)
        ) {
            long start = System.nanoTime();
            expectThrows(FlightRuntimeException.class, cursor::hasNext);
            assertThat(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start), org.hamcrest.Matchers.lessThan(10L));
        }
    }

    public void testWrongTypeIsRejectedBeforeAnyBlockAndLeavesNothingOpen() throws Exception {
        server.answerSchema = new Schema(List.of(new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        try (
            FlightConnector connector = connector(TimeValue.timeValueSeconds(30));
            ResultCursor cursor = connector.execute(request(), Split.SINGLE)
        ) {
            Exception e = expectThrows(IllegalStateException.class, cursor::hasNext);
            assertThat(e.getMessage(), containsString("does not match the fragment output"));
        }
    }

    public void testExtraColumnIsRejected() throws Exception {
        server.answerSchema = new Schema(
            List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("x", FieldType.nullable(new ArrowType.Int(32, true)), null)
            )
        );
        try (
            FlightConnector connector = connector(TimeValue.timeValueSeconds(30));
            ResultCursor cursor = connector.execute(request(), Split.SINGLE)
        ) {
            expectThrows(IllegalStateException.class, cursor::hasNext);
        }
    }

    public void testARejectedRequestIsAClientErrorWithTheSatellitesMessage() throws Exception {
        server.rejectFragmentWith = CallStatus.INVALID_ARGUMENT.withDescription("option units must be one of celsius, fahrenheit");
        try (
            FlightConnector connector = connector(TimeValue.timeValueSeconds(30));
            ResultCursor cursor = connector.execute(request(), Split.SINGLE)
        ) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, cursor::hasNext);
            assertThat(e.getMessage(), containsString("option units must be one of celsius, fahrenheit"));
            assertThat(e.getMessage(), containsString("INVALID_ARGUMENT"));
        }
    }

    public void testAnInternalFailureOfTheSatelliteIsNotMadeAClientError() throws Exception {
        server.rejectFragmentWith = CallStatus.INTERNAL.withDescription("boom");
        try (
            FlightConnector connector = connector(TimeValue.timeValueSeconds(30));
            ResultCursor cursor = connector.execute(request(), Split.SINGLE)
        ) {
            expectThrows(FlightRuntimeException.class, cursor::hasNext);
        }
    }

    /**
     * The satellite has sent a batch and is computing the next one when the query is cancelled: the page already read is complete
     * and must reach its consumer (and be releasable), the wait for the next batch ends the rows, and nothing is reported as a failure.
     */
    public void testCancelWhileWaitingForTheNextBatchKeepsThePageAndEndsTheRows() throws Exception {
        server.stallAfterFirstBatch = true;
        try (
            FlightConnector connector = connector(TimeValue.timeValueSeconds(30));
            ResultCursor cursor = connector.execute(request(), Split.SINGLE)
        ) {
            assertTrue(cursor.hasNext());
            CompletableFuture<Page> next = CompletableFuture.supplyAsync(cursor::next);
            assertTrue("the satellite never reached its stall", server.inSecondBatchStall.await(10, TimeUnit.SECONDS));
            assertFalse("next() should still be waiting for the second batch", next.isDone());

            cursor.cancel();

            Page page = next.get(10, TimeUnit.SECONDS);
            assertEquals(3, page.getPositionCount());
            page.releaseBlocks();
            assertFalse("a cancelled cursor has no more rows", cursor.hasNext());
            assertTrue(server.doGetCancelled.await(10, TimeUnit.SECONDS));
        }
    }

    public void testAClosedCursorOpensNothing() throws Exception {
        try (FlightConnector connector = connector(TimeValue.timeValueSeconds(30))) {
            ResultCursor cursor = connector.execute(request(), Split.SINGLE);
            cursor.close();
            assertFalse(cursor.hasNext());
            assertEquals(0, server.fragments.size());
        }
    }

    /** A plain read cannot carry source options: it must fail instead of returning rows read without them. */
    public void testAPlainReadOfADatasetWithOptionsFails() throws Exception {
        QueryRequest withOptions = new QueryRequest(
            "t",
            List.of("id"),
            ATTRIBUTES,
            Map.of(
                "endpoint",
                "grpc://x:1",
                "target",
                "t",
                "schema_resolution",
                "strict",
                "_definition_version",
                "v",
                "units",
                "fahrenheit"
            ),
            1000,
            0,
            blockFactory,
            null
        );
        try (FlightConnector connector = connector(TimeValue.timeValueSeconds(30))) {
            Exception e = expectThrows(IllegalStateException.class, () -> connector.execute(withOptions, Split.SINGLE));
            assertThat(e.getMessage(), containsString("[units]"));
            // the keys ES sets itself are not options
            QueryRequest plain = new QueryRequest(
                "t",
                List.of("id"),
                ATTRIBUTES,
                Map.of("endpoint", "grpc://x:1", "target", "t", "schema_resolution", "strict", "_datasource_type", "flight"),
                1000,
                0,
                blockFactory,
                null
            );
            connector.execute(plain, Split.SINGLE).close();
        }
    }
}
