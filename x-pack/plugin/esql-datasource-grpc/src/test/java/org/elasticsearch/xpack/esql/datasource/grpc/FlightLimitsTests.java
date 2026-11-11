/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.flight.CallStatus;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.junit.After;
import org.junit.Before;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * What a satellite can make a node do is capped: the size of a message, the columns of a table, the endpoints of a flight info, the
 * time a stream may stay silent and the text of its errors; and a plain Flight server is checked against the plan like a satellite
 * is. Each test drives a satellite that misbehaves in one way.
 */
public class FlightLimitsTests extends ESTestCase {

    private static final List<Attribute> ATTRIBUTES = List.of(referenceAttribute("id", DataType.INTEGER));
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    private BlockFactory blockFactory;
    private BlockingSatelliteServer server;

    @Before
    public void startServer() throws Exception {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(NoopCircuitBreaker.INSTANCE).build();
        server = new BlockingSatelliteServer();
    }

    @After
    public void stopServer() throws Exception {
        assertBusy(() -> assertEquals("the satellite still serves a call", 0L, server.allocatedBytes()));
        server.close();
        assertEquals("a client or stream was left open", 0, blockFactory.arrowAllocator().getChildAllocators().size());
        assertEquals("a page was left unreleased", 0L, blockFactory.arrowAllocator().getAllocatedMemory());
    }

    private static FlightGuard guard(long maxMessageBytes, int maxEndpoints, int maxColumns, TimeValue idle) {
        return new FlightGuard(FlightGuard.DEFAULT_ALLOWED, new FlightGuard.Limits(maxMessageBytes, maxEndpoints, maxColumns, idle));
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

    private FlightConnector connector(FlightGuard guard) {
        return new FlightConnector(
            "flight://localhost:" + server.port(),
            FlightClients.Transport.PLAIN,
            TIMEOUT,
            new SatelliteDeclarationCache(TimeValue.ZERO),
            guard
        );
    }

    private QueryRequest request(List<Attribute> attributes, Map<String, Object> fragment) {
        return new QueryRequest(
            "t",
            attributes.stream().map(Attribute::name).toList(),
            attributes,
            Map.of(),
            1000,
            0,
            blockFactory,
            fragment
        );
    }

    public void testATableWithMoreColumnsThanTheLimitIsNotResolved() {
        server.extraColumns = 5;
        FlightConnectorFactory factory = new FlightConnectorFactory(
            TIMEOUT,
            new SatelliteDeclarationCache(TimeValue.ZERO),
            guard(1 << 20, 10, 3, TimeValue.ZERO)
        );
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> factory.resolveMetadata("flight://localhost:" + server.port() + "/t", Map.of())
        );
        assertThat(e.getMessage(), containsString("[6] columns, the limit is [3]"));
        assertThat(e.getMessage(), containsString("esql.datasource.flight.max_columns"));
        server.extraColumns = 2;
        assertNotNull(factory.resolveMetadata("flight://localhost:" + server.port() + "/t", Map.of()));
    }

    public void testAFlightInfoWithMoreEndpointsThanTheLimitIsNotSplit() {
        server.endpointCount = 5;
        Map<String, Object> config = Map.of("endpoint", "flight://localhost:" + server.port(), "target", "t");
        SplitDiscoveryContext context = new SplitDiscoveryContext(null, FileList.UNRESOLVED, config, null, null);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new FlightSplitProvider(TIMEOUT, guard(1 << 20, 4, 10, TimeValue.ZERO)).discoverSplits(context)
        );
        assertThat(e.getMessage(), containsString("[5] endpoints"));
        assertThat(e.getMessage(), containsString("esql.datasource.flight.max_endpoints"));
        List<ExternalSplit> splits = new FlightSplitProvider(TIMEOUT, guard(1 << 20, 5, 10, TimeValue.ZERO)).discoverSplits(context)
            .splits();
        assertEquals(5, splits.size());
    }

    public void testAMessageLargerThanTheLimitFailsTheReadAndReleasesEverything() throws Exception {
        server.oneBatchOfRows = 200_000; // 800 KB of ids in one record batch
        try (
            FlightConnector connector = connector(guard(64 * 1024, 10, 10, TimeValue.ZERO));
            ResultCursor cursor = connector.execute(request(ATTRIBUTES, scanFragment()), Split.SINGLE)
        ) {
            Exception e = expectThrows(RuntimeException.class, cursor::hasNext);
            assertThat(e.toString(), containsString("RESOURCE_EXHAUSTED"));
        }
        // the server is still failing its writes of the batch the client refused: let it finish before the server is stopped, or
        // gRPC logs a rejected task of the stopping server and the next test is blamed for it
        Thread.sleep(500);
    }

    public void testAMessageUnderTheLimitIsRead() throws Exception {
        server.oneBatchOfRows = 200_000;
        try (
            FlightConnector connector = connector(guard(4 << 20, 10, 10, TimeValue.ZERO));
            ResultCursor cursor = connector.execute(request(ATTRIBUTES, scanFragment()), Split.SINGLE)
        ) {
            assertTrue(cursor.hasNext());
            Page page = cursor.next();
            assertEquals(200_000, page.getPositionCount());
            page.releaseBlocks();
        }
    }

    public void testAStreamThatGoesSilentAfterABatchFailsTheRead() throws Exception {
        server.stallAfterFirstBatch = true;
        try (
            FlightConnector connector = connector(guard(1 << 20, 10, 10, TimeValue.timeValueMillis(300)));
            ResultCursor cursor = connector.execute(request(ATTRIBUTES, scanFragment()), Split.SINGLE)
        ) {
            assertTrue(cursor.hasNext());
            long start = System.nanoTime();
            Exception e = expectThrows(IllegalStateException.class, cursor::next);
            assertThat(e.getMessage(), containsString("sent nothing for"));
            assertThat(e.getMessage(), containsString("esql.datasource.flight.stream_idle_timeout"));
            assertThat(
                "the timeout is what stopped the read",
                TimeValue.timeValueNanos(System.nanoTime() - start).seconds(),
                lessThan(10L)
            );
        }
    }

    public void testAStreamThatNeverStartsFailsTheRead() throws Exception {
        server.blockInDoGet = true;
        try (
            FlightConnector connector = connector(guard(1 << 20, 10, 10, TimeValue.timeValueMillis(300)));
            ResultCursor cursor = connector.execute(request(ATTRIBUTES, scanFragment()), Split.SINGLE)
        ) {
            Exception e = expectThrows(IllegalStateException.class, cursor::hasNext);
            assertThat(e.getMessage(), containsString("sent nothing for"));
            assertTrue("the satellite must see its call cancelled", server.doGetCancelled.await(10, java.util.concurrent.TimeUnit.SECONDS));
        }
    }

    public void testAStreamThatKeepsSendingIsNotCutOff() throws Exception {
        try (
            FlightConnector connector = connector(guard(1 << 20, 10, 10, TimeValue.timeValueSeconds(5)));
            ResultCursor cursor = connector.execute(request(ATTRIBUTES, scanFragment()), Split.SINGLE)
        ) {
            assertTrue(cursor.hasNext());
            cursor.next().releaseBlocks();
            assertFalse(cursor.hasNext());
        }
    }

    public void testWhatASatelliteSaysInAnErrorIsBoundedAndMarked() throws Exception {
        String hostile = "line one\nline two\u0007" + "x".repeat(5_000);
        server.rejectFragmentWith = CallStatus.INVALID_ARGUMENT.withDescription(hostile);
        try (
            FlightConnector connector = connector(FlightGuard.DEFAULT);
            ResultCursor cursor = connector.execute(request(ATTRIBUTES, scanFragment()), Split.SINGLE)
        ) {
            Exception e = expectThrows(IllegalArgumentException.class, cursor::hasNext);
            assertThat(e.getMessage(), containsString("the satellite says:"));
            assertThat(e.getMessage(), not(containsString("\n")));
            assertThat(e.getMessage(), not(containsString("\u0007")));
            assertThat(e.getMessage().length(), lessThan(400));
        }
    }

    public void testTheTextHelperHandlesNothingAndEverything() {
        assertThat(FlightClients.satelliteText(null), containsString("no message"));
        assertThat(FlightClients.satelliteText("  "), containsString("no message"));
        assertEquals("(the satellite says: short)", FlightClients.satelliteText("short"));
    }

    /** A plain Flight server is any server on the network: the stream must still hold what the plan was built for. */
    public void testAPlainReadFailsWhenAColumnTheServerSendsIsNotWhatWasResolved() throws Exception {
        try (FlightConnector connector = connector(FlightGuard.DEFAULT)) {
            List<Attribute> missing = List.of(
                referenceAttribute("id", DataType.INTEGER),
                referenceAttribute("not_there", DataType.INTEGER)
            );
            try (ResultCursor cursor = connector.execute(request(missing, null), Split.SINGLE)) {
                Exception e = expectThrows(IllegalStateException.class, cursor::hasNext);
                assertThat(e.getMessage(), containsString("has no readable field [not_there]"));
            }
            List<Attribute> retyped = List.of(referenceAttribute("id", DataType.KEYWORD));
            try (ResultCursor cursor = connector.execute(request(retyped, null), Split.SINGLE)) {
                Exception e = expectThrows(IllegalStateException.class, cursor::hasNext);
                assertThat(e.getMessage(), containsString("field [id] is [INTEGER], expected [KEYWORD]"));
            }
            try (ResultCursor cursor = connector.execute(request(ATTRIBUTES, null), Split.SINGLE)) {
                assertTrue(cursor.hasNext());
                cursor.next().releaseBlocks();
            }
        }
    }

    /** The table was within the column limit when it was resolved; the stream of a plain server is checked against it again. */
    public void testAPlainStreamWithMoreFieldsThanTheLimitFailsTheRead() throws Exception {
        server.answerSchema = new org.apache.arrow.vector.types.pojo.Schema(
            java.util.stream.IntStream.range(0, 12)
                .mapToObj(
                    i -> new org.apache.arrow.vector.types.pojo.Field(
                        i == 0 ? "id" : "extra" + i,
                        org.apache.arrow.vector.types.pojo.FieldType.nullable(
                            new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true)
                        ),
                        null
                    )
                )
                .toList()
        );
        try (
            FlightConnector connector = connector(guard(1 << 20, 10, 5, TimeValue.ZERO));
            ResultCursor cursor = connector.execute(request(ATTRIBUTES, null), Split.SINGLE)
        ) {
            Exception e = expectThrows(IllegalStateException.class, cursor::hasNext);
            assertThat(e.getMessage(), containsString("has [12] fields, the limit is [5]"));
        }
    }

    public void testACapabilityAnswerWithMoreThanOneResultFailsTheResolution() throws Exception {
        server.capabilityResults = 3;
        FlightConnectorFactory factory = new FlightConnectorFactory(
            TIMEOUT,
            new SatelliteDeclarationCache(TimeValue.ZERO),
            FlightGuard.DEFAULT
        );
        Exception e = expectThrows(
            RuntimeException.class,
            () -> factory.resolveMetadata("flight://localhost:" + server.port() + "/t", Map.of())
        );
        assertThat(e.getMessage() + e.getCause(), containsString("more than one result"));
    }
}
