/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.flight.CallStatus;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class FlightConnectorFactoryTests extends ESTestCase {

    private final FlightConnectorFactory factory = new FlightConnectorFactory();

    public void testType() {
        assertEquals("flight", factory.type());
    }

    public void testCanHandleFlightUri() {
        assertTrue(factory.canHandle("flight://localhost:47470/employees"));
        assertTrue(factory.canHandle("flight://dremio.example.com:47470/my_table"));
    }

    public void testCanHandleGrpcUri() {
        assertTrue(factory.canHandle("grpc://localhost:47470/employees"));
        assertTrue(factory.canHandle("grpc://server:9090/data"));
    }

    public void testCanHandleRejectsOtherSchemes() {
        assertFalse(factory.canHandle("http://example.com/data.parquet"));
        assertFalse(factory.canHandle("s3://bucket/key.parquet"));
        assertFalse(factory.canHandle("jdbc:h2:mem:test"));
        assertFalse(factory.canHandle("/local/path/file.csv"));
    }

    public void testResolveMetadataReturnsCorrectSchema() throws IOException {
        try (EmployeeFlightServer server = new EmployeeFlightServer(0)) {
            String location = "flight://localhost:" + server.port() + "/employees";
            SourceMetadata metadata = factory.resolveMetadata(location, Map.of());

            assertEquals("flight", metadata.sourceType());
            assertEquals(location, metadata.location());

            List<Attribute> schema = metadata.schema();
            assertEquals(6, schema.size());

            assertEquals("emp_no", schema.get(0).name());
            assertEquals(DataType.INTEGER, schema.get(0).dataType());

            assertEquals("first_name", schema.get(1).name());
            assertEquals(DataType.KEYWORD, schema.get(1).dataType());

            assertEquals("last_name", schema.get(2).name());
            assertEquals(DataType.KEYWORD, schema.get(2).dataType());

            assertEquals("salary", schema.get(3).name());
            assertEquals(DataType.INTEGER, schema.get(3).dataType());

            assertEquals("still_hired", schema.get(4).name());
            assertEquals(DataType.BOOLEAN, schema.get(4).dataType());

            assertEquals("height", schema.get(5).name());
            assertEquals(DataType.DOUBLE, schema.get(5).dataType());
        }
    }

    /**
     * A declaration shaped for a spec version ES does not know may hold names ES cannot judge, a reserved function included: it is
     * a plain Level 0 satellite, not a resolution failure (protocol section 5.2). One that shares a version is checked as ever.
     */
    public void testADeclarationOfOtherVersionsIsLevelZeroWhateverItHolds() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            String location = "flight://127.0.0.1:" + satellite.port() + "/t";
            satellite.capabilities = "{\"specVersions\":[2],\"fragmentFormats\":[\"json\"],\"plan\":[\"scan\",\"aggregate\"],"
                + "\"functions\":[\"div\"],\"types\":[\"integer\"]}";
            SourceMetadata metadata = factory.resolveMetadata(location, Map.of());
            assertFalse(metadata.sourceMetadata().containsKey("_satellite"));

            satellite.capabilities = "{\"specVersions\":[0,1],\"fragmentFormats\":[\"json\"],\"plan\":[\"scan\"],"
                + "\"functions\":[\"div\"],\"types\":[\"integer\"]}";
            Exception e = expectThrows(IllegalStateException.class, () -> factory.resolveMetadata(location, Map.of()));
            assertThat(e.getCause().getMessage(), org.hamcrest.Matchers.containsString("div"));

            satellite.capabilities = "{\"specVersions\":[0,1],\"fragmentFormats\":[\"json\"],\"plan\":[\"scan\"],"
                + "\"functions\":[],\"types\":[\"integer\"]}";
            assertTrue(factory.resolveMetadata(location, Map.of()).sourceMetadata().containsKey("_satellite"));
        }
    }

    /** The capability round trip is a cost per query on a remote endpoint: within the time to live a node reads the declaration once. */
    public void testTheDeclarationIsReadOnceWithinTheTimeToLive() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            FlightConnectorFactory caching = new FlightConnectorFactory(
                GrpcDataSourcePlugin.DEFAULT_CALL_TIMEOUT,
                TimeValue.timeValueMinutes(1)
            );
            String location = "flight://127.0.0.1:" + satellite.port() + "/t";
            assertTrue(caching.resolveMetadata(location, Map.of()).sourceMetadata().containsKey("_satellite"));
            assertTrue(caching.resolveMetadata(location, Map.of()).sourceMetadata().containsKey("_satellite"));
            assertEquals(1, satellite.capabilityCalls.get());
        }
    }

    public void testWithoutACacheTheDeclarationIsReadOnEveryResolution() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            String location = "flight://127.0.0.1:" + satellite.port() + "/t";
            factory.resolveMetadata(location, Map.of());
            factory.resolveMetadata(location, Map.of());
            assertEquals(2, satellite.capabilityCalls.get());
        }
    }

    /** What the table is now is not cached with the declaration: the options of each dataset are checked against it every time. */
    public void testTheOptionsOfEachDatasetAreCheckedAgainstTheCachedDeclaration() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            FlightConnectorFactory caching = new FlightConnectorFactory(
                GrpcDataSourcePlugin.DEFAULT_CALL_TIMEOUT,
                TimeValue.timeValueMinutes(1)
            );
            String location = "flight://127.0.0.1:" + satellite.port() + "/t";
            caching.resolveMetadata(location, Map.of("units", "celsius"));
            Exception e = expectThrows(IllegalArgumentException.class, () -> caching.resolveMetadata(location, Map.of("humidity", "high")));
            assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("does not declare the source option [humidity]"));
            assertEquals(1, satellite.capabilityCalls.get());
        }
    }

    private static String declaration(String id, String plan) {
        return "{\"specVersions\":[0],\"fragmentFormats\":[\"json\"],\"plan\":"
            + plan
            + ",\"functions\":[],\"types\":[\"integer\"]"
            + (id == null ? "" : ",\"declarationId\":\"" + id + "\"")
            + "}";
    }

    private static List<?> planOf(SourceMetadata metadata) {
        Map<?, ?> stamp = (Map<?, ?>) metadata.sourceMetadata().get("_satellite");
        return (List<?>) stamp.get("plan");
    }

    /**
     * While the satellite gives the id the copy was kept under, the copy is current whatever the time to live says: the
     * round trip of the capability action is not repeated.
     */
    public void testAKeptDeclarationStaysCurrentWhileTheSatelliteGivesTheSameId() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            AtomicLong now = new AtomicLong();
            FlightConnectorFactory caching = new FlightConnectorFactory(
                GrpcDataSourcePlugin.DEFAULT_CALL_TIMEOUT,
                new SatelliteDeclarationCache(TimeValue.timeValueMinutes(1), now::get)
            );
            String location = "flight://127.0.0.1:" + satellite.port() + "/t";
            satellite.capabilities = declaration("abc", "[\"scan\",\"limit\"]");
            satellite.schemaDeclarationId = "abc";
            caching.resolveMetadata(location, Map.of());
            now.addAndGet(TimeValue.timeValueMinutes(59).nanos());
            assertEquals(List.of("scan", "limit"), planOf(caching.resolveMetadata(location, Map.of())));
            assertEquals(1, satellite.capabilityCalls.get());
            now.addAndGet(TimeValue.timeValueMinutes(1).nanos());
            caching.resolveMetadata(location, Map.of());
            assertEquals("the id is trusted for a bounded time", 2, satellite.capabilityCalls.get());
        }
    }

    /** A satellite that gained a capability is used by the next query: no wait for a time to live. */
    public void testAChangedIdMakesTheNextResolutionUseTheNewDeclaration() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            FlightConnectorFactory caching = new FlightConnectorFactory(
                GrpcDataSourcePlugin.DEFAULT_CALL_TIMEOUT,
                TimeValue.timeValueHours(1)
            );
            String location = "flight://127.0.0.1:" + satellite.port() + "/t";
            satellite.capabilities = declaration("abc", "[\"scan\",\"limit\"]");
            satellite.schemaDeclarationId = "abc";
            assertEquals(List.of("scan", "limit"), planOf(caching.resolveMetadata(location, Map.of())));

            satellite.capabilities = declaration("def", "[\"scan\",\"filter\",\"limit\"]");
            satellite.schemaDeclarationId = "def";
            assertEquals(List.of("scan", "filter", "limit"), planOf(caching.resolveMetadata(location, Map.of())));
            assertEquals(2, satellite.capabilityCalls.get());

            caching.resolveMetadata(location, Map.of());
            assertEquals("and the new declaration is kept", 2, satellite.capabilityCalls.get());
        }
    }

    /** The satellite changed between the schema call and the capability call: the copy is kept under the id the answer carries. */
    public void testTheCopyIsKeptUnderTheIdOfTheAnswerNotTheOneOfTheSchema() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            FlightConnectorFactory caching = new FlightConnectorFactory(
                GrpcDataSourcePlugin.DEFAULT_CALL_TIMEOUT,
                TimeValue.timeValueHours(1)
            );
            String location = "flight://127.0.0.1:" + satellite.port() + "/t";
            satellite.capabilities = declaration("new", "[\"scan\"]");
            satellite.schemaDeclarationId = "old";
            caching.resolveMetadata(location, Map.of());
            satellite.schemaDeclarationId = "new";
            caching.resolveMetadata(location, Map.of());
            assertEquals(1, satellite.capabilityCalls.get());
        }
    }

    /** A satellite that gives no id in its schema is kept for the time to live, whatever its declaration says. */
    public void testWithoutAnIdInTheSchemaTheTimeToLiveDecides() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            AtomicLong now = new AtomicLong();
            FlightConnectorFactory caching = new FlightConnectorFactory(
                GrpcDataSourcePlugin.DEFAULT_CALL_TIMEOUT,
                new SatelliteDeclarationCache(TimeValue.timeValueMinutes(1), now::get)
            );
            String location = "flight://127.0.0.1:" + satellite.port() + "/t";
            satellite.capabilities = declaration("abc", "[\"scan\"]");
            satellite.schemaDeclarationId = null;
            caching.resolveMetadata(location, Map.of());
            now.addAndGet(TimeValue.timeValueSeconds(59).nanos());
            caching.resolveMetadata(location, Map.of());
            assertEquals(1, satellite.capabilityCalls.get());
            now.addAndGet(TimeValue.timeValueSeconds(1).nanos());
            caching.resolveMetadata(location, Map.of());
            assertEquals(2, satellite.capabilityCalls.get());
        }
    }

    /**
     * A satellite that gives an id in its schema but none in its declaration cannot be checked: its declaration is read on every
     * resolution, which is slower but never stale.
     */
    public void testADeclarationWithoutTheIdTheSchemaGivesIsReadEveryTime() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            FlightConnectorFactory caching = new FlightConnectorFactory(
                GrpcDataSourcePlugin.DEFAULT_CALL_TIMEOUT,
                TimeValue.timeValueHours(1)
            );
            String location = "flight://127.0.0.1:" + satellite.port() + "/t";
            satellite.capabilities = declaration(null, "[\"scan\"]");
            satellite.schemaDeclarationId = "abc";
            caching.resolveMetadata(location, Map.of());
            caching.resolveMetadata(location, Map.of());
            assertEquals(2, satellite.capabilityCalls.get());
        }
    }

    /** An id that the protocol does not allow is no id: it must not make a copy last. */
    public void testAnIdThatIsNotValidIsIgnored() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            AtomicLong now = new AtomicLong();
            FlightConnectorFactory caching = new FlightConnectorFactory(
                GrpcDataSourcePlugin.DEFAULT_CALL_TIMEOUT,
                new SatelliteDeclarationCache(TimeValue.timeValueMinutes(1), now::get)
            );
            String location = "flight://127.0.0.1:" + satellite.port() + "/t";
            satellite.capabilities = declaration("abc", "[\"scan\"]");
            satellite.schemaDeclarationId = "has a space";
            caching.resolveMetadata(location, Map.of());
            now.addAndGet(TimeValue.timeValueMinutes(2).nanos());
            caching.resolveMetadata(location, Map.of());
            assertEquals(2, satellite.capabilityCalls.get());
        }
    }

    /** A kept copy must never hide an outage: the schema is asked first, at every resolution. */
    public void testAKeptDeclarationDoesNotHideASatelliteThatIsDown() throws Exception {
        FlightConnectorFactory caching = new FlightConnectorFactory(TimeValue.timeValueSeconds(5), TimeValue.timeValueHours(1));
        String location;
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            location = "flight://127.0.0.1:" + satellite.port() + "/t";
            satellite.capabilities = declaration("abc", "[\"scan\"]");
            satellite.schemaDeclarationId = "abc";
            caching.resolveMetadata(location, Map.of());
        }
        expectThrows(IllegalStateException.class, () -> caching.resolveMetadata(location, Map.of()));
    }

    /** The id changed but the capability action fails: the query fails, it does not plan from the old copy. */
    public void testAChangedIdWhoseDeclarationCannotBeReadFailsTheResolution() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            FlightConnectorFactory caching = new FlightConnectorFactory(
                GrpcDataSourcePlugin.DEFAULT_CALL_TIMEOUT,
                TimeValue.timeValueHours(1)
            );
            String location = "flight://127.0.0.1:" + satellite.port() + "/t";
            satellite.capabilities = declaration("abc", "[\"scan\"]");
            satellite.schemaDeclarationId = "abc";
            caching.resolveMetadata(location, Map.of());
            satellite.schemaDeclarationId = "def";
            satellite.failCapabilitiesWith = CallStatus.INTERNAL.withDescription("boom");
            expectThrows(RuntimeException.class, () -> caching.resolveMetadata(location, Map.of()));
        }
    }

    /** A plain Flight server has no capability action: that answer is kept for the time to live and the dataset still resolves. */
    public void testAPlainFlightServerIsResolvedThroughACachingFactory() throws IOException {
        try (EmployeeFlightServer server = new EmployeeFlightServer(0)) {
            FlightConnectorFactory caching = new FlightConnectorFactory(
                GrpcDataSourcePlugin.DEFAULT_CALL_TIMEOUT,
                TimeValue.timeValueMinutes(1)
            );
            String location = "flight://localhost:" + server.port() + "/employees";
            for (int i = 0; i < 2; i++) {
                SourceMetadata metadata = caching.resolveMetadata(location, Map.of());
                assertFalse(metadata.sourceMetadata().containsKey("_satellite"));
                assertEquals(6, metadata.schema().size());
            }
        }
    }

    /** What depends on the table as it is now is read at every resolution, kept declaration or not. */
    public void testTheRecordCountIsNotKeptWithTheDeclaration() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            FlightConnectorFactory caching = new FlightConnectorFactory(
                GrpcDataSourcePlugin.DEFAULT_CALL_TIMEOUT,
                TimeValue.timeValueHours(1)
            );
            String location = "flight://127.0.0.1:" + satellite.port() + "/t";
            satellite.capabilities = "{\"specVersions\":[0,1],\"fragmentFormats\":[\"json\"],\"plan\":[\"scan\",\"aggregate\"],"
                + "\"functions\":[],\"aggregates\":[\"sum\"],\"types\":[\"integer\"],\"declarationId\":\"abc\"}";
            satellite.schemaDeclarationId = "abc";
            satellite.recordCount = 10;
            assertEquals(10L, totalRecordsOf(caching.resolveMetadata(location, Map.of())));
            satellite.recordCount = 20;
            assertEquals(20L, totalRecordsOf(caching.resolveMetadata(location, Map.of())));
            assertEquals("the declaration itself was read once", 1, satellite.capabilityCalls.get());
        }
    }

    private static Object totalRecordsOf(SourceMetadata metadata) {
        return ((Map<?, ?>) metadata.sourceMetadata().get("_satellite")).get("totalRecords");
    }

    public void testTheLongestIdTheProtocolAllowsIsKept() throws Exception {
        try (BlockingSatelliteServer satellite = new BlockingSatelliteServer()) {
            FlightConnectorFactory caching = new FlightConnectorFactory(
                GrpcDataSourcePlugin.DEFAULT_CALL_TIMEOUT,
                TimeValue.timeValueHours(1)
            );
            String location = "flight://127.0.0.1:" + satellite.port() + "/t";
            String id = "x".repeat(128);
            satellite.capabilities = declaration(id, "[\"scan\"]");
            satellite.schemaDeclarationId = id;
            caching.resolveMetadata(location, Map.of());
            caching.resolveMetadata(location, Map.of());
            assertEquals(1, satellite.capabilityCalls.get());
        }
    }

    public void testResolveMetadataStoresEndpointInConfig() throws IOException {
        try (EmployeeFlightServer server = new EmployeeFlightServer(0)) {
            String location = "flight://localhost:" + server.port() + "/employees";
            SourceMetadata metadata = factory.resolveMetadata(location, Map.of());

            Map<String, Object> config = metadata.config();
            assertNotNull(config);
            assertEquals("flight://localhost:" + server.port(), config.get("endpoint"));
        }
    }
}
