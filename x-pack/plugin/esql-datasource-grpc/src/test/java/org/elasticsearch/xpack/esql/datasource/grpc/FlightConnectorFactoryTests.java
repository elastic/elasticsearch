/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
