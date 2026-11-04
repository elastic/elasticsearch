/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Table parameters: the dataset settings a satellite declares as parameters of the table reach it in a header on every call about
 * the table (not only in a fragment), and the {@code credentials} of the data source reach it in another, over TLS only. The
 * satellite is a test server that records the headers of every call.
 */
public class FlightTableParametersTests extends ESTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);
    private static final List<Attribute> ATTRIBUTES = List.of(referenceAttribute("id", DataType.INTEGER));

    private static String declaration(String id, String tableParameters) {
        return "{\"specVersions\":[0],\"fragmentFormats\":[\"json\"],\"plan\":[\"scan\"],\"functions\":[],\"types\":[\"integer\"],"
            + "\"tableParameters\":"
            + tableParameters
            + ",\"declarationId\":\""
            + id
            + "\"}";
    }

    private static Map<String, Object> decode(String header) throws IOException {
        byte[] json = Base64.getUrlDecoder().decode(header);
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            return parser.map();
        }
    }

    private static FlightConnectorFactory factory(TimeValue ttl) {
        return new FlightConnectorFactory(TIMEOUT, new SatelliteDeclarationCache(ttl), FlightGuard.DEFAULT);
    }

    public void testTheDatasetSettingsGoInAHeaderOnEveryCallOfAResolution() throws Exception {
        try (BlockingSatelliteServer server = new BlockingSatelliteServer()) {
            server.capabilities = declaration("d1", "[\"location\",\"format\"]");
            server.schemaDeclarationId = "d1";
            String location = "grpc://127.0.0.1:" + server.port() + "/t";
            factory(TimeValue.ZERO).resolveMetadata(location, Map.of("location", "s3://bucket/prefix/", "format", "parquet"));
            assertFalse(server.headerLog.isEmpty());
            for (BlockingSatelliteServer.CallHeaders call : server.headerLog) {
                assertNotNull("every call carries the settings: " + call.method(), call.datasetSettings());
                assertThat(decode(call.datasetSettings()), equalTo(Map.of("format", "parquet", "location", "s3://bucket/prefix/")));
                assertNull(call.credentials());
            }
            List<String> methods = server.headerLog.stream().map(BlockingSatelliteServer.CallHeaders::method).toList();
            assertTrue(methods.toString(), methods.contains("GET_SCHEMA") && methods.contains("DO_ACTION"));
        }
    }

    public void testASettingTheSatelliteDidNotDeclareFailsTheResolution() throws Exception {
        try (BlockingSatelliteServer server = new BlockingSatelliteServer()) {
            server.capabilities = declaration("d1", "[\"format\"]");
            String location = "grpc://127.0.0.1:" + server.port() + "/t";
            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> factory(TimeValue.ZERO).resolveMetadata(location, Map.of("location", "s3://bucket/prefix/"))
            );
            assertThat(e.getMessage(), containsString("does not declare the dataset setting [location]"));
        }
    }

    public void testAPlainFlightServerCannotTakeTableParameters() throws Exception {
        try (EmployeeFlightServer server = new EmployeeFlightServer(0)) {
            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> factory(TimeValue.ZERO).resolveMetadata("grpc://127.0.0.1:" + server.port() + "/employees", Map.of("location", "x"))
            );
            assertThat(e.getMessage(), containsString("does not implement the satellite protocol"));
        }
    }

    public void testANameThatIsBothAnOptionAndATableParameterIsRefused() {
        Map<String, Object> caps = new LinkedHashMap<>();
        caps.put("specVersions", List.of(0));
        caps.put("fragmentFormats", List.of("json"));
        caps.put("plan", List.of("scan"));
        caps.put("functions", List.of());
        caps.put("types", List.of("integer"));
        caps.put("options", List.of("units"));
        caps.put("tableParameters", List.of("units"));
        Exception e = expectThrows(IllegalStateException.class, () -> FlightConnectorFactory.validateDeclaration(caps));
        assertThat(e.getMessage(), containsString("both a source option and a table parameter"));
        caps.put("tableParameters", List.of("endpoint"));
        e = expectThrows(IllegalStateException.class, () -> FlightConnectorFactory.validateDeclaration(caps));
        assertThat(e.getMessage(), containsString("a name ES uses for itself"));
    }

    public void testAKeptDeclarationIsNotSharedBetweenDatasetsWithOtherSettings() throws Exception {
        try (BlockingSatelliteServer server = new BlockingSatelliteServer()) {
            server.capabilities = declaration("d1", "[\"location\"]");
            server.schemaDeclarationId = "d1";
            FlightConnectorFactory caching = factory(TimeValue.timeValueMinutes(5));
            String resource = "grpc://127.0.0.1:" + server.port() + "/t";
            caching.resolveMetadata(resource, Map.of("location", "a"));
            caching.resolveMetadata(resource, Map.of("location", "a"));
            assertEquals("the same settings share the copy", 1, server.capabilityCalls.get());
            caching.resolveMetadata(resource, Map.of("location", "b"));
            assertEquals("other settings read the declaration again", 2, server.capabilityCalls.get());
        }
    }

    public void testCredentialsGoOverTlsOnlyAndNotInTheSettingsHeader() throws Exception {
        try (BlockingSatelliteServer server = new BlockingSatelliteServer(true)) {
            server.capabilities = declaration("d1", "[\"location\"]");
            server.schemaDeclarationId = "d1";
            Map<String, Object> config = Map.of(
                "location",
                "s3://bucket/",
                "_datasource",
                Map.of("ca_certificate", BlockingSatelliteServer.caCertificate(), "credentials", "{\"access_key\":\"AKIAXYZ\"}")
            );
            factory(TimeValue.ZERO).resolveMetadata("grpc+tls://127.0.0.1:" + server.port() + "/t", config);
            for (BlockingSatelliteServer.CallHeaders call : server.headerLog) {
                assertThat(decode(call.credentials()), equalTo(Map.of("access_key", "AKIAXYZ")));
                assertThat(
                    new String(Base64.getUrlDecoder().decode(call.datasetSettings()), StandardCharsets.UTF_8),
                    not(containsString("AKIAXYZ"))
                );
            }
        }
        // a plain connection never carries them, whatever the config says
        FlightClients.Transport plain = FlightClients.transport("grpc://h:1", Map.of("credentials", "{\"a\":\"b\"}"));
        assertNull(plain.credentials());
        FlightClients.Transport tls = FlightClients.transport("grpc+tls://h:1", Map.of("credentials", "{\"a\":\"secret-value\"}"));
        assertThat(tls.toString(), not(containsString("secret-value")));
        assertThat(tls.cacheScope(), not(containsString("secret-value")));
    }

    /** The read of a dataset that has table parameters but no fragment is allowed, and its calls carry the header too. */
    public void testAPlainReadWithTableParametersSendsTheHeaderAndIsNotRefused() throws Exception {
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(NoopCircuitBreaker.INSTANCE).build();
        try (BlockingSatelliteServer server = new BlockingSatelliteServer()) {
            String endpoint = "grpc://127.0.0.1:" + server.port();
            Map<String, Object> config = Map.of("location", "s3://bucket/", "_table_parameters", List.of("location"));
            try (
                FlightConnector connector = new FlightConnector(
                    endpoint,
                    FlightClients.transport(endpoint, config),
                    TIMEOUT,
                    new SatelliteDeclarationCache(TimeValue.ZERO),
                    FlightGuard.DEFAULT
                );
                ResultCursor cursor = connector.execute(
                    new QueryRequest("t", List.of("id"), ATTRIBUTES, config, 1000, 0, blockFactory, null),
                    Split.SINGLE
                )
            ) {
                assertTrue(cursor.hasNext());
                cursor.next().releaseBlocks();
            }
            List<String> methods = server.headerLog.stream().map(BlockingSatelliteServer.CallHeaders::method).toList();
            assertTrue(methods.toString(), methods.contains("GET_FLIGHT_INFO") && methods.contains("DO_GET"));
            for (BlockingSatelliteServer.CallHeaders call : server.headerLog) {
                assertThat(decode(call.datasetSettings()), equalTo(Map.of("location", "s3://bucket/")));
            }
        }
        assertEquals(0L, blockFactory.arrowAllocator().getAllocatedMemory());
    }

    public void testTheValidatorChecksTheCredentialsAndTheSizeOfTheSettings() throws Exception {
        FlightDataSourceValidator validator = new FlightDataSourceValidator();
        Map<String, DataSourceSetting> ok = validator.validateDatasource(
            Map.of("credentials", "{\"access_key\":\"a\",\"secret_key\":\"b\"}")
        );
        assertTrue(ok.get("credentials").secret());
        for (String bad : new String[] { "not json", "[1,2]", "\"text\"", "x".repeat(5000) }) {
            expectThrows(ValidationException.class, () -> validator.validateDatasource(Map.of("credentials", bad)));
        }
        Map<String, DataSourceSetting> withCredentials = Map.of("credentials", new DataSourceSetting("{\"a\":\"b\"}", true));
        Exception e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(withCredentials, "grpc://127.0.0.1:1/t", Map.of())
        );
        assertThat(e.getMessage(), containsString("flight+tls or grpc+tls"));
        validator.validateDataset(withCredentials, "grpc+tls://127.0.0.1:1/t", Map.of("location", "s3://bucket/prefix/"));
        e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "grpc://127.0.0.1:1/t", Map.of("location", "x".repeat(5000)))
        );
        assertThat(e.getMessage(), containsString("too large"));
    }

    public void testAnAbsentSecretAndTheTextDashHaveDifferentScopes() {
        String none = FlightClients.transport("grpc+tls://h:1", Map.of("credentials", "{\"a\":\"b\"}")).cacheScope();
        String dash = FlightClients.transport("grpc+tls://h:1", Map.of("credentials", "{\"a\":\"b\"}", "token", "-")).cacheScope();
        assertNotEquals(none, dash);
    }

    public void testATokenAndCredentialsThatDoNotFitTheMetadataAreRefused() {
        FlightDataSourceValidator validator = new FlightDataSourceValidator();
        expectThrows(
            ValidationException.class,
            () -> validator.validateDatasource(Map.of("token", "t".repeat(FlightClients.MAX_TOKEN_CHARS + 1)))
        );
        validator.validateDatasource(Map.of("token", "t".repeat(FlightClients.MAX_TOKEN_CHARS)));
        expectThrows(ValidationException.class, () -> validator.validateDatasource(Map.of("credentials", "{\"a\":\"b\"} trailing")));
        // the three headers of a call at their limits stay under the 8 KiB of metadata gRPC allows
        assertTrue(FlightClients.MAX_TOKEN_CHARS + 2 * FlightClients.MAX_HEADER_VALUE_CHARS + 512 < 8192);
    }

    /** A host a satellite names in a flight info does not receive the token or the credentials of the endpoint the dataset names. */
    public void testASplitOnAnotherHostIsNotSentTheCredentials() throws Exception {
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(NoopCircuitBreaker.INSTANCE).build();
        try (BlockingSatelliteServer server = new BlockingSatelliteServer()) {
            String endpoint = "grpc://127.0.0.1:" + server.port();
            try (
                FlightConnector connector = new FlightConnector(
                    endpoint,
                    new FlightClients.Transport(false, null, null, "s3cret"),
                    TIMEOUT,
                    new SatelliteDeclarationCache(TimeValue.ZERO),
                    FlightGuard.DEFAULT
                );
                ResultCursor cursor = connector.execute(
                    new QueryRequest("t", List.of("id"), ATTRIBUTES, Map.of(), 1000, 0, blockFactory, null),
                    new FlightSplit("t".getBytes(StandardCharsets.UTF_8), "grpc://127.0.0.2:" + server.port(), 3)
                )
            ) {
                Exception e = expectThrows(IllegalArgumentException.class, cursor::hasNext);
                assertThat(e.getMessage(), containsString("not sent them"));
            }
            assertEquals("the satellite on the other host was never called", 0, server.headerLog.size());
        }
    }

    /** A satellite that refuses a location says why, and the user sees it (marked as the satellite's), not "failed to resolve". */
    public void testARefusalOfTheSatelliteAtResolutionIsShownToTheUser() throws Exception {
        try (BlockingSatelliteServer server = new BlockingSatelliteServer()) {
            server.failCapabilitiesWith = org.apache.arrow.flight.CallStatus.INVALID_ARGUMENT.withDescription("location not allowed");
            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> factory(TimeValue.ZERO).resolveMetadata("grpc://127.0.0.1:" + server.port() + "/t", Map.of("location", "/etc/"))
            );
            assertThat(e.getMessage(), containsString("the satellite rejected the request [INVALID_ARGUMENT]"));
            assertThat(e.getMessage(), containsString("the satellite says: location not allowed"));
        }
    }
}
