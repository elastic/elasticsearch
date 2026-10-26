/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import com.sun.net.httpserver.HttpServer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.Split;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/** Exercises the HTTP protocol, schema discovery, typed streaming, and failure paths with a local server. */
@SuppressForbidden(reason = "Local HTTP fixture for the ClickHouse protocol")
public class ClickHouseConnectorTests extends ESTestCase {
    private HttpServer server;
    private Map<String, Object> config;
    private final AtomicReference<String> response = new AtomicReference<>();
    private final AtomicReference<String> sql = new AtomicReference<>();
    private final AtomicReference<String> authorization = new AtomicReference<>();
    private final AtomicReference<String> queryParameters = new AtomicReference<>();
    private volatile int status;

    @Before
    public void startServer() throws IOException {
        status = 200;
        server = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 0);
        server.createContext("/", exchange -> {
            sql.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            queryParameters.set(exchange.getRequestURI().getQuery());
            byte[] body = response.get().getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, body.length);
            try (var output = exchange.getResponseBody()) {
                output.write(body);
            } finally {
                exchange.close();
            }
        });
        server.start();
        config = Map.of("endpoint", "http://127.0.0.1:" + server.getAddress().getPort(), "username", "reader", "password", "test-password");
    }

    @After
    public void stopServer() {
        server.stop(0);
    }

    public void testSchemaAndAuthentication() {
        response.set("[\"message\",\"latency_ms\",\"ts\"]\n[\"LowCardinality(Nullable(String))\",\"Int64\",\"DateTime64(6, 'UTC')\"]\n");
        var metadata = new ClickHouseConnectorFactory().resolveMetadata("clickhouse://default/app_logs", config);
        assertEquals(
            List.of(DataType.KEYWORD, DataType.LONG, DataType.DATETIME),
            metadata.schema().stream().map(Attribute::dataType).toList()
        );
        assertEquals("SELECT * FROM `default`.`app_logs` LIMIT 0 FORMAT JSONCompactEachRowWithNamesAndTypes", sql.get());
        assertEquals(
            "Basic " + Base64.getEncoder().encodeToString("reader:test-password".getBytes(StandardCharsets.UTF_8)),
            authorization.get()
        );
        assertTrue(queryParameters.get().contains("readonly=2"));
        assertFalse(queryParameters.get().contains("test-password"));
        assertTrue(metadata.config().isEmpty());
    }

    public void testBatchedRowsPreserveNullsAndUInt64() throws IOException {
        response.set("""
            ["message","id"]
            ["Nullable(String)","UInt64"]
            ["hello","18446744073709551615"]
            [null,"0"]
            ["last","42"]
            """);
        List<Attribute> attributes = List.of(attribute("message", DataType.KEYWORD), attribute("id", DataType.UNSIGNED_LONG));
        try (
            var connector = new ClickHouseConnectorFactory().open(config);
            var cursor = connector.execute(request(attributes, 2), Split.SINGLE)
        ) {
            var first = cursor.next();
            try {
                assertEquals(2, first.getPositionCount());
                BytesRefBlock messages = first.getBlock(0);
                assertEquals("hello", messages.getBytesRef(0, new BytesRef()).utf8ToString());
                assertTrue(messages.isNull(1));
                LongBlock ids = first.getBlock(1);
                assertEquals("18446744073709551615", NumericUtils.unsignedLongAsBigInteger(ids.getLong(0)).toString());
                assertEquals("0", NumericUtils.unsignedLongAsBigInteger(ids.getLong(1)).toString());
            } finally {
                first.releaseBlocks();
            }
            var second = cursor.next();
            try {
                assertEquals(1, second.getPositionCount());
            } finally {
                second.releaseBlocks();
            }
            assertFalse(cursor.hasNext());
        }
    }

    public void testSpecialFloatValues() throws IOException {
        response.set("[\"value\"]\n[\"Float64\"]\n[\"nan\"]\n[\"inf\"]\n[\"-inf\"]\n[1.25]\n");
        try (
            var connector = new ClickHouseConnectorFactory().open(config);
            var cursor = connector.execute(request(List.of(attribute("value", DataType.DOUBLE)), 10), Split.SINGLE)
        ) {
            var page = cursor.next();
            try {
                DoubleBlock values = page.getBlock(0);
                assertTrue(Double.isNaN(values.getDouble(0)));
                assertEquals(Double.POSITIVE_INFINITY, values.getDouble(1), 0);
                assertEquals(Double.NEGATIVE_INFINITY, values.getDouble(2), 0);
                assertEquals(1.25, values.getDouble(3), 0);
            } finally {
                page.releaseBlocks();
            }
        }
    }

    public void testEmptyProjectionPreservesRowCount() throws IOException {
        response.set("[\"1\"]\n[\"UInt8\"]\n[1]\n[1]\n[1]\n");
        try (
            var connector = new ClickHouseConnectorFactory().open(config);
            var cursor = connector.execute(request(List.of(), 10), Split.SINGLE)
        ) {
            var page = cursor.next();
            try {
                assertEquals(3, page.getPositionCount());
                assertEquals(0, page.getBlockCount());
                assertFalse(cursor.hasNext());
            } finally {
                page.releaseBlocks();
            }
        }
        assertTrue(sql.get().startsWith("SELECT 1 FROM `default`.`app_logs`"));
    }

    public void testLateHttp200ErrorFailsQuery() throws IOException {
        response.set("[\"id\"]\n[\"Int64\"]\n[1]\nCode: 241. Memory limit exceeded\n");
        try (
            var connector = new ClickHouseConnectorFactory().open(config);
            var cursor = connector.execute(request(List.of(attribute("id", DataType.LONG)), 10), Split.SINGLE)
        ) {
            var error = expectThrows(UncheckedIOException.class, cursor::next);
            assertTrue(error.getCause().getMessage().contains("error or an invalid result"));
        }
    }

    public void testHttpFailureDoesNotExposeServerBody() {
        status = 403;
        response.set("test-password should never reach the Elasticsearch error");
        var error = expectThrows(IOException.class, () -> new ClickHouseConnectorFactory().testConnection(config));
        assertEquals("ClickHouse query failed with HTTP status [403]", error.getMessage());
    }

    public void testProbeExecutesAQuery() throws IOException {
        response.set("[\"1\"]\n[\"UInt8\"]\n[1]\n");
        new ClickHouseConnectorFactory().testConnection(config);
        assertEquals("SELECT 1 FORMAT JSONCompactEachRowWithNamesAndTypes", sql.get());
    }

    public void testEmptyTable() throws IOException {
        response.set("[\"id\"]\n[\"Int64\"]\n");
        try (
            var connector = new ClickHouseConnectorFactory().open(config);
            var cursor = connector.execute(request(List.of(attribute("id", DataType.LONG)), 10), Split.SINGLE)
        ) {
            assertFalse(cursor.hasNext());
        }
    }

    public void testTemporalProjectionAndEscapedColumn() {
        String query = ClickHouseConnector.query(
            request(List.of(attribute("ts", DataType.DATETIME), attribute("a`b", DataType.KEYWORD)), 10)
        );
        assertEquals(
            "SELECT toUnixTimestamp64Milli(toDateTime64(`ts`, 3, 'UTC')) AS `ts`, toString(`a\\`b`) AS `a\\`b` FROM `default`.`app_logs`",
            query
        );
    }

    public void testStoredPasswordCanBeOmittedOnUpdate() {
        var validator = new ClickHouseDataSourceValidator();
        assertTrue(validator.validateDatasource(config).get("password").secret());
        Map<String, Object> update = new HashMap<>(config);
        update.remove("password");
        expectThrows(ValidationException.class, () -> validator.validateDatasource(update));
        assertFalse(validator.validateDatasource(update, Set.of("password")).containsKey("password"));
    }

    public void testFrameworkQuerySettingsAreAccepted() {
        Map<String, Object> queryConfig = new HashMap<>(config);
        queryConfig.put("schema_resolution", "union_by_name");
        queryConfig.put("_definition_version", 1);
        var factory = new ClickHouseConnectorFactory();
        factory.validateConfig("clickhouse://default/app_logs", queryConfig);
        queryConfig.put("sql", "SELECT secret FROM another_table");
        expectThrows(IllegalArgumentException.class, () -> factory.validateConfig("clickhouse://default/app_logs", queryConfig));
    }

    public void testDatasetCannotOverrideConnection() {
        var validator = new ClickHouseDataSourceValidator();
        expectThrows(
            IllegalArgumentException.class,
            () -> validator.validateDataset(Map.of(), "clickhouse://default/app_logs", Map.of("endpoint", "https://other.example"))
        );
        for (String resource : List.of(
            "https://example.com/table",
            "clickhouse://user:pass@default/table",
            "clickhouse://default/table?query=DROP",
            "clickhouse://default/table;DROP"
        )) {
            expectThrows(IllegalArgumentException.class, () -> ClickHouseTable.parse(resource));
        }
    }

    public void testEndpointRejectsCredentialsRedirectPathsAndRemoteHttp() {
        for (String endpoint : List.of(
            "http://example.com",
            "https://user:pass@example.com",
            "https://example.com/query",
            "https://example.com?password=secret"
        )) {
            var error = expectThrows(IllegalArgumentException.class, () -> ClickHouseConfiguration.endpoint(endpoint));
            assertFalse(error.getMessage().contains("secret"));
        }
    }

    public void testUnsupportedTypesFailExplicitly() {
        expectThrows(IllegalArgumentException.class, () -> ClickHouseTypes.dataType("Array(String)"));
        assertEquals(DataType.KEYWORD, ClickHouseTypes.dataType("Decimal(38, 9)"));
    }

    private static Attribute attribute(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }

    private static QueryRequest request(List<Attribute> attributes, int batchSize) {
        BlockFactory factory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(NoopCircuitBreaker.INSTANCE).build();
        return new QueryRequest(
            "clickhouse://default/app_logs",
            attributes.stream().map(Attribute::name).toList(),
            attributes,
            Map.of(),
            batchSize,
            factory
        );
    }
}
