/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.AbstractExternalDataSourceIT;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.datasource.jdbc.qa.H2TcpTestServer;
import org.elasticsearch.xpack.esql.datasource.jdbc.qa.HikariPoolTestThreadLeakFilter;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.contains;

/**
 * End-to-end regression IT for the schema-cache-bypass invariant ({@link JdbcStorageProvider#supportsStableMetadata()} {@code == false}).
 * <p>
 * Two datasets are registered against the <b>same clean JDBC URL</b> (one in-process H2 database reached over an
 * in-process H2 TCP server, i.e. one {@code jdbc:h2:tcp://localhost:<port>/mem:...} resource) and differ <em>only</em>
 * in their {@code table} WITH-setting: one points at
 * {@code orders}, the other at {@code products}. The two tables have deliberately disjoint schemas, so if the
 * planning-time external-source schema cache keyed its entries by the resource URL alone — ignoring the JDBC
 * {@code table} option — the second dataset resolved would collide on the first table's cached schema and
 * {@code FROM} it would report the wrong columns.
 * <p>
 * Because {@link JdbcStorageProvider} declares {@code supportsStableMetadata()==false}, the resolver treats every JDBC
 * source as non-cacheable and re-resolves each dataset against its own {@code table}. This test proves that
 * end-to-end: it queries {@code orders}, then {@code products}, then {@code orders} again, asserting each returns its
 * own distinct columns and values every time. A regression that re-enabled stable-metadata caching for JDBC (or that
 * failed to fold the {@code table} into the cache identity) would make the second/third query return the first
 * table's schema and fail here.
 * <p>
 * Single-node, SUITE scope and in-process H2 (no Docker) for the same reasons as {@code JdbcDatasetIT}.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
@ThreadLeakFilters(filters = { HikariPoolTestThreadLeakFilter.class })
public class JdbcSchemaCacheBypassIT extends AbstractExternalDataSourceIT {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);
    private static final String DATASOURCE_NAME = "jdbc_ds";
    private static final String ORDERS_DATASET = "orders_ds";
    private static final String PRODUCTS_DATASET = "products_ds";

    private static H2TcpTestServer h2Server;
    // ONE clean JDBC URL shared by both datasets; they differ only by the `table` WITH-setting.
    private static String jdbcUrl;
    private static Connection keepAliveConnection;

    /**
     * The H2 TCP server listens on {@code localhost}, so permit loopback and the {@code jdbc:h2:tcp://} subprotocol
     * (excluded from the production SSRF default allowlist).
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("esql.jdbc.ssrf.allow_loopback", true)
            .putList("esql.jdbc.ssrf.allowed_subprotocols", "jdbc:h2:tcp://")
            .build();
    }

    /**
     * The JDBC connector is the only format plugin this suite adds; {@link AbstractExternalDataSourceIT} installs the
     * pass-through {@code TestDataSourcePlugin} (type {@code test}), so the connector is chosen by the {@code jdbc:}
     * resource scheme, not the data source type.
     */
    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(JdbcDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    @BeforeClass
    public static void setUpH2() throws Exception {
        Class.forName("org.h2.Driver");
        h2Server = H2TcpTestServer.start();
        jdbcUrl = h2Server.urlFor("esql_jdbc_cache_bypass_it");
        keepAliveConnection = DriverManager.getConnection(jdbcUrl);
        try (Statement stmt = keepAliveConnection.createStatement()) {
            // Two tables in ONE database (one URL) with disjoint schemas: distinct column names AND types.
            stmt.execute("""
                CREATE TABLE "orders" (
                    "order_id" INTEGER,
                    "customer" VARCHAR(64)
                );
                """);
            stmt.execute("""
                INSERT INTO "orders" ("order_id", "customer") VALUES
                    (1, 'Alice'),
                    (2, 'Bob');
                """);
            stmt.execute("""
                CREATE TABLE "products" (
                    "sku" VARCHAR(32),
                    "price" DOUBLE PRECISION,
                    "qty" INTEGER
                );
                """);
            stmt.execute("""
                INSERT INTO "products" ("sku", "price", "qty") VALUES
                    ('A-1', 9.99, 100);
                """);
        }
    }

    @AfterClass
    public static void tearDownH2() throws Exception {
        try {
            if (keepAliveConnection != null) {
                keepAliveConnection.close();
                keepAliveConnection = null;
            }
        } finally {
            if (h2Server != null) {
                h2Server.close();
                h2Server = null;
            }
        }
    }

    @After
    public void cleanupJdbcRegistry() throws Exception {
        for (String ds : List.of(ORDERS_DATASET, PRODUCTS_DATASET)) {
            try {
                client().execute(DeleteDatasetAction.INSTANCE, new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { ds }))
                    .get(30, TimeUnit.SECONDS);
            } catch (ResourceNotFoundException ignored) {
                // already gone
            } catch (Exception e) {
                logger.warn("dataset cleanup [{}] failed", ds, e);
            }
        }
        try {
            client().execute(
                DeleteDataSourceAction.INSTANCE,
                new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { DATASOURCE_NAME })
            ).get(30, TimeUnit.SECONDS);
        } catch (ResourceNotFoundException ignored) {
            // already gone
        } catch (Exception e) {
            logger.warn("data source cleanup [{}] failed", DATASOURCE_NAME, e);
        }
    }

    /**
     * Registers both datasets on the SAME URL (differing only by {@code table}), then interleaves queries
     * ({@code orders}, {@code products}, {@code orders} again) asserting each resolves to its OWN schema and values.
     * Proves the JDBC schema-cache bypass: with {@code supportsStableMetadata()==false} the resolver never serves
     * the first table's cached schema for the second table on the identical URL.
     */
    public void testSameUrlDifferentTableResolveDistinctSchemas() throws Exception {
        registerDataSource();
        registerDataset(ORDERS_DATASET, "orders");
        registerDataset(PRODUCTS_DATASET, "products");

        assertOrdersSchema();
        assertProductsSchema();
        // Query orders AGAIN after products: a stale-cache regression keyed on URL alone would now return products'
        // schema for orders (or vice-versa); the bypass keeps orders resolving to its own columns.
        assertOrdersSchema();
    }

    private void assertOrdersSchema() {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM " + ORDERS_DATASET + " | SORT order_id"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertColumnNamesAndTypes(columns, List.of("order_id", "customer"), List.of("integer", "keyword"));
            List<List<Object>> rows = org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList(response);
            assertEquals("orders row count", 2, rows.size());
            assertThat(rows.get(0), contains(1, "Alice"));
            assertThat(rows.get(1), contains(2, "Bob"));
        }
    }

    private void assertProductsSchema() {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM " + PRODUCTS_DATASET + " | SORT sku"), TIMEOUT)) {
            List<? extends ColumnInfo> columns = response.columns();
            assertColumnNamesAndTypes(columns, List.of("sku", "price", "qty"), List.of("keyword", "double", "integer"));
            List<List<Object>> rows = org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList(response);
            assertEquals("products row count", 1, rows.size());
            assertThat(rows.get(0), contains("A-1", 9.99, 100));
        }
    }

    private static void assertColumnNamesAndTypes(List<? extends ColumnInfo> actual, List<String> names, List<String> types) {
        assertEquals("column count; got " + actual, names.size(), actual.size());
        for (int i = 0; i < names.size(); i++) {
            assertEquals("column[" + i + "] name", names.get(i), actual.get(i).name());
            assertEquals("column[" + i + "] outputType", types.get(i), actual.get(i).outputType());
        }
    }

    private void registerDataSource() throws Exception {
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, DATASOURCE_NAME, "test", null, new HashMap<>(Map.of()))
            )
        );
    }

    private void registerDataset(String datasetName, String table) throws Exception {
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    datasetName,
                    DATASOURCE_NAME,
                    jdbcUrl,
                    null,
                    new HashMap<>(Map.of("table", table))
                )
            )
        );
    }
}
