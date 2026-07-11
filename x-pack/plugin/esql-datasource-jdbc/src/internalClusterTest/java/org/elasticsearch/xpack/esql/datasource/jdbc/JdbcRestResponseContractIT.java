/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
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
import static org.hamcrest.Matchers.equalTo;

/**
 * Pins the Kibana-facing REST response CONTRACT for a {@code FROM jdbc:...} query: the exact
 * {@code columns} ({@code [{name,type}]}) and {@code values} JSON that an external REST client (e.g. Kibana)
 * receives from the {@code POST /_query} endpoint when the source is the JDBC connector.
 * <p>
 * <b>Why this is a REST-format contract test on the {@code internalClusterTest} harness rather than a
 * separate-process {@code yamlRestTest}.</b> Two frictions make a real separate-process YAML/REST test
 * against a JDBC-backed dataset intractable within this iteration's additive, test-only, no-prod-change
 * constraints:
 * <ol>
 *   <li><b>No production {@code jdbc} data-source validator.</b> A separate-process node must register the
 *   data source over REST, and {@code DataSourceService.validatePutDataSource} rejects any type without a
 *   registered {@code DataSourceValidator} ("unknown data source type [jdbc]"). {@link JdbcDataSourcePlugin}
 *   registers connectors/storage-providers/schemes but <em>no</em> validator; the production file-based
 *   validators ({@code s3}/{@code gcs}/{@code azure}/{@code csv}) reject a {@code jdbc:} resource
 *   ({@code FileDataSourceValidator.validateResource}: "[resource] must use one of the supported URI schemes").
 *   The only pass-through validator, {@link AbstractExternalDataSourceIT.TestDataSourcePlugin}, is a test class in
 *   ESQL's internalClusterTest source set and cannot be installed on a real distribution node. Adding a production
 *   {@code JdbcDataSourceValidator} is an SPI change that is out of scope here.</li>
 *   <li><b>H2 driver on an isolated plugin classloader.</b> On a real distribution the
 *   {@code esql-datasource-jdbc} plugin has an isolated classloader that bundles no JDBC driver;
 *   {@link JdbcDataSourcePlugin} loads drivers from {@code plugins/esql-datasource-jdbc/drivers/} (empty on a
 *   fresh install) then falls back to its own classloader, which has no H2. The test-clusters framework has no
 *   clean hook to inject a jar into an installed plugin's {@code drivers/} subdirectory.</li>
 * </ol>
 * Because the REST {@code _query} endpoint serializes {@link EsqlQueryResponse} through exactly the same
 * {@link EsqlQueryResponse#toXContentChunked} path this test exercises, serializing a real response from an
 * end-to-end JDBC query and parsing back the {@code columns}/{@code values} pins the identical wire contract a
 * REST client observes — without the separate-process driver/validator wiring. The in-process H2 fixture
 * mirrors {@link JdbcDatasetIT}.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
@ThreadLeakFilters(filters = { HikariPoolTestThreadLeakFilter.class })
public class JdbcRestResponseContractIT extends AbstractExternalDataSourceIT {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);
    private static final String DATASOURCE_NAME = "jdbc_ds";
    private static final String DATASET_NAME = "employees";

    private static H2TcpTestServer h2Server;
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
     * pass-through {@code TestDataSourcePlugin} (type {@code test}) so the connector is chosen by the {@code jdbc:}
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
        jdbcUrl = h2Server.urlFor("esql_jdbc_contract_it");
        keepAliveConnection = DriverManager.getConnection(jdbcUrl);
        try (Statement stmt = keepAliveConnection.createStatement()) {
            stmt.execute(loadEmployeesTableSql());
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
        try {
            client().execute(DeleteDatasetAction.INSTANCE, new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { DATASET_NAME }))
                .get(30, TimeUnit.SECONDS);
        } catch (org.elasticsearch.ResourceNotFoundException ignored) {
            // already gone
        } catch (Exception e) {
            logger.warn("dataset cleanup [{}] failed", DATASET_NAME, e);
        }
        try {
            client().execute(
                DeleteDataSourceAction.INSTANCE,
                new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { DATASOURCE_NAME })
            ).get(30, TimeUnit.SECONDS);
        } catch (org.elasticsearch.ResourceNotFoundException ignored) {
            // already gone
        } catch (Exception e) {
            logger.warn("data source cleanup [{}] failed", DATASOURCE_NAME, e);
        }
    }

    /**
     * Runs a mixed-type projection ({@code integer} / {@code keyword} / temporal {@code date} / {@code integer})
     * and asserts the exact REST-wire {@code columns} and {@code values} an external client parses. The response is
     * serialized through the production {@link EsqlQueryResponse#toXContentChunked} path (the same one the
     * {@code _query} REST endpoint uses) and parsed back, so the assertion pins the true wire JSON.
     */
    public void testFromJdbcRestResponseContract() throws Exception {
        registerEmployeesDataset();

        try (
            EsqlQueryResponse response = run(
                syncEsqlQueryRequest("FROM employees | KEEP emp_no, first_name, hire_date, salary | SORT emp_no | LIMIT 3"),
                TIMEOUT
            )
        ) {
            // Contract pinned for the current (V9) REST API version: the V8 toXContentChunkedV8 path is identical for
            // the columns/values shape asserted here, so this contract holds for both API versions.
            // Exact REST bytes a Kibana client receives, produced by the same chunked-XContent path as POST /_query.
            String json = Strings.toString(ChunkedToXContent.wrapAsToXContent(response), false, false);
            Map<String, Object> parsed = XContentHelper.convertToMap(new BytesArray(json), false, XContentType.JSON).v2();

            // columns contract: name + type for a numeric / keyword / temporal / numeric mix.
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> columns = (List<Map<String, Object>>) parsed.get("columns");
            assertThat(
                columns,
                equalTo(
                    List.of(
                        Map.of("name", "emp_no", "type", "integer"),
                        Map.of("name", "first_name", "type", "keyword"),
                        Map.of("name", "hire_date", "type", "date"),
                        Map.of("name", "salary", "type", "integer")
                    )
                )
            );

            // values contract: exact rows (SORT emp_no | LIMIT 3), including the datetime string rendering.
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) parsed.get("values");
            assertThat(
                values,
                equalTo(
                    List.of(
                        List.of(10001, "Georgi", "1986-06-26T00:00:00.000Z", 57305),
                        List.of(10002, "Bezalel", "1985-11-21T00:00:00.000Z", 56371),
                        List.of(10003, "Parto", "1986-08-28T00:00:00.000Z", 61805)
                    )
                )
            );
        }
    }

    // -- Setup helpers ----------------------------------------------------------

    private void registerEmployeesDataset() throws Exception {
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, DATASOURCE_NAME, "test", null, new HashMap<>(Map.of()))
            )
        );
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    DATASET_NAME,
                    DATASOURCE_NAME,
                    jdbcUrl,
                    null,
                    new HashMap<>(Map.of("table", "employees"))
                )
            )
        );
    }

    private static String loadEmployeesTableSql() {
        // A tiny, fully deterministic fixture built from explicit timezone-naive TIMESTAMP literals rather than the
        // shared employees.csv. This decouples the wire-format contract from CSVREAD's timezone-dependent parsing of
        // Z-suffixed timestamp strings (H2 caches the default zone at init, so the stored wall-clock -- and therefore
        // the rendered datetime VALUE -- would otherwise shift with the randomized -Dtests.timezone per seed). Naive
        // TIMESTAMP literals are stored as-is; the connector reads them back with a UTC Calendar
        // (see ColumnReader#DATETIME), so the pinned datetime values are reproducible on every seed.
        return """
            CREATE TABLE "employees" (
                "emp_no" INTEGER,
                "first_name" VARCHAR(64),
                "hire_date" TIMESTAMP,
                "salary" INTEGER
            );
            INSERT INTO "employees" ("emp_no", "first_name", "hire_date", "salary") VALUES
                (10001, 'Georgi', TIMESTAMP '1986-06-26 00:00:00', 57305),
                (10002, 'Bezalel', TIMESTAMP '1985-11-21 00:00:00', 56371),
                (10003, 'Parto', TIMESTAMP '1986-08-28 00:00:00', 61805);
            """;
    }
}
