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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * End-to-end IT for the JDBC connector: real ES|QL is parsed, planned, and executed against an in-process
 * H2 database that lives in the same test JVM as the cluster node. Each test sends a {@code FROM <dataset>}
 * request through the standard ES|QL transport action, exercising parser → dataset rewriter → external
 * source resolver → analyzer → planner → {@link JdbcConnectorFactory} → {@link JdbcConnector} → H2 round-trip.
 * <p>
 * <b>H2 fixture lifecycle.</b> An in-process H2 <b>TCP server</b> ({@link H2TcpTestServer}) is booted in
 * {@link #setUpH2}, and a single keep-alive {@link Connection} pins the in-mem database open for the duration of the
 * test class (H2 closes the database when the last connection drops unless {@code DB_CLOSE_DELAY=-1} keeps it alive).
 * The {@code employees} table is loaded from {@code employees.csv} on the test classpath via H2's {@code CSVREAD}.
 * The TCP form ({@code jdbc:h2:tcp://localhost:<port>/mem:...}) is used rather than the opaque {@code jdbc:h2:mem:...}
 * URL because ESQL's external-source resolver parses the dataset resource through {@code StoragePath.of}, which
 * requires a {@code ://} authority separator the opaque form lacks. Because {@code internalClusterTest} runs nodes
 * inside the test JVM, the JDBC connector on the test node reaches the server over a loopback socket — no
 * inter-process plumbing needed.
 * <p>
 * <b>Driver discovery in the IT.</b> Production loads the H2 driver from
 * {@code $path.home/plugins/esql-datasource-jdbc/drivers/}, which is empty in an IT. The plugin's
 * {@code buildRegistry} therefore falls back to the plugin classloader when that directory yields no drivers, and
 * H2 is on the internalClusterTest classpath, so the H2 URLs resolve without any static preset hook (there is no
 * shared-registry static).
 * <p>
 * <b>Loopback opt-in.</b> The H2 TCP server listens on {@code localhost}, so {@link #nodeSettings} enables
 * {@code esql.jdbc.ssrf.allow_loopback=true} and permits the {@code jdbc:h2:tcp://} subprotocol (which the
 * production SSRF default allowlist excludes).
 * <p>
 * Single-node by design (same rationale as {@code FromDatasetIT}): multi-node dataset publication trips an
 * unrelated {@code ProjectMetadata.Builder} assertion on {@code main}.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
@ThreadLeakFilters(filters = { HikariPoolTestThreadLeakFilter.class })
public class JdbcDatasetIT extends AbstractExternalDataSourceIT {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    /** Test names that PUT a dataset; @After loops over this to drop them so SUITE-scoped state doesn't leak. */
    private static final Set<String> CREATED_DATASETS = Set.of("employees", "ssrf_denied");
    private static final String DATASOURCE_NAME = "jdbc_ds";

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
     * pass-through {@code TestDataSourcePlugin} (type {@code test}) so we can register a {@code test} data source
     * without a JDBC-specific validator (connector lookup keys off the {@code jdbc:} resource scheme, not the type).
     */
    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(JdbcDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        // Pin pragmas to defaults so query shape is deterministic across runs.
        return QueryPragmas.EMPTY;
    }

    @BeforeClass
    public static void setUpH2() throws Exception {
        // Force the H2 driver class to load in the test classloader (where it lives) BEFORE the cluster boots,
        // so the ServiceLoader scan in fromClassLoader picks it up reliably.
        Class.forName("org.h2.Driver");
        h2Server = H2TcpTestServer.start();
        jdbcUrl = h2Server.urlFor("esql_jdbc_it");
        keepAliveConnection = DriverManager.getConnection(jdbcUrl);
        try (Statement stmt = keepAliveConnection.createStatement()) {
            stmt.execute(loadEmployeesTableSql());
        }
        // No registry preset needed: the plugin's buildRegistry falls back to the plugin classloader (which carries
        // H2 on the internalClusterTest classpath) when the empty $path.home drivers directory yields no drivers.
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
        for (String ds : CREATED_DATASETS) {
            try {
                client().execute(DeleteDatasetAction.INSTANCE, deleteDatasetRequest(ds)).get(30, TimeUnit.SECONDS);
            } catch (ResourceNotFoundException ignored) {
                // already deleted by the test itself
            } catch (Exception e) {
                logger.warn("dataset cleanup [{}] failed", ds, e);
            }
        }
        try {
            client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(DATASOURCE_NAME)).get(30, TimeUnit.SECONDS);
        } catch (ResourceNotFoundException ignored) {
            // already deleted by the test itself
        } catch (Exception e) {
            logger.warn("data source cleanup [{}] failed", DATASOURCE_NAME, e);
        }
    }

    // -- Tests ------------------------------------------------------------------

    public void testFromEmployeesProjectsKeyColumns() throws Exception {
        registerEmployeesDataset();

        try (
            var response = run(syncEsqlQueryRequest("FROM employees | KEEP emp_no, first_name, last_name | SORT emp_no | LIMIT 3"), TIMEOUT)
        ) {
            List<? extends ColumnInfo> columns = response.columns();
            assertThat(columns, hasSize(3));
            assertThat(columns.get(0).name(), equalTo("emp_no"));
            assertThat(columns.get(1).name(), equalTo("first_name"));
            assertThat(columns.get(2).name(), equalTo("last_name"));

            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(3));
            assertThat(rows.get(0).get(0), equalTo(10001));
            assertThat(rows.get(0).get(1).toString(), equalTo("Georgi"));
            assertThat(rows.get(0).get(2).toString(), equalTo("Facello"));
            assertThat(rows.get(1).get(0), equalTo(10002));
            assertThat(rows.get(1).get(1).toString(), equalTo("Bezalel"));
            assertThat(rows.get(2).get(0), equalTo(10003));
            assertThat(rows.get(2).get(1).toString(), equalTo("Parto"));
        }
    }

    public void testFromEmployeesLimitRespected() throws Exception {
        registerEmployeesDataset();

        try (var response = run(syncEsqlQueryRequest("FROM employees | KEEP emp_no | SORT emp_no | LIMIT 5"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(5));
            for (int i = 0; i < 5; i++) {
                assertThat("row " + i, rows.get(i).get(0), equalTo(10001 + i));
            }
        }
    }

    public void testFromEmployeesWhereIntegerEquals() throws Exception {
        registerEmployeesDataset();

        try (var response = run(syncEsqlQueryRequest("FROM employees | WHERE emp_no == 10010 | KEEP emp_no, first_name"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo(10010));
            assertThat(rows.get(0).get(1).toString(), equalTo("Duangkaew"));
        }
    }

    public void testFromEmployeesWhereIntegerGreaterThan() throws Exception {
        registerEmployeesDataset();

        // Salary > 73000 selects exactly six rows in employees.csv; order varies once a filter pushdown picks
        // its own scan order, so assert as a set.
        try (var response = run(syncEsqlQueryRequest("FROM employees | WHERE salary > 73000 | KEEP emp_no"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(6));
            Set<Integer> empNos = new java.util.HashSet<>();
            for (List<Object> row : rows) {
                empNos.add((Integer) row.get(0));
            }
            assertThat(empNos, equalTo(Set.of(10007, 10019, 10027, 10029, 10045, 10099)));
        }
    }

    public void testFromEmployeesWhereStringEquals() throws Exception {
        registerEmployeesDataset();

        // KEYWORD round-trip: identifier quoting + parameter binding for a string literal.
        try (
            var response = run(syncEsqlQueryRequest("FROM employees | WHERE first_name == \"Patricio\" | KEEP emp_no, last_name"), TIMEOUT)
        ) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo(10012));
            assertThat(rows.get(0).get(1).toString(), equalTo("Bridgland"));
        }
    }

    public void testFromEmployeesWhereStringNoMatch() throws Exception {
        registerEmployeesDataset();

        try (var response = run(syncEsqlQueryRequest("FROM employees | WHERE first_name == \"NonExistent\" | KEEP emp_no"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(0));
        }
    }

    public void testFromEmployeesStatsCount() throws Exception {
        registerEmployeesDataset();

        // STATS COUNT(*) hits the empty-projection / zero-block-page path in JdbcConnector that no other test
        // covers end-to-end. employees.csv has 100 rows.
        try (var response = run(syncEsqlQueryRequest("FROM employees | STATS c = COUNT(*)"), TIMEOUT)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(100L));
        }
    }

    public void testFromSsrfDeniedJdbcUrlIsRejected() throws Exception {
        // Pins the closed bypass: DataSourceModule.LazyConnectorFactory.canHandle returns true on the compound-scheme
        // prefix alone, never consulting our canHandle(). Without the assertAllowed() recheck in resolveMetadata, a
        // user query against an SSRF-denied URL would slip past the guard and let the connector open a socket to it.
        // This test fails if the recheck is ever removed.
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, DATASOURCE_NAME, "test", null, new HashMap<>(Map.of()))
            )
        );
        // A jdbc:h2:tcp:// URL whose host is the link-local cloud-metadata address (169.254.169.254): the subprotocol
        // is allowed (this suite permits jdbc:h2:tcp://) but the SsrfGuard host filter refuses link-local hosts
        // unconditionally. The URL carries a "://" authority so it parses through StoragePath.of and reaches the
        // connector's guard recheck, unlike an opaque file-backed URL which the resolver would reject earlier for a
        // different (missing-scheme) reason.
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "ssrf_denied",
                    DATASOURCE_NAME,
                    "jdbc:h2:tcp://169.254.169.254:9092/mem:should-never-open",
                    null,
                    new HashMap<>(Map.of("table", "x"))
                )
            )
        );
        try {
            Exception ex = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest("FROM ssrf_denied | KEEP x | LIMIT 1"), TIMEOUT));
            // Walk the cause chain looking for the guard's "not allowed" verdict; the exception bubbles up
            // wrapped in the resolver's "Failed to resolve metadata" facade.
            Throwable cause = ex;
            boolean found = false;
            while (cause != null) {
                if (cause.getMessage() != null && cause.getMessage().contains("not allowed")) {
                    found = true;
                    break;
                }
                cause = cause.getCause();
            }
            assertTrue("expected guard rejection in cause chain, got: " + ex, found);
        } finally {
            try {
                client().execute(DeleteDatasetAction.INSTANCE, deleteDatasetRequest("ssrf_denied")).get(30, TimeUnit.SECONDS);
            } catch (ResourceNotFoundException ignored) {
                // already gone
            }
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
        // The dataset's resource is the JDBC URL; the connector's WITH-style config is supplied via the
        // dataset settings map. table -> H2 table, format -> ignored by JDBC connector path but the rewriter
        // expects something there (mirrors how csv datasets set format=csv).
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "employees",
                    DATASOURCE_NAME,
                    jdbcUrl,
                    null,
                    new HashMap<>(Map.of("table", "employees"))
                )
            )
        );
    }

    private static String loadEmployeesTableSql() {
        // Mirrors the SQL plugin's setup: H2's CSVREAD function reads from a classpath: URL when the H2 driver
        // is loaded by the same classloader that owns the resource. caseSensitiveColumnNames=true tells CSVREAD
        // to keep the lowercase header from employees.csv instead of upper-casing every column.
        if (JdbcDatasetIT.class.getResource("/employees.csv") == null) {
            throw new IllegalStateException("employees.csv resource not found on classpath");
        }
        // Explicit CREATE TABLE rather than CSVREAD's inferred types so the schema is reproducible and matches
        // what JdbcConnectorFactory.resolveMetadata sees. CSV header: birth_date, emp_no, first_name, gender,
        // hire_date, languages, last_name, salary.
        return """
            CREATE TABLE "employees" (
                "birth_date" TIMESTAMP,
                "emp_no" INTEGER,
                "first_name" VARCHAR(64),
                "gender" VARCHAR(1),
                "hire_date" TIMESTAMP,
                "languages" INTEGER,
                "last_name" VARCHAR(64),
                "salary" INTEGER
            ) AS SELECT
                "birth_date", "emp_no", "first_name", "gender", "hire_date", "languages", "last_name", "salary"
            FROM CSVREAD('classpath:/employees.csv', null, 'caseSensitiveColumnNames=true');
            """;
    }

    private static DeleteDataSourceAction.Request deleteDataSourceRequest(String name) {
        return new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { name });
    }

    private static DeleteDatasetAction.Request deleteDatasetRequest(String name) {
        return new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { name });
    }
}
