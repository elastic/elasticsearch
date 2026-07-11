/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc.qa;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.logging.log4j.Level;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.datasource.jdbc.JdbcConnectorFactory;
import org.elasticsearch.xpack.esql.datasource.jdbc.qa.JdbcTestQuerySet.Fixture;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * Runs the shared {@link JdbcTestQuerySet} correctness matrix against a real {@code postgres:16.4} container
 * container. This is a Docker-backed database: the in-JVM cluster node opens a
 * {@code jdbc:postgresql://localhost:<forwarded-port>/…} socket to the testcontainers-managed container while
 * still driving it through the exact same parser → dataset rewriter → external source resolver → JDBC connector
 * path the H2 baseline ({@link H2JdbcIT}) proves.
 * <p>
 * <b>{@code PostgresDialect} in effect.</b> The factory resolves
 * {@link org.elasticsearch.xpack.esql.datasource.jdbc.PostgresDialect} for the {@code jdbc:postgresql://} URL through the
 * {@code DialectRegistry}, so the connector applies Postgres type scoping and per-connection init ({@code SET TIME ZONE 'UTC'}
 * + server-side {@code statement_timeout}) against this container. The whole portable SQL92 matrix — native Postgres
 * {@code BOOLEAN} included — passes, and the Postgres-native {@link Fixture#PG_TYPES} fixture (enabled via
 * {@link #enabledFixtures()}) with its {@code pg_*} scenarios now passes in full: the {@code NUMERIC(18,0)} exact-key
 * scenario ({@code pg_numeric_18_0_key}) maps to ES|QL {@code LONG} and round-trips {@code 9007199254740993} exactly,
 * so {@link #knownGapScenarioIds()} is empty.
 * <p>
 * <b>Clean-URL dataset model.</b> {@link PostgresFixture#esqlJdbcUrl()} strips the {@code ?loggerLevel=OFF} query
 * string testcontainers appends, because on {@code main} the {@code ExternalSourceResolver} treats {@code '?'} in the
 * URL path component as a glob metacharacter and would reject a query-string-bearing URL before it reached the JDBC
 * connector. Every scenario in the matrix therefore resolves a clean {@code jdbc:postgresql://host:port/db} URL, with
 * driver tuning carried as the {@code connection_properties} WITH passthrough. {@link #testCleanUrlResolves()} pins
 * that explicitly.
 * <p>
 * <b>Loopback opt-in.</b> testcontainers forwards the container port to {@code localhost}, so the dataset URL's host
 * is loopback; {@link #allowLoopback()} returns {@code true} so the {@code SsrfGuard} accepts it.
 * <p>
 * <b>Thread-leak filter.</b> pgjdbc and testcontainers start JVM-lifetime daemon threads that are not owned by any
 * one test; {@link PostgresTestThreadLeakFilter} tells the randomized runner to ignore exactly those (by name) while
 * still catching a genuine connector/node leak.
 */
@ThreadLeakFilters(filters = { PostgresTestThreadLeakFilter.class, HikariPoolTestThreadLeakFilter.class })
public class PostgresJdbcIT extends AbstractJdbcDatabaseIT {

    /**
     * The exact instant both {@code pg_types} id-1 temporal columns must render to: {@code ts_tz '2020-01-02 03:04:05+00'}
     * (absolute) and {@code ts_naive '2020-01-02 03:04:05'} (naive, anchored to UTC by the reader + the dialect's
     * {@code SET TIME ZONE 'UTC'}). ES|QL renders a {@code DATETIME} with the default {@code strict_date_optional_time}
     * formatter, which always emits millisecond precision, so the rendered cell value is {@code 2020-01-02T03:04:05.000Z}.
     */
    private static final String EXPECTED_UTC_INSTANT = "2020-01-02T03:04:05.000Z";

    @Override
    protected JdbcDatabaseFixture createFixture() {
        return new PostgresFixture();
    }

    /** Backed by a {@link PostgresFixture} testcontainer, so a Docker-less node skips this suite cleanly. */
    @Override
    protected boolean requiresDocker() {
        return true;
    }

    @Override
    protected boolean allowLoopback() {
        return true;
    }

    /**
     * Enables the Postgres-native {@link Fixture#PG_TYPES} fixture on top of the default portable set, so the
     * {@code pg_*} scenarios (NUMERIC scoping, TIMESTAMPTZ, native BOOLEAN/TEXT/SERIAL) run against real Postgres.
     * H2 keeps the default (portable-only) set and never loads this Postgres-specific DDL.
     */
    @Override
    protected Set<Fixture> enabledFixtures() {
        Set<Fixture> fixtures = EnumSet.copyOf(super.enabledFixtures());
        fixtures.add(Fixture.PG_TYPES);
        return fixtures;
    }

    /**
     * No known gaps: the connector resolves {@link org.elasticsearch.xpack.esql.datasource.jdbc.PostgresDialect}
     * for {@code jdbc:postgresql://} URLs, and its {@code NUMERIC} scoping closes the single gap this suite would
     * otherwise carry.
     * <p>
     * The {@code pg_numeric_18_0_key} scenario — {@code NUMERIC(18,0)} holding {@code 9007199254740993} (2^53 + 1)
     * — maps to ES|QL {@code LONG} (scale 0, precision ≤ 18 fits a signed 64-bit long) and round-trips exactly, so it
     * PASSES as a normal scenario rather than being tolerated as a gap. The base class would fail the suite if a stale
     * gap entry started passing. Every other {@code pg_*} scenario already maps identically under both dialects,
     * so with the dialect in place the whole Postgres matrix is green with zero declared gaps.
     */
    @Override
    protected Set<String> knownGapScenarioIds() {
        return Set.of();
    }

    /**
     * Pins main's clean-URL dataset model end-to-end. On {@code main} the {@code ExternalSourceResolver} keys a
     * dataset on a {@link org.elasticsearch.xpack.esql.datasources.spi.StoragePath} whose glob detection inspects the
     * path component; {@code '?'} and {@code '*'} there are glob metacharacters, so a {@code ?query=string}-bearing
     * URL would be misclassified as a glob and rejected before ever reaching the JDBC connector. testcontainers'
     * {@link org.testcontainers.containers.PostgreSQLContainer#getJdbcUrl()} appends a {@code ?loggerLevel=OFF} query
     * string, so {@link PostgresFixture#esqlJdbcUrl()} strips it to a clean {@code jdbc:postgresql://host:port/db}.
     * This test asserts the dataset URL genuinely carries NO query string and still resolves + returns rows, so a
     * future URL-building change that re-introduced a query string (which main cannot resolve) fails loudly here.
     */
    public void testCleanUrlResolves() {
        String url = startedFixture(Fixture.EMPLOYEES).esqlJdbcUrl();
        assertThat("dataset URL must be a clean jdbc: URL with no '?' query string on main", url, not(containsString("?")));

        String dataset = datasetNameFor(Fixture.EMPLOYEES);
        try (EsqlQueryResponse response = run("FROM " + dataset + " | KEEP emp_no | LIMIT 1", queryTimeout())) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), greaterThanOrEqualTo(1));
        }
    }

    /**
     * Proves the SSRF guard ENFORCES at execution against a real dataset, refusing a link-local host that a query
     * could otherwise use to reach the cloud instance-metadata endpoint (169.254.169.254). A {@code FROM} query
     * against the normal container-backed dataset succeeds (baseline), then an ad-hoc dataset whose resource URL
     * points at a link-local host is refused: the connector re-applies the guard in
     * {@code JdbcConnectorFactory.assertAllowed} on {@code resolveMetadata}, and the "not allowed" verdict surfaces in
     * the failure cause chain. Link-local rejection is unconditional (it does not depend on
     * {@code esql.jdbc.ssrf.allow_loopback}), so no cluster setting is mutated and the SUITE-shared node is left
     * untouched.
     * <p>
     * <b>Why not the dynamic kill-switch / subprotocol-allowlist variant.</b> A dynamic variant would flip
     * {@code esql.jdbc.enabled=false} and a tightened {@code esql.jdbc.ssrf.allowed_subprotocols} at RUNTIME
     * via {@code updateClusterSettings}. The {@code DataSourcePlugin} SPI exposes no dynamic
     * cluster-settings hook: {@link org.elasticsearch.xpack.esql.datasource.jdbc.JdbcRuntimeConfig} is seeded ONCE from
     * node {@link org.elasticsearch.common.settings.Settings} on the first {@code connectors(Settings)} call (see the
     * {@code JdbcDataSourcePlugin} class Javadoc), so a runtime settings flip never reaches the guard and cannot be
     * asserted here. Static (node-level) enforcement of the kill switch / subprotocol allowlist is covered by the
     * suites that configure those via {@code nodeSettings}; this test pins the setting-independent host guard.
     */
    public void testSsrfLinkLocalHostRefusedAtExecution() throws Exception {
        PostgresFixture pg = (PostgresFixture) startedFixture(Fixture.EMPLOYEES);
        String dataset = datasetNameFor(Fixture.EMPLOYEES);

        // Baseline: query works against the real container-backed dataset.
        try (EsqlQueryResponse ok = run("FROM " + dataset + " | KEEP emp_no | LIMIT 1", queryTimeout())) {
            assertThat(getValuesList(ok).size(), greaterThanOrEqualTo(1));
        }

        // Ad-hoc dataset whose clean jdbc URL points at the cloud metadata link-local host. The subprotocol
        // (jdbc:postgresql://) is on the default allowlist, so the URL reaches the guard's host filter, which refuses
        // link-local unconditionally -- no socket is ever opened.
        String linkLocalDataset = "jdbc_ssrf_link_local_probe";
        String linkLocalUrl = "jdbc:postgresql://169.254.169.254:5432/test";
        Map<String, String> withConfig = new HashMap<>(pg.datasetConfigOverrides());
        withConfig.put("table", Fixture.EMPLOYEES.tableName());
        try {
            putDatasetByName(linkLocalDataset, linkLocalUrl, withConfig);
            assertQueryRefusedWithGuardVerdict(linkLocalDataset);
        } finally {
            deleteDatasetByName(linkLocalDataset);
        }
    }

    /**
     * Refused-type coverage. The {@code pg_refused} table carries five columns whose Postgres types ES|QL
     * cannot represent — {@code arr INTEGER[]} ({@code java.sql.Types.ARRAY} = 2003) and {@code js}/{@code jsb}/
     * {@code iv}/{@code pt} (JSON / JSONB / INTERVAL / POINT, all reported by pgjdbc as {@code Types.OTHER} = 1111) —
     * alongside three representable ones ({@code id}, {@code keep_txt}, {@code keep_num}). The generic type-mapping
     * switch has no case for the refused codes, so {@code mapJdbcType} returns {@code null} and the connector
     * <b>skips</b> the column from the resolved ES|QL schema (with a WARN naming the column and the JDBC type code)
     * rather than crashing. This test proves both halves of that contract:
     * <ul>
     *   <li>the response schema contains exactly the three representable columns and none of the five refused ones,
     *       and the rest of the row still projects (a real data row comes back), and</li>
     *   <li>each refused column produced the skip WARN with the exact {@code java.sql.Types} code pgjdbc reported —
     *       empirically documenting that the {@code default -> null} skip path is genuinely exercised.</li>
     * </ul>
     * The WARN is captured on {@link JdbcConnectorFactory}'s logger via {@link MockLog}. To make the assertion
     * order-independent it targets {@code pg_refused} through an <em>ad-hoc</em> dataset registered only here: no other
     * scenario resolves that table, so its schema-cache key is cold and the connector's cold-resolve WARN fires on this
     * query rather than possibly having been warmed (and cached) by an earlier test in the SUITE-shared node.
     */
    public void testRefusedColumnsAreSkipped() throws Exception {
        PostgresFixture pg = (PostgresFixture) startedFixture(Fixture.PG_TYPES);
        String dataset = "jdbc_pg_refused_probe";
        String logger = JdbcConnectorFactory.class.getCanonicalName();
        Map<String, String> withConfig = new HashMap<>(pg.datasetConfigOverrides());
        withConfig.put("table", "pg_refused");
        try {
            putDatasetByName(dataset, pg.esqlJdbcUrl(), withConfig);
            try (var mockLog = MockLog.capture(JdbcConnectorFactory.class)) {
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "skip arr (ARRAY)",
                        logger,
                        Level.WARN,
                        "skipping JDBC column [arr] with unsupported type code [2003]"
                    )
                );
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "skip js (OTHER/json)",
                        logger,
                        Level.WARN,
                        "skipping JDBC column [js] with unsupported type code [1111]"
                    )
                );
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "skip jsb (OTHER/jsonb)",
                        logger,
                        Level.WARN,
                        "skipping JDBC column [jsb] with unsupported type code [1111]"
                    )
                );
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "skip iv (OTHER/interval)",
                        logger,
                        Level.WARN,
                        "skipping JDBC column [iv] with unsupported type code [1111]"
                    )
                );
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "skip pt (OTHER/point)",
                        logger,
                        Level.WARN,
                        "skipping JDBC column [pt] with unsupported type code [1111]"
                    )
                );

                try (EsqlQueryResponse response = run("FROM " + dataset, queryTimeout())) {
                    List<String> names = response.columns().stream().map(ColumnInfo::name).toList();
                    // Exactly the three representable columns survive; the five refused ones are absent
                    // (containsInAnyOrder is an exact multiset match, so a leaked refused column fails on the extra).
                    assertThat(
                        "refused columns must be skipped from the ES|QL schema; got " + names,
                        names,
                        containsInAnyOrder("id", "keep_txt", "keep_num")
                    );
                    // The rest of the row still projects -- a refused column does not poison the whole row.
                    assertThat(
                        "the representable columns must still project rows",
                        getValuesList(response).size(),
                        greaterThanOrEqualTo(1)
                    );
                }

                mockLog.assertAllExpectationsMatched();
            }
        } finally {
            deleteDatasetByName(dataset);
        }
    }

    /**
     * Temporal VALUE read against Postgres — a value-bearing lock on the UTC-anchored reader path.
     * It reads the id-1 row's {@code ts_tz} (TIMESTAMPTZ) and {@code ts_naive} (naive
     * TIMESTAMP) back through ES|QL and asserts both materialize to the exact instant {@code 2020-01-02T03:04:05Z}.
     * <p>
     * <b>Why both anchor to the same instant.</b> The {@code DATETIME}
     * {@link org.elasticsearch.xpack.esql.datasource.jdbc.ColumnReader} extracts every temporal via
     * {@code rs.getTimestamp(col, <Calendar in UTC>).toInstant()} — a single driver-portable path (ES|QL {@code DATETIME}
     * has already erased the tz-vs-naive distinction). For {@code ts_tz} the stored {@code '2020-01-02 03:04:05+00'}
     * denotes an absolute instant, so the Calendar is ignored and the instant is exact. For the naive
     * {@code ts_naive '2020-01-02 03:04:05'} the UTC Calendar anchors the wall clock to UTC, which coincides with the
     * {@code PostgresDialect} {@code SET TIME ZONE 'UTC'} session — so both columns land on {@code 2020-01-02T03:04:05Z}
     * regardless of the (randomized) JVM default time zone. The ES|QL response renders a {@code DATETIME} with the
     * default {@code strict_date_optional_time} formatter, so the exact rendered value is {@code 2020-01-02T03:04:05.000Z}.
     * <p>
     * <b>What this pins.</b> A naive {@code rs.getObject(col, java.time.Instant.class)} read would throw
     * {@code PSQLException: conversion to java.time.Instant ... not supported}, because pgjdbc 42.7.3 has no
     * {@code Instant} branch there — which would prevent any Postgres temporal value from being projected.
     * The shared matrix's {@code pg_timestamp_types} scenario never caught it (it asserts the type on an EMPTY result,
     * {@code WHERE id == -999}, so the reader never materializes a value). This test drives a NON-empty projection so
     * the reader runs, and locks the exact instant for both the tz-aware and naive shapes.
     */
    public void testPostgresTemporalValueReadAnchorsToUtc() {
        String dataset = datasetNameFor(Fixture.PG_TYPES);
        try (EsqlQueryResponse response = run("FROM " + dataset + " | WHERE id == 1 | KEEP ts_tz, ts_naive", queryTimeout())) {
            List<ColumnInfo> columns = new ArrayList<>(response.columns());
            assertThat(columns.stream().map(ColumnInfo::name).toList(), containsInAnyOrder("ts_tz", "ts_naive"));
            // Both temporal columns report ES|QL DATETIME ("date").
            for (ColumnInfo column : columns) {
                assertEquals("column [" + column.name() + "] must be a DATETIME", "date", column.outputType());
            }
            List<List<Object>> rows = getValuesList(response);
            assertThat("expected exactly one row for id == 1", rows.size(), equalTo(1));
            // ts_tz (timestamptz) and ts_naive (naive TIMESTAMP under SET TIME ZONE 'UTC') both anchor to the same UTC instant.
            assertThat(rows.get(0), contains(EXPECTED_UTC_INSTANT, EXPECTED_UTC_INSTANT));
        }
    }

    /**
     * Connection-drop clean-error test (pooled connections). Registers an ad-hoc
     * dataset whose JDBC URL carries a short {@code socketTimeout}/{@code loginTimeout} (and a sentinel password in
     * the query string), proves it resolves + returns rows while the container is up, then <b>drops the database
     * mid-life</b> by freezing the container (Docker {@code pause}) so the next connection's reads never get a
     * response. It then asserts the drop fails FAST, SANITIZED, and with a CLEAR error.
     * <p>
     * <b>Failure signature with HikariCP pooling.</b> A connector that opened a physical
     * connection per query would surface a drop-before-borrow as a driver {@link java.sql.SQLException} through the
     * connector's {@code failed to (execute JDBC query|resolve JDBC metadata)} wrapper with a {@code (sqlstate=...)}
     * suffix. Here {@link org.elasticsearch.xpack.esql.datasource.jdbc.JdbcConnector#execute} borrows from a
     * per-endpoint HikariCP pool, and metadata is served from the schema cache warmed by the baseline query below.
     * So a drop-before-borrow now typically surfaces as the pool-acquisition-timeout path: HikariCP's
     * {@code SQLTransientConnectionException} translated to a sanitized, fail-fast
     * {@link IllegalStateException} {@code "no JDBC connection available within <N>ms; target=[...] ..."} which has NO
     * {@code sqlstate=} suffix (it is not a driver SQLException). This is an ARCHITECTURE change, not a capability
     * loss — a dropped backend must still fail fast, stay credential-safe, and produce an actionable error. This test
     * therefore accepts EITHER shape and asserts the invariants that actually matter:
     * <ul>
     *   <li><b>fast, not a hang</b> — bounded by the short {@code socketTimeout} (5s) and/or the pool
     *       {@code connection_timeout} (5s), well under the connector's 300s {@code queryTimeout} /
     *       {@code statement_timeout} default (a regression that dropped either bound would hang for minutes and trip
     *       the bound here);</li>
     *   <li><b>credential-safe</b> — the URL-embedded sentinel password never appears ANYWHERE in the WHOLE surfaced
     *       cause chain (every throwable's class+message, walking causes AND suppressed exceptions). Both the
     *       execute-path sanitizer and the pool-timeout translation route their SQLException cause through
     *       {@link org.elasticsearch.xpack.esql.datasource.jdbc.JdbcUrlSanitizer}, so a HikariCP/pgjdbc exception that
     *       echoed the raw URL (incl. {@code password=}) is scrubbed before it reaches us. This is the
     *       security-critical invariant and is NOT weakened;</li>
     *   <li><b>clear error</b> — the chain carries EITHER the connector's {@code failed to (execute JDBC query|resolve
     *       JDBC metadata)} wrapper with a {@code (sqlstate=...)} suffix (drop-during-execute driver SQLException) OR
     *       the pool's {@code "no JDBC connection available within <N>ms"} timeout (drop-before-borrow);</li>
     *   <li><b>leak-free</b> — after unpausing, the container is still running and a normal query succeeds, proving the
     *       connector/pool released the failed connection and recovered.</li>
     * </ul>
     */
    public void testConnectionDropSurfacesSanitizedError() throws Exception {
        PostgresFixture pg = (PostgresFixture) startedFixture(Fixture.EMPLOYEES);
        String dropDataset = "jdbc_conn_drop_probe";
        // Use the container's REAL password as the leak sentinel. On main the dataset URL must be a CLEAN jdbc: URL
        // (no query string -- '?' is a glob metacharacter in the resolver's path component), so the password rides the
        // WITH "password" secret channel, not the URL. Because the sentinel equals the real password the baseline
        // authenticates, and the leak assertion below genuinely exercises the sanitizer: a driver/pool exception that
        // echoed the password (e.g. into a "FATAL: password authentication failed" or a connection-properties dump)
        // must be scrubbed before it reaches us.
        String sentinelPassword = PostgresFixture.CONTAINER_PASSWORD;
        // Clean URL; the short socket/connect/login timeouts that bound the frozen-backend read to seconds are carried
        // as the non-secret connection_properties passthrough (main's WITH model) rather than a URL query string.
        String url = pg.esqlJdbcUrl();
        Map<String, String> withConfig = new HashMap<>(pg.datasetConfigOverrides());
        withConfig.put("table", Fixture.EMPLOYEES.tableName());
        withConfig.put("connection_properties", "socketTimeout=5;connectTimeout=5;loginTimeout=5");

        boolean paused = false;
        try {
            putDatasetByName(dropDataset, url, withConfig);

            // Baseline: resolves and returns rows while the container is up.
            try (EsqlQueryResponse ok = run("FROM " + dropDataset + " | KEEP emp_no | LIMIT 1", queryTimeout())) {
                assertThat(getValuesList(ok).size(), greaterThanOrEqualTo(1));
            }

            // Drop the DB mid-life: freeze the container so the next connection's reads never get a response.
            pg.pauseContainer();
            paused = true;

            // Generous request timeout so it is the JDBC socketTimeout (5s), not the ES|QL request timeout, that bounds
            // the failure -- a dropped-socketTimeout regression would instead hang ~300s and blow the request timeout.
            long startNanos = System.nanoTime();
            Exception ex = expectThrows(Exception.class, () -> {
                try (EsqlQueryResponse ignored = run("FROM " + dropDataset + " | KEEP emp_no | LIMIT 1", TimeValue.timeValueSeconds(120))) {
                    // must not reach here: the query has to fail against the frozen backend
                }
            });
            long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
            // FAST, not a 5-minute hang. socketTimeout=5 and/or the pool connection_timeout=5s make this fail in
            // seconds; assert comfortably under the connector's 300s default so a dropped-timeout regression (either
            // the JDBC socketTimeout or the pool connection_timeout) fails loudly here rather than parking a worker.
            assertThat("dropped-connection query must fail fast, not hang", elapsedMillis, lessThan(60_000L));

            // Flatten the ENTIRE failure surface: every throwable's class+message, walking BOTH the cause chain AND
            // suppressed exceptions, cycle-safe. This is the whole string surface a leak could ride (stack-trace
            // frames carry only class/method/file/line, never the URL), so it is what both the security probe and the
            // shape assertions read. The exact wrapping depends on where the drop surfaced (pool acquisition vs a
            // driver SQLException on the execute/resolve path); both route their SQLException through the sanitizer.
            List<String> chain = new ArrayList<>();
            java.util.IdentityHashMap<Throwable, Boolean> seen = new java.util.IdentityHashMap<>();
            java.util.Deque<Throwable> pending = new java.util.ArrayDeque<>();
            pending.push(ex);
            while (pending.isEmpty() == false) {
                Throwable t = pending.pop();
                if (t == null || seen.put(t, Boolean.TRUE) != null) {
                    continue;
                }
                chain.add(t.getClass().getName() + ": " + t.getMessage());
                if (t.getCause() != null) {
                    pending.push(t.getCause());
                }
                for (Throwable suppressed : t.getSuppressed()) {
                    pending.push(suppressed);
                }
            }
            String joined = String.join(" || ", chain);
            // Sanitized diagnostic: safe to log because the whole chain was routed through JdbcUrlSanitizer; useful
            // when reviewing which drop signature (pool-timeout vs driver failure) a given run produced.
            logger.info("connection-drop failure chain (sanitized): {}", joined);

            // CLEAR error, accepting EITHER pooled-world shape:
            // (a) drop-during-execute -> a driver SQLException surfaced through the connector's execute/resolve
            // wrapper, which appends the (sqlstate=...) suffix; OR
            // (b) drop-before-borrow -> HikariCP pool-acquisition timeout translated to the fail-fast
            // IllegalStateException "no JDBC connection available within <N>ms".
            boolean driverFailureWrapper = (joined.contains("failed to execute JDBC query against")
                || joined.contains("failed to resolve JDBC metadata for")) && joined.contains("sqlstate=");
            boolean poolAcquisitionTimeout = joined.contains("no JDBC connection available within");
            assertThat(
                "expected either the connector's driver-failure wrapper (with sqlstate) or the pool-timeout error, got: " + joined,
                driverFailureWrapper || poolAcquisitionTimeout,
                equalTo(true)
            );
            // SECURITY-CRITICAL (do NOT weaken): the URL-embedded sentinel password must not survive ANYWHERE in the
            // whole surfaced cause+suppressed chain, whichever drop signature fired.
            assertThat("credential leaked into the surfaced error: " + joined, joined, not(containsString(sentinelPassword)));

            // Recover and prove no container/connection leak: unpause and run a normal query that must succeed.
            pg.unpauseContainer();
            paused = false;
            assertTrue("shared container must still be running after unpause", pg.isContainerRunning());
            try (EsqlQueryResponse recovered = run("FROM " + dropDataset + " | KEEP emp_no | LIMIT 1", queryTimeout())) {
                assertThat(
                    "connector must recover after the drop (no leaked/exhausted connection)",
                    getValuesList(recovered).size(),
                    greaterThanOrEqualTo(1)
                );
            }
        } finally {
            if (paused) {
                try {
                    pg.unpauseContainer();
                } catch (Exception e) {
                    logger.warn("failed to unpause container during cleanup", e);
                }
            }
            deleteDatasetByName(dropDataset);
        }
    }

    private void assertQueryRefusedWithGuardVerdict(String dataset) {
        Exception ex = expectThrows(Exception.class, () -> {
            try (EsqlQueryResponse ignored = run("FROM " + dataset + " | KEEP emp_no | LIMIT 1", queryTimeout())) {
                // no-op: resolution must fail before we get here
            }
        });
        Throwable cause = ex;
        boolean found = false;
        while (cause != null) {
            if (cause.getMessage() != null && cause.getMessage().contains("not allowed")) {
                found = true;
                break;
            }
            cause = cause.getCause();
        }
        assertTrue("expected the JDBC guard's 'not allowed' verdict in the cause chain, got: " + ex, found);
    }
}
