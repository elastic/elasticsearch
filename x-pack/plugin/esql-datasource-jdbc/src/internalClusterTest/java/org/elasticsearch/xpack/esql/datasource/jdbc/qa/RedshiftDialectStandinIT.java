/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc.qa;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.datasource.jdbc.DialectRegistry;
import org.elasticsearch.xpack.esql.datasource.jdbc.RedshiftDialect;
import org.elasticsearch.xpack.esql.datasource.jdbc.qa.JdbcTestQuerySet.Fixture;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

/**
 * Runs the shared {@link JdbcTestQuerySet} correctness matrix with {@link RedshiftDialect} in effect against a real
 * {@code postgres:16.4} backend — the Postgres stand-in for Amazon Redshift, since no local
 * Redshift exists. {@link RedshiftDialect} extends {@code PostgresDialect}, so almost everything is inherited; this
 * suite is the end-to-end proof that the inherited path is <b>unbroken</b> when the dialect's small deltas apply.
 *
 * <h2>How RedshiftDialect gets "forced" against Postgres — via the real production path, not a test hook</h2>
 * {@link RedshiftStandinFixture} hands the connector a {@code jdbc:redshift://} URL (a Postgres container URL with
 * only the scheme swapped). That URL flows through the genuine production machinery end-to-end:
 * <ul>
 *   <li>the {@code SsrfGuard} allowlist entry for {@code jdbc:redshift://} admits it;</li>
 *   <li>the {@code DialectRegistry} resolves it to {@link RedshiftDialect} (pinned by
 *       {@link #testRedshiftSchemeResolvesRedshiftDialect()});</li>
 *   <li>the {@code JdbcDriverRegistry} dispatches it (via {@code acceptsURL}) to {@link RedshiftStandinDriver}, which
 *       delegates to pgjdbc against the real Postgres backend.</li>
 * </ul>
 * So the dialect is not injected by a back door — the connector selects {@link RedshiftDialect} exactly as it would
 * for a real Redshift cluster, and then talks to Postgres. This is the same "reuse the pg-wire path for a
 * Postgres-compatible store" shape applied to the dedicated Redshift scheme.
 *
 * <h2>Deltas exercised vs. covered elsewhere</h2>
 * The inherited path (type mapping, quoting, read path, {@code NUMERIC} scoping) is exercised by the full portable
 * matrix, which passes with zero known gaps (default {@link #knownGapScenarioIds()}). The init-statement delta
 * (drop {@code SET statement_timeout}; use {@code SET timezone TO 'UTC'}) is proven harmless on a real server by
 * {@link #testInitStatementsApplyAndDataReturns()} — a rejected init statement would fail the connection open, so a
 * returning query is the proof both apply. Redshift's type <em>refusals</em> ({@code SUPER}/{@code VARBYTE}/
 * {@code GEOMETRY}/{@code GEOGRAPHY}) cannot be created on a Postgres backend, so they are unit-tested in
 * {@code RedshiftDialectTests} rather than here.
 *
 * <h2>Loopback opt-in + thread-leak filter</h2>
 * testcontainers forwards the container port to {@code localhost}, so {@link #allowLoopback()} returns {@code true}.
 * The backend is pgjdbc + testcontainers, so this reuses {@link PostgresTestThreadLeakFilter} unchanged.
 */
@ThreadLeakFilters(filters = { PostgresTestThreadLeakFilter.class, HikariPoolTestThreadLeakFilter.class })
public class RedshiftDialectStandinIT extends AbstractJdbcDatabaseIT {

    @Override
    protected JdbcDatabaseFixture createFixture() {
        return new RedshiftStandinFixture();
    }

    /**
     * Requires Docker. Although the {@code jdbc:redshift://} <em>driver</em> is a local stand-in
     * ({@link RedshiftStandinDriver}), {@link RedshiftStandinFixture} backs it with a real {@code postgres:16.4}
     * <em>testcontainer</em> (Redshift is a Postgres fork, and there is no local Redshift). So this suite must boot a
     * container just like {@link PostgresJdbcIT}, and a Docker-less node must skip it cleanly
     * rather than ERROR on container boot — hence {@code true} here.
     */
    @Override
    protected boolean requiresDocker() {
        return true;
    }

    @Override
    protected boolean allowLoopback() {
        return true;
    }

    /**
     * Pins the "forcing" mechanism: the fixture URL uses the {@code jdbc:redshift://} scheme, and the production
     * {@code DialectRegistry} resolves that scheme to {@link RedshiftDialect} (not {@code PostgresDialect}). This is
     * what makes the whole matrix below run under {@link RedshiftDialect} — proven through the real resolver, not a
     * test-only override.
     */
    public void testRedshiftSchemeResolvesRedshiftDialect() {
        String url = startedFixture(Fixture.EMPLOYEES).esqlJdbcUrl();
        assertThat("stand-in must present a jdbc:redshift:// URL", url, startsWith("jdbc:redshift://"));
        assertThat(
            "the production DialectRegistry must resolve the Redshift scheme to RedshiftDialect",
            DialectRegistry.defaultRegistry().resolve(url),
            sameInstance(RedshiftDialect.INSTANCE)
        );
    }

    /**
     * Proves the init-statement delta is harmless on a real backend: {@link RedshiftDialect}'s single init statement
     * {@code SET timezone TO 'UTC'} (and, crucially, the ABSENCE of {@code SET statement_timeout}) applies cleanly, so
     * an ordinary aggregation runs to completion and returns data. A rejected init statement would surface as a hard,
     * sanitized failure of the connection open (the connector treats a half-configured session as unusable), so the
     * fact that rows come back is the end-to-end proof both the UTC pin applied and dropping statement_timeout did not
     * break the session.
     */
    public void testInitStatementsApplyAndDataReturns() {
        String dataset = datasetNameFor(Fixture.EMPLOYEES);
        try (EsqlQueryResponse response = run("FROM " + dataset + " | STATS c = COUNT(*)", queryTimeout())) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows, contains(List.of(100L)));
        }
    }

    /**
     * A representative projection + filter round-trips exactly under {@link RedshiftDialect}, confirming the inherited
     * type mapping and read path work against the backend (belt-and-suspenders alongside the full shared matrix).
     */
    public void testProjectionAndFilterRoundTrips() {
        String dataset = datasetNameFor(Fixture.EMPLOYEES);
        try (EsqlQueryResponse response = run("FROM " + dataset + " | WHERE emp_no == 10001 | KEEP emp_no", queryTimeout())) {
            assertThat(response.columns().get(0).name(), containsString("emp_no"));
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), org.hamcrest.Matchers.equalTo(1));
            assertThat(rows.get(0), contains(10001));
        }
    }
}
