/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc.qa;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.fixtures.testcontainers.DockerAvailability;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.AbstractExternalDataSourceIT;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.datasource.jdbc.JdbcDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.jdbc.qa.JdbcTestQuerySet.ExpectedColumn;
import org.elasticsearch.xpack.esql.datasource.jdbc.qa.JdbcTestQuerySet.Fixture;
import org.elasticsearch.xpack.esql.datasource.jdbc.qa.JdbcTestQuerySet.RowOrder;
import org.elasticsearch.xpack.esql.datasource.jdbc.qa.JdbcTestQuerySet.Scenario;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.Before;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;

/**
 * Vendor-neutral base for the JDBC connector's end-to-end correctness suite. A concrete subclass supplies a
 * {@link JdbcDatabaseFixture} (H2, or a Postgres testcontainer); this base boots an in-JVM ES
 * cluster with the {@link JdbcDataSourcePlugin}, seeds the fixture with the portable SQL datasets, registers a
 * {@code FROM <dataset>} target for each fixture table, and drives every {@link JdbcTestQuerySet} scenario through
 * the real ES|QL transport action against that database.
 * <p>
 * <b>Strict assertions.</b> For each scenario the response is checked against the
 * scenario's declared {@link ExpectedColumn columns} and rows with no tolerance for silent degradation:
 * <ul>
 *   <li><b>Columns</b> — the response column count, each column {@code name()}, and each column
 *       {@link ColumnInfo#outputType() outputType()} must equal the scenario's expectation, in order. Asserting the
 *       {@code outputType} (not just the name) is what makes a column that the connector silently mapped to
 *       {@code null}/unsupported <em>fail</em> here rather than pass as an empty-but-present column.</li>
 *   <li><b>Rows</b> — for {@link RowOrder#ORDERED} the actual rows must equal the expected list position-by-position;
 *       for {@link RowOrder#UNORDERED} they must match as a multiset (same rows, same multiplicities, any order), so
 *       a query whose order the ES|QL text does not pin is compared order-independently but still exactly.</li>
 * </ul>
 * Every assertion message carries the scenario's {@code queryId} so a failure in a real-database subclass points
 * straight at the offending scenario.
 * <p>
 * <b>Feature-flag gate.</b> The {@code FROM <dataset>} path traverses the external data-source SPI, which is only
 * wired when the {@code dataset-in-from-command} capability and the local-filesystem feature flag are enabled;
 * {@link AbstractExternalDataSourceIT#requireFeatureFlag()} (inherited) {@code assumeTrue}s on both before any fixture
 * work, so a build without the flag skips cleanly instead of failing.
 * <p>
 * Single-node, SUITE scope for the same reason as {@code JdbcDatasetIT}: multi-node dataset publication trips an
 * unrelated {@code ProjectMetadata.Builder} assertion on {@code main}, and a single in-JVM node keeps boot fast.
 * <p>
 * <b>One fixture instance per table; URL sharing is the fixture's choice.</b> Each {@link Fixture} is seeded into
 * its own {@link JdbcDatabaseFixture} instance via {@link #createFixture()}. A fixture may back each instance with a
 * distinct URL (in-process H2 gives each instance a fresh {@code mem:} database on a shared TCP server) or share one
 * endpoint across all of them (the Postgres testcontainer does: all three tables live in one database behind one
 * {@code jdbc:postgresql://…} URL, differing only by the {@code table} WITH option). Sharing one URL is safe because
 * the JDBC storage stub declares {@code supportsStableMetadata()==false} (the cache-bypass invariant), so
 * {@code ExternalSourceResolver.isCacheable()} is false and each dataset is re-resolved against its own {@code table}
 * rather than colliding on a URL-keyed cache entry — see {@code JdbcSchemaCacheBypassIT} for the end-to-end
 * regression proof.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
@ThreadLeakFilters(filters = { HikariPoolTestThreadLeakFilter.class })
public abstract class AbstractJdbcDatabaseIT extends AbstractExternalDataSourceIT {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);
    private static final String DATASOURCE_NAME = "jdbc_ds";

    /** One started fixture per {@link Fixture} table for the current test-method lifecycle; empty when the flag skipped setup. */
    private final Map<Fixture, JdbcDatabaseFixture> fixtures = new LinkedHashMap<>();

    /**
     * Builds a fresh, empty concrete database fixture (e.g. in-process H2, or a Postgres testcontainer). Called once
     * per {@link Fixture} table so each table gets its own database/URL (see the class Javadoc for why).
     */
    protected abstract JdbcDatabaseFixture createFixture();

    /**
     * The subset of {@link Fixture fixtures} this suite loads and runs scenarios against. Defaults to only the
     * {@link Fixture#portable() portable} SQL92 fixtures, which load unchanged on every vendor — so the H2 baseline
     * is unaffected. A vendor suite that also wants the vendor-native fixtures (e.g. the Postgres suite
     * enabling {@link Fixture#PG_TYPES}) overrides this to widen the set. A fixture that is not enabled has neither its
     * DDL loaded nor its {@link JdbcTestQuerySet} scenarios executed, so a vendor-specific fixture never reaches a
     * database that cannot parse its DDL.
     */
    protected Set<Fixture> enabledFixtures() {
        Set<Fixture> portable = EnumSet.noneOf(Fixture.class);
        for (Fixture f : Fixture.values()) {
            if (f.portable()) {
                portable.add(f);
            }
        }
        return portable;
    }

    /**
     * The JDBC connector plugin is the only format plugin this suite adds; {@link AbstractExternalDataSourceIT}
     * installs the pass-through {@code TestDataSourcePlugin} (type {@code test}) and the rest of the node-plugin
     * wiring, so registering a {@code test} data source needs no JDBC-specific validator — connector lookup keys off
     * the {@code jdbc:} resource URL scheme, not the data source type.
     */
    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(JdbcDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        // Pin pragmas to defaults so the query shape (and thus the connector's SQL) is deterministic across runs.
        return QueryPragmas.EMPTY;
    }

    /**
     * Whether the JDBC connector should accept a loopback host in the dataset URL for this suite. Default {@code false}
     * matches production (and H2's in-mem URL has no host, so it is unaffected). The Postgres suite overrides
     * to {@code true} because testcontainers forwards the container's port to {@code localhost:<random>}, so the
     * dataset URL's host is loopback and the {@code SsrfGuard} would otherwise reject it.
     */
    protected boolean allowLoopback() {
        return false;
    }

    /**
     * Extra SSRF subprotocol allowlist entries this suite needs beyond the production defaults. Empty by default,
     * which leaves {@code esql.jdbc.ssrf.allowed_subprotocols} unset (production defaults apply — Postgres/Redshift/…
     * plus {@code jdbc:h2:mem:}). The H2 suites override this to {@code jdbc:h2:tcp://} because their in-process H2 TCP
     * server URL uses that subprotocol, which the production default deliberately excludes. Setting the list replaces
     * the defaults, so a suite that overrides this must include every subprotocol its own URLs use.
     */
    protected List<String> allowedSubprotocols() {
        return List.of();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (allowLoopback()) {
            builder.put("esql.jdbc.ssrf.allow_loopback", true);
        }
        List<String> subprotocols = allowedSubprotocols();
        if (subprotocols.isEmpty() == false) {
            builder.putList("esql.jdbc.ssrf.allowed_subprotocols", subprotocols);
        }
        return builder.build();
    }

    /**
     * Whether this suite's fixtures require a running Docker daemon (i.e. they boot a testcontainer). Default
     * {@code false}: the H2 baseline ({@link H2JdbcIT}) runs entirely in-process and needs no Docker, so it stays
     * unaffected. A testcontainer-backed suite (Postgres, the Postgres perf baseline, and the Redshift
     * stand-in — whose {@code jdbc:redshift://} <em>driver</em> is local but whose backend is a real Postgres
     * container) overrides this to {@code true} so that on a Docker-less node {@link #setUpFixtureAndDatasets()}
     * <em>skips</em> the suite cleanly via {@link DockerAvailability#assumeDockerIsAvailable()} instead of erroring
     * out when the container fails to boot. See that method for the skip-not-fail semantics (assumeFalse on an
     * excluded OS; on CI it asserts Docker is present; otherwise assumeTrue on the Docker probe).
     */
    protected boolean requiresDocker() {
        return false;
    }

    /**
     * Runs after the inherited feature-flag/capability gate ({@link AbstractExternalDataSourceIT#requireFeatureFlag()}),
     * then (for {@link #requiresDocker() Docker-backed suites}) gates on Docker availability, then brings up one fixture
     * per {@link Fixture} table (each with its own database/URL) and registers a dataset for each. Every
     * {@link JdbcDatabaseFixture#start()} is invoked <em>inside</em> the {@code try} body (not as a try-with-resources
     * resource) so that a failure anywhere in start/seed/register still stops <em>all</em> already-started fixtures in
     * the {@code catch} — preserving the container-leak-cleanup guarantee the base fixture documents.
     * <p>
     * The Docker gate is placed <em>before</em> any fixture work so a
     * Docker-less node aborts (skips) the test method before {@link JdbcDatabaseFixture#start()} ever tries to boot a
     * container — turning "no Docker" into a clean skip rather than a container-boot ERROR. It is a no-op for the
     * no-Docker suites, which leave {@link #requiresDocker()} at its {@code false} default.
     */
    @Before
    public void setUpFixtureAndDatasets() throws Exception {
        // The feature-flag / capability gate is applied by AbstractExternalDataSourceIT#requireFeatureFlag(), which
        // runs first (superclass @Before) and skips the whole method via assumeTrue before this setup ever runs.
        if (requiresDocker()) {
            DockerAvailability.assumeDockerIsAvailable();
        }
        try {
            registerDataSource();
            for (Fixture f : enabledFixtures()) {
                JdbcDatabaseFixture pending = createFixture();
                // Track the fixture BEFORE start() so a start() that fails after bringing an endpoint up (e.g.
                // startDatabase() succeeds but the control connection fails) is still stopped by stopAllFixtures().
                // Without this the partially-started fixture would never reach the map and would leak — harmless
                // for in-process H2, but it would leak a testcontainer for the Postgres suite.
                fixtures.put(f, pending);
                pending.start();
                pending.load(sqlStatements(readResource(f.resourcePath())));
                registerDataset(f, pending);
            }
        } catch (Exception e) {
            stopAllFixtures(e);
            throw e;
        }
    }

    /**
     * Drops every registered dataset and the data source, then stops every started fixture. No-op when the
     * feature-flag {@code assumeTrue} skipped setup (in which case nothing was registered and no fixture was started).
     */
    @After
    public void tearDownFixtureAndDatasets() throws Exception {
        if (fixtures.isEmpty()) {
            return;
        }
        try {
            for (Fixture f : fixtures.keySet()) {
                deleteQuietly(
                    () -> client().execute(DeleteDatasetAction.INSTANCE, deleteDatasetRequest(datasetName(f))).get(30, TimeUnit.SECONDS)
                );
            }
            deleteQuietly(
                () -> client().execute(DeleteDataSourceAction.INSTANCE, deleteDataSourceRequest(DATASOURCE_NAME)).get(30, TimeUnit.SECONDS)
            );
        } finally {
            stopAllFixtures(null);
        }
    }

    /**
     * Stops every started fixture, then clears the map. If {@code primary} is non-null (setup-failure path), any
     * stop failure is suppressed onto it; otherwise (teardown path) the first stop failure is rethrown so a leak is
     * never silent.
     */
    private void stopAllFixtures(Exception primary) throws Exception {
        Exception failure = primary;
        for (JdbcDatabaseFixture f : fixtures.values()) {
            try {
                f.stop();
            } catch (Exception e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        fixtures.clear();
        if (failure != null && failure != primary) {
            throw failure;
        }
    }

    /**
     * Scenario ids this database is <em>known</em> to get wrong under the dialect it currently runs. A scenario listed
     * here is still executed, but a failure is
     * recorded as an <b>expected known gap</b> rather than a test failure — and, symmetrically, a listed scenario
     * that unexpectedly <b>passes</b> fails the test, so the list can never silently rot. This is how a vendor
     * baseline stays green while honestly documenting the type-mapping gaps that motivate the
     * {@code PostgresDialect} work — it does NOT weaken any assertion or fake any value.
     * <p>
     * Default empty: H2 passes the whole matrix, so no gaps are declared and the behaviour is unchanged.
     */
    protected Set<String> knownGapScenarioIds() {
        return Set.of();
    }

    /**
     * Runs the full shared correctness matrix against the fixture. Every scenario in {@link JdbcTestQuerySet} is
     * executed (not merely declared) and asserted strictly. Rather than stopping at the first failure, each scenario
     * is run independently and its outcome recorded, so a single run reports the complete matrix status (which
     * scenarios pass, which fail, and which fail as {@link #knownGapScenarioIds() documented known gaps}). The test
     * then fails iff a scenario outside the known-gap set failed, OR a known-gap scenario unexpectedly passed — the
     * latter keeps the gap list from silently rotting once a later dialect closes it.
     */
    public void testSharedCorrectnessMatrix() {
        // Only run scenarios whose fixture this suite actually loaded (see enabledFixtures()); a vendor-native
        // scenario (e.g. pg_*) is silently absent from suites that did not enable its fixture rather than failing
        // for a missing dataset.
        Set<Fixture> enabled = enabledFixtures();
        List<Scenario> scenarios = JdbcTestQuerySet.scenarios().stream().filter(s -> enabled.contains(s.fixture())).toList();
        Set<String> knownGaps = knownGapScenarioIds();
        List<String> passed = new ArrayList<>();
        Map<String, String> failed = new LinkedHashMap<>();
        Map<String, String> knownGapFailures = new LinkedHashMap<>();
        List<String> unexpectedlyPassedGaps = new ArrayList<>();
        for (Scenario scenario : scenarios) {
            String id = scenario.queryId();
            boolean isKnownGap = knownGaps.contains(id);
            try {
                runAndAssertScenario(scenario);
                if (isKnownGap) {
                    unexpectedlyPassedGaps.add(id);
                    logger.warn("scenario [{}] is listed as a known gap but PASSED -- remove it from knownGapScenarioIds()", id);
                } else {
                    passed.add(id);
                    logger.info("scenario [{}] PASSED", id);
                }
            } catch (AssertionError | Exception e) {
                if (isKnownGap) {
                    knownGapFailures.put(id, e.toString());
                    logger.info("scenario [{}] failed as an EXPECTED known gap: {}", id, e.toString());
                } else {
                    failed.put(id, e.toString());
                    logger.error("scenario [{}] FAILED: {}", id, e.toString());
                }
            }
        }
        logger.info(
            "shared correctness matrix: {} passed, {} known-gap failures, {} unexpected failures "
                + "(of {} scenarios); passed={}; knownGaps={}",
            passed.size(),
            knownGapFailures.size(),
            failed.size(),
            scenarios.size(),
            passed,
            knownGapFailures.keySet()
        );
        StringBuilder problems = new StringBuilder();
        if (failed.isEmpty() == false) {
            problems.append(failed.size()).append('/').append(scenarios.size()).append(" scenarios FAILED unexpectedly:");
            for (Map.Entry<String, String> e : failed.entrySet()) {
                problems.append("\n  - ").append(e.getKey()).append(": ").append(e.getValue());
            }
        }
        if (unexpectedlyPassedGaps.isEmpty() == false) {
            if (problems.length() > 0) {
                problems.append('\n');
            }
            problems.append("known-gap scenarios that now PASS (remove them from knownGapScenarioIds()): ").append(unexpectedlyPassedGaps);
        }
        if (problems.length() > 0) {
            fail(problems.toString());
        }
    }

    private void runAndAssertScenario(Scenario scenario) {
        String query = scenario.esql(datasetName(scenario.fixture()));
        try (EsqlQueryResponse response = run(query, TIMEOUT)) {
            assertColumns(scenario, response.columns());
            assertRows(scenario, getValuesList(response));
        }
    }

    private static void assertColumns(Scenario scenario, List<? extends ColumnInfo> actual) {
        List<ExpectedColumn> expected = scenario.columns();
        assertEquals(
            "column count for [" + scenario.queryId() + "]; expected " + expected + " but got " + describe(actual),
            expected.size(),
            actual.size()
        );
        for (int i = 0; i < expected.size(); i++) {
            ExpectedColumn ec = expected.get(i);
            ColumnInfo ac = actual.get(i);
            assertEquals("column[" + i + "] name for [" + scenario.queryId() + "]", ec.name(), ac.name());
            // outputType is the strict part: a column the connector silently mapped to null/unsupported reports a
            // different outputType (or is absent), so this catches it rather than letting an "empty" column pass.
            assertEquals(
                "column[" + i + "] outputType for [" + scenario.queryId() + "] name=[" + ec.name() + "]",
                ec.type(),
                ac.outputType()
            );
        }
    }

    private static void assertRows(Scenario scenario, List<List<Object>> actual) {
        List<List<Object>> expected = scenario.rows();
        if (scenario.order() == RowOrder.ORDERED) {
            assertEquals("rows (ordered) for [" + scenario.queryId() + "]", expected, actual);
        } else {
            assertEquals(
                "row count for [" + scenario.queryId() + "]; expected " + expected + " but got " + actual,
                expected.size(),
                actual.size()
            );
            assertEquals(
                "rows (unordered multiset) for [" + scenario.queryId() + "]; expected " + expected + " but got " + actual,
                multiset(expected),
                multiset(actual)
            );
        }
    }

    /** Frequency map keyed on the row contents, so UNORDERED scenarios are compared as an exact multiset. */
    private static Map<List<Object>, Integer> multiset(List<List<Object>> rows) {
        Map<List<Object>, Integer> counts = new LinkedHashMap<>();
        for (List<Object> row : rows) {
            counts.merge(row, 1, Integer::sum);
        }
        return counts;
    }

    private static String describe(List<? extends ColumnInfo> columns) {
        List<String> parts = new ArrayList<>(columns.size());
        for (ColumnInfo c : columns) {
            parts.add(c.name() + ":" + c.outputType());
        }
        return parts.toString();
    }

    // -- Dataset registration ---------------------------------------------------

    private void registerDataSource() throws Exception {
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, DATASOURCE_NAME, "test", null, new HashMap<>(Map.of()))
            )
        );
    }

    private void registerDataset(Fixture f, JdbcDatabaseFixture startedFixture) throws Exception {
        // The dataset resource is the fixture's JDBC URL; the WITH-style connector config carries the target table
        // plus any per-fixture connector overrides (e.g. Postgres user/password). Mirrors how JdbcDatasetIT registers
        // the employees dataset (format is ignored on the JDBC path but the rewriter tolerates its absence here).
        // Postgres runs all three fixture tables behind ONE container URL: the datasets differ only by the `table`
        // WITH option, and the JDBC storage stub's supportsStableMetadata()==false makes the resolver re-resolve
        // each dataset against its own table rather than collide on a URL-keyed schema-cache entry.
        Map<String, Object> withConfig = new HashMap<>();
        withConfig.put("table", f.tableName());
        withConfig.putAll(startedFixture.datasetConfigOverrides());
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    datasetName(f),
                    DATASOURCE_NAME,
                    startedFixture.esqlJdbcUrl(),
                    null,
                    withConfig
                )
            )
        );
    }

    /**
     * Registers an ad-hoc JDBC dataset (outside the per-fixture set) against the suite's data source, for tests that
     * need a bespoke resource URL or {@code WITH} config — e.g. the connection-drop test, which points at the shared
     * container URL with a short {@code socketTimeout} so a dropped read fails in seconds rather than at the connector's
     * 300s default. The caller owns cleanup: pair every call with {@link #deleteDatasetByName(String)} in a
     * {@code finally} so no persistent dataset metadata leaks into the SUITE-shared cluster.
     */
    protected void putDatasetByName(String datasetName, String url, Map<String, String> withConfig) throws Exception {
        Map<String, Object> cfg = new HashMap<>(withConfig);
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(TIMEOUT, TIMEOUT, datasetName, DATASOURCE_NAME, url, null, cfg)
            )
        );
    }

    /** Best-effort delete of an ad-hoc dataset registered via {@link #putDatasetByName}; safe to call if never registered. */
    protected void deleteDatasetByName(String datasetName) {
        deleteQuietly(() -> client().execute(DeleteDatasetAction.INSTANCE, deleteDatasetRequest(datasetName)).get(30, TimeUnit.SECONDS));
    }

    /** Stable dataset name for a fixture's {@code FROM} target; namespaced so it never collides with a table name. */
    private static String datasetName(Fixture fixture) {
        return "jdbc_" + fixture.tableName();
    }

    /**
     * The dataset name registered for {@code fixture}'s {@code FROM} target. Exposed to subclasses so an
     * enforcement / URL-variant test can query a specific fixture's dataset without duplicating the naming scheme.
     */
    protected static String datasetNameFor(Fixture fixture) {
        return datasetName(fixture);
    }

    /**
     * The started fixture backing {@code fixture} for the current test method (after {@link #setUpFixtureAndDatasets()}),
     * or {@code null} if none. Exposed so a subclass can assert properties of the resolved endpoint (e.g. that the
     * Postgres URL actually carries a {@code ?query=string}).
     */
    protected JdbcDatabaseFixture startedFixture(Fixture fixture) {
        return fixtures.get(fixture);
    }

    /** The suite's standard per-query timeout, exposed for subclass-authored queries. */
    protected static TimeValue queryTimeout() {
        return TIMEOUT;
    }

    private static DeleteDataSourceAction.Request deleteDataSourceRequest(String name) {
        return new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { name });
    }

    private static DeleteDatasetAction.Request deleteDatasetRequest(String name) {
        return new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { name });
    }

    private interface CleanupStep {
        void run() throws Exception;
    }

    private void deleteQuietly(CleanupStep step) {
        try {
            step.run();
        } catch (ResourceNotFoundException ignored) {
            // already deleted by the test itself
        } catch (Exception e) {
            logger.warn("cleanup step failed", e);
        }
    }

    // -- Fixture SQL loading ----------------------------------------------------

    private static String readResource(String resourcePath) throws Exception {
        try (InputStream in = AbstractJdbcDatabaseIT.class.getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalStateException("fixture resource not found on classpath: " + resourcePath);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /**
     * Splits a portable {@code .sql} fixture into individual statements: full-line {@code --} comments and blank
     * lines are dropped, then the remainder is split on {@code ;} (the fixtures deliberately contain no {@code ;}
     * inside string literals, per their header comments). Each fixture is thus a {@code CREATE TABLE} followed by a
     * single multi-row {@code INSERT}.
     */
    private static List<String> sqlStatements(String script) {
        StringBuilder sb = new StringBuilder();
        for (String line : script.split("\n")) {
            String trimmed = line.strip();
            if (trimmed.isEmpty() || trimmed.startsWith("--")) {
                continue;
            }
            sb.append(line).append('\n');
        }
        List<String> statements = new ArrayList<>();
        for (String candidate : sb.toString().split(";")) {
            String stmt = candidate.strip();
            if (stmt.isEmpty() == false) {
                statements.add(stmt);
            }
        }
        return statements;
    }
}
