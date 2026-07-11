/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc.qa;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.datasource.jdbc.qa.JdbcTestQuerySet.Fixture;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * Postgres-focused <b>performance baseline</b> suite. It loads a ~100K-row dataset into a
 * real {@code postgres:16.4} container (reusing the {@link PostgresFixture} + {@link AbstractJdbcDatabaseIT} harness)
 * and drives five baseline measurements end-to-end through the ES|QL → JDBC connector → Postgres path:
 * <ol>
 *   <li><b>full-scan throughput</b> — scan the whole 100K-row table, log rows/sec;</li>
 *   <li><b>filtered-query latency</b> — {@value #FILTERED_QUERY_COUNT} point-lookup queries, log p50/p95/p99;</li>
 *   <li><b>per-query memory overhead</b> — the ES request circuit-breaker's used-bytes delta while a query's
 *       response is held (before/after <em>within this one method</em>, so it is not confounded across methods; logs
 *       {@code n/a} if the breaker API is unreachable rather than failing);</li>
 *   <li><b>concurrent-query scaling</b> — {@value #CONCURRENCY} genuinely-parallel queries released from a barrier so
 *       they contend for the HikariCP pool's {@code max_per_url} connections (real parallel borrows, not 50
 *       sequential queries), log the wall-clock scaling factor;</li>
 *   <li><b>filter-pushdown benefit</b> — the same {@code KEEP id | LIMIT} query over the same data at two
 *       selectivities (no filter vs {@code WHERE id > <max-1>}), log the rows-transferred reduction.</li>
 * </ol>
 *
 * <b>Metrics are LOGGED, never ASSERTED.</b> Every number is emitted through {@link PerformanceMetricLogger} as a
 * fixed {@code jdbc.perf ...} line for offline trend analysis; this suite asserts <em>no</em> threshold, because
 * performance assertions are brittle in shared CI. The only failures this suite can produce are
 * genuine errors (a query throwing, the container not starting) — not a metric being "too slow". The inherited
 * correctness matrix {@link AbstractJdbcDatabaseIT#testSharedCorrectnessMatrix()} is skipped here (correctness is
 * already covered by {@link PostgresJdbcIT}); this class exists purely to log performance baselines.
 *
 * <p><b>Opt-in / excluded from normal runs.</b> The suite is long-running (100K-row load + 1000 point queries + a
 * 50-way concurrency burst) so it must not run as part of a normal {@code internalClusterTest} invocation. It is
 * excluded by default via a Gradle test-filter (see the {@code internalClusterTest} block in {@code build.gradle},
 * which excludes {@code *PostgresPerformanceIT} unless the run explicitly targets it with
 * {@code --tests '*PostgresPerformanceIT'}). That filter is the JUnit4/randomized-runner equivalent of a
 * {@code @Tag("performance")} exclude.
 *
 * <p><b>Docker gate.</b> Like every {@link PostgresFixture}-backed suite this needs Docker; it overrides
 * {@link AbstractJdbcDatabaseIT#requiresDocker()} to {@code true} so a Docker-less node is <em>skipped</em> cleanly
 * via {@link org.elasticsearch.test.fixtures.testcontainers.DockerAvailability#assumeDockerIsAvailable()} before any
 * container boot, the same way {@link PostgresJdbcIT} is, so it never hard-fails a Docker-less environment.
 */
@TestLogging(
    value = "org.elasticsearch.xpack.esql.datasource.jdbc.qa.PerformanceMetricLogger:INFO",
    reason = "emit the jdbc.perf performance-baseline metric lines so they are captured in the test output"
)
@ThreadLeakFilters(filters = { PostgresTestThreadLeakFilter.class, HikariPoolTestThreadLeakFilter.class })
public class PostgresPerformanceIT extends AbstractJdbcDatabaseIT {

    /** Realistic-but-tractable row count: large enough to expose throughput characteristics, small enough for vfs. */
    private static final int ROW_COUNT = 100_000;

    /** Node-scoped result-truncation ceiling, raised above {@link #ROW_COUNT} so the full scan returns every row. */
    private static final int RESULT_TRUNCATION_MAX = 200_000;

    /** Number of point-lookup queries sampled for the latency-percentile measurement. */
    private static final int FILTERED_QUERY_COUNT = 1000;

    /** Degree of genuine query parallelism for the concurrency measurement. */
    private static final int CONCURRENCY = 50;

    /**
     * The HikariCP default {@code esql.jdbc.pool.max_per_url}. Logged (not enforced) so the concurrency line
     * records the pool ceiling the {@value #CONCURRENCY}-way burst contends against — {@value #CONCURRENCY} concurrent
     * borrowers against a pool of this size is what makes the measurement genuinely exercise pool admission control.
     */
    private static final int POOL_MAX_PER_URL = 10;

    private static final String PERF_TABLE = "perf_dataset";
    private static final String PERF_DATASET = "jdbc_perf_dataset";

    /** Rows returned by the throughput/pushdown scans can far exceed the default 30s; give heavy scans a wide budget. */
    private static final TimeValue LONG_TIMEOUT = TimeValue.timeValueMinutes(3);

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT);
    private static final LocalDateTime TS_BASE = LocalDateTime.of(2020, 1, 1, 0, 0, 0);

    private final PerformanceMetricLogger metrics = new PerformanceMetricLogger("postgres");

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
     * Raises {@code esql.query.result_truncation_max_size} above {@value #ROW_COUNT} so the full-scan measurement's
     * {@code | LIMIT 100000} actually returns all {@value #ROW_COUNT} rows rather than being capped at ES|QL's default
     * 10 000-row truncation ceiling — otherwise the "full scan" throughput would only reflect a 10K-row slice. This
     * only affects this perf suite's in-JVM node (the setting is node-scoped), on top of the base suite's loopback
     * opt-in.
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("esql.query.result_truncation_max_size", RESULT_TRUNCATION_MAX)
            .build();
    }

    /**
     * Enable only the small {@link Fixture#EMPLOYEES} fixture. It is not measured; it merely bootstraps the shared
     * Postgres container + data-source + clean teardown provided by {@link AbstractJdbcDatabaseIT}. The heavy 100K-row
     * {@value #PERF_TABLE} table is created and loaded programmatically (via {@code COPY}) inside the performance test
     * method itself, so it is loaded exactly once (only for the perf method) rather than in a per-method {@code @Before}.
     */
    @Override
    protected Set<Fixture> enabledFixtures() {
        return EnumSet.of(Fixture.EMPLOYEES);
    }

    /**
     * This is a performance-baseline suite, not a correctness suite: skip the inherited shared correctness matrix.
     * Correctness against Postgres is asserted by {@link PostgresJdbcIT}; re-running it here would only add a wasted
     * container lifecycle and blur this suite's "logged, never asserted" contract.
     */
    @Override
    public void testSharedCorrectnessMatrix() {
        assumeTrue("PostgresPerformanceIT logs performance baselines only; correctness is covered by PostgresJdbcIT", false);
    }

    /**
     * Loads the ~100K-row {@value #PERF_TABLE} dataset, registers it as an ES|QL dataset, then runs all five
     * baseline measurements and logs each via {@link PerformanceMetricLogger}. All measurements live in this single
     * method so the circuit-breaker before/after delta (measurement 3) is not confounded across methods and the 100K
     * load happens exactly once.
     */
    public void testPostgresPerformanceBaselines() throws Exception {
        PostgresFixture pg = (PostgresFixture) startedFixture(Fixture.EMPLOYEES);

        long loadStartNanos = System.nanoTime();
        loadPerfDataset(pg);
        long loadMillis = elapsedMillis(loadStartNanos);
        metrics.log(
            "dataset_load",
            PerformanceMetricLogger.metrics()
                .put("rows", ROW_COUNT)
                .put("elapsed_ms", loadMillis)
                .put("load_method", "copy")
                .put("throughput_rows_per_sec", PerformanceMetricLogger.throughputPerSec(ROW_COUNT, loadMillis))
                .build()
        );

        Map<String, String> withConfig = new HashMap<>(pg.datasetConfigOverrides());
        withConfig.put("table", PERF_TABLE);
        putDatasetByName(PERF_DATASET, pg.esqlJdbcUrl(), withConfig);
        try {
            measureFullScanThroughput();
            measureFilteredQueryLatency();
            measurePerQueryMemoryOverhead();
            measureConcurrentScaling();
            measureFilterPushdownBenefit();
        } finally {
            deleteDatasetByName(PERF_DATASET);
        }
    }

    // -- Dataset loading --------------------------------------------------------

    /**
     * Creates {@value #PERF_TABLE} and bulk-loads {@value #ROW_COUNT} deterministic rows with mixed types
     * (int / keyword / double / timestamp) via the Postgres {@code COPY} protocol, chunked to keep the in-memory CSV
     * bounded. {@code COPY} (rather than row-by-row {@code INSERT}) is what keeps a 100K load tractable on the vfs
     * Docker storage driver. The data is deterministic (a pure function of the row id) so successive runs are
     * comparable.
     */
    private void loadPerfDataset(PostgresFixture pg) throws Exception {
        try (Connection connection = pg.newConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS " + PERF_TABLE);
                statement.execute(
                    "CREATE TABLE "
                        + PERF_TABLE
                        + " ("
                        + "id integer NOT NULL, "
                        + "category varchar(32) NOT NULL, "
                        + "amount double precision NOT NULL, "
                        + "quantity integer NOT NULL, "
                        + "created_at timestamp NOT NULL"
                        + ")"
                );
            }
            CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
            final int chunk = 20_000;
            for (int start = 1; start <= ROW_COUNT; start += chunk) {
                int end = Math.min(start + chunk - 1, ROW_COUNT);
                StringBuilder csv = new StringBuilder((end - start + 1) * 48);
                for (int id = start; id <= end; id++) {
                    csv.append(id)
                        .append(",cat_")
                        .append(id % 100)
                        .append(',')
                        .append(id * 1.5)
                        .append(',')
                        .append(id % 1000)
                        .append(',')
                        .append(TS_BASE.plusSeconds(id).format(TS))
                        .append('\n');
                }
                copyManager.copyIn("COPY " + PERF_TABLE + " FROM STDIN WITH (FORMAT csv)", new StringReader(csv.toString()));
            }
        }
    }

    // -- Measurement 1: full-scan throughput ------------------------------------

    private void measureFullScanThroughput() {
        long startNanos = System.nanoTime();
        long rows;
        try (EsqlQueryResponse response = run("FROM " + PERF_DATASET + " | LIMIT " + ROW_COUNT, LONG_TIMEOUT)) {
            rows = getValuesList(response).size();
        }
        long millis = elapsedMillis(startNanos);
        metrics.log(
            "full_scan",
            PerformanceMetricLogger.metrics()
                .put("rows", rows)
                .put("elapsed_ms", millis)
                .put("throughput_rows_per_sec", PerformanceMetricLogger.throughputPerSec(rows, millis))
                .build()
        );
    }

    // -- Measurement 2: filtered-query latency percentiles ----------------------

    private void measureFilteredQueryLatency() {
        long[] samplesNanos = new long[FILTERED_QUERY_COUNT];
        long totalStartNanos = System.nanoTime();
        for (int i = 0; i < FILTERED_QUERY_COUNT; i++) {
            // Spread lookups deterministically across the whole key space (step is coprime-ish with ROW_COUNT).
            int id = 1 + (i * 97) % ROW_COUNT;
            long queryStart = System.nanoTime();
            try (
                EsqlQueryResponse response = run(
                    "FROM " + PERF_DATASET + " | WHERE id == " + id + " | KEEP id, category, amount",
                    queryTimeout()
                )
            ) {
                getValuesList(response);
            }
            samplesNanos[i] = System.nanoTime() - queryStart;
        }
        long totalMillis = elapsedMillis(totalStartNanos);
        long[] sorted = samplesNanos.clone();
        java.util.Arrays.sort(sorted);
        metrics.log(
            "filtered_latency",
            PerformanceMetricLogger.metrics()
                .put("queries", FILTERED_QUERY_COUNT)
                .put("elapsed_ms", totalMillis)
                .put("throughput_queries_per_sec", PerformanceMetricLogger.throughputPerSec(FILTERED_QUERY_COUNT, totalMillis))
                .put("p50_ms", PerformanceMetricLogger.percentileMillis(sorted, 50))
                .put("p95_ms", PerformanceMetricLogger.percentileMillis(sorted, 95))
                .put("p99_ms", PerformanceMetricLogger.percentileMillis(sorted, 99))
                .put("min_ms", sorted[0] / 1_000_000.0)
                .put("max_ms", sorted[sorted.length - 1] / 1_000_000.0)
                .build()
        );
    }

    // -- Measurement 3: per-query memory overhead via circuit-breaker delta ------

    /**
     * Reads the ES request circuit-breaker's used bytes immediately before a query, again while the query's response
     * is still held open (so the response's retained pages are reflected in the breaker), and once more after closing
     * it. The {@code delta_bytes} (during − before) approximates the per-query memory the response occupies in the
     * request breaker. If the breaker API is not reachable in this environment the memory fields are logged as
     * {@code n/a} and the suite continues — a missing measurement is never a test failure.
     */
    private void measurePerQueryMemoryOverhead() {
        CircuitBreaker breaker = requestBreakerOrNull();
        if (breaker == null) {
            metrics.log(
                "memory_breaker_delta",
                PerformanceMetricLogger.metrics()
                    .put("breaker", "request")
                    .put("used_before_bytes", PerformanceMetricLogger.NOT_AVAILABLE)
                    .put("used_during_bytes", PerformanceMetricLogger.NOT_AVAILABLE)
                    .put("used_after_bytes", PerformanceMetricLogger.NOT_AVAILABLE)
                    .put("delta_bytes", PerformanceMetricLogger.NOT_AVAILABLE)
                    .build()
            );
            return;
        }
        try {
            long usedBefore = breaker.getUsed();
            EsqlQueryResponse response = run("FROM " + PERF_DATASET + " | LIMIT 50000", LONG_TIMEOUT);
            long usedDuring;
            long rows;
            try {
                usedDuring = breaker.getUsed();
                rows = getValuesList(response).size();
            } finally {
                response.close();
            }
            long usedAfter = breaker.getUsed();
            metrics.log(
                "memory_breaker_delta",
                PerformanceMetricLogger.metrics()
                    .put("rows", rows)
                    .put("breaker", "request")
                    .put("used_before_bytes", usedBefore)
                    .put("used_during_bytes", usedDuring)
                    .put("used_after_bytes", usedAfter)
                    .put("delta_bytes", usedDuring - usedBefore)
                    .build()
            );
        } catch (Exception e) {
            // Any failure taking the measurement (not a functional query failure we care about here) -> log n/a.
            logger.warn("circuit-breaker memory measurement failed; logging n/a", e);
            metrics.log(
                "memory_breaker_delta",
                PerformanceMetricLogger.metrics()
                    .put("breaker", "request")
                    .put("used_before_bytes", PerformanceMetricLogger.NOT_AVAILABLE)
                    .put("used_during_bytes", PerformanceMetricLogger.NOT_AVAILABLE)
                    .put("used_after_bytes", PerformanceMetricLogger.NOT_AVAILABLE)
                    .put("delta_bytes", PerformanceMetricLogger.NOT_AVAILABLE)
                    .build()
            );
        }
    }

    private CircuitBreaker requestBreakerOrNull() {
        try {
            CircuitBreakerService service = internalCluster().getInstance(CircuitBreakerService.class);
            return service.getBreaker(CircuitBreaker.REQUEST);
        } catch (Exception e) {
            logger.warn("request circuit breaker not reachable; memory measurement will log n/a", e);
            return null;
        }
    }

    // -- Measurement 4: concurrent-query scaling (genuinely exercises the HikariCP pool) ----

    /**
     * Fires {@value #CONCURRENCY} identical queries from {@value #CONCURRENCY} threads that are all released from a
     * single barrier, so the connector performs {@value #CONCURRENCY} <b>parallel</b> connection borrows against the
     * HikariCP pool (default {@code max_per_url}={@value #POOL_MAX_PER_URL}) — genuine pool admission-control
     * contention, not {@value #CONCURRENCY} sequential queries mislabeled "concurrent". It logs the concurrent
     * wall-clock, the mean single-query latency, and the scaling factor (ideal-sequential / concurrent). All requests
     * are built on the test thread (pragmas pinned to {@link QueryPragmas#EMPTY}) and executed via a captured
     * {@link Client}, so no randomized-context / random call happens on a worker thread.
     */
    private void measureConcurrentScaling() throws Exception {
        final Client client = client();
        final String query = "FROM " + PERF_DATASET + " | WHERE id <= 2000 | STATS c = COUNT(*)";

        // Sequential baseline: mean single-query latency (also warms schema-cache / pool).
        int warmup = 5;
        long seqTotalNanos = 0;
        for (int i = 0; i < warmup; i++) {
            long start = System.nanoTime();
            executeAndClose(client, buildRequest(query), LONG_TIMEOUT);
            seqTotalNanos += System.nanoTime() - start;
        }
        double seqAvgMillis = (seqTotalNanos / (double) warmup) / 1_000_000.0;

        List<EsqlQueryRequest> requests = new ArrayList<>(CONCURRENCY);
        for (int i = 0; i < CONCURRENCY; i++) {
            requests.add(buildRequest(query));
        }

        AtomicInteger threadId = new AtomicInteger();
        ThreadFactory threadFactory = r -> new Thread(r, "perf-concurrent-" + threadId.getAndIncrement());
        ExecutorService pool = Executors.newFixedThreadPool(CONCURRENCY, threadFactory);
        CountDownLatch ready = new CountDownLatch(CONCURRENCY);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>(CONCURRENCY);
        try {
            for (EsqlQueryRequest request : requests) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    executeAndClose(client, request, LONG_TIMEOUT);
                    return null;
                }));
            }
            ready.await(60, TimeUnit.SECONDS);
            long startNanos = System.nanoTime();
            go.countDown();
            for (Future<?> future : futures) {
                future.get(3, TimeUnit.MINUTES);
            }
            long concurrentMillis = elapsedMillis(startNanos);
            double idealSequentialMillis = seqAvgMillis * CONCURRENCY;
            double scaling = concurrentMillis > 0 ? idealSequentialMillis / concurrentMillis : Double.NaN;
            metrics.log(
                "concurrent_scaling",
                PerformanceMetricLogger.metrics()
                    .put("concurrency", CONCURRENCY)
                    .put("pool_max_per_url", POOL_MAX_PER_URL)
                    .put("queries", CONCURRENCY)
                    .put("elapsed_ms", concurrentMillis)
                    .put("seq_avg_ms", seqAvgMillis)
                    .put("throughput_queries_per_sec", PerformanceMetricLogger.throughputPerSec(CONCURRENCY, concurrentMillis))
                    .put("scaling_factor", scaling)
                    .build()
            );
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    // -- Measurement 5: filter-pushdown benefit (rows-transferred comparison) ----

    /**
     * Runs the same {@code KEEP id | LIMIT} projection over the same data at two selectivities — no filter (the whole
     * table) vs {@code WHERE id > ROW_COUNT-1} (a single row) — so the <em>only</em> difference is selectivity. Logs
     * the rows-transferred (ES|QL result row count, the rows the connector surfaced end-to-end) and elapsed for each,
     * plus the reduction factor, isolating the benefit of filtering close to the source.
     */
    private void measureFilterPushdownBenefit() {
        long fullStart = System.nanoTime();
        long fullRows;
        try (EsqlQueryResponse response = run("FROM " + PERF_DATASET + " | KEEP id | LIMIT " + ROW_COUNT, LONG_TIMEOUT)) {
            fullRows = getValuesList(response).size();
        }
        long fullMillis = elapsedMillis(fullStart);

        long filteredStart = System.nanoTime();
        long filteredRows;
        String selectiveFilter = "id > " + (ROW_COUNT - 1);
        try (
            EsqlQueryResponse response = run(
                "FROM " + PERF_DATASET + " | WHERE " + selectiveFilter + " | KEEP id | LIMIT " + ROW_COUNT,
                LONG_TIMEOUT
            )
        ) {
            filteredRows = getValuesList(response).size();
        }
        long filteredMillis = elapsedMillis(filteredStart);

        metrics.log(
            "pushdown_full_scan",
            PerformanceMetricLogger.metrics()
                .put("filter", "none")
                .put("rows_transferred", fullRows)
                .put("elapsed_ms", fullMillis)
                .put("throughput_rows_per_sec", PerformanceMetricLogger.throughputPerSec(fullRows, fullMillis))
                .build()
        );
        metrics.log(
            "pushdown_filtered",
            PerformanceMetricLogger.metrics()
                .put("filter", selectiveFilter.replace(' ', '_'))
                .put("rows_transferred", filteredRows)
                .put("elapsed_ms", filteredMillis)
                .build()
        );
        metrics.log(
            "pushdown_benefit",
            PerformanceMetricLogger.metrics()
                .put("full_rows_transferred", fullRows)
                .put("filtered_rows_transferred", filteredRows)
                .put("rows_reduction_factor", (double) fullRows / Math.max(1, filteredRows))
                .put("full_elapsed_ms", fullMillis)
                .put("filtered_elapsed_ms", filteredMillis)
                .build()
        );
    }

    // -- helpers ----------------------------------------------------------------

    private static EsqlQueryRequest buildRequest(String query) {
        EsqlQueryRequest request = syncEsqlQueryRequest(query);
        // Pin pragmas so the query shape is deterministic and no randomized-context call happens off the test thread.
        request.pragmas(QueryPragmas.EMPTY);
        return request;
    }

    private static void executeAndClose(Client client, EsqlQueryRequest request, TimeValue timeout) {
        try (EsqlQueryResponse response = client.execute(EsqlQueryAction.INSTANCE, request).actionGet(timeout)) {
            // Materialize nothing beyond the response itself; closing releases the request breaker reservation.
        }
    }

    private static long elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }
}
