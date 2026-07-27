/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import fixture.s3.S3HttpHandler;

import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.elasticsearch.xpack.esql.heap_attack.HeapAttackExternalFixtures.Compression;
import org.elasticsearch.xpack.esql.heap_attack.HeapAttackExternalFixtures.Format;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.BUCKET;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;

/**
 * Heap-attack suite for the {@code FROM <dataset>} ES|QL command over external datasources. The mirror
 * image of {@link HeapAttackIT}: instead of attacking the cluster from inside (FROM + bulk-indexed data),
 * we attack it from outside by serving pathologically large or fat datasource files via an in-memory S3
 * fixture and submitting {@code FROM <dataset>} queries that would crash the node if the request breaker
 * did not intervene. Each scenario registers a dataset (bound to a single {@code auth=anonymous} S3 data
 * source against the fixture) pointing at its uploaded object key, then queries it by name. Because the
 * data source persists no secrets, no project encryption key is required.
 * <p>
 * The cluster keeps the standard 512&nbsp;MB heap with a reduced request-breaker limit from
 * {@link ExternalClusters#buildExternalCluster}. Payload sizes are auto-tuned per-run from the
 * cluster's actual {@code jvm.mem.heap_max_in_bytes} (see {@link #ensureHeapBudgetDiscovered()}),
 * so the suite stays meaningful if someone later changes the heap setting. Each scenario must
 * trip the request circuit breaker via {@link #assertCircuitBreaks} or
 * {@link #assertCircuitBreaksVia} &mdash; a successful query is a test failure because it would
 * mean the payload was too small to prove anything about breaker protection. The 5-minute
 * {@code /_trigger_out_of_memory} backstop from {@link #runQuery} catches the (unwanted) case
 * where the breaker never trips before timeout.
 */
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class HeapAttackExternalIT extends HeapAttackRestHelpers {

    private static final DataSourcesS3HttpFixture S3_FIXTURE = new DataSourcesS3HttpFixture();

    private static final ElasticsearchCluster CLUSTER = ExternalClusters.buildExternalCluster(S3_FIXTURE::getAddress);

    @ClassRule
    public static final TestRule RULE_CHAIN = RuleChain.outerRule(S3_FIXTURE).around(CLUSTER);

    /** Object-key prefix for blobs of the current test method. Must live under {@code WAREHOUSE/}
     *  because the S3 fixture's {@code isHeadObjectRequest} check requires the request path to start
     *  with {@code /{bucket}/{basePath}/}, where the fixture sets basePath = WAREHOUSE. */
    private static final String KEY_PREFIX = WAREHOUSE + "/heap-attack-external";

    /** Name of the {@code auth=anonymous} S3 data source backing every {@code FROM <dataset>} query in this
     *  suite. Registered once against the in-memory fixture; since it persists no secrets it requires no
     *  project encryption key. */
    private static final String DATA_SOURCE = "heap_attack_s3";

    /** Hard cap on rows generated in the test JVM regardless of cluster heap, to keep the
     *  test-JVM payload byte[] tractable. The actual row count used per scenario is derived from
     *  the cluster's breaker budget (see {@link #runStatsBlowup}); this constant only prevents
     *  runaway scaling on unusually large heaps. */
    private static final int MAX_ROWS = 30_000_000;

    /** Detected at test start from {@code _nodes/stats}; the min across nodes. */
    private static volatile long clusterHeapMax = -1;

    @Override
    protected String getTestRestCluster() {
        return CLUSTER.getHttpAddresses();
    }

    @Before
    public void assumeEnabledAndDiscoverHeap() throws Exception {
        assumeFalse("FIPS mode requires security enabled; this suite uses plain HTTP S3 fixtures", inFipsJvm());
        assumeTrue("FROM <dataset> support required; skipping in release build", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        ensureHeapBudgetDiscovered();
        ensureDataSourceRegistered();
    }

    /**
     * Clear blobs left in the in-memory S3 fixture by this test method. Without this the fixture
     * accumulates hundreds of MB of payloads across the test class (one per attempt × scenario)
     * and the test JVM eventually OOMs while generating the next payload.
     */
    @After
    public void clearTestBlobs() {
        String prefix = "/" + BUCKET + "/" + KEY_PREFIX + "/" + sanitizedTestName() + "/";
        Iterator<String> it = handler().blobs().keySet().iterator();
        while (it.hasNext()) {
            if (it.next().startsWith(prefix)) {
                it.remove();
            }
        }
    }

    /**
     * Drops the datasets and data source this suite registered: they are {@code ProjectCustom} cluster
     * metadata that {@link org.elasticsearch.test.rest.ESRestTestCase}'s between-test wipe does not touch,
     * so they must be deleted explicitly. {@link DatasetRegistry#clearCaches()} runs unconditionally so a
     * failed cleanup does not leave stale cache entries for a later suite in the same JVM fork.
     */
    @AfterClass
    public static void cleanupRegisteredDatasets() throws IOException {
        try {
            DatasetRegistry.cleanup(adminClient());
        } finally {
            DatasetRegistry.clearCaches();
        }
    }

    /**
     * The external datasource plugins (parquet / iceberg readers) keep a small steady-state
     * allocation on the request breaker even between queries, so the parent's strict
     * {@code == 0} idle check fails right out of the gate. Relax the probe to 4&nbsp;MiB: we
     * still detect anything resembling a real leak after a heap-attack scenario.
     */
    @Override
    protected long allowedRequestBreakerBaselineBytes() {
        return 4L * 1024 * 1024;
    }

    private void ensureHeapBudgetDiscovered() throws IOException {
        if (clusterHeapMax > 0) {
            return;
        }
        String override = System.getProperty("heapattack.external.heap.mb");
        if (override != null) {
            clusterHeapMax = Long.parseLong(override) * 1024L * 1024L;
            logger.info("Using overridden heap budget: {} bytes", clusterHeapMax);
            return;
        }
        Response response = adminClient().performRequest(new Request("GET", "/_nodes/stats?filter_path=nodes.*.jvm.mem.heap_max_in_bytes"));
        Map<?, ?> stats = responseAsMap(response);
        Map<?, ?> nodes = (Map<?, ?>) stats.get("nodes");
        long minHeapMax = Long.MAX_VALUE;
        for (Object n : nodes.values()) {
            Map<?, ?> node = (Map<?, ?>) n;
            Map<?, ?> jvm = (Map<?, ?>) node.get("jvm");
            Map<?, ?> mem = (Map<?, ?>) jvm.get("mem");
            Number heapMax = (Number) mem.get("heap_max_in_bytes");
            minHeapMax = Math.min(minHeapMax, heapMax.longValue());
        }
        clusterHeapMax = minHeapMax;
        logger.info("Detected cluster heap budget (min across nodes): {} bytes", clusterHeapMax);
    }

    /**
     * Registers the {@code auth=anonymous} S3 data source the suite's datasets bind to. The fixture serves
     * blobs without an {@code Authorization} header, so anonymous reads succeed; persisting no secrets
     * means no project encryption key is needed. Idempotent via {@link DatasetRegistry#ensureDataSource}.
     */
    private void ensureDataSourceRegistered() throws IOException {
        DatasetRegistry.ensureDataSource(
            adminClient(),
            DATA_SOURCE,
            "s3",
            Map.of("endpoint", S3_FIXTURE.getAddress(), "auth", "anonymous")
        );
    }

    /*
     * Scenario 1 (deferred): single fat value via Parquet.
     *
     * Originally intended to verify the per-value CircuitBreakerByteBufferAllocator path by
     * shipping a single multi-hundred-MB BYTE_ARRAY value through Parquet. Two ergonomic issues
     * currently block it from running cleanly in this module:
     *
     *  * Generating such a payload in the test JVM relies on parquet-mr's writer. The writer's
     *    footer phase copies the column's max value into BinaryStatistics, which roughly doubles
     *    the peak heap pressure during close() — a 4 GB test JVM can't accommodate the multi-
     *    hundred-MB string twice. Possible fixes: write the file via a Gradle JavaExec task with
     *    a larger heap, or extend the writer to skip statistics for huge BINARY columns.
     *  * NDJSON / CSV variants would short-circuit on Jackson's per-string length cap (10 MB by
     *    default) before any breaker-protected allocation, so they verify the Jackson defense,
     *    not the CB.
     *
     * The Parquet path is still partially covered indirectly through unit tests that already
     * exist for CircuitBreakerByteBufferAllocator. Re-add this REST-level scenario here once the
     * payload-generation problem is solved.
     */

    // Scenario 2: many-rows STATS BY high-cardinality key blows the hash table.

    public void testStatsBlowupParquet() throws IOException {
        runStatsBlowup(Format.PARQUET, Compression.NONE);
    }

    public void testStatsBlowupParquetZstd() throws IOException {
        runStatsBlowup(Format.PARQUET, Compression.ZSTD);
    }

    public void testStatsBlowupNdjson() throws IOException {
        runStatsBlowup(Format.NDJSON, Compression.NONE);
    }

    public void testStatsBlowupNdjsonZstd() throws IOException {
        runStatsBlowup(Format.NDJSON, Compression.ZSTD);
    }

    public void testStatsBlowupCsv() throws IOException {
        runStatsBlowup(Format.CSV, Compression.NONE);
    }

    public void testStatsBlowupCsvZstd() throws IOException {
        runStatsBlowup(Format.CSV, Compression.ZSTD);
    }

    private void runStatsBlowup(Format format, Compression compression) throws IOException {
        // Target ~1.5× the breaker budget in STATS hash-table groups so the breaker reliably trips
        // without the total memory footprint (tracked + untracked S3/Netty/parsing overhead) pushing
        // the node into an OOM. STATS state per group is conservatively ~64 bytes (group key +
        // accumulators). Sizing against the breaker budget (not total heap) leaves headroom for the
        // untracked allocations from the S3 client's Netty infrastructure and the format-specific
        // parsing pipeline.
        long breakerBudget = clusterHeapMax * ExternalClusters.BREAKER_LIMIT_PERCENT / 100;
        int baseDistinctKeys = (int) Math.min(MAX_ROWS / 4, breakerBudget / 64L * 3 / 2);
        int baseRowCount = baseDistinctKeys * 4;
        // Same key across attempts so the fixture's blob map holds at most one payload per test
        // method — otherwise five attempts × ~200 MB pile up in the test JVM heap.
        String key = scenarioKey("statsblowup", format, compression);
        // The dataset's resource (the s3:// URI) is stable across attempts — only the payload at that
        // key grows — so a single registration per scenario suffices. The format is inferred from the
        // key's extension, so the dataset carries no WITH settings.
        String dataset = datasetName("statsblowup", format, compression);
        DatasetRegistry.ensureDataset(adminClient(), dataset, DATA_SOURCE, "s3://" + BUCKET + "/" + key, null);
        assertCircuitBreaks(attempt -> {
            int distinctKeys = (int) Math.min(MAX_ROWS / 4, (long) baseDistinctKeys * attempt);
            int rowCount = (int) Math.min(MAX_ROWS, (long) baseRowCount * attempt);
            byte[] payload = HeapAttackExternalFixtures.maybeCompress(
                HeapAttackExternalFixtures.manyRows(format, rowCount, distinctKeys, compression),
                format,
                compression
            );
            S3FixtureUtils.addBlobToFixture(handler(), key, payload);
            String esql = "FROM " + dataset + " | STATS c = COUNT(*) BY id";
            return runQueryAsMap(esql);
        });
    }

    /*
     * Sort/TopN blowup is scoped out for now. The TopN operator's per-row footprint over a
     * single long column is small enough that a 30M-row CSV (which is already near the test-JVM
     * payload limit) doesn't push the request breaker past its threshold. Adding it back
     * will need either a wider per-row payload (e.g. a long keyword column) or a tighter cluster
     * breaker limit; both are mechanical follow-ups.
     */

    // --- Helpers ---

    private S3HttpHandler handler() {
        return S3_FIXTURE.getHandler();
    }

    private String scenarioKey(String scenario, Format format, Compression compression) {
        return String.format(
            Locale.ROOT,
            "%s/%s/%s-%s%s",
            KEY_PREFIX,
            sanitizedTestName(),
            scenario,
            format.extension,
            HeapAttackExternalFixtures.fileExtension(format, compression)
        );
    }

    /**
     * Returns the bare method name without the {@code {seed=[...]}} suffix that the randomized
     * runner appends when {@code -Dtests.iters} is in effect. S3 keys and dataset names must not
     * contain curly braces (glob syntax) or spaces.
     */
    private String sanitizedTestName() {
        String name = getTestName();
        int brace = name.indexOf(" {");
        return brace >= 0 ? name.substring(0, brace) : name;
    }

    /**
     * Index-name-legal dataset id for a scenario/format/compression triple: lowercase, underscore
     * separated, with none of the slashes, dots or braces that {@link #scenarioKey} carries. The test
     * method name keeps datasets distinct across the parameterized scenarios so each binds to its own
     * object key.
     */
    private String datasetName(String scenario, Format format, Compression compression) {
        return String.format(
            Locale.ROOT,
            "heap_attack_%s_%s_%s_%s",
            sanitizedTestName().toLowerCase(Locale.ROOT),
            scenario,
            format.name().toLowerCase(Locale.ROOT),
            compression.name().toLowerCase(Locale.ROOT)
        );
    }

    private Map<String, Object> runQueryAsMap(String esql) throws IOException {
        // Wrap in JSON {"query":"..."} the same way HeapAttackRestHelpers#query expects.
        String body = "{\"query\":\"" + esql.replace("\"", "\\\"") + "\"}";
        Response response = query(body, null);
        Map<String, Object> map = responseAsMap(response);
        return map;
    }
}
