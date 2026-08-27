/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.esql.QueryMetricsListener;
import org.elasticsearch.xpack.esql.action.AbstractExternalDataSourceIT;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.esql.datasource.bzip2.Bzip2DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.lz4.Lz4DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.zstd.ZstdDataSourcePlugin;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class EsqlQueryMetricsCollectorIT extends AbstractExternalDataSourceIT {

    /** Written by the SPI-instantiated collector; read by the test body. */
    static volatile Map<String, Long> lastMetrics;

    public static final class CapturingCollector implements QueryMetricsListener {
        @Override
        public void onQueryCompleted(Map<String, Long> metrics) {
            EsqlQueryMetricsCollectorIT.lastMetrics = metrics;
        }
    }

    /**
     * Drops in for {@link EsqlEnterpriseWithDatasourceExtensions} and additionally loads
     * {@link QueryMetricsListener} extensions by calling
     * {@link EsqlPlugin#loadMetricsCollectors}, which is suppressed in the base
     * {@link EsqlPluginWithEnterpriseOrTrialLicense} no-op override.
     */
    public static final class EsqlEnterpriseWithCollector extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            loadMetricsCollectors(loader);
        }
    }

    @Before
    protected void checkLocalSources() {
        // We're using local data source here, which needs the flag
        assumeTrue("requires local filesystem feature flag", HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());
    }

    @After
    protected void clearLastMetrics() {
        lastMetrics = null;
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(
            CsvDataSourcePlugin.class,
            NdJsonDataSourcePlugin.class,
            GzipDataSourcePlugin.class,
            Bzip2DataSourcePlugin.class,
            ZstdDataSourcePlugin.class,
            Lz4DataSourcePlugin.class,
            ParquetDataSourcePlugin.class
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(EsqlEnterpriseWithCollector.class);
        return plugins;
    }

    public void testMetricsCollectorCsv() throws Exception {
        Path dir = createTempDir();
        String csv = createCsv(5);
        Files.writeString(dir.resolve("data.csv"), csv);

        String datasetName = registerDataset("metrics_test_ds", dir.resolve("data.csv").toUri().toString(), Map.of("format", "csv"));

        try (var ignored = run(syncEsqlQueryRequest("FROM " + datasetName + " | LIMIT 10"), TIMEOUT)) {
            // run the query — result discarded, only the collector side-effect matters
        }

        assertReadCpuNanos("csv");
        assertThat(lastMetrics.get(QueryMetricsListener.PLANNING_NANOS), greaterThan(0L));
        assertThat(lastMetrics.get(QueryMetricsListener.CPU_NANOS), greaterThan(0L));
        assertThat(lastMetrics.get(QueryMetricsListener.SPLIT_DISCOVERY_NANOS), greaterThan(0L));
        // TODO: does not work for CVS for now: assertThat(lastMetrics.get(QueryMetricsListener.BYTES_READ), greaterThan(0L));
    }

    /** NdJson plain file — exercises the single-pass NdJson reader path. */
    public void testMetricsCollectorNdJson() throws Exception {
        Path dir = createTempDir();
        String ndjson = createNdjson(5);
        Files.writeString(dir.resolve("data.ndjson"), ndjson);

        registerDataset("metrics_ndjson_ds", dir.resolve("data.ndjson").toUri().toString(), Map.of());

        try (var ignored = run(syncEsqlQueryRequest("FROM metrics_ndjson_ds | LIMIT 10"), TIMEOUT)) {}

        assertReadCpuNanos("ndjson");
    }

    /**
     * @param splittable true for codecs that use {@code SPLITTABLE_OR_INDEXED_COMPRESSED} (e.g. bzip2), which run the full parse on
     *                   the producer thread so both {@code read_nanos} and {@code read_cpu_nanos} are populated; false for
     *                   {@code STREAM_ONLY_COMPRESSED} (e.g. zstd, gzip), where the producer only polls a queue and
     *                   {@code read_nanos} is near-zero.
     */
    private void assertCompressedDataset(String name, CheckedBiConsumer<Path, String, IOException> writer, boolean splittable)
        throws IOException {
        Path dir = createTempDir();
        lastMetrics = null;
        // CSV
        String csv = createCsv(100);
        writer.accept(dir.resolve("data.csv." + name), csv);

        registerDataset("metrics_csv_ds_" + name, dir.resolve("data.csv." + name).toUri().toString(), Map.of());

        try (var ignored = run(syncEsqlQueryRequest("FROM metrics_csv_ds_" + name + " | LIMIT 200"), TIMEOUT)) {}

        if (splittable) {
            assertReadCpuNanos(name + "-csv");
        } else {
            assertThat(name + "-csv: metrics must be set", lastMetrics, notNullValue());
            assertThat(name + "-csv: read_cpu_nanos > 0", lastMetrics.get(QueryMetricsListener.READ_CPU_NANOS), greaterThan(0L));
        }

        lastMetrics = null;
        // NDJSON
        String ndjson = createNdjson(100);
        writer.accept(dir.resolve("data.ndjson." + name), ndjson);

        registerDataset("metrics_ndjson_ds_" + name, dir.resolve("data.ndjson." + name).toUri().toString(), Map.of());

        try (var ignored = run(syncEsqlQueryRequest("FROM metrics_ndjson_ds_" + name + " | LIMIT 200"), TIMEOUT)) {}

        if (splittable) {
            assertReadCpuNanos(name + "-ndjson");
        } else {
            assertThat(name + "-ndjson: metrics must be set", lastMetrics, notNullValue());
            assertThat(name + "-ndjson: read_cpu_nanos > 0", lastMetrics.get(QueryMetricsListener.READ_CPU_NANOS), greaterThan(0L));
        }
    }

    /**
     * Covers all three compression dispatch modes across both CSV and NDJSON:
     * <ul>
     *   <li><b>bzip2</b> ({@code SPLITTABLE_OR_INDEXED_COMPRESSED}): falls back to single-threaded
     *       {@code CompressionDelegatingFormatReader.read()}. The producer thread runs the full format parse
     *       directly, so both {@code read_nanos} and {@code read_cpu_nanos} are populated.</li>
     *   <li><b>gzip / zstd / lz4</b> ({@code STREAM_ONLY_COMPRESSED}): routes through
     *       {@link org.elasticsearch.xpack.esql.datasources.StreamingParallelParsingCoordinator}. The producer
     *       thread only polls the result queue, so {@code read_nanos} is near-zero; {@code read_cpu_nanos}
     *       captures the real CPU via the coordinator's background-thread accumulator.</li>
     * </ul>
     */
    public void testMetricsCompression() throws IOException {
        // bzip2: SPLITTABLE_OR_INDEXED_COMPRESSED — producer thread does the full parse, so read_nanos is populated
        assertCompressedDataset("bz2", AbstractExternalDataSourceIT::writeBzip2, true);
        // gzip/zstd/lz4: STREAM_ONLY_COMPRESSED — producer only polls the streaming coordinator queue, read_nanos is near-zero
        assertCompressedDataset("gz", AbstractExternalDataSourceIT::writeGzipped, false);
        assertCompressedDataset("zst", AbstractExternalDataSourceIT::writeZstd, false);
        assertCompressedDataset("lz4", AbstractExternalDataSourceIT::writeLz4, false);
    }

    /**
     * Bracket multi-value CSV — exercises {@code SEGMENTABLE_UNCOMPRESSED_SEQUENTIAL} via
     * {@link org.elasticsearch.xpack.esql.datasources.StreamingParallelParsingCoordinator} from an uncompressed input.
     * <p>
     * Default CSV has {@code quoting=true} (CsvRecordSplitter, supportsStridedProbing=false → SEQUENTIAL). Brackets mode
     * additionally sets {@code supportsProvenProbing()=false}, so the sequential dispatcher falls through to the streaming
     * coordinator instead of ParallelParsingCoordinator. This is the only uncompressed path through that coordinator.
     * <p>
     * Same as the gzip tests, {@code read_nanos} is near-zero on the producer thread; only {@code read_cpu_nanos} is asserted.
     */
    public void testMetricsCollectorBracketsCsv() throws Exception {
        Path dir = createTempDir();
        StringBuilder csv = new StringBuilder("id:integer,tags:keyword\n");
        for (int i = 0; i < 100; i++) {
            csv.append(i).append(",[tag_a_").append(i).append(",tag_b_").append(i).append("]\n");
        }
        Files.writeString(dir.resolve("data.csv"), csv.toString());

        registerDataset("metrics_brackets_csv_ds", dir.resolve("data.csv").toUri().toString(), Map.of("multi_value_syntax", "brackets"));

        try (var ignored = run(syncEsqlQueryRequest("FROM metrics_brackets_csv_ds | LIMIT 200"), TIMEOUT)) {}

        assertThat("brackets-csv: metrics must be set", lastMetrics, notNullValue());
        assertThat("brackets-csv: read_cpu_nanos > 0", lastMetrics.get(QueryMetricsListener.READ_CPU_NANOS), greaterThan(0L));
    }

    private static String createNdjson(int x) {
        StringBuilder ndjson = new StringBuilder();
        for (int i = 0; i < x; i++) {
            ndjson.append("{\"emp_no\":").append(i).append(",\"name\":\"name_").append(i).append("\"}\n");
        }
        return ndjson.toString();
    }

    private static String createCsv(int x) {
        StringBuilder csv = new StringBuilder("emp_no:integer,name:keyword\n");
        for (int i = 0; i < x; i++) {
            csv.append(i).append(",name_").append(i).append('\n');
        }
        return csv.toString();
    }

    /** Parquet file — exercises the Parquet format reader path. */
    public void testMetricsCollectorParquet() throws Exception {
        Path dir = createTempDir();
        writeParquet(dir.resolve("data.parquet"), 100, 1024);

        registerDataset("metrics_parquet_ds", dir.resolve("data.parquet").toUri().toString(), Map.of());

        try (var ignored = run(syncEsqlQueryRequest("FROM metrics_parquet_ds | LIMIT 200"), TIMEOUT)) {}

        assertReadCpuNanos("parquet");
    }

    public void testNoCollectionWithoutExternalData() throws Exception {
        try (var ignored = run(syncEsqlQueryRequest("ROW a=1 | LIMIT 10"), TIMEOUT)) {
            // run the query — result discarded, only the collector side-effect matters
        }

        assertThat(lastMetrics, nullValue());

        createIndex("test");
        try (var ignored = run(syncEsqlQueryRequest("FROM test | LIMIT 10"), TIMEOUT)) {
            // run the query — result discarded, only the collector side-effect matters
        }

        assertThat(lastMetrics, nullValue());
    }

    public void testWarmAggregate() throws Exception {
        assumeTrue("requires local filesystem feature flag", HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());

        Path dir = createTempDir();
        StringBuilder csv = new StringBuilder("emp_no:integer,salary:long\n");
        for (int i = 0; i < 5; i++) {
            csv.append(i).append(",").append(50000 + i).append('\n');
        }
        Files.writeString(dir.resolve("data.csv"), csv.toString());

        String ds = registerDataset("warm_agg_ds", dir.resolve("data.csv").toUri().toString(), Map.of("format", "csv"));

        // Cold scan: populates the stats cache (row count, column stats).
        lastMetrics = null;
        try (var ignored = run(syncEsqlQueryRequest("FROM " + ds + " | LIMIT 200"), TIMEOUT)) {}
        assertThat("cold scan must be metered", lastMetrics, notNullValue());

        // Warm aggregate: answered from cached stats — splitsScanned stays 0,
        // so the gate in hasExternalSources() does not fire even though the
        // external datasource drove the result.
        lastMetrics = null;
        try (var ignored = run(syncEsqlQueryRequest("FROM " + ds + " | STATS COUNT(*)"), TIMEOUT)) {}
        assertThat("warm COUNT(*) over external source must be metered", lastMetrics, notNullValue());
    }

    /** Asserts that {@code read_cpu_nanos} is populated and does not exceed {@code read_nanos}. */
    private void assertReadCpuNanos(String format) {
        assertThat(format + ": metrics must be set", lastMetrics, notNullValue());
        assertThat(format + ": read_nanos > 0", lastMetrics.get(QueryMetricsListener.READ_NANOS), greaterThan(0L));
        assertThat(format + ": read_cpu_nanos > 0", lastMetrics.get(QueryMetricsListener.READ_CPU_NANOS), greaterThan(0L));
        assertThat(
            format + ": read_cpu_nanos <= read_nanos",
            lastMetrics.get(QueryMetricsListener.READ_NANOS),
            greaterThanOrEqualTo(lastMetrics.get(QueryMetricsListener.READ_CPU_NANOS))
        );
    }
}
