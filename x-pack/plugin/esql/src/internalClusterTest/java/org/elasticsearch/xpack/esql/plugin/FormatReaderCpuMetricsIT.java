/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.Constants;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.esql.QueryMetricsListener;
import org.elasticsearch.xpack.esql.action.AbstractExternalDataSourceIT;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
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
import static org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry.GA_FORMAT_READERS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Verifies that every GA format reader produces non-zero {@code read_cpu_nanos} when queried.
 *
 * <p>The {@link #COVERED} map is the registry of tested formats. Each entry pairs a format name with
 * a {@link FormatTestCase} that knows how to write sample data and which dataset settings to use.
 * Individual {@code testReadCpuNanos<Format>()} methods call {@link #runAndAssert} for their format.
 *
 * <p>The completeness guard {@link #testAllGaFormatReadersAreCovered()} asserts that
 * {@code COVERED.keySet()} exactly matches {@link FormatReaderRegistry#GA_FORMAT_READERS}.
 * {@code GA_FORMAT_READERS} is a hard runtime prerequisite for shipping a format on release builds
 * (the node refuses to register an unlisted format). When a new format is added there, this test
 * fails until:
 * <ol>
 *   <li>a {@link FormatTestCase} entry is added to {@link #COVERED} with a write helper, and</li>
 *   <li>a {@code testReadCpuNanos<Format>()} method is added that calls {@link #runAndAssert}.</li>
 * </ol>
 *
 * NOTE: this test does not verify the format accounting of read CPU time is correct. It only serves as a reminder
 * that this work needs to be done when the format is released to GA, and other tests may be necessary to actually verify
 * the correctness of the accounting. This especially applies to formats which use off-thread processing which needs separate
 * accounting.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class FormatReaderCpuMetricsIT extends AbstractExternalDataSourceIT {

    static volatile Map<String, Long> lastMetrics;

    /** Captures {@link QueryMetricsListener} callbacks into {@link #lastMetrics} for assertion. */
    public static final class CapturingCollector implements QueryMetricsListener {
        @Override
        public void onQueryCompleted(Map<String, Long> metrics) {
            FormatReaderCpuMetricsIT.lastMetrics = metrics;
        }
    }

    /**
     * Drops in for {@link AbstractExternalDataSourceIT.EsqlEnterpriseWithDatasourceExtensions} and additionally
     * loads {@link QueryMetricsListener} extensions via {@link EsqlPlugin#loadMetricsCollectors}.
     */
    public static final class EsqlEnterpriseWithCollector extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            loadMetricsCollectors(loader);
        }
    }

    /** Writes sample data for a format into a temporary directory. */
    @FunctionalInterface
    interface DataWriter {
        void write(Path dir) throws IOException;
    }

    /**
     * Test specification for a single format reader.
     *
     * @param writer   writes the sample data file into the given directory as {@code data.<formatName>}
     * @param settings additional dataset settings (e.g. {@code "format"} override when extension detection is insufficient)
     */
    record FormatTestCase(DataWriter writer, Map<String, Object> settings) {}

    /**
     * Registry of GA format readers with their test setup. {@code keySet()} must equal
     * {@link FormatReaderRegistry#GA_FORMAT_READERS} — enforced by {@link #testAllGaFormatReadersAreCovered()}.
     * Add a new entry here (plus a {@code testReadCpuNanos<Format>()} method) when promoting a format to GA.
     */
    private static final Map<String, FormatTestCase> COVERED = Map.of(
        "csv",
        new FormatTestCase(dir -> Files.writeString(dir.resolve("data.csv"), createCsv(5)), Map.of("format", "csv")),
        "tsv",
        new FormatTestCase(dir -> Files.writeString(dir.resolve("data.tsv"), createTsv(5)), Map.of("format", "tsv")),
        "ndjson",
        new FormatTestCase(dir -> Files.writeString(dir.resolve("data.ndjson"), createNdjson(5)), Map.of()),
        "parquet",
        new FormatTestCase(dir -> writeParquet(dir.resolve("data.parquet"), 100, 1024), Map.of())
    );

    @Before
    protected void checkLocalSources() {
        assumeTrue("requires local filesystem feature flag", HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());
    }

    @After
    protected void clearLastMetrics() {
        lastMetrics = null;
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, NdJsonDataSourcePlugin.class, ParquetDataSourcePlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(EsqlEnterpriseWithCollector.class);
        return plugins;
    }

    /**
     * Completeness guard: {@link #COVERED} must exactly match {@link FormatReaderRegistry#GA_FORMAT_READERS}.
     * Fails when a format is added to {@code GA_FORMAT_READERS} (required to ship it on release builds)
     * without adding coverage here.
     */
    public void testAllGaFormatReadersAreCovered() {
        assertThat(
            "COVERED must match FormatReaderRegistry.GA_FORMAT_READERS — "
                + "add a FormatTestCase entry to COVERED and a testReadCpuNanos<Format>() method when promoting a format to GA",
            COVERED.keySet(),
            equalTo(GA_FORMAT_READERS)
        );
    }

    public void testReadCpuNanosCsv() throws Exception {
        runAndAssert("csv");
    }

    public void testReadCpuNanosTsv() throws Exception {
        runAndAssert("tsv");
    }

    public void testReadCpuNanosNdjson() throws Exception {
        runAndAssert("ndjson");
    }

    public void testReadCpuNanosParquet() throws Exception {
        runAndAssert("parquet");
    }

    /**
     * Writes sample data for {@code format}, registers a dataset, runs a query, and asserts
     * that both {@code read_nanos} and {@code read_cpu_nanos} are positive.
     */
    private void runAndAssert(String format) throws Exception {
        assumeFalse("Windows has bad timer resolution, metrics are not accurate", Constants.WINDOWS);
        FormatTestCase spec = COVERED.get(format);
        Path dir = createTempDir();
        spec.writer().write(dir);
        registerDataset("cpu_nanos_" + format + "_ds", dir.resolve("data." + format).toUri().toString(), spec.settings());
        try (var ignored = run(syncEsqlQueryRequest("FROM cpu_nanos_" + format + "_ds | LIMIT 10"), TIMEOUT)) {}
        assertThat(format + ": metrics must be set", lastMetrics, notNullValue());
        assertThat(format + ": read_nanos > 0", lastMetrics.get(QueryMetricsListener.READ_NANOS), greaterThan(0L));
        assertThat(format + ": read_cpu_nanos > 0", lastMetrics.get(QueryMetricsListener.READ_CPU_NANOS), greaterThan(0L));
    }

    private static String createCsv(int rows) {
        StringBuilder csv = new StringBuilder("emp_no:integer,name:keyword\n");
        for (int i = 0; i < rows; i++) {
            csv.append(i).append(",name_").append(i).append('\n');
        }
        return csv.toString();
    }

    private static String createTsv(int rows) {
        StringBuilder tsv = new StringBuilder("emp_no:integer\tname:keyword\n");
        for (int i = 0; i < rows; i++) {
            tsv.append(i).append('\t').append("name_").append(i).append('\n');
        }
        return tsv.toString();
    }

    private static String createNdjson(int rows) {
        StringBuilder ndjson = new StringBuilder();
        for (int i = 0; i < rows; i++) {
            ndjson.append("{\"emp_no\":").append(i).append(",\"name\":\"name_").append(i).append("\"}\n");
        }
        return ndjson.toString();
    }
}
