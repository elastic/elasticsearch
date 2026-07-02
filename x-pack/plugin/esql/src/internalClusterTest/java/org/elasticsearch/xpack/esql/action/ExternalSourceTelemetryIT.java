/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * End-to-end integration for the ES|QL external-data-source operational metrics
 * ({@link ExternalSourceMetrics}, the {@code es.esql.datasources.*} family). This is the
 * {@code TestTelemetryPlugin}-on-a-real-cluster precedent ({@code S3BlobStoreRepositoryMetricsTests} /
 * {@code SnapshotMetricsIT} / {@code TelemetryIT}) that the holder-unit coverage lacks: it runs a REAL
 * external scan of a local CSV fixture via {@code FROM <dataset>} on a real cluster and asserts the whole
 * metric pipeline actually fires with the expected values and attributes.
 *
 * <p>It merges two sibling patterns: {@link FromDatasetIT} (register a data source + a file-backed dataset
 * through the CRUD API, then run {@code FROM <dataset>}) and {@link TelemetryIT} (add
 * {@link TestTelemetryPlugin} to {@code nodePlugins()}, pull each node's plugin via {@link PluginsService},
 * and read {@link Measurement}s). The DataSourceModule wires {@link ExternalSourceMetrics} to
 * {@code services.telemetryProvider().getMeterRegistry()}, which is the {@link TestTelemetryPlugin}'s
 * recording registry, so the production {@code recordX} call sites land in the recorder.
 *
 * <p>The metric families fire on different nodes — storage/parse on the data node, discovery/query at the
 * coordinator — so every assertion aggregates each node's {@link TestTelemetryPlugin} measurements. The
 * cluster is single-node (like {@link FromDatasetIT}) so coordinator and data node coincide and the values
 * are deterministic, but the aggregation helpers are node-count agnostic and hold for a larger cluster.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ExternalSourceTelemetryIT extends AbstractEsqlIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    /** Minimal pass-through validator registered for type {@code test}; accepts any resource scheme (mirrors {@link FromDatasetIT}). */
    public static final class TestDataSourcePlugin extends Plugin implements DataSourcePlugin {
        @Override
        public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
            return Map.of("test", new TestValidator());
        }
    }

    private static final class TestValidator implements DataSourceValidator {
        @Override
        public String type() {
            return "test";
        }

        @Override
        public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings) {
            Map<String, DataSourceSetting> out = new HashMap<>();
            for (Map.Entry<String, Object> e : datasourceSettings.entrySet()) {
                out.put(e.getKey(), new DataSourceSetting(e.getValue(), e.getKey().startsWith("secret_")));
            }
            return out;
        }

        @Override
        public Map<String, Object> validateDataset(
            Map<String, DataSourceSetting> datasourceSettings,
            String resource,
            Map<String, Object> datasetSettings
        ) {
            return datasetSettings == null ? Map.of() : new HashMap<>(datasetSettings);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(HttpDataSourcePlugin.class);
        plugins.add(CsvDataSourcePlugin.class);
        plugins.add(TestDataSourcePlugin.class);
        plugins.add(TestTelemetryPlugin.class);
        return plugins;
    }

    /** Determinism over planner-regression diversity: a plain scan, no random pragmas, so parse.rows is exact. */
    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    @Before
    public void requireFeatureFlag() {
        assumeTrue("requires external data sources feature flag", DatasetMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled());
    }

    /** SUITE-scoped cluster: names every dataset/data source a test body PUTs so {@link #cleanup} can drop them between methods. */
    private static final Set<String> CREATED_DATASETS = Set.of("emp_glob", "emp_missing");

    @After
    public void cleanup() throws Exception {
        for (String ds : CREATED_DATASETS) {
            try {
                client().execute(DeleteDatasetAction.INSTANCE, new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { ds }))
                    .get(30, TimeUnit.SECONDS);
            } catch (ResourceNotFoundException ignored) {
                // never created by this method
            } catch (Exception e) {
                logger.warn("dataset cleanup [{}] failed", ds, e);
            }
        }
        try {
            client().execute(DeleteDataSourceAction.INSTANCE, new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { "ds" }))
                .get(30, TimeUnit.SECONDS);
        } catch (ResourceNotFoundException ignored) {
            // never created by this method
        } catch (Exception e) {
            logger.warn("data source cleanup [ds] failed", e);
        }
    }

    /**
     * A real external scan proves the whole pipeline metric family fires with the expected values/attributes.
     * A two-file glob dataset is used deliberately: discovery ({@code discovery.*}) is only recorded on the
     * multi-file resolution path (single-file resolution skips glob listing), so the glob is what lets the
     * discovery instruments be asserted end-to-end. The fixture has 10 rows total (6 + 4) across two files.
     */
    public void testExternalScanFiresPipelineMetrics() throws Exception {
        Path dir = createTempDir();
        // Two CSV files, identical schema, 6 + 4 = 10 data rows total.
        StringBuilder f1 = new StringBuilder("emp_no:integer,first_name:keyword\n");
        for (int i = 0; i < 6; i++) {
            f1.append(i).append(",name_").append(i).append('\n');
        }
        StringBuilder f2 = new StringBuilder("emp_no:integer,first_name:keyword\n");
        for (int i = 6; i < 10; i++) {
            f2.append(i).append(",name_").append(i).append('\n');
        }
        Files.writeString(dir.resolve("part-1.csv"), f1.toString());
        Files.writeString(dir.resolve("part-2.csv"), f2.toString());

        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, "ds", "test", null, new HashMap<>())
            )
        );
        // Glob resource so resolution takes the multi-file discovery path.
        String globResource = dir.toUri() + "*.csv";
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(TIMEOUT, TIMEOUT, "emp_glob", "ds", globResource, null, new HashMap<>(Map.of("format", "csv")))
            )
        );

        // Isolate: only this query's measurements should be present on any node.
        resetAllMeters();

        int returnedRows;
        try (var response = run(syncEsqlQueryRequest("FROM emp_glob | SORT emp_no"), TIMEOUT)) {
            returnedRows = getValuesList(response).size();
        }
        assertThat("the scan must return every fixture row", returnedRows, equalTo(10));

        collectAllMeters();

        // --- discovery (coordinator), multi-file listing path ---
        Measurement filesScanned = singleForScheme(histograms(ExternalSourceMetrics.DISCOVERY_FILES_SCANNED), "file");
        assertThat("discovery.files_scanned must record the two-file listing", filesScanned.getLong(), equalTo(2L));
        Measurement bytesScanned = singleForScheme(histograms(ExternalSourceMetrics.DISCOVERY_BYTES_SCANNED), "file");
        assertThat("discovery.bytes_scanned must be positive", bytesScanned.getLong(), greaterThan(0L));
        assertThat(
            "discovery.duration must be recorded (value may be sub-ms)",
            forScheme(histograms(ExternalSourceMetrics.DISCOVERY_DURATION), "file"),
            not(hasSize(0))
        );

        // --- parse (data node): exactly the rows the scan produced ---
        assertThat(
            "parse.rows.total must equal the number of rows scanned",
            counterTotalForScheme(ExternalSourceMetrics.PARSE_ROWS_TOTAL, "file"),
            equalTo(10L)
        );

        // --- query level (coordinator): exactly one successful external-source query ---
        assertThat(
            "queries.total{outcome=success} must be exactly 1",
            counterTotalForOutcome(ExternalSourceMetrics.QUERIES_TOTAL, ExternalSourceMetrics.OUTCOME_SUCCESS),
            equalTo(1L)
        );
        assertThat(
            "queries.total{outcome=failure} must not fire for a successful query",
            counterTotalForOutcome(ExternalSourceMetrics.QUERIES_TOTAL, ExternalSourceMetrics.OUTCOME_FAILURE),
            equalTo(0L)
        );
        assertThat(
            "query.duration must be recorded for the success outcome",
            forOutcome(histograms(ExternalSourceMetrics.QUERY_DURATION), ExternalSourceMetrics.OUTCOME_SUCCESS),
            not(hasSize(0))
        );
        // A clean success trips neither the discovery-failure nor the breaker counter.
        assertThat("no discovery failures on a clean scan", counterTotal(ExternalSourceMetrics.DISCOVERY_FAILURES_TOTAL), equalTo(0L));

        // storage read layer (data node), tagged with the canonical file scheme
        assertThat(
            "storage.requests.total must fire for the file scheme",
            counterTotalForScheme(ExternalSourceMetrics.STORAGE_REQUESTS_TOTAL, "file"),
            greaterThan(0L)
        );
        assertThat(
            "storage.bytes_read.total must fire for the file scheme",
            counterTotalForScheme(ExternalSourceMetrics.STORAGE_BYTES_READ_TOTAL, "file"),
            greaterThan(0L)
        );
    }

    /**
     * A real discovery failure proves the failure-side counter fires. The dataset points at a single
     * nonexistent file, so resolution throws while listing/reading it, the resolver records
     * {@code discovery.failures.total}, and the query surfaces the error. Because resolution fails during
     * analysis — before the analyzed plan carries the external-source flag — the coordinator's
     * {@code queries.total{outcome=failure}} is intentionally NOT bumped (that counter only covers queries
     * that reached execution with a resolved external source; see {@link ExternalSourceMetrics#QUERIES_TOTAL}).
     * The discovery-side counter is what owns a resolution failure, and that is what this asserts.
     */
    public void testFailingExternalScanFiresDiscoveryFailure() throws Exception {
        Path dir = createTempDir();
        // A concrete single-file resource that does not exist: resolution opens it and throws.
        String missingResource = dir.resolve("does-not-exist.csv").toUri().toString();

        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, "ds", "test", null, new HashMap<>())
            )
        );
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "emp_missing",
                    "ds",
                    missingResource,
                    null,
                    new HashMap<>(Map.of("format", "csv"))
                )
            )
        );

        resetAllMeters();

        expectThrows(Exception.class, () -> {
            try (var ignored = run(syncEsqlQueryRequest("FROM emp_missing | LIMIT 10"), TIMEOUT)) {
                // resolution of the missing file must fail before any rows are produced
            }
        });

        collectAllMeters();

        assertThat(
            "discovery.failures.total must fire when resolution of a missing file fails",
            counterTotal(ExternalSourceMetrics.DISCOVERY_FAILURES_TOTAL),
            greaterThanOrEqualTo(1L)
        );
        // The resolution never reached execution with a resolved external source, so the coordinator's
        // per-query success counter must stay untouched.
        assertThat(
            "a resolution failure must not record a successful query",
            counterTotalForOutcome(ExternalSourceMetrics.QUERIES_TOTAL, ExternalSourceMetrics.OUTCOME_SUCCESS),
            equalTo(0L)
        );
    }

    // ---- cross-node measurement helpers ----

    private List<TestTelemetryPlugin> telemetryPlugins(String node) {
        return internalCluster().getInstance(PluginsService.class, node).filterPlugins(TestTelemetryPlugin.class).toList();
    }

    private void resetAllMeters() {
        for (String node : internalCluster().getNodeNames()) {
            for (TestTelemetryPlugin plugin : telemetryPlugins(node)) {
                plugin.resetMeter();
            }
        }
    }

    private void collectAllMeters() {
        for (String node : internalCluster().getNodeNames()) {
            for (TestTelemetryPlugin plugin : telemetryPlugins(node)) {
                plugin.collect();
            }
        }
    }

    private List<Measurement> counters(String name) {
        List<Measurement> all = new ArrayList<>();
        for (String node : internalCluster().getNodeNames()) {
            for (TestTelemetryPlugin plugin : telemetryPlugins(node)) {
                all.addAll(plugin.getLongCounterMeasurement(name));
            }
        }
        return all;
    }

    private List<Measurement> histograms(String name) {
        List<Measurement> all = new ArrayList<>();
        for (String node : internalCluster().getNodeNames()) {
            for (TestTelemetryPlugin plugin : telemetryPlugins(node)) {
                all.addAll(plugin.getLongHistogramMeasurement(name));
            }
        }
        return all;
    }

    private long counterTotal(String name) {
        return counters(name).stream().mapToLong(Measurement::getLong).sum();
    }

    private long counterTotalForScheme(String name, String scheme) {
        return counters(name).stream()
            .filter(m -> scheme.equals(m.attributes().get(ExternalSourceMetrics.SCHEME_ATTRIBUTE)))
            .mapToLong(Measurement::getLong)
            .sum();
    }

    private long counterTotalForOutcome(String name, String outcome) {
        return counters(name).stream()
            .filter(m -> outcome.equals(m.attributes().get(ExternalSourceMetrics.OUTCOME_ATTRIBUTE)))
            .mapToLong(Measurement::getLong)
            .sum();
    }

    private static List<Measurement> forScheme(List<Measurement> measurements, String scheme) {
        return measurements.stream().filter(m -> scheme.equals(m.attributes().get(ExternalSourceMetrics.SCHEME_ATTRIBUTE))).toList();
    }

    private static List<Measurement> forOutcome(List<Measurement> measurements, String outcome) {
        return measurements.stream().filter(m -> outcome.equals(m.attributes().get(ExternalSourceMetrics.OUTCOME_ATTRIBUTE))).toList();
    }

    private static Measurement singleForScheme(List<Measurement> measurements, String scheme) {
        List<Measurement> found = forScheme(measurements, scheme);
        assertThat("expected exactly one measurement for scheme [" + scheme + "]", found, hasSize(1));
        return found.get(0);
    }
}
