/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.gzip.GzipDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DataSourceService;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceTelemetryVocabulary.Type;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceUsageAccumulator;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;
import java.util.zip.GZIPOutputStream;

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
        plugins.add(GzipDataSourcePlugin.class);
        plugins.add(TestDataSourcePlugin.class);
        plugins.add(TestTelemetryPlugin.class);
        return plugins;
    }

    /** Determinism over planner-regression diversity: a plain scan, no random pragmas, so parse.rows is exact. */
    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .putList(ExternalSourceSettings.LOCAL_ALLOWED_PATHS.getKey(), createTempDir().getParent().toString())
            .build();
    }

    @Before
    public void requireFeatureFlag() {
        assumeTrue("requires dataset-in-from-command capability", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        assumeTrue("requires local filesystem feature flag", HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());
    }

    /** SUITE-scoped cluster: names every dataset/data source a test body PUTs so {@link #cleanup} can drop them between methods. */
    private static final Set<String> CREATED_DATASETS = Set.of("emp_glob", "emp_missing", "emp_gz", "emp_crud", "emp_dep", "emp_iae");
    private static final Set<String> CREATED_DATASOURCES = Set.of("ds", "ds_crud", "ds_max", "ds_dep", "ds_iae");

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
        for (String name : CREATED_DATASOURCES) {
            try {
                client().execute(
                    DeleteDataSourceAction.INSTANCE,
                    new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { name })
                ).get(30, TimeUnit.SECONDS);
            } catch (ResourceNotFoundException ignored) {
                // never created by this method
            } catch (Exception e) {
                logger.warn("data source cleanup [{}] failed", name, e);
            }
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

        // Snapshot accumulator before the query so assertions use deltas (SUITE-scoped cluster).
        long parseRowsBefore = clusterTotal(DataSourceUsageAccumulator::parseRows);
        long parseRowsCsvBefore = clusterTotal(a -> a.parseRowsByFormat(DataSourceUsageAccumulator.FORMAT_CSV));
        long storageRequestsBefore = clusterTotal(a -> a.storageRequests(Type.LOCAL));
        long storageBytesReadBefore = clusterTotal(a -> a.storageBytesRead(Type.LOCAL));
        long queriesSuccessBefore = clusterTotal(a -> a.queries(DataSourceUsageAccumulator.OUTCOME_SUCCESS));
        long queriesFailureBefore = clusterTotal(a -> a.queries(DataSourceUsageAccumulator.OUTCOME_FAILURE));
        long filesScannedBucketBefore = clusterTotal(a -> a.discoveryFilesScanned(1));
        long discoveryFailuresBefore = clusterTotal(DataSourceUsageAccumulator::discoveryFailures);

        // Isolate: only this query's measurements should be present on any node.
        resetAllMeters();

        int returnedRows;
        try (var response = run(syncEsqlQueryRequest("FROM emp_glob | SORT emp_no"), TIMEOUT)) {
            returnedRows = getValuesList(response).size();
        }
        assertThat("the scan must return every fixture row", returnedRows, equalTo(10));

        collectAllMeters();
        assertNoSchemeAttribute();

        // --- discovery (coordinator), multi-file listing path ---
        Measurement filesScanned = singleForType(histograms(ExternalSourceMetrics.DISCOVERY_FILES_SCANNED), "local");
        assertThat("discovery.files_scanned must record the two-file listing", filesScanned.getLong(), equalTo(2L));
        Measurement bytesScanned = singleForType(histograms(ExternalSourceMetrics.DISCOVERY_BYTES_SCANNED), "local");
        assertThat("discovery.bytes_scanned must be positive", bytesScanned.getLong(), greaterThan(0L));
        assertThat(
            "discovery.duration must be recorded (value may be sub-ms)",
            forType(histograms(ExternalSourceMetrics.DISCOVERY_DURATION), "local"),
            not(hasSize(0))
        );

        // --- parse (data node): exactly the rows the scan produced, tagged type=local (file folded) and format=csv ---
        assertThat(
            "parse.rows.total must equal the number of rows scanned with {type=local, format=csv}",
            counterTotalForTypeAndFormat(ExternalSourceMetrics.PARSE_ROWS_TOTAL, "local", "csv"),
            equalTo(10L)
        );
        assertThat(
            "parse.duration must be recorded with {type=local, format=csv}",
            forTypeAndFormat(histograms(ExternalSourceMetrics.PARSE_DURATION), "local", "csv"),
            not(hasSize(0))
        );
        assertThat(
            "parse.splits_scanned must be recorded with {type=local, format=csv}",
            forTypeAndFormat(histograms(ExternalSourceMetrics.PARSE_SPLITS_SCANNED), "local", "csv"),
            not(hasSize(0))
        );
        assertThat(
            "time_to_first_row must be recorded with {type=local, format=csv}",
            forTypeAndFormat(histograms(ExternalSourceMetrics.QUERY_TIME_TO_FIRST_ROW), "local", "csv"),
            not(hasSize(0))
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

        // storage read layer (data node), tagged with the canonical local type (file:// folded)
        assertThat(
            "storage.requests.total must fire for type=local (file folded)",
            counterTotalForType(ExternalSourceMetrics.STORAGE_REQUESTS_TOTAL, "local"),
            greaterThan(0L)
        );
        assertThat(
            "storage.bytes_read.total must fire for type=local (file folded)",
            counterTotalForType(ExternalSourceMetrics.STORAGE_BYTES_READ_TOTAL, "local"),
            greaterThan(0L)
        );

        // ---- phone-home accumulator (DataSourceUsageAccumulator) — delta assertions ----
        // The accumulator is never reset (it is a lifetime counter), so compare fresh readings
        // against the before-values captured above.
        assertThat(
            "phone-home: parse.rows must increase by 10",
            clusterTotal(DataSourceUsageAccumulator::parseRows) - parseRowsBefore,
            equalTo(10L)
        );
        assertThat(
            "phone-home: parse.rows.by_format.csv must increase by 10",
            clusterTotal(a -> a.parseRowsByFormat(DataSourceUsageAccumulator.FORMAT_CSV)) - parseRowsCsvBefore,
            equalTo(10L)
        );
        assertThat(
            "phone-home: storage.requests (local type) must fire",
            clusterTotal(a -> a.storageRequests(Type.LOCAL)) - storageRequestsBefore,
            greaterThan(0L)
        );
        assertThat(
            "phone-home: storage.bytes_read (local type) must fire",
            clusterTotal(a -> a.storageBytesRead(Type.LOCAL)) - storageBytesReadBefore,
            greaterThan(0L)
        );
        assertThat(
            "phone-home: queries.total (success outcome) must increase by 1",
            clusterTotal(a -> a.queries(DataSourceUsageAccumulator.OUTCOME_SUCCESS)) - queriesSuccessBefore,
            equalTo(1L)
        );
        assertThat(
            "phone-home: queries.total (failure outcome) must not fire for a successful query",
            clusterTotal(a -> a.queries(DataSourceUsageAccumulator.OUTCOME_FAILURE)) - queriesFailureBefore,
            equalTo(0L)
        );
        assertThat(
            "phone-home: discovery.files_scanned bucket for 2 files must be populated",
            clusterTotal(a -> a.discoveryFilesScanned(1)) - filesScannedBucketBefore,  // COUNT_THRESHOLDS[1]=10, so 2 files → bucket 1
                                                                                       // (lt_10)
            greaterThan(0L)
        );
        assertThat(
            "phone-home: no discovery failures on a clean scan",
            clusterTotal(DataSourceUsageAccumulator::discoveryFailures) - discoveryFailuresBefore,
            equalTo(0L)
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

        // Snapshot before so delta assertions are order-independent.
        long discoveryFailuresBefore = clusterTotal(DataSourceUsageAccumulator::discoveryFailures);
        long queriesSuccessBefore = clusterTotal(a -> a.queries(DataSourceUsageAccumulator.OUTCOME_SUCCESS));

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

        // ---- phone-home accumulator — delta assertions ----
        assertThat(
            "phone-home: discovery.failures must increase when resolution of a missing file fails",
            clusterTotal(DataSourceUsageAccumulator::discoveryFailures) - discoveryFailuresBefore,
            greaterThanOrEqualTo(1L)
        );
        assertThat(
            "phone-home: queries.total (success) must not increase for a resolution failure",
            clusterTotal(a -> a.queries(DataSourceUsageAccumulator.OUTCOME_SUCCESS)) - queriesSuccessBefore,
            equalTo(0L)
        );
    }

    /**
     * A {@code local} dataset whose format is inferred from a compound {@code .csv.gz} extension (no
     * explicit {@code format}) must be accepted by production CRUD and readable on the query path.
     * PUT includes a CSV-only key ({@code header_row}) so registration fails unless
     * {@code FileDataSourceValidator} delegates extension inference to the live registry.
     *
     * <p>APM inventory gauges, phone-home inventory keys, and the query-path format dimension land
     * in #1866 / #1868 and are not on this branch. This test asserts the surfaces that exist:
     * CRUD acceptance, a successful scan, {@code PARSE_ROWS_TOTAL} on scheme {@code local}, and
     * that the coordinator registry resolves the same object as {@code csv}.
     */
    public void testInferredCompoundExtensionDatasetIsQueryable() throws Exception {
        Path gz = createTempDir().resolve("data.csv.gz");
        byte[] csv = "emp_no:integer,first_name:keyword\n1,ann\n".getBytes(StandardCharsets.UTF_8);
        try (GZIPOutputStream out = new GZIPOutputStream(Files.newOutputStream(gz))) {
            out.write(csv);
        }
        String resource = gz.toUri().toString();

        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, "ds", "local", null, new HashMap<>())
            )
        );
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(TIMEOUT, TIMEOUT, "emp_gz", "ds", resource, null, new HashMap<>(Map.of("header_row", true)))
            )
        );

        resetAllMeters();
        int returnedRows;
        try (var response = run(syncEsqlQueryRequest("FROM emp_gz"), TIMEOUT)) {
            returnedRows = getValuesList(response).size();
        }
        assertThat("the scan must return the gzipped fixture row", returnedRows, equalTo(1));

        collectAllMeters();
        assertThat(
            "parse.rows.total must fire for the local scheme",
            counterTotalForScheme(ExternalSourceMetrics.PARSE_ROWS_TOTAL, "local"),
            equalTo(1L)
        );

        FormatReaderRegistry registry = null;
        for (String node : internalCluster().getNodeNames()) {
            PlanExecutor planExecutor = internalCluster().getInstance(PlanExecutor.class, node);
            if (planExecutor.dataSourceModule() != null) {
                registry = planExecutor.dataSourceModule().formatReaderRegistry();
                break;
            }
        }
        assertNotNull("no node has a DataSourceModule format reader registry", registry);
        assertThat(
            "query-path registry must infer csv from the compound extension",
            FormatNameResolver.resolveFormatName(null, resource, registry),
            equalTo("csv")
        );
    }

    /**
     * Dedicated CRUD telemetry: created for both kinds, updated after a changed-description PUT,
     * nothing after an identical dataset PUT, deleted after an in-body delete, and exactly one
     * rejected for an unknown-type PUT. Uses deltas because the accumulator is never reset.
     */
    public void testConfigChangesRecordCreatedUpdatedDeletedRejected() throws Exception {
        Path dir = createTempDir();
        Files.writeString(dir.resolve("part.csv"), "emp_no:integer\n1\n");
        String resource = dir.resolve("part.csv").toUri().toString();

        long dsCreatedBefore = clusterTotal(
            a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASOURCE, DataSourceUsageAccumulator.OP_CREATED)
        );
        long dsUpdatedBefore = clusterTotal(
            a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASOURCE, DataSourceUsageAccumulator.OP_UPDATED)
        );
        long dsDeletedBefore = clusterTotal(
            a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASOURCE, DataSourceUsageAccumulator.OP_DELETED)
        );
        long dsRejectedBefore = clusterTotal(
            a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASOURCE, DataSourceUsageAccumulator.OP_REJECTED)
        );
        long setCreatedBefore = clusterTotal(
            a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASET, DataSourceUsageAccumulator.OP_CREATED)
        );
        long setUpdatedBefore = clusterTotal(
            a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASET, DataSourceUsageAccumulator.OP_UPDATED)
        );
        long setDeletedBefore = clusterTotal(
            a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASET, DataSourceUsageAccumulator.OP_DELETED)
        );
        long setRejectedBefore = clusterTotal(
            a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASET, DataSourceUsageAccumulator.OP_REJECTED)
        );

        resetAllMeters();

        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, "ds_crud", "test", "first", new HashMap<>())
            )
        );
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "emp_crud",
                    "ds_crud",
                    resource,
                    "first",
                    new HashMap<>(Map.of("format", "csv"))
                )
            )
        );
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, "ds_crud", "test", "second", new HashMap<>())
            )
        );
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "emp_crud",
                    "ds_crud",
                    resource,
                    "second",
                    new HashMap<>(Map.of("format", "csv"))
                )
            )
        );
        // Identical dataset PUT is a documented no-op and must not record a change.
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "emp_crud",
                    "ds_crud",
                    resource,
                    "second",
                    new HashMap<>(Map.of("format", "csv"))
                )
            )
        );
        assertAcked(
            client().execute(DeleteDatasetAction.INSTANCE, new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { "emp_crud" }))
        );
        assertAcked(
            client().execute(
                DeleteDataSourceAction.INSTANCE,
                new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { "ds_crud" })
            )
        );
        expectThrows(
            Exception.class,
            () -> client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, "ds_crud", "no_such_type", null, new HashMap<>())
            ).actionGet(TIMEOUT)
        );

        collectAllMeters();

        assertThat(
            "datasource created",
            clusterTotal(a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASOURCE, DataSourceUsageAccumulator.OP_CREATED))
                - dsCreatedBefore,
            equalTo(1L)
        );
        assertThat(
            "datasource updated",
            clusterTotal(a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASOURCE, DataSourceUsageAccumulator.OP_UPDATED))
                - dsUpdatedBefore,
            equalTo(1L)
        );
        assertThat(
            "datasource deleted",
            clusterTotal(a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASOURCE, DataSourceUsageAccumulator.OP_DELETED))
                - dsDeletedBefore,
            equalTo(1L)
        );
        assertThat(
            "exactly one rejected unknown-type PUT",
            clusterTotal(a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASOURCE, DataSourceUsageAccumulator.OP_REJECTED))
                - dsRejectedBefore,
            equalTo(1L)
        );
        assertThat(
            "dataset created",
            clusterTotal(a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASET, DataSourceUsageAccumulator.OP_CREATED))
                - setCreatedBefore,
            equalTo(1L)
        );
        assertThat(
            "dataset updated",
            clusterTotal(a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASET, DataSourceUsageAccumulator.OP_UPDATED))
                - setUpdatedBefore,
            equalTo(1L)
        );
        assertThat(
            "dataset deleted",
            clusterTotal(a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASET, DataSourceUsageAccumulator.OP_DELETED))
                - setDeletedBefore,
            equalTo(1L)
        );
        assertThat(
            "dataset rejected must not fire",
            clusterTotal(a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASET, DataSourceUsageAccumulator.OP_REJECTED))
                - setRejectedBefore,
            equalTo(0L)
        );

        assertThat("APM datasource created", apmConfigChanges("datasource", "created"), equalTo(1L));
        assertThat("APM datasource updated", apmConfigChanges("datasource", "updated"), equalTo(1L));
        assertThat("APM datasource deleted", apmConfigChanges("datasource", "deleted"), equalTo(1L));
        assertThat("APM dataset created", apmConfigChanges("dataset", "created"), equalTo(1L));
        assertThat("APM dataset updated", apmConfigChanges("dataset", "updated"), equalTo(1L));
        assertThat("APM dataset deleted", apmConfigChanges("dataset", "deleted"), equalTo(1L));
        assertThat("APM rejected observations", apmConfigChanges(null, "rejected"), equalTo(1L));
        assertThat(
            "unknown type clamps to unknown",
            counters(ExternalSourceMetrics.CONFIG_CHANGES_TOTAL).stream()
                .anyMatch(
                    m -> "rejected".equals(m.attributes().get(ExternalSourceMetrics.OP_ATTRIBUTE))
                        && "unknown".equals(m.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE))
                        && "unknown_type".equals(m.attributes().get(ExternalSourceMetrics.REASON_ATTRIBUTE))
                ),
            equalTo(true)
        );
    }

    /**
     * Unknown-type PUT dies in the coord {@code doExecute} pre-check. Max-count is thrown from the
     * CAS task body and is the path that reaches {@code recordingListener.onFailure}.
     */
    public void testConfigChangesRecordMaxCountFromTaskBody() throws Exception {
        long rejectedBefore = clusterTotal(
            a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASOURCE, DataSourceUsageAccumulator.OP_REJECTED)
        );
        resetAllMeters();
        assertAcked(
            clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setPersistentSettings(Settings.builder().put(DataSourceService.MAX_DATA_SOURCES_COUNT_SETTING.getKey(), 0).build())
        );
        try {
            expectThrows(
                Exception.class,
                () -> client().execute(
                    PutDataSourceAction.INSTANCE,
                    new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, "ds_max", "test", null, new HashMap<>())
                ).actionGet(TIMEOUT)
            );
            collectAllMeters();
            assertThat(
                "phone-home max-count rejection",
                clusterTotal(a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASOURCE, DataSourceUsageAccumulator.OP_REJECTED))
                    - rejectedBefore,
                equalTo(1L)
            );
            assertThat(
                "APM max_count from task body",
                counters(ExternalSourceMetrics.CONFIG_CHANGES_TOTAL).stream()
                    .anyMatch(
                        m -> "rejected".equals(m.attributes().get(ExternalSourceMetrics.OP_ATTRIBUTE))
                            && "max_count".equals(m.attributes().get(ExternalSourceMetrics.REASON_ATTRIBUTE))
                    ),
                equalTo(true)
            );
        } finally {
            assertAcked(
                clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .setPersistentSettings(Settings.builder().putNull(DataSourceService.MAX_DATA_SOURCES_COUNT_SETTING.getKey()).build())
            );
        }
    }

    /**
     * {@link org.elasticsearch.xpack.esql.datasources.DeclaredSchemaValidator} throws leftover
     * {@link IllegalArgumentException}, not {@link org.elasticsearch.common.ValidationException}.
     * After the dedicated IAE subtypes, that path must still be {@code validation}.
     */
    public void testConfigChangesRecordLeftoverIaeAsValidation() throws Exception {
        Path dir = createTempDir();
        Files.writeString(dir.resolve("part.csv"), "emp_no:integer\n1\n");
        String resource = dir.resolve("part.csv").toUri().toString();
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, "ds_iae", "test", null, new HashMap<>())
            )
        );

        long rejectedBefore = clusterTotal(
            a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASET, DataSourceUsageAccumulator.OP_REJECTED)
        );
        resetAllMeters();
        Map<String, DatasetFieldMapping> properties = Map.of("loc", new DatasetFieldMapping("geo_point", null));
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
        expectThrows(
            Exception.class,
            () -> client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "emp_iae",
                    "ds_iae",
                    resource,
                    null,
                    new HashMap<>(Map.of("format", "csv")),
                    mapping
                )
            ).actionGet(TIMEOUT)
        );
        collectAllMeters();
        assertThat(
            "phone-home leftover IAE is validation",
            clusterTotal(a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASET, DataSourceUsageAccumulator.OP_REJECTED))
                - rejectedBefore,
            equalTo(1L)
        );
        assertThat(
            "APM leftover IAE is validation",
            counters(ExternalSourceMetrics.CONFIG_CHANGES_TOTAL).stream()
                .anyMatch(
                    m -> "rejected".equals(m.attributes().get(ExternalSourceMetrics.OP_ATTRIBUTE))
                        && "dataset".equals(m.attributes().get(ExternalSourceMetrics.KIND_ATTRIBUTE))
                        && "validation".equals(m.attributes().get(ExternalSourceMetrics.REASON_ATTRIBUTE))
                ),
            equalTo(true)
        );
    }

    /** Delete-with-dependents is the CAS path that used to swamp datasource {@code rejected} as {@code other}. */
    public void testConfigChangesRecordHasDependentsOnDelete() throws Exception {
        Path dir = createTempDir();
        Files.writeString(dir.resolve("part.csv"), "emp_no:integer\n1\n");
        String resource = dir.resolve("part.csv").toUri().toString();
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, "ds_dep", "test", null, new HashMap<>())
            )
        );
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(TIMEOUT, TIMEOUT, "emp_dep", "ds_dep", resource, null, new HashMap<>(Map.of("format", "csv")))
            )
        );

        long rejectedBefore = clusterTotal(
            a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASOURCE, DataSourceUsageAccumulator.OP_REJECTED)
        );
        resetAllMeters();
        expectThrows(
            Exception.class,
            () -> client().execute(
                DeleteDataSourceAction.INSTANCE,
                new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { "ds_dep" })
            ).actionGet(TIMEOUT)
        );
        collectAllMeters();
        assertThat(
            "phone-home has-dependents rejection",
            clusterTotal(a -> a.configChanges(DataSourceUsageAccumulator.KIND_DATASOURCE, DataSourceUsageAccumulator.OP_REJECTED))
                - rejectedBefore,
            equalTo(1L)
        );
        assertThat(
            "APM has_dependents",
            counters(ExternalSourceMetrics.CONFIG_CHANGES_TOTAL).stream()
                .anyMatch(
                    m -> "rejected".equals(m.attributes().get(ExternalSourceMetrics.OP_ATTRIBUTE))
                        && "has_dependents".equals(m.attributes().get(ExternalSourceMetrics.REASON_ATTRIBUTE))
                ),
            equalTo(true)
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

    private long apmConfigChanges(String kind, String op) {
        return counters(ExternalSourceMetrics.CONFIG_CHANGES_TOTAL).stream()
            .filter(m -> kind == null || kind.equals(m.attributes().get(ExternalSourceMetrics.KIND_ATTRIBUTE)))
            .filter(m -> op.equals(m.attributes().get(ExternalSourceMetrics.OP_ATTRIBUTE)))
            .mapToLong(Measurement::getLong)
            .sum();
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

    private long counterTotalForType(String name, String type) {
        return counters(name).stream()
            .filter(m -> type.equals(m.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE)))
            .mapToLong(Measurement::getLong)
            .sum();
    }

    private long counterTotalForTypeAndFormat(String name, String type, String format) {
        return counters(name).stream()
            .filter(
                m -> type.equals(m.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE))
                    && format.equals(m.attributes().get(ExternalSourceMetrics.FORMAT_ATTRIBUTE))
            )
            .mapToLong(Measurement::getLong)
            .sum();
    }

    private long counterTotalForOutcome(String name, String outcome) {
        return counters(name).stream()
            .filter(m -> outcome.equals(m.attributes().get(ExternalSourceMetrics.OUTCOME_ATTRIBUTE)))
            .mapToLong(Measurement::getLong)
            .sum();
    }

    private static List<Measurement> forType(List<Measurement> measurements, String type) {
        return measurements.stream().filter(m -> type.equals(m.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE))).toList();
    }

    private static List<Measurement> forTypeAndFormat(List<Measurement> measurements, String type, String format) {
        return measurements.stream()
            .filter(
                m -> type.equals(m.attributes().get(ExternalSourceMetrics.TYPE_ATTRIBUTE))
                    && format.equals(m.attributes().get(ExternalSourceMetrics.FORMAT_ATTRIBUTE))
            )
            .toList();
    }

    private static List<Measurement> forOutcome(List<Measurement> measurements, String outcome) {
        return measurements.stream().filter(m -> outcome.equals(m.attributes().get(ExternalSourceMetrics.OUTCOME_ATTRIBUTE))).toList();
    }

    private static Measurement singleForType(List<Measurement> measurements, String type) {
        List<Measurement> found = forType(measurements, type);
        assertThat("expected exactly one measurement for type [" + type + "]", found, hasSize(1));
        return found.get(0);
    }

    /**
     * Sums a single accumulator field across all cluster nodes that have a live
     * {@link DataSourceUsageAccumulator}. Using a per-field lambda keeps the SPI surface
     * clean — the accumulator has no cross-node merge method.
     */
    private long clusterTotal(ToLongFunction<DataSourceUsageAccumulator> fn) {
        long total = 0;
        boolean found = false;
        for (String node : internalCluster().getNodeNames()) {
            PlanExecutor planExecutor = internalCluster().getInstance(PlanExecutor.class, node);
            if (planExecutor.dataSourceModule() == null) {
                continue;
            }
            DataSourceUsageAccumulator acc = planExecutor.dataSourceModule().externalSourceMetrics().usageAccumulator();
            if (acc == null) {
                continue;
            }
            found = true;
            total += fn.applyAsLong(acc);
        }
        assertTrue("No node has a DataSourceModule with a non-null usageAccumulator", found);
        return total;
    }

    /**
     * Done-when pin: no instrument still carries the retired {@code es_datasource_scheme} attribute.
     */
    private void assertNoSchemeAttribute() {
        for (String name : List.of(
            ExternalSourceMetrics.DISCOVERY_FILES_SCANNED,
            ExternalSourceMetrics.DISCOVERY_BYTES_SCANNED,
            ExternalSourceMetrics.DISCOVERY_DURATION,
            ExternalSourceMetrics.PARSE_ROWS_TOTAL,
            ExternalSourceMetrics.PARSE_DURATION,
            ExternalSourceMetrics.PARSE_SPLITS_SCANNED,
            ExternalSourceMetrics.QUERY_TIME_TO_FIRST_ROW,
            ExternalSourceMetrics.STORAGE_REQUESTS_TOTAL,
            ExternalSourceMetrics.STORAGE_BYTES_READ_TOTAL,
            ExternalSourceMetrics.STORAGE_REQUESTS_DURATION,
            ExternalSourceMetrics.STORAGE_RETRIES_TOTAL,
            ExternalSourceMetrics.STORAGE_ERRORS_TOTAL,
            ExternalSourceMetrics.STORAGE_THROTTLED_TOTAL,
            ExternalSourceMetrics.STORAGE_READ_STALL_DURATION,
            ExternalSourceMetrics.QUERIES_TOTAL,
            ExternalSourceMetrics.QUERY_DURATION
        )) {
            for (Measurement m : counters(name)) {
                assertThat(
                    "instrument [" + name + "] must not carry es_datasource_scheme",
                    m.attributes().containsKey("es_datasource_scheme"),
                    equalTo(false)
                );
            }
            for (Measurement m : histograms(name)) {
                assertThat(
                    "instrument [" + name + "] must not carry es_datasource_scheme",
                    m.attributes().containsKey("es_datasource_scheme"),
                    equalTo(false)
                );
            }
        }
    }
}
