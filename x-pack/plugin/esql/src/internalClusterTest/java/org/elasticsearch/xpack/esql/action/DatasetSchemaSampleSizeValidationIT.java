/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * End-to-end contract for {@code schema_sample_size} on file datasets, through the real
 * {@code EsqlPlugin} wiring (the resolver assembled from registered {@code FormatSpec}s and the
 * {@code local}-type {@link FileDataSourceValidator}), so a resolver-wiring regression cannot hide
 * behind the unit tests' hand-built resolvers. Pins: PUT with a Parquet-resolved format fails naming
 * the setting and the format; PUT with an undeterminable format tells the user to pin {@code format};
 * a dataset already carrying the setting in cluster state still reads instead of failing with
 * "unknown option", and says so in a response {@code Warning} header (elastic/elasticsearch#155636).
 */
public class DatasetSchemaSampleSizeValidationIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, ParquetDataSourcePlugin.class);
    }

    /** Registrations made against the {@code local}-type data source, outside the base-class helpers. */
    private final Set<String> rawDatasets = new LinkedHashSet<>();
    private final Set<String> rawDataSources = new LinkedHashSet<>();

    @After
    public void cleanupRawRegistrations() {
        for (String dataset : rawDatasets) {
            try {
                client().execute(DeleteDatasetAction.INSTANCE, new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { dataset }))
                    .get(30, TimeUnit.SECONDS);
            } catch (ResourceNotFoundException ignored) {
                // already deleted
            } catch (Exception e) {
                logger.warn("dataset cleanup [{}] failed", dataset, e);
            }
        }
        for (String dataSource : rawDataSources) {
            try {
                client().execute(
                    DeleteDataSourceAction.INSTANCE,
                    new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { dataSource })
                ).get(30, TimeUnit.SECONDS);
            } catch (ResourceNotFoundException ignored) {
                // already deleted
            } catch (Exception e) {
                logger.warn("data source cleanup [{}] failed", dataSource, e);
            }
        }
        rawDatasets.clear();
        rawDataSources.clear();
    }

    public void testPutRejectsSchemaSampleSizeWhenExtensionResolvesToParquet() throws Exception {
        registerLocalDataSource("ssz_ext_ds");
        ValidationException e = expectPutDatasetValidationFailure(
            "ssz_ext",
            "ssz_ext_ds",
            "file:///data/events.parquet",
            Map.of("schema_sample_size", 100)
        );
        assertThat(e.getMessage(), containsString(FileDataSourceValidator.notSupportedByFormatError("schema_sample_size", "parquet")));
    }

    public void testPutRejectsSchemaSampleSizeWhenExplicitFormatIsParquet() throws Exception {
        registerLocalDataSource("ssz_fmt_ds");
        ValidationException e = expectPutDatasetValidationFailure(
            "ssz_fmt",
            "ssz_fmt_ds",
            "file:///data/events",
            Map.of("format", "parquet", "schema_sample_size", 100)
        );
        assertThat(e.getMessage(), containsString(FileDataSourceValidator.notSupportedByFormatError("schema_sample_size", "parquet")));
    }

    public void testPutRejectsSchemaSampleSizeWhenFormatCannotBeDetermined() throws Exception {
        registerLocalDataSource("ssz_ambig_ds");
        String resource = "file:///data/events";
        ValidationException e = expectPutDatasetValidationFailure("ssz_ambig", "ssz_ambig_ds", resource, Map.of("schema_sample_size", 100));
        assertThat(
            e.getMessage(),
            containsString(FileDataSourceValidator.cannotDetermineFormatError(resource, Set.of("schema_sample_size")))
        );
    }

    /** The setting keeps working where it applies: a CSV dataset registers with it and reads end to end. */
    public void testCsvDatasetWithSchemaSampleSizeRegistersAndReads() throws Exception {
        Path dir = createTempDir().resolve("ssz_csv");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("rows.csv"), "id,name\n1,alpha\n2,beta\n3,gamma\n", StandardCharsets.UTF_8);

        registerLocalDataSource("ssz_csv_ds");
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    "ssz_csv",
                    "ssz_csv_ds",
                    StoragePath.fileUri(dir) + "/rows.csv",
                    null,
                    new HashMap<>(Map.of("schema_sample_size", 2))
                )
            )
        );
        rawDatasets.add("ssz_csv");

        try (var response = run(syncEsqlQueryRequest("FROM ssz_csv | STATS n = COUNT(*)"), TIMEOUT)) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(3L));
        }
    }

    /**
     * The legacy shape this fix exists for: {@code schema_sample_size} already in cluster state on a
     * Parquet dataset (the pass-through {@code test}-type validator stores it unvalidated, like a
     * pre-tightening registration), with a settings-less parent — so no {@code _datasource} envelope
     * reaches the query config. The query must read the file and ignore the setting — and the ignore
     * must be told to the client as a response {@code Warning} header. Both halves are captured from
     * one execution: a second query could serve the schema from cache and skip validation entirely,
     * and a warning written on the wrong thread would pass a query-succeeds-only check.
     */
    public void testStoredSchemaSampleSizeOnParquetDatasetIsIgnoredAtQueryTimeWithWarning() throws Exception {
        Path dir = createTempDir().resolve("ssz_legacy");
        Files.createDirectories(dir);
        writeParquet(dir.resolve("legacy.parquet"), 3, 100);

        registerDataset("ssz_legacy", StoragePath.fileUri(dir) + "/legacy.parquet", Map.of("schema_sample_size", 100));

        // Pin the coordinator (the warning is emitted during coordinator-side resolution) and read
        // that same node's response Warning headers, mirroring
        // ExternalCsvHivePartitionedIT#testHivePartitionShadowWarningReachesClient. Do NOT close the
        // response inside the listener: the transport framework's respondAndRelease wrapper calls
        // decRef() after onResponse returns, and a manual close double-releases.
        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        List<String> warnings = new CopyOnWriteArrayList<>();
        AtomicReference<Long> count = new AtomicReference<>();
        AtomicReference<Exception> queryFailure = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        client(coordinator.getName()).execute(
            EsqlQueryAction.INSTANCE,
            syncEsqlQueryRequest("FROM ssz_legacy | STATS n = COUNT(*)"),
            new ActionListener<>() {
                @Override
                public void onResponse(EsqlQueryResponse response) {
                    try {
                        count.set(((Number) getValuesList(response).get(0).get(0)).longValue());
                        warnings.addAll(
                            internalCluster().getInstance(TransportService.class, coordinator.getName())
                                .getThreadPool()
                                .getThreadContext()
                                .getResponseHeaders()
                                .getOrDefault("Warning", List.of())
                        );
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    queryFailure.set(e);
                    latch.countDown();
                }
            }
        );
        assertTrue("query did not complete within timeout", latch.await(30, TimeUnit.SECONDS));
        if (queryFailure.get() != null) {
            throw queryFailure.get();
        }
        assertThat(count.get(), equalTo(3L));
        String expected = FileDataSourceValidator.notSupportedByFormatError("schema_sample_size", "parquet") + "; ignored";
        assertTrue(
            "the ignored setting must reach the client as a response Warning header; headers seen: " + warnings,
            warnings.stream().anyMatch(w -> w.contains(expected))
        );
    }

    /**
     * A {@code local}-type data source, validated by the real {@link FileDataSourceValidator} with the
     * {@code EsqlPlugin}-assembled resolver — unlike the base-class helper's pass-through {@code test} type.
     */
    private void registerLocalDataSource(String name) {
        assertAcked(
            client().execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, name, "local", null, new HashMap<>())
            )
        );
        rawDataSources.add(name);
    }

    private ValidationException expectPutDatasetValidationFailure(
        String name,
        String dataSource,
        String resource,
        Map<String, Object> settings
    ) {
        ExecutionException err = expectThrows(
            ExecutionException.class,
            () -> client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(TIMEOUT, TIMEOUT, name, dataSource, resource, null, new HashMap<>(settings))
            ).get()
        );
        assertThat(err.getCause(), instanceOf(ValidationException.class));
        return (ValidationException) err.getCause();
    }
}
