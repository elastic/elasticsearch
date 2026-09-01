/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import fixture.gcs.GoogleCloudStorageHttpFixture;
import fixture.gcs.TestUtils;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.esql.QueryMetricsListener;
import org.elasticsearch.xpack.esql.action.AbstractExternalDataSourceIT;
import org.elasticsearch.xpack.esql.datasource.gcs.GcsDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.ByteArrayInputStream;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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

/**
 * End-to-end metrics test for Parquet reads over GCS, covering the GCS executor-thread CPU gap
 * ({@code asyncCpuNanos}) — the portion of {@code read_cpu_nanos} that comes from buffer-management
 * work done on the GCS executor thread rather than the producer thread.
 *
 * <p>Uses {@link GoogleCloudStorageHttpFixture} as a mock GCS server. A Parquet file is uploaded
 * to the fixture bucket in {@code @BeforeClass} and read by the cluster via a registered {@code gcs}
 * data source. The test asserts {@code READ_CPU_NANOS > 0} in the captured query metrics.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class EsqlQueryMetricsCollectorGcsIT extends AbstractExternalDataSourceIT {

    private static final String BUCKET = "test-bucket";
    private static final String TOKEN = "test-token";
    private static final String OBJECT_KEY = "data/metrics.parquet";

    @ClassRule
    public static GoogleCloudStorageHttpFixture gcsFixture = new GoogleCloudStorageHttpFixture(true, BUCKET, TOKEN);

    private static byte[] serviceAccountJson;
    private static Storage gcsStorage;

    @BeforeClass
    public static void setupGcsFixture() throws Exception {
        serviceAccountJson = TestUtils.createServiceAccount(random());
        String endpoint = gcsFixture.getAddress();
        ServiceAccountCredentials creds = ServiceAccountCredentials.fromStream(new ByteArrayInputStream(serviceAccountJson))
            .toBuilder()
            .setTokenServerUri(URI.create(endpoint + "/" + TOKEN))
            .build();
        gcsStorage = StorageOptions.newBuilder().setCredentials(creds).setProjectId("test-project").setHost(endpoint).build().getService();

        Path parquet = writeParquet(createTempDir("gcs-metrics").resolve("data.parquet"), 100, 50);
        gcsStorage.create(BlobInfo.newBuilder(BUCKET, OBJECT_KEY).build(), Files.readAllBytes(parquet));
    }

    @AfterClass
    public static void closeGcsClient() throws Exception {
        if (gcsStorage != null) {
            gcsStorage.close();
            gcsStorage = null;
        }
    }

    @Before
    public void requireGcsFeatureFlag() {
        assumeTrue("requires GCS feature flag", GcsDataSourcePlugin.ESQL_EXTERNAL_GCS_FEATURE_FLAG.isEnabled());
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(GcsDataSourcePlugin.class, ParquetDataSourcePlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(EsqlQueryMetricsCollectorIT.EsqlEnterpriseWithCollector.class);
        return plugins;
    }

    /**
     * Reads a Parquet file from the GCS fixture and asserts that {@code read_cpu_nanos} is populated.
     * This covers the GCS executor-thread CPU gap: {@link GcsDataSourcePlugin} dispatches each
     * range-read to an executor thread, accumulates CPU via {@code asyncCpuNanos()}, and the Parquet
     * reader drains it into the format-reader counters on iterator close.
     */
    public void testMetricsCollectorGcsParquet() throws Exception {
        assumeTrue(
            "per-thread CPU timing not supported on this JVM",
            ManagementFactory.getThreadMXBean().isCurrentThreadCpuTimeSupported()
        );
        EsqlQueryMetricsCollectorIT.lastMetrics = null;

        registerDataSource(
            "gcs_metrics_ds",
            "gcs",
            Map.of(
                "credentials",
                new String(serviceAccountJson, StandardCharsets.UTF_8),
                "endpoint",
                gcsFixture.getAddress(),
                "token_uri",
                gcsFixture.getAddress() + "/" + TOKEN
            )
        );
        registerDataset("gcs_metrics_parquet", "gcs_metrics_ds", "gs://" + BUCKET + "/" + OBJECT_KEY, Map.of());

        try (var ignored = run(syncEsqlQueryRequest("FROM gcs_metrics_parquet | LIMIT 200"), TIMEOUT)) {}

        Map<String, Long> metrics = EsqlQueryMetricsCollectorIT.lastMetrics;
        assertThat("gcs-parquet: metrics must be set", metrics, notNullValue());
        assertThat("gcs-parquet: read_nanos > 0", metrics.get(QueryMetricsListener.READ_NANOS), greaterThan(0L));
        assertThat("gcs-parquet: read_cpu_nanos > 0", metrics.get(QueryMetricsListener.READ_CPU_NANOS), greaterThan(0L));
        assertThat(
            "gcs-parquet: read_cpu_nanos <= read_nanos",
            metrics.get(QueryMetricsListener.READ_NANOS),
            greaterThanOrEqualTo(metrics.get(QueryMetricsListener.READ_CPU_NANOS))
        );
    }
}
