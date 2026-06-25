/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.fieldcaps.RemoteDatasetNotSupportedException;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Cross-cluster twin of {@link CrossClusterViewIT} for datasets. Registers a dataset (and a normal index) on the
 * remote cluster, then asserts that a {@code FROM cluster-a:<dataset>} query fails on the remote-dataset detection
 * rail with {@link RemoteDatasetNotSupportedException} ("remote datasets are not supported"), while a plain
 * {@code FROM cluster-a:<index>} still succeeds. This is the dataset analogue of CrossClusterViewIT's
 * {@code testRemoteViewConcreteMatchFailsQuery}/{@code testRemoteViewWildcardMatchFailsQuery}.
 *
 * <p>Multi-node remotes are now safe: the harness's random 1-3 node remote cluster publishes the dataset across
 * followers without tripping {@code ProjectMetadata.Builder}'s indices-lookup rebuild assertion (the diff-apply
 * reuse guard now accounts for dataset metadata).
 */
public class CrossClusterDatasetIT extends AbstractCrossClusterTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);
    private static final String REMOTE_DATASET = "remote_employees";
    private static final String REMOTE_PLAIN_INDEX = "logs_idx";

    /** Minimal pass-through validator registered for type {@code test}; accepts any resource scheme. */
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
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        // The dataset lives on the remote, so its data-source validator must be installed there (and harmlessly
        // everywhere). AbstractCrossClusterTestCase already installs the EncryptionService binding the CRUD actions need.
        plugins.add(TestDataSourcePlugin.class);
        return plugins;
    }

    @Before
    public void setupClustersAndDataset() throws IOException {
        assumeTrue("requires external data sources feature flag", DatasetMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled());
        setupClusters(3);

        // A plain index on the remote that the successful query reads from.
        populateRemoteIndices(REMOTE_CLUSTER_1, REMOTE_PLAIN_INDEX, randomIntBetween(1, 3));

        // A CSV fixture on the shared (single-host) filesystem; reachable from every remote node via file://.
        Path csvFixture = createTempFile("ccs-dataset-", ".csv");
        Files.writeString(csvFixture, String.join("\n", "emp_no:integer,first_name:keyword", "1,Alice", "2,Bob", "3,Carol") + "\n");

        // Register the data source + dataset on the REMOTE cluster (root user via the remote client).
        assertAcked(
            client(REMOTE_CLUSTER_1).execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("remote_ds", Map.of()))
                .actionGet(30, TimeUnit.SECONDS)
        );
        assertAcked(
            client(REMOTE_CLUSTER_1).execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest(REMOTE_DATASET, "remote_ds", csvFixture.toUri().toString(), Map.of("format", "csv"))
            ).actionGet(30, TimeUnit.SECONDS)
        );
    }

    public void testRemoteDatasetConcreteMatchFailsQuery() {
        // The query fails on the remote-dataset detection rail; the cause is a RemoteDatasetNotSupportedException
        // (it may be wrapped by the transport layer by the time actionGet rethrows, so we assert on the message —
        // matching CrossClusterViewIT.testRemoteViewConcreteMatchFailsQuery).
        Exception e = expectThrows(Exception.class, () -> runQuery("FROM " + REMOTE_CLUSTER_1 + ":" + REMOTE_DATASET, null));
        assertRemoteDatasetRejected(e);
    }

    public void testRemoteDatasetWildcardMatchFailsQuery() {
        // A wildcard that matches only the remote dataset fails the same way.
        Exception e = expectThrows(
            Exception.class,
            () -> runQuery("FROM " + REMOTE_CLUSTER_1 + ":" + REMOTE_DATASET.substring(0, 6) + "*", null)
        );
        assertRemoteDatasetRejected(e);
    }

    public void testRemoteIndexSucceeds() {
        // The plain remote index resolves and executes normally; the dataset detection rail does not interfere.
        try (var resp = runQuery("FROM " + REMOTE_CLUSTER_1 + ":" + REMOTE_PLAIN_INDEX + " | STATS c = COUNT(*)", null)) {
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, equalTo(List.of(List.of(10L))));
        }
        // And a non-aggregating read returns the remote rows (populateRemoteIndices writes 10 docs).
        try (var resp = runQuery("FROM " + REMOTE_CLUSTER_1 + ":" + REMOTE_PLAIN_INDEX + " | KEEP id | LIMIT 100", null)) {
            assertThat(getValuesList(resp).size(), greaterThan(0));
        }
    }

    /** Walks the cause chain and asserts the remote-dataset rejection message (with the matched name) appears somewhere in it. */
    private void assertRemoteDatasetRejected(Throwable throwable) {
        String matched = REMOTE_CLUSTER_1 + ":" + REMOTE_DATASET;
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            String message = cause.getMessage();
            if (message != null && message.contains("ES|QL queries with remote datasets are not supported") && message.contains(matched)) {
                return;
            }
        }
        throw new AssertionError("expected a remote-dataset rejection mentioning [" + matched + "] in the cause chain", throwable);
    }

    private static PutDataSourceAction.Request putDataSourceRequest(String name, Map<String, Object> settings) {
        return new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, name, "test", null, new HashMap<>(settings));
    }

    private static PutDatasetAction.Request putDatasetRequest(
        String name,
        String dataSource,
        String resource,
        Map<String, Object> settings
    ) {
        return new PutDatasetAction.Request(TIMEOUT, TIMEOUT, name, dataSource, resource, null, new HashMap<>(settings));
    }
}
