/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.xpack.esql.datasources.Federation;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

/**
 * Cross-cluster counterpart of {@link CrossClusterViewIT} for datasets, and deliberately its opposite. Registers a
 * dataset and an index on each remote, then asserts that the dataset is invisible from another cluster rather than
 * fatal to the query: a wildcard returns the index rows beside it, and the exact qualified name resolves to nothing
 * and reports an unknown index. A view in the same position still fails the query, which CrossClusterViewIT covers.
 *
 * <p>Multi-node remotes are safe: the diff-apply indices-lookup reuse guard now accounts for dataset metadata.
 */
public class CrossClusterDatasetIT extends AbstractCrossClusterTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);
    private static final String REMOTE_DATASET = "remote_employees";
    private static final String REMOTE_DATASET_2 = "remote_employees_b";
    private static final String REMOTE_PLAIN_INDEX = "logs_idx";
    private static final String REMOTE_LOGS_INDEX = "remote_logs";
    /** {@code populateRemoteIndices} writes exactly this many documents per index. */
    private static final int DOCS_PER_INDEX = 10;

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
    protected Settings nodeSettings() {
        // Both the local and the remote nodes need federation on: the remote reports its datasets during field
        // resolution only when it is available there, and the local coordinator only asks when it is available here.
        return Settings.builder().put(super.nodeSettings()).put(Federation.FEDERATION_ENABLED.getKey(), true).build();
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
        assumeTrue("requires dataset-in-from-command capability", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        setupClusters(3);

        // A plain index on the remote that the successful query reads from.
        populateRemoteIndices(REMOTE_CLUSTER_1, REMOTE_PLAIN_INDEX, randomIntBetween(1, 3));

        // An index on BOTH remotes whose name the remot*/remote* wildcards match alongside the dataset, so those
        // patterns resolve to an index as well as a dataset on either cluster. Every wildcard assertion below is then
        // positive: a pattern that returns nothing is distinguishable from one that returns only the index's rows.
        populateRemoteIndices(REMOTE_CLUSTER_1, REMOTE_LOGS_INDEX, randomIntBetween(1, 3));
        populateRemoteIndices(REMOTE_CLUSTER_2, REMOTE_LOGS_INDEX, randomIntBetween(1, 3));

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

        // A second dataset on remote-b so the cluster-exclusion / multi-cluster legs exercise "exclude the
        // dataset-bearing cluster, keep the other" rather than relying on the other remote being dataset-free.
        assertAcked(
            client(REMOTE_CLUSTER_2).execute(PutDataSourceAction.INSTANCE, putDataSourceRequest("remote_ds_b", Map.of()))
                .actionGet(30, TimeUnit.SECONDS)
        );
        assertAcked(
            client(REMOTE_CLUSTER_2).execute(
                PutDatasetAction.INSTANCE,
                putDatasetRequest(REMOTE_DATASET_2, "remote_ds_b", csvFixture.toUri().toString(), Map.of("format", "csv"))
            ).actionGet(30, TimeUnit.SECONDS)
        );
    }

    /**
     * The exact qualified name of a remote dataset resolves to nothing, so it reports an unknown index — the error any
     * name that does not exist gives. Nothing in the response advertises that a dataset is what it matched.
     */
    public void testRemoteDatasetResolvesAsMissingIndex() {
        Exception e = expectThrows(Exception.class, () -> runQuery("FROM " + REMOTE_CLUSTER_1 + ":" + REMOTE_DATASET, null));
        String message = ExceptionsHelper.unwrapCause(e).getMessage();
        assertThat(message, containsString("Unknown index [" + REMOTE_CLUSTER_1 + ":" + REMOTE_DATASET + "]"));
        assertThat(message, not(containsString("remote datasets are not supported")));
    }

    /**
     * A wildcard matching both the remote dataset and a remote index returns exactly the index's rows. Asserting the
     * row count rather than mere success is what separates a dropped dataset from a dropped index.
     */
    public void testWildcardOverRemoteDatasetReturnsOnlyIndexRows() {
        try (var resp = runQuery("FROM " + REMOTE_CLUSTER_1 + ":remote* | STATS c = COUNT(*)", null)) {
            assertOk(resp);
            assertThat(getValuesList(resp), equalTo(List.of(List.of((long) DOCS_PER_INDEX))));
        }
        // The rows are the index's: remote_logs carries id/tag/v, the dataset carries emp_no/first_name.
        try (var resp = runQuery("FROM " + REMOTE_CLUSTER_1 + ":remote* | KEEP id, tag | LIMIT 100", null)) {
            assertOk(resp);
            assertThat(getValuesList(resp).size(), equalTo(DOCS_PER_INDEX));
        }
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

    /**
     * Invisibility holds on every cluster a pattern spans, not just the first. Both remotes hold a dataset and an
     * index that {@code remot*} matches; the query succeeds, is not partial, and returns exactly the two indices' rows.
     */
    public void testWildcardSpanningTwoRemotesReturnsOnlyIndexRows() {
        try (var resp = runQuery("FROM " + REMOTE_CLUSTER_1 + ":remot*," + REMOTE_CLUSTER_2 + ":remot* | STATS c = COUNT(*)", null)) {
            assertOk(resp);
            assertThat(getValuesList(resp), equalTo(List.of(List.of((long) (2 * DOCS_PER_INDEX)))));
        }
    }

    /**
     * Dataset analog of {@link CrossClusterViewIT#testUnknownRemote}: a concrete unknown remote throws
     * {@link NoSuchRemoteClusterException}, while a wildcard cluster expression with no concrete match resolves to the
     * empty set and SUCCEEDS (non-partial).
     */
    public void testUnknownRemote() {
        expectThrows(
            NoSuchRemoteClusterException.class,
            containsString("no such remote cluster: [no_such_remote]"),
            () -> runQuery("FROM no_such_remote:" + REMOTE_DATASET, null)
        );
        try (var resp = runQuery("FROM no_such_*:" + REMOTE_DATASET, null)) {
            assertOk(resp);
        }
    }

    private static void assertOk(EsqlQueryResponse response) {
        assertThat(response.isPartial(), equalTo(false));
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
