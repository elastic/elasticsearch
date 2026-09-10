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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Cross-cluster counterpart of {@link CrossClusterViewIT} for datasets, and deliberately its opposite. Registers a
 * dataset and an index on each remote, then asserts that the dataset is invisible from another cluster rather than
 * fatal to the query: a wildcard returns the index rows beside it, and the exact qualified name behaves exactly as a
 * name registered nowhere does, which is why every such assertion is paired with that name as its control. What
 * happens to an unresolved name is decided by the remote's {@code skip_unavailable}, so both of its values are
 * covered. A view in the same position still fails the query, which CrossClusterViewIT covers.
 *
 * <p>Multi-node remotes are safe: the diff-apply indices-lookup reuse guard now accounts for dataset metadata.
 */
public class CrossClusterDatasetIT extends AbstractCrossClusterTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);
    private static final String REMOTE_DATASET = "remote_employees";
    private static final String REMOTE_DATASET_2 = "remote_employees_b";
    private static final String REMOTE_PLAIN_INDEX = "logs_idx";
    private static final String REMOTE_LOGS_INDEX = "remote_logs";
    /** A name registered on no cluster, as the control every dataset assertion is compared against. */
    private static final String NO_SUCH_NAME = "no_such_name_anywhere";
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
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        // Pinned rather than randomised because the two remotes cover the two halves of the skip_unavailable axis, and a
        // remote dataset resolves to a missing index, whose treatment is exactly what that setting decides.
        return Map.of(REMOTE_CLUSTER_1, false, REMOTE_CLUSTER_2, true);
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
     * On a remote whose {@code skip_unavailable} is false, the exact qualified name of a dataset reports an unknown
     * index and fails the query — and {@link #NO_SUCH_NAME}, which exists nowhere, reports the same thing in the same
     * position. Pinning that equality rather than the message is the point. The old remote-dataset rejection did not
     * say {@code Unknown index}, so matching that alone would have discriminated against it — but the wording is not
     * peculiar to a name that does not exist either: {@code DatasetRewriter} surfaces an explicitly named but
     * unauthorized dataset the same way, on purpose, so that a {@code 403} does not become an existence oracle. An
     * assertion on the message pins a wording; the control pins the property, which is what invisibility means.
     */
    public void testRemoteDatasetIsIndistinguishableFromAMissingName() {
        assertThat(failureShape(REMOTE_CLUSTER_1, REMOTE_DATASET), equalTo(failureShape(REMOTE_CLUSTER_1, NO_SUCH_NAME)));
        assertThat(failureShape(REMOTE_CLUSTER_1, REMOTE_DATASET), containsString("Unknown index [" + REMOTE_CLUSTER_1 + ":<name>]"));
    }

    /**
     * The same pair on a remote whose {@code skip_unavailable} is true, where a name that resolves to nothing is not
     * fatal: the query succeeds, the response is partial, and no rows come back. Both halves of that setting are
     * covered because it, not this change, is what decides whether an unresolved name ends the query.
     */
    public void testRemoteDatasetOnSkippableRemoteIsPartialLikeAMissingName() {
        for (String name : List.of(REMOTE_DATASET_2, NO_SUCH_NAME)) {
            try (var resp = runQuery("FROM " + REMOTE_CLUSTER_2 + ":" + name, null)) {
                assertThat("[" + name + "] should not have failed the query", resp.isPartial(), equalTo(true));
                assertThat(getValuesList(resp), empty());
            }
        }
    }

    /**
     * The failure a name that resolves to nothing produces on a remote that cannot be skipped, as its exception type
     * and its message with the queried name replaced by a placeholder — so two different names can be compared for
     * having failed the same way, which is what invisibility means here.
     */
    private String failureShape(String clusterAlias, String name) {
        Exception e = expectThrows(Exception.class, () -> runQuery("FROM " + clusterAlias + ":" + name, null));
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        return cause.getClass().getName() + ": " + cause.getMessage().replace(name, "<name>");
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

    /**
     * The cell no other wildcard test reaches: a pattern on a remote that cannot be skipped whose only match there is
     * the dataset. With no index beside it to carry the result, this is where the change is furthest from the old
     * behaviour, which failed the whole query naming the dataset. It resolves the way a pattern matching nothing
     * anywhere resolves, and that control is what the outcome is compared against rather than a fixed expectation,
     * since a wildcard and an exact name do not take the same branch when a remote resolves nothing.
     */
    public void testWildcardMatchingOnlyTheRemoteDatasetResolvesLikeAPatternMatchingNothing() {
        String shape = outcomeShape(REMOTE_CLUSTER_1 + ":" + REMOTE_DATASET + "*", REMOTE_DATASET);
        assertThat(shape, equalTo(outcomeShape(REMOTE_CLUSTER_1 + ":" + NO_SUCH_NAME + "*", NO_SUCH_NAME)));
        // The equality alone would also hold if both arms failed identically for some reason unrelated to datasets, so
        // the outcome is anchored as well: a wildcard resolving nothing succeeds rather than failing, and returns none.
        assertThat(shape, equalTo("succeeded partial=false rows=0"));
    }

    /**
     * The outcome of a query as a comparable string, whether it failed or succeeded, with the queried name replaced by
     * a placeholder so two different names can be compared for having resolved the same way.
     */
    private String outcomeShape(String indexExpression, String nameToMask) {
        try (var resp = runQuery("FROM " + indexExpression, null)) {
            return "succeeded partial=" + resp.isPartial() + " rows=" + getValuesList(resp).size();
        } catch (Exception e) {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            return cause.getClass().getName() + ": " + cause.getMessage().replace(nameToMask, "<name>");
        }
    }

    /**
     * The exclusion the old rejection told authors to write still parses and still runs, now that the name it excludes
     * resolves to nothing. The spelling matters: {@code RemoteResourceNotSupportedException.exclusionsOf} builds
     * {@code <cluster>:-<name>}, so that is the form queries in the wild carry and the one that must not start failing.
     * The other placement of the minus sign is asserted alongside it, since both are natural to write.
     */
    public void testExcludingTheRemoteDatasetStillSucceeds() {
        // The form the rejection's message prescribed: cluster-a:-remote_employees
        assertOnlyTheIndexRows(REMOTE_CLUSTER_1 + ":remote*," + REMOTE_CLUSTER_1 + ":-" + REMOTE_DATASET);
        // And the other placement: -cluster-a:remote_employees
        assertOnlyTheIndexRows(REMOTE_CLUSTER_1 + ":remote*,-" + REMOTE_CLUSTER_1 + ":" + REMOTE_DATASET);
    }

    private void assertOnlyTheIndexRows(String indexExpression) {
        try (var resp = runQuery("FROM " + indexExpression + " | STATS c = COUNT(*)", null)) {
            assertOk(resp);
            assertThat(indexExpression, getValuesList(resp), equalTo(List.of(List.of((long) DOCS_PER_INDEX))));
        }
    }

    /**
     * The control on the other side: an ordinary index on the same remote that holds a dataset still resolves and reads
     * in full, so a suite that went green by making everything on that cluster invisible would be caught here.
     */
    public void testRemoteIndexSucceeds() {
        try (var resp = runQuery("FROM " + REMOTE_CLUSTER_1 + ":" + REMOTE_PLAIN_INDEX + " | STATS c = COUNT(*)", null)) {
            assertThat(getValuesList(resp), equalTo(List.of(List.of((long) DOCS_PER_INDEX))));
        }
        try (var resp = runQuery("FROM " + REMOTE_CLUSTER_1 + ":" + REMOTE_PLAIN_INDEX + " | KEEP id | LIMIT 100", null)) {
            assertThat(getValuesList(resp).size(), equalTo(DOCS_PER_INDEX));
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
