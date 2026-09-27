/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.crossproject.ProjectRoutingInfo;
import org.elasticsearch.search.crossproject.ProjectTags;
import org.elasticsearch.search.crossproject.TargetProjects;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end coverage for a cross-project {@code FROM} that combines datasets, their remote index
 * namesakes, and ordinary index reads.
 */
public class CrossProjectDatasetFanInIT extends AbstractCrossClusterTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);
    private static final String DATA_SOURCE = "fan_in_source";

    private final Set<String> registeredDatasets = new LinkedHashSet<>();
    private boolean dataSourceRegistered;

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, DEFAULT_SKIP_UNAVAILABLE);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(AbstractExternalDataSourceIT.EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(HttpDataSourcePlugin.class);
        plugins.add(AbstractExternalDataSourceIT.TestDataSourcePlugin.class);
        plugins.add(CsvDataSourcePlugin.class);
        plugins.add(ViewOverDatasetCpsIT.CpsSettingPlugin.class);
        if (LOCAL_CLUSTER.equals(clusterAlias)) {
            plugins.add(AuthorizedProjectsPlugin.class);
        }
        return plugins;
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(Federation.FEDERATION_ENABLED.getKey(), true)
            .put("serverless.cross_project.enabled", true)
            .putList(ExternalSourceSettings.LOCAL_ALLOWED_PATHS.getKey(), createTempDir().getParent().toString())
            .build();
    }

    @Before
    public void setupCrossProjectDatasetTest() throws IOException {
        assumeTrue("requires dataset-in-from-command capability", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        assumeTrue("requires local filesystem feature flag", HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());
        setupClusters(2);
        assertAcked(
            client(LOCAL_CLUSTER).execute(
                PutDataSourceAction.INSTANCE,
                new PutDataSourceAction.Request(TIMEOUT, TIMEOUT, DATA_SOURCE, "test", null, new HashMap<>())
            )
        );
        dataSourceRegistered = true;
    }

    @After
    public void cleanupDataSources() {
        for (String dataset : registeredDatasets) {
            try {
                client(LOCAL_CLUSTER).execute(
                    DeleteDatasetAction.INSTANCE,
                    new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { dataset })
                ).actionGet(30, TimeUnit.SECONDS);
            } catch (ResourceNotFoundException ignored) {
                // Already removed by an interrupted setup or another cleanup path.
            }
        }
        registeredDatasets.clear();
        if (dataSourceRegistered) {
            try {
                client(LOCAL_CLUSTER).execute(
                    DeleteDataSourceAction.INSTANCE,
                    new DeleteDataSourceAction.Request(TIMEOUT, TIMEOUT, new String[] { DATA_SOURCE })
                ).actionGet(30, TimeUnit.SECONDS);
            } catch (ResourceNotFoundException ignored) {
                // Already removed by an interrupted setup or another cleanup path.
            }
            dataSourceRegistered = false;
        }
    }

    public void testLoadsSourceOnlyFieldAfterMergingDatasetNamesakeWithOrdinaryIndex() throws Exception {
        String dataset = "fan_in_mapped";
        Path csv = writeCsv(dataset, "dataset");
        registerDataset(dataset, csv);

        assertAcked(client(REMOTE_CLUSTER_1).admin().indices().prepareCreate(dataset).setMapping("value", "type=keyword"));
        client(REMOTE_CLUSTER_1).prepareIndex(dataset).setSource("value", "remote").get();
        client(REMOTE_CLUSTER_1).admin().indices().prepareRefresh(dataset).get();

        String localIndex = "fan_in_unmapped";
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareCreate(localIndex).setMapping("""
            {"dynamic":false,"properties":{"marker":{"type":"keyword"}}}
            """));
        client(LOCAL_CLUSTER).prepareIndex(localIndex).setSource("value", "source", "marker", "present").get();
        client(LOCAL_CLUSTER).admin().indices().prepareRefresh(localIndex).get();

        try (
            var response = runQuery(
                crossProjectRequest(
                    "SET unmapped_fields=\"load\"; FROM "
                        + dataset
                        + ","
                        + localIndex
                        + " | WHERE value IS NOT NULL | KEEP value | SORT value"
                )
            )
        ) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("dataset"), List.of("remote"), List.of("source"))));
        }
    }

    public void testMergesDisjointIndexReadBesideOverlappingDatasetNamesakes() throws Exception {
        List<String> datasets = new ArrayList<>();
        for (int i = 1; i <= 6; i++) {
            String dataset = "fan_in_dataset_" + i;
            datasets.add(dataset);
            registerDataset(dataset, writeCsv(dataset, "dataset-" + i));
        }

        for (String dataset : datasets.subList(0, 2)) {
            assertAcked(client(REMOTE_CLUSTER_1).admin().indices().prepareCreate(dataset).setMapping("value", "type=keyword"));
            client(REMOTE_CLUSTER_1).prepareIndex(dataset).setSource("value", "remote-" + dataset).get();
        }
        client(REMOTE_CLUSTER_1).admin().indices().prepareRefresh(datasets.get(0), datasets.get(1)).get();

        String sources = String.join(",", datasets) + ",fan_in_dataset_*";
        try (var response = runQuery(crossProjectRequest("FROM " + sources + " | STATS count = COUNT(*)"))) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(10L))));
        }
    }

    private static EsqlQueryRequest crossProjectRequest(String query) {
        EsqlQueryRequest request = syncEsqlQueryRequest(query);
        request.set(QuerySettings.PROJECT_ROUTING, "*");
        return request;
    }

    private Path writeCsv(String prefix, String value) throws IOException {
        Path csv = createTempFile(prefix, ".csv");
        Files.writeString(csv, "value:keyword\n" + value + "\n");
        return csv;
    }

    private void registerDataset(String name, Path csv) {
        assertAcked(
            client(LOCAL_CLUSTER).execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(
                    TIMEOUT,
                    TIMEOUT,
                    name,
                    DATA_SOURCE,
                    csv.toUri().toString(),
                    null,
                    new HashMap<>(Map.of("format", "csv"))
                )
            )
        );
        registeredDatasets.add(name);
    }

    /**
     * Supplies the authorized project set and flat index expansion normally provided by the security action filter.
     * Security is disabled in this fixture so the test can focus on cross-project source resolution and execution.
     */
    public static class AuthorizedProjectsPlugin extends Plugin implements ActionPlugin {
        private static final ProjectRoutingInfo ORIGIN_PROJECT = new ProjectRoutingInfo(
            ProjectId.DEFAULT,
            "elasticsearch",
            "_origin",
            "organization",
            new ProjectTags(Map.of())
        );
        private static final ProjectRoutingInfo LINKED_PROJECT = new ProjectRoutingInfo(
            ProjectId.DEFAULT,
            "elasticsearch",
            REMOTE_CLUSTER_1,
            "organization",
            new ProjectTags(Map.of())
        );
        private static final TargetProjects TARGET_PROJECTS = new TargetProjects(ORIGIN_PROJECT, List.of(LINKED_PROJECT), null, true);

        private ClusterService clusterService;

        @Override
        public Collection<?> createComponents(PluginServices services) {
            clusterService = services.clusterService();
            return List.of();
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return List.of(new ActionFilter.Simple() {
                @Override
                protected boolean apply(String action, ActionRequest request, ActionListener<?> listener) {
                    if (request instanceof IndicesRequest.CrossProjectCandidate candidate
                        && candidate.allowsCrossProject()
                        && candidate.getProjectRouting() != null) {
                        candidate.setResolvedTargetProjects(TARGET_PROJECTS);
                        if (request instanceof IndicesRequest.Replaceable replaceable) {
                            List<String> expandedIndices = new ArrayList<>();
                            for (String index : replaceable.indices()) {
                                if (RemoteClusterAware.isRemoteIndexName(index)) {
                                    expandedIndices.add(index);
                                    continue;
                                }
                                if (matchesLocalIndex(index)) {
                                    expandedIndices.add(index);
                                }
                                expandedIndices.add(REMOTE_CLUSTER_1 + ":" + index);
                            }
                            replaceable.indices(expandedIndices.toArray(String[]::new));
                        }
                    }
                    return true;
                }

                @Override
                public int order() {
                    return Integer.MIN_VALUE;
                }
            });
        }

        private boolean matchesLocalIndex(String expression) {
            var indexNames = clusterService.state().metadata().getProject().getIndicesLookup().keySet();
            return Regex.isSimpleMatchPattern(expression)
                ? indexNames.stream().anyMatch(index -> Regex.simpleMatch(expression, index))
                : indexNames.contains(expression);
        }
    }
}
