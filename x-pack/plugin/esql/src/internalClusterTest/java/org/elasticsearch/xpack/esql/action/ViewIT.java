/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.datasource.DeleteDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.elasticsearch.xpack.esql.view.ViewService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class ViewIT extends AbstractEsqlIntegTestCase {

    /**
     * Registers a historic (not net-new) system index, so that accessing it without a system origin only issues a deprecation warning.
     */
    public static class TestSystemIndexPlugin extends Plugin implements SystemIndexPlugin {

        private static final String SYSTEM_INDEX_NAME = ".system-index";

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(
                SystemIndexDescriptor.builder()
                    .setIndexPattern(SYSTEM_INDEX_NAME + "*")
                    .setDescription("test system index for views")
                    .setType(SystemIndexDescriptor.Type.INTERNAL_UNMANAGED)
                    .build()
            );
        }

        @Override
        public String getFeatureName() {
            return "view-it-system-index";
        }

        @Override
        public String getFeatureDescription() {
            return "test system index for views";
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(DataStreamsPlugin.class);
        plugins.add(ReindexPlugin.class);
        plugins.add(TestSystemIndexPlugin.class);
        // External datasets need the datasource-aware ES|QL plugin, a validator for the "test" data source type and a CSV reader.
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(AbstractExternalDataSourceIT.EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(HttpDataSourcePlugin.class);
        plugins.add(AbstractExternalDataSourceIT.TestDataSourcePlugin.class);
        plugins.add(CsvDataSourcePlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .putList(ExternalSourceSettings.LOCAL_ALLOWED_PATHS.getKey(), createTempDir().getParent().toString())
            .build();
    }

    public void testIndicesAreNotValidateUponCreation() {
        assertAcked(createView("my_view", "FROM not-validated"));
    }

    public void testInvalidSyntaxQueryIsRejected() {
        expectThrows(ElasticsearchException.class, containsString("mismatched input"), () -> createView("my_view", "NOT VALID ESQL $$$$"));
    }

    public void testSetIsRejected() {
        expectThrows(
            ElasticsearchException.class,
            containsString("SET statements are not allowed in views"),
            () -> createView("my_view", "SET time_zone=\"Europe/Berlin\"; FROM index")
        );
    }

    public void testCannotCreateAliasToView() {
        assertAcked(createView("my-view", "FROM not-validated"));

        expectThrows(
            IndexNotFoundException.class,
            containsString("no such index [my-view]"),
            () -> indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(IndicesAliasesRequest.AliasActions.add().index("my-view").alias("some-alias"))
                .get()
        );
    }

    public void testViewOverRow() {
        assertAcked(createView("row-view", "ROW f1=1"));

        try (EsqlQueryResponse response = run("FROM row-view")) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(1))));
        }
    }

    public void testViewOverAlias() {
        assertAcked(indicesAdmin().prepareCreate("source-index").setMapping("f1", "type=integer"));
        prepareIndex("source-index").setSource("f1", 42).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(IndicesAliasesRequest.AliasActions.add().index("source-index").alias("source-alias"))
                .get()
        );

        assertAcked(createView("alias-view", "FROM source-alias | KEEP f1"));

        try (EsqlQueryResponse response = run("FROM alias-view")) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(42))));
        }
    }

    public void testViewOverTimeSeries() {
        assertAcked(
            indicesAdmin().prepareCreate("ts-source")
                .setSettings(Settings.builder().put("index.mode", "time_series").putList("index.routing_path", List.of("host")))
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "host",
                    "type=keyword,time_series_dimension=true",
                    "cpu",
                    "type=long,time_series_metric=gauge"
                )
        );
        long now = System.currentTimeMillis();
        String time1 = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(now);
        String time2 = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(now + 1000);
        assertNoFailures(
            client().prepareBulk()
                .add(prepareIndex("ts-source").setSource("@timestamp", time1, "host", "a", "cpu", 10))
                .add(prepareIndex("ts-source").setSource("@timestamp", time2, "host", "a", "cpu", 30))
                .add(prepareIndex("ts-source").setSource("@timestamp", time1, "host", "b", "cpu", 20))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get()
        );

        // Per-series averages are 20 (host a) and 20 (host b). A plain sum over all docs would give 60.
        assertAcked(createView("ts-view", "TS ts-source | STATS total = sum(avg_over_time(cpu)) | EVAL f1=1"));
        try (EsqlQueryResponse response = run("FROM ts-view | EVAL f2=2")) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(40.0, 1, 2))));
        }
    }

    public void testViewOverExternalDataset() throws IOException {
        assumeTrue("requires local filesystem feature flag", HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());

        var file = createTempDir().resolve("data.csv");
        Files.writeString(file, "id,value\n1,alpha\n2,beta\n3,gamma\n", StandardCharsets.UTF_8);
        try {
            assertAcked(
                client().execute(
                    PutDataSourceAction.INSTANCE,
                    new PutDataSourceAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "csv_ds", "test", null, new HashMap<>())
                )
            );
            assertAcked(
                client().execute(
                    PutDatasetAction.INSTANCE,
                    new PutDatasetAction.Request(
                        TEST_REQUEST_TIMEOUT,
                        TEST_REQUEST_TIMEOUT,
                        "csv_dataset",
                        "csv_ds",
                        file.toUri().toString(),
                        null,
                        new HashMap<>(Map.of("format", "csv"))
                    )
                )
            );
            assertAcked(createView("dataset-view", "FROM csv_dataset | WHERE id > 1 | KEEP value"));

            try (EsqlQueryResponse response = run("FROM dataset-view | SORT value")) {
                assertThat(getValuesList(response), equalTo(List.of(List.of("beta"), List.of("gamma"))));
            }
        } finally {
            // Datasets and data sources are cluster metadata that the test framework does not wipe between tests.
            assertAcked(
                client().execute(
                    DeleteDatasetAction.INSTANCE,
                    new DeleteDatasetAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[] { "csv_dataset" })
                )
            );
            assertAcked(
                client().execute(
                    DeleteDataSourceAction.INSTANCE,
                    new DeleteDataSourceAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[] { "csv_ds" })
                )
            );
        }
    }

    public void testViewOverSystemIndex() {
        prepareIndex(TestSystemIndexPlugin.SYSTEM_INDEX_NAME).setSource("f1", 42)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertAcked(createView("system-view", "FROM " + TestSystemIndexPlugin.SYSTEM_INDEX_NAME + " | KEEP f1"));

        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        ThreadContext threadContext = internalCluster().getInstance(TransportService.class, coordinator.getName())
            .getThreadPool()
            .getThreadContext();

        Tuple<List<String>, List<List<Object>>> result = safeAwait(listener -> {
            client(coordinator.getName()).filterWithHeader(Map.of(SystemIndices.SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, "false"))
                .execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest("FROM system-view"), listener.map(r -> {
                    List<String> warnings = threadContext.getResponseHeaders()
                        .getOrDefault("Warning", List.of())
                        .stream()
                        .map(w -> HeaderWarning.decodeAndUnescape(HeaderWarning.extractWarningValueFromWarningHeader(w, false)))
                        .toList();
                    return Tuple.tuple(warnings, getValuesList(r));
                }));
        });

        assertThat(
            result.v1(),
            hasItem(containsString("this request accesses system indices: [" + TestSystemIndexPlugin.SYSTEM_INDEX_NAME + "]"))
        );
        assertThat(result.v2(), equalTo(List.of(List.of(42L))));
    }

    public void testViewOverDataStream() throws IOException {
        assertAcked(
            client().execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request("ds-view-template").indexTemplate(
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of("logs-view-test*"))
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                        .template(Template.builder().mappings(new CompressedXContent("""
                            {
                              "properties": {
                                "@timestamp": { "type": "date" },
                                "f1": { "type": "integer" }
                              }
                            }""")))
                        .build()
                )
            )
        );

        String time = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
        prepareIndex("logs-view-test").setOpType(DocWriteRequest.OpType.CREATE)
            .setSource("@timestamp", time, "f1", 42)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        assertAcked(createView("ds-view", "FROM logs-view-test | KEEP f1"));

        try (EsqlQueryResponse response = run("FROM ds-view")) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(42))));
        }
    }

    public void testViewCannotBeReindexSource() {
        assertAcked(createView("my-view", "FROM not-validated"));
        assertAcked(indicesAdmin().prepareCreate("dest-index"));

        expectThrows(
            IndexNotFoundException.class,
            containsString("no such index [my-view]"),
            () -> client().execute(ReindexAction.INSTANCE, new ReindexRequest().setSourceIndices("my-view").setDestIndex("dest-index"))
                .actionGet()
        );
    }

    public void testViewIsInvisibleInFieldCaps() {
        assertAcked(indicesAdmin().prepareCreate("my-index"));
        assertAcked(createView("my-view", "FROM my-index"));

        String[] indices = client().prepareFieldCaps("*").setFields("*").get().getIndices();
        assertThat(List.of(indices), contains("my-index"));
    }

    public void testReservedViewCannotBeUpdatedOrDeletedByUser() {
        String viewName = "reserved-view";
        assertAcked(createView(viewName, "FROM some-index", null, true));

        expectThrows(
            IllegalArgumentException.class,
            containsString("cannot modify reserved view [" + viewName + "]"),
            () -> createView(viewName, "FROM something-else")
        );

        // but can update definition of reserved view with another reserved view
        assertAcked(createView(viewName, "FROM some-other-index", null, true));

        expectThrows(
            IllegalArgumentException.class,
            containsString("cannot delete reserved view [" + viewName + "]"),
            () -> client().execute(
                DeleteViewAction.INSTANCE,
                new DeleteViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[] { viewName })
            ).actionGet(30, TimeUnit.SECONDS)
        );

        // but can be deleted when explicitly asked for
        assertAcked(
            client().execute(
                DeleteViewAction.INSTANCE,
                new DeleteViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[] { viewName }, true)
            )
        );
    }

    public void testEnsureReservedViewExists() {
        var viewService = internalCluster().getCurrentMasterNodeInstance(ViewService.class);

        var future = new PlainActionFuture<AcknowledgedResponse>();
        viewService.ensureReservedViewExists(ProjectId.DEFAULT, new View("reserved", "ROW f1=1", null, true), future);
        // the view is created in response to a cluster state change
        assertAcked(indicesAdmin().prepareCreate("trigger-index"));
        assertAcked(future.actionGet(30, TimeUnit.SECONDS));

        View view = viewService.get(ProjectId.DEFAULT, "reserved");
        assertThat(view, equalTo(new View("reserved", "ROW f1=1", null, true)));
    }

    private AcknowledgedResponse createView(String viewName, String query) {
        return createView(viewName, query, null, false);
    }

    private AcknowledgedResponse createView(String viewName, String query, String description, boolean reserved) {
        return client().execute(
            PutViewAction.INSTANCE,
            new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(viewName, query, description, reserved))
        ).actionGet(30, TimeUnit.SECONDS);
    }
}
