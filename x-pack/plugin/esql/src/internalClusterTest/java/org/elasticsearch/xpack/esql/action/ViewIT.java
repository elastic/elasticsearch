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
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
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
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.elasticsearch.xpack.esql.view.ViewService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.jsonEntityToMap;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

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
        return plugins;
    }

    /** Real HTTP, so a test can read the get-view response body. */
    @Override
    protected boolean addMockHttpTransport() {
        return false;
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
            containsString("SET [time_zone] is not allowed in views"),
            () -> createView("my_view", "SET time_zone=\"Europe/Berlin\"; FROM index")
        );
    }

    public void testBoundaryScopedSettingsAreAccepted() {
        assertAcked(createView("load_view", "SET unmapped_fields=\"load\"; FROM index"));
        assertAcked(createView("null_view", "SET unmapped_fields=\"nullify\"; FROM index"));
        assertAcked(createView("views_view", "SET wildcards_match_views=true; FROM index"));
        assertAcked(createView("datasets_view", "SET wildcards_match_datasets=false; FROM index"));
    }

    public void testLoadAllInsideViewIsRejected() {
        expectThrows(
            ElasticsearchException.class,
            containsString("SET unmapped_fields=\"LOAD_ALL\" is not supported in views"),
            () -> createView("my_view", "SET unmapped_fields=\"LOAD_ALL\"; FROM index")
        );
    }

    public void testTsWithoutStatsIsRejected() {
        expectThrows(
            IllegalArgumentException.class,
            containsString("view [ts-view] uses TS, which must be followed by a STATS command"),
            () -> createView("ts-view", "TS metrics | WHERE cpu > 1 | KEEP cpu")
        );
    }

    public void testTsWithStatsIsAccepted() {
        assertAcked(createView("ts-view", "TS metrics | STATS max(cpu) BY host"));
        assertAcked(createView("ts-view", "TS metrics | FORK (STATS max(cpu)) (WHERE cpu > 1)"));
    }

    public void testPromqlIsAccepted() {
        assertAcked(createView("promql-view", "PROMQL index=metrics step=1m cpu_avg=(avg(cpu))"));
        assertAcked(createView("promql-view", "PROMQL index=metrics step=1m cpu_avg=(avg(cpu)) | WHERE cpu_avg > 1 | KEEP cpu_avg"));
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
        assertAcked(createView("ts-view-1", "TS ts-source | STATS total = sum(avg_over_time(cpu)) | EVAL f1=1"));
        try (EsqlQueryResponse response = run("FROM ts-view-1 | EVAL f2=2")) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(40.0, 1, 2))));
        }
        expectThrows(
            VerificationException.class,
            containsString("Views are not supported in TS command, found view(s) [ts-view-1]"),
            () -> run("TS ts-view-1 | EVAL f2=2").close()
        );
        expectThrows(
            VerificationException.class,
            containsString("Views are not supported in TS command, found view(s) [ts-view-1]"),
            () -> run("TS ts-source,ts-view-1 | EVAL f2=2").close()
        );
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

    /**
     * Reads a reserved view through {@code GET /_query/view} and asserts the response body contains
     * {@code reserved: true}. A plain view omits the field. The view is created through the internal client
     * because {@code reserved} is not part of the public create API.
     */
    public void testGetViewRestResponseIncludesReserved() throws IOException {
        String reservedName = "reserved-rest-view";
        String plainName = "plain-rest-view";
        String query = "FROM some-index";
        String description = "installed by the cluster";
        boolean reservedCreated = false;
        boolean plainCreated = false;
        try {
            assertAcked(createView(reservedName, query, description, true));
            reservedCreated = true;
            assertAcked(createView(plainName, query, null, false));
            plainCreated = true;

            Map<String, Object> reserved = getViewFromRest(reservedName);
            assertThat(reserved.get("name"), equalTo(reservedName));
            assertThat(reserved.get("query"), equalTo(query));
            assertThat(reserved.get("description"), equalTo(description));
            assertThat(reserved.get("reserved"), equalTo(true));

            Map<String, Object> plain = getViewFromRest(plainName);
            assertThat(plain.get("name"), equalTo(plainName));
            assertThat(plain.get("query"), equalTo(query));
            assertThat(plain, not(hasKey("reserved")));

            List<Map<String, Object>> listed = listViewsFromRest();
            assertThat(findView(listed, reservedName).get("reserved"), equalTo(true));
            assertThat(findView(listed, plainName), not(hasKey("reserved")));
        } finally {
            if (reservedCreated) {
                deleteView(reservedName, true);
            }
            if (plainCreated) {
                deleteView(plainName, false);
            }
        }
    }

    public void testCommandLimitSettingAppliesToQueriesThatExpandViews() {
        assertAcked(indicesAdmin().prepareCreate("source-index").setMapping("f1", "type=integer"));
        prepareIndex("source-index").setSource("f1", 42).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        String pipes = " | WHERE f1 > 0".repeat(300);
        assertAcked(createView("long-view", "FROM source-index" + pipes));
        String query = "FROM long-view" + pipes;

        expectThrows(
            VerificationException.class,
            startsWith("ES|QL query exceeded the maximum of [500] plan nodes allowed after expanding views, subqueries and FORKs."),
            () -> run(query)
        );

        try {
            updateClusterSettings(Settings.builder().put(EsqlFlags.ESQL_MAX_COMMAND_COUNT.getKey(), 1000));
            try (EsqlQueryResponse response = run(query)) {
                assertThat(getValuesList(response), equalTo(List.of(List.of(42))));
            }
        } finally {
            updateClusterSettings(Settings.builder().putNull(EsqlFlags.ESQL_MAX_COMMAND_COUNT.getKey()));
        }
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

    private Map<String, Object> getViewFromRest(String name) throws IOException {
        Response response = getRestClient().performRequest(new Request("GET", "/_query/view/" + name));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        return singleView(jsonEntityToMap(response.getEntity()));
    }

    private List<Map<String, Object>> listViewsFromRest() throws IOException {
        Response response = getRestClient().performRequest(new Request("GET", "/_query/view"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        return viewsFrom(jsonEntityToMap(response.getEntity()));
    }

    private static Map<String, Object> singleView(Map<String, Object> body) {
        List<Map<String, Object>> views = viewsFrom(body);
        assertThat(views, hasSize(1));
        return views.get(0);
    }

    private static List<Map<String, Object>> viewsFrom(Map<String, Object> body) {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> views = (List<Map<String, Object>>) body.get("views");
        assertNotNull(views);
        return views;
    }

    private static Map<String, Object> findView(List<Map<String, Object>> views, String name) {
        for (Map<String, Object> view : views) {
            if (name.equals(view.get("name"))) {
                return view;
            }
        }
        throw new AssertionError("view [" + name + "] missing from " + views);
    }

    private void deleteView(String name, boolean reserved) {
        assertAcked(
            client().execute(
                DeleteViewAction.INSTANCE,
                new DeleteViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[] { name }, reserved)
            )
        );
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
