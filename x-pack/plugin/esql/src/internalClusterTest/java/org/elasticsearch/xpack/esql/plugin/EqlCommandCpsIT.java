/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsAction;
import org.elasticsearch.xpack.esql.action.SliceSelectionCpsIT;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class EqlCommandCpsIT extends AbstractEsqlIntegTestCase {

    private static final String INDEX = "eql_cps";
    private static final String PROJECT_ROUTING = "_alias:_origin";
    private static final String SET_PROJECT_ROUTING = "SET project_routing=\"" + PROJECT_ROUTING + "\"; ";
    private static final String KEEP_PROCESS_NAMES = " | KEEP process.name | SORT process.name";
    private static final String ALL_PROCESSES_QUERY = "EQL " + INDEX + " \"process where true\"" + KEEP_PROCESS_NAMES;
    private static final List<List<Object>> ALL_PROCESSES = List.of(List.of("cmd.exe"), List.of("explorer.exe"));

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EqlPlugin.class);
        plugins.add(SliceSelectionCpsIT.CpsSettingPlugin.class);
        plugins.add(RequestCapturePlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put("serverless.cross_project.enabled", true).build();
    }

    @Before
    public void setupIndex() {
        assumeTrue("EQL command is snapshot-only", Build.current().isSnapshot());
        assertAcked(
            prepareCreate(INDEX).setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("@timestamp", "type=date", "event.category", "type=keyword", "process.name", "type=keyword")
        );
        client().prepareBulk()
            .add(
                new IndexRequest(INDEX).id("1")
                    .source("@timestamp", "2026-07-22T10:00:00Z", "event.category", "process", "process.name", "cmd.exe")
            )
            .add(
                new IndexRequest(INDEX).id("2")
                    .source("@timestamp", "2026-07-22T10:01:00Z", "event.category", "process", "process.name", "explorer.exe")
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        RequestCapturePlugin.ESQL_RESOLUTIONS.clear();
        RequestCapturePlugin.EQL_SEARCHES.clear();
        RequestCapturePlugin.EQL_FIELD_CAPS.clear();
    }

    public void testEqlRequestsUseCrossProjectOptions() {
        assertEqlSourceRows(ALL_PROCESSES_QUERY, ALL_PROCESSES, null);
    }

    public void testProjectRoutingReachesEqlSearch() {
        assertEqlSourceRows(SET_PROJECT_ROUTING + ALL_PROCESSES_QUERY, ALL_PROCESSES, PROJECT_ROUTING);
    }

    public void testProjectRoutingReachesEqlSequence() {
        assertEqlSourceRows(
            SET_PROJECT_ROUTING
                + "EQL "
                + INDEX
                + " \"sequence [process where process.name == \\\"cmd.exe\\\"] [process where process.name == \\\"explorer.exe\\\"]\""
                + KEEP_PROCESS_NAMES,
            ALL_PROCESSES,
            PROJECT_ROUTING
        );
    }

    public void testProjectRoutingReachesEqlSubquery() {
        assertEqlSourceRows(
            SET_PROJECT_ROUTING
                + "FROM "
                + INDEX
                + " | WHERE process.name IN (EQL "
                + INDEX
                + " \"process where process.name == \\\"cmd.exe\\\"\" | KEEP process.name)"
                + KEEP_PROCESS_NAMES,
            List.of(List.of("cmd.exe")),
            PROJECT_ROUTING
        );
    }

    private void assertEqlSourceRows(String query, List<List<Object>> expectedRows, String expectedProjectRouting) {
        try (EsqlQueryResponse response = run(query)) {
            assertThat(getValuesList(response), equalTo(expectedRows));
            assertThat(response.isPartial(), equalTo(false));
        }
        assertThat(RequestCapturePlugin.ESQL_RESOLUTIONS, not(empty()));
        assertThat(RequestCapturePlugin.EQL_SEARCHES, hasSize(1));
        assertThat(
            "field caps are never reused across projects, so the EQL engine resolves them",
            RequestCapturePlugin.EQL_FIELD_CAPS,
            not(empty())
        );
        Captured search = RequestCapturePlugin.EQL_SEARCHES.peek();
        assertThat(search.indicesOptions().resolveCrossProjectIndexExpression(), equalTo(true));
        assertThat(search.projectRouting(), equalTo(expectedProjectRouting));
        assertThat(RequestCapturePlugin.ESQL_RESOLUTIONS, everyItem(equalTo(search)));
        assertThat(RequestCapturePlugin.EQL_FIELD_CAPS, everyItem(equalTo(search)));
    }

    record Captured(IndicesOptions indicesOptions, String projectRouting) {}

    public static class RequestCapturePlugin extends Plugin implements ActionPlugin {
        static final Queue<Captured> ESQL_RESOLUTIONS = new ConcurrentLinkedQueue<>();
        static final Queue<Captured> EQL_SEARCHES = new ConcurrentLinkedQueue<>();
        static final Queue<Captured> EQL_FIELD_CAPS = new ConcurrentLinkedQueue<>();

        @Override
        public Collection<ActionFilter> getActionFilters() {
            return List.of(new ActionFilter() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Request request,
                    ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain
                ) {
                    if (request instanceof IndicesRequest.Replaceable replaceable
                        && replaceable.indices() != null
                        && Arrays.asList(replaceable.indices()).contains(INDEX)) {
                        Captured captured = new Captured(replaceable.indicesOptions(), replaceable.getProjectRouting());
                        if (EsqlResolveFieldsAction.NAME.equals(action)) {
                            ESQL_RESOLUTIONS.add(captured);
                        } else if (EqlSearchAction.NAME.equals(action)) {
                            EQL_SEARCHES.add(captured);
                        } else if (TransportFieldCapabilitiesAction.NAME.equals(action)) {
                            EQL_FIELD_CAPS.add(captured);
                        }
                    }
                    chain.proceed(task, action, request, listener);
                }
            });
        }
    }
}
