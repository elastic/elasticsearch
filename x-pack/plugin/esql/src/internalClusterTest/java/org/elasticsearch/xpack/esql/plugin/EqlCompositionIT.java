/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.Build;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * End-to-end integration test for the {@code EQL} source command in composition contexts: as a subquery source
 * ({@code FROM (EQL ...)}), on the right-hand side of {@code WHERE x IN (EQL ...)}, and as the upstream of
 * {@code FORK}. The EQL portion executes coordinator-side (it delegates to {@code EqlSearchAction} and has no
 * data-node fragment); sibling {@code FROM} branches still distribute. This test pins real typed merged VALUES,
 * proving the coordinator-only leaf composes soundly through the subplan/merge machinery.
 *
 * @see EqlCommandIT for the top-level command E2E test whose setup this mirrors.
 */
public class EqlCompositionIT extends AbstractEsqlIntegTestCase {

    private static final String INDEX = "eql_events";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // The EQL source command delegates to the EQL engine, so the EQL plugin must be loaded on the nodes.
        return CollectionUtils.appendToCopy(super.nodePlugins(), EqlPlugin.class);
    }

    @Before
    public void setupIndex() {
        assumeTrue("EQL command is snapshot-only", Build.current().isSnapshot());
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "event.category",
                    "type=keyword",
                    "process.name",
                    "type=keyword",
                    "process.pid",
                    "type=long"
                )
        );
        client().prepareBulk()
            .add(
                new IndexRequest(INDEX).id("p1")
                    .source(
                        "@timestamp",
                        "2026-07-22T10:00:00Z",
                        "event.category",
                        "process",
                        "process.name",
                        "cmd.exe",
                        "process.pid",
                        100
                    )
            )
            .add(
                new IndexRequest(INDEX).id("n1")
                    .source("@timestamp", "2026-07-22T10:00:01Z", "event.category", "network", "process.pid", 100)
            )
            .add(
                new IndexRequest(INDEX).id("p2")
                    .source(
                        "@timestamp",
                        "2026-07-22T10:00:02Z",
                        "event.category",
                        "process",
                        "process.name",
                        "powershell.exe",
                        "process.pid",
                        200
                    )
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    public void testEqlAsSubquerySource() {
        // A lone EQL subquery collapses to the EQL relation; the piped KEEP/SORT compose over its typed columns.
        String query = "FROM (EQL " + INDEX + " \"process where true\" | KEEP process.name, process.pid) | SORT process.name";
        try (EsqlQueryResponse resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("process.name", "process.pid"));
            assertColumnTypes(resp.columns(), List.of("keyword", "long"));
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(2)); // the two process events
            assertThat(Objects.toString(rows.get(0).get(0)), equalTo("cmd.exe"));
            assertEquals(100L, rows.get(0).get(1));
            assertThat(Objects.toString(rows.get(1).get(0)), equalTo("powershell.exe"));
            assertEquals(200L, rows.get(1).get(1));
        }
    }

    public void testEqlAndFromAsSubquerySiblings() {
        // One subquery branch is an EQL source, the sibling is a FROM source; the union merges typed rows.
        String query = "FROM (EQL "
            + INDEX
            + " \"process where process.pid == 100\" | KEEP process.name, process.pid),"
            + " (FROM "
            + INDEX
            + " | WHERE event.category == \"process\" AND process.pid == 200 | KEEP process.name, process.pid)"
            + " | STATS count = COUNT(*) BY process.name | SORT process.name";
        try (EsqlQueryResponse resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("count", "process.name"));
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(2));
            assertThat(Objects.toString(rows.get(0).get(1)), equalTo("cmd.exe"));       // from the EQL branch
            assertEquals(1L, rows.get(0).get(0));
            assertThat(Objects.toString(rows.get(1).get(1)), equalTo("powershell.exe")); // from the FROM branch
            assertEquals(1L, rows.get(1).get(0));
        }
    }

    public void testEqlAsWhereInSubquerySource() {
        // The same subquery rule feeds WHERE x IN (...); the EQL branch supplies the pid set {100}.
        String query = "FROM "
            + INDEX
            + " | WHERE process.pid IN (EQL "
            + INDEX
            + " \"process where process.pid == 100\" | KEEP process.pid)"
            + " | STATS count = COUNT(*)";
        try (EsqlQueryResponse resp = run(query)) {
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(1));
            // Both docs with pid 100 (the process event p1 and the network event n1) match the IN set.
            assertEquals(2L, rows.get(0).get(0));
        }
    }

    public void testEqlUpstreamOfFork() {
        // EQL as the FORK upstream: the coordinator-side EQL leaf is replicated into each branch subplan (so the
        // delegated EQL search runs once per branch); the merged result is typed and correct. Every branch reads the
        // same retained field-caps (a read, not a consume), so all branches reuse the coordinator's resolution.
        String query = "EQL "
            + INDEX
            + " \"process where true\""
            + " | FORK ( WHERE process.pid == 100 ) ( WHERE process.pid == 200 )"
            + " | KEEP process.name, process.pid, _fork | SORT process.pid";
        try (EsqlQueryResponse resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("process.name", "process.pid", "_fork"));
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(2));
            assertThat(Objects.toString(rows.get(0).get(0)), equalTo("cmd.exe"));
            assertEquals(100L, rows.get(0).get(1));
            assertThat(Objects.toString(rows.get(0).get(2)), equalTo("fork1"));
            assertThat(Objects.toString(rows.get(1).get(0)), equalTo("powershell.exe"));
            assertEquals(200L, rows.get(1).get(1));
            assertThat(Objects.toString(rows.get(1).get(2)), equalTo("fork2"));
        }
    }

    public void testEqlSubquerySourceColumnNamesMatchTopLevel() {
        // The subquery-sourced EQL exposes the same typed schema as the top-level command (name-sorted field-caps).
        try (EsqlQueryResponse resp = run("FROM (EQL " + INDEX + " \"process where true\")")) {
            assertColumnNames(resp.columns(), List.of("@timestamp", "event.category", "process.name", "process.pid"));
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(2));
            List<String> names = rows.stream().map(r -> Objects.toString(r.get(2))).toList();
            assertThat(names, containsInAnyOrder("cmd.exe", "powershell.exe"));
        }
    }

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);
    private static final String EQL_VIEW = "eql_process_view";

    @After
    public void cleanupView() {
        // Views are project-custom metadata and survive the framework index wipe, so delete explicitly. Tests
        // that never created the view raise ResourceNotFoundException on delete — ignore it (mirrors
        // FromDatasetSubqueryIT#cleanupViews).
        if (Build.current().isSnapshot() == false) {
            return;
        }
        try {
            client().execute(DeleteViewAction.INSTANCE, new DeleteViewAction.Request(TIMEOUT, TIMEOUT, new String[] { EQL_VIEW }))
                .actionGet(TIMEOUT);
        } catch (ResourceNotFoundException ignored) {
            // no view created by this test
        }
    }

    public void testViewBodyIsEqlCommand() {
        // A stored view whose body is an EQL command, reached through FROM <view>. The body is not a bare relation,
        // so it rides the NamedSubquery -> UnionAll -> mapFork machinery with a coordinator-only EQL leaf — no
        // production code beyond the view store itself.
        createEqlView();
        try (EsqlQueryResponse resp = run("FROM " + EQL_VIEW + " | STATS count = COUNT(*)")) {
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(1));
            assertEquals(2L, rows.get(0).get(0)); // the two process events
        }
    }

    public void testViewBodyIsEqlCommandInsideSubquery() {
        // The EQL-bodied view reached from inside a subquery — the "view backdoor" composition path.
        createEqlView();
        try (EsqlQueryResponse resp = run("FROM (FROM " + EQL_VIEW + " | KEEP process.name, process.pid) | SORT process.name")) {
            assertColumnNames(resp.columns(), List.of("process.name", "process.pid"));
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(2));
            assertThat(Objects.toString(rows.get(0).get(0)), equalTo("cmd.exe"));
            assertEquals(100L, rows.get(0).get(1));
            assertThat(Objects.toString(rows.get(1).get(0)), equalTo("powershell.exe"));
            assertEquals(200L, rows.get(1).get(1));
        }
    }

    private void createEqlView() {
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TIMEOUT, TIMEOUT, new View(EQL_VIEW, "EQL " + INDEX + " \"process where true\""))
            ).actionGet(TIMEOUT)
        );
    }
}
