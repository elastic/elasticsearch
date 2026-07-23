/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.Build;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

/**
 * End-to-end integration test for the {@code EQL <indexPattern> "<query>"} source command. Loads the EQL plugin
 * into the test cluster alongside ES|QL and exercises the real delegation path: the ES|QL coordinator resolves
 * the target index's mapping through field-caps, issues an {@code EqlSearchAction} requesting those fields, and
 * converts the response into typed rows. This test pins real typed VALUES (not just column names) so it catches
 * fields-API value-shape surprises the unit tests cannot model.
 */
public class EqlCommandIT extends AbstractEsqlIntegTestCase {

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

    public void testEventQueryReturnsTypedRows() {
        // Columns are the mapping resolved through field-caps, name-sorted, one typed column per field.
        String query = "EQL " + INDEX + " \"process where true\"";
        try (EsqlQueryResponse resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("@timestamp", "event.category", "process.name", "process.pid"));
            assertColumnTypes(resp.columns(), List.of("date", "keyword", "keyword", "long"));

            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(2)); // the two process events, not the network event

            List<String> names = rows.stream().map(row -> Objects.toString(row.get(2))).collect(Collectors.toList());
            assertThat(names, containsInAnyOrder("cmd.exe", "powershell.exe"));

            // Pin real typed values end-to-end: the long pid and the date path (the response renders datetime as ISO).
            List<Object> cmd = rowByName(rows, "cmd.exe");
            assertEquals(100L, cmd.get(3));
            assertEquals("2026-07-22T10:00:00.000Z", cmd.get(0));
        }
    }

    public void testSequenceQueryUnnestsToOneRowPerEventWithTypedColumns() {
        // process (pid 100) followed by network (pid 100) forms one sequence; pid 200 has no network follow-up.
        String query = "EQL " + INDEX + " \"sequence by process.pid [process where true] [network where true]\"";
        try (EsqlQueryResponse resp = run(query)) {
            assertColumnNames(
                resp.columns(),
                List.of("_sequence", "_sequence_stage", "join_keys", "@timestamp", "event.category", "process.name", "process.pid")
            );
            assertColumnTypes(resp.columns(), List.of("long", "integer", "keyword", "date", "keyword", "keyword", "long"));

            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(2)); // one sequence, two events (process then network), unnested

            // Row 0: the process event (stage 0). join_keys is the pid the sequence matched on.
            assertEquals(0L, rows.get(0).get(0));
            assertEquals(0, rows.get(0).get(1));
            assertEquals("100", Objects.toString(rows.get(0).get(2)));
            assertEquals("process", Objects.toString(rows.get(0).get(4)));
            assertEquals("cmd.exe", Objects.toString(rows.get(0).get(5)));
            assertEquals(100L, rows.get(0).get(6));

            // Row 1: the network event (stage 1). It has no process.name, so that column is null.
            assertEquals(0L, rows.get(1).get(0));
            assertEquals(1, rows.get(1).get(1));
            assertEquals("network", Objects.toString(rows.get(1).get(4)));
            assertNull(rows.get(1).get(5));
        }
    }

    private static List<Object> rowByName(List<List<Object>> rows, String processName) {
        return rows.stream().filter(row -> processName.equals(Objects.toString(row.get(2)))).findFirst().orElseThrow();
    }
}
