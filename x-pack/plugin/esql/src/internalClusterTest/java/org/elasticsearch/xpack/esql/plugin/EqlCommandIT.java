/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

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

    public void testEventQueryMetadataColumns() {
        String query = "EQL " + INDEX + " \"process where true\" METADATA _index, _id, _source | KEEP process.name, _index, _id, _source";
        try (EsqlQueryResponse resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("process.name", "_index", "_id", "_source"));
            assertColumnTypes(resp.columns(), List.of("keyword", "keyword", "keyword", "_source"));

            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(2));
            for (List<Object> row : rows) {
                assertEquals("eql_events", Objects.toString(row.get(1))); // _index — the concrete index, both rows
            }
            List<Object> cmd = rows.stream().filter(r -> "cmd.exe".equals(Objects.toString(r.get(0)))).findFirst().orElseThrow();
            assertEquals("p1", Objects.toString(cmd.get(2)));                 // _id matches the seeded doc id
            assertThat(cmd.get(3), instanceOf(Map.class));                    // _source parses to a map
            assertThat(((Map<?, ?>) cmd.get(3)).get("process.name"), equalTo("cmd.exe"));
        }
    }

    public void testSequenceQueryMetadataColumns() {
        String query = "EQL "
            + INDEX
            + " \"sequence by process.pid [process where true] [network where true]\" METADATA _id | KEEP _sequence_stage, _id";
        try (EsqlQueryResponse resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("_sequence_stage", "_id"));
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(2));
            // _id tracks the per-event envelope through unnesting: stage 0 is the process event, stage 1 the network one.
            assertEquals(0, rows.get(0).get(0));
            assertEquals("p1", Objects.toString(rows.get(0).get(1)));
            assertEquals(1, rows.get(1).get(0));
            assertEquals("n1", Objects.toString(rows.get(1).get(1)));
        }
    }

    public void testUnsupportedMetadataFieldFailsE2E() {
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> run("EQL " + INDEX + " \"process where true\" METADATA _score").close()
        );
        assertThat(e.getMessage(), containsString("metadata field [_score] is not supported by the EQL command"));
    }

    public void testRequestFilterOnEqlSourceRejectedE2E() {
        // The EQL delegate does not yet honor the out-of-band request filter; combining them must fail loud rather than
        // silently drop the filter and return rows it should have excluded.
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> run(syncEsqlQueryRequest("EQL " + INDEX + " \"process where true\"").filter(new MatchAllQueryBuilder())).close()
        );
        assertThat(e.getMessage(), containsString("cannot yet be combined with a request filter"));
    }

    public void testRequestFilterOnEqlSubquerySourceRejectedE2E() {
        // The filter guard runs on the analyzed plan (anyMatch(EqlRelation)), so an EQL source hidden inside a subquery
        // must be rejected in every position, not just at top level: as a FROM subquery, and behind WHERE x IN (EQL ...).
        VerificationException fromSubquery = expectThrows(
            VerificationException.class,
            () -> run(syncEsqlQueryRequest("FROM (EQL " + INDEX + " \"process where true\")").filter(new MatchAllQueryBuilder())).close()
        );
        assertThat(fromSubquery.getMessage(), containsString("cannot yet be combined with a request filter"));

        VerificationException inSubquery = expectThrows(
            VerificationException.class,
            () -> run(
                syncEsqlQueryRequest(
                    "FROM " + INDEX + " | WHERE process.pid IN (EQL " + INDEX + " \"process where true\" | KEEP process.pid)"
                ).filter(new MatchAllQueryBuilder())
            ).close()
        );
        assertThat(inSubquery.getMessage(), containsString("cannot yet be combined with a request filter"));
    }

    public void testDelegateRuntimeErrorSurfacesE2E() {
        // A query that passes ES|QL analysis but fails inside the EQL engine (an unknown timestamp_field) must surface
        // the engine's error to the client rather than hang or come back empty.
        Exception e = expectThrows(
            Exception.class,
            () -> run("EQL " + INDEX + " \"process where true\" WITH {\"timestamp_field\": \"no_such_ts\"}").close()
        );
        assertThat(e.getMessage(), containsString("no_such_ts"));
        // ES|QL passes timestamp_field through opaquely, so the error must originate in the EQL delegate, not ES|QL
        // analysis — pin that origin so a future change that resolves the field ES|QL-side does not silently pass here.
        assertThat(e, not(instanceOf(VerificationException.class)));
    }

    public void testLimitDrivesSizeAndSuppressesTruncationWarning() throws Exception {
        // Lower the cap to 1. A pushed LIMIT 1 must set size from the LIMIT (not the cap), so NO truncation warning
        // fires. If the pushed limit failed to reach the request, size would fall back to the cap and the warning
        // WOULD fire at size == cap == 1 — so asserting its absence discriminates the whole plumbing chain.
        updateClusterSettings(Settings.builder().put("esql.query.result_truncation_max_size", 1));
        try {
            CapturedQuery result = runCapturingWarnings("EQL " + INDEX + " \"process where true\" | LIMIT 1");
            assertThat(result.rows(), hasSize(1));
            assertTrue(
                "a LIMIT-driven size must not warn about truncation, got: " + result.warnings(),
                result.warnings().stream().noneMatch(w -> w.contains("results may be incomplete"))
            );
        } finally {
            updateClusterSettings(Settings.builder().putNull("esql.query.result_truncation_max_size"));
        }
    }

    public void testTruncationWarningWhenCapHit() throws Exception {
        // Lower the cap below the number of process events. STATS has no LIMIT pushed past the aggregation, so the
        // request size defaults to the cap and the (real) truncation must surface a client Warning.
        updateClusterSettings(Settings.builder().put("esql.query.result_truncation_max_size", 1));
        try {
            CapturedQuery result = runCapturingWarnings("EQL " + INDEX + " \"process where true\" | STATS count = COUNT(*)");
            assertThat("STATS must count only the capped events", ((Number) result.rows().get(0).get(0)).longValue(), equalTo(1L));
            assertTrue(
                "client must receive a truncation Warning, got: " + result.warnings(),
                result.warnings().stream().anyMatch(w -> w.contains("results may be incomplete"))
            );
        } finally {
            updateClusterSettings(Settings.builder().putNull("esql.query.result_truncation_max_size"));
        }
    }

    public void testUnmappedLoadReturnsSourceValues() {
        setupUnmappedIndex();
        // note lives only in _source (the index is dynamic:false); LOAD must fetch it via the fields API's
        // include_unmapped — the E2E proof that the flag survives EqlSearchRequest → SourceGenerator → fetch phase.
        String query =
            "SET unmapped_fields = \"load\"; EQL eql_unmapped \"process where true\" | KEEP process.name, note | SORT process.name";
        try (EsqlQueryResponse resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("process.name", "note"));
            assertColumnTypes(resp.columns(), List.of("keyword", "keyword"));
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(2));
            assertEquals("alpha", Objects.toString(rows.get(0).get(1)));  // cmd.exe
            assertEquals("beta", Objects.toString(rows.get(1).get(1)));   // powershell.exe
        }
    }

    public void testUnmappedLoadSequenceReturnsSourceValues() {
        setupUnmappedIndex();
        // Sequence reconstruction uses a different EQL fetch path (BasicQueryClient) than event queries; prove
        // include_unmapped is honored there too.
        String query = "SET unmapped_fields = \"load\"; EQL eql_unmapped "
            + "\"sequence by process.pid [process where true] [network where true]\" | KEEP _sequence_stage, note";
        try (EsqlQueryResponse resp = run(query)) {
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(2));
            assertEquals("alpha", Objects.toString(rows.get(0).get(1)));  // process event p1
            assertEquals("gamma", Objects.toString(rows.get(1).get(1)));  // network event n1
        }
    }

    public void testUnmappedNullifyReturnsNulls() {
        setupUnmappedIndex();
        String query = "SET unmapped_fields = \"nullify\"; EQL eql_unmapped \"process where true\" | KEEP process.name, note";
        try (EsqlQueryResponse resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("process.name", "note"));
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(2));
            rows.forEach(row -> assertNull("nullify must not leak the _source value", row.get(1)));
        }
    }

    public void testUnmappedDefaultFailsUnknownColumnE2E() {
        setupUnmappedIndex();
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> run("EQL eql_unmapped \"process where true\" | KEEP note").close()
        );
        assertThat(e.getMessage(), containsString("Unknown column [note]"));
    }

    private void setupUnmappedIndex() {
        // dynamic:false so `note` stays in _source without being added to the mapping — a genuinely unmapped field.
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("eql_unmapped")
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("""
                    {"dynamic":false,"properties":{
                      "@timestamp":{"type":"date"},
                      "event.category":{"type":"keyword"},
                      "process.name":{"type":"keyword"},
                      "process.pid":{"type":"long"}
                    }}""")
        );
        client().prepareBulk()
            .add(
                new IndexRequest("eql_unmapped").id("p1")
                    .source(
                        "@timestamp",
                        "2026-07-22T10:00:00Z",
                        "event.category",
                        "process",
                        "process.name",
                        "cmd.exe",
                        "process.pid",
                        100,
                        "note",
                        "alpha"
                    )
            )
            .add(
                new IndexRequest("eql_unmapped").id("n1")
                    .source("@timestamp", "2026-07-22T10:00:01Z", "event.category", "network", "process.pid", 100, "note", "gamma")
            )
            .add(
                new IndexRequest("eql_unmapped").id("p2")
                    .source(
                        "@timestamp",
                        "2026-07-22T10:00:02Z",
                        "event.category",
                        "process",
                        "process.name",
                        "powershell.exe",
                        "process.pid",
                        200,
                        "note",
                        "beta"
                    )
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    private record CapturedQuery(List<List<Object>> rows, List<String> warnings) {}

    /**
     * Runs a query and captures both its rows and the coordinator's response {@code Warning} headers — the only way
     * to observe a runtime warning end to end. Mirrors the header capture in {@code ExternalMaxRecordSizeTruncationIT}
     * / {@code WarningsIT}: {@code ActionListener.wrap} (not {@code run()}) since the transport client owns the
     * response ref-count, so we must not close it here.
     */
    private CapturedQuery runCapturingWarnings(String query) throws Exception {
        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<List<List<Object>>> rows = new AtomicReference<>(List.of());
        List<String> warnings = new CopyOnWriteArrayList<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query), ActionListener.wrap(response -> {
            try {
                rows.set(getValuesList(response));
                ThreadContext threadContext = internalCluster().getInstance(TransportService.class, coordinator.getName())
                    .getThreadPool()
                    .getThreadContext();
                warnings.addAll(threadContext.getResponseHeaders().getOrDefault("Warning", List.of()));
            } finally {
                latch.countDown();
            }
        }, e -> {
            failure.set(e);
            latch.countDown();
        }));
        assertTrue("query did not complete within 1 minute", latch.await(1, TimeUnit.MINUTES));
        if (failure.get() != null) {
            throw new AssertionError("query must not fail", failure.get());
        }
        return new CapturedQuery(rows.get(), warnings);
    }

    private static List<Object> rowByName(List<List<Object>> rows, String processName) {
        return rows.stream().filter(row -> processName.equals(Objects.toString(row.get(2)))).findFirst().orElseThrow();
    }
}
