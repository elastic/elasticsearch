/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.Build;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.FailingFieldPlugin;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
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
    private static final String PARTIAL_PATTERN = "eql_partial_*";
    private static final String PARTIAL_SEQUENCE = "sequence [process where value == 1 or fail_me == 1] [process where value == 3]";
    private static final List<String> PARTIAL_SEQUENCE_IDS = List.of("ok1", "ok3");

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // The EQL source command delegates to the EQL engine, so the EQL plugin must be loaded on the nodes.
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EqlPlugin.class);
        plugins.add(FailingFieldPlugin.class);
        return plugins;
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

    public void testLimitTrimsRowsOfTheDefaultTailResult() {
        assertSingleProcessName("EQL " + INDEX + " \"process where true\" | LIMIT 1", "cmd.exe");
        assertSingleProcessName("EQL " + INDEX + " \"process where true\" | KEEP process.name | LIMIT 1", "cmd.exe");
    }

    public void testHeadResultPositionReturnsTheOldestEvent() {
        assertSingleProcessName("EQL " + INDEX + " \"process where true\" WITH {\"result_position\": \"head\", \"size\": 1}", "cmd.exe");
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

    public void testRequestFilterMatchesEqlSearchForEventQuery() {
        List<QueryBuilder> filters = List.of(
            new MatchAllQueryBuilder(),
            new BoolQueryBuilder(),
            new TermQueryBuilder("process.name", "cmd.exe"),
            new RangeQueryBuilder("@timestamp").gte("2026-07-22T10:00:01Z")
        );
        for (QueryBuilder filter : filters) {
            List<String> expected = eqlSearchIds("process where true", filter);
            assertThat(filter.toString(), expected, not(hasSize(0)));
            assertThat(filter.toString(), esqlIds("process where true", filter), equalTo(expected));
        }
    }

    public void testRequestFilterMatchesEqlSearchForSequenceQuery() {
        String sequence = "sequence by process.pid [process where true] [network where true]";
        assertThat(esqlIds(sequence, new MatchAllQueryBuilder()), equalTo(List.of("n1", "p1")));
        QueryBuilder samePid = new TermQueryBuilder("process.pid", 100);
        assertThat(eqlSearchIds(sequence, samePid), equalTo(List.of("n1", "p1")));
        assertThat(esqlIds(sequence, samePid), equalTo(List.of("n1", "p1")));
        List<QueryBuilder> withoutSequence = List.of(
            new TermQueryBuilder("process.pid", 200),
            new RangeQueryBuilder("@timestamp").lte("2026-07-22T10:00:00Z")
        );
        for (QueryBuilder filter : withoutSequence) {
            assertThat(filter.toString(), eqlSearchIds(sequence, filter), hasSize(0));
            assertThat(filter.toString(), esqlIds(sequence, filter), hasSize(0));
        }
    }

    public void testRequestFilterAppliesBeforeTheEqlResultSize() {
        QueryBuilder filter = new RangeQueryBuilder("@timestamp").lte("2026-07-22T10:00:01Z");
        QueryBuilder afterFirstEvent = new RangeQueryBuilder("@timestamp").gte("2026-07-22T10:00:01Z");
        assertSingleProcessName(
            syncEsqlQueryRequest("EQL " + INDEX + " \"process where true\" WITH {\"result_position\": \"head\"} | LIMIT 1").filter(
                afterFirstEvent
            ),
            "powershell.exe"
        );
        assertSingleProcessName(
            syncEsqlQueryRequest("EQL " + INDEX + " \"process where true\" WITH {\"size\": 1}").filter(filter),
            "cmd.exe"
        );
    }

    public void testRequestFilterDoesNotNarrowTheEqlColumns() {
        for (String index : List.of("eql_window_a", "eql_window_b")) {
            String onlyField = index.equals("eql_window_a") ? "only_a" : "only_b";
            assertAcked(
                client().admin()
                    .indices()
                    .prepareCreate(index)
                    .setMapping("@timestamp", "type=date", "event.category", "type=keyword", onlyField, "type=keyword")
            );
            client().prepareIndex(index)
                .setSource("@timestamp", "2026-07-22T10:00:00Z", "event.category", "process", onlyField, "v")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }
        String query = "EQL eql_window_* \"process where true\"";
        try (EsqlQueryResponse resp = run(syncEsqlQueryRequest(query).filter(new ExistsQueryBuilder("only_a")))) {
            assertColumnNames(resp.columns(), List.of("@timestamp", "event.category", "only_a", "only_b"));
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(1));
            assertEquals("v", Objects.toString(rows.get(0).get(2)));
            assertNull(rows.get(0).get(3));
        }
    }

    public void testTimeRangeOfAFromDoesNotNarrowTheEqlColumnsOnItsPattern() {
        for (String index : List.of("eql_range_old", "eql_range_new")) {
            boolean old = index.equals("eql_range_old");
            String onlyField = old ? "old_only" : "new_only";
            assertAcked(
                client().admin()
                    .indices()
                    .prepareCreate(index)
                    .setMapping("@timestamp", "type=date", "event.category", "type=keyword", onlyField, "type=keyword")
            );
            client().prepareIndex(index)
                .setSource("@timestamp", old ? "2020-01-01T00:00:00Z" : "2026-07-22T10:00:00Z", "event.category", "process", onlyField, "v")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }
        String query = "FROM (FROM eql_range_* | WHERE @timestamp >= \"2024-01-01T00:00:00Z\"), (EQL eql_range_* \"process where true\")";
        try (EsqlQueryResponse resp = run(query)) {
            List<String> names = resp.columns().stream().map(column -> column.name()).toList();
            assertThat(names, containsInAnyOrder("@timestamp", "event.category", "new_only", "old_only"));
            int oldOnly = names.indexOf("old_only");
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(3));
            assertThat(rows.stream().filter(row -> "v".equals(Objects.toString(row.get(oldOnly), null))).count(), equalTo(1L));
        }
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

    public void testEventQueryWarnsOnALostShard() throws Exception {
        setupFailingShardIndices();
        String query = "EQL " + PARTIAL_PATTERN + " \"process where value == 1 or fail_me == 1\" METADATA _id | KEEP _id";
        assertPartialResults(query, List.of("ok1"));
    }

    public void testPartialSequenceResultsFollowTheEqlClusterDefault() throws Exception {
        setupFailingShardIndices();
        assertPartialResults(partialSequenceQuery(""), List.of());
        assertThat(eqlSearchIds(partialSequenceSearch()), empty());
        updateClusterSettings(Settings.builder().put(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEQUENCE_RESULTS.getKey(), true));
        try {
            assertPartialResults(partialSequenceQuery(""), PARTIAL_SEQUENCE_IDS);
            assertThat(eqlSearchIds(partialSequenceSearch()), equalTo(PARTIAL_SEQUENCE_IDS));
        } finally {
            updateClusterSettings(Settings.builder().putNull(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEQUENCE_RESULTS.getKey()));
        }
    }

    public void testWithOptionOverridesTheEqlClusterDefaultForPartialSequences() throws Exception {
        setupFailingShardIndices();
        assertPartialResults(partialSequenceQuery(" WITH {\"allow_partial_sequence_results\": true}"), PARTIAL_SEQUENCE_IDS);
        updateClusterSettings(Settings.builder().put(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEQUENCE_RESULTS.getKey(), true));
        try {
            assertPartialResults(partialSequenceQuery(" WITH {\"allow_partial_sequence_results\": false}"), List.of());
        } finally {
            updateClusterSettings(Settings.builder().putNull(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEQUENCE_RESULTS.getKey()));
        }
    }

    public void testPartialSequenceClusterDefaultDoesNotAllowPartialSearchResults() {
        setupFailingShardIndices();
        updateClusterSettings(Settings.builder().put(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEQUENCE_RESULTS.getKey(), true));
        try {
            Exception e = expectThrows(
                Exception.class,
                () -> run(syncEsqlQueryRequest(partialSequenceQuery("")).allowPartialResults(false)).close()
            );
            assertThat(ExceptionsHelper.stackTrace(e), containsString("Accessing failing field"));
        } finally {
            updateClusterSettings(Settings.builder().putNull(EqlPlugin.DEFAULT_ALLOW_PARTIAL_SEQUENCE_RESULTS.getKey()));
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

    private void setupFailingShardIndices() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("eql_partial_ok")
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("@timestamp", "type=date", "event.category", "type=keyword", "value", "type=long")
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("eql_partial_fail")
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("""
                    {"properties":{
                      "@timestamp":{"type":"date"},
                      "event.category":{"type":"keyword"},
                      "value":{"type":"long"}
                    },
                    "runtime":{"fail_me":{"type":"long","script":{"source":"","lang":"failing_field"}}}}""")
        );
        client().prepareBulk()
            .add(
                new IndexRequest("eql_partial_ok").id("ok1")
                    .source("@timestamp", "2026-07-22T10:00:00Z", "event.category", "process", "value", 1)
            )
            .add(
                new IndexRequest("eql_partial_fail").id("fail2")
                    .source("@timestamp", "2026-07-22T10:00:01Z", "event.category", "process", "value", 2)
            )
            .add(
                new IndexRequest("eql_partial_ok").id("ok3")
                    .source("@timestamp", "2026-07-22T10:00:02Z", "event.category", "process", "value", 3)
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    private void assertPartialResults(String query, List<String> expectedIds) throws Exception {
        CapturedQuery result = runCapturingWarnings(syncEsqlQueryRequest(query).allowPartialResults(true));
        assertThat(result.rows().stream().map(row -> Objects.toString(row.get(0))).sorted().toList(), equalTo(expectedIds));
        assertTrue(
            "a lost shard must surface as a partial-results warning, got: " + result.warnings(),
            result.warnings().stream().anyMatch(w -> w.contains("EQL query returned partial results"))
        );
    }

    private static String partialSequenceQuery(String with) {
        return "EQL " + PARTIAL_PATTERN + " \"" + PARTIAL_SEQUENCE + "\" METADATA _id" + with + " | KEEP _id";
    }

    private static EqlSearchRequest partialSequenceSearch() {
        return new EqlSearchRequest().indices(PARTIAL_PATTERN).query(PARTIAL_SEQUENCE).allowPartialSearchResults(true);
    }

    private record CapturedQuery(List<List<Object>> rows, List<String> warnings) {}

    private CapturedQuery runCapturingWarnings(EsqlQueryRequest request) throws Exception {
        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<List<List<Object>>> rows = new AtomicReference<>(List.of());
        List<String> warnings = new CopyOnWriteArrayList<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
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

    private List<String> esqlIds(String eqlQuery, QueryBuilder filter) {
        String query = "EQL " + INDEX + " \"" + eqlQuery + "\" METADATA _id | KEEP _id";
        try (EsqlQueryResponse resp = run(syncEsqlQueryRequest(query).filter(filter))) {
            return getValuesList(resp).stream().map(row -> Objects.toString(row.get(0))).sorted().toList();
        }
    }

    private List<String> eqlSearchIds(String eqlQuery, QueryBuilder filter) {
        return eqlSearchIds(new EqlSearchRequest().indices(INDEX).query(eqlQuery).filter(filter));
    }

    private List<String> eqlSearchIds(EqlSearchRequest request) {
        EqlSearchResponse response = client().execute(EqlSearchAction.INSTANCE, request).actionGet();
        try {
            EqlSearchResponse.Hits hits = response.hits();
            Stream<EqlSearchResponse.Event> events = hits.sequences() != null
                ? hits.sequences().stream().flatMap(sequence -> sequence.events().stream())
                : hits.events().stream();
            return events.map(EqlSearchResponse.Event::id).sorted().toList();
        } finally {
            response.decRef();
        }
    }

    private void assertSingleProcessName(String query, String expected) {
        assertSingleProcessName(syncEsqlQueryRequest(query), expected);
    }

    private void assertSingleProcessName(EsqlQueryRequest request, String expected) {
        try (EsqlQueryResponse resp = run(request)) {
            int nameColumn = resp.columns().stream().map(c -> c.name()).toList().indexOf("process.name");
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(1));
            assertEquals(expected, Objects.toString(rows.get(0).get(nameColumn)));
        }
    }

    private static List<Object> rowByName(List<List<Object>> rows, String processName) {
        return rows.stream().filter(row -> processName.equals(Objects.toString(row.get(2)))).findFirst().orElseThrow();
    }
}
