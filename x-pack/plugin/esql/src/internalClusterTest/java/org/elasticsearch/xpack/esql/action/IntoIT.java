/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.FailingFieldPlugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class IntoIT extends AbstractEsqlIntegTestCase {

    private static final String DESTINATION = "dest-into";
    private static final String SOURCE = "src-into";
    private static final String FAILING_SOURCE = "src-into-failing";
    private static final String RETRY_SOURCE = "src-into-retry";
    private static final String MAPPING = """
        {
          "dynamic": "strict",
          "properties": {
            "k":  { "type": "keyword" },
            "l":  { "type": "long" },
            "d":  { "type": "double" },
            "b":  { "type": "boolean" },
            "dt": { "type": "date" },
            "ip": { "type": "ip" }
          }
        }
        """;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.addAll(List.of(MockTransportService.TestPlugin.class, FailingFieldPlugin.class, FailOnceFieldPlugin.class));
        return plugins;
    }

    @Before
    public void createDestination() {
        assumeTrue("INTO is only enabled in snapshot builds", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
        assertAcked(indicesAdmin().prepareCreate(DESTINATION).setMapping(MAPPING));
    }

    public void testTypes() {
        assertInto(
            "ROW k = \"hello\", l = 42, d = 1.5, b = true, dt = TO_DATETIME(\"2026-01-15T12:34:56.789Z\"), ip = TO_IP(\"192.168.1.1\")",
            1
        );
        assertReadBack(List.of(List.of(true, 1.5, "2026-01-15T12:34:56.789Z", "192.168.1.1", "hello", 42L)));
    }

    public void testRowsAndMultiValues() {
        assertInto("ROW k = [\"alpha\", \"beta\"], l = [1, 2] | MV_EXPAND k", 2);
        assertReadBack("KEEP k, l | SORT k", List.of(List.of("alpha", List.of(1L, 2L)), List.of("beta", List.of(1L, 2L))));
    }

    public void testSourceOmitsNullsAndHasDatesInUtc() {
        assertInto(
            "SET time_zone = \"Europe/Paris\"; ROW k = \"only_k\", l = null::long, dt = TO_DATETIME(\"2026-01-15T12:34:56.789Z\")",
            1
        );
        refresh(DESTINATION);
        assertResponse(
            prepareSearch(DESTINATION),
            response -> assertThat(
                Arrays.stream(response.getHits().getHits()).map(SearchHit::getSourceAsMap).toList(),
                equalTo(List.of(Map.of("k", "only_k", "dt", "2026-01-15T12:34:56.789Z")))
            )
        );
    }

    public void testInlineStats() {
        assertInto("ROW k = [\"a\", \"b\"] | MV_EXPAND k | INLINE STATS l = COUNT(*)", 2);
        assertReadBack("KEEP k, l | SORT k", List.of(List.of("a", 2L), List.of("b", 2L)));
    }

    public void testZeroRows() {
        assertInto("ROW k = \"x\" | WHERE k == \"y\"", 0);
        assertInto("ROW k = \"x\" | WHERE false", 0);
    }

    public void testEvalColumnsAreWritten() {
        assertInto("ROW k = [\"ab\", \"c\"] | MV_EXPAND k | EVAL l = LENGTH(k)", 2);
        assertReadBack("KEEP k, l | SORT k", List.of(List.of("ab", 2L), List.of("c", 1L)));
    }

    public void testExplainIntoDoesNotIndex() {
        try (EsqlQueryResponse response = run("EXPLAIN (ROW k = \"explained\" | INTO " + DESTINATION + ")")) {
            List<String> plans = getValuesList(response).stream().map(row -> (String) row.getLast()).toList();
            assertThat(plans, hasItem(containsString("IntoExec")));
        }
        assertReadBack(List.of());
    }

    public void testRefusedRowFailsTheQuery() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> run("ROW k = \"ok\", unmapped_col = \"nope\" | INTO " + DESTINATION).close()
        );
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("unmapped_col"));
    }

    public void testFromSource() {
        createSource();
        assertInto("FROM " + SOURCE, 3);
        assertReadBack(
            "SORT k",
            List.of(
                List.of(true, 1.5, "2026-01-15T12:34:56.789Z", "192.168.1.1", "a", 1L),
                List.of(false, 2.5, "2026-01-16T12:34:56.789Z", "192.168.1.2", "b", 2L),
                List.of(true, 3.5, "2026-01-17T12:34:56.789Z", "192.168.1.3", "c", 3L)
            )
        );
    }

    public void testFromWithCommandsBeforeInto() {
        createSource();
        assertInto("FROM " + SOURCE + " | WHERE l > 1 | EVAL d = l * 10.0 | KEEP k, d", 2);
        assertReadBack("KEEP k, d, l | SORT k", List.of(Arrays.asList("b", 20.0, null), Arrays.asList("c", 30.0, null)));
    }

    public void testFromStatsInto() {
        createSource();
        assertInto("FROM " + SOURCE + " | STATS l = SUM(l) BY b | EVAL k = TO_STRING(b)", 2);
        assertReadBack("KEEP k, l | SORT k", List.of(List.of("false", 2L), List.of("true", 4L)));
    }

    public void testFromIntoSendsDataNodeRequests() {
        createSource();
        AtomicInteger dataNodeRequests = new AtomicInteger();
        for (String node : internalCluster().getNodeNames()) {
            MockTransportService.getInstance(node)
                .addRequestHandlingBehavior(ComputeService.DATA_ACTION_NAME, (handler, request, channel, task) -> {
                    dataNodeRequests.incrementAndGet();
                    handler.messageReceived(request, channel, task);
                });
        }
        try {
            assertInto("FROM " + SOURCE + " | KEEP k", 3);
            assertThat(dataNodeRequests.get(), greaterThan(0));
        } finally {
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node).clearAllRules();
            }
        }
        assertReadBack("KEEP k | SORT k", List.of(List.of("a"), List.of("b"), List.of("c")));
    }

    /**
     * A shard failure must fail an INTO query even when partial results are allowed, since the
     * rows of the other shards would otherwise be written while the query reports success.
     */
    public void testFromShardFailureFailsEvenWithPartialResults() {
        createSource();
        assertAcked(indicesAdmin().prepareCreate(FAILING_SOURCE).setMapping("""
            {
              "runtime": {
                "fail_me": { "type": "long", "script": { "source": "", "lang": "failing_field" } }
              },
              "properties": { "k": { "type": "keyword" } }
            }
            """));
        indexRandom(true, client().prepareIndex(FAILING_SOURCE).setSource("k", "boom"));
        EsqlQueryRequest request = syncEsqlQueryRequest(
            "FROM " + SOURCE + "," + FAILING_SOURCE + " | EVAL l = COALESCE(fail_me, l) | KEEP k, l | INTO " + DESTINATION
        ).allowPartialResults(true);
        // One shard at a time, so the failing shard fails alone and would be reported as a partial result
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_CONCURRENT_SHARDS_PER_NODE.getKey(), 1).build()));
        request.acceptedPragmaRisks(true);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> run(request).close());
        assertThat(e.getMessage(), equalTo("Accessing failing field"));
    }

    /**
     * A shard copy that fails after its writer has sent a bulk must fail the query. Retrying the shard on
     * another copy would write the rows of the first attempt again.
     */
    public void testShardFailureAfterBulkIsNotRetried() {
        assertAcked(indicesAdmin().prepareCreate(RETRY_SOURCE).setSettings(indexSettings(1, 1)).setMapping("""
            {
              "runtime": {
                "fail_once": { "type": "long", "script": { "source": "", "lang": "fail_once" } }
              },
              "properties": { "k": { "type": "keyword" } }
            }
            """));
        indexRandom(
            true,
            client().prepareIndex(RETRY_SOURCE).setSource("k", "a"),
            client().prepareIndex(RETRY_SOURCE).setSource("k", "b"),
            client().prepareIndex(RETRY_SOURCE).setSource("k", "c")
        );
        ensureGreen(RETRY_SOURCE);
        FailOnceFieldPlugin.CALLS.set(0);
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + RETRY_SOURCE + " | EVAL l = fail_once | KEEP k, l | INTO " + DESTINATION);
        // One row per page and one driver, so the first row is bulked before the second row fails
        request.pragmas(
            new QueryPragmas(
                Settings.builder()
                    .put(QueryPragmas.PAGE_SIZE.getKey(), 1)
                    .put(QueryPragmas.DATA_PARTITIONING.getKey(), "shard")
                    .put(QueryPragmas.TASK_CONCURRENCY.getKey(), 1)
                    .build()
            )
        );
        request.acceptedPragmaRisks(true);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> run(request).close());
        assertThat(e.getMessage(), equalTo("Failing once"));
        assertReadBack("STATS c = COUNT(*) BY k | WHERE c > 1", List.of());
    }

    public void testUnsupportedSourcesAreRejected() {
        String error = "INTO currently only supports ROW or local FROM as a source, without FORK";
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> run("FROM " + DESTINATION + " | FORK (WHERE true) (WHERE false) | INTO " + DESTINATION).close()
        );
        assertThat(e.getMessage(), containsString(error));
    }

    public void testAllowPartialResultsDoesNotBypassRowFailure() {
        ElasticsearchStatusException failed = expectThrows(
            ElasticsearchStatusException.class,
            () -> run(syncEsqlQueryRequest("ROW k = \"ok\", unmapped_col = \"nope\" | INTO " + DESTINATION).allowPartialResults(true))
                .close()
        );
        assertThat(failed.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(failed.getMessage(), containsString("unmapped_col"));
    }

    public void testIntoRunsOnCoordinatorWithoutDataNodeRequests() {
        AtomicInteger dataNodeRequests = new AtomicInteger();
        for (String node : internalCluster().getNodeNames()) {
            MockTransportService.getInstance(node)
                .addRequestHandlingBehavior(ComputeService.DATA_ACTION_NAME, (handler, request, channel, task) -> {
                    dataNodeRequests.incrementAndGet();
                    handler.messageReceived(request, channel, task);
                });
        }
        try {
            assertInto("ROW k = [\"r1\", \"r2\"] | MV_EXPAND k", 2);
            assertThat(dataNodeRequests.get(), equalTo(0));
        } finally {
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node).clearAllRules();
            }
        }
        assertReadBack("KEEP k | SORT k", List.of(List.of("r1"), List.of("r2")));
    }

    private void createSource() {
        assertAcked(indicesAdmin().prepareCreate(SOURCE).setSettings(indexSettings(between(2, 5), 0)).setMapping(MAPPING));
        indexRandom(
            true,
            client().prepareIndex(SOURCE)
                .setSource("k", "a", "l", 1, "d", 1.5, "b", true, "dt", "2026-01-15T12:34:56.789Z", "ip", "192.168.1.1"),
            client().prepareIndex(SOURCE)
                .setSource("k", "b", "l", 2, "d", 2.5, "b", false, "dt", "2026-01-16T12:34:56.789Z", "ip", "192.168.1.2"),
            client().prepareIndex(SOURCE)
                .setSource("k", "c", "l", 3, "d", 3.5, "b", true, "dt", "2026-01-17T12:34:56.789Z", "ip", "192.168.1.3")
        );
    }

    private void assertInto(String producer, long rows) {
        try (EsqlQueryResponse response = run(producer + " | INTO " + DESTINATION)) {
            assertColumnNames(response.columns(), List.of("total", "created"));
            assertColumnTypes(response.columns(), List.of("long", "long"));
            assertThat(getValuesList(response), equalTo(List.of(List.of(rows, rows))));
        }
    }

    private void assertReadBack(List<List<Object>> expected) {
        assertReadBack("", expected);
    }

    private void assertReadBack(String commands, List<List<Object>> expected) {
        refresh(DESTINATION);
        String query = commands.isEmpty() ? "FROM " + DESTINATION : "FROM " + DESTINATION + " | " + commands;
        try (EsqlQueryResponse response = run(query)) {
            assertThat(getValuesList(response), equalTo(expected));
        }
    }

    /** A {@code long} runtime field that throws on its second evaluation in the JVM, and returns no value otherwise. */
    public static class FailOnceFieldPlugin extends Plugin implements ScriptPlugin {
        static final AtomicInteger CALLS = new AtomicInteger();

        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new ScriptEngine() {
                @Override
                public String getType() {
                    return "fail_once";
                }

                @Override
                @SuppressWarnings("unchecked") // the only supported context is LongFieldScript.CONTEXT
                public <FactoryType> FactoryType compile(
                    String name,
                    String code,
                    ScriptContext<FactoryType> context,
                    Map<String, String> params
                ) {
                    LongFieldScript.Factory factory = (fieldName, scriptParams, searchLookup, onScriptError) -> ctx -> new LongFieldScript(
                        fieldName,
                        scriptParams,
                        searchLookup,
                        onScriptError,
                        ctx
                    ) {
                        @Override
                        public void execute() {
                            if (CALLS.incrementAndGet() == 2) {
                                throw new IllegalStateException("Failing once");
                            }
                        }
                    };
                    return (FactoryType) factory;
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.of(LongFieldScript.CONTEXT);
                }
            };
        }
    }
}
