/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.ingest.IngestPipelineTestUtils;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class IntoIT extends AbstractEsqlIntegTestCase {

    private static final String DESTINATION = "dest-into";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(super.nodePlugins(), IngestCommonPlugin.class),
            MockTransportService.TestPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (BatchIndexingEnabled.FEATURE_FLAG.isEnabled()) {
            // The setting is rejected while the feature flag is off, which would fail the cluster before the tests can be skipped.
            settings.put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true);
        }
        return settings.build();
    }

    @Before
    public void createDestination() {
        assumeTrue("INTO is only enabled in snapshot builds", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
        assumeTrue("INTO writes through batch indexing", BatchIndexingEnabled.FEATURE_FLAG.isEnabled());
        assertAcked(indicesAdmin().prepareCreate(DESTINATION).setSettings(batchIndexingSettings()).setMapping("""
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
            """));
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
        assertSources(DESTINATION, List.of(Map.of("k", "only_k", "dt", "2026-01-15T12:34:56.789Z")));
    }

    public void testInlineStats() {
        assertInto("ROW k = [\"a\", \"b\"] | MV_EXPAND k | INLINE STATS l = COUNT(*)", 2);
        assertReadBack("KEEP k, l | SORT k", List.of(List.of("a", 2L), List.of("b", 2L)));
    }

    public void testDateMathDestination() {
        String dateMathDest = "<dest-into-{now/d}>";
        String resolvedDest = IndexNameExpressionResolver.resolveDateMathExpression(dateMathDest);
        assertAcked(
            indicesAdmin().prepareCreate(resolvedDest)
                .setSettings(batchIndexingSettings())
                .setMapping("{\"properties\":{\"k\":{\"type\":\"keyword\"}}}")
        );
        assertIntoTarget("ROW k = \"x\"", dateMathDest, 1);
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

    /**
     * A destination with an ingest pipeline cannot be written as a pre-built batch, so the query fails.
     */
    public void testIngestPipelineIsRejected() {
        String pipeline = "into-add-field-pipeline";
        String pipelineDest = "dest-into-with-pipeline";

        IngestPipelineTestUtils.putJsonPipeline(client(), pipeline, """
            {"processors": [{"set": {"field": "pipeline_ran", "value": true}}]}""");

        assertAcked(
            indicesAdmin().prepareCreate(pipelineDest)
                .setSettings(batchIndexingSettings().put("index.default_pipeline", pipeline))
                .setMapping("""
                    {
                      "dynamic": "strict",
                      "properties": {
                        "k":            { "type": "keyword" },
                        "pipeline_ran": { "type": "boolean" }
                      }
                    }""")
        );

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> run("ROW k = \"pipelined\" | INTO " + pipelineDest).close()
        );
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("ingest pipelines"));
    }

    public void testAllowPartialResultsDoesNotBypassRejectionOrRowFailure() {
        VerificationException rejected = expectThrows(
            VerificationException.class,
            () -> run(syncEsqlQueryRequest("FROM " + DESTINATION + " | INTO " + DESTINATION).allowPartialResults(true)).close()
        );
        assertThat(rejected.getMessage(), containsString("INTO currently only supports ROW as a source, without FORK"));

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

    private void assertSources(String index, List<Map<String, Object>> expected) {
        refresh(index);
        assertResponse(
            prepareSearch(index),
            r -> assertThat(Arrays.stream(r.getHits().getHits()).map(SearchHit::getSourceAsMap).toList(), equalTo(expected))
        );
    }

    private static Settings.Builder batchIndexingSettings() {
        return Settings.builder().put(IndexSettings.BATCH_INDEXING.getKey(), true);
    }

    private void assertInto(String producer, long rows) {
        assertIntoTarget(producer, DESTINATION, rows);
    }

    private void assertIntoTarget(String producer, String target, long rows) {
        try (EsqlQueryResponse response = run(producer + " | INTO " + target)) {
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
}
