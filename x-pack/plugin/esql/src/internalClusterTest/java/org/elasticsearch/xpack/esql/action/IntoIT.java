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
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class IntoIT extends AbstractEsqlIntegTestCase {

    private static final String DESTINATION = "dest-into";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), IngestCommonPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true)
            .build();
    }

    @Before
    public void createDestination() {
        assumeTrue("INTO is only enabled in snapshot builds", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
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
