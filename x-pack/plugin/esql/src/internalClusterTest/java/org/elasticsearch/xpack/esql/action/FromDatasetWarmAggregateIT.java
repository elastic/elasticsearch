/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.parquet.example.data.Group;
import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.dataset.DeleteDatasetAction;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * The warm-aggregate cache (PR #150920) serves COUNT/MIN/MAX from cached per-column summaries keyed by the file's
 * PHYSICAL column names and INFERRED types. A declared mapping renames and re-types columns AFTER those summaries are
 * produced, so without the declared-overlay stats boundary a warm {@code COUNT(renamed)} silently serves 0 and a warm
 * aggregate over a re-typed column serves a value a coerced scan never produces. These tests pin the fix end to end:
 * a renamed column stays warm AND correct; a re-typed column falls back to a correct scan; {@code COUNT(*)} stays warm.
 * <p>
 * Parquet serves from footer statistics at resolution, so a warm serve shows up as {@code externalWarmAggregates() == 1}
 * with {@code documentsFound() == 0} (nothing scanned); a safe-miss shows {@code externalWarmAggregates() == 0} and a
 * non-zero scan.
 */
public class FromDatasetWarmAggregateIT extends AbstractExternalDataSourceIT {

    private static final String SRC = "warm_agg_src";
    private final List<String> mappedDatasets = new ArrayList<>();

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    @After
    public void cleanupMappedDatasets() {
        for (String dataset : mappedDatasets) {
            try {
                client().execute(DeleteDatasetAction.INSTANCE, new DeleteDatasetAction.Request(TIMEOUT, TIMEOUT, new String[] { dataset }))
                    .actionGet();
            } catch (Exception ignored) {
                // best-effort teardown, mirroring the base cleanupRegistry()
            }
        }
    }

    /** Registers a mapping-carrying dataset (the base registerDataset helper has no mapping arg). */
    private void putMappedDataset(String name, String resourceUri, Map<String, Object> settings, DatasetMapping mapping) {
        registerDataSource(SRC, Map.of()); // idempotent-per-test; base tracks + tears down the source
        assertAcked(
            client().execute(
                PutDatasetAction.INSTANCE,
                new PutDatasetAction.Request(TIMEOUT, TIMEOUT, name, SRC, resourceUri, null, new HashMap<>(settings), mapping)
            )
        );
        mappedDatasets.add(name);
    }

    /**
     * Renamed column stays warm and correct. The stats live under the physical name {@code id}; the query asks for the
     * logical name {@code emp_id}. Before the fix, the physical-keyed stat is looked up by the logical name, misses, and
     * the footer implicit-nulls contract makes {@code COUNT(emp_id)} serve 0. After the rename-aware rekey it serves the
     * true count, warm. Declared {@code long} equals inferred {@code long}, so this is a pure rename (no poison) — it
     * proves rename-only columns keep the warm speed-up.
     */
    public void testRenamedColumnCountIsServedWarmAndCorrect() throws Exception {
        int rows = 100;
        Path parquet = writeParquet(createTempDir().resolve("emp.parquet"), rows, 100); // {id:int64, name:binary, value:int32}
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("emp_id", new DatasetFieldMapping("long", "id")); // rename id -> emp_id (same type)
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
        putMappedDataset("warm_rename", StoragePath.fileUri(parquet), Map.of("format", "parquet"), mapping);

        try (var response = run(syncEsqlQueryRequest("FROM warm_rename | STATS c = COUNT(emp_id)").profile(true), TIMEOUT)) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo((long) rows));
            assertEquals(
                "renamed COUNT must be served warm, not scanned",
                1,
                response.getExecutionInfo().queryProfile().externalWarmAggregates()
            );
            assertThat("warm serve scans nothing", (int) response.documentsFound(), equalTo(0));
        }
    }

    /**
     * Re-typed column falls back to a correct scan, and {@code COUNT(*)} stays warm. {@code code} is {@code int64} in the
     * file, declared {@code keyword}. The footer MIN of {@code code} is the numeric {@code 2}; before the fix a warm
     * {@code MIN(code_str)} serves the stringified {@code "2"} (numeric order), which is wrong — the lexicographic min of
     * {@code {"9","10","2"}} is {@code "10"}. After poisoning the re-typed column's extrema the query scans and answers
     * {@code "10"}. {@code COUNT(*)} still serves warm from the surviving row count.
     */
    public void testRetypedColumnSafeMissesButCountStarStaysWarm() throws Exception {
        Path parquet = writeParquet(
            createTempDir().resolve("codes.parquet"),
            "message test { required int64 code; }",
            3,
            100,
            (Group g, int i) -> g.add("code", new long[] { 9L, 10L, 2L }[i])
        );
        Map<String, DatasetFieldMapping> properties = new LinkedHashMap<>();
        properties.put("code_str", new DatasetFieldMapping("keyword", "code")); // int64 -> keyword (re-type)
        DatasetMapping mapping = new DatasetMapping(new DatasetMapping.Mappings(DatasetMapping.Dynamic.TRUE, properties));
        putMappedDataset("warm_retype", StoragePath.fileUri(parquet), Map.of("format", "parquet"), mapping);

        // MIN over the re-typed column must be the lexicographic min via a scan, not the footer's numeric min stringified.
        try (var response = run(syncEsqlQueryRequest("FROM warm_retype | STATS m = MIN(code_str)").profile(true), TIMEOUT)) {
            assertThat(String.valueOf(getValuesList(response).get(0).get(0)), equalTo("10"));
            assertEquals(
                "re-typed MIN must safe-miss, not serve warm",
                0,
                response.getExecutionInfo().queryProfile().externalWarmAggregates()
            );
            assertThat("re-typed MIN scans", (int) response.documentsFound(), greaterThan(0));
        }

        // COUNT(*) is type-independent: the row count survives the poison, so it stays warm.
        try (var response = run(syncEsqlQueryRequest("FROM warm_retype | STATS c = COUNT(*)").profile(true), TIMEOUT)) {
            assertThat(((Number) getValuesList(response).get(0).get(0)).longValue(), equalTo(3L));
            assertEquals("COUNT(*) stays warm", 1, response.getExecutionInfo().queryProfile().externalWarmAggregates());
            assertThat((int) response.documentsFound(), equalTo(0));
        }
    }
}
