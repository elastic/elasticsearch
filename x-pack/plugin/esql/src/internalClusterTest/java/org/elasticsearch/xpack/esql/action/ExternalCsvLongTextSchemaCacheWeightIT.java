/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheService;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheTestAccess;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * End-to-end guard for {@code elastic/esql-planning#2075}: after a MIN/MAX harvest over CSV files whose
 * text cells are a megabyte wide, the coordinator schema cache's retained weight must stay inside its
 * slice of {@code esql.external.cache.size}, and oversized extrema must not be retained.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class ExternalCsvLongTextSchemaCacheWeightIT extends AbstractExternalDataSourceIT {

    private static final int FILE_COUNT = 8;
    private static final int VALUE_CHARS = 1_000_000;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put("esql.external.cache.size", "2mb").build();
    }

    public void testSchemaRetainedWeightStaysInsideBudgetAfterLongTextMinMax() throws Exception {
        Path dir = createTempDir();
        String pad = "x".repeat(VALUE_CHARS);
        // Distinct prefixes so MIN/MAX are well-defined (lexicographic) and the query result is checkable.
        String expectedMin = "a0-" + pad;
        String expectedMax = "a" + (FILE_COUNT - 1) + "-" + pad;
        for (int i = 0; i < FILE_COUNT; i++) {
            Path file = dir.resolve("part" + i + ".csv");
            String note = "a" + i + "-" + pad;
            Files.writeString(file, "id,note\n" + i + "," + note + "\n");
        }
        String dataset = registerLocalFileDataset("wide_text", StoragePath.fileUri(dir) + "/*.csv", Map.of("format", "csv"));
        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | STATS MIN(note), MAX(note)"), TimeValue.timeValueMinutes(5))) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.size(), equalTo(1));
            assertThat(rows.get(0).get(0), equalTo(expectedMin));
            assertThat(rows.get(0).get(1), equalTo(expectedMax));
        }

        ExternalSourceCacheService cacheService = internalCluster().getInstance(PlanExecutor.class, internalCluster().getMasterName())
            .cacheService();
        Map<String, Object> stats = cacheService.usageStats();
        long schemaBudget = (Long) stats.get("schema_budget_bytes");
        long retained = ExternalSourceCacheTestAccess.retainedSchemaWeightBytes(cacheService);
        assertThat(
            "schema cache retained weight [" + retained + "] must stay inside schema budget [" + schemaBudget + "]",
            retained,
            lessThanOrEqualTo(schemaBudget)
        );
        // Megabyte-wide extrema exceed the per-entry ceiling (quarter of the schema slice at a 2mb
        // total budget — well above the tiny-budget floor), so refuse-before-put leaves no per-file
        // schema entries. Retained-weight ≤ budget is the durable bound; count == 0 is the refuse-path
        // signal for this fixture's ceiling coupling.
        assertThat(
            "oversized long-text extrema must not be retained in the schema cache",
            (Integer) stats.get("schema_cache.count"),
            equalTo(0)
        );
    }
}
