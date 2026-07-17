/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end guard for the gather-boundary decision in {@code ComputeService.applyExternalDistributionStrategy}. Under
 * the {@code external_distribution=coordinator_only} pragma the scan is kept off data nodes, so a multi-split ungrouped
 * {@code STATS} has to gather its parallel per-split partial aggregations on the coordinator into a single final row.
 * Dropping the gather boundary (collapsing the external exchange instead) would emit one row per split group, so this
 * test pins the correct single-row result.
 */
public class ExternalParquetCoordinatorOnlyGatherIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    @Override
    protected QueryPragmas getPragmas() {
        return new QueryPragmas(Settings.builder().put("external_distribution", "coordinator_only").build());
    }

    public void testUngroupedStatsGathersToSingleRowUnderCoordinatorOnly() throws Exception {
        int fileCount = 100;
        Path dir = createTempDir().resolve("coordinator_only_gather");
        Files.createDirectories(dir);
        // One single-row file per split: 100 files means 100 external splits, well above the coalescing threshold, so
        // the scan runs as many split groups. File i carries value=i.
        for (int i = 0; i < fileCount; i++) {
            int value = i;
            writeParquet(
                dir.resolve("f" + i + ".parquet"),
                "message test { required int32 value; }",
                1,
                1024,
                (g, r) -> g.add("value", value)
            );
        }
        String glob = StoragePath.fileUri(dir) + "/*.parquet";
        String dataset = registerDataset("coordinator_only_gather", glob, Map.of());

        // SUM is not metadata-pushable, so split discovery runs and the many files become multiple split groups; the
        // ungrouped STATS then depends on the gather boundary to merge them into a single row.
        String query = "FROM " + dataset + " | STATS s = SUM(value), c = COUNT(*)";

        try (var response = run(query)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat("ungrouped STATS must gather to exactly one row, not one per split group", rows.size(), equalTo(1));
            long expectedSum = (long) fileCount * (fileCount - 1) / 2;
            assertThat(
                "SUM must aggregate every file across all split groups",
                ((Number) rows.get(0).get(0)).longValue(),
                equalTo(expectedSum)
            );
            assertThat(
                "COUNT must total every file across all split groups",
                ((Number) rows.get(0).get(1)).longValue(),
                equalTo((long) fileCount)
            );
        }
    }
}
