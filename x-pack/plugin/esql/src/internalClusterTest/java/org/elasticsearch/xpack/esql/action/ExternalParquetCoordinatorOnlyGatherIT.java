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
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * End-to-end guard for the gather-boundary decision in {@code ComputeService.applyExternalDistributionStrategy}. Under
 * the {@code external_distribution=coordinator_only} pragma the scan is kept off data nodes, so an ungrouped {@code STATS}
 * that runs several parallel partial-aggregation scan drivers on the coordinator must gather those partials into a single
 * final row. Dropping the gather boundary (collapsing the external exchange instead) would emit one row per parallel
 * driver, so this test pins the correct single-row result.
 *
 * <p>The scenario is made deterministic rather than left to the host: the number of parallel scan drivers is
 * {@code min(splitCount, task_concurrency)}, so both knobs are fixed. {@code task_concurrency} is pinned to {@value
 * #TASK_CONCURRENCY} (the class-load default is derived from the host processor count and can collapse to 1), and the
 * file count is kept below the split-coalescing threshold so every file stays its own split ({@code splitCount ==
 * fileCount}) instead of being coalesced down to a host-dependent group count. Both together guarantee more than one
 * parallel partial-aggregation driver, which the test also asserts from the query profile so the multi-driver gather path
 * cannot be silently skipped.
 */
public class ExternalParquetCoordinatorOnlyGatherIT extends AbstractExternalDataSourceIT {

    private static final int TASK_CONCURRENCY = 4;

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    public void testUngroupedStatsGathersToSingleRowUnderCoordinatorOnly() throws Exception {
        // Below the split-coalescing threshold (32), so the files are never coalesced and splitCount == fileCount. With
        // task_concurrency pinned to 4, the scan runs min(fileCount, 4) = 4 parallel partial-aggregation drivers whose
        // partials must be gathered.
        int fileCount = 8;
        Path dir = createTempDir().resolve("coordinator_only_gather");
        Files.createDirectories(dir);
        // One single-row file per split; file i carries value=i.
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

        QueryPragmas pragmas = new QueryPragmas(
            Settings.builder()
                .put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), "coordinator_only")
                .put(QueryPragmas.TASK_CONCURRENCY.getKey(), TASK_CONCURRENCY)
                .build()
        );
        // SUM is not metadata-pushable, so split discovery runs and every file becomes its own split; the ungrouped STATS
        // then depends on the gather boundary to merge the parallel drivers' partials into a single row.
        String query = "FROM " + dataset + " | STATS s = SUM(value), c = COUNT(*)";
        var request = syncEsqlQueryRequest(query).pragmas(pragmas);
        request.acceptedPragmaRisks(true); // pragmas are rejected on non-snapshot builds without this
        request.profile(true);

        try (var response = run(request)) {
            // Keeps the single-row assertion below from passing vacuously. Each parallel scan driver emits its own
            // partial aggregation into the exchange, so with a single driver there is nothing to gather and a missing
            // gather boundary would still yield one row. Requiring >= 2 scan drivers means the exchange has >= 2 partials
            // to merge, so a dropped gather boundary surfaces as >= 2 rows below rather than going undetected.
            assertThat(
                "expected more than one parallel partial-aggregation scan driver to feed the gather",
                externalScanStatuses(response).size(),
                greaterThanOrEqualTo(2)
            );

            List<List<Object>> rows = getValuesList(response);
            assertThat("ungrouped STATS must gather to exactly one row, not one per parallel driver", rows.size(), equalTo(1));
            long expectedSum = (long) fileCount * (fileCount - 1) / 2;
            assertThat(
                "SUM must aggregate every file across all parallel drivers",
                ((Number) rows.get(0).get(0)).longValue(),
                equalTo(expectedSum)
            );
            assertThat(
                "COUNT must total every file across all parallel drivers",
                ((Number) rows.get(0).get(1)).longValue(),
                equalTo((long) fileCount)
            );
        }
    }
}
